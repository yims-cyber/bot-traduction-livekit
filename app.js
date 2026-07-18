require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const { AccessToken } = require('livekit-server-sdk');
let GoogleGenAI, LiveKitRtc, LiveKitAudioFrame;
 
const app = express();
app.use(cors());
app.use(express.json());
app.options('/events', cors());

const PORT = process.env.PORT || 3000;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const TRANSLATE_MODEL = 'gemini-3.5-live-translate-preview';
const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY;
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET;
const LIVEKIT_URL = process.env.LIVEKIT_URL;
const O2SWITCH_API_URL = process.env.O2SWITCH_API_URL;
const API_SECRET = process.env.API_SECRET;
const BOT_VERSION = '1.1.2';

/**
 * Configuration des langues - TESTÉE avec la documentation officielle Google
 * - Codes BCP-47 officiels (source : ai.google.dev/gemini-api/docs/live-api/live-translate)
 * - Lingala (ln) n'a pas de voix de sortie TTS dans le modèle : SOUS-TITRES UNIQUEMENT
 */
const LANG_CONFIG = {
    'fr': { targetCode: 'fr',   echo: true,  audioOut: true, label: 'Français' },
    'ln': { targetCode: 'ln',   echo: false, audioOut: false, label: 'Lingala (sous-titres)' },
    'sw': { targetCode: 'sw',   echo: false, audioOut: true, label: 'Swahili' },
    'en': { targetCode: 'en',   echo: false, audioOut: true, label: 'Anglais' },
    'pt': { targetCode: 'pt-PT',echo: false, audioOut: true, label: 'Portugais' },
    'es': { targetCode: 'es',   echo: false, audioOut: true, label: 'Espagnol' },
    'de': { targetCode: 'de',   echo: false, audioOut: true, label: 'Allemand' },
};
const LANGUES = Object.keys(LANG_CONFIG);

const SAMPLE_RATE = 16000;
const CHUNK_MS = 100;
const BYTES_PER_CHUNK = Math.round(SAMPLE_RATE * (CHUNK_MS / 1000)) * 2; // 3200 octets
const GEMINI_OUTPUT_SAMPLE_RATE = 24000;
// Latence maximum avant vidange de la file (800ms)
const MAX_LATENCY_MS = 800;
const orators = new Map();

function log(msg) {
    const line = `[${new Date().toISOString()}] ${msg}`;
    console.log(line);
    broadcastSSE(msg);
}
const sseClients = new Set();
function broadcastSSE(msg) {
    const clean = msg.replace(/^\[[^\]]+\]\s*/, '');
    const data = `data: ${JSON.stringify({t: Date.now(), m: clean})}\n\n`;
    for (const res of sseClients) { try { res.write(data); } catch(e){} }
}
log(`=== Serveur traduction v${BOT_VERSION} - son TESTÉ ===`);
process.on('uncaughtException', (err) => log(`💥 ERREUR: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 REJET: ${reason?.stack || reason}`));
let ai;

async function ensureLiveKitRtc() {
    if (!LiveKitRtc) {
        log('Chargement module @livekit/rtc-node...');
        LiveKitRtc = await import('@livekit/rtc-node');
        LiveKitAudioFrame = LiveKitRtc.AudioFrame;
    }
    return LiveKitRtc;
}

async function generateLiveKitToken(room, identity, role) {
    const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, { identity, ttl: '6h' });
    at.addGrant({ room, roomJoin: true, canPublish: role === 'orator' || role === 'translator', canSubscribe: true, canPublishData: true });
    return await at.toJwt();
}

async function saveTranscription(roomId, langue, texte) {
    if (!O2SWITCH_API_URL || !texte || texte.length < 2) return;
    fetch(`${O2SWITCH_API_URL}/save-transcription.php`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: API_SECRET, session_id: roomId, langue, texte, est_final: true }),
    }).catch(e => log(`❌ Sauvegarde BDD: ${e.message}`));
}

function broadcastSubtitle(room, langue, texte) {
    if (!room || !texte) return;
    try {
        const payload = Buffer.from(JSON.stringify({ type: 'sous_titre', texte, langue }), 'utf-8');
        room.localParticipant.publishData(payload, { reliable: true, topic: `sous-titres-${langue}` })
            .then(() => {})
            .catch(e => log(`❌ Sous-titre ${langue}: ${e.message}`));
    } catch(e) {}
}

let audioPumpStarted = false;
function attachOratorTrack(orateurId, track, pub, participant, room) {
    if (!track) return;
    if ((pub?.name || '') !== 'orator-mic') return;
    if (audioPumpStarted) return;
    audioPumpStarted = true;
    log(`🎙️ MICRO ORATEUR CONNECTÉ ! Démarrage traduction...`);
    LANGUES.forEach(l => getOrCreateLangSession(orateurId, l, room));
    pumpAudioTrack(orateurId, track);
}

async function createLangSession(orateurId, lang, room) {
    const cfg = LANG_CONFIG[lang];
    const s = {
        geminiSession: null, ready: false, pendingAudio: [],
        lastText: '', closing: false,
        audioSource: null,
        audioQueue: Promise.resolve(),
        queueLatencyMs: 0,
        cfg, lang,
    };

    try {
        log(`🔄 Connexion Gemini [${lang}] (${cfg.label})...`);
        const geminiSession = await ai.live.connect({
            model: TRANSLATE_MODEL,
            config: {
                responseModalities: ['AUDIO'],
                inputAudioTranscription: {},
                outputAudioTranscription: {},
                translationConfig: {
                    targetLanguageCode: cfg.targetCode,
                    echoTargetLanguage: cfg.echo,
                },
            },
            callbacks: {
                onopen: () => {
                    log(`✅ Gemini [${lang}] CONNECTÉ ! Envoi ${s.pendingAudio.length} chunks en attente...`);
                    s.ready = true;
                    s.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => {
                        try { geminiSession.sendRealtimeInput({ media: { data: buf.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); }
                        catch(e) { log(`❌ Chunk attente [${lang}]: ${e.message}`); }
                    });
                    s.pendingAudio = [];
                },
                onmessage: (message) => {
                    try {
                        const c = message.serverContent;
                        if (!c) { if (message.setupComplete) log(`🔧 [${lang}] setup terminé`); return; }

                        if (c.inputTranscription?.text) {
                            const t = c.inputTranscription.text.trim();
                            if (t && t !== s.lastText && lang === 'fr') {
                                s.lastText = t;
                                log(`📝 FR: "${t}"`);
                                saveTranscription(orateurId, lang, t);
                                broadcastSubtitle(room, lang, t);
                            }
                        }
                        if (c.outputTranscription?.text) {
                            const t = c.outputTranscription.text.trim();
                            if (t) {
                                saveTranscription(orateurId, lang, t);
                                broadcastSubtitle(room, lang, t);
                                if (!cfg.audioOut) log(`📝 [${lang}] SOUS-TITRE: "${t}"`);
                                else log(`🌐 [${lang}] "${t}"`);
                            }
                        }
                        if (c.turnComplete) s.lastText = '';

                        // ✅ TRAITEMENT AUDIO IDENTIQUE À v1.0.9 qui fonctionnait
                        if (cfg.audioOut && c.modelTurn?.parts && s.audioSource) {
                            for (const p of c.modelTurn.parts) {
                                if (p.inlineData?.data) {
                                    const buf = Buffer.from(p.inlineData.data, 'base64');
                                    const numBytes = buf.length;
                                    const numSamples = numBytes / 2;
                                    const durationMs = (numSamples / GEMINI_OUTPUT_SAMPLE_RATE) * 1000;

                                    const i16 = new Int16Array(buf.buffer, buf.byteOffset, numSamples);
                                    const frame = new LiveKitAudioFrame(i16, GEMINI_OUTPUT_SAMPLE_RATE, 1, numSamples);

                                    // ✅ GESTION DE LATENCE : si la file dépasse MAX_LATENCY_MS,
                                    // ON REMPLACE COMPLÈTEMENT la file (pas de coupure dans un mot)
                                    // plutôt que de sauter des frames au milieu
                                    if (s.queueLatencyMs > MAX_LATENCY_MS) {
                                        log(`⚠️ Latence [${lang}] ${Math.round(s.queueLatencyMs)}ms → vidage de la file pour rattraper le direct`);
                                        s.audioQueue = Promise.resolve();
                                        s.queueLatencyMs = 0;
                                    }
                                    s.queueLatencyMs += durationMs;
                                    s.audioQueue = s.audioQueue.then(() => {
                                        s.queueLatencyMs = Math.max(0, s.queueLatencyMs - durationMs);
                                        return s.audioSource.captureFrame(frame);
                                    }).catch(e => log(`❌ Audio ${lang}: ${e.message}`));
                                }
                            }
                        }
                    } catch(e) { log(`❌ Message [${lang}]: ${e.message}`); }
                },
                onerror: (e) => { log(`❌ Gemini [${lang}] ERREUR: ${e?.message || e}`); if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
                onclose: (e) => { log(`🔌 Gemini [${lang}] déconnecté, reconnexion...`); s.ready = false; if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
            }
        });
        s.geminiSession = geminiSession;
    } catch(e) { log(`❌ Session ${lang}: ${e.message}`); }
    return s;
}

async function reconnectLangSession(orateurId, lang) {
    const o = orators.get(orateurId); if (!o) return;
    const old = o.langSessions.get(lang); if (!old || old.closing) return;
    const n = await createLangSession(orateurId, lang, o.bot?.room);
    n.audioSource = old.audioSource;
    n.audioQueue = Promise.resolve();
    n.queueLatencyMs = 0;
    o.langSessions.set(lang, n);
}

function getOrator(id) {
    let o = orators.get(id);
    if (!o) { o = { langSessions: new Map(), bot: null }; orators.set(id, o); }
    return o;
}

async function getOrCreateLangSession(id, lang, room) {
    const o = getOrator(id); let s = o.langSessions.get(lang);
    if (s) return s;
    s = await createLangSession(id, lang, room);
    o.langSessions.set(lang, s);
    if (s.cfg.audioOut) publishLangAudioTrack(id, lang, s);
    else log(`ℹ️ [${lang}] Pas de sortie audio (sous-titres uniquement)`);
    return s;
}

function feedAudio(id, buffer, seq) {
    const o = orators.get(id); if (!o) return;
    for (const [lang, s] of o.langSessions.entries()) {
        if (s.ready && s.geminiSession) {
            try { s.geminiSession.sendRealtimeInput({ media: { data: buffer.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); }
            catch(e) { s.pendingAudio.push({seq, buf: Buffer.from(buffer)}); }
        } else s.pendingAudio.push({seq, buf: Buffer.from(buffer)});
    }
}

async function publishLangAudioTrack(id, lang, s) {
    const o = getOrator(id); if (!o.bot?.room) return;
    const room = o.bot.room; const rtc = await ensureLiveKitRtc();
    // ✅ CONSTRUCTEUR TESTÉ : new AudioSource(rate, channels) - PAS d'objet options !
    const src = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
    const tr = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, src);
    await room.localParticipant.publishTrack(tr, { name: `lang-${lang}` });
    s.audioSource = src;
    log(`📡 Piste audio [${lang}] publiée`);
}

async function pumpAudioTrack(id, track) {
    const rtc = await ensureLiveKitRtc();
    log(`🎧 Pompe audio démarrée...`);
    const stream = new rtc.AudioStream(track, { sampleRate: SAMPLE_RATE, numChannels: 1 });
    let leftover = Buffer.alloc(0), seq = 0, totalBytes = 0, lastLog = Date.now();
    try {
        for await (const frame of stream) {
            const fb = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
            totalBytes += fb.length;
            let comb = leftover.length ? Buffer.concat([leftover, fb]) : fb;
            let off = 0;
            while (comb.length - off >= BYTES_PER_CHUNK) {
                feedAudio(id, Buffer.from(comb.subarray(off, off+BYTES_PER_CHUNK)), seq++);
                off += BYTES_PER_CHUNK;
            }
            leftover = off < comb.length ? Buffer.from(comb.subarray(off)) : Buffer.alloc(0);
            if (Date.now() - lastLog > 3000) {
                const queues = [];
                const o = orators.get(id);
                if (o) for (const [l, s] of o.langSessions) queues.push(`${l}:${Math.round(s.queueLatencyMs)}ms`);
                log(`📊 ${Math.round(totalBytes/1024)} KB → ${seq} chunks | latence=[${queues.join(',')}]`);
                totalBytes = 0; lastLog = Date.now();
            }
        }
    } catch(e) { log(`❌ Flux audio interrompu: ${e.message}`); }
}

async function scanForMic(orateurId, room) {
    const rtc = await ensureLiveKitRtc();
    const parts = Array.from(room.remoteParticipants.values());
    for (const p of parts) {
        const pubs = p.publications ? Array.from(p.publications.values()) : [];
        log(`🔍 Scan: ${p.identity} → ${pubs.length} pistes`);
        for (const pub of pubs) {
            const isAudio = pub.kind === rtc.TrackKind.KIND_AUDIO;
            if (isAudio && pub.name === 'orator-mic') {
                try {
                    let track = pub.track;
                    if (!track) track = await pub.setSubscribed(true);
                    attachOratorTrack(orateurId, track, pub, p, room);
                    return true;
                } catch(e) { log(`❌ Abonnement: ${e.message}`); }
            }
        }
    }
    return audioPumpStarted;
}

async function startBotForRoom(orateurId) {
    const orator = getOrator(orateurId);
    if (orator.bot) return orator.bot.connecting;
    if (!LIVEKIT_URL) { log('❌ LIVEKIT_URL manquant'); return; }
    if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY MANQUANTE !'); return; }
    audioPumpStarted = false;
    log(`🚀 Démarrage bot pour ${orateurId}`);
    const connecting = (async () => {
        const rtc = await ensureLiveKitRtc();
        const identity = 'bot-' + Math.random().toString(36).slice(2,8);
        const token = await generateLiveKitToken(orateurId, identity, 'translator');
        const room = new rtc.Room();

        room.on(rtc.RoomEvent.TrackSubscribed, (track, pub, part) => {
            log(`📥 Piste souscrite: ${pub.name}`);
            attachOratorTrack(orateurId, track, pub, part, room);
        });
        room.on(rtc.RoomEvent.TrackPublished, (pub, part) => {
            log(`📢 Nouvelle piste: ${pub.name} de ${part.identity}`);
            if (pub.name === 'orator-mic' && pub.kind === rtc.TrackKind.KIND_AUDIO) {
                pub.setSubscribed(true).then(t => attachOratorTrack(orateurId, t, pub, part, room)).catch(e=>log(`❌ ${e.message}`));
            }
        });
        room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté`); if (orator.bot) orator.bot = null; });
        room.on(rtc.RoomEvent.ParticipantConnected, p => log(`👋 Participant: ${p.identity}`));

        await room.connect(LIVEKIT_URL, token, { autoSubscribe: false });
        log(`✅ Bot connecté`);
        for (let i = 0; i < 20; i++) {
            await new Promise(r => setTimeout(r, 500));
            if (audioPumpStarted) break;
            await scanForMic(orateurId, room);
        }
        return room;
    })();
    orator.bot = { room: null, connecting };
    connecting.then(r => { if (orator.bot) orator.bot.room = r; }).catch(e => { log(`❌ Échec: ${e.message}`); orator.bot = null; });
    return connecting;
}

function stopBotForRoom(id) {
    const o = orators.get(id); if (!o) return;
    log(`⏹️ Arrêt bot`);
    try { o.bot?.room?.disconnect(); } catch(e){}
    for (const s of o.langSessions.values()) {
        s.closing = true;
        try { s.geminiSession?.close(); } catch(e){}
        try { s.audioSource?.close(); } catch(e){}
    }
    orators.delete(id);
}

app.get('/health', (req, res) => res.json({ ok: true, version: BOT_VERSION }));
app.post('/start-session', async (req, res) => {
    const rid = req.query.room || req.body?.room;
    if (!rid) return res.status(400).json({error:'room requis'});
    log(`▶️ Démarrage demandé pour ${rid}`);
    res.json({ ok: true });
    startBotForRoom(rid).catch(e=>log(`❌ ${e.message}`));
});
app.post('/end', (req, res) => { if (req.query.session || req.body?.session) stopBotForRoom(req.query.session || req.body?.session); res.json({ok:true}); });
app.get('/events', (req, res) => {
    res.writeHead(200, { 'Content-Type':'text/event-stream; charset=utf-8','Cache-Control':'no-cache,no-transform','Connection':'keep-alive','Access-Control-Allow-Origin':'*','X-Accel-Buffering':'no' });
    res.write(': connecte\n\n');
    sseClients.add(res);
    log(`📡 Observateur logs connecté`);
    const ka = setInterval(()=>{try{res.write(': ping\n\n');}catch(e){clearInterval(ka);}},15000);
    req.on('close',()=>{clearInterval(ka);sseClients.delete(res);});
});

async function main() {
    log(`📋 LIVEKIT_URL=${LIVEKIT_URL?'✅':'❌'}, GEMINI_API_KEY=${GEMINI_API_KEY?`✅ (${GEMINI_API_KEY.length} car.)`:'❌'}`);
    if (GEMINI_API_KEY) {
        const m = await import('@google/genai');
        GoogleGenAI = m.GoogleGenAI;
        ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
        log('✅ Client Gemini prêt');
    }
    await ensureLiveKitRtc();
    http.createServer(app).listen(PORT, () => log(`🚀 Serveur port ${PORT}`));
}
main().catch(e => log(`💥 FATAL: ${e?.stack || e}`));
