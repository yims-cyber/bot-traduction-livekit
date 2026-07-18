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
const BOT_VERSION = '1.1.1';

// Codes langue officiels supportés par Gemini 3.5 Live Translate
// Lingala (ln) n'a pas de voix de sortie TTS dans le modèle : on fait SOUS-TITRES LINGALA SEULS
const LANG_CONFIG = {
  'fr': { targetCode: 'fr', echo: true,  audioOut: true,  label: 'Français' },
  'ln': { targetCode: 'ln', echo: false, audioOut: false, label: 'Lingala (sous-titres uniquement)' },
  'sw': { targetCode: 'sw', echo: false, audioOut: true,  label: 'Swahili' },
  'en': { targetCode: 'en', echo: false, audioOut: true,  label: 'Anglais' },
  'pt': { targetCode: 'pt-PT', echo: false, audioOut: true,  label: 'Portugais' },
  'es': { targetCode: 'es', echo: false, audioOut: true,  label: 'Espagnol' },
  'de': { targetCode: 'de', echo: false, audioOut: true,  label: 'Allemand' },
};
const LANGUES = Object.keys(LANG_CONFIG);

const SAMPLE_RATE = 16000;
const CHUNK_MS = 100;
const BYTES_PER_CHUNK = Math.round(SAMPLE_RATE * (CHUNK_MS / 1000)) * 2; // 3200 octets
const GEMINI_OUTPUT_SAMPLE_RATE = 24000;
// Latence cible : ~500ms max. Si la file audio dépasse, on saute les morceaux pour rattraper.
const MAX_BUFFER_MS = 500;
const MAX_BUFFER_FRAMES = Math.round(MAX_BUFFER_MS / 10); // 10ms par frame envoyée à AudioSource
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
log(`=== Serveur traduction v${BOT_VERSION} - version TESTÉE ===`);
process.on('uncaughtException', (err) => log(`💥 ERREUR: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 REJET: ${reason?.stack || reason}`));
let ai;

async function ensureLiveKitRtc() {
  if (!LiveKitRtc) {
    log('Chargement module @livekit/rtc-node...');
    LiveKitRtc = await import('@livekit/rtc-node');
    LiveKitAudioFrame = LiveKitRtc.AudioFrame;
    log('✅ Module @livekit/rtc-node chargé');
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
    room.localParticipant.publishData(payload, { reliable: true, topic: `sous-titres-${langue}` });
  } catch(e) { log(`❌ Erreur broadcast sous-titre ${langue}: ${e.message}`); }
}

let audioPumpStarted = false;
function attachOratorTrack(orateurId, track, pub, participant, room) {
  if (!track) return;
  const name = pub?.name || '';
  if (name !== 'orator-mic') return;
  if (audioPumpStarted) return;
  audioPumpStarted = true;
  log(`🎙️ MICRO ORATEUR CONNECTÉ ! Démarrage traduction 7 langues`);
  LANGUES.forEach(l => getOrCreateLangSession(orateurId, l, room));
  pumpAudioTrack(orateurId, track);
}

async function createLangSession(orateurId, lang, room) {
  const cfg = LANG_CONFIG[lang];
  const isCaption = lang === 'fr';
  const s = {
    geminiSession: null, ready: false, pendingAudio: [], lastInput: '', lastOutput: '',
    closing: false, audioSource: null, queueSize: 0,
    cfg, lang,
  };
  s.audioQueue = Promise.resolve();

  const config = {
    responseModalities: ['AUDIO'],
    inputAudioTranscription: {},
    outputAudioTranscription: {},
    translationConfig: {
      targetLanguageCode: cfg.targetCode,
      echoTargetLanguage: cfg.echo,
    },
  };

  try {
    log(`🔄 Connexion Gemini [${lang}] → ${cfg.label} (audio=${cfg.audioOut ? 'oui':'non'})...`);
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config,
      callbacks: {
        onopen: () => {
          log(`✅ Gemini [${lang}] CONNECTÉ ! Envoi ${s.pendingAudio.length} chunks en attente...`);
          s.ready = true;
          s.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => {
            try {
              geminiSession.sendRealtimeInput({ media: { data: buf.toString('base64'), mimeType: 'audio/pcm;rate=16000' } });
            } catch(e) { log(`❌ Erreur envoi chunk attente [${lang}]: ${e.message}`); }
          });
          s.pendingAudio = [];
        },
        onmessage: (message) => {
          try {
            const c = message.serverContent;
            if (!c) {
              if (message.setupComplete) log(`🔧 Gemini [${lang}] setup terminé`);
              return;
            }
            // Transcription entrante (ce que l'orateur dit)
            if (c.inputTranscription?.text) {
              const t = c.inputTranscription.text.trim();
              if (t && t !== s.lastInput) {
                s.lastInput = t;
                if (isCaption) {
                  log(`📝 TRANSCRIPTION FR: "${t}"`);
                  saveTranscription(orateurId, lang, t);
                  broadcastSubtitle(room, lang, t);
                }
              }
            }
            // Traduction sortante (sous-titre de la langue cible)
            if (c.outputTranscription?.text) {
              const t = c.outputTranscription.text.trim();
              if (t) {
                s.lastOutput = t;
                saveTranscription(orateurId, lang, t);
                broadcastSubtitle(room, lang, t);
                if (!cfg.audioOut) {
                  // Pas d'audio pour cette langue (ex: Lingala), on logue
                  log(`📝 SOUS-TITRE [${lang}]: "${t}"`);
                } else {
                  log(`🌐 TRADUCTION [${lang}]: "${t}"`);
                }
              }
            }
            if (c.turnComplete) { s.lastInput = ''; s.lastOutput = ''; }

            // Audio traduit (seulement si la langue a une voix)
            if (cfg.audioOut && c.modelTurn?.parts && s.audioSource) {
              const frames = [];
              for (const p of c.modelTurn.parts) {
                if (p.inlineData?.data) {
                  const buf = Buffer.from(p.inlineData.data, 'base64');
                  const i16 = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength/2);
                  const samplesPerFrame = Math.round(GEMINI_OUTPUT_SAMPLE_RATE * 10 / 1000); // 10ms
                  for (let i = 0; i < i16.length; i += samplesPerFrame) {
                    const end = Math.min(i + samplesPerFrame, i16.length);
                    const f = new Int16Array(i16.buffer, i16.byteOffset + i*2, end - i);
                    const frame = new LiveKitAudioFrame(f, GEMINI_OUTPUT_SAMPLE_RATE, 1, f.length);
                    frames.push(frame);
                  }
                }
              }
              if (frames.length > 0) {
                // ✅ CONTRÔLE DE LATENCE : si la file est pleine, sauter des frames pour rester en direct
                if (s.queueSize > MAX_BUFFER_FRAMES) {
                  const drop = s.queueSize - Math.round(MAX_BUFFER_FRAMES / 2);
                  s.queueSize = Math.round(MAX_BUFFER_FRAMES / 2);
                  // Recalculer frames : ne garder que les N dernières frames
                  frames.splice(0, Math.min(drop, frames.length));
                }
                for (const frame of frames) {
                  s.queueSize++;
                  s.audioQueue = s.audioQueue.then(() => {
                    s.queueSize = Math.max(0, s.queueSize - 1);
                    return s.audioSource.captureFrame(frame);
                  }).catch(e => log(`❌ Audio ${lang}: ${e.message}`));
                }
              }
            }
          } catch(e) {
            log(`❌ Erreur traitement message [${lang}]: ${e.message}`);
          }
        },
        onerror: (e) => { log(`❌ Gemini [${lang}] ERREUR: ${e?.message || e}`); if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
        onclose: (e) => { log(`🔌 Gemini [${lang}] déconnecté (${e?.reason || 'inconnu'}), reconnexion...`); s.ready = false; if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
      }
    });
    s.geminiSession = geminiSession;
  } catch(e) { log(`❌ Impossible créer session ${lang}: ${e.message}`); }
  return s;
}

async function reconnectLangSession(orateurId, lang) {
  const o = orators.get(orateurId); if (!o) return;
  const old = o.langSessions.get(lang); if (!old || old.closing) return;
  const n = await createLangSession(orateurId, lang, o.bot?.room);
  n.audioSource = old.audioSource;
  n.audioQueue = Promise.resolve();
  n.queueSize = 0;
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
  // Publier la piste audio seulement si la langue supporte la sortie audio
  if (s.cfg.audioOut) publishLangAudioTrack(id, lang, s);
  else log(`ℹ️ [${lang}] Pas de piste audio (sous-titres uniquement)`);
  return s;
}

function feedAudio(id, buffer, seq) {
  const o = orators.get(id); if (!o) return;
  for (const [lang, s] of o.langSessions.entries()) {
    if (s.ready && s.geminiSession) {
      try {
        s.geminiSession.sendRealtimeInput({ media: { data: buffer.toString('base64'), mimeType: 'audio/pcm;rate=16000' } });
      } catch(e) {
        log(`❌ Envoi chunk [${lang}]: ${e.message}`);
        s.pendingAudio.push({seq, buf: Buffer.from(buffer)});
      }
    } else s.pendingAudio.push({seq, buf: Buffer.from(buffer)});
  }
}

async function publishLangAudioTrack(id, lang, s) {
  const o = getOrator(id); if (!o.bot?.room) return;
  const room = o.bot.room; const rtc = await ensureLiveKitRtc();
  // ✅ CONSTRUCTEUR TESTÉ : new AudioSource(sampleRate, numChannels) — PAS d'options objet !
  const src = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
  const tr = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, src);
  await room.localParticipant.publishTrack(tr, { name: `lang-${lang}` });
  s.audioSource = src;
  log(`📡 Piste audio [${lang}] publiée`);
}

async function pumpAudioTrack(id, track) {
  const rtc = await ensureLiveKitRtc();
  log(`🎧 Pompe audio démarrée (16kHz mono, 100ms chunks)...`);
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
        if (o) for (const [l, s] of o.langSessions) queues.push(`${l}:${s.queueSize}`);
        log(`📊 ${Math.round(totalBytes/1024)} KB envoyés, ${seq} chunks, files=[${queues.join(',')}]`);
        totalBytes = 0; lastLog = Date.now();
      }
    }
  } catch(e) { log(`❌ Flux audio interrompu: ${e.message}`); }
}

async function scanForMic(orateurId, room) {
  const rtc = await ensureLiveKitRtc();
  const parts = Array.from(room.remoteParticipants.values());
  log(`🔍 Scan: ${parts.length} participants`);
  for (const p of parts) {
    const pubs = p.publications ? Array.from(p.publications.values()) : [];
    log(`   👤 Participant : ${p.identity}`);
    log(`      → ${pubs.length} piste(s) publiée(s)`);
    for (const pub of pubs) {
      const isAudio = pub.kind === rtc.TrackKind.KIND_AUDIO;
      log(`      → Piste : "${pub.name}" (${isAudio ? 'AUDIO' : 'autre'})`);
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
  log(`🚀 Démarrage bot pour salle ${orateurId}`);
  const connecting = (async () => {
    const rtc = await ensureLiveKitRtc();
    const identity = 'bot-' + Math.random().toString(36).slice(2,8);
    const token = await generateLiveKitToken(orateurId, identity, 'translator');
    const room = new rtc.Room();

    room.on(rtc.RoomEvent.TrackSubscribed, (track, pub, participant) => {
      log(`📥 Piste souscrite: ${pub.name}`);
      attachOratorTrack(orateurId, track, pub, participant, room);
    });
    room.on(rtc.RoomEvent.TrackPublished, (pub, participant) => {
      log(`📢 Nouvelle piste: ${pub.name} de ${participant.identity}`);
      if (pub.name === 'orator-mic' && pub.kind === rtc.TrackKind.KIND_AUDIO) {
        pub.setSubscribed(true).then(track => attachOratorTrack(orateurId, track, pub, participant, room)).catch(e => log(`❌ ${e.message}`));
      }
    });
    room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté`); if (orator.bot) orator.bot = null; });
    room.on(rtc.RoomEvent.ParticipantConnected, (p) => log(`👋 Participant: ${p.identity}`));

    await room.connect(LIVEKIT_URL, token, { autoSubscribe: false });
    log(`✅ Bot connecté à la salle`);

    for (let i = 0; i < 20; i++) {
      await new Promise(r => setTimeout(r, 500));
      if (audioPumpStarted) break;
      await scanForMic(orateurId, room);
    }
    if (!audioPumpStarted) log(`⏳ En attente du micro...`);
    return room;
  })();
  orator.bot = { room: null, connecting };
  connecting.then(r => { if (orator.bot) orator.bot.room = r; }).catch(e => { log(`❌ Échec connexion: ${e.message}`); orator.bot = null; });
  return connecting;
}

function stopBotForRoom(id) {
  const o = orators.get(id); if (!o) return;
  log(`⏹️ Arrêt bot`);
  try { o.bot?.room?.disconnect(); } catch(e){}
  for (const s of o.langSessions.values()) { s.closing = true; try { s.geminiSession?.close(); } catch(e){} try{ s.audioSource?.close(); }catch(e){} }
  orators.delete(id);
}

// ROUTES
app.get('/health', (req, res) => res.json({ ok: true, version: BOT_VERSION, langues: LANG_CONFIG }));
app.get('/livekit-token', async (req, res) => {
  const { room, identity, role } = req.query;
  if (!room || !identity) return res.status(400).json({ error: 'room/identity requis' });
  try { res.json({ token: await generateLiveKitToken(room, identity, role || 'listener') }); }
  catch(e) { res.status(500).json({ error: e.message }); }
});
app.post('/start-session', async (req, res) => {
  const rid = req.query.room || req.body?.room;
  if (!rid) return res.status(400).json({ error: 'room requis' });
  log(`▶️ Démarrage demandé pour ${rid}`);
  res.json({ ok: true });
  startBotForRoom(rid).catch(e => log(`❌ ${e.message}`));
});
app.post('/end', (req, res) => { if (req.query.session || req.body?.session) stopBotForRoom(req.query.session || req.body?.session); res.json({ ok: true }); });
app.get('/events', (req, res) => {
  res.writeHead(200, { 'Content-Type':'text/event-stream; charset=utf-8','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','Access-Control-Allow-Origin':'*','X-Accel-Buffering':'no' });
  res.write(': connecte\n\n');
  sseClients.add(res);
  log(`📡 Nouvel observateur logs`);
  const ka = setInterval(()=>{ try{res.write(': ping\n\n');}catch(e){clearInterval(ka);} },15000);
  req.on('close',()=>{ clearInterval(ka); sseClients.delete(res); });
});

async function main() {
  log(`📋 Config: LIVEKIT_URL=${LIVEKIT_URL?'✅':'❌'}, GEMINI_API_KEY=${GEMINI_API_KEY?`✅ (${GEMINI_API_KEY.length} car.)`:'❌'}`);
  if (!GEMINI_API_KEY) log('⚠️ GEMINI_API_KEY manquante !');
  else {
    const m = await import('@google/genai');
    GoogleGenAI = m.GoogleGenAI;
    ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
    log('✅ Client Gemini prêt');
  }
  await ensureLiveKitRtc();
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur port ${PORT}`));
}
main().catch(e => log(`💥 FATAL: ${e?.stack || e}`));
