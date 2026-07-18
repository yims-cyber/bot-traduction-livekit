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
const LANGUES = ['fr', 'ln', 'sw', 'en', 'pt', 'es', 'de'];
const BOT_VERSION = '1.1.0';

const SAMPLE_RATE = 16000;
const CHUNK_MS = 100;
const BYTES_PER_CHUNK = Math.round(SAMPLE_RATE * (CHUNK_MS / 1000)) * 2;
const GEMINI_OUTPUT_SAMPLE_RATE = 24000;
// ✅ Latence cible : ~400ms max. Si la file a plus de MAX_BUFFER_MS, on saute des frames pour rattraper.
const MAX_BUFFER_FRAMES = Math.round(400 / 10); // ~400ms de buffer max
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
log(`=== Serveur traduction v${BOT_VERSION} - Latence optimisée ===`);
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
    // ✅ Diffuse à TOUS les participants sans restriction (destinationIdentities omis)
    room.localParticipant.publishData(payload, { reliable: true, topic: `sous-titres-${langue}` })
      .then(() => log(`📤 Sous-titre [${langue}] diffusé: "${texte.substring(0,30)}..."`))
      .catch(e => log(`❌ Erreur broadcast sous-titre ${langue}: ${e.message}`));
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
  const isCaption = lang === 'fr';
  const translationConfig = isCaption
    ? { targetLanguageCode: 'fr', echoTargetLanguage: true }
    : { targetLanguageCode: lang, echoTargetLanguage: false };
  const s = {
    geminiSession: null, ready: false, pendingAudio: [], lastText: '',
    closing: false, audioSource: null,
    queueSize: 0, // ✅ Compteur de frames en attente pour contrôler la latence
    droppedFrames: 0,
  };
  s.audioQueue = Promise.resolve();

  try {
    log(`🔄 Connexion Gemini [${lang}]...`);
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config: {
        responseModalities: ['AUDIO'],
        inputAudioTranscription: {},
        outputAudioTranscription: {},
        translationConfig,
      },
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
              if (t && t !== s.lastText) {
                s.lastText = t;
                if (isCaption) {
                  log(`📝 TRANSCRIPTION FR: "${t}"`);
                  saveTranscription(orateurId, lang, t);
                  broadcastSubtitle(room, lang, t);
                }
              }
            }
            // Traduction sortante + sous-titres
            if (c.outputTranscription?.text) {
              const t = c.outputTranscription.text.trim();
              if (t && !isCaption) {
                saveTranscription(orateurId, lang, t);
                broadcastSubtitle(room, lang, t);
                log(`🌐 TRADUCTION [${lang}]: "${t}"`);
              }
            }
            if (c.turnComplete) s.lastText = '';

            // Audio traduit sortant
            if (!isCaption && c.modelTurn?.parts && s.audioSource) {
              const frames = [];
              for (const p of c.modelTurn.parts) {
                if (p.inlineData?.data) {
                  const buf = Buffer.from(p.inlineData.data, 'base64');
                  const i16 = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength/2);
                  const numFrames = i16.length; // 24kHz, échantillons
                  // Découper en frames de ~10ms pour un contrôle précis du buffer
                  const samplesPerFrame = Math.round(GEMINI_OUTPUT_SAMPLE_RATE * 10 / 1000);
                  for (let i = 0; i < numFrames; i += samplesPerFrame) {
                    const end = Math.min(i + samplesPerFrame, numFrames);
                    const frameData = i16.subarray(i, end);
                    const frame = new LiveKitAudioFrame(frameData, GEMINI_OUTPUT_SAMPLE_RATE, 1, frameData.length);
                    frames.push(frame);
                  }
                }
              }
              if (frames.length > 0) {
                // ✅ CONTRÔLE DE LATENCE : si on a déjà trop de frames en file, sauter des frames
                if (s.queueSize > MAX_BUFFER_FRAMES) {
                  const drop = s.queueSize - Math.round(MAX_BUFFER_FRAMES / 2);
                  s.droppedFrames += drop;
                  s.queueSize = Math.round(MAX_BUFFER_FRAMES / 2);
                  if (s.droppedFrames % 50 === 1) log(`⚠️ Rattrapage latence [${lang}]: ${drop} frames sautées (buffer saturé)`);
                }
                for (const frame of frames) {
                  s.queueSize++;
                  s.audioQueue = s.audioQueue.then(() => {
                    s.queueSize = Math.max(0, s.queueSize - 1);
                    return s.audioSource.captureFrame(frame);
                  }).catch(e => log(`❌ Audio ${lang}: ${e.message}`));
                }
                log(`🔊 Audio [${lang}]: +${frames.length} frames (file=${s.queueSize})`);
              }
            }
          } catch(e) {
            log(`❌ Erreur traitement message [${lang}]: ${e.message}`);
          }
        },
        onerror: (e) => { log(`❌ Gemini [${lang}] ERREUR: ${e?.message || e}`); if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
        onclose: (e) => { log(`🔌 Gemini [${lang}] déconnecté (${e?.reason || 'raison inconnue'}), reconnexion...`); s.ready = false; if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
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
  if (lang !== 'fr') publishLangAudioTrack(id, lang, s);
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
  // ✅ Désactiver le DTX / confort pour réduire la latence
  const src = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1, { noiseSuppression: false, echoCancellation: false, autoGainControl: false });
  const tr = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, src);
  await room.localParticipant.publishTrack(tr, { name: `lang-${lang}` });
  s.audioSource = src;
  log(`📡 Piste audio [${lang}] publiée`);
}

async function pumpAudioTrack(id, track) {
  const rtc = await ensureLiveKitRtc();
  log(`🎧 Pompe audio démarrée (16kHz mono PCM16, chunks 100ms, latence cible <500ms)...`);
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
        // Afficher la latence moyenne des files
        const queues = [];
        const o = orators.get(id);
        if (o) for (const [l, s] of o.langSessions) queues.push(`${l}:${s.queueSize}`);
        log(`📊 Débit: ${Math.round(totalBytes/1024)} KB, ${seq} chunks, files=[${queues.join(',')}]`);
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
  if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY MANQUANTE dans Render !'); return; }
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
  for (const s of o.langSessions.values()) { s.closing = true; try { s.geminiSession?.close(); } catch(e){} }
  orators.delete(id);
}

// ROUTES
app.get('/health', (req, res) => res.json({ ok: true, version: BOT_VERSION }));
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
  res.write(': connecté\n\n');
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
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur port ${PORT}`));
}
main().catch(e => log(`💥 FATAL: ${e?.stack || e}`));
