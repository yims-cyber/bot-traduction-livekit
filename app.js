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

const SAMPLE_RATE = 16000;
const CHUNK_MS = 100;
const BYTES_PER_CHUNK = Math.round(SAMPLE_RATE * (CHUNK_MS / 1000)) * 2;
const GEMINI_OUTPUT_SAMPLE_RATE = 24000;
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
log('=== Démarrage serveur traduction v1.0.6 ===');
process.on('uncaughtException', (err) => log(`💥 Erreur: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 Rejet: ${reason?.stack || reason}`));
let ai;

async function ensureLiveKitRtc() {
  if (!LiveKitRtc) {
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
  }).catch(e => log(`❌ Erreur BDD: ${e.message}`));
}

function broadcastSubtitle(room, langue, texte) {
  if (!room || !texte) return;
  try {
    const payload = Buffer.from(JSON.stringify({ type: 'sous_titre', texte, langue }), 'utf-8');
    room.localParticipant.publishData(payload, { reliable: true, topic: `sous-titres-${langue}` });
  } catch(e) {}
}

let audioPumpStarted = false;
function attachOratorTrack(orateurId, track, pub, participant, room) {
  if (!track) return;
  const name = pub?.name || pub?.trackName || '';
  if (pub?.kind && pub.kind !== LiveKitRtc?.TrackKind?.KIND_AUDIO && name !== 'orator-mic') return;
  if (name && name !== 'orator-mic') return;
  if (audioPumpStarted) return;
  audioPumpStarted = true;
  log(`🎙️ Micro orateur DÉTECTÉ ! (${participant?.identity || 'inconnu'}) → lancement traduction 7 langues`);
  LANGUES.forEach(l => getOrCreateLangSession(orateurId, l, room));
  pumpAudioTrack(orateurId, track);
}

async function createLangSession(orateurId, lang, room) {
  const isCaption = lang === 'fr';
  const translationConfig = isCaption
    ? { targetLanguageCode: 'fr', echoTargetLanguage: true }
    : { targetLanguageCode: lang, echoTargetLanguage: false };
  const s = { geminiSession: null, ready: false, pendingAudio: [], lastText: '', closing: false, audioSource: null, audioQueue: Promise.resolve() };
  try {
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config: { responseModalities: ['AUDIO'], inputAudioTranscription: {}, outputAudioTranscription: {}, translationConfig },
      callbacks: {
        onopen: () => {
          log(`🔗 [${lang}] Session Gemini ouverte`);
          s.ready = true;
          s.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => {
            try { geminiSession.sendRealtimeInput({ audio: { data: buf.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); } catch(e){}
          });
          s.pendingAudio = [];
        },
        onmessage: (message) => {
          const c = message.serverContent; if (!c) return;
          if (c.inputTranscription?.text) {
            const t = c.inputTranscription.text.trim();
            if (t && t !== s.lastText) { s.lastText = t; if (isCaption) { saveTranscription(orateurId, lang, t); broadcastSubtitle(room, lang, t); } }
          }
          if (c.outputTranscription?.text) {
            const t = c.outputTranscription.text.trim();
            if (t && !isCaption && t !== s.lastText) { s.lastText = t; saveTranscription(orateurId, lang, t); broadcastSubtitle(room, lang, t); log(`✅ [${lang}] ${t}`); }
          }
          if (c.turnComplete) s.lastText = '';
          if (!isCaption && c.modelTurn?.parts && s.audioSource) {
            for (const p of c.modelTurn.parts) {
              if (p.inlineData?.data) {
                const buf = Buffer.from(p.inlineData.data, 'base64');
                const i16 = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength/2);
                const frame = new LiveKitAudioFrame(i16, GEMINI_OUTPUT_SAMPLE_RATE, 1, i16.length);
                s.audioQueue = s.audioQueue.then(() => s.audioSource.captureFrame(frame)).catch(e => log(`❌ Erreur audio ${lang}: ${e.message}`));
              }
            }
          }
        },
        onerror: (e) => { log(`❌ [${lang}] Erreur Gemini: ${e?.message}`); if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
        onclose: () => { log(`🔌 [${lang}] Session fermée`); s.ready = false; if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
      }
    });
    s.geminiSession = geminiSession;
  } catch(e) { log(`❌ Session ${lang} impossible: ${e.message}`); }
  return s;
}

async function reconnectLangSession(orateurId, lang) {
  const o = orators.get(orateurId); if (!o) return;
  const old = o.langSessions.get(lang); if (!old || old.closing) return;
  const n = await createLangSession(orateurId, lang, o.bot?.room);
  n.audioSource = old.audioSource; n.audioQueue = Promise.resolve();
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
      try { s.geminiSession.sendRealtimeInput({ audio:{data:buffer.toString('base64'),mimeType:'audio/pcm;rate=16000'}}); } catch(e){}
    } else s.pendingAudio.push({seq, buf: buffer});
  }
}

async function publishLangAudioTrack(id, lang, s) {
  const o = getOrator(id); if (!o.bot?.room) return;
  const room = o.bot.room; const rtc = await ensureLiveKitRtc();
  const src = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
  const tr = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, src);
  await room.localParticipant.publishTrack(tr, { name: `lang-${lang}` });
  s.audioSource = src;
  log(`📡 Piste [${lang}] publiée`);
}

async function pumpAudioTrack(id, track) {
  const rtc = await ensureLiveKitRtc();
  const stream = new rtc.AudioStream(track, { sampleRate: SAMPLE_RATE, numChannels: 1 });
  let leftover = Buffer.alloc(0), seq = 0;
  try {
    for await (const frame of stream) {
      const fb = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
      let comb = leftover.length ? Buffer.concat([leftover, fb]) : fb;
      let off = 0;
      while (comb.length - off >= BYTES_PER_CHUNK) {
        feedAudio(id, Buffer.from(comb.subarray(off, off+BYTES_PER_CHUNK)), seq++);
        off += BYTES_PER_CHUNK;
      }
      leftover = off < comb.length ? Buffer.from(comb.subarray(off)) : Buffer.alloc(0);
    }
  } catch(e) { log(`❌ Flux interrompu: ${e.message}`); }
}

async function scanForMic(orateurId, room) {
  const rtc = await ensureLiveKitRtc();
  const parts = Array.from(room.remoteParticipants.values());
  log(`🔍 Scan : ${parts.length} participants dans la salle`);
  for (const p of parts) {
    const pubs = p.publications ? Array.from(p.publications.values()) : [];
    log(`   👤 ${p.identity} : ${pubs.length} pistes`);
    for (const pub of pubs) {
      const kind = pub.kind; const name = pub.name || '';
      const isAudio = kind === rtc.TrackKind.KIND_AUDIO;
      log(`      - ${name} (${isAudio ? 'AUDIO' : 'autre'})`);
      if (isAudio && name === 'orator-mic') {
        log(`✅ Micro orateur trouvé chez ${p.identity}`);
        try {
          const track = pub.track || await pub.setSubscribed(true);
          attachOratorTrack(orateurId, track, pub, p, room);
          return true;
        } catch(e) { log(`❌ Erreur abonnement: ${e.message}`); }
      }
    }
  }
  return false;
}

async function startBotForRoom(orateurId) {
  const orator = getOrator(orateurId);
  if (orator.bot) return orator.bot.connecting;
  if (!LIVEKIT_URL) { log('❌ LIVEKIT_URL manquant'); return; }
  if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY MANQUANTE sur Render'); return; }
  audioPumpStarted = false;
  const connecting = (async () => {
    const rtc = await ensureLiveKitRtc();
    const identity = 'bot-' + Math.random().toString(36).slice(2,8);
    const token = await generateLiveKitToken(orateurId, identity, 'translator');
    const room = new rtc.Room();

    // Événements
    room.on(rtc.RoomEvent.TrackSubscribed, (track, pub, participant) => {
      log(`📥 Piste souscrite : ${pub.name} de ${participant.identity}`);
      attachOratorTrack(orateurId, track, pub, participant, room);
    });
    // ✅ ÉVÉNEMENT CLÉ : quand une piste est PUBLIÉE par quelqu'un (même avant abonnement)
    room.on(rtc.RoomEvent.TrackPublished, (pub, participant) => {
      log(`📢 Nouvelle piste publiée : ${pub.name} par ${participant.identity}`);
      if (pub.kind === rtc.TrackKind.KIND_AUDIO && pub.name === 'orator-mic') {
        pub.setSubscribed(true).then(track => {
          attachOratorTrack(orateurId, track, pub, participant, room);
        }).catch(e => log(`❌ Souscription échouée: ${e.message}`));
      }
    });
    room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté`); if (orator.bot) orator.bot = null; });
    room.on(rtc.RoomEvent.ParticipantConnected, (p) => log(`👋 Participant connecté : ${p.identity}`));

    await room.connect(LIVEKIT_URL, token, { autoSubscribe: false });
    log(`✅ Bot connecté à la salle "${orateurId}"`);

    // Scans répétés pendant 15s pour attraper les pistes qui arrivent
    for (let i = 0; i < 15; i++) {
      await new Promise(r => setTimeout(r, 1000));
      if (audioPumpStarted) break;
      await scanForMic(orateurId, room);
    }
    if (!audioPumpStarted) log(`⏳ En attente du micro de l'orateur...`);

    return room;
  })();
  orator.bot = { room: null, connecting };
  connecting.then(r => { if (orator.bot) orator.bot.room = r; }).catch(e => { log(`❌ Échec connexion bot: ${e.message}`); orator.bot = null; });
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
app.get('/health', (req, res) => res.json({ ok: true, version: '1.0.6' }));
app.get('/livekit-token', async (req, res) => {
  const { room, identity, role } = req.query;
  if (!room || !identity) return res.status(400).json({ error: 'paramètres requis' });
  try { res.json({ token: await generateLiveKitToken(room, identity, role || 'listener') }); }
  catch(e) { res.status(500).json({ error: e.message }); }
});
app.post('/start-session', async (req, res) => {
  const rid = req.query.room || req.body?.room;
  if (!rid) return res.status(400).json({ error: 'room requis' });
  log(`▶️ Démarrage session "${rid}"`);
  res.json({ ok: true });
  startBotForRoom(rid).catch(e => log(`❌ Erreur démarrage: ${e.message}`));
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
  if (!GEMINI_API_KEY) log('⚠️ GEMINI_API_KEY absente (vérifie Render)');
  else {
    const m = await import('@google/genai');
    GoogleGenAI = m.GoogleGenAI;
    ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
    log('✅ Client Gemini prêt');
  }
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur port ${PORT}`));
}
main().catch(e => log(`💥 Fatal: ${e?.stack || e}`));
