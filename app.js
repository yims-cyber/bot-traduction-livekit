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
log('=== Démarrage serveur traduction Studio Zaloria v1.0.5 ===');
process.on('uncaughtException', (err) => log(`💥 Erreur non capturée: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 Promesse rejetée: ${reason?.stack || reason}`));
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
  try {
    fetch(`${O2SWITCH_API_URL}/save-transcription.php`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: API_SECRET, session_id: roomId, langue, texte, est_final: true }),
    }).catch(e => log(`❌ Erreur sauvegarde BDD: ${e.message}`));
  } catch(e) { log(`❌ Erreur sauvegarde BDD: ${e.message}`); }
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
  if (pub && pub.kind !== 'audio') return;
  if (pub && pub.trackName && pub.trackName !== 'orator-mic') return;
  if (audioPumpStarted) return;
  audioPumpStarted = true;
  log(`🎙️ Micro de l'orateur détecté (${participant?.identity || 'inconnu'}), lancement traduction 7 langues...`);
  LANGUES.forEach(l => getOrCreateLangSession(orateurId, l, room));
  pumpAudioTrack(orateurId, track);
}

async function createLangSession(orateurId, lang, room) {
  const isCaption = lang === 'fr';
  const translationConfig = isCaption
    ? { targetLanguageCode: 'fr', echoTargetLanguage: true }
    : { targetLanguageCode: lang, echoTargetLanguage: false };
  const langSession = { geminiSession: null, ready: false, pendingAudio: [], lastText: '', closing: false, audioSource: null, audioQueue: Promise.resolve() };
  try {
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config: { responseModalities: ['AUDIO'], inputAudioTranscription: {}, outputAudioTranscription: {}, translationConfig },
      callbacks: {
        onopen: () => {
          log(`🔗 [${orateurId}/${lang}] Session Gemini ouverte`);
          langSession.ready = true;
          langSession.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => {
            try { geminiSession.sendRealtimeInput({ audio: { data: buf.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); } catch(e){}
          });
          langSession.pendingAudio = [];
        },
        onmessage: (message) => {
          const content = message.serverContent;
          if (!content) return;
          if (content.inputTranscription?.text) {
            const text = content.inputTranscription.text.trim();
            if (text && text !== langSession.lastText) {
              langSession.lastText = text;
              if (isCaption) { saveTranscription(orateurId, lang, text); broadcastSubtitle(room, lang, text); }
            }
          }
          if (content.outputTranscription?.text) {
            const text = content.outputTranscription.text.trim();
            if (text && !isCaption && text !== langSession.lastText) {
              langSession.lastText = text;
              saveTranscription(orateurId, lang, text);
              broadcastSubtitle(room, lang, text);
              log(`✅ [${orateurId}/${lang}] ${text}`);
            }
          }
          if (content.turnComplete) langSession.lastText = '';
          if (!isCaption && content.modelTurn?.parts && langSession.audioSource) {
            for (const part of content.modelTurn.parts) {
              if (part.inlineData?.data) {
                const buf = Buffer.from(part.inlineData.data, 'base64');
                const int16 = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength/2);
                const frame = new LiveKitAudioFrame(int16, GEMINI_OUTPUT_SAMPLE_RATE, 1, int16.length);
                langSession.audioQueue = langSession.audioQueue.then(() => langSession.audioSource.captureFrame(frame)).catch(e => log(`❌ Erreur audio ${lang}: ${e.message}`));
              }
            }
          }
        },
        onerror: (err) => { log(`❌ [${orateurId}/${lang}] Erreur Gemini: ${err?.message || err}`); if (!langSession.closing) setTimeout(() => reconnectLangSession(orateurId, lang), 3000); },
        onclose: () => { log(`🔌 [${orateurId}/${lang}] Session fermée`); langSession.ready = false; if (!langSession.closing) setTimeout(() => reconnectLangSession(orateurId, lang), 3000); },
      },
    });
    langSession.geminiSession = geminiSession;
  } catch(e) { log(`❌ Impossible de créer session ${lang}: ${e.message}`); }
  return langSession;
}

async function reconnectLangSession(orateurId, lang) {
  const orator = orators.get(orateurId);
  if (!orator) return;
  const old = orator.langSessions.get(lang);
  if (!old || old.closing) return;
  const nouvelle = await createLangSession(orateurId, lang, orator.bot?.room);
  nouvelle.audioSource = old.audioSource;
  nouvelle.audioQueue = Promise.resolve();
  orator.langSessions.set(lang, nouvelle);
}

function getOrator(id) {
  let o = orators.get(id);
  if (!o) { o = { langSessions: new Map(), lastSeen: Date.now(), bot: null }; orators.set(id, o); }
  o.lastSeen = Date.now();
  return o;
}

async function getOrCreateLangSession(orateurId, lang, room) {
  const orator = getOrator(orateurId);
  let s = orator.langSessions.get(lang);
  if (s) return s;
  s = await createLangSession(orateurId, lang, room);
  orator.langSessions.set(lang, s);
  if (lang !== 'fr') publishLangAudioTrack(orateurId, lang, s);
  return s;
}

function feedAudio(orateurId, buffer, seq) {
  const orator = orators.get(orateurId);
  if (!orator) return;
  orator.lastSeen = Date.now();
  for (const [lang, s] of orator.langSessions.entries()) {
    if (s.ready && s.geminiSession) {
      try { s.geminiSession.sendRealtimeInput({ audio: { data: buffer.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); } catch(e){}
    } else s.pendingAudio.push({ seq, buf: buffer });
  }
}

async function publishLangAudioTrack(orateurId, lang, s) {
  const orator = getOrator(orateurId);
  if (!orator.bot || !orator.bot.room) return;
  const room = orator.bot.room;
  const rtc = await ensureLiveKitRtc();
  const source = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
  const track = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, source);
  await room.localParticipant.publishTrack(track, { name: `lang-${lang}` });
  s.audioSource = source;
  log(`📡 Piste audio traduite [${lang}] publiée`);
}

async function pumpAudioTrack(orateurId, track) {
  const rtc = await ensureLiveKitRtc();
  const stream = new rtc.AudioStream(track, { sampleRate: SAMPLE_RATE, numChannels: 1 });
  let leftover = Buffer.alloc(0);
  let seq = 0;
  try {
    for await (const frame of stream) {
      const frameBuf = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
      let combined = leftover.length ? Buffer.concat([leftover, frameBuf]) : frameBuf;
      let offset = 0;
      while (combined.length - offset >= BYTES_PER_CHUNK) {
        feedAudio(orateurId, Buffer.from(combined.subarray(offset, offset+BYTES_PER_CHUNK)), seq++);
        offset += BYTES_PER_CHUNK;
      }
      leftover = offset < combined.length ? Buffer.from(combined.subarray(offset)) : Buffer.alloc(0);
    }
  } catch(e) { log(`❌ Flux audio interrompu: ${e.message}`); }
}

// Fonction qui scanne TOUTES les pistes de TOUS les participants pour trouver l'orateur
async function scanForOratorMic(orateurId, room) {
  const rtc = await ensureLiveKitRtc();
  const participants = Array.from(room.remoteParticipants.values());
  log(`🔍 Scan des participants : ${participants.length} participant(s) dans la salle`);
  for (const participant of participants) {
    log(`   👤 Participant : ${participant.identity}`);
    // ✅ ICI ON UTILISE `.publications` (nom de propriété CORRECT dans rtc-node)
    const pubs = participant.publications ? Array.from(participant.publications.values()) : [];
    log(`      → ${pubs.length} piste(s) publiée(s)`);
    for (const pub of pubs) {
      log(`      → Piste : "${pub.name}" (${pub.kind === rtc.TrackKind.KIND_AUDIO ? 'AUDIO' : 'autre'})`);
      if (pub.kind === rtc.TrackKind.KIND_AUDIO && pub.name === 'orator-mic') {
        log(`✅ Micro orateur trouvé chez ${participant.identity}, abonnement en cours...`);
        try {
          if (!pub.track) {
            const track = await pub.setSubscribed(true);
            attachOratorTrack(orateurId, track, pub, participant, room);
          } else {
            attachOratorTrack(orateurId, pub.track, pub, participant, room);
          }
        } catch(e) { log(`❌ Erreur abonnement micro: ${e.message}`); }
      }
    }
  }
  if (!audioPumpStarted) log(`⏳ Pas encore de micro orateur détecté, en attente des événements...`);
}

async function startBotForRoom(orateurId) {
  const orator = getOrator(orateurId);
  if (orator.bot) return orator.bot.connecting;
  if (!LIVEKIT_URL) { log('❌ LIVEKIT_URL manquante'); return; }
  if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY MANQUANTE dans Render !'); return; }
  audioPumpStarted = false;
  const connecting = (async () => {
    const rtc = await ensureLiveKitRtc();
    const identity = 'translator-bot-' + Math.random().toString(36).slice(2,8);
    const token = await generateLiveKitToken(orateurId, identity, 'translator');
    const room = new rtc.Room();

    // Cas 1 : nouvelle piste qui arrive APRÈS la connexion du bot
    room.on(rtc.RoomEvent.TrackSubscribed, (track, pub, participant) => {
      attachOratorTrack(orateurId, track, pub, participant, room);
    });
    room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté de ${orateurId}`); if (orator.bot) orator.bot = null; });

    await room.connect(LIVEKIT_URL, token, { autoSubscribe: true });
    log(`✅ Bot connecté à la salle LiveKit "${orateurId}"`);

    // Cas 2 : scanner les pistes déjà présentes, avec un petit délai le temps que LiveKit remplisse les participants/pistes
    setTimeout(() => scanForOratorMic(orateurId, room), 1500);

    return room;
  })();
  orator.bot = { room: null, connecting };
  connecting.then(room => { if (orator.bot) orator.bot.room = room; }).catch(e => { log(`❌ Échec connexion bot: ${e.message}`); orator.bot = null; });
  return connecting;
}

function stopBotForRoom(id) {
  const o = orators.get(id);
  if (!o) return;
  log(`⏹️ Arrêt bot session ${id}`);
  try { o.bot?.room?.disconnect(); } catch(e){}
  for (const s of o.langSessions.values()) { s.closing = true; try { s.geminiSession?.close(); } catch(e){} }
  orators.delete(id);
}

// ROUTES
app.get('/health', (req, res) => res.json({ ok: true, service: 'bot-traduction-studio', version: '1.0.5' }));

app.get('/livekit-token', async (req, res) => {
  const { room, identity, role } = req.query;
  if (!room || !identity) return res.status(400).json({ error: 'room et identity requis' });
  try { res.json({ token: await generateLiveKitToken(room, identity, role || 'listener') }); }
  catch(e) { res.status(500).json({ error: e.message }); }
}); 

app.post('/start-session', async (req, res) => {
  const roomId = req.query.room || req.body?.room;
  if (!roomId) return res.status(400).json({ error: 'room requis' });
  log(`▶️ Démarrage session "${roomId}" demandé`);
  res.json({ ok: true, room: roomId });
  startBotForRoom(roomId).catch(e => log(`❌ Erreur démarrage: ${e.message}`));
});

app.post('/end', (req, res) => {
  const id = req.query.session || req.body?.session;
  if (id) stopBotForRoom(id);
  res.json({ ok: true });
});

app.get('/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no'
  });
  res.write(': connecté\n\n');
  res.write('event: ready\ndata: {"ok":true}\n\n');
  sseClients.add(res);
  log(`📡 Nouvel observateur connecté aux logs`);
  const keepAlive = setInterval(() => { try { res.write(': ping\n\n'); } catch(e){ clearInterval(keepAlive); } }, 15000);
  req.on('close', () => { clearInterval(keepAlive); sseClients.delete(res); });
});

// DEMARRAGE
async function main() {
  if (!GEMINI_API_KEY) log('❌ GEMINI_API_KEY MANQUANTE dans Render !');
  else {
    const mod = await import('@google/genai');
    GoogleGenAI = mod.GoogleGenAI;
    ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
    log('✅ Client Gemini initialisé');
  }
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur démarré sur port ${PORT}`));
}
main().catch(e => log(`💥 Erreur fatale: ${e?.stack || e}`));
