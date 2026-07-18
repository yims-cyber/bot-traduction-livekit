require('dotenv').config();
const http = require('http');
const express = require('express');
const cors = require('cors');
const { AccessToken } = require('livekit-server-sdk');
let GoogleGenAI, LiveKitRtc, LiveKitAudioFrame;

const app = express();
app.use(cors());
app.use(express.json());

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
  console.log(`[${new Date().toISOString()}] ${msg}`);
}
log('=== Démarrage serveur traduction ===');
process.on('uncaughtException', (err) => log(`💥 Erreur: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 Promesse rejetée: ${reason?.stack || reason}`));
let ai;

async function ensureLiveKitRtc() {
  if (!LiveKitRtc) {
    LiveKitRtc = await import('@livekit/rtc-node');
    LiveKitAudioFrame = LiveKitRtc.AudioFrame;
  }
  return LiveKitRtc;
}

function generateLiveKitToken(room, identity, role) {
  const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, { identity, ttl: '6h' });
  at.addGrant({ room, roomJoin: true, canPublish: role === 'orator' || role === 'translator', canSubscribe: true, canPublishData: true });
  return at.toJwt();
}

async function saveTranscription(roomId, langue, texte) {
  if (!O2SWITCH_API_URL || !texte || texte.length < 2) return;
  try {
    await fetch(`${O2SWITCH_API_URL}/save-transcription.php`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: API_SECRET, session_id: roomId, langue, texte, est_final: true })
    });
  } catch(e) { log(`❌ Erreur sauvegarde BDD: ${e.message}`); }
}

async function createLangSession(orateurId, lang) {
  const isCaption = lang === 'fr';
  const translationConfig = isCaption ? { targetLanguageCode: 'fr', echoTargetLanguage: true } : { targetLanguageCode: lang, echoTargetLanguage: false };
  const langSession = { geminiSession: null, ready: false, pendingAudio: [], lastText: '', closing: false, audioSource: null, audioQueue: Promise.resolve() };
  try {
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config: { responseModalities: ['AUDIO'], inputAudioTranscription: {}, outputAudioTranscription: {}, translationConfig },
      callbacks: {
        onopen: () => {
          log(`🔗 [${orateurId}/${lang}] Session Gemini ouverte`);
          langSession.ready = true;
          langSession.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => { try { geminiSession.sendRealtimeInput({audio:{data:buf.toString('base64'),mimeType:'audio/pcm;rate=16000'}}); } catch(e){} });
          langSession.pendingAudio = [];
        },
        onmessage: (message) => {
          const content = message.serverContent;
          if (!content) return;
          if (content.inputTranscription?.text) {
            const text = content.inputTranscription.text.trim();
            if (text && text !== langSession.lastText) { langSession.lastText = text; if (isCaption) saveTranscription(orateurId, lang, text); }
          }
          if (content.outputTranscription?.text) {
            const text = content.outputTranscription.text.trim();
            if (text && !isCaption && text !== langSession.lastText) { langSession.lastText = text; saveTranscription(orateurId, lang, text); log(`✅ [${orateurId}/${lang}] Traduit: ${text}`); }
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
        onclose: () => { log(`🔌 [${orateurId}/${lang}] Session fermée`); langSession.ready = false; if (!langSession.closing) setTimeout(() => reconnectLangSession(orateurId, lang), 3000); }
      }
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
  const nouvelle = await createLangSession(orateurId, lang);
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

async function getOrCreateLangSession(orateurId, lang) {
  const orator = getOrator(orateurId);
  let s = orator.langSessions.get(lang);
  if (s) return s;
  s = await createLangSession(orateurId, lang);
  orator.langSessions.set(lang, s);
  if (lang !== 'fr') publishLangAudioTrack(orateurId, lang, s);
  return s;
}

function feedAudio(orateurId, buffer, seq) {
  const orator = orators.get(orateurId);
  if (!orator) return;
  orator.lastSeen = Date.now();
  for (const [lang, s] of orator.langSessions.entries()) {
    if (s.ready && s.geminiSession) { try { s.geminiSession.sendRealtimeInput({audio:{data:buffer.toString('base64'),mimeType:'audio/pcm;rate=16000'}}); } catch(e){} }
    else s.pendingAudio.push({seq, buf: buffer});
  }
}

async function publishLangAudioTrack(orateurId, lang, s) {
  const orator = getOrator(orateurId);
  if (!orator.bot) await startBotForRoom(orateurId);
  const room = orator.bot?.room;
  if (!room) return;
  const rtc = await ensureLiveKitRtc();
  const source = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
  const track = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, source);
  await room.localParticipant.publishTrack(track, { name: `lang-${lang}` });
  s.audioSource = source;
  log(`📡 Piste ${lang} publiée`);
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

async function startBotForRoom(orateurId) {
  const orator = getOrator(orateurId);
  if (orator.bot) return orator.bot.connecting;
  if (!LIVEKIT_URL) return;
  const connecting = (async () => {
    const rtc = await ensureLiveKitRtc();
    const identity = 'translator-bot-' + orateurId;
    const token = generateLiveKitToken(orateurId, identity, 'translator');
    const room = new rtc.Room();
    room.on(rtc.RoomEvent.TrackSubscribed, (track, _, participant) => {
      if (track.kind === rtc.TrackKind.KIND_AUDIO) { log(`🎙️ Abonné au micro de ${participant.identity}`); LANGUES.forEach(l => getOrCreateLangSession(orateurId, l)); pumpAudioTrack(orateurId, track); }
    });
    room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté de ${orateurId}`); if (orator.bot) orator.bot = null; });
    await room.connect(LIVEKIT_URL, token, { autoSubscribe: true });
    log(`✅ Bot connecté à la salle ${orateurId}`);
    return room;
  })();
  orator.bot = { room: null, connecting };
  connecting.then(room => { if (orator.bot) orator.bot.room = room; }).catch(e => { log(`❌ Connexion bot échouée: ${e.message}`); orator.bot = null; });
  return connecting;
}

function stopBotForRoom(id) {
  const o = orators.get(id);
  if (!o) return;
  try { o.bot?.room?.disconnect(); } catch(e){}
  for (const s of o.langSessions.values()) { s.closing = true; try { s.geminiSession?.close(); } catch(e){} }
  orators.delete(id);
}

app.get('/livekit-token', (req, res) => {
  const { room, identity, role } = req.query;
  if (!room || !identity) return res.status(400).json({error:'Paramètres manquants'});
  try { res.json({ token: generateLiveKitToken(room, identity, role || 'listener') }); } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/start-session', async (req, res) => {
  const roomId = req.query.room;
  log(`▶️ Démarrage session ${roomId}`);
  getOrator(roomId);
  await startBotForRoom(roomId);
  res.json({ ok: true });
});

app.post('/end', (req, res) => { stopBotForRoom(req.query.session); res.json({ ok: true }); });

app.get('/events', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control':'no-cache', 'Connection':'keep-alive' });
  res.write(': connecté\n\n');
  const keepAlive = setInterval(()=>{ try{res.write(': ping\n\n');}catch(e){clearInterval(keepAlive);} }, 20000);
  req.on('close', () => clearInterval(keepAlive));
});

async function main() {
  if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY manquante'); process.exit(1); }
  const mod = await import('@google/genai');
  GoogleGenAI = mod.GoogleGenAI;
  ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
  log('✅ Client Gemini initialisé');
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur démarré sur port ${PORT}`));
}
main().catch(e => log(`💥 Erreur fatale: ${e?.stack || e}`));
