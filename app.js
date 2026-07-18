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
log('=== 🚀 Serveur traduction v1.0.7 DEBUG ===');
process.on('uncaughtException', (err) => log(`💥 ERREUR CRASH: ${err?.stack || err}`));
process.on('unhandledRejection', (reason) => log(`💥 PROMESSE REJETÉE: ${reason?.stack || reason}`));
let ai;

async function ensureLiveKitRtc() {
  if (!LiveKitRtc) {
    log('📦 Chargement module @livekit/rtc-node...');
    LiveKitRtc = await import('@livekit/rtc-node');
    LiveKitAudioFrame = LiveKitRtc.AudioFrame;
    log(`✅ Module rtc-node chargé, TrackKind.AUDIO = ${LiveKitRtc.TrackKind.KIND_AUDIO}`);
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
  }).catch(e => log(`❌ Sauvegarde BDD échouée: ${e.message}`));
}

function broadcastSubtitle(room, langue, texte) {
  if (!room || !texte) return;
  try {
    const payload = Buffer.from(JSON.stringify({ type: 'sous_titre', texte, langue }), 'utf-8');
    room.localParticipant.publishData(payload, { reliable: true, topic: `sous-titres-${langue}` });
    log(`📤 Sous-titre [${langue}] diffusé aux auditeurs: "${texte.substring(0,50)}..."`);
  } catch(e) { log(`❌ Erreur diffusion sous-titre: ${e.message}`); }
}

let audioPumpStarted = false;
function attachOratorTrack(orateurId, track, pub, participant, room) {
  log(`🔍 attachOratorTrack appelée pour participant ${participant?.identity}, nom piste: ${pub?.name}, kind: ${pub?.kind}`);
  if (!track) { log('⚠️ track est null/vide, abandon'); return; }
  const name = pub?.name || '';
  if (name !== 'orator-mic') { log(`⏭️ Ce n'est pas le micro orateur (nom="${name}"), on ignore`); return; }
  if (audioPumpStarted) { log('ℹ️ Pompe audio déjà démarrée, doublon ignoré'); return; }
  audioPumpStarted = true;
  log(`🎙️ ==============================================`);
  log(`🎙️ MICRO ORATEUR CONNECTÉ AVEC SUCCÈS !`);
  log(`🎙️ ==============================================`);
  log(`🎙️ Démarrage des 7 sessions de traduction Gemini...`);
  LANGUES.forEach(l => getOrCreateLangSession(orateurId, l, room));
  log(`🎙️ Démarrage pompage audio vers Gemini...`);
  pumpAudioTrack(orateurId, track);
}

async function createLangSession(orateurId, lang, room) {
  const isCaption = lang === 'fr';
  const translationConfig = isCaption
    ? { targetLanguageCode: 'fr', echoTargetLanguage: true }
    : { targetLanguageCode: lang, echoTargetLanguage: false };
  const s = { geminiSession: null, ready: false, pendingAudio: [], lastText: '', closing: false, audioSource: null, audioQueue: Promise.resolve(), bytesReceived: 0 };
  try {
    log(`🔄 Connexion session Gemini pour [${lang}]...`);
    const geminiSession = await ai.live.connect({
      model: TRANSLATE_MODEL,
      config: { responseModalities: ['AUDIO'], inputAudioTranscription: {}, outputAudioTranscription: {}, translationConfig },
      callbacks: {
        onopen: () => {
          log(`✅ Gemini [${lang}] CONNECTÉ ! Envoi de ${s.pendingAudio.length} chunks en attente...`);
          s.ready = true;
          s.pendingAudio.sort((a,b)=>a.seq-b.seq).forEach(({buf}) => {
            s.bytesReceived += buf.length;
            try { geminiSession.sendRealtimeInput({ audio: { data: buf.toString('base64'), mimeType: 'audio/pcm;rate=16000' } }); } catch(e){}
          });
          s.pendingAudio = [];
          log(`✅ Gemini [${lang}] PRÊTE à recevoir l'audio`);
        },
        onmessage: (message) => {
          const c = message.serverContent; if (!c) return;
          if (c.inputTranscription?.text) {
            const t = c.inputTranscription.text.trim();
            if (t && t !== s.lastText) {
              s.lastText = t;
              log(`📝 TRANSCRIPTION FR reçue de Gemini: "${t}"`);
              if (isCaption) { saveTranscription(orateurId, lang, t); broadcastSubtitle(room, lang, t); }
            }
          }
          if (c.outputTranscription?.text) {
            const t = c.outputTranscription.text.trim();
            if (t && !isCaption && t !== s.lastText) {
              s.lastText = t;
              saveTranscription(orateurId, lang, t);
              broadcastSubtitle(room, lang, t);
              log(`🌐 TRADUCTION [${lang}]: "${t}"`);
            }
          }
          if (c.turnComplete) { s.lastText = ''; log(`🔄 Fin de tour de parole pour [${lang}]`); }
          if (!isCaption && c.modelTurn?.parts && s.audioSource) {
            let bytes = 0;
            for (const p of c.modelTurn.parts) if (p.inlineData?.data) bytes += p.inlineData.data.length;
            if (bytes > 0) log(`🔊 Audio traduit [${lang}] reçu de Gemini : ${Math.round(bytes/1024)} KB`);
            for (const p of c.modelTurn.parts) {
              if (p.inlineData?.data) {
                const buf = Buffer.from(p.inlineData.data, 'base64');
                const i16 = new Int16Array(buf.buffer, buf.byteOffset, buf.byteLength/2);
                const frame = new LiveKitAudioFrame(i16, GEMINI_OUTPUT_SAMPLE_RATE, 1, i16.length);
                s.audioQueue = s.audioQueue.then(() => s.audioSource.captureFrame(frame)).catch(e => log(`❌ Erreur envoi audio ${lang}: ${e.message}`));
              }
            }
          }
        },
        onerror: (e) => { log(`❌ ERREUR Gemini [${lang}]: ${e?.message || e}`); if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
        onclose: () => { log(`🔌 Session Gemini [${lang}] fermée, reconnexion dans 3s...`); s.ready = false; if (!s.closing) setTimeout(()=>reconnectLangSession(orateurId,lang),3000); },
      }
    });
    s.geminiSession = geminiSession;
  } catch(e) { log(`❌ IMPOSSIBLE de créer session ${lang}: ${e.message}`); }
  return s;
}

async function reconnectLangSession(orateurId, lang) {
  const o = orators.get(orateurId); if (!o) return;
  const old = o.langSessions.get(lang); if (!old || old.closing) return;
  log(`🔄 Reconnexion session Gemini [${lang}]...`);
  const n = await createLangSession(orateurId, lang, o.bot?.room);
  n.audioSource = old.audioSource; n.audioQueue = Promise.resolve(); n.bytesReceived = old.bytesReceived;
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
      s.bytesReceived += buffer.length;
      try { s.geminiSession.sendRealtimeInput({ audio:{data:buffer.toString('base64'),mimeType:'audio/pcm;rate=16000'}}); }
      catch(e) { log(`❌ Erreur envoi chunk audio à ${lang}: ${e.message}`); s.pendingAudio.push({seq, buf: Buffer.from(buffer)}); }
    } else s.pendingAudio.push({seq, buf: Buffer.from(buffer)});
  }
}

async function publishLangAudioTrack(id, lang, s) {
  const o = getOrator(id); if (!o.bot?.room) { log(`⚠️ Pas de room pour publier piste ${lang}`); return; }
  const room = o.bot.room; const rtc = await ensureLiveKitRtc();
  log(`📡 Création piste audio traduite [${lang}]...`);
  const src = new rtc.AudioSource(GEMINI_OUTPUT_SAMPLE_RATE, 1);
  const tr = rtc.LocalAudioTrack.createAudioTrack(`lang-${lang}`, src);
  await room.localParticipant.publishTrack(tr, { name: `lang-${lang}` });
  s.audioSource = src;
  log(`✅ Piste audio [${lang}] PUBLIÉE dans LiveKit, les auditeurs peuvent s'abonner`);
}

async function pumpAudioTrack(id, track) {
  const rtc = await ensureLiveKitRtc();
  log(`🎧 Création AudioStream sur la piste du micro orateur (${SAMPLE_RATE}Hz mono)...`);
  const stream = new rtc.AudioStream(track, { sampleRate: SAMPLE_RATE, numChannels: 1 });
  let leftover = Buffer.alloc(0), seq = 0, totalBytes = 0, lastLog = Date.now();
  try {
    log(`🎧 Pompe audio DÉMARRÉE, envoi à Gemini...`);
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
        log(`📊 Débit audio : ${Math.round(totalBytes/1024)} KB envoyés à Gemini en ${Math.round((Date.now()-lastLog)/1000)}s, ${seq} chunks traités`);
        totalBytes = 0; lastLog = Date.now();
      }
    }
    log(`🛑 Flux audio du micro terminé`);
  } catch(e) { log(`❌ ERREUR pompe audio: ${e.message}\n${e.stack}`); }
}

async function scanForMic(orateurId, room) {
  const rtc = await ensureLiveKitRtc();
  const parts = Array.from(room.remoteParticipants.values());
  log(`🔍 Scan: ${parts.length} participants dans la salle`);
  for (const p of parts) {
    const pubs = p.publications ? Array.from(p.publications.values()) : [];
    log(`   👤 ${p.identity} : ${pubs.length} pistes`);
    for (const pub of pubs) {
      const kindStr = pub.kind === rtc.TrackKind.KIND_AUDIO ? 'AUDIO' : pub.kind === rtc.TrackKind.KIND_VIDEO ? 'VIDEO' : `INCONNU(${pub.kind})`;
      log(`      → Piste "${pub.name}" | kind=${kindStr} | subscribed=${!!pub.track}`);
      if (pub.name === 'orator-mic' && pub.kind === rtc.TrackKind.KIND_AUDIO) {
        log(`✅ MICRO TROUVÉ lors du scan !`);
        try {
          let track = pub.track;
          if (!track) {
            log(`🔈 Abonnement à la piste...`);
            track = await pub.setSubscribed(true);
          }
          attachOratorTrack(orateurId, track, pub, p, room);
          return true;
        } catch(e) { log(`❌ Échec abonnement: ${e.message}\n${e.stack}`); }
      }
    }
  }
  return audioPumpStarted;
}

async function startBotForRoom(orateurId) {
  const orator = getOrator(orateurId);
  if (orator.bot) { log(`⚠️ Bot déjà en cours de connexion pour ${orateurId}, on ne recrée pas`); return orator.bot.connecting; }
  if (!LIVEKIT_URL) { log('❌ LIVEKIT_URL manquant dans les variables d\'environnement'); return; }
  if (!GEMINI_API_KEY) { log('❌ GEMINI_API_KEY MANQUANTE dans Render !'); return; }
  audioPumpStarted = false;
  log(`🚀 === Démarrage du bot pour la session ${orateurId} ===`);
  const connecting = (async () => {
    const rtc = await ensureLiveKitRtc();
    const identity = 'bot-' + Math.random().toString(36).slice(2,8);
    log(`🔑 Génération token LiveKit pour identité ${identity}...`);
    const token = await generateLiveKitToken(orateurId, identity, 'translator');
    log(`🔑 Token généré (${token.length} caractères)`);
    const room = new rtc.Room();

    room.on(rtc.RoomEvent.TrackSubscribed, (track, pub, participant) => {
      log(`📥 Événement TrackSubscribed: piste "${pub.name}" de ${participant.identity}`);
      attachOratorTrack(orateurId, track, pub, participant, room);
    });
    room.on(rtc.RoomEvent.TrackPublished, (pub, participant) => {
      log(`📢 Événement TrackPublished: "${pub.name}" par ${participant.identity}, kind=${pub.kind}`);
      if (pub.name === 'orator-mic') {
        log(`🎯 C'est le micro orateur ! Abonnement...`);
        pub.setSubscribed(true).then(track => {
          attachOratorTrack(orateurId, track, pub, participant, room);
        }).catch(e => log(`❌ Échec souscription: ${e.message}`));
      }
    });
    room.on(rtc.RoomEvent.Disconnected, () => { log(`🔌 Bot déconnecté de la salle`); if (orator.bot) orator.bot = null; });
    room.on(rtc.RoomEvent.ParticipantConnected, (p) => log(`👋 Participant connecté: ${p.identity}`));
    room.on(rtc.RoomEvent.ParticipantDisconnected, (p) => log(`👋 Participant déconnecté: ${p.identity}`));

    log(`🔌 Connexion à LiveKit ${LIVEKIT_URL}...`);
    await room.connect(LIVEKIT_URL, token, { autoSubscribe: false });
    log(`✅ BOT CONNECTÉ À LA SALLE "${orateurId}"`);

    for (let i = 0; i < 20; i++) {
      await new Promise(r => setTimeout(r, 500));
      if (audioPumpStarted) break;
      await scanForMic(orateurId, room);
    }
    if (!audioPumpStarted) log(`⏳ En attente de l'arrivée du micro de l'orateur...`);
    return room;
  })();
  orator.bot = { room: null, connecting };
  connecting.then(r => { if (orator.bot) orator.bot.room = r; log(`✅ Instance bot prête`); }).catch(e => { log(`❌ ÉCHEC CONNEXION BOT: ${e.message}\n${e.stack}`); orator.bot = null; });
  return connecting;
}

function stopBotForRoom(id) {
  const o = orators.get(id); if (!o) return;
  log(`⏹️ Arrêt du bot pour la session ${id}`);
  try { o.bot?.room?.disconnect(); } catch(e){}
  for (const s of o.langSessions.values()) { s.closing = true; try { s.geminiSession?.close(); } catch(e){} }
  orators.delete(id);
  log(`✅ Bot arrêté et nettoyé`);
}

// ROUTES
app.get('/health', (req, res) => res.json({ ok: true, version: '1.0.7' }));
app.get('/livekit-token', async (req, res) => {
  const { room, identity, role } = req.query;
  if (!room || !identity) return res.status(400).json({ error: 'room et identity requis' });
  try { res.json({ token: await generateLiveKitToken(room, identity, role || 'listener') }); }
  catch(e) { res.status(500).json({ error: e.message }); }
});
app.post('/start-session', async (req, res) => {
  const rid = req.query.room || req.body?.room;
  if (!rid) return res.status(400).json({ error: 'room requis' });
  log(`▶️ Requête de démarrage reçue pour salle "${rid}"`);
  res.json({ ok: true });
  startBotForRoom(rid).catch(e => log(`❌ Erreur dans startBot: ${e.message}`));
});
app.post('/end', (req, res) => {
  const id = req.query.session || req.body?.session;
  if (id) stopBotForRoom(id);
  res.json({ ok: true });
});
app.get('/events', (req, res) => {
  res.writeHead(200, { 'Content-Type':'text/event-stream; charset=utf-8','Cache-Control':'no-cache, no-transform','Connection':'keep-alive','Access-Control-Allow-Origin':'*','X-Accel-Buffering':'no' });
  res.write(': connecté\n\n');
  sseClients.add(res);
  log(`📡 Nouvel observateur connecté aux logs`);
  const ka = setInterval(()=>{ try{res.write(': ping\n\n');}catch(e){clearInterval(ka);} },15000);
  req.on('close',()=>{ clearInterval(ka); sseClients.delete(res); });
});

async function main() {
  log(`📋 Variables d'environnement:`);
  log(`   - LIVEKIT_URL: ${LIVEKIT_URL ? '✅ présent' : '❌ MANQUANT'}`);
  log(`   - LIVEKIT_API_KEY: ${LIVEKIT_API_KEY ? '✅ présent' : '❌ MANQUANT'}`);
  log(`   - LIVEKIT_API_SECRET: ${LIVEKIT_API_SECRET ? '✅ présent' : '❌ MANQUANT'}`);
  log(`   - GEMINI_API_KEY: ${GEMINI_API_KEY ? `✅ présent (${GEMINI_API_KEY.length} caractères)` : '❌ MANQUANT'}`);
  log(`   - O2SWITCH_API_URL: ${O2SWITCH_API_URL || 'non défini'}`);
  if (!GEMINI_API_KEY) log('⚠️ GEMINI_API_KEY absente');
  else {
    const m = await import('@google/genai');
    GoogleGenAI = m.GoogleGenAI;
    ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });
    log('✅ Client Gemini initialisé');
  }
  http.createServer(app).listen(PORT, () => log(`🚀 Serveur à l'écoute sur port ${PORT}`));
}
main().catch(e => log(`💥 ERREUR FATALE DÉMARRAGE: ${e?.stack || e}`));
