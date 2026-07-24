import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, { 
  useMultiFileAuthState, 
  makeCacheableSignalKeyStore, 
  DisconnectReason, 
  Browsers, 
  fetchLatestBaileysVersion 
} from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';
import crypto from 'crypto';
import { Boom } from '@hapi/boom';
import cors from 'cors';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const PORT = process.env.PORT || 5000;

// ============================================
// FIX: Ensure uploads and session folders exist
// ============================================
if (!fs.existsSync('uploads')) {
  fs.mkdirSync('uploads', { recursive: true });
}
if (!fs.existsSync('session')) {
  fs.mkdirSync('session', { recursive: true });
}
if (!fs.existsSync('public')) {
  fs.mkdirSync('public', { recursive: true });
}

const upload = multer({ 
  dest: 'uploads/',
  limits: { fileSize: 10 * 1024 * 1024 }
});

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

const SESSION_FILE = './running_sessions.json';
const userSessions = {};
const stopFlags = {};
const activeSockets = {};
const messageQueues = {};
const reconnectAttempts = {};
const sessionStats = {};

const saveSessions = () => {
  try {
    fs.writeFileSync(SESSION_FILE, JSON.stringify(userSessions, null, 2), 'utf8');
  } catch (error) {
    console.error(chalk.red(`Error saving sessions: ${error.message}`));
  }
};

const removeDir = (dirPath) => {
  try {
    if (fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive: true, force: true });
    }
  } catch (e) {}
};

const generateUniqueKey = () => {
  return crypto.randomBytes(16).toString('hex');
};

const cleanupSession = (uniqueKey) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }
  delete stopFlags[uniqueKey];
  delete messageQueues[uniqueKey];
  delete activeSockets[uniqueKey];
};

const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false
    };
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
  }

  const queue = messageQueues[uniqueKey];

  const sendNextMessage = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete messageQueues[uniqueKey];
      return;
    }

    if (!activeSockets[uniqueKey]) {
      console.log(chalk.yellow(`⚠️ Socket disconnected for ${uniqueKey}, waiting for reconnection...`));
      return;
    }

    if (queue.isSending) return;
    if (queue.messages.length === 0) return;

    queue.isSending = true;

    let chatId;
    if (target.includes('@g.us') || target.includes('@s.whatsapp.net')) {
      chatId = target;
    } else {
      const cleanTarget = target.replace(/[^0-9]/g, '');
      chatId = `${cleanTarget}@s.whatsapp.net`;
    }

    const currentMessage = queue.messages[queue.currentIndex];
    const formattedMessage = hatersName ? `${hatersName} ${currentMessage}` : currentMessage;

    try {
      await MznKing.sendMessage(chatId, { text: formattedMessage });
      sessionStats[uniqueKey].sent++;
      sessionStats[uniqueKey].lastMessage = formattedMessage.substring(0, 60);
      console.log(chalk.green(`✉️ [${sessionStats[uniqueKey].sent}] Sent to ${chatId}: ${formattedMessage.substring(0, 50)}...`));

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        console.log(chalk.cyan(`🔄 All messages sent! Restarting from beginning...`));
        queue.currentIndex = 0;
      }
    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Send failed: ${err.message}`));
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[uniqueKey] = { stopped: false, interval: messageInterval };
  console.log(chalk.cyan(`📨 Messaging started! Every ${speed}s → ${target}`));

  sendNextMessage();
};

const connectWithCredsOnly = async (phoneNumber, uniqueKey, sendResponse) => {
  const sessionPath = `./session/${uniqueKey}`;
  let connectionAttempts = 0;
  const MAX_RECONNECT_DELAY = 60000;

  const startConnection = async () => {
    try {
      let displayPhone = phoneNumber || 'Unknown';
      
      console.log(chalk.magenta(`🚀 Connecting [${uniqueKey}] with creds.json`));

      const credsPath = path.join(sessionPath, 'creds.json');
      if (!fs.existsSync(credsPath)) {
        console.log(chalk.red(`❌ creds.json not found for ${uniqueKey}`));
        if (sendResponse) sendResponse('creds.json not found! Please upload valid session file.');
        return;
      }

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      if (!state.creds || !state.creds.registered) {
        console.log(chalk.red(`❌ Invalid creds for ${uniqueKey}`));
        if (sendResponse) sendResponse('Invalid creds! Please upload a valid WhatsApp session.');
        return;
      }

      try {
        if (state.creds.me && state.creds.me.id) {
          const id = state.creds.me.id;
          if (id.includes(':')) {
            displayPhone = id.split(':')[0];
          } else {
            displayPhone = id;
          }
        }
      } catch (e) {}

      const MznKing = makeWASocket({
        version,
        logger: pino.default({ level: 'silent' }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, pino.default({ level: 'silent' }))
        },
        printQRInTerminal: false,
        generateHighQualityLinkPreview: true,
        markOnlineOnConnect: true,
        getMessage: async () => undefined,
        keepAliveIntervalMs: 30000,
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: undefined,
        retryRequestDelayMs: 250,
        shouldSyncHistoryMessage: () => false,
        fireInitQueries: true,
        syncFullHistory: false,
      });

      activeSockets[uniqueKey] = MznKing;

      if (state.creds.registered && sendResponse) {
        console.log(chalk.green(`✅ Session valid for ${uniqueKey}`));
        sendResponse(null);
      }

      MznKing.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          console.log(chalk.green(`✅✅✅ Connected! [${uniqueKey}]`));
          connectionAttempts = 0;
          reconnectAttempts[uniqueKey] = 0;

          userSessions[uniqueKey] = {
            ...userSessions[uniqueKey],
            phoneNumber: displayPhone,
            uniqueKey,
            connected: true,
            lastUpdateTimestamp: Date.now(),
            neverExpire: true,
          };
          saveSessions();

          if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
            const { target, hatersName, messages, speed } = userSessions[uniqueKey];
            console.log(chalk.cyan(`🔄 Resuming messaging for ${uniqueKey}...`));
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = {
                messages: [...messages],
                currentIndex: 0,
                isSending: false
              };
            }
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }
        }

        if (connection === 'close') {
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;

          console.log(chalk.red(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`));

          if (reason === DisconnectReason.badSession || 
              reason === DisconnectReason.loggedOut || 
              reason === 401) {
            console.log(chalk.yellow(`Invalid session, will need re-upload of creds.json`));
            userSessions[uniqueKey].connected = false;
            userSessions[uniqueKey].messaging = false;
            saveSessions();
            return;
          }

          if (!stopFlags[uniqueKey]?.stopped) {
            connectionAttempts++;
            reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
            const delay = Math.min(3000 * connectionAttempts, MAX_RECONNECT_DELAY);
            console.log(chalk.yellow(`🔄 Reconnecting in ${delay / 1000}s... (Attempt ${connectionAttempts})`));
            setTimeout(() => startConnection(), delay);
          }
        }
      });

      MznKing.ev.on('creds.update', saveCreds);
      MznKing.ev.on('messages.upsert', () => {});

    } catch (error) {
      console.error(chalk.red(`❌ ERROR: ${error.message}`));
      if (sendResponse) sendResponse(error.message);
      if (!stopFlags[uniqueKey]?.stopped) {
        connectionAttempts++;
        const delay = Math.min(5000 * connectionAttempts, MAX_RECONNECT_DELAY);
        setTimeout(() => startConnection(), delay);
      }
    }
  };

  await startConnection();
};

// ============================================
// ROUTES
// ============================================

app.post('/login-with-creds', upload.single('credsFile'), async (req, res) => {
  try {
    const credsFile = req.file;

    if (!credsFile) {
      return res.status(400).json({ 
        success: false, 
        message: 'creds.json file is required!' 
      });
    }

    console.log(chalk.cyan(`📞 Login with creds.json`));

    const uniqueKey = generateUniqueKey();
    const sessionPath = `./session/${uniqueKey}`;

    if (!fs.existsSync(sessionPath)) {
      fs.mkdirSync(sessionPath, { recursive: true });
    }

    const credsDestPath = path.join(sessionPath, 'creds.json');
    fs.copyFileSync(credsFile.path, credsDestPath);
    
    try { fs.unlinkSync(credsFile.path); } catch (e) {}

    try {
      const credsContent = fs.readFileSync(credsDestPath, 'utf8');
      const credsData = JSON.parse(credsContent);
      
      if (!credsData.creds || !credsData.creds.registered) {
        return res.status(400).json({ 
          success: false, 
          message: 'Invalid creds.json file!' 
        });
      }
    } catch (err) {
      return res.status(400).json({ 
        success: false, 
        message: 'Invalid creds.json format!' 
      });
    }

    stopFlags[uniqueKey] = { stopped: false };
    reconnectAttempts[uniqueKey] = 0;

    const sendResponse = (errorMsg = null) => {
      if (errorMsg) {
        res.json({ success: false, message: errorMsg, uniqueKey });
      } else {
        res.json({ success: true, message: 'WhatsApp Connected!', uniqueKey });
      }
    };

    await connectWithCredsOnly(null, uniqueKey, sendResponse);
    
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const filePath = req.file?.path;

    if (!uniqueKey || !target || !speed) {
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    }
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    if (!filePath) return res.status(400).json({ success: false, message: 'No message file uploaded!' });

    let messages = [];
    try {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0) return res.status(400).json({ success: false, message: 'File has no valid messages!' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file!' });
    } finally {
      try { fs.unlinkSync(filePath); } catch (e) {}
    }

    const MznKing = activeSockets[uniqueKey];

    userSessions[uniqueKey].target = target;
    userSessions[uniqueKey].hatersName = hatersName || '';
    userSessions[uniqueKey].messages = messages;
    userSessions[uniqueKey].speed = speed;
    userSessions[uniqueKey].messaging = true;
    saveSessions();

    delete messageQueues[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };

    startMessaging(MznKing, uniqueKey, target, hatersName || '', messages, speed);

    res.json({
      success: true,
      message: 'Message automation started!',
      uniqueKey,
      messageCount: messages.length,
      target
    });
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session = userSessions[uniqueKey];
  const stats = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  res.json({
    exists: true,
    connected: !!activeSockets[uniqueKey],
    messaging: session.messaging && !stopFlags[uniqueKey]?.stopped,
    sent: stats.sent,
    failed: stats.failed,
    lastMessage: stats.lastMessage,
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || 0,
  });
});

app.get('/sessionStatus/all', (req, res) => {
  const sessions = {};
  for (const [key, session] of Object.entries(userSessions)) {
    const stats = sessionStats[key] || { sent: 0, failed: 0, lastMessage: '' };
    sessions[key] = {
      exists: true,
      connected: !!activeSockets[key],
      messaging: session.messaging && !stopFlags[key]?.stopped,
      sent: stats.sent,
      failed: stats.failed,
      lastMessage: stats.lastMessage,
      target: session.target,
      speed: session.speed,
      messageCount: session.messages?.length || 0,
      phoneNumber: session.phoneNumber,
    };
  }
  res.json(sessions);
});

app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
  if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    if (stopFlags[uniqueKey]?.interval) {
      stopFlags[uniqueKey].stopped = true;
      clearInterval(stopFlags[uniqueKey].interval);
    }
    delete stopFlags[uniqueKey];
    delete messageQueues[uniqueKey];
    delete sessionStats[uniqueKey];

    if (activeSockets[uniqueKey]) {
      try {
        await activeSockets[uniqueKey].logout();
      } catch (e) {}
      delete activeSockets[uniqueKey];
    }

    const sessionPath = `./session/${uniqueKey}`;
    removeDir(sessionPath);
    delete userSessions[uniqueKey];
    saveSessions();

    console.log(chalk.red(`✅ Stopped & logged out: ${uniqueKey}`));
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

app.get('/health', (req, res) => {
  const sessions = Object.keys(userSessions).length;
  const active = Object.keys(activeSockets).length;
  res.json({
    status: 'online',
    uptime: process.uptime(),
    sessions,
    active,
    memory: process.memoryUsage(),
    timestamp: Date.now()
  });
});

const restoreSessions = async () => {
  if (fs.existsSync(SESSION_FILE)) {
    try {
      const data = fs.readFileSync(SESSION_FILE, 'utf8');
      const savedSessions = JSON.parse(data);
      Object.assign(userSessions, savedSessions);

      console.log(chalk.green(`📂 Found ${Object.keys(userSessions).length} saved sessions`));

      for (const [key, session] of Object.entries(userSessions)) {
        if (session.phoneNumber && session.uniqueKey) {
          const sessionPath = `./session/${session.uniqueKey}`;
          const credsPath = path.join(sessionPath, 'creds.json');
          
          if (fs.existsSync(credsPath)) {
            console.log(chalk.cyan(`🔄 Restoring: ${session.uniqueKey}`));
            stopFlags[session.uniqueKey] = { stopped: false };
            reconnectAttempts[session.uniqueKey] = 0;

            userSessions[session.uniqueKey].neverExpire = true;

            if (session.messaging && session.messages) {
              messageQueues[session.uniqueKey] = {
                messages: [...session.messages],
                currentIndex: 0,
                isSending: false
              };
            }

            await connectWithCredsOnly(session.phoneNumber, session.uniqueKey, null);
            await new Promise(resolve => setTimeout(resolve, 1000));
          } else {
            console.log(chalk.yellow(`⚠️ creds.json missing for ${session.uniqueKey}, skipping...`));
          }
        }
      }
      console.log(chalk.green(`✅ Session restoration complete!`));
    } catch (err) {
      console.error(chalk.red(`Error loading sessions: ${err.message}`));
    }
  }
};

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 http://localhost:${PORT}`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

  await restoreSessions();
});
