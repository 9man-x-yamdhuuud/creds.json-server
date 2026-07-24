import express from 'express';
import fs from 'fs';
import chalk from 'chalk';
import multer from 'multer';
import makeWASocket, { useMultiFileAuthState, makeCacheableSignalKeyStore, DisconnectReason, Browsers, fetchLatestBaileysVersion } from '@whiskeysockets/baileys';
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
const upload = multer({ dest: 'uploads/' });

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
const messageCounters = {}; // Track total messages sent per session

// Auto-restart configuration for 24/7 operation
const AUTO_RESTART_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours
const HEALTH_CHECK_INTERVAL = 5 * 60 * 1000; // 5 minutes
const MAX_RECONNECT_ATTEMPTS = 50;
const MESSAGE_RETRY_DELAY = 5000; // 5 seconds retry delay

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
  if (stopFlags[uniqueKey]?.retryTimeout) {
    clearTimeout(stopFlags[uniqueKey].retryTimeout);
  }
  delete stopFlags[uniqueKey];
  delete messageQueues[uniqueKey];
  delete activeSockets[uniqueKey];
};

// Enhanced messaging with retry and persistence
const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }
  if (stopFlags[uniqueKey]?.retryTimeout) {
    clearTimeout(stopFlags[uniqueKey].retryTimeout);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false,
      failedMessages: [],
      lastSentTime: Date.now()
    };
  }

  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { 
      sent: 0, 
      failed: 0, 
      lastMessage: '',
      totalSent: 0,
      startTime: Date.now()
    };
  }

  if (!messageCounters[uniqueKey]) {
    messageCounters[uniqueKey] = 0;
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
      // Try to reconnect
      if (!stopFlags[uniqueKey]?.reconnecting) {
        stopFlags[uniqueKey].reconnecting = true;
        setTimeout(() => {
          stopFlags[uniqueKey].reconnecting = false;
        }, 30000);
      }
      return;
    }

    if (queue.isSending) return;
    if (queue.messages.length === 0) {
      // If no messages, reload from session
      if (userSessions[uniqueKey]?.messages) {
        queue.messages = [...userSessions[uniqueKey].messages];
        queue.currentIndex = 0;
        console.log(chalk.cyan(`🔄 Reloaded messages for ${uniqueKey}`));
      } else {
        return;
      }
    }

    queue.isSending = true;

    // Support both group IDs and personal numbers
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
      sessionStats[uniqueKey].totalSent++;
      messageCounters[uniqueKey]++;
      sessionStats[uniqueKey].lastMessage = formattedMessage.substring(0, 60);
      
      // Update session with progress
      userSessions[uniqueKey].lastMessageIndex = queue.currentIndex;
      userSessions[uniqueKey].totalSent = sessionStats[uniqueKey].totalSent;
      saveSessions();
      
      console.log(chalk.green(`✉️ [${sessionStats[uniqueKey].totalSent}] Sent to ${chatId}: ${formattedMessage.substring(0, 50)}...`));

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        console.log(chalk.cyan(`🔄 All messages sent! Restarting from beginning...`));
        queue.currentIndex = 0;
        // Save progress
        userSessions[uniqueKey].lastMessageIndex = 0;
        saveSessions();
      }

      queue.lastSentTime = Date.now();

    } catch (err) {
      sessionStats[uniqueKey].failed++;
      console.error(chalk.red(`❌ Send failed: ${err.message}`));
      
      // Retry failed message after delay
      if (!stopFlags[uniqueKey]?.retryTimeout) {
        stopFlags[uniqueKey].retryTimeout = setTimeout(() => {
          stopFlags[uniqueKey].retryTimeout = null;
          queue.isSending = false;
          sendNextMessage(); // Retry
        }, MESSAGE_RETRY_DELAY);
      }
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[uniqueKey] = { 
    stopped: false, 
    interval: messageInterval,
    reconnecting: false,
    retryTimeout: null
  };
  
  console.log(chalk.cyan(`📨 Messaging started! Every ${speed}s → ${target}`));
  console.log(chalk.green(`🔄 Will run continuously until stopped`));

  // Send first message immediately
  sendNextMessage();
};

// Health check and auto-restart for 24/7 operation
const startHealthCheck = () => {
  setInterval(() => {
    const now = Date.now();
    for (const [uniqueKey, session] of Object.entries(userSessions)) {
      if (session.messaging && !stopFlags[uniqueKey]?.stopped) {
        const queue = messageQueues[uniqueKey];
        if (queue && (now - queue.lastSentTime) > 300000) { // 5 minutes no activity
          console.log(chalk.yellow(`⚠️ No activity for ${uniqueKey}, checking health...`));
          
          // Restart messaging if stuck
          if (activeSockets[uniqueKey]) {
            console.log(chalk.green(`🔄 Restarting messaging for ${uniqueKey}`));
            const { target, hatersName, messages, speed } = session;
            if (messages && messages.length > 0) {
              startMessaging(activeSockets[uniqueKey], uniqueKey, target, hatersName, messages, speed);
            }
          }
        }
      }
    }
  }, HEALTH_CHECK_INTERVAL);
};

// Auto-restart sessions every 24 hours to prevent memory leaks
const startAutoRestart = () => {
  setInterval(async () => {
    console.log(chalk.yellow(`🔄 Performing 24-hour auto-restart cycle...`));
    const sessionsToRestart = [];
    
    for (const [uniqueKey, session] of Object.entries(userSessions)) {
      if (session.messaging && activeSockets[uniqueKey]) {
        sessionsToRestart.push({
          uniqueKey,
          phoneNumber: session.phoneNumber,
          target: session.target,
          hatersName: session.hatersName,
          messages: session.messages,
          speed: session.speed
        });
      }
    }

    // Gracefully restart each session
    for (const session of sessionsToRestart) {
      try {
        console.log(chalk.cyan(`🔄 Auto-restarting session: ${session.uniqueKey}`));
        
        // Stop current session
        if (stopFlags[session.uniqueKey]?.interval) {
          clearInterval(stopFlags[session.uniqueKey].interval);
        }
        delete stopFlags[session.uniqueKey];
        
        // Reconnect
        await connectAndLogin(session.phoneNumber, session.uniqueKey, null);
        
        // Restart messaging after delay
        setTimeout(() => {
          if (activeSockets[session.uniqueKey]) {
            startMessaging(
              activeSockets[session.uniqueKey],
              session.uniqueKey,
              session.target,
              session.hatersName || '',
              session.messages,
              session.speed
            );
          }
        }, 5000);
        
      } catch (error) {
        console.error(chalk.red(`❌ Auto-restart failed for ${session.uniqueKey}: ${error.message}`));
      }
    }
    
    console.log(chalk.green(`✅ Auto-restart cycle completed for ${sessionsToRestart.length} sessions`));
  }, AUTO_RESTART_INTERVAL);
};

const connectAndLogin = async (phoneNumber, uniqueKey, sendPairingCode = null) => {
  const sessionPath = `./session/${uniqueKey}`;
  let pairingCodeSent = false;

  const startConnection = async () => {
    try {
      console.log(chalk.magenta(`🚀 Connecting ${phoneNumber} [${uniqueKey}]`));

      if (!fs.existsSync(sessionPath)) {
        fs.mkdirSync(sessionPath, { recursive: true });
      }

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

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
        syncFullHistory: false,
        patchMessageBeforeSending: (message) => {
          // Ensure message is sent properly
          return message;
        }
      });

      activeSockets[uniqueKey] = MznKing;

      if (!MznKing.authState.creds.registered && !pairingCodeSent && sendPairingCode) {
        await new Promise(resolve => setTimeout(resolve, 2000));
        try {
          const cleanedNumber = phoneNumber.replace(/[^0-9]/g, '');
          console.log(chalk.cyan(`🔐 Requesting pairing code for ${cleanedNumber}...`));

          const code = await MznKing.requestPairingCode(cleanedNumber);
          const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;

          console.log(chalk.green(`✅ Pairing Code: ${pairingCode}`));

          if (!pairingCodeSent) {
            pairingCodeSent = true;
            sendPairingCode(pairingCode, false);
          }
        } catch (error) {
          console.error(chalk.red(`❌ Pairing error: ${error.message}`));
          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, false, error.message);
          }
        }
      } else if (MznKing.authState.creds.registered) {
        console.log(chalk.green(`✅ Session already registered for ${uniqueKey}`));
        if (!pairingCodeSent && sendPairingCode) {
          pairingCodeSent = true;
          sendPairingCode(null, true);
        }
      }

      MznKing.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          console.log(chalk.green(`✅✅✅ Connected! [${uniqueKey}]`));
          reconnectAttempts[uniqueKey] = 0;

          userSessions[uniqueKey] = {
            ...userSessions[uniqueKey],
            phoneNumber,
            uniqueKey,
            connected: true,
            lastUpdateTimestamp: Date.now()
          };
          saveSessions();

          if (!pairingCodeSent && sendPairingCode) {
            pairingCodeSent = true;
            sendPairingCode(null, true);
          }

          if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
            const { target, hatersName, messages, speed } = userSessions[uniqueKey];
            console.log(chalk.cyan(`🔄 Resuming messaging for ${uniqueKey}...`));
            if (!messageQueues[uniqueKey]) {
              const lastIndex = userSessions[uniqueKey].lastMessageIndex || 0;
              messageQueues[uniqueKey] = {
                messages: [...messages],
                currentIndex: lastIndex,
                isSending: false,
                lastSentTime: Date.now()
              };
            }
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }
        }

        if (connection === 'close') {
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;

          console.log(chalk.red(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`));

          if (reason === DisconnectReason.badSession) {
            console.log(chalk.red(`Bad session, deleting and reconnecting...`));
            removeDir(sessionPath);
          } else if (reason === DisconnectReason.connectionReplaced) {
            console.log(chalk.red(`Connection replaced, reconnecting...`));
            cleanupSession(uniqueKey);
            // Try to reconnect
            setTimeout(() => startConnection(), 5000);
            return;
          } else if (reason === DisconnectReason.loggedOut) {
            console.log(chalk.red(`Device logged out, reconnecting...`));
            removeDir(sessionPath);
            cleanupSession(uniqueKey);
            if (userSessions[uniqueKey]) {
              userSessions[uniqueKey].connected = false;
              userSessions[uniqueKey].messaging = false;
              saveSessions();
            }
            // Try to reconnect with new session
            setTimeout(() => startConnection(), 10000);
            return;
          } else if (reason === 401) {
            console.log(chalk.red(`Unauthorized (401), reconnecting...`));
            removeDir(sessionPath);
            cleanupSession(uniqueKey);
            if (userSessions[uniqueKey]) {
              userSessions[uniqueKey].connected = false;
              userSessions[uniqueKey].messaging = false;
              saveSessions();
            }
            setTimeout(() => startConnection(), 10000);
            return;
          }

          // Auto-reconnect with exponential backoff
          if (!stopFlags[uniqueKey]?.stopped) {
            reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
            if (reconnectAttempts[uniqueKey] <= MAX_RECONNECT_ATTEMPTS) {
              const delay = Math.min(5000 * reconnectAttempts[uniqueKey], 60000);
              console.log(chalk.yellow(`🔄 Reconnecting in ${delay / 1000}s... (Attempt ${reconnectAttempts[uniqueKey]})`));
              setTimeout(() => startConnection(), delay);
            } else {
              console.log(chalk.red(`❌ Max reconnect attempts reached for ${uniqueKey}`));
              // Reset attempts and try again after longer delay
              reconnectAttempts[uniqueKey] = 0;
              setTimeout(() => startConnection(), 120000); // 2 minutes
            }
          }
        }
      });

      MznKing.ev.on('creds.update', saveCreds);
      MznKing.ev.on('messages.upsert', () => {});

    } catch (error) {
      console.error(chalk.red(`❌ ERROR: ${error.message}`));
      if (!pairingCodeSent && sendPairingCode) {
        pairingCodeSent = true;
        sendPairingCode(null, false, error.message);
      }
      if (!stopFlags[uniqueKey]?.stopped) {
        reconnectAttempts[uniqueKey] = (reconnectAttempts[uniqueKey] || 0) + 1;
        if (reconnectAttempts[uniqueKey] <= MAX_RECONNECT_ATTEMPTS) {
          const delay = Math.min(5000 * reconnectAttempts[uniqueKey], 60000);
          setTimeout(() => startConnection(), delay);
        } else {
          reconnectAttempts[uniqueKey] = 0;
          setTimeout(() => startConnection(), 120000);
        }
      }
    }
  };

  await startConnection();
};

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
          if (fs.existsSync(sessionPath)) {
            console.log(chalk.cyan(`🔄 Restoring: ${session.uniqueKey} (${session.phoneNumber})`));
            stopFlags[session.uniqueKey] = { stopped: false };
            reconnectAttempts[session.uniqueKey] = 0;

            if (session.messaging && session.messages) {
              const lastIndex = session.lastMessageIndex || 0;
              messageQueues[session.uniqueKey] = {
                messages: [...session.messages],
                currentIndex: lastIndex,
                isSending: false,
                lastSentTime: Date.now()
              };
            }

            await connectAndLogin(session.phoneNumber, session.uniqueKey, null);
            await new Promise(resolve => setTimeout(resolve, 2000));
          }
        }
      }
      console.log(chalk.green(`✅ Session restoration complete!`));
      
      // Start health check and auto-restart
      startHealthCheck();
      startAutoRestart();
      
    } catch (err) {
      console.error(chalk.red(`Error loading sessions: ${err.message}`));
    }
  }
};

// ── Routes ──────────────────────────────────────────────────────────────────

app.post('/login', async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    if (!phoneNumber) return res.status(400).json({ success: false, message: 'Phone number is required!' });

    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    console.log(chalk.cyan(`📞 Login: ${phoneNumber}`));

    const uniqueKey = generateUniqueKey();
    stopFlags[uniqueKey] = { stopped: false };
    reconnectAttempts[uniqueKey] = 0;

    const sendPairingCode = (pairingCode, isConnected = false, errorMsg = null) => {
      if (errorMsg) {
        res.json({ success: false, message: 'Error generating pairing code', error: errorMsg, uniqueKey });
      } else if (isConnected) {
        res.json({ success: true, message: 'WhatsApp Connected!', connected: true, uniqueKey });
      } else {
        res.json({ success: true, message: 'Pairing code generated', pairingCode, uniqueKey });
      }
    };

    await connectAndLogin(phoneNumber, uniqueKey, sendPairingCode);
  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

app.post('/getGroupUID', async (req, res) => {
  try {
    const { uniqueKey } = req.body;
    if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No active session' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected' });

    const MznKing = activeSockets[uniqueKey];
    await new Promise(resolve => setTimeout(resolve, 1000));

    const groups = await MznKing.groupFetchAllParticipating();
    const groupUIDs = Object.values(groups).map(group => ({
      groupName: group.subject,
      groupId: group.id,
    }));

    console.log(chalk.green(`✅ Fetched ${groupUIDs.length} groups for ${uniqueKey}`));
    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
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

    userSessions[uniqueKey] = {
      ...userSessions[uniqueKey],
      target: target,
      hatersName: hatersName || '',
      messages: messages,
      speed: speed,
      messaging: true,
      lastMessageIndex: 0,
      totalSent: 0,
      startTime: Date.now()
    };
    saveSessions();

    // Reset queue for fresh start
    delete messageQueues[uniqueKey];
    delete messageCounters[uniqueKey];
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '', totalSent: 0, startTime: Date.now() };

    startMessaging(MznKing, uniqueKey, target, hatersName || '', messages, speed);

    res.json({
      success: true,
      message: 'Message automation started! Will run 24/7',
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
  const stats = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '', totalSent: 0 };
  const queue = messageQueues[uniqueKey];

  if (!session) return res.json({ exists: false });

  res.json({
    exists: true,
    connected: !!activeSockets[uniqueKey],
    messaging: session.messaging && !stopFlags[uniqueKey]?.stopped,
    sent: stats.sent,
    failed: stats.failed,
    totalSent: stats.totalSent || 0,
    lastMessage: stats.lastMessage,
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || 0,
    currentIndex: queue?.currentIndex || 0,
    uptime: Math.floor((Date.now() - (session.startTime || Date.now())) / 1000)
  });
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
    if (stopFlags[uniqueKey]?.retryTimeout) {
      clearTimeout(stopFlags[uniqueKey].retryTimeout);
    }
    delete stopFlags[uniqueKey];
    delete messageQueues[uniqueKey];
    delete sessionStats[uniqueKey];
    delete messageCounters[uniqueKey];

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

app.get('/stats', (req, res) => {
  const totalSessions = Object.keys(userSessions).length;
  const activeSessions = Object.keys(activeSockets).length;
  const totalMessages = Object.values(sessionStats).reduce((sum, stat) => sum + (stat.totalSent || 0), 0);
  
  res.json({
    totalSessions,
    activeSessions,
    totalMessages,
    sessions: Object.entries(userSessions).map(([key, session]) => ({
      id: key,
      phone: session.phoneNumber,
      messaging: session.messaging,
      connected: !!activeSockets[key],
      totalSent: sessionStats[key]?.totalSent || 0,
      uptime: Math.floor((Date.now() - (session.startTime || Date.now())) / 3600)
    }))
  });
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(chalk.green(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`));
  console.log(chalk.green(`✅ Server running on port ${PORT}`));
  console.log(chalk.cyan(`🌐 CORS enabled for all origins`));
  console.log(chalk.yellow(`🔄 24/7 Auto-restart enabled`));
  console.log(chalk.yellow(`🔄 Health check every 5 minutes`));
  console.log(chalk.green(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`));

  await restoreSessions();
});
