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
// LOGGER
// ============================================
const logger = pino.default({
  level: 'info',
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'SYS:standard',
      ignore: 'pid,hostname',
    },
  },
});

// ============================================
// ENSURE FOLDERS
// ============================================
['uploads', 'session', 'public', 'logs'].forEach(folder => {
  if (!fs.existsSync(folder)) {
    fs.mkdirSync(folder, { recursive: true });
    logger.info(`✅ Created folder: ${folder}`);
  }
});

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

// ============================================
// GLOBALS
// ============================================
const userSessions = {};
const stopFlags = {};
const activeSockets = {};
const messageQueues = {};
const sessionStatus = {};
const sessionStats = {};
const pairingCodes = {};
const activePairingRequests = {};
const socketCleanupTimers = {};

// ============================================
// SESSION STATUS TRACKING
// ============================================
const getDefaultStatus = (uniqueKey) => ({
  uniqueKey,
  connected: false,
  lastConnectedAt: null,
  lastMessageAt: null,
  lastErrorAt: null,
  reconnectCount: 0,
  uptimeSeconds: 0,
  totalSent: 0,
  totalFailed: 0,
  lastMessage: null,
  connectionStartTime: null,
  status: 'idle',
  errorMessage: null,
});

const updateSessionStatus = (uniqueKey, updates) => {
  if (!sessionStatus[uniqueKey]) {
    sessionStatus[uniqueKey] = getDefaultStatus(uniqueKey);
  }
  Object.assign(sessionStatus[uniqueKey], updates);
  
  if (sessionStatus[uniqueKey].connected && sessionStatus[uniqueKey].connectionStartTime) {
    sessionStatus[uniqueKey].uptimeSeconds = Math.floor(
      (Date.now() - sessionStatus[uniqueKey].connectionStartTime) / 1000
    );
  }
  
  saveSessions();
};

// ============================================
// SESSION PERSISTENCE
// ============================================
const saveSessions = () => {
  try {
    const data = {
      userSessions,
      sessionStatus,
      timestamp: Date.now()
    };
    fs.writeFileSync('./running_sessions.json', JSON.stringify(data, null, 2), 'utf8');
  } catch (error) {
    logger.error(`Error saving sessions: ${error.message}`);
  }
};

const loadSessions = () => {
  try {
    if (fs.existsSync('./running_sessions.json')) {
      const data = JSON.parse(fs.readFileSync('./running_sessions.json', 'utf8'));
      Object.assign(userSessions, data.userSessions || {});
      Object.assign(sessionStatus, data.sessionStatus || {});
      logger.info(`📂 Loaded ${Object.keys(userSessions).length} sessions`);
      return true;
    }
  } catch (error) {
    logger.error(`Error loading sessions: ${error.message}`);
  }
  return false;
};

// ============================================
// 🔥 CLEANUP HELPERS - FIXED
// ============================================
const removeDir = (dirPath) => {
  try {
    if (fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive: true, force: true });
    }
  } catch (e) {}
};

const cleanupSessionResources = (uniqueKey) => {
  // Clear message interval
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
    delete stopFlags[uniqueKey].interval;
  }
  
  // Clear message queue
  delete messageQueues[uniqueKey];
  
  // Close and cleanup socket
  if (activeSockets[uniqueKey]) {
    try {
      // Remove all event listeners
      activeSockets[uniqueKey].ev.removeAllListeners();
      activeSockets[uniqueKey].end();
    } catch (e) {}
    try {
      activeSockets[uniqueKey].logout();
    } catch (e) {}
    delete activeSockets[uniqueKey];
  }
  
  // Clear any pending cleanup timer
  if (socketCleanupTimers[uniqueKey]) {
    clearTimeout(socketCleanupTimers[uniqueKey]);
    delete socketCleanupTimers[uniqueKey];
  }
  
  // Update status
  if (sessionStatus[uniqueKey]) {
    sessionStatus[uniqueKey].connected = false;
    sessionStatus[uniqueKey].status = 'stopped';
    saveSessions();
  }
  
  logger.info(`🧹 Cleaned up resources for session: ${uniqueKey}`);
};

const generateUniqueKey = () => {
  return crypto.randomBytes(16).toString('hex');
};

// ============================================
// MESSAGING ENGINE
// ============================================
const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  if (stopFlags[uniqueKey]?.interval) {
    clearInterval(stopFlags[uniqueKey].interval);
  }

  if (!messageQueues[uniqueKey]) {
    messageQueues[uniqueKey] = {
      messages: [...messages],
      currentIndex: 0,
      isSending: false,
      startTime: Date.now()
    };
  }

  const queue = messageQueues[uniqueKey];

  const sendNextMessage = async () => {
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete messageQueues[uniqueKey];
      return;
    }

    if (!activeSockets[uniqueKey]) {
      logger.warn(`⚠️ Socket disconnected for ${uniqueKey}, waiting...`);
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
      
      if (sessionStats[uniqueKey]) {
        sessionStats[uniqueKey].sent++;
        sessionStats[uniqueKey].lastMessage = formattedMessage.substring(0, 60);
      }
      
      if (sessionStatus[uniqueKey]) {
        sessionStatus[uniqueKey].totalSent++;
        sessionStatus[uniqueKey].lastMessageAt = Date.now();
        sessionStatus[uniqueKey].lastMessage = formattedMessage.substring(0, 60);
        saveSessions();
      }
      
      logger.info(`✉️ [${sessionStats[uniqueKey]?.sent || 0}] Sent: ${formattedMessage.substring(0, 40)}...`);

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        logger.info(`🔄 All messages sent! Restarting...`);
        queue.currentIndex = 0;
      }
    } catch (err) {
      if (sessionStats[uniqueKey]) {
        sessionStats[uniqueKey].failed++;
      }
      if (sessionStatus[uniqueKey]) {
        sessionStatus[uniqueKey].totalFailed++;
        sessionStatus[uniqueKey].lastErrorAt = Date.now();
        sessionStatus[uniqueKey].errorMessage = err.message;
        saveSessions();
      }
      logger.error(`❌ Send failed: ${err.message}`);
    } finally {
      queue.isSending = false;
    }
  };

  const interval = parseInt(speed) * 1000;
  const messageInterval = setInterval(sendNextMessage, interval);
  stopFlags[uniqueKey] = { stopped: false, interval: messageInterval };
  logger.info(`📨 Messaging started! Every ${speed}s → ${target}`);

  setTimeout(sendNextMessage, 100);
};

// ============================================
// 🔥 FIXED: Send Confirmation Message After Pairing
// ============================================
const sendConfirmationMessage = async (MznKing, phoneNumber) => {
  try {
    const chatId = `${phoneNumber}@s.whatsapp.net`;
    await MznKing.sendMessage(chatId, {
      text: `✅ WhatsApp successfully linked!\n\nYour session is now active and secure.`
    });
    logger.info(`📨 Confirmation message sent to ${phoneNumber}`);
    return true;
  } catch (error) {
    logger.warn(`⚠️ Could not send confirmation message: ${error.message}`);
    return false;
  }
};

// ============================================
// 🔥 PAIRING CODE LOGIN - COMPLETELY FIXED
// ============================================
const startPairing = async (phoneNumber, res) => {
  // Unique request ID for this pairing attempt
  const requestId = Date.now() + '_' + Math.random().toString(36).substring(2, 8);
  logger.info(`🔐 Pairing request ${requestId} for ${phoneNumber}`);

  try {
    // 🔥 CLEANUP: Remove any existing session for this number
    const existingKeys = Object.keys(userSessions);
    for (const key of existingKeys) {
      if (userSessions[key]?.phoneNumber === phoneNumber) {
        logger.info(`⚠️ Removing existing session for ${phoneNumber} (${key})`);
        cleanupSessionResources(key);
        delete userSessions[key];
        delete sessionStatus[key];
        delete sessionStats[key];
        delete pairingCodes[key];
        saveSessions();
        break;
      }
    }

    // Generate unique key for this session
    const uniqueKey = generateUniqueKey();
    const sessionPath = `./session/${uniqueKey}`;

    // Ensure clean session directory
    if (fs.existsSync(sessionPath)) {
      removeDir(sessionPath);
    }
    fs.mkdirSync(sessionPath, { recursive: true });

    // Initialize session data
    userSessions[uniqueKey] = {
      phoneNumber,
      uniqueKey,
      connected: false,
      messaging: false,
      lastUpdateTimestamp: Date.now(),
      neverExpire: true,
      loginMethod: 'pairing',
      pairingRequestId: requestId,
    };

    sessionStatus[uniqueKey] = getDefaultStatus(uniqueKey);
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
    stopFlags[uniqueKey] = { stopped: false };
    saveSessions();

    let pairingCodeSent = false;
    let socketCreated = false;
    let cleanupTimeout = null;

    logger.info(`🔐 Starting pairing for ${phoneNumber} (${uniqueKey})`);

    // Create auth state with clean directory
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();

    // Create socket with clean configuration
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

    socketCreated = true;
    activeSockets[uniqueKey] = MznKing;

    // 🔥 CRITICAL FIX: Request pairing code
    if (!pairingCodeSent) {
      try {
        const code = await MznKing.requestPairingCode(phoneNumber);
        const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
        pairingCodeSent = true;
        pairingCodes[uniqueKey] = pairingCode;
        
        logger.info(`📱 Pairing Code for ${phoneNumber}: ${pairingCode}`);
        
        // Send response immediately
        if (res && !res.headersSent) {
          res.json({
            success: true,
            message: 'Pairing code generated successfully',
            pairingCode,
            uniqueKey,
            requestId,
          });
        }
      } catch (error) {
        logger.error(`❌ Pairing error for ${phoneNumber}: ${error.message}`);
        pairingCodeSent = true;
        
        // Cleanup on error
        cleanupSessionResources(uniqueKey);
        delete userSessions[uniqueKey];
        delete sessionStatus[uniqueKey];
        delete sessionStats[uniqueKey];
        delete pairingCodes[uniqueKey];
        saveSessions();
        
        if (res && !res.headersSent) {
          res.status(500).json({
            success: false,
            message: 'Error generating pairing code: ' + error.message,
            error: error.message,
            uniqueKey: null,
            requestId,
          });
        }
        return;
      }
    }

    // 🔥 Set cleanup timer for stale sockets (5 minutes)
    cleanupTimeout = setTimeout(() => {
      if (activeSockets[uniqueKey] && !sessionStatus[uniqueKey]?.connected) {
        logger.info(`🧹 Cleaning up stale socket for ${uniqueKey}`);
        cleanupSessionResources(uniqueKey);
        delete userSessions[uniqueKey];
        delete sessionStatus[uniqueKey];
        delete sessionStats[uniqueKey];
        delete pairingCodes[uniqueKey];
        saveSessions();
      }
    }, 300000); // 5 minutes

    socketCleanupTimers[uniqueKey] = cleanupTimeout;

    // 🔥 CONNECTION UPDATE HANDLER - Single instance
    const connectionHandler = async (update) => {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        logger.info(`✅ Connected! [${uniqueKey}]`);
        
        // Clear cleanup timer
        if (socketCleanupTimers[uniqueKey]) {
          clearTimeout(socketCleanupTimers[uniqueKey]);
          delete socketCleanupTimers[uniqueKey];
        }
        
        updateSessionStatus(uniqueKey, {
          connected: true,
          status: 'connected',
          connectionStartTime: Date.now(),
          lastConnectedAt: Date.now(),
        });

        userSessions[uniqueKey].connected = true;
        userSessions[uniqueKey].lastUpdateTimestamp = Date.now();
        saveSessions();

        // 🔥 Send confirmation message
        await sendConfirmationMessage(MznKing, phoneNumber);

        // Resume messaging if any
        if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
          const { target, hatersName, messages, speed } = userSessions[uniqueKey];
          startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
        }
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
        
        logger.warn(`⚠️ Connection closed - Reason: ${reason}`);
        
        if (reason === DisconnectReason.badSession || 
            reason === DisconnectReason.loggedOut || 
            reason === 401) {
          logger.error(`❌ Invalid session for ${uniqueKey}`);
          updateSessionStatus(uniqueKey, {
            connected: false,
            status: 'error',
            errorMessage: 'Invalid session',
          });
          cleanupSessionResources(uniqueKey);
          return;
        }

        // Auto-reconnect
        if (!stopFlags[uniqueKey]?.stopped) {
          updateSessionStatus(uniqueKey, {
            connected: false,
            status: 'reconnecting',
            errorMessage: `Disconnected: ${reason || 'Unknown'}`,
          });
          setTimeout(() => {
            startPairing(phoneNumber, null);
          }, 5000);
        }
      }
    };

    // 🔥 CREDENTIALS UPDATE HANDLER
    const credsHandler = () => {
      saveCreds();
      logger.debug(`🔑 Creds updated for ${uniqueKey}`);
    };

    // Register handlers
    MznKing.ev.on('connection.update', connectionHandler);
    MznKing.ev.on('creds.update', credsHandler);

    // Store handlers reference for cleanup
    MznKing._handlers = {
      connection: connectionHandler,
      creds: credsHandler,
    };

  } catch (error) {
    logger.error(`❌ Pairing error: ${error.message}`);
    logger.error(error.stack);
    
    if (res && !res.headersSent) {
      res.status(500).json({
        success: false,
        message: 'Error starting pairing: ' + error.message,
        error: error.message,
        uniqueKey: null,
      });
    }
  }
};

// ============================================
// API ROUTES
// ============================================

// ============================================
// 🔥 PAIRING CODE LOGIN - FIXED
// ============================================
app.post('/login', async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    
    // Validate phone number
    if (!phoneNumber) {
      return res.status(400).json({ 
        success: false, 
        message: 'Phone number is required!' 
      });
    }

    // Normalize phone number
    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    
    if (phoneNumber.length < 10 || phoneNumber.length > 15) {
      return res.status(400).json({ 
        success: false, 
        message: 'Invalid phone number! Must be 10-15 digits.' 
      });
    }

    logger.info(`📞 Pairing request for: ${phoneNumber}`);

    // Check if there's already a pending request for this number
    if (activePairingRequests[phoneNumber]) {
      return res.status(429).json({
        success: false,
        message: 'A pairing request is already in progress for this number. Please wait.',
      });
    }

    // Set active request flag
    activePairingRequests[phoneNumber] = Date.now();

    try {
      await startPairing(phoneNumber, res);
    } finally {
      // Clear active request flag after completion
      setTimeout(() => {
        delete activePairingRequests[phoneNumber];
      }, 5000);
    }

  } catch (error) {
    logger.error(`Login error: ${error.message}`);
    if (!res.headersSent) {
      res.status(500).json({ 
        success: false, 
        message: `Server Error: ${error.message}` 
      });
    }
  }
});

// ============================================
// GET GROUPS
// ============================================
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

    res.json({ success: true, groupUIDs });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error fetching groups' });
  }
});

// ============================================
// START MESSAGING
// ============================================
app.post('/startMessaging', upload.single('messageFile'), async (req, res) => {
  try {
    const { uniqueKey, target, hatersName, speed } = req.body;
    const fileBuffer = req.file?.buffer;

    if (!uniqueKey || !target || !speed) {
      return res.status(400).json({ success: false, message: 'Missing required fields!' });
    }
    if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'Invalid session key!' });
    if (!activeSockets[uniqueKey]) return res.status(400).json({ success: false, message: 'WhatsApp not connected!' });
    if (!fileBuffer) return res.status(400).json({ success: false, message: 'No message file uploaded!' });

    let messages = [];
    try {
      const fileContent = fileBuffer.toString('utf-8');
      messages = fileContent.split('\n').map(l => l.trim()).filter(l => l.length > 0);
      if (messages.length === 0) return res.status(400).json({ success: false, message: 'File has no valid messages!' });
    } catch (err) {
      return res.status(500).json({ success: false, message: 'Error reading file!' });
    }

    const MznKing = activeSockets[uniqueKey];

    userSessions[uniqueKey].target = target;
    userSessions[uniqueKey].hatersName = hatersName || '';
    userSessions[uniqueKey].messages = messages;
    userSessions[uniqueKey].speed = speed;
    userSessions[uniqueKey].messaging = true;
    
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
    saveSessions();

    delete messageQueues[uniqueKey];
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

// ============================================
// SESSION STATUS
// ============================================
app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session = userSessions[uniqueKey];
  const status = sessionStatus[uniqueKey];
  const stats = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  let uptimeSeconds = 0;
  if (status?.connected && status?.connectionStartTime) {
    uptimeSeconds = Math.floor((Date.now() - status.connectionStartTime) / 1000);
  }

  res.json({
    exists: true,
    connected: !!activeSockets[uniqueKey] && status?.connected,
    messaging: session.messaging && !stopFlags[uniqueKey]?.stopped,
    sent: stats.sent || 0,
    failed: stats.failed || 0,
    lastMessage: stats.lastMessage || '-',
    target: session.target,
    speed: session.speed,
    messageCount: session.messages?.length || 0,
    status: status?.status || 'unknown',
    lastConnectedAt: status?.lastConnectedAt || null,
    lastMessageAt: status?.lastMessageAt || null,
    lastErrorAt: status?.lastErrorAt || null,
    reconnectCount: status?.reconnectCount || 0,
    uptimeSeconds: uptimeSeconds,
    totalSent: status?.totalSent || 0,
    totalFailed: status?.totalFailed || 0,
    errorMessage: status?.errorMessage || null,
    loginMethod: session.loginMethod || 'pairing',
    pairingCode: pairingCodes[uniqueKey] || null,
  });
});

// ============================================
// ALL SESSIONS STATUS
// ============================================
app.get('/sessionStatus/all', (req, res) => {
  const sessions = {};
  for (const [key, session] of Object.entries(userSessions)) {
    const status = sessionStatus[key];
    const stats = sessionStats[key] || { sent: 0, failed: 0, lastMessage: '' };
    
    let uptimeSeconds = 0;
    if (status?.connected && status?.connectionStartTime) {
      uptimeSeconds = Math.floor((Date.now() - status.connectionStartTime) / 1000);
    }
    
    sessions[key] = {
      exists: true,
      connected: !!activeSockets[key] && status?.connected,
      messaging: session.messaging && !stopFlags[key]?.stopped,
      sent: stats.sent || 0,
      failed: stats.failed || 0,
      lastMessage: stats.lastMessage || '-',
      target: session.target,
      speed: session.speed,
      messageCount: session.messages?.length || 0,
      phoneNumber: session.phoneNumber || 'Unknown',
      status: status?.status || 'unknown',
      lastConnectedAt: status?.lastConnectedAt || null,
      lastMessageAt: status?.lastMessageAt || null,
      lastErrorAt: status?.lastErrorAt || null,
      reconnectCount: status?.reconnectCount || 0,
      uptimeSeconds: uptimeSeconds,
      totalSent: status?.totalSent || 0,
      totalFailed: status?.totalFailed || 0,
      errorMessage: status?.errorMessage || null,
      loginMethod: session.loginMethod || 'pairing',
      pairingCode: pairingCodes[key] || null,
    };
  }
  res.json(sessions);
});

// ============================================
// STOP SESSION
// ============================================
app.post('/stop', async (req, res) => {
  const { uniqueKey } = req.body;
  if (!uniqueKey) return res.status(400).json({ success: false, message: 'Missing uniqueKey' });
  if (!userSessions[uniqueKey]) return res.status(400).json({ success: false, message: 'No session found' });

  try {
    cleanupSessionResources(uniqueKey);
    
    if (sessionStatus[uniqueKey]) {
      sessionStatus[uniqueKey].status = 'stopped';
      sessionStatus[uniqueKey].connected = false;
    }
    
    const phoneNumber = userSessions[uniqueKey]?.phoneNumber;
    delete userSessions[uniqueKey];
    delete pairingCodes[uniqueKey];
    delete activePairingRequests[phoneNumber];
    saveSessions();

    logger.info(`✅ Stopped & logged out: ${uniqueKey}`);
    res.json({ success: true, message: 'Process stopped and logged out!' });
  } catch (error) {
    res.status(500).json({ success: false, message: 'Error stopping process' });
  }
});

// ============================================
// HEALTH ENDPOINT
// ============================================
app.get('/health', (req, res) => {
  const totalSessions = Object.keys(userSessions).length;
  const activeSocketsCount = Object.keys(activeSockets).filter(
    key => activeSockets[key]
  ).length;
  const connectedSessions = Object.values(sessionStatus).filter(
    s => s?.connected
  ).length;
  
  const uptime = process.uptime();
  const uptimeDays = Math.floor(uptime / 86400);
  const uptimeHours = Math.floor((uptime % 86400) / 3600);
  const uptimeMinutes = Math.floor((uptime % 3600) / 60);
  
  res.json({
    status: 'online',
    timestamp: Date.now(),
    uptime: {
      seconds: Math.floor(uptime),
      formatted: `${uptimeDays}d ${uptimeHours}h ${uptimeMinutes}m`,
      startTime: new Date(Date.now() - uptime * 1000).toISOString()
    },
    sessions: {
      total: totalSessions,
      connected: connectedSessions,
      activeSockets: activeSocketsCount,
    },
    memory: process.memoryUsage(),
    nodeVersion: process.version,
    platform: process.platform,
  });
});

// ============================================
// ROOT
// ============================================
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================
// RESTORE SESSIONS
// ============================================
const restoreSessions = async () => {
  const loaded = loadSessions();
  
  if (!loaded) {
    logger.info('📂 No saved sessions found');
    return;
  }

  for (const [key, session] of Object.entries(userSessions)) {
    if (session.phoneNumber && session.uniqueKey) {
      const sessionPath = `./session/${session.uniqueKey}`;
      const credsPath = path.join(sessionPath, 'creds.json');
      
      if (!sessionStatus[session.uniqueKey]) {
        sessionStatus[session.uniqueKey] = getDefaultStatus(session.uniqueKey);
      }
      
      if (fs.existsSync(credsPath)) {
        logger.info(`🔄 Restoring: ${session.uniqueKey}`);
        stopFlags[session.uniqueKey] = { stopped: false };
        userSessions[session.uniqueKey].neverExpire = true;

        if (session.messaging && session.messages) {
          messageQueues[session.uniqueKey] = {
            messages: [...session.messages],
            currentIndex: 0,
            isSending: false,
            startTime: Date.now()
          };
        }

        // Auto-connect with saved creds
        setTimeout(async () => {
          try {
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
              shouldSyncHistoryMessage: () => false,
              fireInitQueries: true,
              syncFullHistory: false,
            });

            activeSockets[session.uniqueKey] = MznKing;

            MznKing.ev.on('connection.update', async (update) => {
              const { connection, lastDisconnect } = update;

              if (connection === 'open') {
                logger.info(`✅ Restored connection! [${session.uniqueKey}]`);
                updateSessionStatus(session.uniqueKey, {
                  connected: true,
                  status: 'connected',
                  connectionStartTime: Date.now(),
                  lastConnectedAt: Date.now(),
                });
                userSessions[session.uniqueKey].connected = true;
                userSessions[session.uniqueKey].lastUpdateTimestamp = Date.now();
                saveSessions();

                if (userSessions[session.uniqueKey]?.messaging && userSessions[session.uniqueKey]?.messages) {
                  const { target, hatersName, messages, speed } = userSessions[session.uniqueKey];
                  startMessaging(MznKing, session.uniqueKey, target, hatersName, messages, speed);
                }
              }

              if (connection === 'close') {
                const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
                if (reason === DisconnectReason.badSession || 
                    reason === DisconnectReason.loggedOut || 
                    reason === 401) {
                  logger.error(`❌ Invalid session for ${session.uniqueKey}`);
                  cleanupSessionResources(session.uniqueKey);
                  return;
                }
                if (!stopFlags[session.uniqueKey]?.stopped) {
                  setTimeout(() => restoreSessions(), 5000);
                }
              }
            });

            MznKing.ev.on('creds.update', saveCreds);

          } catch (error) {
            logger.error(`Restore connect error: ${error.message}`);
          }
        }, 1000);
      }
    }
  }
};

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
const gracefulShutdown = async (signal) => {
  logger.info(`\n🛑 Received ${signal}, initiating graceful shutdown...`);
  
  saveSessions();
  
  for (const uniqueKey of Object.keys(userSessions)) {
    cleanupSessionResources(uniqueKey);
  }
  
  setTimeout(() => {
    logger.info('✅ Graceful shutdown complete');
    process.exit(0);
  }, 5000);
};

process.on('uncaughtException', (error) => {
  logger.error('💥 Uncaught Exception:', error);
  logger.error(error.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('💥 Unhandled Rejection:', reason);
});

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ============================================
// START SERVER
// ============================================
const server = app.listen(PORT, '0.0.0.0', async () => {
  logger.info(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  logger.info(`✅ WhatsApp Pairing System running on port ${PORT}`);
  logger.info(`🌐 http://localhost:${PORT}`);
  logger.info(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
  
  await restoreSessions();
});

export { app, server };
