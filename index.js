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

// ============================================
// CONFIGURATION
// ============================================
const CONFIG = {
  HEALTH_CHECK_INTERVAL: 60 * 1000, // 60 seconds
  MAX_RECONNECT_DELAY: 60000, // 60 seconds max
  INITIAL_RECONNECT_DELAY: 3000, // 3 seconds
  SESSION_FILE: './running_sessions.json',
  MAX_RECONNECT_ATTEMPTS: 999999, // Practically infinite
  CLEANUP_INTERVAL: 5 * 60 * 1000, // 5 minutes
  GRACEFUL_SHUTDOWN_TIMEOUT: 10000, // 10 seconds
};

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

// ============================================
// MULTER SETUP
// ============================================
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }
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
const healthCheckInterval = null;
const cleanupInterval = null;

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
  status: 'idle', // idle | connecting | connected | reconnecting | error | stopped
  errorMessage: null,
});

const updateSessionStatus = (uniqueKey, updates) => {
  if (!sessionStatus[uniqueKey]) {
    sessionStatus[uniqueKey] = getDefaultStatus(uniqueKey);
  }
  Object.assign(sessionStatus[uniqueKey], updates);
  
  // Calculate uptime if connected
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
    fs.writeFileSync(CONFIG.SESSION_FILE, JSON.stringify(data, null, 2), 'utf8');
  } catch (error) {
    logger.error(`Error saving sessions: ${error.message}`);
  }
};

const loadSessions = () => {
  try {
    if (fs.existsSync(CONFIG.SESSION_FILE)) {
      const data = JSON.parse(fs.readFileSync(CONFIG.SESSION_FILE, 'utf8'));
      Object.assign(userSessions, data.userSessions || {});
      Object.assign(sessionStatus, data.sessionStatus || {});
      logger.info(`📂 Loaded ${Object.keys(userSessions).length} sessions from persistence`);
      return true;
    }
  } catch (error) {
    logger.error(`Error loading sessions: ${error.message}`);
  }
  return false;
};

// ============================================
// CLEANUP HELPERS
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
  
  // Close socket
  if (activeSockets[uniqueKey]) {
    try {
      activeSockets[uniqueKey].end();
    } catch (e) {}
    try {
      activeSockets[uniqueKey].logout();
    } catch (e) {}
    delete activeSockets[uniqueKey];
  }
  
  // Update status
  if (sessionStatus[uniqueKey]) {
    sessionStatus[uniqueKey].connected = false;
    sessionStatus[uniqueKey].status = 'stopped';
    saveSessions();
  }
  
  logger.info(`🧹 Cleaned up resources for session: ${uniqueKey}`);
};

// ============================================
// GENERATE UNIQUE KEY
// ============================================
const generateUniqueKey = () => {
  return crypto.randomBytes(16).toString('hex');
};

// ============================================
// MESSAGING ENGINE
// ============================================
const startMessaging = (MznKing, uniqueKey, target, hatersName, messages, speed) => {
  // Clear existing interval
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
    // Check if stopped
    if (stopFlags[uniqueKey]?.stopped) {
      clearInterval(stopFlags[uniqueKey].interval);
      delete messageQueues[uniqueKey];
      return;
    }

    // Check socket
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
      
      // Update stats
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
      
      logger.info(`✉️ [${sessionStats[uniqueKey]?.sent || 0}] Sent to ${chatId}: ${formattedMessage.substring(0, 40)}...`);

      queue.currentIndex++;
      if (queue.currentIndex >= queue.messages.length) {
        logger.info(`🔄 All messages sent! Restarting from beginning...`);
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

  // Send first message immediately
  setTimeout(sendNextMessage, 100);
};

// ============================================
// SESSION STATS (for backward compatibility)
// ============================================
const sessionStats = {};

const getSessionStats = (uniqueKey) => {
  if (!sessionStats[uniqueKey]) {
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
  }
  return sessionStats[uniqueKey];
};

// ============================================
// WATCHDOG HEALTH CHECK
// ============================================
const performHealthCheck = () => {
  const now = Date.now();
  
  for (const [uniqueKey, session] of Object.entries(userSessions)) {
    const status = sessionStatus[uniqueKey];
    const socket = activeSockets[uniqueKey];
    
    if (!status) continue;
    
    // Check if session should be connected but isn't
    if (session.messaging && !stopFlags[uniqueKey]?.stopped) {
      if (!socket || !status.connected) {
        logger.warn(`⚠️ Health check: Session ${uniqueKey} disconnected, attempting recovery...`);
        
        // Attempt recovery if not already reconnecting
        if (status.status !== 'reconnecting') {
          status.status = 'reconnecting';
          status.reconnectCount = (status.reconnectCount || 0) + 1;
          saveSessions();
          
          // Try to reconnect
          connectWithCredsOnly(session.phoneNumber, uniqueKey, null).catch(err => {
            logger.error(`Health check reconnect failed for ${uniqueKey}: ${err.message}`);
          });
        }
      } else {
        // Update uptime
        if (status.connectionStartTime) {
          status.uptimeSeconds = Math.floor((now - status.connectionStartTime) / 1000);
        }
        
        // Check if socket is stale (no messages in last 5 minutes)
        if (status.lastMessageAt && (now - status.lastMessageAt) > 300000) {
          // Stale but still connected, just log
          logger.debug(`ℹ️ Session ${uniqueKey} connected but idle for 5+ minutes`);
        }
      }
    }
  }
};

// ============================================
// CLEANUP STALE RESOURCES
// ============================================
const cleanupStaleResources = () => {
  const now = Date.now();
  
  for (const [uniqueKey, status] of Object.entries(sessionStatus)) {
    // Cleanup sessions that are stopped but have resources
    if (status.status === 'stopped') {
      if (activeSockets[uniqueKey] || stopFlags[uniqueKey]?.interval) {
        logger.info(`🧹 Cleaning up stopped session resources: ${uniqueKey}`);
        cleanupSessionResources(uniqueKey);
      }
    }
    
    // Cleanup sessions with no activity for 7 days and stopped
    if (status.status === 'stopped' && status.lastConnectedAt) {
      const inactiveDays = (now - status.lastConnectedAt) / (24 * 60 * 60 * 1000);
      if (inactiveDays > 7) {
        logger.info(`🧹 Removing stale stopped session: ${uniqueKey} (inactive ${Math.round(inactiveDays)} days)`);
        delete sessionStatus[uniqueKey];
        delete userSessions[uniqueKey];
        cleanupSessionResources(uniqueKey);
        saveSessions();
      }
    }
  }
};

// ============================================
// CONNECT WITH CREDS.JSON ONLY
// ============================================
const connectWithCredsOnly = async (phoneNumber, uniqueKey, sendResponse) => {
  const sessionPath = `./session/${uniqueKey}`;
  let connectionAttempts = 0;
  const MAX_RECONNECT_DELAY = CONFIG.MAX_RECONNECT_DELAY;

  // Update status
  updateSessionStatus(uniqueKey, {
    status: 'connecting',
    reconnectCount: (sessionStatus[uniqueKey]?.reconnectCount || 0) + 1,
    lastErrorAt: null,
    errorMessage: null,
  });

  const startConnection = async () => {
    try {
      let displayPhone = phoneNumber || 'Unknown';
      
      logger.info(`🚀 Connecting [${uniqueKey}] with creds.json (attempt ${connectionAttempts + 1})`);

      const credsPath = path.join(sessionPath, 'creds.json');
      if (!fs.existsSync(credsPath)) {
        logger.error(`❌ creds.json not found for ${uniqueKey}`);
        updateSessionStatus(uniqueKey, {
          status: 'error',
          errorMessage: 'creds.json not found',
          lastErrorAt: Date.now(),
        });
        if (sendResponse) sendResponse('creds.json not found! Please upload valid session file.');
        return;
      }

      const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion();

      if (!state.creds || !state.creds.registered) {
        logger.error(`❌ Invalid creds for ${uniqueKey}`);
        updateSessionStatus(uniqueKey, {
          status: 'error',
          errorMessage: 'Invalid creds',
          lastErrorAt: Date.now(),
        });
        if (sendResponse) sendResponse('Invalid creds! Please upload a valid WhatsApp session.');
        return;
      }

      // Extract phone from creds
      try {
        if (state.creds.me && state.creds.me.id) {
          const id = state.creds.me.id;
          displayPhone = id.includes(':') ? id.split(':')[0] : id;
        }
      } catch (e) {}

      // Clean up any existing socket for this session
      if (activeSockets[uniqueKey]) {
        try {
          activeSockets[uniqueKey].end();
        } catch (e) {}
        delete activeSockets[uniqueKey];
      }

      // Create socket with proper configuration for long-running operation
      const MznKing = makeWASocket({
        version,
        logger: pino.default({ 
          level: 'warn',
          transport: {
            target: 'pino-pretty',
            options: { colorize: true, translateTime: 'SYS:standard', ignore: 'pid,hostname' }
          }
        }),
        browser: Browsers.windows('Firefox'),
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, pino.default({ level: 'warn' }))
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
        patchMessageBeforeSending: (message) => message,
      });

      activeSockets[uniqueKey] = MznKing;

      // Send success response if this is initial connection
      if (state.creds.registered && sendResponse) {
        logger.info(`✅ Session valid for ${uniqueKey}`);
        sendResponse(null);
      }

      // ============================================
      // CONNECTION UPDATE HANDLER
      // ============================================
      MznKing.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;

        if (connection === 'open') {
          logger.info(`✅✅✅ Connected! [${uniqueKey}]`);
          connectionAttempts = 0;
          
          updateSessionStatus(uniqueKey, {
            connected: true,
            status: 'connected',
            connectionStartTime: Date.now(),
            lastConnectedAt: Date.now(),
            lastErrorAt: null,
            errorMessage: null,
            uptimeSeconds: 0,
          });

          // Update user session
          if (userSessions[uniqueKey]) {
            userSessions[uniqueKey].connected = true;
            userSessions[uniqueKey].lastUpdateTimestamp = Date.now();
            userSessions[uniqueKey].neverExpire = true;
          }
          saveSessions();

          // Resume messaging if it was active
          if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
            const { target, hatersName, messages, speed } = userSessions[uniqueKey];
            logger.info(`🔄 Resuming messaging for ${uniqueKey}...`);
            if (!messageQueues[uniqueKey]) {
              messageQueues[uniqueKey] = {
                messages: [...messages],
                currentIndex: 0,
                isSending: false,
                startTime: Date.now()
              };
            }
            startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
          }
        }

        if (connection === 'close') {
          const statusCode = lastDisconnect?.error?.output?.statusCode;
          const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
          
          logger.warn(`⚠️ Connection closed - Status: ${statusCode}, Reason: ${reason}`);

          updateSessionStatus(uniqueKey, {
            connected: false,
            status: 'reconnecting',
            lastErrorAt: Date.now(),
            errorMessage: `Disconnected: ${reason || 'Unknown reason'}`,
          });

          // Handle different disconnect reasons
          if (reason === DisconnectReason.badSession || 
              reason === DisconnectReason.loggedOut || 
              reason === 401) {
            logger.error(`❌ Invalid session for ${uniqueKey}, will need re-upload of creds.json`);
            updateSessionStatus(uniqueKey, {
              status: 'error',
              errorMessage: 'Invalid session - re-upload creds.json required',
            });
            userSessions[uniqueKey].connected = false;
            userSessions[uniqueKey].messaging = false;
            saveSessions();
            
            // Cleanup resources
            cleanupSessionResources(uniqueKey);
            return;
          }

          // Reconnect with exponential backoff - INFINITE RETRY
          if (!stopFlags[uniqueKey]?.stopped) {
            connectionAttempts++;
            const delay = Math.min(
              CONFIG.INITIAL_RECONNECT_DELAY * Math.pow(1.5, connectionAttempts - 1),
              MAX_RECONNECT_DELAY
            );
            
            logger.info(`🔄 Reconnecting in ${Math.round(delay/1000)}s... (Attempt ${connectionAttempts})`);
            
            // Update reconnect count
            updateSessionStatus(uniqueKey, {
              reconnectCount: connectionAttempts,
              status: 'reconnecting',
            });
            
            setTimeout(() => startConnection(), delay);
          }
        }
      });

      // ============================================
      // CREDS UPDATE HANDLER
      // ============================================
      MznKing.ev.on('creds.update', () => {
        saveCreds();
        logger.debug(`🔑 Creds updated for ${uniqueKey}`);
      });

      // ============================================
      // MESSAGES UPSERT HANDLER
      // ============================================
      MznKing.ev.on('messages.upsert', async (m) => {
        // Just for presence, no action needed
      });

    } catch (error) {
      logger.error(`❌ ERROR: ${error.message}`);
      updateSessionStatus(uniqueKey, {
        status: 'error',
        errorMessage: error.message,
        lastErrorAt: Date.now(),
      });
      
      if (sendResponse) sendResponse(error.message);
      
      // Retry with backoff
      if (!stopFlags[uniqueKey]?.stopped) {
        connectionAttempts++;
        const delay = Math.min(
          CONFIG.INITIAL_RECONNECT_DELAY * Math.pow(1.5, connectionAttempts - 1),
          MAX_RECONNECT_DELAY
        );
        logger.info(`🔄 Retrying in ${Math.round(delay/1000)}s... (Attempt ${connectionAttempts})`);
        setTimeout(() => startConnection(), delay);
      }
    }
  };

  await startConnection();
};

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
      
      // Initialize status if not exists
      if (!sessionStatus[session.uniqueKey]) {
        sessionStatus[session.uniqueKey] = getDefaultStatus(session.uniqueKey);
      }
      
      // Only restore if creds.json exists
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

        // Attempt connection
        setTimeout(() => {
          connectWithCredsOnly(session.phoneNumber, session.uniqueKey, null).catch(err => {
            logger.error(`Restore failed for ${session.uniqueKey}: ${err.message}`);
          });
        }, 1000);
      } else {
        logger.warn(`⚠️ creds.json missing for ${session.uniqueKey}, skipping...`);
        sessionStatus[session.uniqueKey].status = 'error';
        sessionStatus[session.uniqueKey].errorMessage = 'creds.json missing';
      }
    }
  }
  saveSessions();
};

// ============================================
// 🔥 PAIRING CODE LOGIN (NEW)
// ============================================
app.post('/login', upload.none(), async (req, res) => {
  try {
    let { phoneNumber } = req.body;
    if (!phoneNumber) {
      return res.status(400).json({ success: false, message: 'Phone number is required!' });
    }

    phoneNumber = phoneNumber.replace(/[^0-9]/g, '');
    logger.info(`📞 Login request for: ${phoneNumber}`);

    const uniqueKey = generateUniqueKey();
    const sessionPath = `./session/${uniqueKey}`;

    if (!fs.existsSync(sessionPath)) {
      fs.mkdirSync(sessionPath, { recursive: true });
    }

    // Initialize session
    userSessions[uniqueKey] = {
      phoneNumber,
      uniqueKey,
      connected: false,
      messaging: false,
      lastUpdateTimestamp: Date.now(),
      neverExpire: true,
      loginMethod: 'pairing'
    };

    sessionStatus[uniqueKey] = getDefaultStatus(uniqueKey);
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
    stopFlags[uniqueKey] = { stopped: false };
    saveSessions();

    let pairingCodeSent = false;

    const startPairing = async () => {
      try {
        logger.info(`🔐 Starting pairing for ${phoneNumber}`);

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

        activeSockets[uniqueKey] = MznKing;

        // Request pairing code
        if (!pairingCodeSent) {
          try {
            const code = await MznKing.requestPairingCode(phoneNumber);
            const pairingCode = code?.match(/.{1,4}/g)?.join('-') || code;
            pairingCodeSent = true;
            
            logger.info(`📱 Pairing Code for ${phoneNumber}: ${pairingCode}`);
            
            res.json({
              success: true,
              message: 'Pairing code generated',
              pairingCode,
              uniqueKey
            });
          } catch (error) {
            logger.error(`Pairing error: ${error.message}`);
            pairingCodeSent = true;
            res.json({
              success: false,
              message: 'Error generating pairing code',
              error: error.message,
              uniqueKey
            });
          }
        }

        // Connection update handler
        MznKing.ev.on('connection.update', async (update) => {
          const { connection, lastDisconnect } = update;

          if (connection === 'open') {
            logger.info(`✅ Connected! [${uniqueKey}]`);
            
            updateSessionStatus(uniqueKey, {
              connected: true,
              status: 'connected',
              connectionStartTime: Date.now(),
              lastConnectedAt: Date.now(),
            });

            userSessions[uniqueKey].connected = true;
            userSessions[uniqueKey].lastUpdateTimestamp = Date.now();
            saveSessions();

            // Resume messaging if any
            if (userSessions[uniqueKey]?.messaging && userSessions[uniqueKey]?.messages) {
              const { target, hatersName, messages, speed } = userSessions[uniqueKey];
              startMessaging(MznKing, uniqueKey, target, hatersName, messages, speed);
            }
          }

          if (connection === 'close') {
            const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
            
            if (reason === DisconnectReason.badSession || 
                reason === DisconnectReason.loggedOut || 
                reason === 401) {
              logger.error(`Invalid session for ${uniqueKey}`);
              cleanupSessionResources(uniqueKey);
              return;
            }

            // Auto-reconnect
            if (!stopFlags[uniqueKey]?.stopped) {
              setTimeout(startPairing, 5000);
            }
          }
        });

        MznKing.ev.on('creds.update', saveCreds);

      } catch (error) {
        logger.error(`Pairing error: ${error.message}`);
        if (!pairingCodeSent) {
          pairingCodeSent = true;
          res.json({
            success: false,
            message: 'Error starting pairing',
            error: error.message,
            uniqueKey
          });
        }
      }
    };

    await startPairing();

  } catch (error) {
    res.status(500).json({ success: false, message: `Server Error: ${error.message}` });
  }
});

// ============================================
// API ROUTES
// ============================================

// ============================================
// LOGIN WITH CREDS.JSON
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

    logger.info(`📞 Login with creds.json`);

    const uniqueKey = generateUniqueKey();
    const sessionPath = `./session/${uniqueKey}`;

    if (!fs.existsSync(sessionPath)) {
      fs.mkdirSync(sessionPath, { recursive: true });
    }

    // Save creds.json
    const credsDestPath = path.join(sessionPath, 'creds.json');
    fs.writeFileSync(credsDestPath, credsFile.buffer);

    // Validate
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

    // Initialize session data
    userSessions[uniqueKey] = {
      phoneNumber: null,
      uniqueKey,
      connected: false,
      messaging: false,
      lastUpdateTimestamp: Date.now(),
      neverExpire: true,
      loginMethod: 'creds'
    };
    
    sessionStatus[uniqueKey] = getDefaultStatus(uniqueKey);
    sessionStats[uniqueKey] = { sent: 0, failed: 0, lastMessage: '' };
    
    stopFlags[uniqueKey] = { stopped: false };
    
    saveSessions();

    // Connect
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

    // Reset message queue
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
// SESSION STATUS (Enhanced with live data)
// ============================================
app.get('/sessionStatus/:uniqueKey', (req, res) => {
  const { uniqueKey } = req.params;
  const session = userSessions[uniqueKey];
  const status = sessionStatus[uniqueKey];
  const stats = sessionStats[uniqueKey] || { sent: 0, failed: 0, lastMessage: '' };

  if (!session) return res.json({ exists: false });

  // Calculate current uptime
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
    loginMethod: session.loginMethod || 'unknown',
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
      loginMethod: session.loginMethod || 'unknown',
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
    
    delete userSessions[uniqueKey];
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
    watchdog: {
      enabled: true,
      checkInterval: CONFIG.HEALTH_CHECK_INTERVAL / 1000 + 's',
      cleanupInterval: CONFIG.CLEANUP_INTERVAL / 1000 + 's',
      maxReconnectDelay: CONFIG.MAX_RECONNECT_DELAY / 1000 + 's',
    }
  });
});

// ============================================
// ROOT
// ============================================
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================
const gracefulShutdown = async (signal) => {
  logger.info(`\n🛑 Received ${signal}, initiating graceful shutdown...`);
  
  // Stop health check
  if (healthCheckInterval) {
    clearInterval(healthCheckInterval);
  }
  
  // Stop cleanup
  if (cleanupInterval) {
    clearInterval(cleanupInterval);
  }
  
  // Save all sessions
  saveSessions();
  
  // Cleanup all sessions
  for (const uniqueKey of Object.keys(userSessions)) {
    cleanupSessionResources(uniqueKey);
  }
  
  // Close server
  setTimeout(() => {
    logger.info('✅ Graceful shutdown complete');
    process.exit(0);
  }, CONFIG.GRACEFUL_SHUTDOWN_TIMEOUT);
};

// ============================================
// UNCAUGHT EXCEPTION / REJECTION HANDLING
// ============================================
process.on('uncaughtException', (error) => {
  logger.error('💥 Uncaught Exception:', error);
  // Don't exit, let the process continue
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('💥 Unhandled Rejection:', reason);
});

// ============================================
// SIGNAL HANDLERS
// ============================================
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ============================================
// START SERVER
// ============================================
const server = app.listen(PORT, '0.0.0.0', async () => {
  logger.info(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  logger.info(`✅ Server running on port ${PORT}`);
  logger.info(`🌐 http://localhost:${PORT}`);
  logger.info(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);
  
  // Load and restore sessions
  await restoreSessions();
  
  // Start health check
  logger.info(`🔄 Starting watchdog system (check every ${CONFIG.HEALTH_CHECK_INTERVAL/1000}s)`);
  setInterval(performHealthCheck, CONFIG.HEALTH_CHECK_INTERVAL);
  
  // Start cleanup
  setInterval(cleanupStaleResources, CONFIG.CLEANUP_INTERVAL);
  
  logger.info(`✅ Watchdog system initialized`);
  logger.info(`✅ 60-Day Long-Running Mode Active`);
});

// ============================================
// EXPORT FOR TESTING
// ============================================
export { app, server };
