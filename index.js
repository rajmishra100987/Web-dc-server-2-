// ============================================================
// 🚀 24/7 HEADLESS MESSENGER BOT — PRODUCTION GRADE
// - File-based config (cookies.txt, convo.txt, messages.txt, etc.)
// - Timeout protection (no infinite hang)
// - Exponential backoff (FB rate-limit safe)
// - Cookie dead → auto-exit for Railway/Render restart
// - Interruptible sleep (fast graceful shutdown)
// - Zero terminal log spam (optional via DEBUG=1)
// ============================================================

global.WebSocket = require('ws'); // Railway & Node WebSocket fix

const express = require('express');
const { MessengerClient, Platform, CookieManager } = require('messagix-js');
const { setTimeout: sleep } = require('timers/promises');
const fs = require('fs');

// ============================================================
// 🔇 LOG SILENCER (optional — set DEBUG=1 to enable logs)
// ============================================================
const DEBUG = process.env.DEBUG === '1';
if (!DEBUG) {
    const noop = () => {};
    console.log = noop;
    console.info = noop;
    console.debug = noop;
    console.warn = noop;
    console.error = noop;
}

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================
// CONSTANTS
// ============================================================
const CONNECT_TIMEOUT_MS = 30000;   // 30 sec
const SEND_TIMEOUT_MS = 15000;      // 15 sec
const MAX_CONSECUTIVE_FAILS = 3;    // send fails → reauth
const MAX_REAUTH_ATTEMPTS = 5;      // reauth fails → exit
const BACKOFF_BASE_MS = 15000;      // 15 sec
const BACKOFF_MAX_MS = 300000;      // 5 min cap
const CONFIG_ERROR_WAIT_MS = 10000; // 10 sec when files missing

// ============================================================
// STATE
// ============================================================
let activeClient = null;
let isShuttingDown = false;
let reauthAttempts = 0;
let lastSendAt = null;
let lastLoopAt = null;
let totalSent = 0;
let totalReauths = 0;

// ============================================================
// ANTI-CRASH
// ============================================================
process.on('uncaughtException', (err) => {
    if (DEBUG) console.error('[ANTI-CRASH] Uncaught:', err.message);
});
process.on('unhandledRejection', (reason) => {
    if (DEBUG) console.error('[ANTI-CRASH] Rejection:', reason?.message || reason);
});

// ============================================================
// GRACEFUL SHUTDOWN (interruptible sleep ke wajah se fast)
// ============================================================
async function gracefulShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    if (DEBUG) console.log(`[SHUTDOWN] ${signal} received. Cleaning up...`);
    try {
        if (activeClient) await activeClient.disconnect();
    } catch (_) {}
    process.exit(0);
}
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ============================================================
// HELPERS
// ============================================================
function readTextFile(filename, defaultValue = '') {
    try {
        if (fs.existsSync(filename)) {
            return fs.readFileSync(filename, 'utf8').trim();
        }
    } catch (e) {
        if (DEBUG) console.error(`Error reading ${filename}:`, e.message);
    }
    return defaultValue;
}

function isConnectionError(err) {
    if (!err || !err.message) return true;
    return /auth|login|connect|socket|closed|timeout|expired|token|network|ECONN|ETIMEDOUT|ENOTFOUND|EPIPE|spawn|disconnected/i
        .test(err.message);
}

async function safeDisconnect(client) {
    if (!client) return;
    try {
        await client.disconnect();
    } catch (_) { /* silent */ }
}

// ⏱️ Timeout wrapper — infinite hang prevent
function withTimeout(promise, ms, label) {
    let timer;
    const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(
            () => reject(new Error(`${label} timeout after ${ms}ms`)),
            ms
        );
    });
    return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
}

// 💤 Interruptible sleep — shutdown signal turant catch kare
async function interruptibleSleep(ms) {
    const step = 1000;
    for (let elapsed = 0; elapsed < ms; elapsed += step) {
        if (isShuttingDown) return;
        await sleep(Math.min(step, ms - elapsed));
    }
}

// 📈 Exponential backoff with cap
function backoffMs(attempt) {
    return Math.min(BACKOFF_BASE_MS * Math.pow(2, attempt - 1), BACKOFF_MAX_MS);
}

// ============================================================
// KEEP-ALIVE & HEALTH ROUTES
// ============================================================
app.head('/', (req, res) => res.status(200).end());
app.get('/', (req, res) => res.send('raj mishra dc server running ✅️'));
app.get('/ping', (req, res) => res.status(200).send('Pong - Server is Active!'));
app.get('/health', (req, res) =>
    res.status(200).json({
        status: 'ONLINE',
        uptime: Math.round(process.uptime()),
        isShuttingDown,
        hasClient: !!activeClient,
        reauthAttempts,
        totalSent,
        totalReauths,
        lastSendAt: lastSendAt ? new Date(lastSendAt).toISOString() : null,
        lastLoopAt: lastLoopAt ? new Date(lastLoopAt).toISOString() : null,
    })
);

app.listen(PORT, '0.0.0.0', () => {
    if (DEBUG) console.log(`[SYSTEM LIVE] Headless server on port ${PORT}`);
    startInfiniteBot();
});

// ============================================================
// MAIN BOT LOOP
// ============================================================
async function startInfiniteBot() {
    if (DEBUG) console.log('[BOT] Starting Headless Infinite Loop Messenger Bot...');

    while (!isShuttingDown) {
        let client = null;
        try {
            lastLoopAt = Date.now();

            // ---- Read config files ----
            const cookies = readTextFile('cookies.txt');
            const threadId = readTextFile('convo.txt');
            const delaySec = parseInt(readTextFile('time.txt', '120'), 10) || 120;
            const hatersName = readTextFile('hatersname.txt');
            const messagesRaw = readTextFile('messages.txt', 'Hello');
            const messages = messagesRaw
                .split('\n')
                .map((m) => m.trim())
                .filter(Boolean);

            // ---- Validation ----
            if (!cookies || !threadId || messages.length === 0) {
                if (DEBUG) console.log('[CONFIG ERROR] Missing cookies.txt / convo.txt / messages.txt. Wait 10s...');
                await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                continue;
            }

            // ---- Auth ----
            if (DEBUG) console.log(`[AUTH] Authenticating for Thread ID: ${threadId}...`);
            const cookieManager = CookieManager.fromString(Platform.Messenger, cookies);
            client = new MessengerClient({
                platform: Platform.Messenger,
                cookies: cookieManager.getAll(),
                enableE2EE: false,
            });
            activeClient = client;

            // ⏱️ Timeout-protected connect
            await withTimeout(
                client.loadMessagesPage(),
                CONNECT_TIMEOUT_MS,
                'loadMessagesPage'
            );
            await withTimeout(
                client.connect(),
                CONNECT_TIMEOUT_MS,
                'connect'
            );

            // ---- Success: reset reauth counter ----
            const wasReauth = reauthAttempts > 0;
            reauthAttempts = 0;
            if (wasReauth) totalReauths++;
            if (DEBUG) console.log('[AUTH] ✅ Login Successful!');

            // ---- Inner loop variables ----
            let msgIndex = 0;
            let loopCount = 1;
            let consecutiveFails = 0;
            let needReauth = false;

            // ---- Inner Message Loop ----
            while (!isShuttingDown && !needReauth) {
                lastLoopAt = Date.now();

                // ---- Dynamic re-read files ----
                const currentDelaySec =
                    parseInt(readTextFile('time.txt', String(delaySec)), 10) || 120;
                const currentHaters = readTextFile('hatersname.txt');
                const currentMsgsRaw = readTextFile('messages.txt');
                const currentMsgs = currentMsgsRaw
                    ? currentMsgsRaw.split('\n').map((m) => m.trim()).filter(Boolean)
                    : messages;

                if (currentMsgs.length === 0) {
                    if (DEBUG) console.log('[WARN] messages.txt empty. Wait 10s...');
                    await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                    continue;
                }

                if (msgIndex >= currentMsgs.length) {
                    msgIndex = 0;
                    loopCount++;
                    if (DEBUG) console.log(`[LOOP] 🔄 Round ${loopCount} starting...`);
                }

                const rawMsg = currentMsgs[msgIndex] || 'Hello';
                const finalMessage = currentHaters ? `${currentHaters} ${rawMsg}` : rawMsg;

                try {
                    // ⏱️ Timeout-protected send
                    await withTimeout(
                        client.sendMessage(threadId, finalMessage),
                        SEND_TIMEOUT_MS,
                        'sendMessage'
                    );
                    consecutiveFails = 0;
                    totalSent++;
                    lastSendAt = Date.now();
                    if (DEBUG) console.log(`[${new Date().toLocaleTimeString()}] 🚀 Sent: ${finalMessage}`);
                } catch (err) {
                    consecutiveFails++;
                    if (DEBUG) {
                        console.error(
                            `[${new Date().toLocaleTimeString()}] ⚠️ Send Error (${consecutiveFails}/${MAX_CONSECUTIVE_FAILS}): ${err.message}`
                        );
                    }

                    // Reauth trigger
                    if (isConnectionError(err) || consecutiveFails >= MAX_CONSECUTIVE_FAILS) {
                        if (DEBUG) console.log('[REAUTH] Connection dropped. Re-authenticating...');
                        needReauth = true;
                        break;
                    }
                }

                msgIndex++;

                // 💤 Interruptible sleep
                if (!isShuttingDown && !needReauth) {
                    await interruptibleSleep(currentDelaySec * 1000);
                }
            }
        } catch (err) {
            // ---- Fatal error: exponential backoff ----
            reauthAttempts++;

            if (reauthAttempts >= MAX_REAUTH_ATTEMPTS) {
                if (DEBUG) {
                    console.error(
                        `❌ [FATAL] Cookie invalid/expired or account blocked ` +
                        `(${MAX_REAUTH_ATTEMPTS} attempts failed). ` +
                        `Update cookies.txt & restart bot.`
                    );
                }
                // Cleanup + exit → Railway/Render will restart (or user fixes cookies)
                await safeDisconnect(client);
                if (activeClient === client) activeClient = null;
                isShuttingDown = true;
                process.exit(1);
            }

            const wait = backoffMs(reauthAttempts);
            if (DEBUG) {
                console.error(
                    `[FATAL] Attempt ${reauthAttempts}/${MAX_REAUTH_ATTEMPTS}: ${err.message}. ` +
                    `Retry in ${Math.round(wait / 1000)}s...`
                );
            }
            await interruptibleSleep(wait);
        } finally {
            await safeDisconnect(client);
            if (activeClient === client) activeClient = null;
            client = null;
        }
    }

    if (DEBUG) console.log('[BOT] Shutdown complete. Loop exited cleanly.');
                }
