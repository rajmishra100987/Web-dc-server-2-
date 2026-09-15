global.WebSocket = require('ws');

const express = require('express');
const { MessengerClient, Platform, CookieManager } = require('messagix-js');
const crypto = require('crypto');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_FILE = './tasks.json';

// Optional JSONBin persistence
const JSONBIN_BIN_ID = process.env.JSONBIN_BIN_ID || '';
const JSONBIN_API_KEY = process.env.JSONBIN_API_KEY || '';

// ==================== CONSTANTS ====================
const CONNECT_TIMEOUT_MS = 25000;         // 25s connect timeout
const SEND_TIMEOUT_MS = 15000;            // 15s send timeout
const PRIMARY_RETRIES = 3;
const BACKUP_RETRIES = 2;
const MAX_CONNECT_FAILS = 10;             // after this → status = error
const BACKOFF_STEPS_MS = [60000, 120000, 240000, 300000]; // 1m → 2m → 4m → 5m cap
const SEND_RETRY_BACKOFF_MS = [3000, 6000, 12000, 30000, 60000];
const SAVE_MIN_INTERVAL_MS = 30000;       // min 30s between disk/cloud saves
const SAVE_DEBOUNCE_MS = 5000;
const MAX_LOG_LINES = 60;
const RESTORE_STAGGER_MS = 4000;          // 4s between task restores

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// ==================== STATE ====================
const activeTasks = new Map();

let isShuttingDown = false;
let saveTimer = null;
let saveInProgress = false;
let lastSaveAt = 0;
let saveDirty = false;

// ==================== ANTI-CRASH ====================
process.on('uncaughtException', (err) => console.error('[ANTI-CRASH]', err.message));
process.on('unhandledRejection', (r) => console.error('[ANTI-CRASH]', r?.message || r));

// ==================== HELPERS ====================
function getUptimeString(startTime) {
    const diff = Date.now() - startTime;
    const d = Math.floor(diff / 86400000);
    const h = Math.floor((diff % 86400000) / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    return `${d} Days, ${h} Hours, ${m} Mins`;
}

function pushLog(task, msg) {
    task.logs.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
    if (task.logs.length > MAX_LOG_LINES) {
        task.logs.splice(0, task.logs.length - MAX_LOG_LINES);
    }
}

function withTimeout(promise, ms, label) {
    let timer;
    const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timeout ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timer));
}

async function interruptibleSleep(ms) {
    const step = 1000;
    for (let elapsed = 0; elapsed < ms; elapsed += step) {
        if (isShuttingDown) return false;
        await new Promise(r => setTimeout(r, Math.min(step, ms - elapsed)));
    }
    return true;
}

function backoffFor(failCount) {
    if (failCount <= 0) return BACKOFF_STEPS_MS[0];
    const idx = Math.min(failCount - 1, BACKOFF_STEPS_MS.length - 1);
    return BACKOFF_STEPS_MS[idx];
}

// ==================== SAVE (DEBOUNCED) ====================
function scheduleSave(force = false) {
    saveDirty = true;

    if (force) {
        if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
        return doSave();
    }

    if (saveTimer) return; // already scheduled

    const wait = Math.max(SAVE_DEBOUNCE_MS, SAVE_MIN_INTERVAL_MS - (Date.now() - lastSaveAt));
    saveTimer = setTimeout(() => {
        saveTimer = null;
        doSave();
    }, wait);
}

async function doSave() {
    if (saveInProgress) { saveDirty = true; return; }
    saveInProgress = true;
    saveDirty = false;
    lastSaveAt = Date.now();

    const out = {};
    for (const [id, t] of activeTasks.entries()) {
        out[id] = {
            cookies: t.cookies,
            backupCookies: t.backupCookies || '',
            threadId: t.threadId,
            hatersName: t.hatersName,
            messages: t.messages,
            delaySec: t.delaySec,
            status: t.status,
            startTime: t.startTime,
            logs: t.logs.slice(-MAX_LOG_LINES),
            activeCookieSource: t.activeCookieSource || 'primary'
        };
    }

    try {
        if (JSONBIN_BIN_ID && JSONBIN_API_KEY) {
            const res = await fetch(`https://api.jsonbin.io/v3/b/${JSONBIN_BIN_ID}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Master-Key': JSONBIN_API_KEY
                },
                body: JSON.stringify(out)
            });
            if (!res.ok) console.error(`[SAVE] JSONBin HTTP ${res.status}`);
        } else {
            fs.writeFileSync(DB_FILE, JSON.stringify(out, null, 2));
        }
    } catch (e) {
        console.error("[SAVE ERROR]", e.message);
    } finally {
        saveInProgress = false;
        if (saveDirty) scheduleSave(false);
    }
}

// ==================== RESTORE ====================
async function loadTasks() {
    try {
        let dataRecord = null;
        if (JSONBIN_BIN_ID && JSONBIN_API_KEY) {
            const res = await fetch(`https://api.jsonbin.io/v3/b/${JSONBIN_BIN_ID}/latest`, {
                headers: { 'X-Master-Key': JSONBIN_API_KEY }
            });
            const j = await res.json();
            dataRecord = j.record;
            console.log('[RESTORE] Loaded from JSONBin.');
        } else if (fs.existsSync(DB_FILE)) {
            dataRecord = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
            console.log('[RESTORE] Loaded from local file.');
        }

        if (!dataRecord) return;

        const toRestore = [];
        for (const [taskId, taskData] of Object.entries(dataRecord)) {
            if (taskData.status === 'running') {
                activeTasks.set(taskId, {
                    ...taskData,
                    client: null,
                    forceReconnect: false,
                    connectFailCount: 0,
                    sendFailCount: 0
                });
                toRestore.push(taskId);
            }
        }

        // Staggered restore — FB flag avoid
        for (let i = 0; i < toRestore.length; i++) {
            const id = toRestore[i];
            console.log(`[RESTORE] Task ${id} starting in ${i * RESTORE_STAGGER_MS / 1000}s...`);
            if (i > 0) await new Promise(r => setTimeout(r, RESTORE_STAGGER_MS));
            if (isShuttingDown) break;
            runPersistentTask(id);
        }
    } catch (e) {
        console.error("[LOAD ERROR]", e.message);
    }
}

// ==================== HEALTH ====================
app.head('/', (req, res) => res.status(200).end());
app.get('/ping', (req, res) => res.send('Pong'));
app.get('/health', (req, res) => res.json({
    status: 'ONLINE',
    uptime: Math.round(process.uptime()),
    isShuttingDown,
    activeTasks: activeTasks.size,
    taskIds: Array.from(activeTasks.keys())
}));

// ==================== HTML UI ====================
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html lang="hi"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>24/7 Messenger Bot</title>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap" rel="stylesheet">
<style>
body{font-family:Poppins,sans-serif;background:linear-gradient(135deg,#fff0f3,#ffe5ec);color:#2b2d42;padding:20px;margin:0;min-height:100vh}
.c{max-width:680px;margin:auto;background:#fff;padding:30px;border-radius:20px;box-shadow:0 15px 35px rgba(255,105,135,.15);border:1px solid #ffd1dc}
h2{text-align:center;margin:0 0 5px;font-size:24px}
.b{text-align:center;background:linear-gradient(135deg,#ff758c,#ff7eb3);color:#fff;display:block;padding:5px 15px;border-radius:20px;font-size:12px;font-weight:600;margin:0 auto 20px;width:fit-content}
label{font-weight:600;margin-top:15px;display:block;font-size:14px}
input,textarea{width:100%;padding:12px;margin-top:6px;border-radius:10px;border:1.5px solid #ffd1dc;background:#fff9fa;box-sizing:border-box;font-family:Poppins;font-size:14px}
textarea{height:80px;resize:vertical}
.fb{margin-top:6px;background:#fff5f7;border:1.5px dashed #ff477e;padding:12px;border-radius:10px;text-align:center;cursor:pointer}
.fb input{display:none}
.fl{color:#ff477e;font-weight:500;font-size:13px;cursor:pointer}
button{padding:14px;border:none;border-radius:10px;font-weight:600;cursor:pointer;font-size:15px;width:100%;margin-top:15px;color:#fff}
.bs{background:linear-gradient(135deg,#ff477e,#ff1f59)}
.bc{background:linear-gradient(135deg,#3b82f6,#2563eb)}
.bt{background:linear-gradient(135deg,#ff6b6b,#ee5253)}
.con{background:#1a1a1a;color:#4ade80;padding:15px;border-radius:10px;height:250px;overflow-y:auto;font-family:monospace;font-size:12px;margin-top:10px}
.tb{margin-top:30px;border-top:1.5px dashed #ffd1dc;padding:15px;background:#fafafa;border-radius:15px}
.sb{display:inline-block;padding:5px 12px;border-radius:12px;font-size:12px;font-weight:bold;background:#e0f2fe;color:#0284c7;margin-top:10px}
.sb.err{background:#fee2e2;color:#dc2626}
.sb.ok{background:#dcfce7;color:#16a34a}
</style></head><body><div class="c">
<h2>⚡ 24/7 Messenger Bot ⚡</h2>
<span class="b">DEVELOPER: RAJ MISHRA</span>
<form id="f">
<label>Primary Cookies (Required):</label>
<textarea name="cookies" placeholder="c_user=...; xs=...;" required></textarea>
<label>Backup Cookies (Optional):</label>
<textarea name="backupCookies" placeholder="Agar primary fail ho jaye to ye use hongi..."></textarea>
<label>Target ID:</label>
<input type="text" name="threadId" placeholder="Group/User ID" required>
<label>Haters Name (Prefix):</label>
<input type="text" name="hatersName" placeholder="Optional">
<label>Messages:</label>
<div class="fb" onclick="document.getElementById('mf').click()">
<span class="fl" id="fl">📁 Upload Messages File</span>
<input type="file" id="mf" accept=".txt" onchange="loadF(event)">
</div>
<textarea name="messages" id="mb" placeholder="Hello&#10;Test" required></textarea>
<label>Delay (Seconds):</label>
<input type="number" name="delay" value="10" min="2" required>
<button type="submit" class="bs">🚀 Start 24/7 Task</button>
</form>
<div class="tb">
<h3>🔍 Task Control</h3>
<label>Task ID:</label>
<input type="text" id="tid" placeholder="Paste Task ID">
<div style="display:flex;gap:10px">
<button type="button" class="bc" onclick="check()">👁️ Check</button>
<button type="button" class="bt" onclick="del()">🗑️ Delete</button>
</div>
<div style="margin-top:15px;border-top:1px dashed #ffd1dc;padding-top:10px">
<label>Update Primary Cookies:</label>
<textarea id="nc" placeholder="Nayi primary cookies" style="height:50px"></textarea>
<label>Update Backup Cookies:</label>
<textarea id="nb" placeholder="Nayi backup cookies" style="height:50px"></textarea>
<button type="button" class="bc" onclick="upd()">🔄 Update Cookies</button>
</div>
<div id="si" class="sb" style="display:none"></div>
<div class="con" id="cl">Waiting...</div>
</div></div>
<script>
let iv;
function loadF(e){const f=e.target.files[0];if(!f)return;document.getElementById('fl').innerText="📄 "+f.name;const r=new FileReader();r.onload=x=>document.getElementById('mb').value=x.target.result;r.readAsText(f);}
document.getElementById('f').addEventListener('submit',async e=>{
e.preventDefault();
const fd=new FormData(e.target);
const r=await fetch('/start-task',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(Object.fromEntries(fd))});
const j=await r.json();
if(j.success){alert("Task ID: "+j.taskId+"\\n\\nIse save karein!");document.getElementById('tid').value=j.taskId;check();}
else alert('Error: '+j.error);
});
function check(){
const id=document.getElementById('tid').value.trim();
if(!id)return alert('Task ID daaliye!');
if(iv)clearInterval(iv);
iv=setInterval(async()=>{
try{
const r=await fetch('/logs/'+id);const d=await r.json();
const cl=document.getElementById('cl'),si=document.getElementById('si');
if(d.success){
si.style.display="block";
si.className="sb"+(d.status==='error'?' err':(d.status==='running'?' ok':''));
si.innerHTML="🟢 "+d.status.toUpperCase()+" | ⏱️ "+d.uptime+" | "+d.activeCookieSource;
cl.innerHTML=d.logs.join('<br>');cl.scrollTop=cl.scrollHeight;
if(d.status==='error'||d.status==='stopped'){clearInterval(iv);}
}else{clearInterval(iv);si.style.display="none";cl.innerHTML=d.message||"Not found!";}
}catch(e){}
},2000);
}
async function del(){
const id=document.getElementById('tid').value.trim();
if(!id)return alert('Task ID daaliye!');
if(confirm("STOP aur DELETE karein?")){
const r=await fetch('/stop-task/'+id,{method:'POST'});const j=await r.json();
alert(j.message);if(iv)clearInterval(iv);
document.getElementById('cl').innerHTML="Deleted.";document.getElementById('si').style.display="none";
}
}
async function upd(){
const id=document.getElementById('tid').value.trim();
const nc=document.getElementById('nc').value.trim();
const nb=document.getElementById('nb').value.trim();
if(!id)return alert('Task ID daaliye!');
if(!nc&&!nb)return alert('Cookies daaliye!');
const r=await fetch('/update-cookies/'+id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({newCookies:nc,newBackup:nb})});
const j=await r.json();alert(j.message);
if(j.success){document.getElementById('nc').value='';document.getElementById('nb').value='';}
}
</script></body></html>`);
});

// ==================== START TASK ====================
app.post('/start-task', async (req, res) => {
    try {
        const { cookies, backupCookies, threadId, hatersName, messages, delay } = req.body;

        if (!cookies || !cookies.trim()) return res.status(400).json({ success: false, error: 'Cookies required' });
        if (!threadId || !threadId.trim()) return res.status(400).json({ success: false, error: 'Thread ID required' });
        if (!messages || !messages.trim()) return res.status(400).json({ success: false, error: 'Messages required' });

        const messageList = messages.split('\n').map(m => m.trim()).filter(Boolean);
        if (messageList.length === 0) return res.status(400).json({ success: false, error: 'No valid messages' });

        const taskId = crypto.randomBytes(4).toString('hex');

        // ✅ FIX 8: Same cookies check — backup === primary to backup clear kar do
        let backup = (backupCookies || '').trim();
        if (backup && backup === cookies.trim()) backup = '';

        const task = {
            cookies: cookies.trim(),
            backupCookies: backup,
            threadId: threadId.trim(),
            hatersName: (hatersName || '').trim(),
            messages: messageList,
            delaySec: Math.max(2, parseInt(delay) || 10),
            logs: [],
            status: 'running',
            startTime: Date.now(),
            activeCookieSource: 'primary',
            forceReconnect: false,
            client: null,             // ✅ FIX 5: client store karo
            connectFailCount: 0,      // ✅ FIX 3: backoff counter
            sendFailCount: 0          // ✅ FIX 9: send retry backoff
        };

        pushLog(task, `Task ${taskId} created.`);
        if (backup) pushLog(task, '🛡️ Backup cookies enabled.');

        activeTasks.set(taskId, task);

        await scheduleSave(true); // force save on create
        res.json({ success: true, taskId });

        runPersistentTask(taskId);
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// ==================== UPDATE COOKIES ====================
app.post('/update-cookies/:taskId', async (req, res) => {
    const task = activeTasks.get(req.params.taskId);
    if (!task || task.status !== 'running') {
        return res.status(404).json({ success: false, message: 'Task not found or not running.' });
    }

    if (req.body.newCookies && req.body.newCookies.trim()) {
        task.cookies = req.body.newCookies.trim();
    }
    if (req.body.newBackup !== undefined) {
        let newBackup = (req.body.newBackup || '').trim();
        // ✅ FIX 8: same cookies check
        if (newBackup && newBackup === task.cookies) newBackup = '';
        task.backupCookies = newBackup;
    }

    task.forceReconnect = true;
    task.activeCookieSource = 'primary';
    task.connectFailCount = 0; // reset backoff
    pushLog(task, '🔄 Cookies updated by user. Reconnecting...');

    await scheduleSave(true);
    res.json({ success: true, message: 'Cookies updated! Bot next cycle mein nayi cookies use karega.' });
});

// ==========================================
// 🚀 PERSISTENT TASK RUNNER
// ==========================================
async function runPersistentTask(taskId) {
    const task = activeTasks.get(taskId);
    if (!task) return;

    let msgIndex = 0;
    let loopCount = 1;

    // ---- Connect helper ----
    async function tryConnect(cookies) {
        let nc = null;
        try {
            const cm = CookieManager.fromString(Platform.Messenger, cookies);
            nc = new MessengerClient({
                platform: Platform.Messenger,
                cookies: cm.getAll(),
                enableE2EE: false
            });

            await withTimeout(nc.loadMessagesPage(), CONNECT_TIMEOUT_MS, 'loadMessagesPage');
            await withTimeout(nc.connect(), CONNECT_TIMEOUT_MS, 'connect');
            return nc;
        } catch (e) {
            if (nc) { try { await nc.disconnect(); } catch (_) {} }
            throw e;
        }
    }

    async function doDisconnect() {
        const c = task.client;
        task.client = null;
        if (c) {
            try { await c.disconnect(); } catch (_) {}
        }
    }

    while (activeTasks.has(taskId) && task.status === 'running' && !isShuttingDown) {

        // ---- Live cookie update ----
        if (task.forceReconnect) {
            task.forceReconnect = false;
            await doDisconnect();
        }

        // ========== CONNECT PHASE ==========
        if (!task.client) {
            let connected = false;

            // Phase 1: Primary
            for (let i = 1; i <= PRIMARY_RETRIES; i++) {
                if (!activeTasks.has(taskId) || task.status !== 'running' || isShuttingDown) return;

                pushLog(task, `🔌 [PRIMARY] Attempt ${i}/${PRIMARY_RETRIES}...`);
                await scheduleSave(false);

                try {
                    const c = await tryConnect(task.cookies);
                    task.client = c;
                    task.activeCookieSource = 'primary';
                    task.connectFailCount = 0;
                    pushLog(task, '✅ Primary connected!');
                    await scheduleSave(true);
                    connected = true;
                    break;
                } catch (e) {
                    pushLog(task, `⚠️ Primary ${i}/${PRIMARY_RETRIES} failed: ${e.message}`);
                    await scheduleSave(false);
                    if (i < PRIMARY_RETRIES) await interruptibleSleep(5000);
                }
            }

            // Phase 2: Backup
            if (!connected && task.backupCookies && task.backupCookies.trim()) {
                pushLog(task, '🔄 Primary fail. Backup try...');
                await scheduleSave(false);

                for (let i = 1; i <= BACKUP_RETRIES; i++) {
                    if (!activeTasks.has(taskId) || task.status !== 'running' || isShuttingDown) return;

                    pushLog(task, `🔌 [BACKUP] Attempt ${i}/${BACKUP_RETRIES}...`);
                    await scheduleSave(false);

                    try {
                        const c = await tryConnect(task.backupCookies);
                        task.client = c;
                        task.activeCookieSource = 'backup';
                        task.connectFailCount = 0;
                        pushLog(task, '✅ Backup connected!');
                        await scheduleSave(true);
                        connected = true;
                        break;
                    } catch (e) {
                        pushLog(task, `⚠️ Backup ${i}/${BACKUP_RETRIES} failed: ${e.message}`);
                        await scheduleSave(false);
                        if (i < BACKUP_RETRIES) await interruptibleSleep(5000);
                    }
                }
            }

            // Phase 3: Cooldown with exponential backoff
            if (!connected) {
                task.connectFailCount++;
                task.activeCookieSource = 'primary';

                // ✅ FIX 3+4: Max attempts → error status
                if (task.connectFailCount >= MAX_CONNECT_FAILS) {
                    task.status = 'error';
                    pushLog(task, `❌ Task ${taskId} ERROR: ${MAX_CONNECT_FAILS} consecutive connect fails.`);
                    pushLog(task, '💡 Cookies invalid/expired hain. Panel se nayi cookies update karein.');
                    await scheduleSave(true);
                    return; // loop se exit
                }

                const wait = backoffFor(task.connectFailCount);
                pushLog(task, `❌ Saare attempts fail (${task.connectFailCount}/${MAX_CONNECT_FAILS}). ${wait / 1000}s wait...`);
                pushLog(task, '💡 Tip: Panel se nayi cookies update karein.');
                await scheduleSave(false);

                const ok = await interruptibleSleep(wait);
                if (!ok) return;
                continue;
            }
        }

        // ========== SEND PHASE ==========
        const rawMsg = task.messages[msgIndex] || 'Hello';
        const finalMessage = task.hatersName ? `${task.hatersName} ${rawMsg}` : rawMsg;

        try {
            // ✅ FIX 2: sendMessage timeout wrapper
            await withTimeout(
                task.client.sendMessage(task.threadId, finalMessage),
                SEND_TIMEOUT_MS,
                'sendMessage'
            );

            pushLog(task, `🚀 [${task.activeCookieSource.toUpperCase()}] Sent: ${finalMessage}`);
            task.sendFailCount = 0;

            msgIndex++;
            if (msgIndex >= task.messages.length) {
                msgIndex = 0;
                loopCount++;
                pushLog(task, `🔄 Round ${loopCount} started...`);
            }

            await scheduleSave(false);

            const ok = await interruptibleSleep(task.delaySec * 1000);
            if (!ok) return;

        } catch (e) {
            task.sendFailCount++;
            pushLog(task, `⚠️ Send error: ${e.message}. Reconnecting...`);
            await scheduleSave(false);

            await doDisconnect();

            // ✅ FIX 9: Exponential backoff for send retries
            const idx = Math.min(task.sendFailCount - 1, SEND_RETRY_BACKOFF_MS.length - 1);
            const wait = SEND_RETRY_BACKOFF_MS[idx];
            const ok = await interruptibleSleep(wait);
            if (!ok) return;
        }
    }

    await doDisconnect();
}

// ==================== LOGS ====================
app.get('/logs/:taskId', (req, res) => {
    const t = activeTasks.get(req.params.taskId);
    if (t) {
        const src = t.activeCookieSource === 'backup' ? '🍪 BACKUP' : '🍪 PRIMARY';
        res.json({
            success: true,
            status: t.status,
            uptime: getUptimeString(t.startTime),
            activeCookieSource: src,
            logs: t.logs
        });
    } else {
        res.json({ success: false, message: 'Task not found.' });
    }
});

// ==================== STOP TASK ====================
app.post('/stop-task/:taskId', async (req, res) => {
    const id = req.params.taskId;
    const t = activeTasks.get(id);
    if (!t) return res.status(404).json({ success: false, message: 'Task not found.' });

    t.status = 'stopped';

    // ✅ FIX 5: disconnect client immediately
    if (t.client) {
        try { await t.client.disconnect(); } catch (_) {}
        t.client = null;
    }

    activeTasks.delete(id);
    await scheduleSave(true);

    res.json({ success: true, message: `Task ${id} deleted!` });
});

// ==================== GRACEFUL SHUTDOWN ====================
async function gracefulShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`[SHUTDOWN] ${signal} received. Cleaning up...`);

    // Disconnect all clients
    for (const [id, t] of activeTasks.entries()) {
        t.status = 'stopped';
        if (t.client) {
            try { await t.client.disconnect(); } catch (_) {}
            t.client = null;
        }
    }

    // Final save
    try { await doSave(); } catch (_) {}

    // Give sockets a moment to close cleanly
    await new Promise(r => setTimeout(r, 500));
    process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ==================== BOOT ====================
app.listen(PORT, '0.0.0.0', async () => {
    console.log(`[LIVE] Port ${PORT} - Raj Mishra`);
    await loadTasks();
});
