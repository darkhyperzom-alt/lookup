// ===== savebank-server — server.js (v2.1: broadcast results back to sender + no-silent-expiry) =====
// รองรับทั้ง:
//   KBiz AutoFill v11        → /api/send, /api/poll/:name, /api/done/:id, /api/status
//   KBiz BankLookup (BO)     → /api/send, /api/status, WS register (รับผลกลับ real-time)
//   KBiz AutoSearch          → WebSocket register + search

const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;

// WebSocket clients: name → ws   (ใช้ร่วมกันทั้งฝั่ง BO และฝั่ง KBiz — คนละชื่อกัน)
const clients = new Map();

// Job queue: id → { target, accountNo, bankName, sentBy, status, createdAt }
const jobs = new Map();

// KBiz Result store (legacy poll-based, เก็บไว้เผื่อใช้): sentBy → { accountNo, bankName, holderName, ts }
const kbizResults = new Map();

// ★ เวลาสูงสุดที่ job จะรอผลได้ก่อนถูกถือว่า "หมดเวลา" — นานกว่าตอนแรก (10 นาที) พอสมควร
// เผื่อคิวยาว/KBiz ช้า และเมื่อหมดเวลาจะ "แจ้งเตือนผู้ส่งก่อนลบ" แทนที่จะลบเงียบๆ
const JOB_MAX_AGE = 900000; // 15 นาที

function sendWS(name, payload) {
  const ws = clients.get(name);
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
    return true;
  }
  return false;
}

// ★ ถ้า job หมดเวลาโดยยังไม่ done/error ให้แจ้งผู้ส่งก่อนลบ ไม่ให้ผลลัพธ์หายเงียบ
function scheduleExpiry(job) {
  setTimeout(() => {
    const current = jobs.get(job.id);
    if (!current) return; // ถูกจัดการ/ลบไปแล้ว (เช่น done ปกติ)
    if (current.status !== 'done' && current.status !== 'error') {
      current.status = 'error';
      current.message = 'หมดเวลารอผลจาก KBiz (job expired)';
      console.log(`[Expire] ${current.id} → expired (สถานะก่อนหน้า: ${job.status})`);
      if (current.sentBy) {
        sendWS(current.sentBy, {
          type: 'result',
          id: current.id,
          status: 'error',
          accountNo: current.accountNo,
          bankName: current.bankName,
          recipientName: null,
          recipientBank: null,
          recipientImage: null,
          message: current.message,
          target: current.target,
          ts: Date.now()
        });
      }
    }
    jobs.delete(job.id);
  }, JOB_MAX_AGE);
}

// ===== HTTP =====
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const url = req.url.split('?')[0];

  // ─── GET /api/status ───────────────────────────────────────────────
  if (req.method === 'GET' && url === '/api/status') {
    const online = [...clients.keys()];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, online, count: online.length }));
    return;
  }

  // ─── GET /ping (wake-up endpoint) ─────────────────────────────────
  if (req.method === 'GET' && (url === '/ping' || url === '/')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, clients: clients.size }));
    return;
  }

  // ─── GET /api/poll/:name  (polling สำหรับ KBiz AutoFill) ──────────
  const pollMatch = url.match(/^\/api\/poll\/(.+)$/);
  if (req.method === 'GET' && pollMatch) {
    const name = decodeURIComponent(pollMatch[1]);
    const items = [...jobs.values()]
      .filter(j => j.target === name && j.status === 'pending')
      .slice(0, 5)
      .map(j => ({ id: j.id, accountNo: j.accountNo, bankName: j.bankName, sentBy: j.sentBy, username: j.username || '' }));

    items.forEach(i => { if (jobs.has(i.id)) jobs.get(i.id).status = 'polled'; });

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, items }));
    return;
  }

  // ─── POST /api/done/:id  (KBiz AutoFill รายงานผล) ─────────────────
  const doneMatch = url.match(/^\/api\/done\/(.+)$/);
  if (req.method === 'POST' && doneMatch) {
    const id = doneMatch[1];
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { status, message, recipientName, recipientBank, recipientImage } = JSON.parse(body || '{}');
        const job = jobs.get(id);
        if (job) {
          job.status = status || 'done';
          job.recipientName = recipientName || job.recipientName || null;
          job.recipientBank = recipientBank || job.recipientBank || null;
          if (recipientImage) job.recipientImage = recipientImage; // ★ อัปเดตรูปเฉพาะเมื่อมีค่าใหม่ ไม่ทับด้วย null
          job.message = message || null;
          job.doneAt = Date.now();
          console.log(`[Done] ${id} → ${status} | ${recipientName || ''}${recipientImage ? ' [+รูป]' : ''}`);

          // ★ ส่งผลกลับไปหา "ผู้ส่ง" (sentBy) แบบ real-time ผ่าน WS
          // ฝั่ง BO (KBiz BankLookup) ต้อง register ด้วยชื่อเดียวกับ "myName" ที่ใช้ส่ง (sentBy)
          if (job.sentBy) {
            sendWS(job.sentBy, {
              type: 'result',
              id,
              status: job.status,
              accountNo: job.accountNo,
              bankName: job.bankName,
              recipientName: job.recipientName,
              recipientBank: job.recipientBank,
              recipientImage: job.recipientImage || null,
              target: job.target,
              ts: Date.now()
            });
          }

          // ★ ลบเฉพาะ "รูป" (ส่วนที่กินพื้นที่ memory เยอะสุด) ออกก่อน หลังจากส่งให้ครบทุกช่องทางแล้ว
          // (WS แบบ real-time + polling fallback ที่ BO เช็คทุก ~3 วิ) เก็บแค่ 15 วิให้ชัวร์ว่าถึงแน่ๆ
          // ตัว job (ข้อความ/สถานะ) ยังอยู่ต่อจนครบ 5 นาทีตามเดิม เผื่อ query ย้อนหลัง
          if (job.recipientImage) {
            setTimeout(() => {
              const j = jobs.get(id);
              if (j) j.recipientImage = null;
            }, 15000);
          }

          // ลบทั้ง job หลัง 5 นาที (job เสร็จแล้ว ไม่ต้องรอ scheduleExpiry อีก)
          setTimeout(() => jobs.delete(id), 300000);
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400); res.end(JSON.stringify({ ok: false }));
      }
    });
    return;
  }

  // ─── GET /api/result/:name  (★ polling fallback — เผื่อ WS หลุด ก็ยังได้ผลภายใน ≤5 วิ) ───
  const resultMatch = url.match(/^\/api\/result\/(.+)$/);
  if (req.method === 'GET' && resultMatch) {
    const name = decodeURIComponent(resultMatch[1]);
    let latest = null;
    for (const job of jobs.values()) {
      if (job.sentBy !== name) continue;
      if (job.status !== 'done' && job.status !== 'error') continue;
      if (!latest || (job.doneAt || 0) > (latest.doneAt || 0)) latest = job;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    if (!latest) { res.end(JSON.stringify({ found: false })); return; }
    // ★ ส่งรูป (base64 ก้อนใหญ่) ให้แค่ "ครั้งแรก" ที่ poll มาเจอ job นี้ เท่านั้น — หลังจากนั้นถือว่าส่งไปแล้ว
    // (WS เป็นช่องทางหลักที่ push รูปให้อยู่แล้ว polling นี้เป็นแค่ fallback เผื่อ WS หลุด)
    // ไม่งั้นทุก 3 วิที่ poll ซ้ำ จะโดนส่งรูปเดิมซ้ำไปเรื่อยๆ จนกว่า job จะถูกแทนที่ — กิน bandwidth ฟรีๆ
    const includeImage = !!latest.recipientImage && !latest.imageDeliveredViaPoll;
    if (includeImage) latest.imageDeliveredViaPoll = true;
    res.end(JSON.stringify({
      found: true,
      id: latest.id,
      status: latest.status,
      accountNo: latest.accountNo,
      bankName: latest.bankName,
      recipientName: latest.recipientName || null,
      recipientBank: latest.recipientBank || null,
      recipientImage: includeImage ? latest.recipientImage : null,
      message: latest.message || null,
      ts: latest.doneAt || Date.now()
    }));
    return;
  }

  // ─── POST /api/send  (BO ส่งงานมา) ──────────────────
  if (req.method === 'POST' && url === '/api/send') {
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { target, accountNo, bankName, sentBy, username } = JSON.parse(body);
        if (!target || !accountNo || !bankName) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: false, error: 'Missing fields' }));
          return;
        }

        const id = crypto.randomUUID();
        const job = { id, target, accountNo, bankName, sentBy: sentBy || 'unknown', username: username || '', status: 'pending', createdAt: Date.now() };
        jobs.set(id, job);

        // ★ แทนที่ setTimeout ลบทิ้งเฉยๆ ด้วย scheduleExpiry ที่แจ้งผู้ส่งก่อนลบถ้ายังไม่มีผล
        scheduleExpiry(job);

        console.log(`[Send] ${sentBy} → ${target} | ${accountNo} ${bankName}${username ? ' | user=' + username : ''} | id=${id}`);

        const sentWs = sendWS(target, { type: 'job', id, accountNo, bankName, sentBy: sentBy || 'unknown', username: username || '' });
        sendWS(target, { type: 'search', id, accountNo, bankName, sentBy: sentBy || 'unknown', username: username || '' });
        if (sentWs) job.status = 'sent-ws';

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, id, online: sentWs }));
      } catch(e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: 'Invalid JSON' }));
      }
    });
    return;
  }

  // ─── POST /api/kbiz-result  (legacy, เก็บไว้เผื่อใช้) ────────
  if (req.method === 'POST' && url === '/api/kbiz-result') {
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { sentBy, accountNo, bankName, holderName } = JSON.parse(body);
        if (!holderName || !accountNo) {
          res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'Missing fields' })); return;
        }
        kbizResults.set(sentBy || 'default', { accountNo, bankName, holderName, ts: Date.now() });
        console.log(`[KBiz Result] ${sentBy} → ${holderName} | ${accountNo}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'Invalid JSON' }));
      }
    });
    return;
  }

  // ─── GET /api/kbiz-result  (legacy) ────────────────
  if (req.method === 'GET' && url === '/api/kbiz-result') {
    const now = Date.now();
    let latest = null;
    for (const [, r] of kbizResults) {
      if (now - r.ts < 30000) {
        if (!latest || r.ts > latest.ts) latest = r;
      }
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(latest || {}));
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ===== WebSocket =====
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let name = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);

      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      if (msg.type === 'register' && msg.name) {
        name = msg.name;
        clients.set(name, ws);
        console.log(`[WS] registered: ${name} (total: ${clients.size})`);
        ws.send(JSON.stringify({ type: 'registered', name }));

        // ส่ง pending jobs ที่มีอยู่แล้วทันที (กรณีนี้คือชื่อนี้เป็น "target"/ฝั่ง KBiz)
        const pending = [...jobs.values()].filter(j => j.target === name && j.status === 'pending');
        pending.forEach(j => {
          ws.send(JSON.stringify({ type: 'job', id: j.id, accountNo: j.accountNo, bankName: j.bankName, sentBy: j.sentBy, username: j.username || '' }));
          ws.send(JSON.stringify({ type: 'search', id: j.id, accountNo: j.accountNo, bankName: j.bankName, sentBy: j.sentBy, username: j.username || '' }));
          j.status = 'sent-ws';
        });

        // ถ้าชื่อนี้เป็น "sentBy" ของงานที่เพิ่ง done แล้วยังไม่ถูกส่งกลับ (เผื่อ reconnect พลาดจังหวะ)
        const doneForMe = [...jobs.values()].filter(j => j.sentBy === name && (j.status === 'done' || j.status === 'error'));
        doneForMe.forEach(j => {
          ws.send(JSON.stringify({
            type: 'result', id: j.id, status: j.status, accountNo: j.accountNo,
            bankName: j.bankName, recipientName: j.recipientName || null, recipientBank: j.recipientBank || null,
            recipientImage: j.recipientImage || null, message: j.message || null, target: j.target, ts: Date.now()
          }));
        });
        return;
      }
    } catch(e) {}
  });

  ws.on('close', () => {
    if (name) {
      clients.delete(name);
      console.log(`[WS] disconnected: ${name} (total: ${clients.size})`);
    }
  });

  ws.on('error', () => { if (name) clients.delete(name); });
});

// ===== Cleanup jobs เก่าตกค้าง (เผื่อ scheduleExpiry ไม่ทำงานเพราะ server รีสตาร์ท) ทุก 5 นาที =====
setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if (now - job.createdAt > JOB_MAX_AGE + 300000) { jobs.delete(id); }
  }
}, 300000);

server.listen(PORT, () => {
  console.log(`✅ savebank-server (v2.1 — realtime result relay + no-silent-expiry) running on port ${PORT}`);
});
