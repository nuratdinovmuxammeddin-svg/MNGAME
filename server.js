#!/usr/bin/env node
/*
 * ROYALE ARENA — online server (MNAI Company)
 * Zero dependencies: only Node.js built-ins (needs Node 18+).
 *
 *   node server.js            -> http://localhost:3000
 *
 * - Serves the game file (index.html, or the newest royale-arena-vNN.html next to this file)
 * - Hosts WebSocket rooms (room code, up to MAX_PLAYERS per room)
 * - HP and damage are decided HERE. Clients only report "my bullet hit player X in zone Z";
 *   the server checks fire-rate tokens, range, spawn protection and applies its own damage table.
 * - Movement speed is sanity-checked and cheaters get rubber-banded.
 *
 * Env: PORT, MAX_PLAYERS (default 8), GAME_FILE, ALLOWED_ORIGINS (comma separated, optional)
 */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const PORT = parseInt(process.env.PORT, 10) || 3000;
const MAX_PLAYERS = Math.min(16, Math.max(2, parseInt(process.env.MAX_PLAYERS, 10) || 8));
const MAX_ROOMS = 100;
const MAX_CONN_PER_IP = 10;
const MAX_FRAME_BYTES = 8192;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const START_HP = 500;
const MAX_SPEED = 22;          // units/s. Sprint in the game is ~10.5, this leaves lag slack.
const WORLD_LIMIT = 130;       // Lone Wolf arena is +-90
const SPAWN_PROTECT_MS = 2000;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// Damage table — generated from the game's own WEAPONS table so both sides always agree.
const WEAPONS = {"pistol": {"dmgHead": 380, "dmgBody": 42, "dmgLegs": 24, "cd": 0.34, "range": 55, "falloffStart": 20, "falloffEnd": 55, "falloffMin": 0.65}, "smg": {"dmgHead": 230, "dmgBody": 38, "dmgLegs": 22, "cd": 0.11, "range": 26, "falloffStart": 10, "falloffEnd": 26, "falloffMin": 0.55}, "sniper": {"dmgHead": 520, "dmgBody": 220, "dmgLegs": 120, "cd": 0.95, "range": 150, "falloffStart": 150, "falloffEnd": 150, "falloffMin": 1}, "shotgun": {"dmgHead": 95, "dmgBody": 44, "dmgLegs": 25, "cd": 0.55, "range": 16, "falloffStart": 6, "falloffEnd": 16, "falloffMin": 0.3, "pellets": 5}, "rifle": {"dmgHead": 290, "dmgBody": 50, "dmgLegs": 29, "cd": 0.09, "range": 78, "falloffStart": 35, "falloffEnd": 78, "falloffMin": 0.6}, "fists": {"dmg": 40, "cd": 0.5, "range": 2.2, "melee": true}, "sword": {"dmg": 110, "cd": 0.6, "range": 2.8, "melee": true}};

/* ---------------------------------------------------------------- game file hosting */
function findGameFile() {
  if (process.env.GAME_FILE && fs.existsSync(process.env.GAME_FILE)) return process.env.GAME_FILE;
  const idx = path.join(__dirname, 'index.html');
  if (fs.existsSync(idx)) return idx;
  try {
    const c = fs.readdirSync(__dirname).filter(f => /^royale-arena.*\.html$/i.test(f));
    c.sort((a, b) => (parseInt((a.match(/v(\d+)/) || [0, 0])[1], 10)) - (parseInt((b.match(/v(\d+)/) || [0, 0])[1], 10)));
    if (c.length) return path.join(__dirname, c[c.length - 1]);
  } catch (e) { /* ignore */ }
  return null;
}
const gameFile = findGameFile();
let gzCache = null;
if (gameFile) {
  fs.readFile(gameFile, (err, buf) => {
    if (err) return;
    zlib.gzip(buf, { level: 6 }, (e2, gz) => { if (!e2) { gzCache = gz; console.log('[http] gzip cache ready:', (gz.length / 1048576).toFixed(1) + ' MB'); } });
  });
}

function serveGame(req, res) {
  if (!gameFile) {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('Server ishlayapti, lekin o\'yin fayli topilmadi.\nindex.html (yoki royale-arena-vNN.html) ni server.js yoniga qo\'ying.');
  }
  const acceptsGzip = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  if (gzCache && acceptsGzip) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Encoding': 'gzip', 'Content-Length': gzCache.length, 'Cache-Control': 'public, max-age=60' });
    return res.end(gzCache);
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'public, max-age=60' });
  fs.createReadStream(gameFile).pipe(res);
}

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/stats') {
    const out = [];
    rooms.forEach(r => out.push({ room: r.code, players: r.players.size, alive: aliveList(r).length }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ rooms: out, connections: conns.size }));
  }
  if (url === '/' || url === '/index.html' || url === '/game') return serveGame(req, res);
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

/* ---------------------------------------------------------------- minimal WebSocket (RFC 6455) */
function wsFrame(op, payload) {
  const len = payload.length;
  let header;
  if (len < 126) { header = Buffer.alloc(2); header[1] = len; }
  else if (len < 65536) { header = Buffer.alloc(4); header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  header[0] = 0x80 | op;
  return Buffer.concat([header, payload]);
}
const conns = new Set();
const ipCount = new Map();

function rawWrite(conn, frame) {
  const s = conn.socket;
  if (!s || s.destroyed || !s.writable) return;
  s.write(frame);
}
function send(conn, obj) { rawWrite(conn, wsFrame(0x1, Buffer.from(JSON.stringify(obj)))); }
function sendFrame(conn, frame, droppable) {
  if (droppable && conn.socket.writableLength > 262144) return; // slow client: drop state updates
  rawWrite(conn, frame);
}
function closeConn(conn, code, reason) {
  if (conn.closing) return;
  conn.closing = true;
  try {
    const r = Buffer.from(reason || '');
    const p = Buffer.alloc(2 + r.length); p.writeUInt16BE(code || 1000, 0); r.copy(p, 2);
    conn.socket.write(wsFrame(0x8, p));
  } catch (e) { /* ignore */ }
  try { conn.socket.end(); } catch (e) { /* ignore */ }
  setTimeout(() => { try { conn.socket.destroy(); } catch (e) { /* ignore */ } }, 1500);
}

function parseFrames(conn) {
  for (;;) {
    const buf = conn.buf;
    if (buf.length < 2) return;
    const b0 = buf[0], b1 = buf[1];
    const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (buf.length < 10) return; const big = buf.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME_BYTES)) return closeConn(conn, 1009, 'too big'); len = Number(big); off = 10; }
    if (len > MAX_FRAME_BYTES) return closeConn(conn, 1009, 'too big');
    if (!masked) return closeConn(conn, 1002, 'unmasked');
    if (buf.length < off + 4 + len) return;
    const mask = buf.subarray(off, off + 4);
    const payload = Buffer.from(buf.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    conn.buf = buf.subarray(off + 4 + len);
    conn.lastSeen = Date.now();

    if (op === 0x8) return closeConn(conn, 1000, 'bye');
    if (op === 0x9) { rawWrite(conn, wsFrame(0xA, payload)); continue; }
    if (op === 0xA) continue;
    if (op === 0x1 || op === 0x0) {
      if (op === 0x1) { conn.frag = []; conn.fragLen = 0; }
      conn.frag.push(payload); conn.fragLen += payload.length;
      if (conn.fragLen > MAX_FRAME_BYTES) return closeConn(conn, 1009, 'too big');
      if (fin) {
        const text = Buffer.concat(conn.frag).toString('utf8');
        conn.frag = []; conn.fragLen = 0;
        try { onMessage(conn, text); } catch (e) { console.error('[msg error]', e && e.stack || e); }
      }
    }
    // binary frames (0x2) are ignored
  }
}

server.on('upgrade', (req, socket) => {
  const up = String(req.headers.upgrade || '').toLowerCase();
  const key = req.headers['sec-websocket-key'];
  const origin = req.headers.origin;
  if (up !== 'websocket' || !key) { socket.destroy(); return; }
  if (ALLOWED_ORIGINS.length && origin && !ALLOWED_ORIGINS.includes(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
  }
  const fwd = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const ip = fwd || req.socket.remoteAddress || '?';
  if ((ipCount.get(ip) || 0) >= MAX_CONN_PER_IP) { socket.write('HTTP/1.1 429 Too Many Requests\r\n\r\n'); socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);

  const conn = { socket, ip, buf: Buffer.alloc(0), frag: [], fragLen: 0, player: null, closing: false, lastSeen: Date.now(), winStart: 0, winCount: 0 };
  conns.add(conn);
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);

  socket.on('data', chunk => {
    conn.buf = conn.buf.length ? Buffer.concat([conn.buf, chunk]) : chunk;
    if (conn.buf.length > MAX_FRAME_BYTES * 4) return closeConn(conn, 1009, 'flood');
    parseFrames(conn);
  });
  const cleanup = () => {
    if (!conns.has(conn)) return;
    conns.delete(conn);
    const n = (ipCount.get(ip) || 1) - 1;
    if (n <= 0) ipCount.delete(ip); else ipCount.set(ip, n);
    leaveRoom(conn);
  };
  socket.on('close', cleanup);
  socket.on('end', cleanup);
  socket.on('error', () => { cleanup(); try { socket.destroy(); } catch (e) { /* ignore */ } });
});

setInterval(() => {
  const now = Date.now();
  conns.forEach(c => {
    if (now - c.lastSeen > 45000) { try { c.socket.destroy(); } catch (e) { /* ignore */ } return; }
    rawWrite(c, wsFrame(0x9, Buffer.alloc(0)));
  });
}, 15000).unref();

/* ---------------------------------------------------------------- rooms & players */
const rooms = new Map();
let nextId = 1;
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v)) ? Math.min(hi, Math.max(lo, v)) : null;
const rnd = n => Math.floor(Math.random() * n);

function aliveList(room) { const a = []; room.players.forEach(p => { if (p.alive) a.push(p); }); return a; }
function broadcast(room, obj, except) {
  const frame = wsFrame(0x1, Buffer.from(JSON.stringify(obj)));
  const droppable = obj.t === 'st';
  room.players.forEach(p => { if (p !== except) sendFrame(p.conn, frame, droppable); });
}
function freeSlot(room) {
  const used = new Set(); room.players.forEach(p => used.add(p.slot));
  for (let i = 0; i < MAX_PLAYERS; i++) if (!used.has(i)) return i;
  return 0;
}
function updateMatchState(room) { if (!room.matchActive && aliveList(room).length >= 2) room.matchActive = true; }
function checkWin(room) {
  if (room.matchActive && aliveList(room).length <= 1) {
    room.matchActive = false;
    const w = aliveList(room)[0];
    broadcast(room, { t: 'win', id: w ? w.id : null });
  }
}
function leaveRoom(conn) {
  const p = conn.player;
  if (!p) return;
  conn.player = null;
  const room = p.room;
  room.players.delete(p.id);
  if (room.players.size === 0) { rooms.delete(room.code); return; }
  broadcast(room, { t: 'left', id: p.id });
  checkWin(room);
}

function handleJoin(conn, m) {
  if (conn.player) leaveRoom(conn);
  const code = String(m.room || 'PUBLIC').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'PUBLIC';
  const name = String(m.name || 'Player').replace(/[^\p{L}\p{N} _-]/gu, '').trim().slice(0, 14) || 'Player';
  let room = rooms.get(code);
  if (!room) {
    if (rooms.size >= MAX_ROOMS) return send(conn, { t: 'err', code: 'rooms', msg: 'Serverda xonalar to\'la' });
    room = { code, seed: 1 + rnd(999998), players: new Map(), matchActive: false };
    rooms.set(code, room);
  }
  if (room.players.size >= MAX_PLAYERS) return send(conn, { t: 'err', code: 'full', msg: 'Xona to\'la (' + MAX_PLAYERS + ')' });
  const now = Date.now();
  const p = {
    id: nextId++, name, conn, room, slot: freeSlot(room),
    x: 0, y: 0, z: 0, yw: 0, my: 0, pt: 0, f: 0, w: 'pistol', armor: 0,
    hp: START_HP, alive: true, kills: 0, deaths: 0, hasState: false,
    lastT: 0, viol: 0, tokens: 0, lastFire: {}, lastSwing: 0, lastMeleeHit: 0, lastHeal: 0,
    invulUntil: now + SPAWN_PROTECT_MS
  };
  conn.player = p;
  const others = [];
  room.players.forEach(o => others.push({ id: o.id, name: o.name, slot: o.slot, alive: o.alive, kills: o.kills, hp: o.hp, hasState: o.hasState, x: o.x, y: o.y, z: o.z, yw: o.yw, my: o.my, f: o.f, w: o.w }));
  room.players.set(p.id, p);
  send(conn, { t: 'welcome', id: p.id, slot: p.slot, seed: room.seed, cap: MAX_PLAYERS, room: code, hp: START_HP, players: others });
  broadcast(room, { t: 'joined', id: p.id, name: p.name, slot: p.slot }, p);
  updateMatchState(room);
  console.log('[room ' + code + '] +' + name + ' (#' + p.id + ') players=' + room.players.size);
}

function validDir(m) {
  const dx = num(m.dx, -1.5, 1.5), dy = num(m.dy, -1.5, 1.5), dz = num(m.dz, -1.5, 1.5);
  if (dx === null || dy === null || dz === null) return null;
  const l = Math.hypot(dx, dy, dz);
  if (l < 0.5 || l > 1.5) return null;
  return [dx / l, dy / l, dz / l];
}

function applyDamage(room, a, v, dealt, zone, now) {
  v.hp -= dealt;
  const died = v.hp <= 0;
  if (died) { v.hp = 0; v.alive = false; v.deaths++; if (a !== v) a.kills++; }
  broadcast(room, { t: 'dmg', id: v.id, by: a.id, hp: Math.round(v.hp), d: Math.round(dealt), z: zone });
  if (died) { broadcast(room, { t: 'died', id: v.id, by: a.id }); checkWin(room); }
}

function onMessage(conn, text) {
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  const now = Date.now();
  if (now - conn.winStart > 1000) { conn.winStart = now; conn.winCount = 0; }
  if (++conn.winCount > 150) return closeConn(conn, 1008, 'rate limit');

  if (m.t === 'join') return handleJoin(conn, m);
  if (m.t === 'ping') return send(conn, { t: 'pong', c: typeof m.c === 'number' ? m.c : 0 });
  const p = conn.player;
  if (!p) return;
  const room = p.room;

  switch (m.t) {
    case 'st': {
      if (!p.alive) return;
      const x = num(m.x, -WORLD_LIMIT, WORLD_LIMIT), z = num(m.z, -WORLD_LIMIT, WORLD_LIMIT), y = num(m.y, -5, 60);
      if (x === null || z === null) return;
      if (p.hasState && p.lastT) {
        const dt = Math.min(1, Math.max(0.03, (now - p.lastT) / 1000));
        const dist = Math.hypot(x - p.x, z - p.z);
        if (dist > MAX_SPEED * dt + 1.5) {
          p.viol++;
          if (p.viol >= 3) { p.viol = 0; send(conn, { t: 'snap', x: p.x, z: p.z }); }
          return;
        }
        if (p.viol > 0) p.viol = Math.max(0, p.viol - 0.25);
      }
      p.x = x; p.z = z; p.y = y === null ? 0 : y; p.lastT = now; p.hasState = true;
      p.yw = num(m.yw, -20, 20) || 0; p.my = num(m.my, -20, 20) || 0; p.pt = num(m.pt, -2, 2) || 0;
      p.f = (m.f | 0) & 63;
      p.w = (typeof m.w === 'string' && WEAPONS[m.w]) ? m.w : 'fists';
      p.armor = num(m.ar, 0, 2) || 0;
      broadcast(room, { t: 'st', id: p.id, x: p.x, y: p.y, z: p.z, yw: p.yw, my: p.my, pt: p.pt, f: p.f, w: p.w, hp: Math.round(p.hp) }, p);
      return;
    }
    case 'fire': {
      if (!p.alive) return;
      const w = WEAPONS[m.k];
      if (!w || w.melee) return;
      if (now - (p.lastFire[m.k] || 0) < w.cd * 1000 * 0.4) return;
      const d = validDir(m);
      const mx = num(m.x, -WORLD_LIMIT, WORLD_LIMIT), my = num(m.y, -5, 60), mz = num(m.z, -WORLD_LIMIT, WORLD_LIMIT);
      if (!d || mx === null || my === null || mz === null) return;
      p.lastFire[m.k] = now;
      p.tokens = Math.min(16, p.tokens + (w.pellets || 1));
      broadcast(room, { t: 'fire', id: p.id, k: m.k, x: mx, y: my, z: mz, dx: d[0], dy: d[1], dz: d[2] }, p);
      return;
    }
    case 'melee': {
      if (!p.alive) return;
      const w = WEAPONS[m.k];
      if (!w || !w.melee) return;
      if (now - p.lastSwing < w.cd * 1000 * 0.5) return;
      p.lastSwing = now;
      broadcast(room, { t: 'melee', id: p.id, k: m.k }, p);
      return;
    }
    case 'hit': {
      if (!p.alive) return;
      const v = room.players.get(m.v | 0);
      const w = WEAPONS[m.k];
      if (!v || v === p || !v.alive || !w) return;
      if (v.invulUntil > now) return;
      const d = Math.hypot(p.x - v.x, p.z - v.z);
      let dealt;
      let zone = 'b';
      if (w.melee) {
        if (now - p.lastMeleeHit < w.cd * 1000 * 0.7) return;
        if (d > w.range + 3) return;
        p.lastMeleeHit = now;
        dealt = w.dmg;
      } else {
        if (p.tokens <= 0) return;
        if (d > w.range * 1.25 + 6) return;
        p.tokens--;
        zone = m.z === 'h' ? 'h' : (m.z === 'l' ? 'l' : 'b');
        const base = zone === 'h' ? w.dmgHead : (zone === 'l' ? w.dmgLegs : w.dmgBody);
        let mult = 1;
        if (w.falloffEnd > w.falloffStart && d > w.falloffStart) {
          const t = Math.min(1, (d - w.falloffStart) / (w.falloffEnd - w.falloffStart));
          mult = 1 - t * (1 - w.falloffMin);
        }
        dealt = base * mult;
      }
      dealt *= (1 - (v.armor || 0) * 0.15);
      applyDamage(room, p, v, dealt, zone, now);
      return;
    }
    case 'heal': {
      if (!p.alive) return;
      if (now - p.lastHeal < 800) return;
      p.lastHeal = now;
      p.hp = m.k === 'medkit' ? START_HP : Math.min(START_HP, p.hp + 40);
      broadcast(room, { t: 'hp', id: p.id, hp: Math.round(p.hp) });
      return;
    }
    case 'selfdied': {
      if (!p.alive) return;
      p.alive = false; p.hp = 0; p.deaths++;
      broadcast(room, { t: 'died', id: p.id, by: 0 });
      checkWin(room);
      return;
    }
    case 'respawn': {
      const taken = new Set();
      room.players.forEach(o => { if (o !== p && o.alive) taken.add(o.slot); });
      const free = []; for (let i = 0; i < MAX_PLAYERS; i++) if (!taken.has(i)) free.push(i);
      p.slot = free.length ? free[rnd(free.length)] : p.slot;
      p.hp = START_HP; p.alive = true; p.armor = 0; p.hasState = false; p.lastT = 0; p.viol = 0; p.tokens = 0;
      p.invulUntil = now + SPAWN_PROTECT_MS;
      send(conn, { t: 'you', slot: p.slot, hp: START_HP, seed: room.seed });
      broadcast(room, { t: 'respawned', id: p.id, slot: p.slot }, p);
      updateMatchState(room);
      return;
    }
    case 'leave': return leaveRoom(conn);
    default: return;
  }
}

server.listen(PORT, () => {
  console.log('Royale Arena server listening on :' + PORT);
  console.log('Game file:', gameFile || '(not found — put index.html next to server.js)');
  console.log('Max players / room:', MAX_PLAYERS);
});
