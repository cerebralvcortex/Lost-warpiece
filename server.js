/**
 * Watch-Party Sync Server
 * ------------------------
 * A tiny WebSocket relay that keeps playback state (play/pause/seek/time)
 * in sync between people in the same "room". It does NOT stream video —
 * each viewer plays their own local copy of the same file. This server
 * only relays small JSON messages (timestamps, events), so it's cheap
 * to run and works over any network.
 *
 * DEPLOY:
 *   1. Put this file + package.json in a folder, push to GitHub.
 *   2. Deploy on Render.com / Railway.app / Fly.io (all have free tiers)
 *      - Build command: npm install
 *      - Start command: node server.js
 *   3. Note the public URL, e.g. wss://your-app.onrender.com
 *   4. Paste that URL into the frontend's "Server URL" field.
 *
 * LOCAL TEST:
 *   npm install
 *   node server.js
 *   -> ws://localhost:8080
 */

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;

// ---------- limits (kept generous but bounded, so one bad client can't hurt everyone else) ----------
const MAX_ROOM_CODE_LEN = 40;
const MAX_NAME_LEN = 40;
const MAX_TEXT_LEN = 500;          // chat / caption messages
const MAX_ROOM_SIZE = 8;           // this app is designed for two, but leaves headroom
const MAX_MESSAGES_PER_WINDOW = 40; // per client
const RATE_WINDOW_MS = 10_000;      // 10s sliding window
const ROOM_IDLE_EXPIRY_MS = 6 * 60 * 60 * 1000; // drop empty/idle rooms after 6h
const ROOM_CODE_PATTERN = /^[A-Za-z0-9_-]+$/;

// rooms: Map<roomCode, { clients: Set<ws>, playback: PlaybackState, lastActivity: number, passwordHash: string|null }>
// PlaybackState lets a late joiner instantly learn "we're already playing at 42.3s"
// instead of sitting frozen at 0:00 until the next play/pause/seek event happens to fire.
const rooms = new Map();

function makeRoomState() {
  return {
    clients: new Set(),
    lastActivity: Date.now(),
    passwordHash: null, // set by whoever creates the room, if they choose to
    playback: {
      status: 'paused',   // 'playing' | 'paused'
      time: 0,            // seconds, as of `updatedAt`
      rate: 1,            // playback speed, inherited by late joiners
      updatedAt: Date.now(),
    },
  };
}

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, makeRoomState());
  return rooms.get(code);
}

function roomSize(roomCode) {
  return rooms.get(roomCode)?.clients.size || 0;
}

// estimate "where playback should be right now" by projecting forward from the
// last known state — so a joiner catches the current moment, not a stale one
function estimateCurrentTime(room) {
  const p = room.playback;
  if (p.status !== 'playing') return p.time;
  const elapsed = (Date.now() - p.updatedAt) / 1000;
  return p.time + Math.max(0, elapsed);
}

function broadcast(roomCode, senderWs, payload) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const msg = JSON.stringify(payload);
  for (const client of room.clients) {
    if (client !== senderWs && client.readyState === client.OPEN) {
      client.send(msg);
    }
  }
}

// ---------- basic shape / size validation ----------
function isValidRoomCode(code) {
  return typeof code === 'string' &&
    code.length > 0 &&
    code.length <= MAX_ROOM_CODE_LEN &&
    ROOM_CODE_PATTERN.test(code);
}
function cleanName(name) {
  if (typeof name !== 'string' || !name.trim()) return 'Guest';
  return name.trim().slice(0, MAX_NAME_LEN);
}
function cleanText(text) {
  if (typeof text !== 'string') return '';
  return text.slice(0, MAX_TEXT_LEN);
}
function isFiniteNumber(n) {
  return typeof n === 'number' && Number.isFinite(n) && n >= 0 && n < 1e7;
}

// Rooms are ephemeral and this is a lightweight relay, not an account
// system — a fast, salted hash is the right amount of protection here
// (keeps a plaintext password out of server memory/logs) without pulling
// in a heavyweight KDF meant for long-lived credentials.
function hashPassword(password, roomCode) {
  return crypto.createHash('sha256').update(roomCode + ':' + password).digest('hex');
}

// Basic HTTP server so hosting platforms have something to health-check
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('watch-party sync server: ok\n');
});

const wss = new WebSocketServer({
  server: httpServer,
  // Reject any single message over ~64KB before it's ever handed to our
  // message handler. Real sync messages (timestamps, short chat lines) are
  // a few hundred bytes at most — this caps how much memory/CPU a single
  // malicious or buggy client can force the server to spend per message,
  // which matters most on a memory-limited free host where every room
  // shares the same process.
  maxPayload: 64 * 1024,
});

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.name = null;
  ws.isAlive = true;
  ws.msgTimestamps = []; // for rate limiting

  // CRITICAL: without this, an oversized message (which trips maxPayload)
  // or any other per-socket error emits an 'error' event with no listener,
  // and Node's default behavior for an unhandled EventEmitter error is to
  // throw — crashing the ENTIRE process, not just this connection. This
  // was found during testing: a single malicious message was taking down
  // the whole server for every room, which is far worse than the bug it
  // was meant to fix. Now it just closes the one offending connection.
  ws.on('error', (err) => {
    try { ws.terminate(); } catch {}
  });

  // heartbeat so dead/zombie connections (phone locked, wifi dropped without
  // a clean close) get cleaned up instead of silently blocking a room slot
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // ---- rate limiting: drop clients that flood the socket ----
    const now = Date.now();
    ws.msgTimestamps = ws.msgTimestamps.filter(t => now - t < RATE_WINDOW_MS);
    ws.msgTimestamps.push(now);
    if (ws.msgTimestamps.length > MAX_MESSAGES_PER_WINDOW) {
      return; // silently drop; do not reward flooding with an error round-trip
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed messages
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'join': {
        // reject bad room codes outright instead of silently accepting garbage
        if (!isValidRoomCode(msg.roomCode)) {
          ws.send(JSON.stringify({ type: 'join-error', reason: 'invalid-room-code' }));
          return;
        }
        const roomExisted = rooms.has(msg.roomCode);
        const room = getRoom(msg.roomCode);

        if (room.clients.size >= MAX_ROOM_SIZE) {
          ws.send(JSON.stringify({ type: 'join-error', reason: 'room-full' }));
          return;
        }

        // password handling: whoever creates the room (first to join) can
        // set a password; everyone after must supply the matching one.
        // This closes the "anyone who guesses the room code can join"
        // gap — the room code becomes an address, the password becomes
        // the actual access control, same as it should be.
        const suppliedPassword = typeof msg.password === 'string' ? msg.password.slice(0, 100) : '';
        if (!roomExisted || room.clients.size === 0) {
          // room is being created (or was fully empty) — this join sets the password
          room.passwordHash = suppliedPassword ? hashPassword(suppliedPassword, msg.roomCode) : null;
        } else if (room.passwordHash) {
          const suppliedHash = suppliedPassword ? hashPassword(suppliedPassword, msg.roomCode) : null;
          if (suppliedHash !== room.passwordHash) {
            ws.send(JSON.stringify({ type: 'join-error', reason: 'wrong-password' }));
            return;
          }
        }

        ws.roomCode = msg.roomCode;
        ws.name = cleanName(msg.name);

        // prevent one client from silently impersonating another active
        // participant by claiming the same display name — this doesn't
        // require accounts, just stops the cheapest version of spoofing
        for (const existing of room.clients) {
          if (existing.name === ws.name) {
            ws.send(JSON.stringify({ type: 'join-error', reason: 'name-taken' }));
            ws.roomCode = null;
            return;
          }
        }

        room.clients.add(ws);
        room.lastActivity = now;

        // tell everyone else someone joined
        broadcast(msg.roomCode, ws, { type: 'peer-joined', name: ws.name });

        // tell the joiner the room's CURRENT playback state, so they land in
        // the right spot immediately instead of sitting paused at 0:00 while
        // everyone else is already watching — this is the core fix for
        // "one player starting before the second is connected"
        ws.send(JSON.stringify({
          type: 'joined',
          roomCode: msg.roomCode,
          peers: room.clients.size - 1,
          hasPassword: !!room.passwordHash,
          playback: {
            status: room.playback.status,
            time: estimateCurrentTime(room),
            rate: room.playback.rate || 1,
          },
        }));
        break;
      }

      // Playback control events: play, pause, seek
      case 'play':
      case 'pause':
      case 'seek': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        if (!isFiniteNumber(msg.time)) return;
        const room = rooms.get(ws.roomCode);
        room.lastActivity = now;

        // remember room state so future joiners land in the right place
        room.playback.status = (msg.type === 'pause') ? 'paused' : (msg.type === 'play' ? 'playing' : room.playback.status);
        room.playback.time = msg.time;
        room.playback.updatedAt = now;

        broadcast(ws.roomCode, ws, {
          type: msg.type,
          time: msg.time,
          ts: now,
          from: ws.name,
        });
        break;
      }

      // Playback speed changes — kept in room state too, so a late joiner
      // (or a reconnect) inherits the current rate instead of silently
      // watching at a different speed than everyone else.
      case 'speed': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        if (typeof msg.rate !== 'number' || !Number.isFinite(msg.rate) || msg.rate <= 0 || msg.rate > 4) return;
        const room = rooms.get(ws.roomCode);
        room.lastActivity = now;
        room.playback.rate = msg.rate;
        broadcast(ws.roomCode, ws, { type: 'speed', rate: msg.rate, from: ws.name });
        break;
      }

      // "Start together" ready-check: request + a synced go-time. The
      // server just relays these — the actual countdown timing is computed
      // client-side using a shared target timestamp.
      case 'sync-start-request': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        broadcast(ws.roomCode, ws, { type: 'sync-start-request', from: ws.name });
        break;
      }
      case 'sync-start-go': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        if (typeof msg.targetAt !== 'number' || !Number.isFinite(msg.targetAt)) return;
        broadcast(ws.roomCode, ws, { type: 'sync-start-go', targetAt: msg.targetAt, from: ws.name });
        break;
      }

      // Round-trip drift measurement: the client sends a ping carrying only
      // its OWN send timestamp; the peer replies with a pong once it's
      // received, echoing that same timestamp back untouched. The server
      // just relays both — all the actual RTT math happens client-side,
      // compared only against the sender's own clock (see client comments
      // for why: comparing two different devices' clocks directly was
      // misreading ordinary clock skew as playback drift).
      case 'sync-ping': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        if (typeof msg.sentAt !== 'number' || !Number.isFinite(msg.sentAt)) return;
        rooms.get(ws.roomCode).lastActivity = now;
        broadcast(ws.roomCode, ws, { type: 'sync-ping', sentAt: msg.sentAt });
        break;
      }
      case 'sync-pong': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        if (typeof msg.echoSentAt !== 'number' || !isFiniteNumber(msg.time)) return;
        rooms.get(ws.roomCode).lastActivity = now;
        broadcast(ws.roomCode, ws, {
          type: 'sync-pong',
          echoSentAt: msg.echoSentAt,
          time: msg.time,
          playing: !!msg.playing,
        });
        break;
      }

      case 'caption': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        broadcast(ws.roomCode, ws, { type: 'caption', text: cleanText(msg.text), from: ws.name });
        break;
      }

      case 'chat': {
        if (!ws.roomCode || !rooms.has(ws.roomCode)) return;
        broadcast(ws.roomCode, ws, { type: 'chat', text: cleanText(msg.text), from: ws.name, ts: now });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (ws.roomCode && rooms.has(ws.roomCode)) {
      const room = rooms.get(ws.roomCode);
      room.clients.delete(ws);
      broadcast(ws.roomCode, ws, { type: 'peer-left', name: ws.name });
      if (room.clients.size === 0) {
        room.lastActivity = Date.now(); // start the idle-expiry clock
      }
    }
  });
});

// ---------- connection liveness check ----------
// terminate zombie sockets (phone locked / network dropped without a clean
// close event) so they don't sit in a room forever blocking reconnection
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

// ---------- idle room cleanup ----------
setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.clients.size === 0 && now - room.lastActivity > ROOM_IDLE_EXPIRY_MS) {
      rooms.delete(code);
    }
  }
}, 30 * 60 * 1000);

httpServer.listen(PORT, () => {
  console.log(`watch-party sync server listening on :${PORT}`);
});

// ---------- defense in depth ----------
// Per-connection error handling above should catch the known failure modes,
// but a relay serving untrusted input should never let ONE unanticipated
// error take down every active room. Log it and keep the process alive
// rather than let one bad message end movie night for everyone.
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception (server kept running):', err);
});
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled promise rejection (server kept running):', reason);
});
