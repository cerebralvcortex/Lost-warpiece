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

const PORT = process.env.PORT || 8080;

// rooms: Map<roomCode, Set<ws>>
const rooms = new Map();

// Basic HTTP server so hosting platforms have something to health-check
const httpServer = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('watch-party sync server: ok\n');
});

const wss = new WebSocketServer({ server: httpServer });

function getRoom(code) {
  if (!rooms.has(code)) rooms.set(code, new Set());
  return rooms.get(code);
}

function broadcast(roomCode, senderWs, payload) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const msg = JSON.stringify(payload);
  for (const client of room) {
    if (client !== senderWs && client.readyState === client.OPEN) {
      client.send(msg);
    }
  }
}

function roomSize(roomCode) {
  return rooms.get(roomCode)?.size || 0;
}

wss.on('connection', (ws) => {
  ws.roomCode = null;
  ws.name = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore malformed messages
    }

    switch (msg.type) {
      case 'join': {
        const { roomCode, name } = msg;
        if (!roomCode) return;
        ws.roomCode = roomCode;
        ws.name = name || 'Guest';
        getRoom(roomCode).add(ws);

        // Tell everyone else someone joined, and tell the joiner the count
        broadcast(roomCode, ws, { type: 'peer-joined', name: ws.name });
        ws.send(JSON.stringify({
          type: 'joined',
          roomCode,
          peers: roomSize(roomCode) - 1,
        }));
        break;
      }

      // Playback control events: play, pause, seek
      // payload: { type: 'play'|'pause'|'seek', time: <seconds>, ts: <clientTimestampMs> }
      case 'play':
      case 'pause':
      case 'seek': {
        if (!ws.roomCode) return;
        broadcast(ws.roomCode, ws, {
          type: msg.type,
          time: msg.time,
          ts: msg.ts || Date.now(),
          from: ws.name,
        });
        break;
      }

      // Periodic heartbeat so both sides can auto-correct drift.
      // Relays playing state + original send timestamp so the receiver can
      // compensate for network transit time rather than comparing against
      // an already-stale position (this is what used to look like multi-
      // second "drift" even when both sides were basically in sync).
      case 'ping-time': {
        if (!ws.roomCode) return;
        broadcast(ws.roomCode, ws, {
          type: 'peer-time',
          time: msg.time,
          playing: !!msg.playing,
          sentAt: msg.sentAt || Date.now(),
          from: ws.name,
        });
        break;
      }

      // Live caption text relay (so both sides can optionally see
      // captions generated on either device, e.g. if only one mic is used)
      case 'caption': {
        if (!ws.roomCode) return;
        broadcast(ws.roomCode, ws, {
          type: 'caption',
          text: msg.text,
          from: ws.name,
        });
        break;
      }

      // Simple text chat
      case 'chat': {
        if (!ws.roomCode) return;
        broadcast(ws.roomCode, ws, {
          type: 'chat',
          text: msg.text,
          from: ws.name,
          ts: Date.now(),
        });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (ws.roomCode && rooms.has(ws.roomCode)) {
      rooms.get(ws.roomCode).delete(ws);
      broadcast(ws.roomCode, ws, { type: 'peer-left', name: ws.name });
      if (rooms.get(ws.roomCode).size === 0) {
        rooms.delete(ws.roomCode);
      }
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`watch-party sync server listening on :${PORT}`);
});
