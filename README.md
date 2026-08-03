# Watch-Party Sync Server

A minimal WebSocket relay that keeps two (or more) viewers' playback in sync.
It never touches the video file itself — each person plays their own local
copy of the same movie file, and this server just relays small play/pause/
seek/time messages between them. That's how it stays legal, cheap, and fast:
no video ever crosses the network.

## Run locally

```bash
npm install
npm start
```

Server listens on `ws://localhost:8080`.

## Deploy for free (so it works over the real internet, not just same WiFi)

### Option A: Render.com
1. Push this folder to a GitHub repo.
2. On Render: New → Web Service → connect the repo.
3. Build command: `npm install`
4. Start command: `node server.js`
5. Once deployed, your URL will be like `https://your-app.onrender.com`.
   Use `wss://your-app.onrender.com` (note **wss**, not https) in the frontend.

### Option B: Railway.app
1. New Project → Deploy from GitHub repo.
2. Railway auto-detects Node and runs `npm start`.
3. Generate a public domain in Settings → Networking.
4. Use `wss://<your-domain>` in the frontend.

### Option C: Fly.io
1. `fly launch` in this folder (accept defaults, Node detected automatically).
2. `fly deploy`
3. Use `wss://<your-app>.fly.dev` in the frontend.

> Free tiers on Render/Railway may "sleep" after inactivity — the first
> connection after a while can take ~30s to wake up. That's normal.

## Protocol (for reference / extending it)

Client → Server messages (JSON over the WebSocket):

| type        | fields                  | meaning                              |
|-------------|--------------------------|---------------------------------------|
| `join`      | `roomCode`, `name`       | join/create a room                    |
| `play`      | `time`                   | I hit play at this timestamp          |
| `pause`     | `time`                   | I hit pause at this timestamp         |
| `seek`      | `time`                   | I jumped to this timestamp            |
| `ping-time` | `time`                   | periodic heartbeat of my current time |
| `caption`   | `text`                   | live caption text to relay            |
| `chat`      | `text`                   | chat message                          |

Server → Client messages:

| type          | fields             | meaning                          |
|---------------|--------------------|-----------------------------------|
| `joined`      | `roomCode`, `peers`| confirms your join, peer count    |
| `peer-joined` | `name`             | someone else joined the room      |
| `peer-left`   | `name`             | someone left the room             |
| `play`/`pause`/`seek` | `time`, `from` | relayed control event from peer |
| `peer-time`   | `time`, `from`     | peer's current playback position  |
| `caption`     | `text`, `from`     | relayed caption text              |
| `chat`        | `text`, `from`, `ts` | relayed chat message            |

No authentication, no persistence, no video data — intentionally minimal.
Add auth/rate-limiting before using this for anything beyond you and a friend.
