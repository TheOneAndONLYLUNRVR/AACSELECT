# START HERE
1. Install Node.js (LTS) from https://nodejs.org (one time).
2. UNZIP this folder first (do not run it from inside the zip).
3. Double-click: start-windows.bat (Windows) | start-mac.command (Mac) | start-linux.sh (Linux).
   Or in a terminal inside this folder: node server.js
4. Your browser opens by itself. Keep the black window open while the site is running.
If a port is busy it picks the next one and prints the exact address. Use THAT address, never open index.html directly.

# AACSelect.com v4
Run: `node server.js` (Node 18+, no npm install) -> http://localhost:3000
Env: PORT, DB_FILE (persist this on a disk), CYCLE_MIN (e.g. 2 = a full cycle lasts 2 minutes, to test the ignition ceremony).
Host on any Node host (Render / Railway / Fly / VPS). Everything shared (Board, nods, ballots, champions, The Frequency) lives in the server's DB_FILE.

## 21st.dev MCP (for editing this site with Claude Code)
`.mcp.json` reads your key from the env var TWENTYFIRST_API_KEY, so no secret is stored in the project:
  export TWENTYFIRST_API_KEY="<your NEW key>"   then run   claude mcp list
Claude Desktop (no key needed, OAuth): Settings -> Connectors -> Add custom connector -> name "21st", URL https://21st.dev/api/mcp
Reign mode test: stop the server, add a champion to data.json (champs:[{"name":"X","social":"@x","votes":5,"at":<now ms>}]) and restart.

## v6 notes
- Motion-graphic intro (kinetic type, ~9s, skippable, shows live server stats) plays once per browser session; then the original Enter gate, then the "Hey, Drifters" sting.
- Legal: /legal (Not-official disclaimer, Terms, Privacy, Cookies, Third-party, Takedown). EDIT public/legal.html: replace [YOUR CONTACT EMAIL] and [YOUR COUNTRY / JURISDICTION]. Have a lawyer review it.
- The badge artwork belongs to Another Axiom. Disclaimers reduce risk but do not remove it: get permission or swap in your own artwork (public/aac-badge.png).

## v7 — always-on hosting (visitors install nothing)
Visitors only open your URL. YOU deploy once to a host that never sleeps:
- Docker anywhere (VPS, home server): `docker compose up -d --build` (restart: always = auto-restart)
- Render: push this folder to GitHub -> New + Blueprint (uses render.yaml, paid plan keeps it on)
- Health URL: /healthz . Free hosts that sleep? Ping /healthz every 5 min with UptimeRobot.
Votes live in the /data volume — do not delete it.
The community's pick is celebrated on the site; no badge is awarded.

## RENDER (one Web Service does everything)
Type: Web Service (NOT Static Site) · Runtime: Node · Root Directory: (leave EMPTY, or the folder name that holds package.json)
Build Command: npm install · Start Command: node server.js · Health Check Path: /healthz
Env: DB_FILE=/data/data.json · NODE_VERSION=22 · Disk: mount path /data (paid plan). Free plan: sleeps + forgets data on restart.
