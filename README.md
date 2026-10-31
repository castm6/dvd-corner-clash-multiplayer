# DVD Corner Clash

Room-code multiplayer DVD arcade game for the Handshake multiplayer challenge.

## Included

- Unlimited-player prediction rooms: bet `discs` on an exact corner bounce or a range from 1-500.
- Shared pot: exact predictions receive two payout shares and range predictions receive one.
- Two-player DVD Pong: top player uses `A` / `D`; bottom player uses left/right arrows; spectators can join a room and watch.
- Server-authoritative rooms, balances, pots, ball position, scores, and results.

## Run locally

Requires Node.js 20+.

The easiest option on Windows is to double-click `Start DVD Corner Clash.cmd`. A visible `DVD Corner Clash Server` window should open first, followed by the game in your normal browser. Keep the server window open while playing. Do not use ChatGPT's in-app browser for `localhost`; it is isolated from your computer's local server.

If an older game window is still running, double-click `Run Fixed DVD Corner Clash.cmd` instead. It starts the current build on private port `3002` and opens that version automatically.

```powershell
cd C:\Users\marce\Documents\Codex\dvd-corner-clash-multiplayer
npm start
```

Open `http://localhost:3000` in two browser windows. Create a room in the first, then join its six-character code from the second.

## Publish

This is a standard Node HTTP/WebSocket server. Deploy the whole folder to a Node-capable host such as Render, Railway, Fly.io, or a similar service. The host must run `npm start` and expose the `PORT` environment variable.
