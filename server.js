import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "public");
const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();
const clients = new Set();

const WIDTH = 1000;
const HEIGHT = 620;
const TICK_MS = 1000 / 60;
const MAX_DISCS = 9999;
const PONG_SEATS = ["top", "bottom", "left", "right"];
const PREDICTION_POWERS = {
  mini: { price: 15, cooldown: 15 },
  scramble: { price: 55, cooldown: 150 },
  turbo: { price: 28, cooldown: 25, duration: 15 },
  slow: { price: 22, cooldown: 25, duration: 15 },
  grow: { price: 24, cooldown: 25, duration: 15 },
  shrink: { price: 20, cooldown: 25, duration: 15 },
  fire: { price: 36, cooldown: 75, duration: 60 },
};
const DERBY_POWERS = {
  turbo: { price: 22, cooldown: 20, duration: 12 },
  slow: { price: 18, cooldown: 20, duration: 12 },
  grow: { price: 20, cooldown: 20, duration: 12 },
  shrink: { price: 18, cooldown: 20, duration: 12 },
  chaos: { price: 48, cooldown: 35, duration: 10 },
};

const server = http.createServer((req, res) => {
  const requestedPath = req.url === "/" ? "/index.html" : req.url.split("?")[0];
  const safePath = path.normalize(requestedPath).replace(/^([.][.][/\\])+/, "");
  const filePath = path.join(publicDir, safePath);

  if (!filePath.startsWith(publicDir) || !fs.existsSync(filePath)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not found");
    return;
  }

  const type = filePath.endsWith(".html") ? "text/html; charset=utf-8" : filePath.endsWith(".mp4") ? "video/mp4" : "text/plain; charset=utf-8";
  res.writeHead(200, { "Content-Type": type, "Cache-Control": "no-store" });
  fs.createReadStream(filePath).pipe(res);
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) {
    socket.destroy();
    return;
  }

  const accept = crypto
    .createHash("sha1")
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest("base64");

  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "\r\n",
  ].join("\r\n"));

  const client = {
    id: crypto.randomUUID(),
    socket,
    buffer: Buffer.alloc(0),
    roomCode: null,
    playerId: null,
  };

  clients.add(client);
  sendRoomList(client);

  socket.on("data", (chunk) => receiveTextFrame(client, chunk));
  socket.on("close", () => disconnect(client));
  socket.on("error", () => disconnect(client));
});

// Browser clients mask frames. This parser preserves incomplete TCP packets and
// reconstructs each text frame before it reaches the room game logic.
function receiveTextFrame(client, data) {
  let buffer = Buffer.concat([client.pending || Buffer.alloc(0), data]);

  while (buffer.length >= 2) {
    const first = buffer[0];
    const second = buffer[1];
    const opcode = first & 0x0f;
    const masked = (second & 0x80) !== 0;
    let payloadLength = second & 0x7f;
    let offset = 2;

    if (payloadLength === 126) {
      if (buffer.length < 4) break;
      payloadLength = buffer.readUInt16BE(offset);
      offset += 2;
    } else if (payloadLength === 127) {
      if (buffer.length < 10) break;
      const value = buffer.readBigUInt64BE(offset);
      if (value > BigInt(1024 * 1024)) {
        client.socket.destroy();
        return;
      }
      payloadLength = Number(value);
      offset += 8;
    }

    const mask = masked ? buffer.subarray(offset, offset + 4) : null;
    if (masked) offset += 4;
    if (buffer.length < offset + payloadLength) break;

    const payload = Buffer.from(buffer.subarray(offset, offset + payloadLength));
    buffer = buffer.subarray(offset + payloadLength);

    if (masked) {
      for (let index = 0; index < payload.length; index += 1) {
        payload[index] ^= mask[index % 4];
      }
    }

    if (opcode === 0x8) {
      client.socket.end();
      return;
    }
    if (opcode === 0x9) {
      sendFrame(client.socket, payload, 0xA);
      continue;
    }
    if (opcode !== 0x1) continue;

    try {
      handleMessage(client, JSON.parse(payload.toString("utf8")));
    } catch {
      send(client, { type: "error", message: "That request could not be read." });
    }
  }

  client.pending = buffer;
}

function createRoom(requestedCode, isPublic, solo) {
  let code = normalizeRoomCode(requestedCode);
  if (code && rooms.has(code)) return null;
  if (!code) do { code = crypto.randomBytes(3).toString("hex").toUpperCase(); } while (rooms.has(code));

  const room = {
    code,
    isPublic: Boolean(isPublic) && !solo,
    solo: Boolean(solo),
    createdAt: Date.now(),
    hostId: null,
    clients: new Set(),
    players: new Map(),
    phase: "lobby",
    mode: "prediction",
    predictionStyle: "chance",
    derbyStyle: "chance",
    maxPlayers: 20,
    prediction: null,
    pong: null,
    derby: null,
  };
  rooms.set(code, room);
  return room;
}

function normalizeRoomCode(value) {
  const code = String(value || "").trim().toUpperCase();
  if (!code) return "";
  return /^[A-Z0-9]{4,10}$/.test(code) ? code : null;
}

function makePlayer(name) {
  return {
    id: crypto.randomUUID(),
    name: sanitizeName(name),
    discs: 100,
    roundWon: 0,
    connected: true,
    seat: null,
    input: 0,
    dodge: { up: false, down: false, left: false, right: false },
  };
}

function addSoloBots(room, humanId) {
  for (let index = 1; index <= 9; index += 1) {
    const bot = makePlayer(`CPU ${index}`); bot.bot = true; room.players.set(bot.id, bot);
  }
  const human = room.players.get(humanId);
  if (human) human.seat = "bottom";
  const firstBot = [...room.players.values()].find((player) => player.bot);
  if (firstBot) firstBot.seat = "top";
}

function sendRoomList(client) {
  if (client.roomCode) return;
  send(client, { type: "rooms", rooms: [...rooms.values()].filter((room) => room.isPublic && room.phase !== "results").map((room) => ({ code: room.code, mode: room.mode, phase: room.phase, playerCount: [...room.players.values()].filter((player) => player.connected || player.bot).length })) });
}

function broadcastRoomList() { for (const client of clients) sendRoomList(client); }

function sanitizeName(name) {
  const value = String(name || "Player").trim().replace(/[^a-zA-Z0-9 _-]/g, "");
  return (value || "Player").slice(0, 16);
}

function handleMessage(client, message) {
  if (!message || typeof message.type !== "string") return;

  if (message.type === "create-room") {
    const requested = normalizeRoomCode(message.code);
    if (requested === null) { send(client, { type: "error", message: "Custom room codes need 4-10 letters or numbers." }); return; }
    const room = createRoom(requested, message.isPublic, message.solo);
    if (!room) { send(client, { type: "error", message: "That room code is already taken." }); return; }
    joinRoom(client, room, message.name, true);
    if (room.solo) addSoloBots(room, client.playerId);
    broadcastRoomList();
    return;
  }

  if (message.type === "join-room") {
    const room = rooms.get(String(message.code || "").trim().toUpperCase());
    if (!room) {
      send(client, { type: "error", message: "Room not found. Check the code and try again." });
      return;
    }
    joinRoom(client, room, message.name, false);
    return;
  }

  const room = rooms.get(client.roomCode);
  const player = room?.players.get(client.playerId);
  if (!room || !player) {
    send(client, { type: "error", message: "Join a room first." });
    return;
  }

  if (message.type === "set-mode") {
    if (!isHost(room, player) || room.phase !== "lobby") return;
    room.mode = message.mode === "pong" ? "pong" : message.mode === "derby" ? "derby" : "prediction";
    if (message.derbyStyle === "normal" || message.derbyStyle === "chance") room.derbyStyle = message.derbyStyle;
    if (room.mode === "derby") room.derby = createDerby(room.derbyStyle);
    else room.derby = null;
    if (message.predictionStyle === "normal" || message.predictionStyle === "chance") {
      room.predictionStyle = message.predictionStyle;
    }
    broadcastState(room);
    return;
  }

  if (message.type === "set-room-cap") {
    if (!isHost(room, player) || room.phase !== "lobby") return;
    room.maxPlayers = message.maxPlayers === "unlimited" ? null : 20;
    broadcastState(room);
    return;
  }

  if (message.type === "take-seat") {
    takePongSeat(room, player, message.seat);
    return;
  }

  if (message.type === "paddle") {
    if (room.phase === "pong" && player.seat && Number.isFinite(message.direction)) {
      player.input = Math.max(-1, Math.min(1, Number(message.direction)));
    }
    return;
  }

  if (message.type === "place-bet") {
    placeBet(room, player, message);
    return;
  }

  if (message.type === "claim-derby-logo") { claimDerbyLogo(room, player, message.logoId); return; }
  if (message.type === "place-derby-bet") { placeDerbyBet(room, player, message); return; }
  if (message.type === "buy-derby-power") { buyDerbyPower(room, player, message.power, message.target); return; }

  if (message.type === "place-mini-bet") {
    placeMiniBet(room, player, message);
    return;
  }

  if (message.type === "buy-power") {
    buyPredictionPower(room, player, message.power);
    return;
  }

  if (message.type === "dodge") {
    if (room.prediction?.fire?.phase === "active" || room.prediction?.fire?.phase === "countdown") {
      player.dodge = {
        up: Boolean(message.up), down: Boolean(message.down),
        left: Boolean(message.left), right: Boolean(message.right),
      };
    }
    return;
  }

  if (message.type === "reset-angle") {
    if (isHost(room, player) && room.phase === "resolving" && room.prediction?.ball) scramblePrediction(room.prediction);
    return;
  }

  if (message.type === "reset-derby-angles") {
    if (isHost(room, player) && room.phase === "derby-racing" && room.derby) resetDerbyAngles(room.derby);
    return;
  }

  if (message.type === "start-round") {
    if (!isHost(room, player)) return;
    if (room.mode === "pong") startPong(room);
    else if (room.mode === "derby") startDerbyBetting(room);
    else startPredictionBetting(room);
    return;
  }

  if (message.type === "restart") {
    if (!isHost(room, player)) return;
    resetToLobby(room);
  }
}

function joinRoom(client, room, name, isHost) {
  const connectedPlayers = [...room.players.values()].filter((candidate) => candidate.connected).length;
  if (!isHost && room.maxPlayers !== null && connectedPlayers >= room.maxPlayers) {
    send(client, { type: "error", message: "This room has reached its 20-player limit." });
    return;
  }
  const player = makePlayer(name);
  room.clients.add(client);
  room.players.set(player.id, player);
  client.roomCode = room.code;
  client.playerId = player.id;
  if (isHost || !room.hostId) room.hostId = player.id;
  send(client, { type: "joined", roomCode: room.code, playerId: player.id });
  broadcastState(room);
  broadcastRoomList();
}

function disconnect(client) {
  clients.delete(client);
  const room = rooms.get(client.roomCode);
  if (!room) return;

  room.clients.delete(client);
  const player = room.players.get(client.playerId);
  if (player) {
    player.connected = false;
    player.input = 0;
    if (room.phase === "pong" && player.seat && room.pong) {
      room.pong.seatIds[player.seat] = null;
      const remaining = activePongSeats(room, room.pong);
      if (remaining.length <= 1) finishPong(room, remaining[0], "Opponents disconnected");
    }
  }

  if (room.clients.size === 0) {
    rooms.delete(room.code);
    broadcastRoomList();
  } else {
    if (room.hostId === client.playerId) {
      const nextHost = [...room.players.values()].find((candidate) => candidate.connected);
      room.hostId = nextHost?.id || null;
    }
    broadcastState(room);
    broadcastRoomList();
  }
}

function isHost(room, player) {
  return room.hostId === player.id;
}

function takePongSeat(room, player, requestedSeat) {
  if (room.phase !== "lobby") return;
  const seat = PONG_SEATS.includes(requestedSeat) ? requestedSeat : "bottom";
  const occupant = [...room.players.values()].find((candidate) => candidate.seat === seat && candidate.id !== player.id);
  if (occupant) {
    sendToPlayer(room, player.id, { type: "error", message: "That seat is already taken." });
    return;
  }

  for (const candidate of room.players.values()) {
    if (candidate.id === player.id) candidate.seat = seat;
  }
  broadcastState(room);
}

function createDerby(style = "chance") {
  return {
    style,
    logos: Array.from({ length: 10 }, (_, index) => ({ id: index + 1, ownerId: null, effects: [], ball: createDerbyBall(index), bounces: 0, target: style === "chance" ? randomInt(35, 500) : null, aimedForCorner: false })),
    bets: [], pot: 0, cooldowns: {}, closesAt: null, lastTick: null, winnerId: null,
  };
}

function createDerbyBall(index) {
  const column = index % 5, row = Math.floor(index / 5);
  const angle = Math.random() * Math.PI * 2, speed = 285 + randomInt(0, 70);
  return { x: 70 + column * 180, y: 90 + row * 340, width: 102, height: 56, vx: Math.cos(angle) * speed, vy: Math.sin(angle) * speed, hue: (index * 36 + randomInt(0, 20)) % 360, effectSpeedFactor: 1 };
}

function claimDerbyLogo(room, player, rawId) {
  if (room.mode !== "derby" || room.phase !== "lobby" || !room.derby) return;
  const logoId = Math.floor(Number(rawId));
  const logo = room.derby.logos.find((entry) => entry.id === logoId);
  if (!logo || logo.ownerId) { sendToPlayer(room, player.id, { type: "error", message: "That DVD racer is already claimed." }); return; }
  const alreadyOwns = room.derby.logos.some((entry) => entry.ownerId === player.id);
  if (alreadyOwns) { sendToPlayer(room, player.id, { type: "error", message: "You can only own one Derby logo." }); return; }
  logo.ownerId = player.id;
  broadcastState(room);
}

function startDerbyBetting(room) {
  if (room.phase !== "lobby" || !room.derby) return;
  for (const player of room.players.values()) player.roundWon = 0;
  const derby = room.derby;
  derby.bets = []; derby.pot = 0; derby.winnerId = null; derby.closesAt = Date.now() + 22000;
  derby.logos.forEach((logo, index) => { logo.ball = createDerbyBall(index); logo.bounces = 0; logo.target = derby.style === "chance" ? randomInt(35, 500) : null; logo.effects = []; logo.aimedForCorner = false; });
  room.phase = "derby-betting";
  derby.bettingTimer = setTimeout(() => beginDerbyCountdown(room), 22000);
  broadcastState(room);
  if (room.solo) setTimeout(() => autoDerbyBets(room), 400);
}

function autoDerbyBets(room) {
  if (room.phase !== "derby-betting" || !room.derby) return;
  for (const bot of room.players.values()) {
    if (!bot.bot || room.derby.bets.some((bet) => bet.playerId === bot.id)) continue;
    const stake = Math.min(bot.discs, randomInt(4, 14)); const logoId = randomInt(1, 10);
    bot.discs -= stake; room.derby.pot += stake; room.derby.bets.push({ playerId: bot.id, playerName: bot.name, logoId, stake });
  }
  broadcastState(room);
}

function beginDerbyCountdown(room) {
  if (room.phase !== "derby-betting" || !room.derby) return;
  room.phase = "derby-countdown";
  room.derby.countdownEndsAt = Date.now() + 5000;
  broadcastState(room);
  room.derby.countdownTimer = setTimeout(() => startDerbyRace(room), 5000);
}

function placeDerbyBet(room, player, payload) {
  const derby = room.derby, logoId = Math.floor(Number(payload.logoId)), stake = Math.floor(Number(payload.stake));
  if (room.phase !== "derby-betting" || !derby) return;
  if (!derby.logos.some((logo) => logo.id === logoId) || !Number.isInteger(stake) || stake < 1 || stake > player.discs) {
    sendToPlayer(room, player.id, { type: "error", message: "Choose a racer and a stake you can afford." }); return;
  }
  if (derby.bets.some((bet) => bet.playerId === player.id)) { sendToPlayer(room, player.id, { type: "error", message: "You already backed a racer this Derby." }); return; }
  player.discs -= stake; derby.pot += stake; derby.bets.push({ playerId: player.id, playerName: player.name, logoId, stake });
  broadcastState(room);
}

function startDerbyRace(room) {
  if (room.phase !== "derby-countdown" || !room.derby) return;
  room.phase = "derby-racing"; room.derby.lastTick = Date.now(); broadcastState(room);
}

function resetDerbyAngles(derby) {
  for (const logo of derby.logos) {
    const speed = Math.hypot(logo.ball.vx, logo.ball.vy) || 320;
    const angle = Math.random() * Math.PI * 2;
    logo.ball.vx = Math.cos(angle) * speed;
    logo.ball.vy = Math.sin(angle) * speed;
    logo.aimedForCorner = false;
    if (derby.style === "chance") logo.target = Math.max(logo.bounces + 1, logo.target);
  }
}

function buyDerbyPower(room, player, key, rawTarget) {
  const derby = room.derby, power = DERBY_POWERS[key], target = Math.floor(Number(rawTarget));
  if (room.mode !== "derby" || room.phase !== "derby-racing" || !derby || !power) return;
  if (key !== "chaos" && !derby.logos.some((logo) => logo.id === target)) { sendToPlayer(room, player.id, { type: "error", message: "Choose a Derby logo to affect." }); return; }
  const remaining = (derby.cooldowns[key] || 0) - Date.now();
  if (remaining > 0) { sendToPlayer(room, player.id, { type: "error", message: `${key} is cooling down for ${Math.ceil(remaining / 1000)}s.` }); return; }
  if (player.discs < power.price) { sendToPlayer(room, player.id, { type: "error", message: `You need ${power.price} discs for this power.` }); return; }
  player.discs -= power.price; derby.cooldowns[key] = Date.now() + power.cooldown * 1000;
  if (key === "chaos") {
    derby.logos.forEach((logo) => logo.effects.push({ type: ["turbo", "slow", "grow", "shrink"][randomInt(0, 3)], expiresAt: Date.now() + power.duration * 1000 }));
  } else {
    derby.logos.find((logo) => logo.id === target).effects.push({ type: key, expiresAt: Date.now() + power.duration * 1000 });
  }
  derby.logos.forEach(applyDerbyEffects); broadcastState(room);
}

function applyDerbyEffects(logo) {
  const ball = logo.ball; logo.effects = logo.effects.filter((effect) => effect.expiresAt > Date.now());
  const speedFactor = logo.effects.reduce((total, effect) => total * (effect.type === "turbo" ? 1.4 : effect.type === "slow" ? 0.7 : 1), 1);
  const sizeFactor = logo.effects.reduce((total, effect) => total * (effect.type === "grow" ? 1.32 : effect.type === "shrink" ? 0.72 : 1), 1);
  const x = ball.x + ball.width / 2, y = ball.y + ball.height / 2, previous = ball.effectSpeedFactor || 1;
  ball.vx *= speedFactor / previous; ball.vy *= speedFactor / previous; ball.effectSpeedFactor = speedFactor;
  ball.width = 102 * sizeFactor; ball.height = 56 * sizeFactor; ball.x = clamp(x - ball.width / 2, 0, WIDTH - ball.width); ball.y = clamp(y - ball.height / 2, 0, HEIGHT - ball.height);
}

function stepDerby(room, deltaSeconds) {
  const derby = room.derby; if (!derby || derby.winnerId) return;
  for (const logo of derby.logos) {
    applyDerbyEffects(logo);
    if (derby.style === "chance" && logo.bounces === logo.target - 1 && !logo.aimedForCorner) { aimPredictionAtCorner(logo.ball); logo.aimedForCorner = true; }
    let cornered = false;
    stepMovingBall(logo.ball, deltaSeconds, (both) => { logo.bounces += 1; if (both) { cornered = true; return true; } return false; });
    if (cornered) { finishDerby(room, logo.id); return; }
  }
}

function finishDerby(room, winnerId) {
  const derby = room.derby; derby.winnerId = winnerId;
  const winningBets = derby.bets.filter((bet) => bet.logoId === winnerId);
  const winnerLogo = derby.logos.find((logo) => logo.id === winnerId);
  const ownerRoyalty = winnerLogo?.ownerId ? Math.floor(derby.pot * 0.08) : 0;
  const owner = room.players.get(winnerLogo?.ownerId);
  if (owner && ownerRoyalty) { owner.discs = Math.min(MAX_DISCS, owner.discs + ownerRoyalty); owner.roundWon += ownerRoyalty; }
  const distributable = derby.pot - ownerRoyalty, totalStake = winningBets.reduce((sum, bet) => sum + bet.stake, 0);
  for (const bet of winningBets) { const player = room.players.get(bet.playerId); if (!player) continue; const payout = totalStake ? Math.floor(distributable * bet.stake / totalStake) : 0; player.discs = Math.min(MAX_DISCS, player.discs + payout); player.roundWon += payout; bet.payout = payout; }
  room.phase = "results"; derby.winningBets = winningBets; broadcastState(room);
  derby.resultsTimer = setTimeout(() => resetToLobby(room), 9000);
}

function startPredictionBetting(room, continuing = false) {
  if (!continuing && room.phase !== "lobby") return;
  for (const player of room.players.values()) player.roundWon = 0;
  room.phase = "betting";
  const previous = continuing ? room.prediction : null;
  room.prediction = {
    roundId: crypto.randomUUID(),
    mode: room.predictionStyle,
    bets: [],
    closesAt: Date.now() + 20000,
    result: null,
    ball: previous?.ball || null,
    bounces: previous?.bounces || 0,
    startBounces: previous?.bounces || 0,
    hitAt: null,
    aimedForCorner: false,
    lastTick: null,
    pot: 0,
    effects: [],
    cooldowns: previous?.cooldowns || {},
    mini: null,
    fire: null,
    frozen: false,
  };
  broadcastState(room);
  if (room.solo) setTimeout(() => autoPredictionBets(room), 400);

  room.prediction.bettingTimer = setTimeout(() => beginPredictionCountdown(room), 20000);
}

function autoPredictionBets(room) {
  if (room.phase !== "betting" || !room.prediction) return;
  for (const bot of room.players.values()) {
    if (!bot.bot || room.prediction.bets.some((bet) => bet.playerId === bot.id)) continue;
    const min = randomInt(1, 440), max = Math.min(500, min + randomInt(0, 30)), stake = Math.min(bot.discs, randomInt(3, 12));
    bot.discs -= stake; room.prediction.pot += stake; room.prediction.bets.push({ playerId: bot.id, playerName: bot.name, kind: min === max ? "exact" : "range", min, max, stake });
  }
  broadcastState(room);
}

function beginPredictionCountdown(room) {
  if (room.phase !== "betting" || !room.prediction) return;
  room.phase = "countdown";
  room.prediction.countdownEndsAt = Date.now() + 5000;
  broadcastState(room);
  room.prediction.countdownTimer = setTimeout(() => beginPredictionAnimation(room), 5000);
}

function placeBet(room, player, payload) {
  if (room.phase !== "betting" || !room.prediction) return;
  const prediction = room.prediction;
  const stake = Math.floor(Number(payload.stake));
  const kind = payload.kind === "exact" ? "exact" : "range";
  const min = Math.floor(Number(payload.min));
  const max = kind === "exact" ? min : Math.floor(Number(payload.max));

  const width = max - min + 1;
  const stakeCap = maxStakeForBet(player.discs, kind, width);
  if (!Number.isInteger(stake) || stake < 1 || stake > stakeCap) {
    sendToPlayer(room, player.id, { type: "error", message: `This call can stake at most ${stakeCap} discs.` });
    return;
  }
  if (stake > player.discs) {
    sendToPlayer(room, player.id, { type: "error", message: "You need enough discs for that bet." });
    return;
  }
  if (!Number.isInteger(min) || !Number.isInteger(max) || min < 1 || max > 500 || min > max) {
    sendToPlayer(room, player.id, { type: "error", message: "Choose a bounce number or range from 1 to 500." });
    return;
  }
  if (kind === "range" && max - min > 149) {
    sendToPlayer(room, player.id, { type: "error", message: "Ranges can be up to 150 bounces wide." });
    return;
  }
  if (prediction.bets.some((bet) => bet.playerId === player.id)) {
    sendToPlayer(room, player.id, { type: "error", message: "You already locked a bet this round." });
    return;
  }

  player.discs -= stake;
  prediction.pot += stake;
  prediction.bets.push({ playerId: player.id, playerName: player.name, kind, min, max, stake });
  broadcastState(room);
}

function beginPredictionAnimation(room) {
  if (room.phase !== "countdown" || !room.prediction) return;
  const prediction = room.prediction;
  prediction.result = prediction.bounces + choosePredictionResult(prediction.mode);
  prediction.ball = prediction.ball || createPredictionBall();
  prediction.lastTick = Date.now();
  room.phase = "resolving";
  broadcastState(room);
}

function createPredictionBall() {
  const speed = 520;
  const angle = Math.random() * Math.PI * 2;
  return {
    x: randomInt(80, WIDTH - 230),
    y: randomInt(80, HEIGHT - 150),
    width: 150,
    height: 82,
    vx: Math.cos(angle) * speed,
    vy: Math.sin(angle) * speed,
    hue: randomInt(0, 359),
  };
}

function stepPrediction(room, deltaSeconds) {
  const prediction = room.prediction;
  const ball = prediction?.ball;
  if (!prediction || !ball) return;
  if (prediction.hitAt) { stepMini(room, deltaSeconds); return; }

  if (prediction.frozen) {
    stepFireDodge(room, deltaSeconds);
    return;
  }
  stepFireDodge(room, deltaSeconds);
  applyPredictionEffects(prediction);

  let remaining = deltaSeconds;
  let safety = 0;
  while (remaining > 0 && safety < 10) {
    safety += 1;
    if (prediction.bounces === prediction.result - 1 && !prediction.aimedForCorner) {
      aimPredictionAtCorner(ball);
      prediction.aimedForCorner = true;
    }

    const maxX = WIDTH - ball.width;
    const maxY = HEIGHT - ball.height;
    const timeToVertical = ball.vx > 0 ? (maxX - ball.x) / ball.vx : (0 - ball.x) / ball.vx;
    const timeToHorizontal = ball.vy > 0 ? (maxY - ball.y) / ball.vy : (0 - ball.y) / ball.vy;
    const tx = timeToVertical >= 0 ? timeToVertical : Infinity;
    const ty = timeToHorizontal >= 0 ? timeToHorizontal : Infinity;
    const hitTime = Math.min(tx, ty);

    if (!Number.isFinite(hitTime) || hitTime > remaining) {
      ball.x += ball.vx * remaining;
      ball.y += ball.vy * remaining;
      break;
    }

    ball.x += ball.vx * hitTime;
    ball.y += ball.vy * hitTime;
    remaining -= hitTime;
    const hitVertical = Math.abs(tx - hitTime) < 0.00001;
    const hitHorizontal = Math.abs(ty - hitTime) < 0.00001;
    ball.x = clamp(ball.x, 0, maxX);
    ball.y = clamp(ball.y, 0, maxY);
    prediction.bounces += 1;
    ball.hue = (ball.hue + 31) % 360;

    if (hitVertical) ball.vx *= -1;
    if (hitHorizontal) ball.vy *= -1;

    if (hitVertical && hitHorizontal) {
      prediction.hitAt = prediction.bounces;
      if (prediction.mini?.phase !== "active") prediction.resolveTimer = setTimeout(() => resolvePrediction(room), 1400);
      break;
    }
  }
  stepMini(room, deltaSeconds);
}

function aimPredictionAtCorner(ball) {
  const maxX = WIDTH - ball.width;
  const maxY = HEIGHT - ball.height;
  const corners = [
    { x: 0, y: 0 },
    { x: maxX, y: 0 },
    { x: 0, y: maxY },
    { x: maxX, y: maxY },
  ].filter((corner) => Math.abs(corner.x - ball.x) > 2 && Math.abs(corner.y - ball.y) > 2);
  const target = corners[randomInt(0, corners.length - 1)] || { x: 0, y: 0 };
  const dx = target.x - ball.x;
  const dy = target.y - ball.y;
  const distance = Math.hypot(dx, dy) || 1;
  const speed = Math.hypot(ball.vx, ball.vy) || 520;
  ball.vx = (dx / distance) * speed;
  ball.vy = (dy / distance) * speed;
}

function choosePredictionResult(mode) {
  if (mode === "normal") return randomInt(1, 500);
  const roll = Math.random();
  if (roll < 0.12) return randomInt(1, 90);
  if (roll > 0.88) return randomInt(410, 500);
  return randomInt(91, 409);
}

function resolvePrediction(room) {
  if (room.phase !== "resolving" || !room.prediction?.result) return;
  const prediction = room.prediction;
  const relativeResult = prediction.result - prediction.startBounces;
  const winners = prediction.bets.filter((bet) => relativeResult >= bet.min && relativeResult <= bet.max);
  const totalShares = winners.reduce((sum, bet) => sum + (bet.kind === "exact" ? 2 : 1), 0);

  if (totalShares > 0) {
    for (const winner of winners) {
      const player = room.players.get(winner.playerId);
      if (!player) continue;
      const shares = winner.kind === "exact" ? 2 : 1;
      const payout = Math.floor((prediction.pot * shares) / totalShares);
      player.discs = Math.min(MAX_DISCS, player.discs + payout);
      player.roundWon += payout;
    }
  }

  room.phase = "results";
  prediction.winners = winners.map((winner) => ({ ...winner, shares: winner.kind === "exact" ? 2 : 1 }));
  broadcastState(room);
  prediction.resultsTimer = setTimeout(() => {
    if (room.predictionStyle === "normal") startPredictionBetting(room, true);
    else resetToLobby(room);
  }, 8500);
}

function maxStakeForBet(discs, kind, width) {
  if (kind === "exact" || width <= 14) return discs;
  const fraction = Math.max(0.2, 0.85 - ((width - 15) / 135) * 0.65);
  return Math.max(1, Math.floor(discs * fraction));
}

function buyPredictionPower(room, player, key) {
  const prediction = room.prediction;
  const power = PREDICTION_POWERS[key];
  if (!prediction || room.phase !== "resolving" || !power) return;
  if (key === "fire" && room.maxPlayers === null) {
    sendToPlayer(room, player.id, { type: "error", message: "Fire Dodge is disabled in unlimited rooms." });
    return;
  }
  const remaining = Math.max(0, (prediction.cooldowns[key] || 0) - Date.now());
  if (remaining) {
    sendToPlayer(room, player.id, { type: "error", message: `${key} is cooling down for ${Math.ceil(remaining / 1000)}s.` });
    return;
  }
  if (player.discs < power.price) {
    sendToPlayer(room, player.id, { type: "error", message: `You need ${power.price} discs for this power.` });
    return;
  }
  player.discs -= power.price;
  prediction.cooldowns[key] = Date.now() + power.cooldown * 1000;

  if (key === "mini") {
    prediction.frozen = true;
    prediction.mini = { phase: "betting", closesAt: Date.now() + 8000, bets: [], pot: 0, ownerId: player.id };
    room.phase = "mini-betting";
    prediction.mini.timer = setTimeout(() => launchMini(room), 8000);
  } else if (key === "scramble") {
    scramblePrediction(prediction);
  } else if (key === "fire") {
    prediction.frozen = true;
    prediction.fire = { phase: "countdown", countdown: 5, players: {} };
    for (const entry of room.players.values()) {
      if (entry.connected) prediction.fire.players[entry.id] = { x: randomInt(30, WIDTH - 56), y: randomInt(30, HEIGHT - 56), hitCooldown: 0 };
    }
  } else {
    prediction.effects.push({ type: key, expiresAt: Date.now() + power.duration * 1000 });
    applyPredictionEffects(prediction);
  }
  broadcastState(room);
}

function scramblePrediction(prediction) {
  const speed = Math.hypot(prediction.ball.vx, prediction.ball.vy) || 520;
  const angle = Math.random() * Math.PI * 2;
  prediction.ball.vx = Math.cos(angle) * speed;
  prediction.ball.vy = Math.sin(angle) * speed;
}

function applyPredictionEffects(prediction) {
  const ball = prediction.ball;
  if (!ball) return;
  prediction.effects = prediction.effects.filter((effect) => effect.expiresAt > Date.now());
  const speedFactor = prediction.effects.reduce((total, effect) => total * (effect.type === "turbo" ? 1.45 : effect.type === "slow" ? 0.68 : effect.type === "fire" ? 1.6 : 1), 1);
  const sizeFactor = prediction.effects.reduce((total, effect) => total * (effect.type === "grow" ? 1.42 : effect.type === "shrink" ? 0.62 : 1), 1);
  const currentSpeed = ball.effectSpeedFactor || 1;
  const centerX = ball.x + ball.width / 2;
  const centerY = ball.y + ball.height / 2;
  ball.vx *= speedFactor / currentSpeed;
  ball.vy *= speedFactor / currentSpeed;
  ball.effectSpeedFactor = speedFactor;
  ball.width = 150 * sizeFactor;
  ball.height = 82 * sizeFactor;
  ball.x = clamp(centerX - ball.width / 2, 0, WIDTH - ball.width);
  ball.y = clamp(centerY - ball.height / 2, 0, HEIGHT - ball.height);
}

function placeMiniBet(room, player, payload) {
  const mini = room.prediction?.mini;
  const number = Math.floor(Number(payload.number));
  const stake = Math.floor(Number(payload.stake));
  if (room.phase !== "mini-betting" || !mini || mini.phase !== "betting") return;
  if (!Number.isInteger(number) || number < 1 || number > 10 || !Number.isInteger(stake) || stake < 1 || stake > player.discs) {
    sendToPlayer(room, player.id, { type: "error", message: "Choose one number from 1 to 10 and a valid stake." });
    return;
  }
  if (mini.bets.some((bet) => bet.playerId === player.id)) return;
  player.discs -= stake;
  mini.pot += stake;
  mini.bets.push({ playerId: player.id, playerName: player.name, number, stake });
  broadcastState(room);
}

function launchMini(room) {
  const prediction = room.prediction;
  const mini = prediction?.mini;
  if (!mini || room.phase !== "mini-betting") return;
  mini.phase = "active";
  mini.ball = { ...createPredictionBall(), width: 112, height: 62 };
  mini.target = randomInt(1, 10);
  mini.bounces = 0;
  room.phase = "resolving";
  prediction.frozen = false;
  broadcastState(room);
}

function stepMini(room, deltaSeconds) {
  const prediction = room.prediction;
  const mini = prediction?.mini;
  if (!mini || mini.phase !== "active") return;
  if (mini.bounces === mini.target - 1 && !mini.aimedForCorner) { aimPredictionAtCorner(mini.ball); mini.aimedForCorner = true; }
  stepMovingBall(mini.ball, deltaSeconds, (both) => {
    mini.bounces += 1;
    if (!both) return;
    const winners = mini.bets.filter((bet) => bet.number === mini.bounces);
    if (winners.length) {
      const share = Math.floor(mini.pot / winners.length);
      for (const bet of winners) {
        const winner = room.players.get(bet.playerId);
        if (winner) { winner.discs = Math.min(MAX_DISCS, winner.discs + share); winner.roundWon += share; }
      }
    }
    mini.winners = winners;
    mini.result = mini.bounces;
    mini.phase = "result";
    setTimeout(() => {
      if (prediction.mini !== mini) return;
      prediction.mini = null;
      if (prediction.hitAt) prediction.resolveTimer = setTimeout(() => resolvePrediction(room), 800);
    }, 3200);
  });
}

function stepMovingBall(ball, deltaSeconds, onHit) {
  let remaining = deltaSeconds;
  while (remaining > 0) {
    const maxX = WIDTH - ball.width, maxY = HEIGHT - ball.height;
    const tx = ball.vx > 0 ? (maxX - ball.x) / ball.vx : (0 - ball.x) / ball.vx;
    const ty = ball.vy > 0 ? (maxY - ball.y) / ball.vy : (0 - ball.y) / ball.vy;
    const hitTime = Math.min(tx >= 0 ? tx : Infinity, ty >= 0 ? ty : Infinity);
    if (!Number.isFinite(hitTime) || hitTime > remaining) { ball.x += ball.vx * remaining; ball.y += ball.vy * remaining; break; }
    ball.x += ball.vx * hitTime; ball.y += ball.vy * hitTime; remaining -= hitTime;
    const hitX = Math.abs(tx - hitTime) < 0.00001, hitY = Math.abs(ty - hitTime) < 0.00001;
    ball.x = clamp(ball.x, 0, maxX); ball.y = clamp(ball.y, 0, maxY);
    if (hitX) ball.vx *= -1; if (hitY) ball.vy *= -1;
    if (onHit(hitX && hitY)) break;
  }
}

function stepFireDodge(room, deltaSeconds) {
  const prediction = room.prediction, fire = prediction?.fire;
  if (!fire) return;
  if (fire.phase === "countdown") {
    fire.countdown -= deltaSeconds;
    if (fire.countdown <= 0) {
      fire.phase = "active";
      prediction.frozen = false;
      prediction.effects.push({ type: "fire", expiresAt: Date.now() + PREDICTION_POWERS.fire.duration * 1000 });
    }
    return;
  }
  if (fire.phase !== "active") return;
  const ball = prediction.ball;
  for (const player of room.players.values()) {
    const cube = fire.players[player.id]; if (!cube || !player.connected) continue;
    cube.x = clamp(cube.x + ((player.dodge.right ? 1 : 0) - (player.dodge.left ? 1 : 0)) * 330 * deltaSeconds, 0, WIDTH - 26);
    cube.y = clamp(cube.y + ((player.dodge.down ? 1 : 0) - (player.dodge.up ? 1 : 0)) * 330 * deltaSeconds, 0, HEIGHT - 26);
    cube.hitCooldown = Math.max(0, cube.hitCooldown - deltaSeconds);
    if (!cube.hitCooldown && cube.x < ball.x + ball.width && cube.x + 26 > ball.x && cube.y < ball.y + ball.height && cube.y + 26 > ball.y) {
      player.discs = Math.max(0, player.discs - 5); cube.hitCooldown = 0.8;
      cube.x = randomInt(20, WIDTH - 50); cube.y = randomInt(20, HEIGHT - 50);
    }
  }
  if (!prediction.effects.some((effect) => effect.type === "fire" && effect.expiresAt > Date.now())) prediction.fire = null;
}

function startPong(room) {
  if (room.phase !== "lobby") return;
  for (const player of room.players.values()) player.roundWon = 0;
  const seated = PONG_SEATS.map((seat) => [seat, [...room.players.values()].find((player) => player.seat === seat && player.connected)]);
  if (seated.filter(([, player]) => player).length < 2) {
    sendToPlayer(room, room.hostId, { type: "error", message: "Pong needs at least two players in any seats." });
    return;
  }

  room.phase = "pong";
  room.pong = {
    seatIds: Object.fromEntries(seated.map(([seat, player]) => [seat, player?.id || null])),
    score: Object.fromEntries(PONG_SEATS.map((seat) => [seat, 0])),
    paddle: {
      top: { x: WIDTH / 2 - 100, y: 36, width: 200, height: 16 },
      bottom: { x: WIDTH / 2 - 100, y: HEIGHT - 52, width: 200, height: 16 },
      left: { x: 36, y: HEIGHT / 2 - 100, width: 16, height: 200 },
      right: { x: WIDTH - 52, y: HEIGHT / 2 - 100, width: 16, height: 200 },
    },
    ball: createPongBall(seated.filter(([, player]) => player)[randomInt(0, seated.filter(([, player]) => player).length - 1)][0]),
    lastTick: Date.now(),
    power: null,
    powerTimer: 6,
    effect: null,
    lastHitSeat: null,
  };
  broadcastState(room);
}

function createPongBall(targetSeat = "top") {
  const speed = 360;
  const base = { top: -Math.PI / 2, bottom: Math.PI / 2, left: Math.PI, right: 0 }[targetSeat] ?? -Math.PI / 2;
  const angle = base + (Math.random() * 0.9 - 0.45);
  return {
    x: WIDTH / 2,
    y: HEIGHT / 2,
    width: 120,
    height: 66,
    vx: Math.cos(angle) * speed,
    vy: Math.sin(angle) * speed,
    speed,
    cornerBonuses: 0,
    cornerLatched: false,
  };
}

function stepPong(room, deltaSeconds) {
  const pong = room.pong;
  if (!pong) return;
  const activeSeats = activePongSeats(room, pong);
  if (activeSeats.length < 2) return;
  for (const seat of activeSeats) {
    const player = room.players.get(pong.seatIds[seat]);
    const paddle = pong.paddle[seat];
    if (player?.bot) player.input = clamp((seat === "top" || seat === "bottom" ? pong.ball.x + pong.ball.width / 2 - (paddle.x + paddle.width / 2) : pong.ball.y + pong.ball.height / 2 - (paddle.y + paddle.height / 2)) / 80, -1, 1);
  }

  const paddleSpeed = 620;
  for (const seat of activeSeats) {
    const player = room.players.get(pong.seatIds[seat]);
    const paddle = pong.paddle[seat];
    if (seat === "top" || seat === "bottom") paddle.x = clamp(paddle.x + player.input * paddleSpeed * deltaSeconds, 0, WIDTH - paddle.width);
    else paddle.y = clamp(paddle.y + player.input * paddleSpeed * deltaSeconds, 0, HEIGHT - paddle.height);
  }

  const ball = pong.ball;
  pong.powerTimer -= deltaSeconds;
  if (!pong.power && pong.powerTimer <= 0) {
    const types = ["turbo", "slow", "grow", "shrink", "fire"];
    pong.power = { type: types[randomInt(0, types.length - 1)], x: randomInt(130, WIDTH - 130), y: randomInt(160, HEIGHT - 160) };
    pong.powerTimer = 7;
  }
  if (pong.effect) {
    pong.effect.expiresAt -= deltaSeconds;
    if (pong.effect.expiresAt <= 0) clearPongEffect(pong);
  }
  ball.x += ball.vx * deltaSeconds;
  ball.y += ball.vy * deltaSeconds;

  for (const [seat, direction] of [["top", 1], ["bottom", -1], ["left", 1], ["right", -1]]) {
    const movingToward = (seat === "top" && ball.vy < 0) || (seat === "bottom" && ball.vy > 0) || (seat === "left" && ball.vx < 0) || (seat === "right" && ball.vx > 0);
    if (!movingToward || !activeSeats.includes(seat) || !collides(ball, pong.paddle[seat])) continue;
    if (pong.effect?.type === "fire") { scorePong(room, choosePongScorer(room, pong, seat), seat); return; }
    if (seat === "top" || seat === "bottom") hitPaddle(ball, pong.paddle[seat], direction);
    else hitPaddleVertical(ball, pong.paddle[seat], direction);
    pong.lastHitSeat = seat;
  }

  if (pong.power && Math.hypot(ball.x + ball.width / 2 - pong.power.x, ball.y + ball.height / 2 - pong.power.y) < 52) {
    applyPongPower(pong, pong.power.type);
    pong.power = null;
  }

  if (pong.effect?.type === "fire") {
    if (ball.x <= 0 || ball.x + ball.width >= WIDTH) { ball.x = clamp(ball.x, 0, WIDTH - ball.width); ball.vx *= -1; }
    if (ball.y <= 0 || ball.y + ball.height >= HEIGHT) { ball.y = clamp(ball.y, 0, HEIGHT - ball.height); ball.vy *= -1; }
    return;
  }

  if (!activeSeats.includes("left") && ball.x <= 0) { ball.x = 0; ball.vx = Math.abs(ball.vx); }
  if (!activeSeats.includes("right") && ball.x + ball.width >= WIDTH) { ball.x = WIDTH - ball.width; ball.vx = -Math.abs(ball.vx); }
  if (!activeSeats.includes("top") && ball.y <= 0) { ball.y = 0; ball.vy = Math.abs(ball.vy); }
  if (!activeSeats.includes("bottom") && ball.y + ball.height >= HEIGHT) { ball.y = HEIGHT - ball.height; ball.vy = -Math.abs(ball.vy); }

  const touchingTopCorner = (ball.x <= 1 || ball.x + ball.width >= WIDTH - 1) && ball.y <= 1;
  if (touchingTopCorner && !ball.cornerLatched) {
    ball.cornerBonuses += 1;
    ball.cornerLatched = true;
  }
  if (!touchingTopCorner) {
    ball.cornerLatched = false;
  }

  if (activeSeats.includes("top") && ball.y + ball.height < -30) scorePong(room, choosePongScorer(room, pong, "top"), "top");
  if (activeSeats.includes("bottom") && ball.y > HEIGHT + 30) scorePong(room, choosePongScorer(room, pong, "bottom"), "bottom");
  if (activeSeats.includes("left") && ball.x + ball.width < -30) scorePong(room, choosePongScorer(room, pong, "left"), "left");
  if (activeSeats.includes("right") && ball.x > WIDTH + 30) scorePong(room, choosePongScorer(room, pong, "right"), "right");
}

function activePongSeats(room, pong) { return PONG_SEATS.filter((seat) => { const player = room.players.get(pong.seatIds[seat]); return player?.connected; }); }

function choosePongScorer(room, pong, missedSeat) { const options = activePongSeats(room, pong).filter((seat) => seat !== missedSeat); return options.includes(pong.lastHitSeat) ? pong.lastHitSeat : options[randomInt(0, options.length - 1)]; }

function clearPongEffect(pong) {
  const effect = pong.effect, ball = pong.ball;
  if (!effect) return;
  if (effect.type === "turbo") { ball.vx /= 1.45; ball.vy /= 1.45; ball.speed /= 1.45; }
  if (effect.type === "slow") { ball.vx /= 0.68; ball.vy /= 0.68; ball.speed /= 0.68; }
  if (effect.type === "grow" || effect.type === "shrink") {
    const x = ball.x + ball.width / 2, y = ball.y + ball.height / 2;
    ball.width = 120; ball.height = 66; ball.x = clamp(x - 60, 0, WIDTH - 120); ball.y = clamp(y - 33, 0, HEIGHT - 66);
  }
  pong.effect = null;
}

function applyPongPower(pong, type) {
  clearPongEffect(pong);
  const ball = pong.ball;
  pong.effect = { type, expiresAt: 6 };
  if (type === "turbo") { ball.vx *= 1.45; ball.vy *= 1.45; ball.speed *= 1.45; }
  if (type === "slow") { ball.vx *= 0.68; ball.vy *= 0.68; ball.speed *= 0.68; }
  if (type === "grow" || type === "shrink") {
    const factor = type === "grow" ? 1.42 : 0.62, x = ball.x + ball.width / 2, y = ball.y + ball.height / 2;
    ball.width = 120 * factor; ball.height = 66 * factor; ball.x = clamp(x - ball.width / 2, 0, WIDTH - ball.width); ball.y = clamp(y - ball.height / 2, 0, HEIGHT - ball.height);
  }
}

function collides(ball, paddle) {
  return ball.x + ball.width >= paddle.x && ball.x <= paddle.x + paddle.width && ball.y + ball.height >= paddle.y && ball.y <= paddle.y + paddle.height;
}

function hitPaddle(ball, paddle, direction) {
  ball.y = direction > 0 ? paddle.y + paddle.height : paddle.y - ball.height;
  const ballCenter = ball.x + ball.width / 2;
  const paddleCenter = paddle.x + paddle.width / 2;
  const offset = clamp((ballCenter - paddleCenter) / (paddle.width / 2), -0.92, 0.92);
  ball.speed = Math.min(740, ball.speed + 22);
  ball.vx = ball.speed * offset;
  ball.vy = direction * Math.sqrt(Math.max(ball.speed ** 2 - ball.vx ** 2, 170 ** 2));
}

function hitPaddleVertical(ball, paddle, direction) {
  ball.x = direction > 0 ? paddle.x + paddle.width : paddle.x - ball.width;
  const ballCenter = ball.y + ball.height / 2;
  const paddleCenter = paddle.y + paddle.height / 2;
  const offset = clamp((ballCenter - paddleCenter) / (paddle.height / 2), -0.92, 0.92);
  ball.speed = Math.min(740, ball.speed + 22);
  ball.vy = ball.speed * offset;
  ball.vx = direction * Math.sqrt(Math.max(ball.speed ** 2 - ball.vy ** 2, 170 ** 2));
}

function scorePong(room, seat, serveAtSeat = null) {
  const pong = room.pong;
  if (!seat) return;
  pong.score[seat] += 1;
  if (pong.score[seat] >= 5) {
    finishPong(room, seat, "First to five");
    return;
  }
  pong.effect = null;
  pong.ball = createPongBall(serveAtSeat || seat);
  pong.lastHitSeat = null;
}

function finishPong(room, winnerSeat, reason) {
  if (room.phase !== "pong") return;
  const winnerId = room.pong?.seatIds[winnerSeat];
  const winner = room.players.get(winnerId);
  if (winner) winner.discs = Math.min(MAX_DISCS, winner.discs + 25);
  for (const seat of PONG_SEATS) { const player = room.players.get(room.pong?.seatIds[seat]); if (player && player.id !== winnerId) player.discs = Math.min(MAX_DISCS, player.discs + 5); }

  room.phase = "results";
  room.pong.result = { winnerId, winnerName: winner?.name || "Winner", reason };
  broadcastState(room);
  setTimeout(() => resetToLobby(room), 8500);
}

function resetToLobby(room) {
  clearTimeout(room.prediction?.bettingTimer);
  clearTimeout(room.prediction?.resolveTimer);
  clearTimeout(room.prediction?.countdownTimer);
  clearTimeout(room.derby?.bettingTimer);
  clearTimeout(room.derby?.resultsTimer);
  clearTimeout(room.derby?.countdownTimer);
  room.phase = "lobby";
  room.prediction = null;
  room.pong = null;
  if (room.mode === "derby") room.derby = createDerby(room.derbyStyle);
  for (const player of room.players.values()) player.input = 0;
  broadcastState(room);
}

function loop() {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (room.phase === "pong" && room.pong) {
      const delta = Math.min((now - room.pong.lastTick) / 1000, 0.04);
      room.pong.lastTick = now;
      stepPong(room, delta);
    }
    if (room.phase === "resolving" && room.prediction?.ball) {
      const delta = Math.min((now - room.prediction.lastTick) / 1000, 0.04);
      room.prediction.lastTick = now;
      stepPrediction(room, delta);
    }
    if (room.phase === "derby-racing" && room.derby) {
      const delta = Math.min((now - room.derby.lastTick) / 1000, 0.04);
      room.derby.lastTick = now;
      stepDerby(room, delta);
    }
    broadcastState(room);
  }
}

function publicState(room) {
  return {
    type: "state",
    room: {
      code: room.code,
      phase: room.phase,
      mode: room.mode,
      predictionStyle: room.predictionStyle,
      derbyStyle: room.derbyStyle,
      maxPlayers: room.maxPlayers,
      fireDodgeEnabled: room.maxPlayers !== null,
      hostId: room.hostId,
      playerCount: [...room.players.values()].filter((player) => player.connected).length,
      players: [...room.players.values()].filter((player) => player.connected).map((player) => ({
        id: player.id,
        name: player.name,
        discs: player.discs,
        roundWon: player.roundWon,
        seat: player.seat,
      })),
      prediction: room.prediction && {
        mode: room.prediction.mode,
        closesAt: room.prediction.closesAt,
        countdownEndsAt: room.prediction.countdownEndsAt,
        bounces: room.prediction.bounces,
        startBounces: room.prediction.startBounces,
        hitAt: room.prediction.hitAt,
        ball: room.prediction.ball,
        result: room.phase === "results" ? room.prediction.result : null,
        pot: room.prediction.pot,
        betCount: room.prediction.bets.length,
        bets: room.prediction.bets,
        winners: room.prediction.winners || [],
        effects: room.prediction.effects.filter((effect) => effect.expiresAt > Date.now()),
        cooldowns: room.prediction.cooldowns,
        frozen: room.prediction.frozen,
        fire: room.prediction.fire,
        mini: room.prediction.mini && {
          phase: room.prediction.mini.phase,
          closesAt: room.prediction.mini.closesAt,
          bets: room.prediction.mini.bets,
          pot: room.prediction.mini.pot,
          ball: room.prediction.mini.ball,
          bounces: room.prediction.mini.bounces,
          result: room.prediction.mini.result,
          winners: room.prediction.mini.winners || [],
        },
      },
      pong: room.pong && {
        score: room.pong.score,
        seatIds: room.pong.seatIds,
        paddle: room.pong.paddle,
        ball: room.pong.ball,
        power: room.pong.power,
        effect: room.pong.effect,
        result: room.pong.result || null,
      },
      derby: room.derby && {
        style: room.derby.style,
        closesAt: room.derby.closesAt,
        countdownEndsAt: room.derby.countdownEndsAt,
        pot: room.derby.pot,
        betCount: room.derby.bets.length,
        winnerId: room.derby.winnerId,
        logos: room.derby.logos.map((logo) => ({ id: logo.id, ownerId: logo.ownerId, ball: logo.ball, bounces: logo.bounces, effects: logo.effects })),
        winningBets: room.phase === "results" ? (room.derby.winningBets || []) : [],
        cooldowns: room.derby.cooldowns,
      },
    },
  };
}

function broadcastState(room) {
  const state = publicState(room);
  for (const client of room.clients) send(client, state);
}

function sendToPlayer(room, playerId, message) {
  for (const client of room.clients) {
    if (client.playerId === playerId) send(client, message);
  }
}

function send(client, message) {
  if (!client.socket.destroyed) sendFrame(client.socket, Buffer.from(JSON.stringify(message)));
}

function sendFrame(socket, payload, opcode = 0x1) {
  const length = payload.length;
  let header;
  if (length < 126) {
    header = Buffer.from([0x80 | opcode, length]);
  } else if (length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
  }
  socket.write(Buffer.concat([header, payload]));
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

setInterval(loop, TICK_MS);
server.listen(PORT, () => {
  console.log(`DVD Corner Clash running at http://localhost:${PORT}`);
});
