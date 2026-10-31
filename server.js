import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, "public");
const PORT = Number(process.env.PORT || 3000);
const rooms = new Map();

const WIDTH = 1000;
const HEIGHT = 620;
const TICK_MS = 1000 / 60;
const MAX_DISCS = 9999;
const PREDICTION_POWERS = {
  mini: { price: 15, cooldown: 15 },
  scramble: { price: 55, cooldown: 150 },
  turbo: { price: 28, cooldown: 25, duration: 15 },
  slow: { price: 22, cooldown: 25, duration: 15 },
  grow: { price: 24, cooldown: 25, duration: 15 },
  shrink: { price: 20, cooldown: 25, duration: 15 },
  fire: { price: 36, cooldown: 75, duration: 60 },
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

  const type = filePath.endsWith(".html") ? "text/html; charset=utf-8" : "text/plain; charset=utf-8";
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

function createRoom() {
  let code;
  do {
    code = crypto.randomBytes(3).toString("hex").toUpperCase();
  } while (rooms.has(code));

  const room = {
    code,
    createdAt: Date.now(),
    hostId: null,
    clients: new Set(),
    players: new Map(),
    phase: "lobby",
    mode: "prediction",
    predictionStyle: "chance",
    maxPlayers: 20,
    prediction: null,
    pong: null,
  };
  rooms.set(code, room);
  return room;
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

function sanitizeName(name) {
  const value = String(name || "Player").trim().replace(/[^a-zA-Z0-9 _-]/g, "");
  return (value || "Player").slice(0, 16);
}

function handleMessage(client, message) {
  if (!message || typeof message.type !== "string") return;

  if (message.type === "create-room") {
    const room = createRoom();
    joinRoom(client, room, message.name, true);
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
    room.mode = message.mode === "pong" ? "pong" : "prediction";
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

  if (message.type === "start-round") {
    if (!isHost(room, player)) return;
    if (room.mode === "pong") startPong(room);
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
}

function disconnect(client) {
  const room = rooms.get(client.roomCode);
  if (!room) return;

  room.clients.delete(client);
  const player = room.players.get(client.playerId);
  if (player) {
    player.connected = false;
    player.input = 0;
    if (room.phase === "pong" && player.seat) {
      finishPong(room, player.seat === "top" ? "bottom" : "top", "Opponent disconnected");
    }
  }

  if (room.clients.size === 0) {
    rooms.delete(room.code);
  } else {
    if (room.hostId === client.playerId) {
      const nextHost = [...room.players.values()].find((candidate) => candidate.connected);
      room.hostId = nextHost?.id || null;
    }
    broadcastState(room);
  }
}

function isHost(room, player) {
  return room.hostId === player.id;
}

function takePongSeat(room, player, requestedSeat) {
  if (room.phase !== "lobby") return;
  const seat = requestedSeat === "top" ? "top" : "bottom";
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

  room.prediction.bettingTimer = setTimeout(() => beginPredictionAnimation(room), 20000);
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
  if (room.phase !== "betting" || !room.prediction) return;
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
  if (mini.bounces === mini.target - 1) aimPredictionAtCorner(mini.ball);
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
  const top = [...room.players.values()].find((player) => player.seat === "top" && player.connected);
  const bottom = [...room.players.values()].find((player) => player.seat === "bottom" && player.connected);
  if (!top || !bottom) {
    sendToPlayer(room, room.hostId, { type: "error", message: "Pong needs two players in the top and bottom seats." });
    return;
  }

  room.phase = "pong";
  room.pong = {
    topId: top.id,
    bottomId: bottom.id,
    score: { top: 0, bottom: 0 },
    paddle: {
      top: { x: WIDTH / 2 - 100, y: 36, width: 200, height: 16 },
      bottom: { x: WIDTH / 2 - 100, y: HEIGHT - 52, width: 200, height: 16 },
    },
    ball: createPongBall(Math.random() < 0.5 ? 1 : -1),
    lastTick: Date.now(),
    power: null,
    powerTimer: 6,
    effect: null,
  };
  broadcastState(room);
}

function createPongBall(verticalDirection) {
  const speed = 360;
  const angle = (Math.random() * 0.9 - 0.45) + (verticalDirection > 0 ? Math.PI / 2 : -Math.PI / 2);
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
  const top = room.players.get(pong.topId);
  const bottom = room.players.get(pong.bottomId);
  if (!top || !bottom || !top.connected || !bottom.connected) return;

  const paddleSpeed = 620;
  for (const [seat, player] of [["top", top], ["bottom", bottom]]) {
    const paddle = pong.paddle[seat];
    paddle.x = clamp(paddle.x + player.input * paddleSpeed * deltaSeconds, 0, WIDTH - paddle.width);
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

  if (ball.x <= 0 || ball.x + ball.width >= WIDTH) {
    ball.x = clamp(ball.x, 0, WIDTH - ball.width);
    ball.vx *= -1;
  }

  const topPaddle = pong.paddle.top;
  const bottomPaddle = pong.paddle.bottom;
  if (ball.vy < 0 && collides(ball, topPaddle)) {
    if (pong.effect?.type === "fire") { scorePong(room, "bottom"); return; }
    hitPaddle(ball, topPaddle, 1);
  }
  if (ball.vy > 0 && collides(ball, bottomPaddle)) {
    if (pong.effect?.type === "fire") { scorePong(room, "top"); return; }
    hitPaddle(ball, bottomPaddle, -1);
  }

  if (pong.power && Math.hypot(ball.x + ball.width / 2 - pong.power.x, ball.y + ball.height / 2 - pong.power.y) < 52) {
    applyPongPower(pong, pong.power.type);
    pong.power = null;
  }

  const touchingTopCorner = (ball.x <= 1 || ball.x + ball.width >= WIDTH - 1) && ball.y <= 1;
  if (touchingTopCorner && !ball.cornerLatched) {
    ball.cornerBonuses += 1;
    ball.cornerLatched = true;
  }
  if (!touchingTopCorner) {
    ball.cornerLatched = false;
  }

  if (pong.effect?.type === "fire" && (ball.y <= 0 || ball.y + ball.height >= HEIGHT)) {
    ball.y = clamp(ball.y, 0, HEIGHT - ball.height); ball.vy *= -1;
  } else {
    if (ball.y + ball.height < -30) scorePong(room, "bottom");
    if (ball.y > HEIGHT + 30) scorePong(room, "top");
  }
}

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

function scorePong(room, seat) {
  const pong = room.pong;
  pong.score[seat] += 1;
  if (pong.score[seat] >= 5) {
    finishPong(room, seat, "First to five");
    return;
  }
  pong.effect = null;
  pong.ball = createPongBall(seat === "top" ? 1 : -1);
}

function finishPong(room, winnerSeat, reason) {
  if (room.phase !== "pong") return;
  const winnerId = winnerSeat === "top" ? room.pong?.topId : room.pong?.bottomId;
  const loserId = winnerSeat === "top" ? room.pong?.bottomId : room.pong?.topId;
  const winner = room.players.get(winnerId);
  const loser = room.players.get(loserId);
  if (winner) winner.discs = Math.min(MAX_DISCS, winner.discs + 25);
  if (loser) loser.discs = Math.min(MAX_DISCS, loser.discs + 5);

  room.phase = "results";
  room.pong.result = { winnerId, winnerName: winner?.name || "Winner", reason };
  broadcastState(room);
  setTimeout(() => resetToLobby(room), 8500);
}

function resetToLobby(room) {
  clearTimeout(room.prediction?.bettingTimer);
  clearTimeout(room.prediction?.resolveTimer);
  room.phase = "lobby";
  room.prediction = null;
  room.pong = null;
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
        paddle: room.pong.paddle,
        ball: room.pong.ball,
        power: room.pong.power,
        effect: room.pong.effect,
        result: room.pong.result || null,
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
