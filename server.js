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

function startPredictionBetting(room) {
  if (room.phase !== "lobby") return;
  for (const player of room.players.values()) player.roundWon = 0;
  room.phase = "betting";
  room.prediction = {
    roundId: crypto.randomUUID(),
    mode: room.predictionStyle,
    bets: [],
    closesAt: Date.now() + 20000,
    result: null,
    ball: null,
    bounces: 0,
    hitAt: null,
    aimedForCorner: false,
    lastTick: null,
    pot: 0,
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

  if (!Number.isInteger(stake) || stake < 1 || stake > player.discs) {
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
  prediction.result = choosePredictionResult(prediction.mode);
  prediction.ball = createPredictionBall();
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
  if (!prediction || !ball || prediction.hitAt) return;

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
      prediction.resolveTimer = setTimeout(() => resolvePrediction(room), 1400);
      break;
    }
  }
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
  const speed = 520;
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
  const winners = prediction.bets.filter((bet) => prediction.result >= bet.min && prediction.result <= bet.max);
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
  prediction.resultsTimer = setTimeout(() => resetToLobby(room), 8500);
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
  ball.x += ball.vx * deltaSeconds;
  ball.y += ball.vy * deltaSeconds;

  if (ball.x <= 0 || ball.x + ball.width >= WIDTH) {
    ball.x = clamp(ball.x, 0, WIDTH - ball.width);
    ball.vx *= -1;
  }

  const topPaddle = pong.paddle.top;
  const bottomPaddle = pong.paddle.bottom;
  if (ball.vy < 0 && collides(ball, topPaddle)) {
    hitPaddle(ball, topPaddle, 1);
  }
  if (ball.vy > 0 && collides(ball, bottomPaddle)) {
    hitPaddle(ball, bottomPaddle, -1);
  }

  const touchingTopCorner = (ball.x <= 1 || ball.x + ball.width >= WIDTH - 1) && ball.y <= 1;
  if (touchingTopCorner && !ball.cornerLatched) {
    ball.cornerBonuses += 1;
    ball.cornerLatched = true;
  }
  if (!touchingTopCorner) {
    ball.cornerLatched = false;
  }

  if (ball.y + ball.height < -30) scorePong(room, "bottom");
  if (ball.y > HEIGHT + 30) scorePong(room, "top");
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
        hitAt: room.prediction.hitAt,
        ball: room.prediction.ball,
        result: room.phase === "results" ? room.prediction.result : null,
        pot: room.prediction.pot,
        betCount: room.prediction.bets.length,
        bets: room.prediction.bets,
        winners: room.prediction.winners || [],
      },
      pong: room.pong && {
        score: room.pong.score,
        paddle: room.pong.paddle,
        ball: room.pong.ball,
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
