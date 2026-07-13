/* =====================================================================
   ANIMAL ARENA — PVP 매치메이킹 + 릴레이 서버
   Node.js + ws. Render 배포용.
   구조:
     - 클라이언트가 접속해서 {type:'queue'} 보내면 대기열에 등록
     - 두 명이 모이면 방을 만들고 한쪽을 host로 지정
     - 이후 모든 게임 메시지(입력/상태/발사)를 상대에게 그대로 중계
     - host가 데미지/킬 판정 권한을 가짐 (서버는 중계만)
   ===================================================================== */
'use strict';
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// 간단한 health-check용 http 서버 (Render가 포트 열림 확인)
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', players: clients.size, rooms: rooms.size }));
  } else {
    res.writeHead(404); res.end();
  }
});

const wss = new WebSocketServer({ server });

const clients = new Map();   // ws -> {id, room, ready}
const queue = [];            // 대기 중인 ws 목록
const rooms = new Map();     // roomId -> {host, guest, id}
const codeRooms = new Map(); // 6자리코드 -> {host, code}
let nextId = 1;
let nextRoom = 1;

/* ===== 개인전(FFA) 매칭: 최대 5명, 2명 이상 모인 뒤 10초간 추가 입장 없으면 시작 ===== */
const FFA_MIN = 2, FFA_MAX = 5, FFA_WAIT_MS = 10000;
const ffaQueue = [];           // 대기 중인 ws (선착순)
let ffaTimer = null, ffaDeadline = 0, ffaTick = null;

function ffaPrune() {
  for (let i = ffaQueue.length - 1; i >= 0; i--) {
    const ws = ffaQueue[i];
    if (!ws || ws.readyState !== ws.OPEN) ffaQueue.splice(i, 1);
  }
}
function ffaBroadcastWait() {
  const secs = ffaTimer ? Math.max(0, Math.ceil((ffaDeadline - Date.now()) / 1000)) : -1;
  for (const ws of ffaQueue) send(ws, { type: 'ffa_wait', count: ffaQueue.length, secs });
}
function ffaClearTimer() {
  if (ffaTimer) { clearTimeout(ffaTimer); ffaTimer = null; }
  if (ffaTick) { clearInterval(ffaTick); ffaTick = null; }
}
// 새 인원이 들어올 때마다 10초 타이머 리셋; 5명 차면 즉시 시작
function ffaResetTimer() {
  ffaClearTimer();
  ffaPrune();
  if (ffaQueue.length >= FFA_MIN) {
    ffaDeadline = Date.now() + FFA_WAIT_MS;
    ffaTimer = setTimeout(ffaStart, FFA_WAIT_MS);
    ffaTick = setInterval(ffaBroadcastWait, 1000); // 남은 초 카운트다운 전송
  }
  ffaBroadcastWait();
}
function ffaStart() {
  ffaClearTimer();
  ffaPrune();
  if (ffaQueue.length < FFA_MIN) { ffaBroadcastWait(); return; }
  const members = ffaQueue.splice(0, FFA_MAX);
  const roomId = 'f' + (nextRoom++);
  const seed = (Math.random() * 1e9) | 0;
  rooms.set(roomId, { id: roomId, ffa: true, members }); // members 배열 인덱스 = 플레이어 idx (null=이탈)
  const players = members.map((ws, i) => {
    const c = clients.get(ws);
    return { idx: i, species: c ? c.species : 'rabbit', nick: c ? c.nick : '' };
  });
  members.forEach((ws, i) => {
    const c = clients.get(ws);
    if (c) { c.room = roomId; c.role = (i === 0 ? 'host' : 'guest'); c.idx = i; c.ffa = true; }
    send(ws, { type: 'ffa_matched', room: roomId, idx: i, seed, players });
  });
  console.log(`[ffa] ${roomId}: ${players.length} players`);
  if (ffaQueue.length >= FFA_MIN) ffaResetTimer(); else ffaBroadcastWait();
}
function ffaQueueRemove(ws) {
  const i = ffaQueue.indexOf(ws);
  if (i >= 0) { ffaQueue.splice(i, 1); ffaResetTimer(); }
}

function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 헷갈리는 0/O/1/I 제외
  let code;
  do { code = ''; for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)]; }
  while (codeRooms.has(code));
  return code;
}

function startPrivateMatch(host, guest, code) {
  const roomId = 'r' + (nextRoom++);
  rooms.set(roomId, { id: roomId, host, guest });
  const ca = clients.get(host), cb = clients.get(guest);
  if (ca) { ca.room = roomId; ca.role = 'host'; ca.code = null; }
  if (cb) { cb.room = roomId; cb.role = 'guest'; cb.code = null; }
  send(host, { type: 'matched', room: roomId, role: 'host', code,
               opponentSpecies: cb ? cb.species : 'fox', opponentNick: cb ? cb.nick : '' });
  send(guest, { type: 'matched', room: roomId, role: 'guest', code,
                opponentSpecies: ca ? ca.species : 'fox', opponentNick: ca ? ca.nick : '' });
  codeRooms.delete(code);
  console.log(`[private] ${roomId} code=${code}: host=${ca?.id} guest=${cb?.id}`);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

function pairFromQueue() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();
    // 끊긴 소켓은 건너뜀
    if (!a || a.readyState !== a.OPEN) { if (b) queue.unshift(b); continue; }
    if (!b || b.readyState !== b.OPEN) { queue.unshift(a); continue; }

    const roomId = 'r' + (nextRoom++);
    const room = { id: roomId, host: a, guest: b };
    rooms.set(roomId, room);

    const ca = clients.get(a), cb = clients.get(b);
    if (ca) { ca.room = roomId; ca.role = 'host'; }
    if (cb) { cb.room = roomId; cb.role = 'guest'; }

    // host에게: 너가 호스트, 상대 species
    send(a, { type: 'matched', room: roomId, role: 'host',
              opponentSpecies: cb ? cb.species : 'fox',
              opponentNick: cb ? cb.nick : '' });
    send(b, { type: 'matched', room: roomId, role: 'guest',
              opponentSpecies: ca ? ca.species : 'fox',
              opponentNick: ca ? ca.nick : '' });

    console.log(`[match] ${roomId}: host=${ca?.id} guest=${cb?.id}`);
  }
}

function leaveRoom(ws) {
  const c = clients.get(ws);
  if (!c || !c.room) return;
  const room = rooms.get(c.room);
  if (room && room.ffa) {
    // FFA: 이탈 멤버는 null 처리(인덱스 유지), 남은 인원에게 통지
    const i = room.members.indexOf(ws);
    if (i >= 0) room.members[i] = null;
    for (const o of room.members) if (o && o !== ws) send(o, { type: 'peer_left', idx: c.idx });
    if (room.members.filter(Boolean).length === 0) { rooms.delete(c.room); console.log(`[ffa] ${c.room} closed`); }
    c.room = null;
    return;
  }
  if (room) {
    const other = room.host === ws ? room.guest : room.host;
    send(other, { type: 'opponent_left' });
    const oc = clients.get(other);
    if (oc) oc.room = null;
    rooms.delete(c.room);
    console.log(`[room] ${c.room} closed`);
  }
  c.room = null;
}

function relay(ws, msg) {
  const c = clients.get(ws);
  if (!c || !c.room) return;
  const room = rooms.get(c.room);
  if (!room) return;
  if (room.ffa) {
    msg.from = c.idx; // 발신자 태깅
    // hit/kill은 대상(to)에게만, 나머지는 방 전체 브로드캐스트
    if (msg.to !== undefined && room.members[msg.to]) { send(room.members[msg.to], msg); return; }
    for (const o of room.members) if (o && o !== ws) send(o, msg);
    return;
  }
  const other = room.host === ws ? room.guest : room.host;
  // 원본 메시지를 그대로 상대에게 전달 (input/state/fire/hit/kill 등)
  send(other, msg);
}

wss.on('connection', (ws) => {
  const id = 'p' + (nextId++);
  clients.set(ws, { id, room: null, species: 'rabbit', nick: '', role: null });
  send(ws, { type: 'hello', id });
  console.log(`[conn] ${id} connected (total ${clients.size})`);

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    const c = clients.get(ws);
    if (!c) return;

    switch (msg.type) {
      case 'queue_ffa':
        // 개인전 대기열 등록
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        if (!ffaQueue.includes(ws)) ffaQueue.push(ws);
        if (ffaQueue.length >= FFA_MAX) ffaStart();
        else ffaResetTimer();
        break;

      case 'queue':
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        if (!queue.includes(ws)) queue.push(ws);
        send(ws, { type: 'queued', position: queue.length });
        pairFromQueue();
        break;

      case 'cancel':
        { const i = queue.indexOf(ws); if (i >= 0) queue.splice(i, 1); }
        ffaQueueRemove(ws);
        // 방 만들고 대기 중이었으면 그 방도 취소
        if (c.code) { codeRooms.delete(c.code); c.code = null; }
        send(ws, { type: 'cancelled' });
        break;

      case 'createroom': {
        // 방 생성 -> 코드 발급, 상대 입장 대기
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const code = genCode();
        c.code = code;
        codeRooms.set(code, { host: ws, code });
        send(ws, { type: 'room_created', code });
        console.log(`[room+] ${c.id} created code=${code}`);
        break;
      }

      case 'joinroom': {
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const code = (msg.code || '').toString().toUpperCase().trim();
        const entry = codeRooms.get(code);
        if (!entry) { send(ws, { type: 'join_failed', reason: '존재하지 않는 방 코드입니다' }); break; }
        if (entry.host === ws) { send(ws, { type: 'join_failed', reason: '자기 방에는 입장할 수 없습니다' }); break; }
        if (!entry.host || entry.host.readyState !== entry.host.OPEN) { codeRooms.delete(code); send(ws, { type: 'join_failed', reason: '방장이 나갔습니다' }); break; }
        startPrivateMatch(entry.host, ws, code);
        break;
      }

      case 'leave':
        if (c.code) { codeRooms.delete(c.code); c.code = null; }
        leaveRoom(ws);
        break;

      // 게임 중 실시간 메시지: 그대로 상대에게 중계
      case 'input':    // guest -> host: 내 입력
      case 'state':    // 위치/회전/애니 상태 브로드캐스트
      case 'fire':     // 발사 이벤트
      case 'hit':      // host -> guest: 데미지 판정 결과
      case 'kill':     // host -> guest: 킬 확정
      case 'spawn':    // 무기/픽업 스폰 동기화 (host 권위)
      case 'weapon':   // 무기 획득/드랍
      case 'pkrespawn':// 무기 리스폰 (host 권위)
      case 'gameover': // 경기 종료 결과 통지
      case 'ult':      // 궁극기 발동
      case 'ping':     // 지연 측정
        if (msg.type === 'ping') { send(ws, { type: 'pong', t: msg.t }); break; }
        relay(ws, msg);
        break;

      default:
        break;
    }
  });

  ws.on('close', () => {
    const c = clients.get(ws);
    const i = queue.indexOf(ws); if (i >= 0) queue.splice(i, 1);
    ffaQueueRemove(ws);
    if (c && c.code) codeRooms.delete(c.code);
    leaveRoom(ws);
    clients.delete(ws);
    console.log(`[disc] ${c ? c.id : '?'} disconnected (total ${clients.size})`);
  });

  ws.on('error', () => {});
});

// 좀비 소켓 정리 (30초마다 ping)
setInterval(() => {
  for (const ws of clients.keys()) {
    if (ws.readyState === ws.OPEN) { try { ws.ping(); } catch (e) {} }
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Animal Arena PVP server on :${PORT}`);
});
