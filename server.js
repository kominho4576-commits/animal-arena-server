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

/* ===== 팀전(2v2/3v3) 매칭: 봇 없음, 정원(4명/6명)이 다 찰 때까지 대기 =====
   기존 FFA 방 구조(rooms.set(id,{ffa:true,members}))를 그대로 재사용 —
   봇이 없는 동질적 실제 플레이어 릴레이는 FFA와 구조가 동일하므로 relay()/leaveRoom()은 무수정 */
const teamQueues = { 2: [], 3: [] }; // size(한 팀 인원) -> 대기 중인 ws[]
function teamPrune(size) {
  const q = teamQueues[size];
  for (let i = q.length - 1; i >= 0; i--) { const ws = q[i]; if (!ws || ws.readyState !== ws.OPEN) q.splice(i, 1); }
}
function teamBroadcastWait(size) {
  const q = teamQueues[size];
  for (const ws of q) send(ws, { type: 'team_wait', count: q.length, need: size * 2, size });
}
function teamTryStart(size) {
  teamPrune(size);
  const need = size * 2;
  const q = teamQueues[size];
  if (q.length < need) { teamBroadcastWait(size); return; }
  const members = q.splice(0, need);
  const roomId = 't' + (nextRoom++);
  const seed = (Math.random() * 1e9) | 0;
  const team = {}; // idx -> 'red'|'blue' (짝/홀 idx로 분배)
  members.forEach((ws, i) => { team[i] = (i % 2 === 0) ? 'red' : 'blue'; });
  rooms.set(roomId, { id: roomId, ffa: true, members, team });
  const players = members.map((ws, i) => {
    const c = clients.get(ws);
    return { idx: i, species: c ? c.species : 'rabbit', nick: c ? c.nick : '', team: team[i] };
  });
  members.forEach((ws, i) => {
    const c = clients.get(ws);
    if (c) { c.room = roomId; c.role = (i === 0 ? 'host' : 'guest'); c.idx = i; c.ffa = true; }
    send(ws, { type: 'team_matched', room: roomId, idx: i, seed, size, players });
  });
  console.log(`[team${size}v${size}] ${roomId}: ${players.length} players`);
  if (q.length >= need) teamTryStart(size); else teamBroadcastWait(size);
}
function teamQueueRemove(ws) {
  for (const size of [2, 3]) {
    const i = teamQueues[size].indexOf(ws);
    if (i >= 0) { teamQueues[size].splice(i, 1); teamBroadcastWait(size); }
  }
}

/* ===== 커스텀 대전(파티): 코드 초대 + 봇 슬롯, 방장 권위 =====
   기존 1:1 코드방(createroom/joinroom/codeRooms)과는 완전히 분리된 별도 메시지/저장소 —
   기존 PVP 1:1 방 기능은 전혀 건드리지 않는다. */
const partyCodeRooms = new Map(); // code -> { code, host, members:[ws|null,...], started }
const PARTY_MAX = 6;
function partyRoster(party) {
  return party.members.map((ws, i) => {
    if (!ws) return null;
    const c = clients.get(ws);
    return { partyIdx: i, species: c ? c.species : 'rabbit', nick: c ? c.nick : '', isHost: ws === party.host };
  }).filter(Boolean);
}
function partyBroadcastRoster(party) {
  const roster = partyRoster(party);
  for (const ws of party.members) if (ws) send(ws, { type: 'party_roster', code: party.code, roster });
}
function partyClose(party, reason) {
  for (const ws of party.members) {
    if (!ws) continue;
    send(ws, { type: 'party_closed', reason });
    const c = clients.get(ws); if (c) c.partyCode = null;
  }
  partyCodeRooms.delete(party.code);
}
function partyPreStartLeave(ws) {
  const c = clients.get(ws);
  if (!c || !c.partyCode) return;
  const party = partyCodeRooms.get(c.partyCode);
  c.partyCode = null;
  if (!party) return;
  const i = party.members.indexOf(ws);
  if (i >= 0) party.members[i] = null;
  if (ws === party.host) partyClose(party, '호스트가 나갔습니다');
  else partyBroadcastRoster(party);
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
  if (room && (room.ffa || room.party)) {
    // FFA/파티: 이탈 멤버는 null 처리(인덱스 유지), 남은 인원에게 통지
    const i = room.members.indexOf(ws);
    if (i >= 0) room.members[i] = null;
    for (const o of room.members) if (o && o !== ws) send(o, { type: 'peer_left', idx: c.idx });
    // 파티방은 봇이 호스트 클라이언트에서만 시뮬레이션되므로, 호스트 이탈 시 매치를 지속할 수 없음 -> 전체 종료
    if (room.party && ws === room.host) {
      for (const o of room.members) if (o) send(o, { type: 'opponent_left' });
      rooms.delete(c.room); console.log(`[party] ${c.room} closed (host left)`);
      c.room = null; return;
    }
    if (room.members.filter(Boolean).length === 0) { rooms.delete(c.room); console.log(`[${room.party ? 'party' : 'ffa'}] ${c.room} closed`); }
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
  if (room.party) {
    // 호스트가 자기 봇을 대신해 보내는 메시지(fromBot 지정)는 발신 idx를 덮어쓰지 않고 그대로 신뢰(방장만 가능)
    if (ws === room.host && msg.fromBot !== undefined) msg.from = msg.fromBot;
    else msg.from = c.idx;
    if (msg.to !== undefined) {
      // 봇 대상 메시지(피격 등)는 봇을 시뮬레이션하는 호스트가 대신 처리
      if (room.botIdx && room.botIdx.has(msg.to)) { send(room.host, msg); return; }
      if (room.members[msg.to]) { send(room.members[msg.to], msg); return; }
      return;
    }
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
        teamQueueRemove(ws);
        // 방 만들고 대기 중이었으면 그 방도 취소
        if (c.code) { codeRooms.delete(c.code); c.code = null; }
        send(ws, { type: 'cancelled' });
        break;

      case 'queue_team': {
        // 팀전(2v2/3v3) 빠른 매칭 대기열 등록 — 봇 없음, 정원 다 찰 때까지 대기
        const size = (msg.size === 3) ? 3 : 2;
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        if (!teamQueues[size].includes(ws)) teamQueues[size].push(ws);
        teamTryStart(size);
        break;
      }

      case 'party_create': {
        // 커스텀 대전 방 생성 (친구 초대 코드 발급, 최대 6명, 봇은 서버가 모름 — 시작 시에만 알림)
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const pcode = genCode();
        const party = { code: pcode, host: ws, members: [ws], started: false };
        partyCodeRooms.set(pcode, party);
        c.partyCode = pcode;
        send(ws, { type: 'party_created', code: pcode });
        partyBroadcastRoster(party);
        console.log(`[party+] ${c.id} created code=${pcode}`);
        break;
      }

      case 'party_join': {
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const pcode = (msg.code || '').toString().toUpperCase().trim();
        const party = partyCodeRooms.get(pcode);
        if (!party) { send(ws, { type: 'party_join_failed', reason: '존재하지 않는 방 코드입니다' }); break; }
        if (party.started) { send(ws, { type: 'party_join_failed', reason: '이미 시작된 방입니다' }); break; }
        if (party.members.includes(ws)) { send(ws, { type: 'party_join_failed', reason: '자기 방에는 입장할 수 없습니다' }); break; }
        if (party.members.filter(Boolean).length >= PARTY_MAX) { send(ws, { type: 'party_join_failed', reason: '방 인원이 가득 찼습니다' }); break; }
        party.members.push(ws);
        c.partyCode = pcode;
        send(ws, { type: 'party_joined', code: pcode });
        partyBroadcastRoster(party);
        break;
      }

      case 'party_config': {
        // 방장만 설정 변경 가능. 서버는 내용을 해석하지 않고 나머지 멤버에게 그대로 중계.
        if (!c.partyCode) break;
        const party = partyCodeRooms.get(c.partyCode);
        if (!party || party.host !== ws) break;
        for (const o of party.members) if (o && o !== ws) send(o, { type: 'party_config', config: msg.config });
        break;
      }

      case 'party_start': {
        // 방장이 최종 로스터(실제 멤버 + 봇, 팀 배정 포함)를 확정해서 전송 -> 매치 시작
        if (!c.partyCode) break;
        const party = partyCodeRooms.get(c.partyCode);
        if (!party || party.host !== ws || party.started) break;
        party.started = true;
        const finalRoster = Array.isArray(msg.roster) ? msg.roster : [];
        const roomId = 'c' + (nextRoom++);
        const seed = (Math.random() * 1e9) | 0;
        const memberByPartyIdx = party.members; // partyIdx 그대로 사용 (join 순서)
        const members = [];      // roomId members[idx] = ws (봇 idx는 undefined)
        const botIdx = new Set();
        const players = [];
        finalRoster.forEach((slot, idx) => {
          if (slot.type === 'bot') {
            botIdx.add(idx);
            players.push({ idx, bot: true, species: slot.species || 'rabbit', nick: slot.nick || '봇', team: slot.team || 'ffa' });
          } else {
            const ws2 = memberByPartyIdx[slot.partyIdx];
            members[idx] = ws2 || undefined;
            const cc = ws2 ? clients.get(ws2) : null;
            players.push({ idx, bot: false, species: cc ? cc.species : (slot.species || 'rabbit'), nick: cc ? cc.nick : (slot.nick || ''), team: slot.team || 'ffa' });
          }
        });
        rooms.set(roomId, { id: roomId, party: true, members, botIdx, host: ws });
        members.forEach((ws2, idx) => {
          if (!ws2) return;
          const cc = clients.get(ws2);
          if (cc) { cc.room = roomId; cc.role = (ws2 === ws ? 'host' : 'guest'); cc.idx = idx; cc.ffa = false; cc.party = true; cc.partyCode = null; }
          send(ws2, { type: 'party_matched', room: roomId, idx, seed, gameMode: msg.gameMode, map: msg.map, players });
        });
        partyCodeRooms.delete(party.code);
        console.log(`[party] ${roomId}: ${players.length} slots (${botIdx.size} bots)`);
        break;
      }

      case 'party_leave':
        partyPreStartLeave(ws);
        break;

      case 'bot_state': {
        // 방장만 자신의 봇 상태를 대신 전송할 수 있음
        const rm = c.room ? rooms.get(c.room) : null;
        if (!rm || !rm.party || ws !== rm.host) break;
        relay(ws, msg);
        break;
      }

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
        partyPreStartLeave(ws);
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
    teamQueueRemove(ws);
    if (c && c.code) codeRooms.delete(c.code);
    partyPreStartLeave(ws);
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
