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
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

/* ===== 계정(고유 ID) 저장소 =====
   1순위: Upstash Redis(REST) — 환경변수 UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN 설정 시.
          Render 무료 플랜의 휘발성 파일시스템과 달리 재배포·수면(15분) 후에도 계정/친구/친추가 유지됨.
   2순위(백업): 로컬 파일 — Redis 미설정 시 폴백(서버 재시작 시 초기화될 수 있음). */
const ACCOUNTS_FILE = path.join(__dirname, 'data', 'accounts.json');
const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const REDIS_KEY = 'aa:accounts';
const useRedis = !!(REDIS_URL && REDIS_TOKEN);
let accounts = {}; // id -> {id, token, nickname, friends, friendReqIn, friendReqOut, createdAt}
const tokenToId = new Map();

/* Upstash REST 명령 실행: POST <url> body=["CMD","arg",...] → { result }. 실패 시 null 반환(서버는 계속 동작). */
async function redisCmd(args) {
  if (!useRedis) return null;
  try {
    const res = await fetch(REDIS_URL, {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(args),
    });
    if (!res.ok) { console.error('[redis] HTTP', res.status); return null; }
    const j = await res.json();
    return j && Object.prototype.hasOwnProperty.call(j, 'result') ? j.result : null;
  } catch (e) { console.error('[redis] err', e && e.message); return null; }
}
function rebuildTokenIndex() {
  tokenToId.clear();
  for (const a of Object.values(accounts)) if (a && a.token) tokenToId.set(a.token, a.id);
}
/* 시작 시 계정 로드: Redis 우선, 없으면 파일 폴백 */
async function loadAccounts() {
  if (useRedis) {
    const raw = await redisCmd(['GET', REDIS_KEY]);
    if (raw) { try { accounts = JSON.parse(raw); } catch (e) { accounts = {}; } console.log('[redis] loaded ' + Object.keys(accounts).length + ' accounts'); }
    else console.log('[redis] no existing accounts (fresh)');
  } else {
    try {
      fs.mkdirSync(path.dirname(ACCOUNTS_FILE), { recursive: true });
      accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8'));
    } catch (e) { accounts = {}; }
    console.log('[file] loaded ' + Object.keys(accounts).length + ' accounts (Redis 미설정 — 재시작 시 초기화될 수 있음)');
  }
  rebuildTokenIndex();
}

let accountsSaveTimer = null;
function saveAccountsSoon() {
  if (accountsSaveTimer) return;
  accountsSaveTimer = setTimeout(() => {
    accountsSaveTimer = null;
    const json = JSON.stringify(accounts);
    if (useRedis) redisCmd(['SET', REDIS_KEY, json]);
    else fs.writeFile(ACCOUNTS_FILE, json, () => {});
  }, 2000);
}
function genToken() {
  return Array.from({ length: 24 }, () => Math.floor(Math.random() * 36).toString(36)).join('');
}
/* 영대문자+숫자 6자리 고유 ID (예: A7K9Q2) — 계정 생성 시 한 번만 부여되며 이후 변경되지 않는다. */
const USER_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
function genUserId() {
  let id;
  do {
    id = Array.from({ length: 6 }, () => USER_ID_CHARS[Math.floor(Math.random() * USER_ID_CHARS.length)]).join('');
  } while (accounts[id]);
  return id;
}
function identifyClient(token) {
  if (token && tokenToId.has(token)) {
    const id = tokenToId.get(token);
    if (accounts[id]) return accounts[id];
  }
  const id = genUserId();
  const acc = { id, token: genToken(), nickname: '', friends: [], friendReqIn: [], friendReqOut: [], createdAt: Date.now() };
  accounts[id] = acc;
  tokenToId.set(acc.token, id);
  saveAccountsSoon();
  return acc;
}

/* ===== 접속 중인 계정 추적(친구 온라인 표시용) — 매치/파티용 임시 소켓이 많아 계정당 여러 ws가 동시에 열릴 수 있음 ===== */
const onlineByAccount = new Map(); // accountId -> Set(ws)
function markOnline(accountId, ws) {
  const wasOffline = !onlineByAccount.has(accountId);
  if (wasOffline) onlineByAccount.set(accountId, new Set());
  onlineByAccount.get(accountId).add(ws);
  return wasOffline; // 오프라인 -> 온라인 전환 여부 (친구들에게 실시간 통지용)
}
function markOffline(accountId, ws) {
  const set = onlineByAccount.get(accountId);
  if (!set) return false;
  set.delete(ws);
  if (set.size === 0) { onlineByAccount.delete(accountId); return true; } // 마지막 소켓 종료 -> 오프라인 전환
  return false;
}
/* 특정 계정의 모든 접속 소켓에 최신 친구 목록을 밀어넣음 — 닉네임 변경/온라인 상태 변화의 실시간 반영용 */
function pushFriendListToAccount(accountId) {
  const acc = accounts[accountId];
  const set = onlineByAccount.get(accountId);
  if (!acc || !set) return;
  const friends = buildFriendList(acc);
  for (const ws of set) send(ws, { type: 'friend_list_result', friends });
}
/* 이 계정의 정보(닉네임/온라인 상태)가 바뀌었을 때, 온라인 상태인 친구 전원의 친구 목록을 갱신 */
function notifyFriendsChanged(accountId) {
  const acc = accounts[accountId];
  if (!acc) return;
  for (const fid of (acc.friends || [])) {
    if (onlineByAccount.has(fid)) pushFriendListToAccount(fid);
  }
}

/* ===== 닉네임 검증: 2~12자 · 영문/숫자/한글/밑줄만 허용 · 최소 비속어 필터 · 전역 유일성 ===== */
const BAD_WORDS = ['시발', '씨발', '병신', '개새끼', '좆', '지랄', 'fuck', 'shit', 'nigger', 'bitch'];
function containsBadWord(s) {
  const low = s.toLowerCase();
  return BAD_WORDS.some(w => low.includes(w));
}
function nicknameTaken(nickname, exceptId) {
  const low = nickname.toLowerCase();
  return Object.values(accounts).some(a => a.id !== exceptId && a.nickname && a.nickname.toLowerCase() === low);
}
function buildFriendList(me) {
  return (me.friends || []).map(fid => {
    const f = accounts[fid];
    return { id: fid, nickname: f ? (f.nickname || '') : '(알 수 없음)', online: onlineByAccount.has(fid) };
  });
}
/* 나에게 들어온 친구 요청(수락 대기) 목록 — 보낸 사람의 ID/닉네임 */
function buildRequestList(me) {
  return (me.friendReqIn || []).map(rid => {
    const r = accounts[rid];
    return { id: rid, nickname: r ? (r.nickname || '') : '(알 수 없음)' };
  });
}
/* 특정 계정의 모든 접속 소켓에 최신 친구요청 목록을 밀어넣음 (실시간 요청 알림용) */
function pushRequestsToAccount(accountId) {
  const acc = accounts[accountId];
  const set = onlineByAccount.get(accountId);
  if (!acc || !set) return;
  const requests = buildRequestList(acc);
  for (const ws of set) send(ws, { type: 'friend_requests_result', requests });
}

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
const queues = { deathmatch: [], timeattack: [] }; // gameMode -> 대기 중인 ws 목록 (1v1)
const rooms = new Map();     // roomId -> {host, guest, id}
let nextId = 1;
let nextRoom = 1;

/* ===== 팀전(2v2/3v3) 매칭: 봇 없음, 정원(4명/6명)이 다 찰 때까지 대기 =====
   기존 FFA 방 구조(rooms.set(id,{ffa:true,members}))를 그대로 재사용 —
   봇이 없는 동질적 실제 플레이어 릴레이는 FFA와 구조가 동일하므로 relay()/leaveRoom()은 무수정.
   큐의 각 항목은 "그룹"(ws 배열, 길이 1~size) — 파티로 함께 들어온 인원은 항상 같은 팀에 배정됨. */
const teamQueues = { // size(한 팀 인원) -> gameMode -> 대기 중인 그룹(ws[])의 배열
  2: { deathmatch: [], timeattack: [] },
  3: { deathmatch: [], timeattack: [] },
};
function teamPrune(size, gameMode) {
  const q = teamQueues[size][gameMode];
  for (let i = q.length - 1; i >= 0; i--) {
    const t = q[i]._t;
    q[i] = q[i].filter(ws => ws && ws.readyState === ws.OPEN);
    if (t !== undefined) q[i]._t = t; // filter가 새 배열을 만들어 _t를 잃지 않도록 복원
    if (q[i].length === 0) q.splice(i, 1);
  }
}
function teamBroadcastWait(size, gameMode) {
  const q = teamQueues[size][gameMode];
  const count = q.reduce((s, g) => s + g.length, 0);
  for (const g of q) for (const ws of g) send(ws, { type: 'team_wait', count, need: size * 2, size, gameMode });
}
function teamQueueGroup(size, gameMode, group) {
  // 같은 소켓이 다른 그룹에 중복 등록되어 있으면 먼저 제거 (중복 매칭 방지)
  for (const ws of group) teamQueueRemove(ws);
  group._t = Date.now(); // 봇 채움 타이머 기준 시각
  if (!teamQueues[size][gameMode].some(g => g === group)) teamQueues[size][gameMode].push(group);
  teamTryStart(size, gameMode);
}
function teamTryStart(size, gameMode) {
  teamPrune(size, gameMode);
  const need = size * 2;
  const q = teamQueues[size][gameMode];
  const total = q.reduce((s, g) => s + g.length, 0);
  if (total < need) { teamBroadcastWait(size, gameMode); return; }
  // 그룹(파티)은 쪼개지 않고 그대로 한 팀에 배정 — 정확히 size명이 되도록 그리디하게 채움
  const pickTeam = (need) => {
    const picked = [];
    let sum = 0;
    for (let i = 0; i < q.length && sum < need; i++) {
      if (q[i].length <= need - sum) { picked.push(i); sum += q[i].length; }
    }
    return sum === need ? picked : null;
  };
  const aIdx = pickTeam(size);
  if (!aIdx) { teamBroadcastWait(size, gameMode); return; } // 조합이 안 맞으면 다음 인원 유입을 기다림(클라 봇 자동전환이 최종 백업)
  const aGroups = aIdx.map(i => q[i]);
  const remaining = q.filter((_, i) => !aIdx.includes(i));
  const bIdx = (() => {
    let sum = 0; const picked = [];
    for (let i = 0; i < remaining.length && sum < size; i++) {
      if (remaining[i].length <= size - sum) { picked.push(i); sum += remaining[i].length; }
    }
    return sum === size ? picked : null;
  })();
  if (!bIdx) { teamBroadcastWait(size, gameMode); return; }
  const bGroups = bIdx.map(i => remaining[i]);
  // 큐에서 제거 (역순 splice로 인덱스 꼬임 방지)
  [...aIdx].sort((a, b) => b - a).forEach(i => q.splice(i, 1));
  const remaining2 = q; // aIdx 제거 후 남은 배열 기준으로 bGroups를 다시 찾아 제거
  bGroups.forEach(g => { const i = remaining2.indexOf(g); if (i >= 0) remaining2.splice(i, 1); });

  const members = [...aGroups.flat(), ...bGroups.flat()];
  const aCount = aGroups.flat().length;
  const roomId = 't' + (nextRoom++);
  const seed = (Math.random() * 1e9) | 0;
  const team = {};
  members.forEach((ws, i) => { team[i] = (i < aCount) ? 'red' : 'blue'; });
  rooms.set(roomId, { id: roomId, ffa: true, members, team });
  const players = members.map((ws, i) => {
    const c = clients.get(ws);
    return { idx: i, species: c ? c.species : 'rabbit', nick: c ? c.nick : '', team: team[i] };
  });
  members.forEach((ws, i) => {
    const c = clients.get(ws);
    if (c) { c.room = roomId; c.role = (i === 0 ? 'host' : 'guest'); c.idx = i; c.ffa = true; }
    send(ws, { type: 'team_matched', room: roomId, idx: i, seed, size, gameMode, players });
  });
  console.log(`[team${size}v${size}/${gameMode}] ${roomId}: ${players.length} players (${aGroups.length + bGroups.length} groups)`);
  if (q.reduce((s, g) => s + g.length, 0) >= need) teamTryStart(size, gameMode); else teamBroadcastWait(size, gameMode);
}
function teamQueueRemove(ws) {
  for (const size of [2, 3]) {
    for (const gameMode of ['deathmatch', 'timeattack']) {
      const q = teamQueues[size][gameMode];
      let changed = false;
      for (let i = q.length - 1; i >= 0; i--) {
        const before = q[i].length;
        const t = q[i]._t;
        q[i] = q[i].filter(w => w !== ws);
        if (t !== undefined) q[i]._t = t;
        if (q[i].length !== before) changed = true;
        if (q[i].length === 0) q.splice(i, 1);
      }
      if (changed) teamBroadcastWait(size, gameMode);
    }
  }
}

/* ===== 팀전(2v2/3v3) 봇 채움 매칭 =====
   실제 플레이어를 항상 우선 매칭하되(teamTryStart), 일정 시간(TEAM_BOT_FILL_MS) 안에
   정원이 안 차면 대기 중인 실제 플레이어들만 모아 부족한 자리를 AI 봇으로 채워 시작한다.
   - 파티(그룹)는 절대 쪼개지 않고 통째로 같은 팀에 배정 -> 파티원은 항상 같은 게임/팀/맵
   - 봇 슬롯은 members[idx]=null 로 표시 -> relay()가 to=봇idx 메시지를 방 전체에 브로드캐스트하고
     호스트(idx 0, 항상 실제 플레이어)가 봇을 시뮬레이션/판정 (클라이언트의 파티봇 경로 재사용) */
const TEAM_BOT_FILL_MS = 10000;
const BOT_SPECIES = ['rabbit', 'cat', 'bear', 'panda', 'frog', 'dog', 'tiger', 'koala', 'pig', 'chick', 'wolf', 'fox'];
const BOT_NICK_POOL = ['제로', '칼바람', '야옹이', '불꽃여우', '다크나이트', 'sniper_K', '한방있음', '슈가', '고구마', '바람돌이',
  '무민', '토깽이', '청설모', '달빛', '새벽감성', '폭풍전야', '노랑이', '민초단', '귀요미', '철벽수비',
  '겜잘알', '원샷원킬', '샐러드', '초코송이', '빵야', '라이언', '흑표범', '은하수', 'soju', '치즈볼'];
function teamStartWithBots(size, gameMode) {
  teamPrune(size, gameMode);
  const q = teamQueues[size][gameMode];
  if (!q.length) return;
  // FIFO로 그룹(파티)을 쪼개지 않고 배정: red부터 채우고, 안 들어가면 blue 시도
  const redReal = [], blueReal = [], picked = [];
  for (const g of q) {
    if (g.length <= size - redReal.length) { redReal.push(...g); picked.push(g); }
    else if (g.length <= size - blueReal.length) { blueReal.push(...g); picked.push(g); }
    if (redReal.length === size && blueReal.length === size) break;
  }
  if (!redReal.length && !blueReal.length) return;
  for (const g of picked) { const i = q.indexOf(g); if (i >= 0) q.splice(i, 1); }
  const realMembers = [...redReal, ...blueReal]; // idx 0..n-1 (idx 0 = 항상 실제 플레이어 = 호스트)
  const need = size * 2;
  const botCount = need - realMembers.length;
  // 봇 종/닉네임: 실제 참가자와 겹치지 않게 선택
  const usedNicks = new Set(realMembers.map(w => (clients.get(w) || {}).nick || ''));
  const speciesPool = BOT_SPECIES.filter(s => !realMembers.some(w => (clients.get(w) || {}).species === s));
  const nickPool = BOT_NICK_POOL.filter(n => !usedNicks.has(n));
  const pickSpecies = () => speciesPool.length
    ? speciesPool.splice(Math.floor(Math.random() * speciesPool.length), 1)[0]
    : BOT_SPECIES[Math.floor(Math.random() * BOT_SPECIES.length)];
  const pickNick = () => nickPool.length
    ? nickPool.splice(Math.floor(Math.random() * nickPool.length), 1)[0]
    : 'Player' + (100 + Math.floor(Math.random() * 899));
  const roomId = 't' + (nextRoom++);
  const seed = (Math.random() * 1e9) | 0;
  const members = [], players = [], team = {};
  let idx = 0;
  const addReal = (ws, t) => {
    members[idx] = ws; team[idx] = t;
    const c = clients.get(ws);
    players.push({ idx, species: c ? c.species : 'rabbit', nick: c ? c.nick : '', team: t });
    idx++;
  };
  redReal.forEach(ws => addReal(ws, 'red'));
  blueReal.forEach(ws => addReal(ws, 'blue'));
  const addBot = (t) => { members[idx] = null; team[idx] = t; players.push({ idx, species: pickSpecies(), nick: pickNick(), team: t, bot: true }); idx++; };
  for (let i = 0; i < size - redReal.length; i++) addBot('red');
  for (let i = 0; i < size - blueReal.length; i++) addBot('blue');
  rooms.set(roomId, { id: roomId, ffa: true, members, team });
  realMembers.forEach((ws, i) => {
    const c = clients.get(ws);
    if (c) { c.room = roomId; c.role = (i === 0 ? 'host' : 'guest'); c.idx = i; c.ffa = true; }
    send(ws, { type: 'team_matched', room: roomId, idx: i, seed, size, gameMode, players });
  });
  console.log(`[team${size}v${size}/${gameMode}] ${roomId}: ${realMembers.length} players + ${botCount} bots (fill)`);
  teamBroadcastWait(size, gameMode);
}
// 주기 점검: 실제 인원 우선 매칭 재시도 -> 가장 오래 기다린 그룹이 기준 시간을 넘겼으면 봇 채움 시작
setInterval(() => {
  for (const size of [2, 3]) {
    for (const gameMode of ['deathmatch', 'timeattack']) {
      teamTryStart(size, gameMode); // 실제 플레이어끼리 정원이 차면 항상 이쪽이 우선
      const q = teamQueues[size][gameMode];
      if (q.length && q[0]._t && (Date.now() - q[0]._t) >= TEAM_BOT_FILL_MS) teamStartWithBots(size, gameMode);
    }
  }
}, 2000);

/* ===== 팀 매칭용 파티(초대): 2v2/3v3에서 친구를 초대해 항상 같은 팀으로 큐에 들어감 =====
   봇 없음, 방장 외 전원 준비완료 후 실제 팀 매칭 큐(teamQueues)로 편입됨. */
const teamPartyRooms = new Map(); // code -> { code, host, size, gameMode, members:[ws], ready:Set(ws), started }
function genTeamPartyCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = ''; for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)]; }
  while (teamPartyRooms.has(code));
  return code;
}
// 친구 초대(team_party_invite_send) 시 아직 파티가 없으면 새로 만들고, 있으면 기존 파티를 그대로 사용
function ensureTeamParty(ws, c, size, gameMode) {
  if (c.teamPartyCode && teamPartyRooms.has(c.teamPartyCode)) return teamPartyRooms.get(c.teamPartyCode);
  const code = genTeamPartyCode();
  const party = { code, host: ws, size, gameMode, members: [ws], ready: new Set(), started: false };
  teamPartyRooms.set(code, party);
  c.teamPartyCode = code;
  return party;
}
function tpRoster(party) {
  return party.members.map((ws, i) => {
    const c = clients.get(ws);
    return { i, species: c ? c.species : 'rabbit', nick: c ? c.nick : '', isHost: ws === party.host, ready: party.ready.has(ws), aid: c ? (c.accountId || null) : null };
  });
}
function tpBroadcast(party) {
  const roster = tpRoster(party);
  for (const ws of party.members) send(ws, { type: 'team_party_roster', code: party.code, size: party.size, gameMode: party.gameMode, roster });
}
function tpClose(party, reason) {
  for (const ws of party.members) {
    send(ws, { type: 'team_party_closed', reason });
    const c = clients.get(ws); if (c) c.teamPartyCode = null;
  }
  teamPartyRooms.delete(party.code);
}
function tpPartyLeave(ws) {
  const c = clients.get(ws);
  if (!c || !c.teamPartyCode) return;
  const party = teamPartyRooms.get(c.teamPartyCode);
  c.teamPartyCode = null;
  if (!party) return;
  if (party.started) return; // 이미 실제 매칭 큐로 넘어간 뒤에는 이 경로로 처리하지 않음(teamQueueRemove가 처리)
  party.members = party.members.filter(m => m !== ws);
  party.ready.delete(ws);
  if (ws === party.host || party.members.length === 0) tpClose(party, ws === party.host ? '파티장이 나갔습니다' : '파티가 종료되었습니다');
  else tpBroadcast(party);
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

function pairFromQueue(gameMode) {
  const queue = queues[gameMode];
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
    send(a, { type: 'matched', room: roomId, role: 'host', gameMode,
              opponentSpecies: cb ? cb.species : 'fox',
              opponentNick: cb ? cb.nick : '' });
    send(b, { type: 'matched', room: roomId, role: 'guest', gameMode,
              opponentSpecies: ca ? ca.species : 'fox',
              opponentNick: ca ? ca.nick : '' });

    console.log(`[match/${gameMode}] ${roomId}: host=${ca?.id} guest=${cb?.id}`);
  }
}

function leaveRoom(ws) {
  const c = clients.get(ws);
  if (!c || !c.room) return;
  const room = rooms.get(c.room);
  if (room && room.ffa) {
    // 팀전(다인) 방: 이탈 멤버는 null 처리(인덱스 유지), 남은 인원에게 통지
    const i = room.members.indexOf(ws);
    if (i >= 0) room.members[i] = null;
    for (const o of room.members) if (o && o !== ws) send(o, { type: 'peer_left', idx: c.idx });
    if (room.members.filter(Boolean).length === 0) { rooms.delete(c.room); console.log(`[team] ${c.room} closed`); }
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
      case 'identify': {
        // 고유 ID 발급/조회: token이 있으면 같은 계정으로, 없으면 새로 발급
        const acc = identifyClient((msg.token || '').toString());
        c.accountId = acc.id;
        // 클라이언트가 로컬에 저장한 닉네임을 함께 보냈고, 서버 계정엔 닉네임이 비어 있으면 복원한다.
        // (Render 재배포로 계정 저장소가 초기화돼도 이름/ID가 항상 같이 전달되어 '이름없음'으로 뜨지 않게)
        const sentNick = (msg.nick || '').toString().trim();
        if (sentNick && !acc.nickname && sentNick.length >= 2 && sentNick.length <= 12
            && /^[a-zA-Z0-9가-힣_]+$/.test(sentNick) && !containsBadWord(sentNick) && !nicknameTaken(sentNick, acc.id)) {
          acc.nickname = sentNick; saveAccountsSoon();
          notifyFriendsChanged(acc.id);
        }
        if (acc.nickname && !c.nick) c.nick = acc.nickname; // 계정 닉네임을 소켓 표시명 기본값으로 (파티 로스터 '(닉네임 없음)' 방지)
        const cameOnline = markOnline(acc.id, ws);
        send(ws, { type: 'identified', id: acc.id, token: acc.token, nickname: acc.nickname || '' });
        pushRequestsToAccount(acc.id); // 접속 시 대기 중인 친구 요청 즉시 전달
        if (cameOnline) notifyFriendsChanged(acc.id); // 친구들 목록에 온라인 상태 즉시 반영
        break;
      }

      case 'set_nickname': {
        if (!c.accountId) { send(ws, { type: 'nickname_set_failed', reason: '계정 정보가 없습니다' }); break; }
        const nickname = (msg.nickname || '').toString().trim();
        if (nickname.length < 2 || nickname.length > 12) { send(ws, { type: 'nickname_set_failed', reason: '닉네임은 2~12자로 입력해주세요' }); break; }
        if (!/^[a-zA-Z0-9가-힣_]+$/.test(nickname)) { send(ws, { type: 'nickname_set_failed', reason: '특수문자는 사용할 수 없습니다' }); break; }
        if (containsBadWord(nickname)) { send(ws, { type: 'nickname_set_failed', reason: '부적절한 단어가 포함되어 있습니다' }); break; }
        if (nicknameTaken(nickname, c.accountId)) { send(ws, { type: 'nickname_set_failed', reason: '이미 사용 중인 닉네임입니다' }); break; }
        const acc = accounts[c.accountId];
        if (!acc) { send(ws, { type: 'nickname_set_failed', reason: '계정 정보가 없습니다' }); break; }
        acc.nickname = nickname; c.nick = nickname;
        // 같은 계정의 다른 접속 소켓(파티/매치용)들도 표시명 동기화
        const mySockets = onlineByAccount.get(c.accountId);
        if (mySockets) for (const sws of mySockets) { const sc = clients.get(sws); if (sc) sc.nick = nickname; }
        saveAccountsSoon();
        send(ws, { type: 'nickname_set', nickname });
        notifyFriendsChanged(c.accountId); // 온라인 친구들의 친구 목록에 새 닉네임 즉시 반영
        // 파티에 들어가 있으면 파티 로스터에도 즉시 반영
        if (c.teamPartyCode && teamPartyRooms.has(c.teamPartyCode)) tpBroadcast(teamPartyRooms.get(c.teamPartyCode));
        break;
      }

      case 'friend_search': {
        if (!c.accountId) { send(ws, { type: 'friend_search_result', results: [] }); break; }
        const q = (msg.query || '').toString().trim();
        if (!q) { send(ws, { type: 'friend_search_result', results: [] }); break; }
        const qLow = q.toLowerCase();
        const results = [];
        for (const acc of Object.values(accounts)) {
          if (acc.id === c.accountId) continue;
          const idMatch = acc.id.toLowerCase() === qLow;
          const nickMatch = acc.nickname && acc.nickname.toLowerCase().includes(qLow);
          if (idMatch || nickMatch) results.push({ id: acc.id, nickname: acc.nickname || '' });
          if (results.length >= 10) break;
        }
        send(ws, { type: 'friend_search_result', results });
        break;
      }

      case 'friend_add':        // 구버전 클라 호환: 즉시추가 대신 요청 전송으로 처리
      case 'friend_request': {
        if (!c.accountId) break;
        const me = accounts[c.accountId];
        const target = accounts[(msg.id || '').toString()];
        if (!me || !target) { send(ws, { type: 'friend_add_result', ok: false, reason: '존재하지 않는 ID입니다' }); break; }
        if (target.id === me.id) { send(ws, { type: 'friend_add_result', ok: false, reason: '자기 자신은 추가할 수 없습니다' }); break; }
        me.friends = me.friends || []; target.friends = target.friends || [];
        me.friendReqIn = me.friendReqIn || []; me.friendReqOut = me.friendReqOut || [];
        target.friendReqIn = target.friendReqIn || []; target.friendReqOut = target.friendReqOut || [];
        if (me.friends.includes(target.id)) { send(ws, { type: 'friend_add_result', ok: false, reason: '이미 친구입니다' }); break; }
        // 상대가 이미 나에게 요청을 보내둔 상태라면 → 바로 서로 친구로 성립(맞요청 자동 수락)
        if (me.friendReqIn.includes(target.id)) {
          me.friendReqIn = me.friendReqIn.filter(x => x !== target.id);
          target.friendReqOut = (target.friendReqOut || []).filter(x => x !== me.id);
          me.friends.push(target.id); target.friends.push(me.id);
          saveAccountsSoon();
          send(ws, { type: 'friend_add_result', ok: true, accepted: true, id: target.id, nickname: target.nickname || '' });
          send(ws, { type: 'friend_list_result', friends: buildFriendList(me) });
          pushRequestsToAccount(me.id);
          notifyFriendsChanged(me.id); notifyFriendsChanged(target.id);
          break;
        }
        if (me.friendReqOut.includes(target.id)) { send(ws, { type: 'friend_add_result', ok: false, reason: '이미 요청을 보냈습니다' }); break; }
        me.friendReqOut.push(target.id);
        target.friendReqIn.push(me.id);
        saveAccountsSoon();
        send(ws, { type: 'friend_add_result', ok: true, requested: true, id: target.id, nickname: target.nickname || '' });
        pushRequestsToAccount(target.id); // 상대가 접속 중이면 즉시 요청 목록 갱신
        break;
      }

      case 'friend_accept': {
        if (!c.accountId) break;
        const me = accounts[c.accountId];
        const fromId = (msg.id || '').toString();
        const target = accounts[fromId];
        if (!me) break;
        me.friendReqIn = me.friendReqIn || [];
        if (!me.friendReqIn.includes(fromId) || !target) {
          send(ws, { type: 'friend_requests_result', requests: buildRequestList(me) });
          break;
        }
        me.friendReqIn = me.friendReqIn.filter(x => x !== fromId);
        target.friendReqOut = (target.friendReqOut || []).filter(x => x !== me.id);
        me.friends = me.friends || []; target.friends = target.friends || [];
        if (!me.friends.includes(fromId)) me.friends.push(fromId);
        if (!target.friends.includes(me.id)) target.friends.push(me.id);
        saveAccountsSoon();
        send(ws, { type: 'friend_requests_result', requests: buildRequestList(me) });
        send(ws, { type: 'friend_list_result', friends: buildFriendList(me) });
        pushFriendListToAccount(fromId); // 요청 보낸 쪽 친구목록 즉시 갱신
        notifyFriendsChanged(me.id); notifyFriendsChanged(fromId);
        break;
      }

      case 'friend_decline': {
        if (!c.accountId) break;
        const me = accounts[c.accountId];
        const fromId = (msg.id || '').toString();
        if (!me) break;
        me.friendReqIn = (me.friendReqIn || []).filter(x => x !== fromId);
        const target = accounts[fromId];
        if (target) target.friendReqOut = (target.friendReqOut || []).filter(x => x !== me.id);
        saveAccountsSoon();
        send(ws, { type: 'friend_requests_result', requests: buildRequestList(me) });
        break;
      }

      case 'friend_requests': {
        if (!c.accountId) { send(ws, { type: 'friend_requests_result', requests: [] }); break; }
        const me = accounts[c.accountId];
        send(ws, { type: 'friend_requests_result', requests: me ? buildRequestList(me) : [] });
        break;
      }

      case 'friend_remove': {
        if (!c.accountId) break;
        const me = accounts[c.accountId]; if (!me) break;
        const targetId = (msg.id || '').toString();
        me.friends = (me.friends || []).filter(fid => fid !== targetId);
        me.friendReqIn = (me.friendReqIn || []).filter(fid => fid !== targetId);
        me.friendReqOut = (me.friendReqOut || []).filter(fid => fid !== targetId);
        const target = accounts[targetId];
        if (target) {
          target.friends = (target.friends || []).filter(fid => fid !== c.accountId);
          target.friendReqIn = (target.friendReqIn || []).filter(fid => fid !== c.accountId);
          target.friendReqOut = (target.friendReqOut || []).filter(fid => fid !== c.accountId);
        }
        saveAccountsSoon();
        send(ws, { type: 'friend_list_result', friends: buildFriendList(me) });
        send(ws, { type: 'friend_requests_result', requests: buildRequestList(me) });
        break;
      }

      case 'friend_list': {
        if (!c.accountId) { send(ws, { type: 'friend_list_result', friends: [] }); break; }
        const me = accounts[c.accountId];
        send(ws, { type: 'friend_list_result', friends: me ? buildFriendList(me) : [] });
        break;
      }

      case 'queue': {
        const gameMode = msg.gameMode === 'timeattack' ? 'timeattack' : 'deathmatch';
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        c.queueMode = gameMode;
        if (!queues[gameMode].includes(ws)) queues[gameMode].push(ws);
        send(ws, { type: 'queued', position: queues[gameMode].length });
        pairFromQueue(gameMode);
        break;
      }

      case 'cancel':
        for (const gm of ['deathmatch', 'timeattack']) { const i = queues[gm].indexOf(ws); if (i >= 0) queues[gm].splice(i, 1); }
        teamQueueRemove(ws);
        tpPartyLeave(ws);
        send(ws, { type: 'cancelled' });
        break;

      case 'queue_team': {
        // 팀전(2v2/3v3) 빠른 매칭 대기열 등록 — 봇 없음, 정원 다 찰 때까지 대기 (혼자 큐 = 크기 1짜리 그룹)
        const size = (msg.size === 3) ? 3 : 2;
        const gameMode = msg.gameMode === 'timeattack' ? 'timeattack' : 'deathmatch';
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        teamQueueGroup(size, gameMode, [ws]);
        break;
      }

      case 'team_party_create': {
        // 팀 매칭용 파티(초대) 생성 — 친구를 초대해 항상 같은 팀으로 큐에 들어감.
        const size = (msg.size === 3) ? 3 : 2;
        const gameMode = msg.gameMode === 'timeattack' ? 'timeattack' : 'deathmatch';
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const party = ensureTeamParty(ws, c, size, gameMode);
        send(ws, { type: 'team_party_created', code: party.code });
        tpBroadcast(party);
        break;
      }

      case 'team_party_invite_send': {
        // 친구 목록에서 선택한 친구에게 파티 초대를 보냄 (코드 입력 없이 바로 초대/수락)
        if (!c.accountId) { send(ws, { type: 'team_party_invite_failed', reason: '계정 정보가 없습니다' }); break; }
        const me = accounts[c.accountId];
        const targetId = (msg.targetId || '').toString();
        if (!me || !(me.friends || []).includes(targetId)) { send(ws, { type: 'team_party_invite_failed', reason: '친구 목록에 없는 사용자입니다' }); break; }
        const targetSockets = onlineByAccount.get(targetId);
        if (!targetSockets || targetSockets.size === 0) { send(ws, { type: 'team_party_invite_failed', reason: '친구가 오프라인 상태입니다' }); break; }
        const size = (msg.size === 3) ? 3 : 2;
        const gameMode = msg.gameMode === 'timeattack' ? 'timeattack' : 'deathmatch';
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const party = ensureTeamParty(ws, c, size, gameMode);
        if (party.started) { send(ws, { type: 'team_party_invite_failed', reason: '이미 매칭을 시작한 파티입니다' }); break; }
        if (party.members.length >= party.size) { send(ws, { type: 'team_party_invite_failed', reason: '파티 인원이 가득 찼습니다' }); break; }
        for (const tws of targetSockets) {
          send(tws, { type: 'team_party_invite_received', code: party.code, size: party.size, gameMode: party.gameMode, fromId: c.accountId, fromNick: me.nickname || c.nick || '' });
        }
        send(ws, { type: 'team_party_invite_sent', code: party.code, targetId });
        tpBroadcast(party);
        break;
      }

      case 'team_party_invite_decline': {
        // 초대받은 쪽이 거절 — 파티장에게만 알림 (파티 자체는 그대로 유지)
        const code = (msg.code || '').toString().toUpperCase().trim();
        const party = teamPartyRooms.get(code);
        if (party && party.host) send(party.host, { type: 'team_party_invite_declined', nick: c.nick || '', aid: c.accountId || null });
        break;
      }

      case 'team_party_join': {
        c.species = msg.species || 'rabbit';
        c.nick = (msg.nick || '').toString().slice(0, 12);
        const code = (msg.code || '').toString().toUpperCase().trim();
        const party = teamPartyRooms.get(code);
        if (!party) { send(ws, { type: 'team_party_join_failed', reason: '존재하지 않는 초대 코드입니다' }); break; }
        if (party.started) { send(ws, { type: 'team_party_join_failed', reason: '이미 매칭을 시작한 파티입니다' }); break; }
        if (party.members.includes(ws)) { send(ws, { type: 'team_party_join_failed', reason: '자기 파티에는 참가할 수 없습니다' }); break; }
        if (party.members.length >= party.size) { send(ws, { type: 'team_party_join_failed', reason: '파티 인원이 가득 찼습니다' }); break; }
        party.members.push(ws);
        c.teamPartyCode = code;
        send(ws, { type: 'team_party_joined', code, size: party.size, gameMode: party.gameMode, idx: party.members.length - 1 });
        tpBroadcast(party);
        break;
      }

      case 'team_party_update': {
        // 파티에 들어간 상태에서 캐릭터/닉네임을 바꾸면 로스터에 실시간 반영
        if (msg.species) c.species = msg.species;
        if (msg.nick !== undefined) c.nick = (msg.nick || '').toString().slice(0, 12);
        if (!c.teamPartyCode) break;
        const party = teamPartyRooms.get(c.teamPartyCode);
        if (party && !party.started) tpBroadcast(party);
        break;
      }

      case 'team_party_ready': {
        if (!c.teamPartyCode) break;
        const party = teamPartyRooms.get(c.teamPartyCode);
        if (!party) break;
        if (msg.ready) party.ready.add(ws); else party.ready.delete(ws);
        tpBroadcast(party);
        break;
      }

      case 'team_party_start': {
        // 호스트만 시작 가능. 호스트를 제외한 전원이 준비완료 상태여야 함.
        if (!c.teamPartyCode) break;
        const party = teamPartyRooms.get(c.teamPartyCode);
        if (!party || party.host !== ws || party.started) break;
        const allReady = party.members.every(m => m === party.host || party.ready.has(m));
        if (!allReady) { send(ws, { type: 'team_party_start_failed', reason: '아직 준비를 마치지 않은 파티원이 있습니다' }); break; }
        party.started = true;
        teamPartyRooms.delete(party.code);
        party.members.forEach(m => { const mc = clients.get(m); if (mc) mc.teamPartyCode = null; });
        teamQueueGroup(party.size, party.gameMode, party.members.slice());
        break;
      }

      case 'team_party_leave':
        tpPartyLeave(ws);
        break;

      case 'leave':
        leaveRoom(ws);
        break;

      // 게임 중 실시간 메시지: 그대로 상대에게 중계
      case 'input':    // guest -> host: 내 입력
      case 'state':    // 위치/회전/애니 상태 브로드캐스트
      case 'bot_state':// host -> 나머지: 호스트가 시뮬레이션하는 봇 퍼펫 상태 (없으면 봇이 스폰지점에 멈춰 안 보임)
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
    if (c && c.accountId) {
      const wentOffline = markOffline(c.accountId, ws);
      if (wentOffline) notifyFriendsChanged(c.accountId); // 친구들 목록에 오프라인 상태 즉시 반영
    }
    for (const gm of ['deathmatch', 'timeattack']) { const i = queues[gm].indexOf(ws); if (i >= 0) queues[gm].splice(i, 1); }
    teamQueueRemove(ws);
    tpPartyLeave(ws);
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

// 계정 저장소를 먼저 로드한 뒤 접속을 받기 시작 (Redis 로드 전 접속 시 새 계정이 발급되는 것 방지)
loadAccounts().catch(e => console.error('[boot] loadAccounts failed', e && e.message)).finally(() => {
  server.listen(PORT, () => {
    console.log(`Animal Arena PVP server on :${PORT} (store: ${useRedis ? 'Upstash Redis' : 'file(휘발성)'})`);
  });
});
