# 애니멀 아레나 — PVP 서버

1v1 매칭(랜덤·랭킹·친구방) + 개인전(FFA, 최대 5인) 매치메이킹 · 메시지 중계(relay) 서버. Node.js + ws.

## 로컬 실행
```
npm install
npm start          # 기본 포트 8080, PORT 환경변수로 변경 가능
```
health check: http://localhost:8080/health

## Render 배포 (무료)
1. 이 폴더를 GitHub 저장소에 올림
2. https://render.com → New → Web Service → 그 저장소 선택
3. Environment: Node / Build: `npm install` / Start: `node server.js` / Health Check: `/health`
   (render.yaml 이 있으면 자동 인식됨)
4. 배포 완료 후 나오는 주소를 wss:// 로 게임에 설정:
   index.html 의 `window.__PVP_SERVER = "wss://<새주소>.onrender.com";`

## 프로토콜 (요약)
- 1v1: queue / createroom / joinroom → matched(role: host|guest)
- 개인전: queue_ffa → ffa_wait(count,secs) → ffa_matched(idx, seed, players[])
  - 2명 이상 모인 뒤 10초간 추가 입장 없으면 시작, 5명 차면 즉시 시작
  - 방 내 메시지는 from(발신자 idx) 태깅 브로드캐스트, hit/kill 은 to 대상에게만 라우팅
  - 이탈 시 peer_left(idx) 통지
- 서버는 중계만 함. 판정은 클라이언트(피격자 권위 + 픽업은 idx0/host 권위).
