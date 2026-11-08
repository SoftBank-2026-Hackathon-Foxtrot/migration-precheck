# migration-precheck

시연이나 마이그레이션을 시작하기 전에, 유저 앱 리소스가 **옮길 준비가 된 상태**인지 확인하고, 안전하게 고칠 수 있는 부분은 고칩니다. 몇 번을 돌려도 같은 상태에서 끝나도록(멱등) 만들었습니다.

## 시연 준비 상태 (chiikawa, AWS → GCP)

| 구분 | 있어야 하는 상태 |
|---|---|
| **원본 (AWS)** | 앱 VM 1대가 떠 있고 `/readyz` 200. RDS(app 롤 LOGIN, `wal_level=logical`, 데이터 있음), ElastiCache(KEA), S3에 데이터. PgBouncer와 Redis proxy가 원본을 가리킴. LB 트래픽 100%. |
| **대상 (GCP)** | 미리 만들어 둔 것만 있음: 앱 VM(`/readyz` 200), Cloud SQL(스키마는 있고 행 0, app 롤 NOLOGIN, logical decoding), Memorystore(키 0, KEA), 빈 GCS 버킷. |
| **남은 흔적 없음** | `fxt_*` 퍼블리케이션·구독·슬롯, 버킷 change feed 알림, `om-*` SQS 큐와 Pub/Sub. |
| **플랫폼** | ECS 서비스 안정, 끝나지 않은 워크플로 없음, seed가 배포와 일치(appUrl = VM IP, 환경별 리소스 ref), Tailscale 노드와 라우트 online, LB 풀이 각 앱 터널을 가리킴. |

## 사용법

필요한 것: `aws`(프로필 `foxtrot`, `aws login`), `gcloud`(configuration `foxtrot`), Session Manager 플러그인, Python 3.11+. 외부 패키지는 없습니다.

```bash
python3 -m precheck check          # 읽기만 함. 실패가 있으면 종료 코드 1
python3 -m precheck check -v       # 통과한 항목까지 표시
python3 -m precheck check --json   # 콘솔이나 CI에서 쓸 JSON
python3 -m precheck fix            # fix --apply가 할 일만 표시
python3 -m precheck fix --apply    # 고친 뒤 다시 확인
```

다른 앱이나 방향이면 `config/chiikawa.toml`을 복사해 `-c`로 넘기세요(예: source와 target 교체).

## 어떻게 읽나

- 클라우드 API(EC2, RDS, ElastiCache, S3, Cloud SQL, Memorystore, GCS, Pub/Sub, Cloudflare, Tailscale)는 이 컴퓨터에서 CLI로 읽습니다.
- 앱 VPC 안의 데이터는 이 컴퓨터에서 닿지 않으므로, 플랫폼 ECS 태스크 안에서 probe를 ECS exec로 돌려 읽습니다. 해당 태스크는 tailnet으로 두 앱 VPC에 닿습니다.
  - `probes/db.py`: db-proxy `backend`. 양쪽 DB(롤, 행 수, 복제 객체, wal_level), PgBouncer 경로, 앱 `/readyz`.
  - `probes/redis.py`: redis-migrator `statemig`. 양쪽 Redis 키 수와 proxy의 active 백엔드.
  - `probes/cp.py`: control-plane `server`. 로드된 seed와 끝나지 않은 워크플로.

## 무엇을 자동으로 고치나

| 고침 | 조건 (하나라도 아니면 거부하고 수동 안내) |
|---|---|
| 대상 DB TRUNCATE, 대상 app 롤 NOLOGIN | PgBouncer와 Redis proxy가 원본을 가리킴. probe 안에서도 PgBouncer가 그 DB를 가리키면 거부 |
| 원본 app 롤 LOGIN | 데이터 경로가 원본 |
| 대상 Redis FLUSHALL | proxy의 active 백엔드가 원본 |
| 대상 버킷 비우기 | 데이터 경로가 원본이고 열린 워크플로 없음. **원본에도 있는 키만** 지움(`--force`로 전부) |
| `fxt_*` 복제 객체 삭제 | 열린 워크플로 없음, 데이터 경로가 원본 |
| 버킷 change feed, `om-*` 큐·토픽·구독 삭제 | 열린 워크플로 없음 |
| HALTED/PLANNED/WAITING_APPROVAL 워크플로 ABORT | — |

트래픽 이동, seed 수정, VM·서비스·Tailscale·LB 풀 문제는 고치지 않고 할 일만 알려줍니다.

## 테스트

```bash
python3 -m unittest discover -s tests
```
