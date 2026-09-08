# eventglass

Eventglass: 단일 Rust 프로세스 기반 Error Tracking + Application Log 검색 서버.

관리자·프로젝트 관리, Sentry 수신과 durable Inbox, Indexer의 검색 공개·재시작 복구, Issue·검색·집계·Live·Related Logs·경보 API와 UI, S3 checkpoint 복구와 cold hydration을 구현했습니다. 운영 배포 검증은 진행 중입니다. 전체 범위는 [구현 설계](docs/observe/implementation.md)와 [아키텍처](docs/observe/architecture.md), 개발 명령은 [개발 안내](CONTRIBUTING.md)를 확인하세요.

제품명과 실행 파일은 저장소 이름에 맞춰 **Eventglass / `eventglass`**를 사용합니다. 첨부된 원안은 변경하지 않아 원안과 과거 실행 증거에는 이전 이름이 남아 있습니다.

로컬 시작:

```sh
./scripts/bootstrap
npm --prefix web run build
cargo build --locked --features embed-ui --bin eventglass
EVENTGLASS_DATA_DIR=./data target/debug/eventglass admin setup-token
EVENTGLASS_DATA_DIR=./data target/debug/eventglass serve
```

서버 실행 **전에** 일회용 설정 토큰을 발급합니다. 실행 중에는 데이터 디렉터리 잠금 때문에 관리 CLI를 동시에 사용할 수 없습니다. 기본 주소는 `http://127.0.0.1:8080`이며, 브라우저의 초기 설정 화면에서 토큰으로 첫 관리자를 생성합니다. 토큰은 30분 후 만료됩니다.

`EVENTGLASS_DATA_DIR=./data target/debug/eventglass doctor`는 서버를 시작하거나 DB를 초기화하지 않고 기존 metadata, catalog, local shard manifest를 읽기 전용으로 검사합니다.

`EVENTGLASS_ADDR`, `EVENTGLASS_DATA_DIR`, `EVENTGLASS_BASE_URL`로 주소·데이터 위치·외부 origin을 설정합니다. 원격 접속용 origin은 HTTPS가 필요합니다. S3 빌드는 `EVENTGLASS_S3_URL=s3://bucket/prefix`를 사용하며 새 빈 prefix는 최초 한 번 `EVENTGLASS_S3_INITIALIZE=true`가 필요합니다. 호환 서버는 loopback `EVENTGLASS_S3_ENDPOINT`로만 지정할 수 있습니다.

완료 checkpoint가 검증한 shard는 디스크가 부족할 때 pin이 없는 항목부터 회수됩니다. 이후 검색·집계·상세 조회는 같은 S3 archive를 체크섬 검증 후 단일 실행으로 다시 설치하며, 응답의 `hydrated_shards`에 이번 요청에서 복원한 수를 표시합니다.

Webhook은 HTTPS와 공개 DNS 주소만 허용하고 redirect와 system proxy를 사용하지 않습니다. 사내 private 주소가 꼭 필요하면 정확한 host만 `EVENTGLASS_WEBHOOK_ALLOW_PRIVATE_HOSTS`에 쉼표로 나열합니다. 경보 delivery는 안정적인 `X-Eventglass-Delivery` ID를 보내며, 수신 성공 직후 프로세스가 종료될 수 있어 at-least-once입니다.

작업 이력과 단계별 검증 결과는 Git 커밋과 CI에 기록합니다. 별도 단계별 실행 일지는 만들지 않습니다.

실제 SDK 호환성 게이트는 Python Sentry SDK 2.69.0의 기본 로깅·DEBUG opt-in·FastAPI·Celery fork, Node SDK 10.73.0의 Error·Message·structured logs, Browser SDK 10.73.0의 Error·Message·console log와 Chromium cross-origin CORS, Go SDK 0.49.0의 Error·Message를 localhost의 실제 Eventglass 프로세스에 보냅니다. S3-compatible 게이트는 MinIO RELEASE.2025-09-07T16-13-09Z에서 conditional create, pagination, checksum/multipart abort, checkpoint fallback과 전체 로컬 데이터 유실 복구를 검증합니다. 실제 AWS S3 계약은 RC 전 검증 대상이며 현재 지원 완료로 표시하지 않습니다.
