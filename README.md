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

관리자 비밀번호를 잊었다면 서버를 중지한 뒤 `EVENTGLASS_DATA_DIR=./data target/debug/eventglass admin reset-password admin@example.com`을 실행합니다. 활성 관리자만 복구하며 새 무작위 비밀번호를 stdout에 한 번 출력하고 해당 계정의 기존 세션을 모두 회수합니다. 출력은 비밀번호 관리자에 보관하고 서버를 다시 시작하세요. 비밀번호를 명령 인수나 로그에 넣지 않습니다.

`EVENTGLASS_DATA_DIR=./data target/debug/eventglass doctor`는 서버를 시작하거나 DB를 초기화하지 않고 기존 metadata, catalog, local shard manifest를 읽기 전용으로 검사합니다.

`EVENTGLASS_ADDR`, `EVENTGLASS_DATA_DIR`, `EVENTGLASS_BASE_URL`로 주소·데이터 위치·외부 origin을 설정합니다. 원격 접속용 origin은 HTTPS가 필요합니다. S3 빌드는 `EVENTGLASS_S3_URL=s3://bucket/prefix`를 사용하며 새 빈 prefix는 최초 한 번 `EVENTGLASS_S3_INITIALIZE=true`가 필요합니다. 호환 서버는 loopback `EVENTGLASS_S3_ENDPOINT`로만 지정할 수 있습니다.

완료 checkpoint가 검증한 shard는 디스크가 부족할 때 pin이 없는 항목부터 회수됩니다. 이후 검색·집계·상세 조회는 같은 S3 archive를 체크섬 검증 후 단일 실행으로 다시 설치하며, 응답의 `hydrated_shards`에 이번 요청에서 복원한 수를 표시합니다.

Webhook은 HTTPS와 공개 DNS 주소만 허용하고 redirect와 system proxy를 사용하지 않습니다. 사내 private 주소가 꼭 필요하면 정확한 host만 `EVENTGLASS_WEBHOOK_ALLOW_PRIVATE_HOSTS`에 쉼표로 나열합니다. 경보 delivery는 안정적인 `X-Eventglass-Delivery` ID를 보내며, 수신 성공 직후 프로세스가 종료될 수 있어 at-least-once입니다.

작업 이력과 단계별 검증 결과는 Git 커밋과 CI에 기록합니다. 별도 단계별 실행 일지는 만들지 않습니다.

실제 SDK 호환성 게이트는 Python Sentry SDK 2.69.0의 기본 로깅·DEBUG opt-in·FastAPI·Celery fork, Node SDK 10.73.0의 Error·Message·structured logs, Browser SDK 10.73.0의 Error·Message·console log와 Chromium cross-origin CORS, Go SDK 0.49.0의 Error·Message를 localhost의 실제 Eventglass 프로세스에 보냅니다. S3-compatible 게이트는 MinIO RELEASE.2025-09-07T16-13-09Z에서 conditional create, pagination, checksum/multipart abort, checkpoint fallback과 전체 로컬 데이터 유실 복구를 검증합니다. 실제 AWS S3 계약은 RC 전 검증 대상이며 현재 지원 완료로 표시하지 않습니다.

## 운영과 복구

기준 운영 자원은 1 CPU와 1 GiB RAM이며 swap 없이 실행하는 CI gate가 있습니다. 100K Record PR benchmark는 더 작은 512 MiB 한도에서 실행합니다. 용량과 지연은 데이터 분포·샤드 수·검색 범위에 따라 달라지므로 배포 전 실제 트래픽과 보존 기간으로 측정해야 합니다.

`EVENTGLASS_S3_URL`의 bucket/prefix는 설치 하나가 단독으로 사용해야 합니다. 현재 명시적으로 검증한 호환 대상은 loopback endpoint의 MinIO RELEASE.2025-09-07T16-13-09Z입니다. 같은 prefix에 두 Eventglass 프로세스를 동시에 연결하면 안 됩니다. checkpoint는 샤드 봉인 경계에서 만들어지므로 고정된 시간 RPO를 보장하지 않습니다. `/api/system/status`의 복구 가능 경계와 backup lag를 감시해야 합니다.

전체 로컬 손실 복구 절차는 다음과 같습니다.

1. 원래 프로세스를 정지하고 다시 시작되지 않게 합니다. 남은 `/data`는 조사와 rollback을 위해 별도 위치에 보존합니다.
2. 빈 데이터 디렉터리에 기존과 같은 S3 bucket/prefix 및 자격 증명을 설정하고 Eventglass를 시작합니다. 로컬 `meta.db`가 없으면 완료된 최신 checkpoint부터 검증하며, 손상되거나 불완전한 후보는 건너뜁니다.
3. `/readyz`와 `eventglass doctor`를 확인한 뒤 프로젝트·수신 키·이슈 수와 상태·대표 검색·과거 상세 조회를 대조합니다. 복구 시점보다 나중의 사용자, 이슈 상태 변경, key revoke는 checkpoint로 되돌아갈 수 있습니다.
4. 복구는 모든 기존 session과 검색 token secret을 무효화합니다. 관리자는 다시 로그인하고 관리자 인증 정보와 프로젝트 수신 키의 회전 필요성을 검토해야 합니다.

업그레이드 전에는 완료 checkpoint와 `/data` 사본을 확보하고 현재 바이너리 경로를 기록합니다. 새 바이너리를 설치한 뒤 서버를 띄우기 전에 `eventglass doctor`로 기존 SQLite schema와 native shard format을 검사하고, 시작 후 `/readyz`와 실제 수신·검색을 확인합니다. 호환하지 않는 native format에는 자동 background reindex가 없으므로 시작 거절 시 이전 바이너리로 돌아가 명시적인 migration 경로를 준비해야 합니다. 새 바이너리가 DB migration을 적용한 뒤에는 이전 바이너리가 해당 schema를 읽는다는 검증 없이 rollback하면 안 됩니다.

## 릴리스 산출물

`./scripts/release-check`는 신뢰한 현재 commit에서 embedded UI와 S3가 포함되고 failpoints가 빠진 Linux amd64/arm64 바이너리를 각각 빌드합니다. `.tools/release/`에 두 ELF 바이너리, SPDX 2.3 dependency inventory, third-party license notice, `SHA256SUMS`를 만들고 모든 checksum과 architecture를 다시 검증합니다. 이 명령은 산출물을 게시하거나 배포하지 않습니다.
