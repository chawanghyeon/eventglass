# 품질·CI·pre-commit 실행 설계

이 문서는 품질 기준과 아직 완료되지 않은 RC 검증 요구사항이다. 실제 검사 명령은 scripts/check, 배포 workflow는 .github/workflows/deploy.yml을 따른다. 단계별 검증 결과는 커밋 메시지와 로컬 자원 검사 JSON에 기록한다.

## 1. 검사 계층

| 계층 | 실행 시점 | 내용 | 목표 소요 시간 |
|---|---|---|---|
| pre-commit | staged 변경 commit 전 | syntax/format/secrets/작은 설정 검사 | warm cache 15초 내외 |
| pre-push | push 전 | Rust lint/unit, web type/lint/unit | warm cache 3분 내외 |
| pre-push release | main push 직전 | 변경 관련 hook 검사와 정확한 ARM64 artifact staging | 로컬 머신 |
| GitHub deploy | main push | 미리 staging된 commit SHA 활성화와 readiness 확인 | 3분 hard timeout |
| local extended | 필요 시 배포 후보 준비 | SDK live/E2E/S3-compatible/resource/security/성능 | 개발 머신의 격리 Docker 환경 |
| release candidate | 배포 후보 | 전체 복구/10M benchmark/multiarch image/upgrade 검증 | 로컬 전용 실행 환경 |

2026-09-09 사용자 비용 지시에 따라 GitHub Actions는 로컬에서 검증하고 staging한 commit을 활성화하는 배포만 담당한다. 예약·수동 Capacity workflow와 test/build job은 GitHub Actions에서 실행하지 않는다. 무거운 검사는 제품에서 삭제하지 않고 필요할 때만 로컬 배포 후보 검증으로 실행한다.

hook에서 E2E·Docker pull·SDK 설치·전체 Cargo build를 매 commit 실행하지 않는다. 변경 파일에 맞는 빠른 로컬 hook이 push를 막고, GitHub Actions는 검증을 반복하지 않고 정확한 commit만 활성화한다.

## 2. 실행 명령의 단일 진입점

POSIX shell script는 repo root를 직접 찾고 `set -eu`를 쓴다. pipefail이 필요하면 Bash를 명시한다. 검사와 build는 로컬 script를 단일 진입점으로 사용한다. Python helper는 stdlib 우선이며 필요 도구는 버전 고정한다.

| 명령 | 책임 |
|---|---|
| `./scripts/bootstrap` | 정확한 도구 버전 확인, Cargo fetch --locked, npm ci, pre-commit 환경 준비 |
| `./scripts/check quick` | pre-commit 범위와 같은 비변경 검사 |
| `./scripts/check rust` | fmt/check/clippy/unit·doc test |
| `./scripts/check web` | generated API drift/typecheck/lint/unit/build |
| `./scripts/check integration` | Rust integration + 오프라인 SDK fixture replay |
| `./scripts/check crash-smoke` | PR 필수 대표 crash suite |
| `./scripts/check crash-full` | 모든 failpoint와 interleaving 반복 |
| `./scripts/check e2e` | 실제 서버+Chromium 핵심 사용자 흐름 |
| `./scripts/check storage` | 로컬 seal/checkpoint/restore 및 S3-compatible suite |
| `./scripts/check security` | dependency advisories/licenses/secrets/workflow lint |
| `./scripts/check all` | PR 필수 검사 전체; RC 성능 검사는 제외 |
| `./scripts/bench --records 100000 --seed 20260908` | 재현 가능한 dataset 생성/측정/report |
| `./scripts/release-check` | production artifact, UI 포함, failpoints 제외, offline/local-only smoke |

`bootstrap`은 .git hook 설치 여부를 명시적으로 출력하며 의존성 lock을 바꾸지 않는다. 사용자 global git config를 변경하지 않는다. 도구가 없거나 버전이 다르면 설치할 정확한 명령을 알려주고 실패한다. 네트워크 설치 실패를 무시하지 않는다.

없는 제품 target을 `if exists then test else success`로 처리하지 않는다. P02부터 `all`은 frontend test 부재를 허용하지 않는다. 후속 기능의 검증은 구현 변경과 함께 추가하되 GitHub runner가 아니라 로컬 검사 계층에 연결한다.

Rust 기본 명령:

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --lib
cargo test --locked --doc
cargo test --locked --test contracts
cargo test --locked --test integration
cargo test --locked --test sdk
```

crash 전용 명령은 `cargo test --locked --features failpoints --test crash`다. PR selector는 crash test module `smoke::`로 고정하고 runner가 실행 테스트 수를 확인한다. `cargo test` 필터가 0건이어도 exit 0일 수 있으므로 0 test를 통과로 인정하지 않는다.

`--all-features`를 production build에 무조건 쓰지 않는다. default, failpoints, embed-ui, S3 조합은 필요할 때 로컬 확장 검사에서 확인한다. release binary는 `--features embed-ui,s3`로 만들고 failpoints를 포함하지 않는다.

Frontend npm scripts 계약:

```json
{
  "typecheck": "tsc --noEmit",
  "lint": "eslint . --max-warnings=0",
  "format:check": "prettier --check .",
  "test": "vitest run",
  "build": "vite build",
  "test:e2e": "playwright test"
}
```

Vitest no-tests pass 옵션 금지. OpenAPI codegen은 임시 출력에 재생성해 tracked generated.ts와 비교한다. 검사 중 `npm install`/`cargo update`/format --write로 lock/source를 고치지 않는다. 검사 전후 tracked tree와 생성되어야 할 untracked 결과를 확인한다.

### 커버리지 실행과 판정

`npm --prefix web run test:coverage`는 전체 프론트엔드 제품 소스의 coverage를 수집하고 파일별 라인·함수 100%를 강제한다. `./scripts/check web`과 pre-push의 web 검사도 같은 gate를 실행한다. 생성된 API 타입, 테스트 파일, 테스트 setup만 측정 대상에서 제외한다. HTML과 JSON 요약은 git에서 제외된 `web/coverage/`에 생성한다. 문장·분기 지표도 보고서에 그대로 남기며 라인·함수 gate 통과를 모든 분기 100%로 표현하지 않는다. 실행 불가능한 방어 분기를 숨기기 위한 ignore 지시나 제품 코드 삭제는 허용하지 않는다.

Rust는 고정 toolchain의 `llvm-tools-preview`와 `cargo-llvm-cov 0.9.1`로 별도 계측한다. 비용 효율 신규 모듈 `src/efficiency.rs`, `src/storage/observed.rs`, `src/storage/reuse.rs`의 라인·함수 100%와 기존 모듈을 포함한 전체 결과를 구분한다. SQLite·취소·checksum·checkpoint·권한·자원 제한 계약은 커버리지 수치와 별도로 실제 통합 테스트를 실행해야 한다. stable 계측에서 branch count가 0이면 분기 측정 불가이며 100%가 아니다. Rust 파일에 함께 있는 `#[cfg(test)]` 코드도 LLVM 보고서에 포함되므로 전체 결과를 제품 전용 커버리지로 바꾸어 표현하지 않는다.

## 3. pre-commit 구성

P00 산출물은 `.pre-commit-config.yaml`, 버전 고정된 Python tool requirements, `scripts/hooks/`, contributor 설치 안내다. hook stage 이름은 `pre-commit`, `pre-push`를 사용하고 설치 시 양쪽 hook type을 명시한다.

```sh
pre-commit install --hook-type pre-commit --hook-type pre-push
pre-commit run --all-files --hook-stage pre-commit
pre-commit run --all-files --hook-stage pre-push
```

| Hook | 적용 파일 | 동작 |
|---|---|---|
| merge-conflict/EOF/trailing whitespace | 텍스트, generated/vendor 제외 | conflict marker/파일 끝/공백 확인; 원안 Markdown hard break는 허용 |
| YAML/TOML/JSON syntax | 해당 확장자 | parse 검사; GitHub expression/다중 YAML 허용 설정 확인 |
| secret scan | staged text, fixture 포함 | 검증된 scanner 사용, exact fixture exception만 허용 |
| rustfmt | `.rs` 변경 | cargo fmt --check, pass_filenames=false, require_serial=true |
| web format | web source/config | pinned local Prettier check; 파일명 인자를 안전하게 전달 |
| workflow lint | `.github/workflows/` | actionlint; shell snippets는 shellcheck 포함 |
| pre-push-rust | Rust/manifest/migration 변경 | scripts/check rust |
| pre-push-web | web/API contract 변경 | scripts/check web |

부분 staging에서 hook이 unstaged 변경을 commit에 섞지 않도록 check-only를 기본으로 한다. 자동 포맷은 별도 `scripts/format` 명령이다. hook은 `git add`, `git reset`, stash 조작을 직접 하지 않는다. pre-commit이 제공하는 staged 실행을 사용한다. [공식 pre-commit 문서](https://pre-commit.com/).

외부 hook은 실제 검증한 immutable revision에 고정한다. local `language: system`은 bootstrap의 도구 버전 검사와 짝지어 사용한다. Node hook에서 `npx`가 누락 패키지를 인터넷에서 자동 설치하게 하지 않는다. local binary 또는 npm script만 사용한다.

에이전트는 검사 실패를 숨기려고 `SKIP`이나 `--no-verify`를 사용하지 않는다. 예외가 필요하면 원인·범위·만료일을 기록하고 필수 불변조건 테스트를 제외하지 않는다. pre-push 마지막 단계는 현재 commit의 ARM64 artifact를 root-owned 원격 수신기에 staging하며, 이 단계가 실패하면 push도 실패한다.

## 4. GitHub 배포 workflow

GitHub Actions는 main에 push된 commit을 배포하는 최소 제어면이다. 로컬 pre-push가 현재 commit의 검사와 artifact staging을 완료하지 못하면 push 자체를 실패시킨다.

최종 `.github/workflows/deploy.yml` 계약:

- event는 main push와 명시적 재실행만 허용한다.
- permissions 기본 `contents: read`. 불필요한 write/id-token/secrets 권한 없음.
- production concurrency는 직렬화하고 실행 중 배포를 취소하지 않는다.
- checkout, toolchain setup, dependency install, test, build, artifact upload를 수행하지 않는다.
- 3분 timeout 안에서 제한된 SSH key로 `activate <commit>`만 전송한다.

| Job ID | GitHub-hosted 범위 | 로컬 범위 |
|---|---|---|
| deploy | staging된 정확한 SHA 활성화와 서버 readiness | 모든 검사, build, immutable artifact staging |

배포 수신기는 commit별 immutable bytes와 SHA-256을 확인하고, binary 내장 revision이 요청 SHA와 일치할 때만 staging한다. 활성화 실패 시 직전 binary로 되돌리고 readiness 실패를 성공으로 처리하지 않는다.

현재 운영 방식은 main 직접 push다. force push 없이 pre-push 검증과 staging을 통과한 commit만 올리고, 배포 workflow 실패는 운영 반영 실패로 취급한다.

### CI 보안과 공급망

fork PR에 repository secrets/AWS credentials/self-hosted runner를 노출하지 않는다. `pull_request_target`으로 비신뢰 PR 코드를 실행하지 않는다. action SHA pin과 최소 권한을 적용한다. [GitHub Actions 보안 지침](https://docs.github.com/en/actions/reference/security/secure-use).

Rust advisory/license는 cargo-deny, npm은 npm audit의 machine-readable 결과를 검사한다. new vulnerability로 검사에 실패하면 심각도와 실제 영향 검토를 기록하고 수정 또는 ID별 만료 예외를 둔다. 전체 audit disable/무기한 broad allow는 금지한다. scanner 자체 오류나 advisory DB fetch 실패는 정상 clean 결과가 아니다.

`deny.toml`은 licenses allowlist와 source 정책을 기록한다. 프로젝트 배포 license는 소유자의 선택 사항이므로 임의 MIT/Apache license를 선언하지 않는다. 의존성 license 목록과 notice 산출물은 만들어 검토 가능하게 한다.

GitHub workflow는 cache와 artifact upload를 사용하지 않는다. 배포 binary는 push 직전 로컬에서 현재 commit을 넣어 빌드하고 서버 수신기가 내장 revision과 checksum을 검증한다.

## 5. 실패 재현과 테스트 산출물

테스트 공통 seed `20260908`, 임시 디렉터리는 OS temp 아래 고유 prefix, 포트는 0번 bind로 할당한다. 고정 sleep으로 안정성을 맞추지 말고 ready poll/IPC barrier/deadline을 사용한다. timeout 실패에는 마지막 상태·boundary·seed를 남긴다.

DB가 필요한 테스트는 실제 파일 SQLite WAL/FULL로 실행한다. in-memory SQLite로 fsync/backup/crash를 검증하지 않는다. native Tantivy도 실제 directory/reopen을 사용한다. object store fake는 실패 분기 unit test에만 사용하고 S3 protocol 호환성 증거로 쓰지 않는다.

Crash harness:

1. 부모 test process가 임시 data_dir와 **test-only failpoints binary**를 실행한다.
2. 지정 failpoint에 도달했다는 IPC 신호를 받은 후 SIGKILL한다. 일반 panic/drop을 durable crash의 대체로 쓰지 않는다.
3. 부모가 관측한 ACK receipt와 fixture expected identities를 ledger로 유지한다. ledger는 제품 저장소가 아니라 테스트 oracle이다.
4. failpoint 없이 같은 data_dir로 restart하고 ready까지 deadline으로 기다린다.
5. 모든 페이지의 IDs/count, occurrence/outbox, A/C/W, 아직 pending Inbox를 대조한다.

ACK를 관측한 record는 반드시 존재해야 한다. commit은 됐으나 ACK가 전달되지 않은 record는 존재할 수 있다. source event ID 없는 별도 HTTP retry는 별도 acceptance이므로 정상 중복 가능성을 oracle에서 구분한다.

failpoint names는 `inbox.before_commit`, `inbox.after_commit`, `index.after_add`, `index.before_prepare`, `index.after_prepare`, `index.after_commit`, `db.during_finalize`, `db.after_finalize`, `reader.after_reload`, `live.before_publish`, `seal.before_manifest_rename`, `seal.after_manifest_rename`, `seal.after_catalog`, `active.after_create`, `active.before_register`, `backup.after_snapshot_upload`, `backup.after_checkpoint`, `alert.after_send`를 기준으로 전체 matrix를 확장한다.

환경변수로 production binary에서 crash를 일으킬 수 없어야 한다. feature compile-time 제거, release 기능 목록 검사, production에서 failpoint 인자가 거절/무시되는 smoke를 함께 둔다.

일상 변경은 관련 대표 지점만 실행하고, 로컬 확장 검사는 모든 지점 seed 5개, RC는 seed 20개를 기본으로 한다. 반복 실패를 자동 재시도 후 green으로 덮지 않는다. flaky test는 원인/격리 기간/대체 증거가 필요하며 durability/security 핵심 gate는 격리할 수 없다.

test artifacts는 JUnit 또는 구조화 result JSON, sanitized server log, test seed, version manifest, Playwright 실패 trace/screenshot이다. 실제 secret/원본 사용자 payload는 저장하지 않는다. benchmark/evidence 보고는 release별로 로컬 보존한다.

## 6. SDK 실전 검증

오프라인 fixture replay와 실제 SDK 송신을 분리한다. fixture replay는 네트워크/SDK 설치 없이 실행한다. 로컬 확장 검사와 RC는 고정 SDK의 최소 앱을 실제 실행하여 Eventglass HTTP endpoint로 보낸다.

| 앱 | 실제 검증할 흐름 |
|---|---|
| Python FastAPI | HTTP exception/message/log + shutdown flush |
| Python logging/Celery | INFO 기본·DEBUG opt-in·ERROR log+event, worker fork/flush |
| Browser JS | 실제 Chromium SDK 초기화, console integration, CORS, 오류/로그 |
| Node JS | captureException/message/structured log, process flush |
| Go | error/message, 지원 SDK에서 structured logs, shutdown flush |

Celery broker가 실제 필요하면 **테스트 앱에 한정된** fixture dependency다. Eventglass production compose/image의 dependency로 추가하지 않는다. broker 없는 모드만 실행하면 실제 broker/fork lifecycle 검증 완료라고 하지 않는다.

SDK가 기능을 지원하지 않으면 버전 표에 unsupported/이유를 남긴다. 이를 가짜 envelope를 만들어 실제 SDK 지원이라고 표시하지 않는다. 최소 supported 버전은 G07에서 결정하고 P11에서 매트릭스 전체로 확장한다.

각 fixture 디렉터리: metadata.json(SDK/runtime/integration/압축/기대 kind), sanitized headers, envelope bytes, expected normalized JSON, 생성 script. binary payload와 byte length는 scrub 후 다시 맞춘다. sentinel 외 실제 credentials는 포함하지 않는다.

## 7. S3·디스크·자원 검사

로컬 storage 검사는 고정 digest의 검증 가능한 S3-compatible test server를 격리 실행한다. localhost ephemeral credential만 사용한다. AWS S3 실제 계약은 RC의 전용 bucket/prefix에서 별도로 검사한다. 권한/예산이 없으면 AWS 검증은 BLOCKED이며 release 완료로 표시하지 않는다.

AWS 검사는 로컬의 명시적 RC 명령에서만 실행한다. 고유 run prefix와 installation ID를 사용하고 생성한 테스트 객체만 cleanup한다. bucket 전체 삭제 금지. checksum/multipart/conditional create/list pagination/abort/restore 검사를 실제 SDK로 실행하며 object bytes와 요청 수를 기록한다.

Full-loss test는 **harness가 만든 임시 data_dir**임을 marker와 경로로 확인한 뒤 제거한다. 개발자의 `EVENTGLASS_DATA_DIR`, 홈, repo data 디렉터리를 지우는 명령을 재사용하지 않는다.

disk full/ENOSPC는 제한 크기 테스트 파일시스템 또는 isolated quota에서 발생시킨다. 일반 CI host 디스크를 채우지 않는다. Linux resource tests는 cgroup memory.max=512 MiB/CPU 1개, 제한된 disk volume에서 실행하고 test generator는 서버 cgroup 밖에 둔다. Docker Desktop/macOS 결과를 동일한 Linux 512 MiB 증거로 대신하지 않는다.

핵심 stress: 큰 요청 연속, decompression bomb, node/depth 한계, query timeout 후 permit, slow SSE, hydration single-flight, pin/evict race, backup WAL hold, S3 unavailable, JSON 상세 대용량. timeout된 native 작업이 실제 끝나기 전 admission이 재개되지 않는지 측정한다.

## 8. 벤치마크와 release 기준

100K는 로컬 개발 smoke, 1M은 로컬 추세, 10M은 로컬 RC 전용 검증이다. GitHub-hosted runner는 성능 검증에 사용하지 않는다. 비교는 같은 고정 runner/image/dataset seed에서 한다.

원안 목표: 1 vCPU/512 MiB, 100 logs/sec+5 errors/sec, ACK p95 50ms, 선택적 warm filter 200ms/text 500ms/15분 histogram 500ms. 모두 미달성 상태에서 시작한다.

측정 절차: seed로 데이터 생성 → 사전 적재와 visibility 확인 → 5분 warmup → 30분 지속 수집+지정 query mix → 10분 backlog drain 관찰. 10M 적재 성능과 정상 steady-state 검색을 별도 기록한다. cold 시험은 Eventglass cache/page-cache 조건과 S3 network를 기록한다.

필수 보고: commit/lock/toolchain/native format/SDK 버전, CPU/disk/kernel/cgroup, dataset byte 분포·attribute cardinality·stack 크기·late 비율, shard/segment 수, query/time range/hit ratio/concurrency, p50/p95/p99, acceptance/index lag, Inbox bytes slope, RSS/cgroup peak/page cache, WAL/db/index 크기, upload/download bytes와 checkpoint lag.

OOM=실패, ACK 데이터 silent loss=실패, 무제한 Inbox 상승=실패. 자원 부족으로 명시적 429가 발생하면 정확성 성공과 throughput 목표 미달을 별도 기록한다. 지연 목표 미달을 테스트 숫자 수정만으로 통과시키지 않는다. 정책/목표 변경은 측정 보고와 ADR가 필요하다.

release 전 필수:

- 모든 v1 기능 P00–P12 완료 및 원안 DoD 매핑 증거.
- native format/SQLite migration 이전 release fixture 읽기; 첫 release는 0001 재오픈·incompatible reject fixture.
- 실제 AWS 또는 명시한 지원 S3 제품 검증, 전체 디스크 유실 복구, security/crash/stress.
- amd64/arm64 production image, embedded UI, non-root `/data` 재시작 smoke.
- SBOM/license notices/artifact SHA-256, 지원 SDK 표, 성능 보고, 알려진 한계/RPO/restore 인증 rollback 안내.
- 배포·태그·패키지 push는 별도 사용자 요청 또는 repository release 정책에 따라 실행. 설계/검증만으로 자동 공개하지 않는다.

## 9. PR 작성과 예외

한 PR은 하나의 검증 가능한 작업 묶음으로 한다. 기능을 안 만든 채 TODO endpoint/항상 성공하는 fake health를 합치지 않는다. feature가 도중이면 외부 UI에 완성된 기능처럼 노출하지 않는다.

PR template 필드: 문제와 변경 후 동작, 관련 P/G/T ID, 변경된 migration/native/API 계약, 실행 명령과 결과, 미실행 검증/이유, 복구·보안 영향, screenshot(UI일 때). 작은 수정에는 해당하는 항목만 간결히 적는다.

dependency update는 자동화할 수 있지만 lockfile/API spike/crash regression 통과 없이 auto-merge하지 않는다. lint 전역 disable, 검사 삭제, 0-test green, `|| true`, 원인 없는 ignore로 실패를 덮지 않는다. 외부 서비스 권한 부재는 코드 실패와 구별해 보고하되 통과로 계산하지 않는다.


## 2026-09-09 실행 환경 보완

사용자가 1코어·1GB 환경의 테스트를 요구했다. `./scripts/check resource`는 Linux 바이너리를 별도 빌드한 뒤, CPU quota 1·메모리 1GiB(1,073,741,824 bytes)·swap 0으로 실제 테스트 프로세스를 실행한다. cgroup 제한값, 각 target의 nonzero 실행 수, exit code, memory.peak, memory.events와 cpu.stat를 보고서에 남긴다. 호스트 macOS의 기존 기능 테스트는 이 자원 gate의 증거가 아니다.

빌드 단계는 2 CPU/4GiB이며 실행 테스트와 구분한다. 기존 원안의 512MiB·대규모 workload RC 목표를 이 첫 1GiB gate로 대체하지 않는다. 실제 통과 여부와 제외 범위는 실행 보고서 및 evidence를 따른다.

사용자의 최신 지시에 따라 남은 작업은 주 에이전트가 직접 수행하며 하위 에이전트를 사용하지 않는다.
