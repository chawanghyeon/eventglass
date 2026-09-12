# 작업 순서와 요구 추적

P00–P06 구현·검증이 진행 중이며 P07 이후는 선행 gate에 따라 작업한다. 이 문서는 단계별 완료 계약이다. 실제 진행 상태와 실행 결과는 Git 커밋·CI 결과 및 자원 검사 JSON를 따른다. 일부 테스트 통과를 단계 전체 완료로 해석하지 않는다.

## 1. P01에서 검증할 라이브러리 gate

| ID | 실행할 작은 실험 | 통과 조건 | 실패 시 |
|---|---|---|---|
| G01 | native parser/JSON schema | prefix/phrase/Boolean/range, dotted literal vs nested, mixed numeric/string, Unicode, timestamp μs golden fixture | QueryParser 설정/버전 근거 조사. 자체 DSL 금지 |
| G02 | commit payload/empty commit/reopen | non-empty/duplicate-only/새 active commit payload 보존, uncertain reopen으로 C 확인 | indexer 구현 착수 보류 |
| G03 | distributed native aggregation | 여러 segment/shard global winner, 20,000/20,001 bucket, weighted avg/mixed type/다중 값/memory limit | 정확 집계 구현 보류. 반례와 대안 ADR |
| G04 | native sort | μs 같은/다른 timestamp, seq tie-break, shard top K merge와 keyset 결과가 baseline과 동일 | 지원 native collector/sort API 검증 |
| G05 | rusqlite pinned backup | B read snapshot 고정 → 다른 connection B+1 쓰기 → backup 결과 B, pending Inbox 보존, 취소 후 WAL 해제 | checkpoint 구현 착수 보류 |
| G06 | seal/native file format | merge 종료 후 immutable 파일 집합/manifest 생성, reopen/native version 확인 | archive 구현 보류 |
| G07 | 실제 Sentry SDK 전송 | 우선 Python/Node 고정 버전 error/message/log를 수신기에서 캡처, protocol/response fixture 고정 | 지원표에 검증 안 된 SDK를 넣지 않음 |
| G08 | memory/admission 실험 | 최대 유효 입력의 bounded decode, writer 최소 예산, 취소 시 permit 수명, 실제 Linux 메모리 측정 | 예산/구현 재설계; 512 MiB 통과 주장 금지 |

G01–G06은 자동 Rust contracts target로 남긴다. G07의 캡처 결과는 SDK replay fixture로 들어간다. G08은 P01에서 allocation/ownership 실험, P12에서 실제 cgroup gate로 완료한다. 공식 API 문서를 읽은 것만으로 PASS로 표시하지 않는다.

## 2. 구현 묶음

### P00 — 저장소와 검사 기반

선행: 이 인수인계 문서 전체.

산출물: rust-toolchain.toml, Cargo.toml/Cargo.lock, 최소 src/lib.rs/main.rs, scripts/check/bootstrap, .pre-commit-config.yaml, 도구 lock, 로컬 검사와 배포 전용 .github/workflows/deploy.yml, PR template, CONTRIBUTING.md, .gitignore 보완. GitHub Actions에서는 test/build를 반복하지 않는다.

검증: clean checkout에서 bootstrap → hooks → fmt/check/clippy/test. script/tool 누락을 의도적으로 만들어 비정상 exit 확인. workflow 문법과 git hook 설치 결과를 확인한다.

완료: 새 개발자가 문서 명령으로 같은 도구 버전과 최소 바이너리를 재현한다. 실제 설치하지 못한 외부 repository 설정은 미적용으로 표시.

### P01 — 핵심 API 실험과 데이터/API 계약

선행: P00.

산출물: contracts target의 G01–G06, G07 최소 캡처, G08 초기 실험, 0001 migration, Record/Boundary DTO, schemas/openapi.yaml, version manifest, ADR(필요한 경우).

검증: 모든 작은 실험의 exact command/output와 사용 API를 evidence에 기록. metadata commit/typed JSON/정확 집계를 mock으로 대체하지 않음. 신·구/손상/더 높은 migration version reject.

완료: 각 gate PASS 또는 구체 blocker. block된 gate에 의존하는 기능은 구현을 밀어붙이지 않고 독립 작업은 진행.

### P02 — 실행·인증·프로젝트와 최소 UI

선행: P01의 DB/API 계약. S3 gate와 독립.

산출물: config/data-dir OS lock, DbWorker, migrations, setup token CLI, users/session/CSRF, admin/member 권한, project/key/revoke, health/status, web setup/login/projects, API codegen, embedded build. web/integration/release-smoke CI 활성화.

검증: 동시 setup 한 번만 성공, public DSN key로 query 실패, key revoke race, session logout/role 변경, 두 process lock, wrong schema fail closed. local-only login/DSN 발급 실제 브라우저 흐름.

완료: 외부 서비스 없이 관리자 생성→로그인→DSN 발급. 관리 입력에서 XSS/CSRF/권한 테스트 통과.

### P03 — Sentry durable ingestion

선행: P02 + G07 최소 fixture.

산출물: envelope/store, 압축·길이 검증, normalize/scrub, stable IDs, chunk 생성, atomic accept/ACK, admission/backpressure, offline sdk target.

검증: 지원/미지원 mixed items, malformed 지원 item 전체 rollback, 마지막 newline/UTF8/binary length, body/node/depth limits, sentinel persistence scan, restart 후 ACK Inbox 잔존. parsing 도중 revoke는 commit 재검사.

완료: 지원 데이터의 sanitized durable ACK. 이 단계에서 searchable 완료라고 하지 않는다.

### P04 — Indexer와 복구

선행: P03 + G02/G06.

산출물: 단일 writer, complete chunk batch, Error dedupe, finalize transaction, manual reload/W publish, restart reconciliation, compile-time failpoints, PR crash-smoke.

검증: C>A finalize-only, C=A resume, C<A fail, duplicate-only payload, mid-transaction rollback, same Error batch/cross-batch dedupe, no-ID Log acceptance 구분. A와 C의 seq/id pair 검사.

완료: ACK ledger의 지원 Record가 재시작 후 누락되지 않고 같은 Inbox 재처리 검색/occurrence/outbox 중복 0.

### P05 — Issue 기능과 상세 UI

선행: P04.

산출물: fingerprint golden cases, lifecycle/revision, issue listing/occurrence/detail, stacktrace/breadcrumb/raw, resolve/ignore, 관련 Error count, E2E/container smoke CI.

검증: late timestamp min/max/release, in-app frame fallback, SDK default fingerprint expansion, resolve와 finalize 동시 실행, ignored 유지, first new seq regression 한 번. hostile text XSS fixture.

완료: SDK error→Issue→상세→resolve→새 occurrence regression의 실제 UI E2E.

### P06 — Logs 검색과 cursor

선행: P04 + G01/G04.

산출물: native schema/query builder, time/auth/W scope, typed filters, keyset/read/detail tokens, bounded global merge, Logs UI/JSON detail/field panel.

검증: query grammar golden, 같은 timestamp 다수, 첫 page 뒤 늦은 과거 Record 삽입, query OR 권한 우회, 변조 token, project 비활성화, row raw payload 미포함, 0건과 unavailable 구분.

완료: 모든 페이지를 합한 identity/order가 고정 W baseline과 같음. query/attribute 정규식 치환 parser 없음.

### P07 — 집계와 Explore/Dashboard

선행: P06 + G03.

산출물: native intermediate merge, metrics/group/histogram DTO, exact bucket/memory limits, shared read token, Explore/Dashboard/Logs histogram.

검증: local Top-K 밖 global winner, multi-segment bucket adversary, 20,001 오류, avg weighted, empty/null/mixed type, histogram [start,end)·UTC, 누락 shard 오류.

완료: count/membership exact, float 명시 tolerance, rows/histogram 동일 W. 구현 편의를 위한 sampling 없음.

### P08 — Seal과 로컬 공간 관리

선행: P04 + G06.

산출물: batch 경계 rollover, immutable manifest, registry pin/handle LRU/reservations, received-time catalog bounds, seal/active recovery, local storage CI.

검증: 첫 Record 뒤 timer, 빈/duplicate-only shard 무한 생성 방지, 모든 seal crash point, old timestamp in new shard, pin/evict 경합, ENOSPC와 local-only 429.

완료: active 하나, manifest 이후 파일 불변, 복구 불가 로컬 사본 자동 삭제 0.

### P09 — S3 archive/checkpoint/cold restore

선행: P08 + G05.

산출물: AWS SDK object store, checksum/multipart, consistent candidate B, checkpoint/latest, first install/restore/quarantine, safe tar hydrate/single-flight, storage UI, compatible CI.

검증: 원안 §33.4의 B 이후 수집 지속→전체 임시 disk 삭제→B 복구; pending Inbox 한 번 적용; B 이후 미참조 archive 미혼합; latest 손상 fallback; corrupt/missing object; path escape; cold download 취소; generation/session 무효화.

완료: Issue count/status·projects/keys·query·cold detail까지 대조. AWS 미검증은 호환 범위에 분명히 표시하고 P12 RC에서 해소.

### P10 — Live·correlation·Alerts·운영

선행: P05/P06/P07/P08. cold 동작은 P09와 통합.

산출물: seq ASC SSE catch-up/resume, received-time query scope, related logs 방식 표시, alert CRUD/평가/outbox/backoff, storage/health/doctor UI 및 CLI.

검증: subscribe/W 경합, 0-match scan checkpoint, disconnect/lag/slow client/resync, revoke 연결 종료, received-time pruning, query 평가 incomplete, revision/cooldown race, webhook send 후 crash stable ID 재전송, SSRF 우회.

완료: Live 누락 0(지원 resume 범위), 경보 at-least-once 한계/실패 표시, status가 archive와 recovery를 구분.

### P11 — 지원 SDK 행렬과 보안/호환성

선행: P03–P10.

산출물: FastAPI/Celery/Browser/Node/Go 실제 SDK app, 고정 버전/lock/fixture 생성기, 로컬 확장 검사, auth/privacy/archive/security regression, upgrade fixtures.

검증: 로그 레벨·DEBUG 기본/opt-in·ERROR Event+Log·flush/fork·CORS·Unicode; DB/WAL/snapshot/native store/log scrub; production failpoints 제거.

완료: 지원표에 PASS/unsupported/미검증 명확히 구분. 미검증 기능을 지원 완료로 광고하지 않음.

### P12 — 자원·성능·패키징과 RC

선행: P00–P11 + G08 실제 Linux 자원 검증.

산출물: 100K/1M/10M seeded benchmark, 512 MiB stress, full crash, 실제 AWS 또는 명시 supported S3 검증, multiarch release artifact, SBOM/notice, 운영/백업/복구/upgrade 문서.

검증: quality.md §7–8 전체. same-filesystem restart/full-loss restore, image `/data` persistence, graceful termination deadline, offline local-only 실행. 실제 배포/공개는 별도 지시 범위.

완료: 원안 §36 DoD 모두 evidence 연결. 목표 미달은 숫자/조건과 보완 계획을 명시하고 ‘제품 완성’으로 덮지 않음.

## 3. 요구사항→테스트→필수 실행 위치

| Test ID | 원안 요구 | 구현 | 필수 증거/실행 |
|---|---|---|---|
| T01 | §5/8 SDK protocol/atomic ACK | P03/P11 | sdk replay, actual SDK local extended/RC |
| T02 | §6/7 identity/privacy | P03 | golden IDs + DB/WAL/store/log sentinel PR |
| T03 | §9–11 durability/dedupe | P04 | file-backed recovery + full SIGKILL local extended |
| T04 | §12 grouping/lifecycle | P05 | resolve/backlog/concurrent finalize PR |
| T05 | §13–17 native search/auth/paging | P06 | single dataset oracle + W pagination PR |
| T06 | §18 exact aggregation | P07 | global winner + 20,001 bucket PR |
| T07 | §19 Live/correlation | P10 | disconnect/late/lag/scope E2E PR |
| T08 | §20/21/24 seal/startup | P08 | manifest/active fault matrix local extended |
| T09 | §22–24 checkpoint/full loss | P09 | local+compatible PR, actual S3 RC |
| T10 | §25/26 disk/pin/timeout | P08/P12 | race unit, isolated quota/cgroup local extended |
| T11 | §27 alert/outbox | P10 | send-crash/retry/revision/SSRF local extended |
| T12 | §28/29 UI/auth | P02/P05/P10 | admin/member E2E, XSS/CSRF PR |
| T13 | §30/31 operation/format | P02/P12 | doctor nonmutation/upgrade/release image RC |
| T14 | §34–36 low resource/DoD | P12 | 100K/1M/10M versioned report RC |

원안 §1–4/32/39는 전 단계 구조 제약, §33은 위 테스트 계약, §37–38은 출처/변경 이력이다. README/문서만으로 T01–T14 완료를 표시하지 않는다.

## 4. 단계 완료 보고 형식

```text
단계: Pxx / 관련 Gxx, Txx
상태: PASS | BLOCKED | IN_PROGRESS
commit과 실행 환경:
변경 파일과 실제 사용자 동작:
실행 명령 / 종료 코드 / 테스트 수 / 소요 시간:
검증한 불변조건:
산출물 위치:
미실행 검증과 구체 이유:
알려진 한계:
다음 단계와 선행조건:
```

원래 필요 없는 기능을 늘리거나 v1 기능을 빼려면 범위 변경을 명시한다. 일반적인 파일 분할/구체 함수명/fixture 배치에는 추가 허가가 필요 없다. 불변조건을 만족할 수 없는 라이브러리 제약을 발견하면 실패 재현과 대안을 먼저 남긴다.

## 5. 별도 관리 없이 동작하는 비용 효율 설계

2026-09-12 최신 사용자 기준은 기존 코드 고정이 아니라 사람의 설정·승인·튜닝 없이 알아서 동작하는 것이다. 앞선 C01–C07/A01–A03 계획을 [자동 비용 효율 설계](cost-efficiency.md)의 U01–U04로 대체한다. 데이터 신뢰성과 기존 P00–P12 gate는 유지하며 구체 완료 기준은 해당 문서 §8을 따른다. 2026-09-12 후속 사용자 승인에 따라 구현·100% 커버리지 검증을 진행한다.
