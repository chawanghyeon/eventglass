# Eventglass 구현 계약

상태: 구현 기준. 원안 §1–39의 v1 기능을 모두 유지한다. 아래 수치 중 원안에 없던 것은 이번 설계의 초기 정책이며, 성능 측정 결과와 함께 변경할 수 있다. 내구성·권한·정확성은 튜닝 대상이 아니다.

2026-09-12 최신 설계 범위: [별도 관리 없이 동작하는 비용 효율 설계](cost-efficiency.md)의 U01–U04를 따른다. 앞선 C01–C07/A01–A03 계획을 대체한다. 사용자의 비용 설정·승인 없이 자동 계측·읽기 재사용·안전한 사본 회수·검증된 압축 선택을 수행하는 것이 목표다. 데이터 의미·durable ACK·검색 정확성·복구는 유지한다. 현재는 설계 단계다.

## 1. 먼저 닫아야 할 설계 공백

| ID | 원안에서 구현 판단이 남은 부분 | 이번 결정 |
|---|---|---|
| D01 | Inbox chunk 일부를 처리하면 경계가 모호함 | chunk는 분할 적용 금지. 수신 시 충분히 작게 나누고 완전한 chunk들만 인덱싱 |
| D02 | 미리 계산한 Issue 변화가 resolve와 충돌 | Tantivy에는 불변 Record만 넣는다. Issue 상태·경보 결정은 SQLite 최종 확정 transaction의 현재 상태에서 계산 |
| D03 | 같은 batch를 복구할 때 코드·순서 차이 | Inbox의 버전·식별자·fingerprint 재사용. 순서 고정, fingerprint 재계산 금지 |
| D04 | received-time Live/Alerts가 event-time shard pruning에 걸림 | 공통 검색의 내부 TimeBasis를 구분하고 shard에 received-time min/max 추가 |
| D05 | 별도 요청의 histogram과 rows가 다른 W 사용 | 서명된 read token을 재사용. 같은 검색 화면에서 W/절대 시간 범위를 공유 |
| D06 | DB shard 통계와 reader가 서로 다른 세대 | W 캡처 후 catalog를 읽고, reader 세대의 공개 경계를 검증. publish는 reload 후 |
| D07 | 모든 bucket 보존은 API 이름만으로 증명 안 됨 | 초기 spike의 20,001 bucket·다중 segment 반례 테스트를 기능 선행조건으로 둠 |
| D08 | 20 MiB JSON을 64 MiB 메모리에서 처리 가능하다는 추정 | streaming decode/제한된 Record DOM과 전송 버퍼 소유권으로 설계하고 실제 메모리 gate 적용 |
| D09 | S3가 켜진 첫 설치와 복구 대상 부재 혼동 | 빈 prefix 확인 후 installation을 조건부 생성. 접근 실패/기존 설치의 checkpoint 부재는 자동 초기화 금지 |
| D10 | 존재하지 않는 검사 성공 처리 | P00은 명시적 bootstrap 단계. P01부터 manifest/test target 부재는 로컬 검사 오류 |

## 2. 구현 형태와 소유권

Cargo package 하나, library target와 `eventglass` binary target 하나. library는 테스트에서 같은 애플리케이션을 구동하기 위한 것이며 별도 서비스가 아니다. Frontend는 `web/`의 React/TypeScript/Vite 프로젝트, package manager는 npm 하나다.

```text
src/main.rs              CLI 입력, exit code, tracing 초기화
src/lib.rs               통합 테스트가 사용하는 공개 경계
src/app.rs               startup/recovery/task 종료 순서와 AppState
src/config.rs            env validation, 내부 Limits
src/http/                라우팅, 인증, DTO, error 응답
src/sentry/              envelope, store, normalize, scrub
src/db/                  DbWorker, command, query, migrations
src/indexer.rs           단일 Indexer와 batch finalization
src/issue.rs             fingerprint와 순수 lifecycle 계산
src/search/              schema, native query, sort, aggregate, tokens
src/storage/             shard registry/pin, seal, S3, checkpoint, restore, budget
src/alerts.rs            평가와 outbox 전송
migrations/              embed SQL
tests/contracts.rs       G01–G08 실행 진입점
tests/integration.rs     tests/integration/ 모듈 진입점
tests/crash.rs           tests/crash/ 모듈 진입점
tests/sdk.rs             오프라인 SDK fixture replay 진입점
tests/support/           임시 디렉터리, process harness, seeded fixtures
web/                     UI, Vitest, Playwright
scripts/                 동일한 로컬/CI 검사 진입점
```

`tests/integration/*.rs`만 만들어 Cargo가 자동 발견할 것이라고 가정하지 않는다. 각 root integration target가 하위 module을 선언해야 한다. 공용 테스트 helper가 독립 test binary로 발견되지 않게 한다.

| 소유자 | 소유하는 자원 | 다른 작업과의 계약 |
|---|---|---|
| DbWorker | 운영 SQLite write connection 1개 | bounded command channel, 짧은 transaction, 완료 oneshot |
| Indexer | active writer 1개, 진행 중 batch 1개 | 이전 finalize/reload 끝나야 다음 batch 시작 |
| Search | 캡처한 W, 제한된 shard pins/reader | blocking 작업 안에서 permit 소유 |
| Storage registry | shard별 pin·설치·eviction exclusion | 파일 존재 검사부터 사용권 취득까지 한 경계 |
| Backup task | 전용 source read connection, snapshot destination | B snapshot 고정 확인 후 indexing 재개 |
| Alert sender | 전송 중 delivery 1개 | 외부 I/O 후 DbWorker로 결과 기록 |

rusqlite connection을 Tokio worker에서 긴 동기 호출에 쓰지 않는다. DbWorker는 전용 thread, search/index/압축 작업은 한정된 blocking 경로다. connection을 전역 mutex로 감싸 모든 HTTP가 직접 SQL을 실행하지 않는다.

객체 저장소와 AlertSender는 실제 fault injection이 필요한 최소 경계만 추상화한다. Clock은 wall time/monotonic time을 분리한다. RepositoryInterface, 범용 executor/DSL/작업 큐 프레임워크를 만들지 않는다.

## 3. 버전·타입·스키마

Rust는 P00에서 사용 가능한 stable의 **숫자 버전**을 `rust-toolchain.toml`에 기록한다. 현재 1.97.1을 고정하고 Linux release container와 실제 ARM64 artifact에서 검증한다. Tantivy는 원안 기준 `=0.26.1`을 검증한다. 변경하면 G01–G08, native format 호환성과 ADR를 함께 갱신한다. 나머지 라이브러리도 실제 resolve 후 Cargo.lock을 commit한다. AWS SDK MSRV 때문에 임의 구버전 Rust를 먼저 고정하지 않는다.

Node/npm/Python/Go/SDK/브라우저 버전은 실제 사용 버전과 lockfile을 기록한다. `latest`, 미확인 SHA, placeholder를 출시 manifest에 남기지 않는다. 검증하지 않은 SDK 버전을 지금 임의로 호환 표에 넣지 않는다.

내부는 `IngestSeq(i64)`, `InboxId(i64)`, `TimestampUs(i64)`의 얇은 newtype을 쓴다. sequence는 양수, 초기 적용 watermark만 0 허용. 덧셈은 checked arithmetic이고 i64 고갈은 503 + unhealthy다. HTTP의 모든 i64 ID/sequence/count는 10진 문자열로 통일하고 프로젝트 ID 입력은 원안의 작은 JSON 정수도 하위 호환으로 받는다. TS에서 `Number(sequence)` 금지. 시간 연산은 정수 μs이며 native DateTime 범위도 검증한다.

Identity 규칙:

- source event ID는 유효한 16바이트 Sentry ID를 lowercase 32 hex로 canonicalize. 명시됐지만 malformed인 ID는 400.
- Error `event_key`와 `record_id`: SHA-256(domain + canonical project ID + source event ID)의 hex 문자열. 길이 prefix를 가진 바이트 encoding을 helper 하나로 고정한다.
- SDK ID 없는 Event/Log: 서버 UUID v4 acceptance ID + item ordinal + record ordinal을 같은 방식으로 encoding/hash. ordinal은 0부터, 원래 envelope 위치 기준이다.
- issue ID: project ID + fingerprint version + fingerprint hash. fingerprint canonical bytes는 JSON string array 직렬화로 고정하고 Unicode는 임의 NFKC 변환하지 않는다.
- UUID 생성/hash golden vectors를 fixture에 둔다. sequence/시간/메시지 hash로 서로 다른 로그를 dedupe하지 않는다.

원안 §10 DDL을 기반으로 최초 migration에 다음을 추가한다. 아직 배포 DB가 없으므로 개발 초기에는 0001에 합칠 수 있지만 최초 release 이후 적용된 migration을 수정하지 않는다.

| 테이블 | 보완 |
|---|---|
| runtime_state | authorization_epoch INTEGER, schema/설치 초기화 상태를 명시. next_ingest_seq 초기 1 |
| shards | min/max_received_at_us, first_record_received_at_us, native format 문자열/토크나이저 버전. 실제 format 표현은 G06에서 확정 |
| issues | first_seen_ingest_seq/last_seen_ingest_seq, revision. release tie-break에 사용 |
| alerts | revision, deleted_at_us, last_evaluation_error, 마지막 성공 evaluation window와 W, pending_evaluation_end_us/pending_cut_seq |
| alert_deliveries | sent_at_us, last_status_code. event/evaluation dedupe key와 payload는 immutable |
| schema_migrations | SQL checksum. 이미 적용한 migration의 내용 변경 감지 |
| settings | setup token hash/만료/사용됨, cursor HMAC secret 등 버전 있는 값 |

`issue_occurrences.record_id`는 UNIQUE로 보강한다. project/key 물리 삭제 대신 비활성화/revoke를 쓴다. alert 삭제는 soft delete하고 미전송 delivery는 sender가 보내지 않게 한다. in-flight 외부 요청 취소까지 보장하지 않는다. 이 기능용 최소 cancelled 상태를 delivery에 추가해 운영 UI에 남긴다.

## 4. 수신과 Inbox 계약

처리 순서: wire admission → 인증·project 상태 → bounded body/decompress → 전체 지원 item 검증 → Record별 normalize/scrub → canonical serialized Record 검증 → DbWorker atomic accept → ACK.

검증 중 앞부분만 저장하지 않는다. DB transaction은 정규화 완료 후 시작한다. 임시 파일로 원본 request를 spill하지 않는다. 요청 종료/취소가 commit과 경합하면 이미 commit된 수신은 보존하며 client disconnect를 rollback 명령으로 해석하지 않는다.

DbWorker는 accept transaction 안에서 key revoke/project 상태를 재검사한다. parsing 중 revoke되면 저장 전 거절한다. revoke가 accept commit 뒤 실행되면 기존 ACK 데이터는 정상 수신이다. admission의 auth snapshot을 commit 권한으로 영구 재사용하지 않는다.

Inbox chunk 상한은 **128 Record 또는 serialized payload 512 KiB**다. 512 KiB를 넘는 단일 Record는 단독 chunk로 허용하되 Record hard limit 1 MiB를 따른다. wrapper bytes도 총 요청 20 MiB 검증에 포함한다. chunk ordinal·first/last seq·count가 payload와 일치하는지 검사한다.

Indexer batch는 완전한 chunk들을 앞에서부터 선택하여 1,000 Record/4 MiB 이하로 구성한다. 다음 chunk가 넘으면 거기서 끊는다. 행 일부를 삭제하거나 `last_applied_inbox_id`로 부분 처리를 표현하지 않는다.

전체 요청의 chunks, next sequence 갱신은 하나의 SQLite transaction이다. group commit의 4 MiB는 flush 목표이며 hard request limit가 아니다. 4 MiB보다 큰 정상 요청은 단독 transaction으로 처리한다. P02는 요청별 transaction부터 구현해도 되며 group commit은 P12 측정 후 추가할 수 있다. ACK 의미는 동일해야 한다.

원안 body 20 MiB/정규화 합계 20 MiB/node 20,000/depth 64/Record 수 10,000을 그대로 적용한다. 노드 카운터는 배열·객체·scalar 모두 센다. 유효 지원 item 하나라도 실패하면 400/413이고 이번 요청의 Inbox/sequence 변경은 0이다. 미지원 item만 있는 envelope는 202이고 sequence를 할당하지 않는다.

64 MiB ingress 예산에서 wire+decompressed+DOM+normalized 전체 복사본을 동시에 유지하지 않는다. decoder는 Record 단위 traversal, bounded buffer, 이동 가능한 serialized payload를 사용한다. 원안 한도까지 정상 입력을 처리할 수 없으면 G08 실패로 기록하고 구현/예산을 조정한다. 임의로 20 MiB 지원을 4 MiB 지원으로 줄여 통과시키지 않는다.

Ingestion 기본 level canonicalization은 trace/debug/info/warning/error/fatal이고 실제 SDK enum 매핑은 fixture에서 고정한다. `warn` 등 SDK alias는 normalize에서만 처리하고 query 별칭 문법은 만들지 않는다.

Scrub 이후에 message/attributes/search_text/fingerprint/title를 계산하여 비밀값이 파생 필드에 남지 않게 한다. envelope/header/raw dump를 tracing에 보내지 않는다. known-key scrub 경로와 user configured keys는 하나의 함수다. header pair 배열, URL query, 중첩 object를 sentinel 검사에 포함한다.

## 5. Indexer 최종 확정과 동시 수정

경계 구조를 `Boundary { inbox_id, ingest_seq }` 하나로 통일한다. payload에 version, installation_id, shard_id, boundary를 저장한다. 두 값 모두 일치해야 한다. 새 active의 initial commit은 현재 A를 상속한다.

정상 batch:

1. `A < inbox.id`의 선두 완전한 chunks를 읽는다. 중간에 누락된 미처리 chunk가 없는지 payload/count를 검증한다.
2. Error event_key를 **한정된 SQLite batch query**로 lookup하고 batch 내부에서 순서대로 dedupe한다. 처음 나온 unique Record만 Tantivy에 추가한다.
3. immutable normalized Record만 native index에 추가한다. issue status/count 같은 mutable 필드는 index에 복제하지 않는다.
4. prepare → boundary payload → commit. commit 결과가 불확실하면 reopen하여 persisted boundary 확인 전 추가 쓰기 금지.
5. DbWorker `FinalizeBatch(expected_A, C, records)` transaction 실행.
6. reader manual reload 후 reader가 C까지 읽는지 확인하고 Release publish. 검색 캡처는 Acquire.

FinalizeBatch는 다음을 한 transaction에서 수행한다.

- 현재 A가 expected_A인지 검사. 이미 A=C면 idempotent no-op. 이외 mismatch는 실패.
- 새 issue는 동일 transaction 안에서 먼저 초기 row를 생성하여 occurrence의 FK를 만족시킨다. occurrence를 순서대로 insert하고 처음 삽입된 Error만 issue count/first/last/release를 갱신한다. 초기 row 생성 여부를 new-issue trigger로 사용하며 rollback 시 둘 다 사라진다.
- **transaction 시점의** status/resolved_through_ingest_seq/alert configuration을 읽어 regression과 delivery를 결정.
- issue 상태 변경, deterministic outbox insert, shard 통계, runtime boundary 갱신, 적용 Inbox delete.

Tantivy commit 전에 산출한 stale issue row를 통째로 UPDATE하지 않는다. Resolve도 같은 DbWorker에서 직렬화하며 `next_ingest_seq - 1`을 수신 경계로 저장한다. Indexer finalize 전에 resolve된 backlog는 regression이 아니다. 최초 신규 occurrence가 resolve 경계보다 클 때만 unresolved 전이·regression delivery를 만든다. ignored는 유지한다.

관리 상태 linearization point는 SQLite commit이다. native commit과 finalize 사이의 관리 요청이 먼저 commit되면 그 최신 관리 상태를 반영한다. 복구 시 새로운 wall clock이나 alert 조회 때문에 존재했던 delivery를 다시 생성하지 않도록 delivery identity를 trigger sequence 또는 evaluation window로 결정한다. 성공하지 않은 finalize transaction의 결정을 보존할 별도 WAL은 만들지 않는다.

first/last는 `(timestamp_us, ingest_seq)`의 min/max이며 title 등 대표 필드의 교체 규칙도 이 tuple에 맞춘다. release가 없는 최초/최후 Record는 해당 release=null이다. 이를 ‘가장 최근 non-null release’와 혼동하지 않는다.

`C>A` 복구는 Inbox의 A 이후 C까지 **전부** 읽어 동일 finalize 함수만 호출한다. add_document/commit을 다시 하지 않는다. 순서·count·boundary가 맞지 않으면 중단한다. 모든 Error가 중복이어도 empty native metadata commit과 A 전진이 필요하다.

Startup에서는 복구 전 auth/admin mutation을 포함한 정상 API를 열지 않는다. liveness/진단 상태만 제공한다. reader reload 실패 중 신규 batch 시작 금지; Indexer stalled와 ingest 가능 잔여 Inbox 예산을 각각 표시한다.

## 6. 검색·집계·토큰

`SearchScope { principal, projects, time_basis, start, end, W }`를 내부 공통 경계로 쓴다. Search/aggregate/related/live/alerts 모두 scope validation과 mandatory query 결합을 통과한다. SQL과 native query에는 bound parameters/typed values를 사용한다.

API의 일반 검색은 event timestamp, Live는 received timestamp, threshold alert는 기본 received timestamp다. catalog pruning도 같은 basis의 min/max를 쓴다. histogram field는 v1에서 timestamp로 제한하며 event-time Explore 계약을 따른다. 내부 Alert count에는 histogram이 필요 없다.

Tantivy 내장 QueryParser strict parse, 기본 AND, message/search_text를 default field로 쓴다. 라이브러리 AST 검사로 길이·절 수·depth를 제한한 뒤 native build를 사용한다. 별도 query 문자열 재작성이나 자체 AST는 없다. [공식 QueryParser API](https://docs.rs/tantivy/0.26.1/tantivy/query/struct.QueryParser.html).

G01은 dotted literal key와 nested key, JSON raw string/number 혼합, prefix, DateTime μs 범위를 **정확히 고정 버전에서** 검사한다. UI 필드 패널이 생성할 escaping 예시는 G01 결과에 넣는다. 확인 전 대략적인 escape를 구현 지침에 넣지 않는다.

검색 snapshot 순서:

1. auth/project 상태와 authorization_epoch를 캡처하고 W를 읽는다.
2. 그 뒤 SQLite read snapshot에서 candidate catalog를 조회한다. `ingest_seq<=W` 필터는 항상 native query에 포함한다.
3. 후보 shard마다 registry에서 pin을 취득하고 reader generation이 필요한 공개 경계를 포함하는지 검사한다.
4. auth epoch가 중간에 바뀌면 결과를 반환하지 않고 권한 재검사 후 오류/재시작한다.

원격 hydration을 포함해 모든 후보를 검색해야 성공이다. catalog 이후 들어온 W보다 큰 데이터는 무관하며 W 이전 shard의 물리 purge는 v1에 없다. reader 자동 reload는 사용하지 않는다.

Rows는 `(timestamp DESC, seq DESC)` keyset이며 shard마다 `limit+1`을 받아 global bounded top `limit+1`로 merge한다. doc address는 내부 일시 참조이고 cursor에 저장하지 않는다. stored detail은 최종 선택 row에 한해 읽고 raw를 목록에서 제외한다.

토큰은 JSON canonical DTO + HMAC-SHA256, base64url, constant-time 검증. version/storage_generation/auth epoch/권한 hash/request hash/W/issued_at/expires_at을 포함한다. row cursor에는 마지막 tuple, detail token에는 project/shard/record identity, Live token에는 scan sequence를 추가한다. token type domain을 분리해 상호 대체를 막는다.

- cursor/read token TTL: 15분. detail token TTL: 1시간. Live resume token TTL: 15분. 내부 초기 정책.
- 최초 rows response에 `read_token` 추가. histogram request는 같은 absolute start/end와 token을 전달해 W를 공유한다.
- request hash는 query 원문, 정렬, 중복 제거·정렬한 fixed filters, 프로젝트, absolute time을 canonical serialize한다. query 원문을 의미 정규화하지 않는다.
- read token은 aggregation op를 고정하지 않아 같은 데이터 snapshot으로 histogram을 그릴 수 있다. row cursor는 row limit/sort까지 고정한다.
- token 만료는 410, 변조/요청 불일치는 400, 현재 권한 위반은 403. 복구 generation 불일치는 409.

집계는 고정 DTO의 metrics 최대 8개, group_by 최대 2개, histogram 최대 1개로 시작한다. service/level/environment/release group 지원. 모든 중간·최종 nested bucket을 합한 한도 20,000, request native 메모리 예산 16 MiB부터 시작한다.

Top-K는 final merge 후 적용한다. G03은 모든 segment와 shard에서 local winner가 다른 반례를 포함한다. native collector가 후보를 잘라낸 경우 count error=0처럼 보여도 성공 처리하지 않도록 source/API/fixture로 입증한다. 불가능하면 blocker ADR를 작성하고 라이브러리 설정/버전 대안을 검증한다. 자체 bucket 엔진으로 대체하지 않는다.

count/bucket membership은 exact, sum/avg 비교 허용 오차는 `max(1e-9, abs(expected)*1e-9)`로 시작한다. 비유한 sum/avg는 422 `numeric_overflow`; 값 없는 metric=null. 빈 데이터 count는 "0". boolean/string number는 numeric metric에서 제외하고 `excluded_non_numeric_values` 또는 검증 가능한 경고를 제공한다. 해당 제외 수를 native API로 정확히 산출할 수 있는지는 G03에서 확인하고 미확인 값을 0으로 표시하지 않는다.

명시적으로 존재하지 않는 정적 필드는 400. 구 schema에서 unavailable인 필드는 request 전체에서 명시적인 `field_unavailable` 오류로 처리하여 조용한 shard별 누락을 피한다. v1 native format 자동 변환은 없다.

## 7. Live와 Alerts의 순서

Live catch-up 정렬은 일반 rows와 달리 **ingest_seq ASC**다. 같은 native search 공통 경계의 제한된 sort enum에만 추가하며 별도 evaluator를 만들지 않는다.

연결은 broadcast subscribe → W 캡처 → `(resume_seq,W]`를 limit 단위로 scan → 이후 notification마다 새 W까지 scan한다. notification은 payload가 아니라 재조회 신호다. 기본 최초 catch-up은 received-time 최근 15분; 기존 resume token은 고정된 scope와 seq를 사용한다. 지나간 event timestamp 때문에 live record를 제외하지 않는다.

SSE id는 마지막 **전달 또는 scan 완료 경계**의 서명 토큰이다. 필터 일치 0건이어도 checkpoint event로 scan W를 알려 반복 전체 scan을 방지한다. record event의 id는 그 record seq라서 batch 도중 disconnect해도 남은 결과가 누락되지 않는다. 마지막 batch를 전송한 뒤에만 scan W checkpoint를 보낸다.

연결당 buffer 256 KiB, 최대 32 connections, heartbeat 15초, catch-up 한 번 최대 10,000 record 또는 10초로 시작한다. 한도를 넘으면 `resync_required` 후 닫는다. auth/project 비활성화는 heartbeat 또는 batch마다 재확인하고 닫는다. client record_id dedupe는 보조 UX이며 서버 누락을 숨기지 않는다.

Alert threshold window는 60초 aligned evaluation end E와 설정된 window로 `[E-window,E)`를 쓴다. E를 평가 대상으로 예약할 때 DbWorker가 현재 최대 수신 sequence S를 pending_cut_seq로 함께 저장한다. W>=S까지 기다린 뒤 query를 실행하여 이미 ACK한 backlog를 아직 공개되지 않았다는 이유로 0건 처리하지 않는다. 기다리는 동안 다음 E로 건너뛰지 않고 evaluation lag를 표시한다.

성공 후 다음 E는 이전 E+60초이며, 뒤처진 평가는 제한된 실행 예산으로 순서대로 따라잡는다. 별도 무한 queue를 만들지 않고 alert row의 마지막 완료/현재 pending 한 개로 표현한다. 새 alert 활성화 시 첫 E는 다음 분 경계이고 생성 전 모든 과거 window를 평가하지 않는다. 기본 received-time에서는 이미 닫힌 E 이후 수신을 해당 window에 끼워 넣지 않는다. 서버 시계 역전은 진단 경고 대상으로 두며 과거 완료 window 재평가를 자동 보장하지 않는다. 선택적 event-time mode는 E 평가 이후 도착한 과거 event까지 소급 알림하지 않는다는 한계를 표시한다.

동일 `(alert_id, revision, E)` 평가가 이미 성공했으면 no-op. 성공한 query 결과와 alert revision 재확인, cooldown, delivery insert, last successful E 갱신, pending 해제를 한 transaction에 저장한다. 불완전 조회는 last successful E를 전진시키지 않는다. crash 후 재시도는 같은 E/S를 사용한다. 편집된 revision의 결과는 폐기하고 새 revision의 시작 E를 다시 정한다. cooldown은 threshold의 evaluation end E를 기준으로 판단하여 catch-up 실행 속도에 따라 결과가 달라지지 않게 한다.

전송은 timeout 10초, response body 64 KiB, redirect 비활성. 2xx 성공; 네트워크/408/429/5xx 재시도, 다른 4xx는 failed. backoff는 5초부터 최대 1시간, jitter, 최대 12회. Retry-After는 유효한 경우 최대 1시간 clamp. manual retry는 같은 delivery ID로 수행한다.

운영 경보 발송은 사용자 생성/활성화한 설정에만 따른다. 테스트는 로컬 수신기만 쓴다. private 목적지 차단 및 opt-in allowlist, DNS 결과 검증과 실제 접속 IP 일치를 검증한다. SMTP는 선택 기능이며 webhook 완료를 지연시키지 않는다.

## 8. Shard·백업·복구

원안 seal/고정 경로/manifest 절차를 유지한다. Indexer task만 active를 바꾼다. maintenance는 seal 요청을 보내며 직접 writer를 교체하지 않는다. 첫 unique indexed Record의 수신 시각을 seal 시간 기준으로 저장한다. duplicate-only batch로 빈 shard를 한 시간마다 만들지 않는다.

| 발견 상태 | 재시작 처리 |
|---|---|
| active row + 유효 sealed manifest | native boundary=A 검증 후 row를 local로 확정 |
| active 없음 + 마지막 sealed boundary=A | 새 active 생성, initial payload=A |
| 미등록 empty index + initial payload=A | installation/shard identity 검증 후 채택 가능 |
| 미등록 non-empty index | 자동 삭제/채택 금지, doctor 보고 |
| C>A + 해당 Inbox 존재 | SQLite finalize만 수행 |
| C<A / payload 손실 / identity 불일치 | startup 실패, 진단 정보 보존 |
| remote_verified 파일 없음 | 완료 checkpoint 증거 확인 후 remote_only로 정리 |
| local/active 파일 없음 | 복구 가능한 증거 없으면 503, 빈 index 생성 금지 |

native manifest 생성은 writer drop/merge 종료 뒤 실제 참조 파일을 열거한다. 디렉터리의 임시파일 전부를 archive에 넣지 않는다. file checksum/크기/fsync와 디렉터리 fsync를 구분한다. failpoint는 rename 직전/직후와 DB state 변경 사이마다 둔다.

Backup은 seal B 직후 source read connection의 `BEGIN + SELECT` 완료를 handshake로 확인한 뒤 새 active를 만든다. pinned source에서 다른 작업 금지. Backup destination에는 transaction을 미리 열지 않는다. step이 DONE인지 확인해야 성공이며 finish 성공만으로 완료 판단 금지. [SQLite Backup 계약](https://sqlite.org/c3ref/backup_finish.html).

초기 backup deadline은 120초, WAL 증가 예산은 시작 이후 128 MiB와 남은 disk reserve 중 작은 값이다. 초과하면 incomplete candidate 폐기, read transaction 해제. 512 MiB RAM 환경에서 snapshot 전체를 읽지 않고 stream compression한다. 이 수치는 큰 SQLite에서 checkpoint 지연을 만들 수 있으므로 P12에서 성공 가능한 dataset 크기와 함께 평가한다.

checkpoint는 cut B뿐 아니라 DB snapshot의 SHA-256/size, catalog의 정확한 shard 집합, installation ID, format 버전, 생성 순서와 시간을 담는다. latest는 최적화 포인터다. 복구 시 fallback listing은 paginated, 같은 installation의 완료 checkpoint만 검사한다. 최신 후보가 손상돼 직전 정상 후보로 복구하면 복구 시점 후퇴를 명시한다. 후보를 섞지 않는다.

첫 S3 설치는 prefix가 비어 있다는 검증 후 installation.json을 conditional create한다. 권한 오류/일시 장애를 empty로 해석하지 않는다. 기존 installation만 있고 checkpoint가 없으면 자동 fresh setup을 하지 않는다. 관리자가 새 설치 의도를 명시할 수 있는 별도 초기화 절차를 운영 문서에 둔다.

Restore는 staging DB 검증 → snapshot/catalog 대조 → old DB/WAL/SHM quarantine → fsync된 DB 설치 → generation/token secret 회전·sessions 삭제 → active=B 생성이다. restore 완료 마커/설치 순서의 crash fixture를 둔다. 정상 로컬 DB가 있으면 remote 복구로 덮지 않는다.

Hydration single-flight가 waiter 0이 되면 취소 가능하다. 설치 완료 시 registry exclusion을 유지한 채 waiter pin으로 넘겨 eviction이 중간에 끼지 못하게 한다. tar entry는 정규화 상대경로의 regular file/directory만 허용; symlink/hardlink/device/중복 path/manifest 외 파일/총 해제 한도 초과 거절.

Remote_verified는 object upload 성공만으로 설정하지 않는다. 완료 checkpoint가 참조하고 복구 검증된 shard만 회수 가능하다. eviction은 pin 0 확인과 삭제 권한 획득이 atomic이어야 한다. archive 삭제/프로젝트 physical purge는 v1 범위 밖이다.

## 9. API·관리 보안과 화면

원안 §29 route를 유지하고 user 관리(`/api/users`), occurrence detail(`/api/issues/{id}/events/{record_id}`), delivery 목록/재시도 route를 추가한다. schema는 P01에서 OpenAPI 3.1로 기록하고 Rust request/response와 contract test로 비교한다. Rust DTO와 다른 수동 TS 타입을 유지하지 말고 고정 codegen 도구로 `web/src/api/generated.ts`를 생성한다.

공통 오류 envelope:

```json
{"error":{"code":"query_timeout","message":"검색 시간이 초과되었습니다.","request_id":"...","retryable":true}}
```

| HTTP | 의미 |
|---|---|
| 400 | protocol/query/DTO/token 형식 오류 |
| 401/403 | session/key 없음·무효 / 현재 scope 접근 불가 |
| 404 | 존재하지 않는 권한 내 자원 |
| 409/410 | revision/generation 충돌 / token 만료 |
| 413 | body/Record hard limit |
| 422 | bucket limit, numeric overflow, 지원하지 않는 metric 의미 |
| 429 | admission/resource queue 포화; Retry-After |
| 503 | DB/index/shard unavailable, 복구 미완료 |
| 504 | 실제 query deadline 초과 |

validation 오류에 raw input/secret/path를 echo하지 않는다. Search 실패에 items=[]/count=0 성공 응답을 보내지 않는다. API source DTO는 unknown fields 거절을 기본으로 하고 protocol parser의 forward compatibility와 구분한다.

관리 역할: admin은 사용자/프로젝트/경보/운영 변경, member는 활성 프로젝트 조회 및 Issue resolve/ignore만 가능. 사용자 비활성화/role 변경은 sessions 무효화, auth epoch 증가. 마지막 활성 admin 비활성화/강등을 막는다. Issue PATCH는 revision으로 concurrent edit의 lost update를 409 처리한다.

Setup token은 CSPRNG 32바이트, 30분 만료, hash만 DB 저장. 로컬 `eventglass admin setup-token` CLI로 발급/교체하고 stdout에 한 번만 출력한다. 로그에 token을 쓰지 않는다. 첫 관리자 생성은 단일 transaction 조건부 처리. CLI password reset은 TTY 입력을 사용하며 command-line password 인자를 두지 않는다.

Session TTL 7일, 절대 만료, 로그아웃 즉시 hash 삭제. CSRF token과 Origin 검증을 cookie mutation에 적용. Argon2id는 검증된 crate로 구현하고 동시 hash 1개, 메모리 32 MiB부터 측정한다. HTTP bootstrap local 개발 외에는 Secure cookie를 사용한다. reverse proxy 헤더를 무조건 신뢰하지 않는다.

Ingest 전용 CORS는 공개 DSN 수신에만 적용하고 credentials=false. admin API same-origin. 브라우저 UI raw/stack/message는 text로 렌더링. 정적 assets는 fingerprint immutable cache, index.html은 no-cache; `/api` 오류에 SPA fallback 금지.

필수 UI는 원안 §28 전부이며 각 화면에 loading/empty/error/permission/timeout 상태를 둔다. Logs URL에는 query·absolute time·filters를 보존하되 secret/detail token을 공유 URL에 넣지 않는다. Dashboard의 rows/histogram은 read token을 공유한다. field panel은 sampled 표시, Live는 received-time 의미, storage는 archive 완료/복구 가능 여부를 분리한다.

키보드 검색/필터/상세 닫기, 폼 label, focus restore를 검증한다. row virtualization을 사용하더라도 복사·접근성 동작을 확인한다. UI snapshot만으로 제품 검증을 대신하지 않는다.

## 10. 자원·운영·빌드

Limits concrete struct 하나에서 ingress 64 MiB, writer 32 MiB, aggregation 16 MiB, query 동시성 1, hydration 1, upload 1, hash 1로 시작한다. native library 최소 writer 예산과 실제 증폭은 G08에서 확인한다. 모든 task가 각각 최대를 가져도 된다는 뜻이 아니며 shared budget admission으로 조율한다.

queue는 개수+bytes 양쪽 제한. DB command queue의 ingest payload 소유권은 ACK 대기까지 budget permit과 함께 이동한다. timeout/cancellation/drop 경로에 같은 RAII guard를 사용한다. query permit은 spawn_blocking closure 안으로 이동하며 HTTP timeout에서 조기 반환하지 않는다.

query 한 개가 많은 shard를 처리해도 handle cache 16개, 후보 결과 top K만 유지한다. DB read connections 기본 2개 + backup 전용 1개. connection별 page cache 예산을 명시하고 총량으로 계산한다. idle cache/allocator/mmap/cgroup page cache까지 측정한다.

`/readyz`는 안전한 core 경로 준비 상태다. startup recovery/DB 불능/reader 경계 불능은 503. disk pressure로 ingest만 429이고 query가 안전하면 ready와 system의 ingest_accepting을 구별한다. S3 단독 실패로 정상 로컬 서버를 종료하지 않는다.

Shutdown 최대 30초: 신규 수신 차단 → in-flight accept 결과 확정 → 현재 index batch/finalize/reload → 가능한 seal/backup 시도 → task/connection 종료. 남은 Inbox는 durable하므로 전체 drain을 종료 조건으로 강제하지 않는다. 제한 초과의 incomplete S3 backup을 완료 처리하지 않는다.

빌드 순서는 `npm ci → typecheck/test → npm run build → cargo build --release --locked`다. `build.rs`는 외부 네트워크/npm 설치를 실행하지 않는다. `embed-ui` feature의 빌드는 dist가 없으면 실패; test용 backend build만 embedded UI를 생략할 수 있다. release는 반드시 embed-ui로 빌드하며 failpoints feature가 없어야 한다.

Docker는 multi-stage build, numeric non-root UID, CA certificates, `/data` volume, SIGTERM 전달. Linux amd64/arm64를 release target로 검증한다. node/python/컴파일러는 runtime image에 넣지 않는다. CI test image의 기능을 production image에 섞지 않는다.

## 11. 명시적으로 미검증인 사항

이 문서 작성 중 QueryParser와 SQLite Backup 공식 계약은 확인했다. Tantivy sort/aggregation 개별 문서는 조회 실패가 있어 API 세부 동작을 확인했다고 주장하지 않는다. 원안의 `0.26.1` 고정 버전에서 **컴파일 가능한 G01–G08**로 검증한다.

SDK 버전 행렬, typed JSON mixed type 집계, 정확 bucket collector, tuple sort, backup read snapshot, fsync/crash, AWS checksum, 512 MiB 성능은 모두 구현 단계 증거가 필요하다. 미검증 사항을 숨기지 않는 것이 인수인계 완료 조건이며, 제품 완료 조건은 해당 gate의 실제 통과다.
