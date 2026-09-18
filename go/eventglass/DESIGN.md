# Eventglass Go — Sentry 호환 S3 중심 로그·오류 분석기 구현 명세

작성일: 2026-09-18. 상태: **구현할 설계**, 제품 구현·성능 검증 완료가 아님.

이 문서는 새 에이전트의 단독 구현 기준이다. 대화 이력이나 Rust의 암묵적 동작에 의존하지 않는다. MUST/금지는 정확성 계약, 초기값은 계측 후 변경 가능한 정책이다. 미검증 라이브러리 동작은 §22의 선행 게이트로 닫고, 실제 한계가 발견되면 근거 없이 계약을 약화하지 않는다.

## 0. 결정과 적용 범위

### 0.1 확정한 결정

- 우리가 제품 및 분산 조정 계층을 직접 구현한다. Quickwit/OpenObserve/ClickHouse 제품을 뒤에 붙이지 않는다.
- Go 제품 코드 + DuckDB 내장 실행 엔진 + immutable Parquet/Zstd + S3 호환 객체 저장소 + PostgreSQL을 사용한다.
- DuckDB는 분석 실행기다. 트랜잭션 메타데이터 DB, 분산 scheduler, 공유 `.duckdb` 파일로 사용하지 않는다.
- 기본 검색은 구조화 조건·문자열·정규식·정확한 집계다. 전문검색 역색인/FTS, BM25, fuzzy, stemming, 언어별 형태소는 v1 필수 기능이 아니다.
- 일반 검색 언어는 기존 CEL parser를 사용하는 제한된 식이다. Sentry 검색창·Tantivy 문법을 그대로 호환한다고 하지 않는다. **Sentry SDK ingestion 호환성과 UI query 문법은 별개다.**
- S3가 이벤트 바이트의 정본이고 PostgreSQL이 수신 확정·공개 파일 집합·권한·Issue 상태의 정본이다. 두 저장소를 모두 복구해야 한다.
- 작은 설치와 분산 설치가 같은 데이터 계약을 사용한다. 작업자별 데이터 소유권/물리 shard 이동을 확장 필수 절차로 만들지 않는다.
- 1 vCPU/512MiB는 API 또는 worker 실행 단위 목표다. PostgreSQL·객체 저장소·호스트 여유분은 별도이며 총비용 보고에서 제외할 수 없다.

구체 기술 선택: Go 표준 `net/http`/`slog`, PostgreSQL `pgx/v5`, AWS SDK for Go v2(S3), 공식 duckdb-go/v2, cel-go, Prometheus client. UI는 React+TypeScript+Vite, TanStack Query, OpenAPI 생성 DTO, Vitest/Playwright다. JSON/OpenAPI/SQL migration은 저장소에서 버전 관리하고 driver·codec·암호 구현을 직접 작성하지 않는다. 정확한 의존성 patch와 container digest는 G00에서 실제 지원성을 확인한 lock이 정본이다.

이 구조의 비용상 약점도 확정한다. S3 원문 전체 substring/regex는 역색인보다 많은 bytes를 스캔할 수 있고, 단일 서버보다 PG+S3의 최소 비용·운영 의존성이 크다. 프로젝트/시간/컬럼 pruning, 배치화, cache/compaction으로 줄이되 비용 우위는 §21 비교 전 미입증이다. 모든 기간의 임의 검색을 512MiB 한 worker에서 항상 즉시 끝낸다고 약속하지 않는다.

### 0.2 보존할 비교군과 경로

신규 구현 루트는 `go/eventglass/`. 예정 module은 `eventglass/go/eventglass`, 실행 파일은 `eventglass-go`. 루트 Rust와 `go/benchmark/`는 수정하지 않는다. 설계 시 Rust 기준 commit은 `9b8c7c0`이며, 구현 시작 시 dirty 상태와 실제 비교 revision을 추가 기록한다.

원안 `docs/observe/source-design.md` SHA-256:

```text
4cccddc98f76d5c38099958587386c55756a08cf366c26ed9e65eeedec0526e5
```

원안의 SQLite/Indexer/Tantivy 선택은 **Rust 비교군에 남긴다**. 이 신규 제품에서의 변경은 본 문서가 명시적으로 대체한다. 기본 검색 의미·S3 ACK 비용이 달라지므로 단순 언어 속도 비교라고 부르지 않는다.

### 0.3 v1 제품 경계

필수: Error/default Event, Structured Log, client report 진단, transaction의 기본 보관·관련 로그 연결, SDK 입력 프레임 표시, Issue grouping/resolve/ignore/regression, 프로젝트/키/사용자 권한, 검색·상세·histogram·group-by, Live, 경보, 보존·자동 병합·복구·자동 확장 지표, UI, 실제 SDK 검증.

transaction은 `kind=transaction` 한 행으로 저장하고 내부 spans를 상세에 보존한다. v1에서 span마다 독립 로그/Issue를 생성하지 않는다. trace ID 연관 조회는 제공하지만 완전한 APM waterfall·샘플링·성능 제품 호환은 주장하지 않는다.

Replay, profile, metrics, sessions/release health, check-in/cron, feedback, native minidump, 임의 attachment, artifact/source-map 업로드·symbolication은 **기능 미지원으로 명시**한다. 혼합 envelope가 정상 오류·로그를 포함하면 지원 항목은 처리하고 미지원 항목의 타입·개수·바이트를 진단한다. 미지원 bytes를 몰래 보관하거나 오류/로그로 변환하지 않는다. Rust의 Replay/Feedback UI까지 Go v1에 이관됐다고 하지 않는다.

"완벽한 SDK 분석"의 출시 정의는 **명시한 버전·항목·행동의 fixture 계약 통과**다. 모든 Sentry 기능과 모든 미래 SDK의 완전 호환은 보장하지 않는다. SDK별 enable/flush/transport 설정을 함께 문서화한다.

## 1. 구성과 소유권

```text
SDK → API(normalize/scrub) → S3 journal → PG Accept → HTTP ACK
                                      ↓ jobs
                               converter supervisor
                                      ↓ DuckDB child
                              S3 Parquet bundle
                                      ↓ PG Publish
UI → API(scope/plan) → PG snapshot → workers(DuckDB) → reducer → response
                                      ↑
                         compact / retain / repair jobs
```

프로세스 역할:

| 역할 | 소유 상태 | 영구 상태 여부 |
|---|---|---|
| API | bounded ingress buffers, 인증 요청, 검색 계획 | 없음 |
| worker supervisor | 임대 heartbeat, 다운로드, task 결과 | 없음 |
| DuckDB child | 한 작업의 DB·캐시·spill·실행 상태 | 없음 |
| scheduler | 주기적 DB 검사·작업 예약 | lease로 교체 가능 |
| PostgreSQL | catalog, receipts, jobs, Issues, auth, outbox | 정본 |
| S3 | sanitized journal, queryable bundles, checkpoint artifacts | 정본 |

`run --roles=api,worker,scheduler`는 소규모 실행, 역할 분리는 분산 실행이다. DuckDB는 동일 binary의 `engine-child` subcommand에만 로드한다. Go supervisor와 child를 합한 cgroup RSS를 측정한다. 여러 worker가 같은 DuckDB 파일을 열지 않는다.

의존성 방향은 `http → operations → domain/storage/query`다. 트랜잭션 정책은 handler 밖 concrete operation에 둔다. Clock/ObjectStore/EngineProcess 같은 실제 경계 외에 범용 Repository/Service 계층을 만들지 않는다.

예정 구조:

```text
cmd/eventglass-go/       CLI, child 진입점
internal/app/           역할 조립·shutdown
internal/ingest/         framing·normalization·scrub·accept
internal/sdk/            wire DTO·version adapters
internal/control/        pgx SQL, transaction operations, jobs, catalog
internal/model/          canonical record, IDs, typed attributes
internal/issues/         grouping 및 lifecycle 순수 계산
internal/query/          CEL allowlist→typed plan→SQL, scope, snapshot, merge
internal/engine/         DuckDB child, conversion, projection, SQL 실행
internal/storage/        S3, cache, object inventory, verified downloads
internal/maintenance/    compact, retain, GC, backup checks
internal/alerts/         평가·outbox·전송
internal/api/            OpenAPI DTO·인증·HTTP·SSE
migrations/             PostgreSQL SQL migrations
web/                    독립 UI, 생성 API DTO
tests/                  contracts, sdk, integration, crash, resources, comparison
deploy/                 local 및 cluster 구성, 버전·digest lock
scripts/                검증/개발 진입점
```

## 2. SDK 조사 결과와 지원 정책

조사한 소스 태그는 Python `2.69.0`, JavaScript Node/Browser `10.73.0`, Go `v0.49.0`. 기존 fixture lock과 동일하다. 커밋·소스 링크·확인 수준은 [SDK-SOURCES.md](SDK-SOURCES.md)에 있다. **이번 문서 작성에서 실행한 Go 제품 SDK 테스트는 없다.**

### 2.1 반드시 구분할 데이터

| 입력 | canonical kind | Issue 생성 | 의미 |
|---|---|---|---|
| `event` (exception 있음/없음) | `error` | 예 | captureException뿐 아니라 captureMessage 이벤트도 포함 |
| `log`의 `items[]` | `log` | 아니오 | level=error/fatal이어도 오류 이벤트로 승격 금지 |
| event의 `breadcrumbs` | 부모 상세 | 아니오 | 독립 로그로 복제하면 count 왜곡 |
| `transaction` | `transaction` | 아니오 | trace 관련 기본 조회, 성능 제품 아님 |
| `client_report` | SDK outcome | 아니오 | SDK에서 전송 전에 버린 데이터의 best-effort 진단 |
| 미지원 item | unsupported outcome | 아니오 | framing은 이해하되 bytes는 저장하지 않음 |

Python logging 한 호출이 breadcrumb + structured log + error event로 이어질 수 있다. 이것은 중복 전송 오류가 아니다. 세 경로를 메시지/시간 hash로 합치지 않는다. SDK `before_send`, `before_send_log`, sampling, transport buffer에서 사라진 데이터는 서버에서 복구할 수 없다.

### 2.2 조사로 확인한 wire 차이

| SDK | 로그 container | severity/trace/template 특징 |
|---|---|---|
| Python 2.69.0 | `type=log`, `version=2`, `items`, `item_count` | severity가 `sentry.severity_number/text` typed attribute에 들어감. timestamp는 epoch seconds. top-level trace_id/span_id 가능 |
| JS 10.73.0 | `version=2`, browser는 `ingest_settings` 추가 가능 | top-level severity_number, trace_id; parent span은 `sentry.trace.parent_span_id`; `sentry.message.parameter.N` |
| Go 0.49.0 | `items` container에 version이 없는 경로 존재 | RFC3339 time.Time timestamp, top-level severity_number/span_id; `sentry.message.parameters.N`(복수형) 경로 존재 |

Log container의 version **없음, 1, 2**를 별도 adapter로 지원한다. version=1은 과거 fixture/명세로 G01에서 확인하고 다른 필드를 추측해 해석하지 않는다. 알려지지 않은 숫자 version은 지원 데이터 계약 오류로 400을 반환한다. version 없는 Go container를 거절하지 않는다. `items` 없는 legacy `logs` wrapper/단일 log payload는 fixture 근거 없이 지원하지 않는다.

severity 우선순위: 유효한 top-level `severity_number` → typed `sentry.severity_number` → 정규화 level의 대표값(trace=1, debug=5, info=9, warning=13, error=17, fatal=21). 원래 level과 숫자를 별도로 보존한다. level alias `warn→warning`, `critical→fatal`; 임의 값은 `unknown` 및 warning, 거짓으로 info/error로 바꾸지 않는다. 로그 level과 severity가 충돌하면 level 표시는 유지하고 conflict warning을 남긴다.

유효 severity 범위는1..24다. 범위 밖 숫자는 원문 보존+warning 후 fallback한다. event에서 level이 missing이면 프로토콜 기본 error, structured log에서 missing이면 unknown이다. trace/span ID는 각각32/16 hex로 검증·소문자화하고 all-zero는 미설정으로 처리한다. 잘못된 trace/span은 raw에 보존+warning, 관계 검색 projection은 null이며 전체 정상 로그를 거절하지 않는다.

trace 우선순위: log `trace_id`/`span_id` → log `sentry.trace.parent_span_id`(span fallback만). event/transaction은 `contexts.trace`. envelope의 trace 정보는 배치 전체 로그의 trace ID로 덮어쓰지 않는다. 서로 다른 trace의 로그가 같은 배치에 올 수 있다.

## 3. HTTP·envelope 계약

### 3.1 엔드포인트와 인증

- `POST /api/{project_id}/envelope/`: 기본.
- `POST /api/{project_id}/store/`: legacy event JSON adapter.
- 인증 입력: `X-Sentry-Auth`, query `sentry_key`/`sentry_version`, envelope `dsn`. 최소 하나가 필요하며 여러 개면 public key·project가 일치해야 한다. secret DSN 부분은 권한으로 사용하지 않는다.
- envelope DSN은 identity 검증용이며 그 host로 proxy/fetch하지 않는다. path project는 인증된 DB project와 일치해야 한다.
- 프로젝트 비활성화·키 revoke는 **Accept transaction에서 다시 검사**한다. commit 후 revoke는 이미 ACK된 데이터를 취소하지 않는다.
- browser CORS: 프로젝트 origin allowlist, `Vary: Origin`, 필요한 content/auth header만 허용. ingestion에는 cookie를 사용하지 않는다. 노출 헤더에 `Retry-After`, `X-Sentry-Rate-Limits`, `X-Eventglass-Receipt`를 포함한다.
- public DSN은 수집 권한뿐이다. 검색·관리·artifact 업로드 권한으로 사용할 수 없다.

### 3.2 framing과 resource limits

UTF-8 JSON envelope header와 item header를 LF 기준으로 읽는다. `length`는 문자 수가 아닌 bytes다. 있으면 정확히 그 길이를 소비하고 LF/EOF만 허용한다. 없으면 다음 LF/EOF까지 payload다. binary 미지원 item도 length대로 건너뛰어 다음 item을 손상시키지 않는다. 마지막 LF는 선택이다. framing은 모든 item을 검사하며 미지원이라는 이유로 잘못된 length를 무시하지 않는다.

지원 item의 JSON에 duplicate key가 있으면 400. envelope header에 동일 `sent_at` 중복도 허용하지 않는다. UTF-8 오류는 400, JSON escape의 lone surrogate는 U+FFFD로 정규화하고 warning을 남겨 SDK 차이를 명시한다.

한 envelope의 `event`/`transaction`은 합쳐 최대 하나다. 둘이 함께 있거나 같은 타입이 둘 이상이면 400이다. 여러 structured log와 하나의 event는 함께 올 수 있다. header-only envelope는 합법적 empty 요청이며 item header 없는 잔여 bytes는 오류다.

초기 hard limit(Go 정책, Sentry SaaS의 전체 한도와 동일하다는 주장 금지):

| 항목 | 한도 |
|---|---:|
| wire body / 압축 해제 body | 각각 20MiB |
| request의 canonical payload 합 | 20MiB |
| event 또는 log 한 개의 canonical payload | 1MiB |
| envelope/item header | 각각 16KiB |
| item 수 / canonical record 수 | 1,000 / 10,000 |
| JSON depth / nodes (한 record) | 64 / 20,000 |
| typed attributes(한 log) | 1,000 |
| 동시 decode | API당 2, byte admission과 함께 적용 |

전체 요청을 unbounded DOM으로 열지 않는다. item/record 단위로 파싱·마스킹한 결과만 제한된 spool에 저장 가능하며 **마스킹 전 bytes를 disk/log/S3에 쓰지 않는다**. 마지막 item 검증 실패 시 지원 데이터 전체를 미수신 처리한다. 이미 S3 staging이 있다면 미공개 orphan일 뿐 ACK receipt가 없어야 한다.

HTTP 압축은 identity/gzip/deflate(zlib)/br/zstd를 decoder별 bound와 함께 지원 목표로 둔다. 각 decoder는 G01 실제 transport fixture 통과 전 enable하지 않는다. Content-Encoding 미지원은 415; decompression limit 초과 413. legacy store의 base64+zlib는 JSON/content-encoding 경로와 분리해 제한된 adapter로 구현하고 테스트한다. gzip 자동 감지 같은 추측 파싱 금지.

### 3.3 item 처리·응답

지원 item 중 유효하지 않은 것이 있으면 요청 전체 400/413, 지원 record 수신 0. 알 수 없는 item은 framing 검증 후 skip하고 타입/수량/bytes만 기록한다. 이는 forwarding Relay가 아닌 제한된 수신 제품 정책이다. 미지원 payload를 보존했다고 말하지 않는다.

지원 데이터가 확정되면 envelope는 200 JSON `{}` 및 receipt header, store는 200 `{"id":"..."}`. 비어 있거나 미지원 항목뿐이면 200 및 `X-Eventglass-Unsupported-Items`; UI에 diagnostic counter를 노출한다. 이 응답이 모든 Sentry 기능 지원을 뜻하지 않음을 SDK 설정 안내에 명시한다. client report만 있으면 outcome 저장 후 200.

400 malformed, 401 missing/invalid key, 403 project disabled, 413 limit, 415 encoding, 429 admission/quota, 503 S3/PG dependency unavailable. 실패 응답에는 sanitized reason code만 넣는다.

429에는 `Retry-After`와 `X-Sentry-Rate-Limits`를 보낸다. v1 request 전체 거절은 빈 categories(all)로 일치시켜 부분 수신 착각을 막는다. category별 quota를 추가할 때 `log` item type과 `log_item`/`log_byte` data category를 혼동하지 않는다. SDK의 429 반응은 보통 이후 항목의 **drop**이며 보존 후 재전송 약속이 아니다. 503도 SDK별 재시도 보장이 없으므로 최소 warm ingress와 burst 예산이 필요하다.

## 4. 정규화·마스킹·식별자

### 4.1 시간·숫자

JSON 숫자는 Go `json.Number`로 읽고 typed integer를 float64 경유하지 않는다. event timestamp는 RFC3339 또는 epoch seconds decimal, log도 두 표현을 지원한다. decimal 문자열에서 정수 연산으로 UTC microsecond와 0..999 nanosecond remainder를 계산한다. negative 시각은 floor division을 사용한다. 원래 시각 표현과 유효한 sub-microsecond는 raw에 남긴다.

missing timestamp는 API가 요청을 처음 받은 `arrival_time_us`로 대체하고 `timestamp_source=arrival` 표시. 잘못된 제공 timestamp는 supported record 오류. `sent_at`는 별도 metadata이며 자동 clock correction은 v1에서 하지 않는다. 미래/과거 이벤트를 조용히 삭제하지 않고 skew 진단한다.

운영용 `received_time_us`는 arrival과 다르다. Accept가 lane을 잠근 뒤 DB clock과 `lane.last_received_time_us` 중 큰 값으로 배치 전체에 부여하며 lane에도 저장한다. journal에는 arrival만 있고 received/seq는 확정 receipt에서 변환 시 overlay한다. 따라서 늦게 확정된 요청이 이미 닫힌 received-time 경보 창에 소급 진입하지 않는다. 보존도 이 확정 received-time 기준이다. DB 시계 역행은 lane별 clamp 및 clock-skew 경보로 드러낸다.

고정 seq/count는 signed 64-bit; typed attribute integer는 DECIMAL(38,0)까지 정확하게 다룬다. 그 범위를 넘는 integer는 `big_integer`와 원문 숫자 bytes를 보존하고 숫자 집계 대상에서는 제외/명시한다. NaN/Infinity 같은 비JSON 숫자는 400. double의 wire type은 정수처럼 보여도 double로 유지한다. UI의 64-bit ID/count/decimal integer는 문자열이다.

### 4.2 속성·promoted columns

로그 `{type,value,unit?}`를 해제하되 wire type을 보존한다. integer/string/boolean/double/array를 지원한다. null·missing·빈 배열·문자열 `"null"`을 구분한다. 배열은 원소 타입을 보존하고 heterogeneous 배열도 무조건 문자열로 만들지 않는다. 선언과 값 불일치는 해당 attribute를 `invalid`로 보존하고 진단하며, 검색은 잘못된 타입을 맞는 값으로 취급하지 않는다.

소스 namespace를 분리한다: `attributes`, `tags`, `extra`, `contexts`, `user`, `request`, `sdk`. 각 path는 RFC6901 JSON Pointer 문자열이다. `/a.b`와 `/a/b`는 다르다. 배열 위치는 `/a/0`. object key 순서는 의미에 포함하지 않는다. dynamic path 때문에 파일당 컬럼 수가 무한히 늘어나면 안 된다.

| 고정 컬럼 | log 출처 | event/transaction 출처 |
|---|---|---|
| message | body | logentry.formatted → message string/interface formatted → exception type/value → logentry.message → 빈 문자열 |
| message_template | sentry.message.template | logentry.message(포맷 이전) |
| release/environment | sentry.release/environment → release/environment attribute | top-level |
| service | service.name attribute → project configured service | tags service.name → contexts service.name → project configured service |
| logger | logger.name → logger attribute | logger |
| sdk_name/version | sentry.sdk.name/version → envelope sdk | event sdk → envelope sdk |
| trace_id/span_id | §2.2 | contexts.trace |
| server_name | server.address → sentry.server.address → server.name | server_name |

빈 값은 명시적 empty로 보존하고 fallback은 missing에서만 적용한다. unknown property는 scrubbed raw에 남긴다. `sentry.message.parameter.*`와 Go `parameters.*`는 원래 key를 보존하고 분석용 parameters view에서만 공통화한다. SDK가 이미 stringify한 객체는 다시 JSON object로 해석하지 않는다.

### 4.3 마스킹

프로젝트 설정·전역 기본 키를 하나의 traversal로 처리한다. authorization/cookie/password/token/secret 키, header pair 배열, URL query, nested attrs, request body, stack vars, breadcrumbs, user 데이터를 포함한다. known typed attribute의 value와 파생 message/template도 같은 정책에 들어간다. message에 포함된 비밀값을 모두 탐지한다는 약속은 하지 않는다. body regex rule은 제한된 RE2로 적용한다.

마스킹 후에 검색 projection·Issue title·fingerprint·content checksum을 만든다. `ingest_settings.infer_ip/agent`가 와도 v1에서는 IP/UA를 자동 수집하지 않고 `inference_disabled` 진단한다. DSN/header/raw request를 로그·trace·오류 메시지에 출력하지 않는다.

### 4.4 ID와 중복

- `acceptance_id`: API가 검증된 HTTP 요청에 부여한 UUIDv4. 내부 재시도에서 유지, 다른 HTTP 요청이면 새 값.
- envelope event_id가 있으면 event/transaction payload event_id보다 우선한다(공식 규약). mismatch는 진단한다. 32hex/UUID hyphen 형식을 32 lowercase hex로 정규화한다.
- 오류/transaction ID가 모두 없으면 acceptance_id/item ordinal로 ID를 만든다. 합법적인 missing을 허위 SDK ID로 표시하지 않는다.
- `record_id`: 길이 prefix를 포함한 domain-separated SHA-256. source event가 있으면 `(project,kind,event_id)`, ID 없는 record는 `(project,acceptance_id,item_ordinal,record_ordinal)`.
- structured log에는 표준적인 event_id가 없다고 가정한다. envelope의 event_id를 모든 로그 ID로 쓰지 않는다. JS sequence attribute, trace/span ID, message hash를 dedupe ID로 쓰지 않는다.
- 명시적 source event ID는 project+kind별 dedupe table에서 수신 확정 시 claim한다. 먼저 확정된 payload가 승자. 동일 ID/다른 content는 overwrite하지 않고 conflict outcome. 재전송 duplicate는 성공 ACK지만 신규 row/Issue count는 0.
- dedupe 보존은 전체 이벤트 보존 기간 + 7일. 그 이후 재전송은 새 수신이 될 수 있음을 명시. ID 없는 로그의 서로 다른 HTTP 재전송을 exactly-once라고 주장하지 않는다.

## 5. 영구 record와 Parquet 스키마

wire DTO, canonical DTO, analytics schema를 별도로 둔다. canonical JSON은 버전 있는 내부 journal 형식이며 raw envelope와 다르다. normalizer/grouping/scrub/schema version을 저장하고 재시도 때 다시 계산하지 않는다.

### 5.1 analytics 행

| 컬럼 | DuckDB 타입/의미 |
|---|---|
| tenant_id, project_id | BIGINT, 필수 권한 scope |
| record_id, acceptance_id, batch_id | VARCHAR(hex/UUID), stable identity |
| lane_id, batch_seq, record_ordinal | INTEGER, BIGINT, INTEGER, 수신/공개 위치 |
| kind | VARCHAR: error/log/transaction |
| event_time_us, arrival_time_us, received_time_us | BIGINT, UTC μs |
| event_time_ns_remainder | USMALLINT, 0..999 |
| source_event_id, trace_id, span_id | nullable VARCHAR, validated hex |
| level, original_level, severity_number | VARCHAR, VARCHAR, SMALLINT nullable |
| message, message_template | VARCHAR nullable, 마스킹 후 |
| service, environment, release, logger | VARCHAR nullable |
| sdk_name, sdk_version, platform, server_name | VARCHAR nullable |
| issue_id, exception_type, exception_value, handled | nullable projections |
| attrs | 아래 정의한 STRUCT[] |
| search_values | VARCHAR[], 마스킹된 검색 대상 scalar 문자열 |
| schema_version, normalizer_version, grouping_version | INTEGER |
| warnings | VARCHAR[], bounded codes |

`attrs` element는 `{namespace VARCHAR, path VARCHAR, value_type VARCHAR, string_value VARCHAR, integer_value DECIMAL(38,0), double_value DOUBLE, boolean_value BOOLEAN, json_value VARCHAR, unit VARCHAR}`. type에 해당하는 scalar slot 하나만 채우며 object/array/null/invalid/big_integer는 json_value와 type으로 보존한다. namespace+path는 행 안에서 UNIQUE. root object도 필요하면 container entry로 보존하며 하위 scalar entry를 함께 제공한다. 중복 entry로 array membership/group count를 늘리지 않는다.

`search_values`는 message, exception 문자열, breadcrumb message, user가 검색 가능한 namespace의 scalar 값들이다. 키 이름·token secret·raw JSON escape를 검색 값으로 넣지 않는다. phrase/substring은 **한 scalar 내부**에서만 일치한다. concatenation 경계를 넘어 매칭하지 않는다. 임의 64KiB truncation으로 검색 완전성을 깨지 않는다. 원문 제한 내에서 전체 projection을 만들 수 없으면 413 또는 명시적 오류이며 조용한 누락 금지.

### 5.2 payload 행과 bundle

상세용 payload는 별도 `payload.parquet`에 `(record_id, raw_json, envelope_sdk_json, normalization_warnings_json)`로 저장한다. `raw_json`은 scrubbed 원형이고 canonical index projection을 재구성할 유일 근거로 삼지 않는다. analytics/payload는 한 bundle로 함께 공개한다. list/count 쿼리는 payload를 읽지 않는다. detail은 catalog가 지정한 bundle 안에서 record_id를 찾아 읽는다.

Parquet는 UTC 의미가 명확한 정수 time 컬럼, Zstd 초기 level=3, dictionary 적합 컬럼(service/level/release/sdk 등)에만 활용한다. v1 row group 목표 16,384행, canonical 처리량 8MiB 단위 flush를 초기값으로 둔다. DuckDB writer의 실제 row-group·메모리 behavior는 G00에서 확인한다. 파일 목표는 압축 후 32~64MiB, hard 목표 128MiB이며 저유량 파일은 시간으로 먼저 공개한다. 작은 파일을 즉시 큰 파일까지 기다리게 해서 visibility를 무한 지연시키지 않는다.

초기 파일 partition은 `(tenant,lane,event UTC day,kind)`이고 파일 내부 정렬은 `(project_id,service,event_time_us,record_id)`다. 24시간 이상 wide event time은 spill/repartition하며 tenant/project마다 별도 writer를 무제한 유지하지 않는다. late event를 오늘 partition에 억지로 넣지 않는다. receipt-time min/max도 반드시 catalog에 둔다.

## 6. 수신 journal과 batch 구성

초기 microbatch 목표: 마스킹 canonical 4MiB 또는 1,000 records 또는 oldest request 대기 100ms 중 먼저 도달. 요청은 batch 사이에 나누지 않는다. 목표보다 큰 합법적인 요청(최대20MiB/10,000 records)은 전용 batch. 총 ingress byte budget 64MiB와 spool quota로 제어하며 target를 hard request limit로 오용하지 않는다.

S3 journal은 Zstd 압축 JSONL이며 첫 행은 format/normalizer/schema/scrub version, 이후 요청별 header 및 canonical records. 각 요청의 ordinal/range·checksum을 기록한다. 길이·count·hash 검증 없이 재생하지 않는다. job은 SDK network envelope를 다시 normalize하지 않는다.

`tenant`마다 virtual ingest lane 16개를 초기 생성한다. 한 request는 `hash(acceptance_id) mod lane_count`로 한 lane에 속한다. batch는 tenant+lane별 묶음이다. lane 수는 설치의 버전 있는 topology 설정이며 v1에서 자동 변경하지 않는다. 이것은 worker 수가 아니며 임의 worker가 어느 lane의 작업이든 실행한다. 수집 API는 demand-driven buffer만 만들고 lane마다 4MiB씩 선할당하지 않는다.

lane별 seq는 PostgreSQL counter row를 transaction에서 갱신한다. `nextval()`/시간/UUID를 contiguous committed watermark로 사용하지 않는다. `(lane,batch_seq)`가 commit 순서이며 실패한 Accept transaction은 counter도 rollback한다. 서로 다른 lane 사이 전역 순서는 약속하지 않는다.

Journal key 예: `v1/{installation}/journals/{tenant}/{lane}/{batch_uuid}.jsonl.zst`. URI에는 credential을 넣지 않는다. 서버가 만든 key만 사용하고 사용자 경로를 연결하지 않는다.

## 7. PostgreSQL 모델과 실제 트랜잭션

PostgreSQL 17 계열을 초기 지원선으로 잡고 실제 patch/image digest는 G00에서 고정한다. `synchronous_commit=on`. HA 설치는 failover 시 ACK durability를 보존할 동기 복제 설정이 필요하다. asynchronous replica failover를 RPO=0이라고 부르지 않는다.

### 7.1 논리 테이블과 제약

| 테이블 | 필수 키·필드·역할 |
|---|---|
| installations | singleton id, storage_generation, schema_version, topology_version |
| tenants/projects/project_keys | state, auth_revision, scrub_revision, key hash, 설정 |
| users/memberships/sessions | scoped auth, hash-only session tokens |
| lanes | PK(tenant,lane), accepted_seq, published_seq, catalog_generation, last_received_time_us |
| object_intents | object_id PK, server key UNIQUE, kind, state, owner/fence, expires_at, bytes/checksum |
| ingest_batches | PK(tenant,lane,seq), batch_uuid UNIQUE, journal_object FK, record_count, accepted selection, state |
| receipts | acceptance_id UNIQUE, batch FK, ordinal range, accepted/duplicate/unsupported counts |
| event_dedupe | UNIQUE(project,kind,source_event_id), record_id, owning receipt, content hash, expires_at |
| jobs | id PK, UNIQUE(kind,input_identity), state, attempt, fence, owner, lease_until, retry_at, error code |
| job_outputs | job+fence+object refs, count/checksum, prepared manifests |
| bundles/files | scope, generation interval, schema, checksums, row count, both time bounds, object refs |
| query_snapshots | id, principal/scope hash, lane position+generation vector, TTL, heartbeat |
| issues | UNIQUE(project,grouping_version,fingerprint_hash), status, revision, count, first/last, resolved_cut |
| issue_occurrences | record_id UNIQUE, issue FK, lane/seq/ordinal, times, release, payload locator |
| alerts/evaluations/deliveries | revision, cut/window, unique evaluation/delivery key, state/retry |
| sdk_outcomes | receipt/item ordinal UNIQUE, category/reason/count, approximate flag |
| schema_migrations | version UNIQUE, checksum |

FK에 tenant/project scope를 포함하거나 검증된 operation에서 scope 일치를 보장하고 cross-tenant negative test를 둔다. 큰 JSON receipt에 모든 raw records를 넣지 않는다. 일반 로그마다 PostgreSQL 행을 만들지 않는다. 단, source-ID dedupe와 Issue occurrence는 오류/transaction당 관리 비용이 있어 총비용에 포함한다.

### 7.2 object intent: upload와 GC 경합 방지

업로드 전 짧은 PG transaction으로 `object_intents(pending,key,expiry,fence)`를 등록한다. 업로드 뒤 checksum/size를 확인한다. Accept/Publish는 intent row를 잠그고 pending/uploaded 상태와 fence를 검사한 뒤 referenced로 전환한다. GC는 만료 intent를 잠가 deleting으로 전환한 후에만 S3 삭제한다. deleting 상태에서는 공개할 수 없다.

만료된 worker가 삭제 직후 뒤늦게 PUT할 수 있으므로 tombstone key 재검사·유예기간을 둔다. 무기한 늦은 PUT이 정본으로 채택되는 일은 없고, 주기적 orphan sweep가 회수한다. S3 LIST 결과만으로 live 파일을 삭제하지 않는다. intent가 없는 파일은 격리·진단하며 해당 prefix의 운영 파일을 추측 삭제하지 않는다.

### 7.3 Accept operation

S3 journal 업로드 성공 뒤 아래를 한 transaction으로 수행한다.

1. receipt 존재 시 같은 batch/content인지 확인하고 기존 결과 반환(내부 재시도).
2. 관련 project/key 행을 정렬된 순서로 잠그고 활성/권한/scrub_revision을 재확인. normalize 이후 scrub 설정이 변했으면 stale 결과를 ACK하지 말고 재정규화 또는 409/503 내부 재시도로 처리한다.
3. lane row를 잠그고 source event IDs를 stable 순서로 claim. winner/duplicate/conflict selection을 확정한다. selection은 input ordinal interval/bitmap과 checksum으로 저장한다.
4. accepted_seq를 1 증가하고 §4.1의 received 시각을 확정. 배치·receipt·convert job·outcomes 삽입.
5. intent referenced 전환 후 COMMIT. durable ACK는 여기 이후다.

S3에는 duplicate 후보가 있어도 변환은 receipt selection만 사용한다. 네트워크 reply가 유실돼도 성공 commit을 rollback했다고 말하지 않는다. 한 batch에 여러 request가 있으면 validation이 끝난 요청만 모으고 Accept는 전체 batch를 atomic 확정한다. deadlock/serialization retry는 같은 batch/acceptance ID로 bounded 재시도한다.

accepted selection이 비어 있는 duplicate/outcome-only 배치도 seq를 할당했다면 빈 publication으로 정상 완료시킨다. 존재하지 않는 Parquet 파일을 요구하거나 lane watermark를 멈추면 안 된다. 아직 seq를 할당하지 않은 empty/unsupported-only 요청은 진단 transaction만으로 응답할 수 있다.

### 7.4 Claim / prepare / publish

Claim은 `FOR UPDATE SKIP LOCKED`로 짧게 선택하고 `fence=fence+1, owner, lease_until=DB clock+60s`를 기록한 뒤 commit. heartbeat 15초. 긴 native 작업 동안 DB transaction을 열지 않는다. DB와 연결이 끊긴 worker는 publish를 시도해 fence 검증 없이 성공할 수 없다.

conversion은 같은 lane의 여러 batch를 병렬 처리할 수 있다. 결과는 `prepared`로 둔다. **공개는 lane별 contiguous 순서**로만 전진한다. batch N이 실패하면 N+1을 이미 계산했어도 watermark를 건너뛰지 않는다. 다른 lane은 진행한다. poisoned ACK batch는 무음 skip하지 않고 lane stalled 및 repair 상태를 표시한다.

Publish transaction:

1. 확정 receipt의 project/scope 일치 검증 → lane row 잠금 → job/fence/intent 검증 → Issue 행을 stable ID 순서로 잠금. Accept 이후 키 revoke/project disable은 신규 수신·조회에 적용할 뿐 이미 ACK된 배치의 공개를 막지 않는다.
2. 입력 seq가 `published_seq+1`인지 검사. 이미 완료된 동일 output은 no-op; 다른 output의 중복 공개는 오류.
3. accepted error occurrence를 unique insert하고 §14의 최신 Issue 상태에 반영.
4. 모든 analytics+payload output을 같은 catalog_generation에 공개하고 source input을 처리 완료로 변경.
5. outbox 삽입, job completed, lane published_seq/counter 갱신, COMMIT.

단일 요청 최대 10,000행 때문에 발생할 PG transaction 시간은 gate에서 측정한다. 부하를 이유로 요청의 durable accept atomicity를 깨지 않는다. Publish는 batch boundary이며 일부 output 파일만 먼저 공개할 수 없다.

모든 operation의 lock 순서를 공통화한다: `project/key rows → lane rows → event_dedupe keys → job/object intents → issue rows → child rows`. 같은 종류는 stable key 순서다. project/key 상태 검사는 FOR SHARE로 병렬 Accept를 허용하고 설정 변경은 충돌하는 exclusive lock을 사용한다. claim/heartbeat/GC는 다른 상위 row를 나중에 잡지 않는다. serialization/deadlock은 retry 가능하되 외부 전송을 transaction 안에서 재실행하지 않는다.

## 8. Snapshot·cursor·권한의 공통 계약

전역 scalar watermark 대신 `Cut = {(tenant,lane): published_seq}`와 `Generations = {(tenant,lane): catalog_generation}`를 사용한다. 수신 완료 cut은 accepted_seq vector다. 비교는 componentwise이며 서로 다른 lane 간 임의의 total order를 만들어 correctness에 쓰지 않는다.

새 검색 snapshot은 다음 방식으로 생성한다.

1. 현재 principal/project 권한 검증. PG REPEATABLE READ transaction에서 대상 lane rows를 정렬된 순서로 공유 잠금한다.
2. 그 snapshot의 published/cat generation vectors를 읽고 `query_snapshots`와 lane refs를 등록한다. retention/GC의 exclusive 잠금과 이 등록이 직렬화되어야 한다.
3. catalog에서 `valid_from_generation <= G < valid_to_generation`(끝 NULL=무한)을 만족하는 파일을 선택한다. 두 time basis와 project pruning을 적용한다.
4. 해당 snapshot scope/cut을 모든 worker와 reducer에 전달하고 행 필터에도 tenant/project/cut을 넣는다. 파일이 한 프로젝트만 가진다고 권한 필터를 생략하지 않는다.
5. 모든 작업 완료 후 응답 직전 auth revision 재검사. 중간 revoke면 응답 중단. stream은 batch/heartbeat마다 재검사.

snapshot은 TTL 15분, 실행 중 heartbeat 30초, 최대 연장 1시간. 오래된 토큰은 기존 snapshot row가 살아 있을 때만 재사용한다. 과거 generation을 토큰만 보고 새로 부활시키지 않는다. 목록+histogram+상세가 같은 snapshot을 사용한다. 페이지 정렬은 `(event_time_us DESC,event_time_ns_remainder DESC,record_id DESC)`; cursor에 stable tuple을 넣고 OFFSET은 쓰지 않는다. received-time 정렬은 별도 enum이다.

HMAC 토큰: version, installation generation, snapshot_id, principal/scope hash, normalized query hash, sort, cursor tuple, expiry. secret은 관리 secret이며 data URL에 노출하지 않는다. 변조/요청 mismatch 400, 권한 실패403, 만료410, restore generation mismatch409. detail token도 snapshot 및 권한을 검증한다.

파일 선택/다운로드/SQL/merge 중 하나라도 실패하면 전체 결과를 실패 처리한다. UI의 progressive 결과에는 `complete=false`를 붙이고 정확한 count/경보 성공값으로 사용할 수 없다.

## 9. 검색 표현식과 의미 — 구현 시 재해석 금지

### 9.1 공개 입력

API는 `filter`(typed JSON AST) 또는 `expression`(아래 CEL subset) 중 하나를 받는다. UI는 같은 AST를 사용한다. CEL은 `github.com/google/cel-go`의 parser/type checker를 재사용하고 evaluator는 query 실행에 쓰지 않는다. 검증된 AST를 DuckDB SQL fragment로 변환한다. 이것은 제품 filter adapter이며 범용 SQL compiler가 아니다.

허용 예:

```text
level == "error" && contains(message, "connection refused")
(service == "api" || service == "worker") && severity_number >= 17
exists("attributes", "/http.response.status_code")
iattr("attributes", "/http.response.status_code") >= 500
sattr("tags", "/region") == "ap-northeast-2"
matches(message, "timeout|timed out")
text("database") && !text("healthcheck")
array_contains("attributes", "/features", "checkout")
```

top-level 고정 필드: kind, level, severity_number, message, message_template, service, environment, release, logger, sdk_name, sdk_version, platform, server_name, trace_id, span_id, source_event_id, issue_id. 시간 범위·프로젝트는 별도 필수 request scope다.

함수:

- `contains/starts_with/ends_with(field,string)`: literal substring/접두/접미. `%`/`_`를 wildcard로 해석하지 않는다.
- `matches(field,pattern)`: DuckDB RE2 search 의미. 정규식 길이 1KiB, query의 regex 최대4개. RE2 미지원 구문은400.
- `text(string)`: search_values의 어느 한 scalar에 literal substring. raw 전체 JSON 직렬화에 대한 문자열 검색이 아니다.
- `sattr/iattr/dattr/battr(namespace,pointer)`: 각각 string/integer/double/boolean 타입만 반환. 타입 불일치/missing은 없음. integer→double 자동 coercion 금지.
- `exists(namespace,pointer)`: null 포함 존재. `is_null`은 명시적 null만 true.
- `array_contains(namespace,pointer,literal)`: 타입을 보존한 원소 equality, 한 record당 true/false 하나. array를 펼쳐 count를 증가시키지 않는다.
- 고정 필드 `== != < <= > >=`, 문자열/숫자/bool literal, `&& || !`, 괄호, 제한된 literal `in [...]`만 허용.

missing/type mismatch의 모든 일반 비교는 false, `!=`도 존재하는 올바른 타입에서만 true다. `!predicate`는 이 false를 뒤집으므로 missing이 true일 수 있다. SQL의 NULL 3값 논리가 노출되지 않게 **각 비교 leaf를 COALESCE(...,FALSE)**로 내린 뒤 Boolean을 조합한다. 실제 SQL golden tests로 고정한다.

문자열 equality와 기본 substring은 case-sensitive, Unicode normalization 없음. case-insensitive는 명시적 `icontains`로만 제공하고 DuckDB의 고정 버전 lower semantics를 golden test로 기록한다. 정규식의 `(?i)`는 RE2 semantics다. 자동 stemming, token boundary phrase, fuzzy, BM25는 없음. 인용 문자열의 공백은 literal substring 의미이며 Tantivy phrase와 같다고 하지 않는다.

길이8KiB, AST nodes128, depth16, literal list100개. CEL comprehension/macros(`all`,`map`,`filter`), arbitrary member/index access, 사용자 함수, duration math, dynamic path, eval, 임의 SQL은 거절한다. JSON AST도 같은 validator를 거친다. numeric literal은 signed64 범위를 초과하면 tagged decimal string DTO로 전달하고 iattr 비교에만 DECIMAL로 bind한다.

### 9.2 SQL 보안과 공통 scope

필드명·operator·SQL function은 고정 mapping이며 값은 bind parameters다. URL·table function·column identifier를 사용자 문자열로 조합하지 않는다. `tenant_id/project_id`, time range `[start,end)`, snapshot lane cutoff를 mandatory predicate로 넣는다. `OR true` 같은 식은 scope를 제거할 수 없다. 동적 JSON path도 검증한 namespace+pointer를 bind한다.

API·rows·aggregate·related·Live·alerts는 동일한 typed QueryPlan builder를 사용한다. SQL 원문 endpoint는 없다. DuckDB의 ATTACH/COPY/INSTALL/LOAD/외부 HTTP 등은 내부 operation에서만 실행할 수 있고 사용자 AST에서 도달할 수 없다.

## 10. 분산 실행·집계 정확성

### 10.1 plan과 task

계획은 `snapshot_id, scope, filter AST hash, schema version, projection, sort, aggregation spec, explicit file manifests, estimated bytes, deadline`으로 구성한다. 작업자는 catalog를 임의로 다시 조회해 다른 파일 집합을 선택하지 않는다.

한 작업의 초기 input 목표는 압축64MiB 또는 파일8개, 초기 병렬도는 snapshot당 최대4다. 작은 쿼리는 worker 하나에서 끝낸다. native row-group subset을 확실히 지정할 수 없는 경우 **한 파일을 여러 task에 중복 배정하지 않는다**. partition split 단위는 explicit 파일 집합이고 G00에서 row-group API를 검증한 경우에만 더 작게 나눈다.

`query_tasks`는 UNIQUE(query_id,stage,partition_id), attempt/fence, output checksum을 기록한다. reducer는 한 partition의 성공 attempt 하나만 소비한다. 재전송 결과 중복 합산 금지. timeout/cancel된 query의 늦은 결과를 채택하지 않는다. 큰 partial 결과는 내부 S3 temp 객체로 spill하고 query TTL/GC 대상에 포함한다.

### 10.2 rows

각 task에서 동일 scope/filter/cursor/sort의 `limit+1`만 구한다. reducer는 stable tuple로 bounded k-way merge한다. rows limit 최대1,000, 기본100. 이 Top-K 절단은 row 정렬에는 안전하지만 group-by Top-K에는 적용하면 안 된다. projection은 list용 컬럼만; raw payload는 최종 detail에서만 읽는다.

### 10.3 aggregates

필수 count, sum, min, max, avg, histogram, 최대2개 group dimensions, 최대8개 metrics. DISTINCT/percentile/join/임의 UDF는 v1 API에서 400 unsupported_operation. 향후 approximate 기능은 이름·오차·버전을 별도로 계약한다.

- count는 BIGINT checked overflow, empty0.
- integer sum은 DECIMAL(38,0), overflow422. double sum/avg는 finite 확인, 병합 순서 고정; oracle 허용오차 `max(1e-9,abs(expected)*1e-9)`.
- avg는 partial sum와 valid numeric count를 병합한다. 부분 평균의 단순 평균 금지.
- 모든 NULL/missing numeric 대상만 있으면 sum/min/max/avg=null. 유효 numeric count와 excluded type count를 노출한다.
- histogram은 UTC 정수 μs, request `[start,end)`, bucket start=floor division(time,interval)*interval. negative epoch와 경계 테스트. 빈 bucket 포함 정책을 DTO에 명시한다.
- group key에 namespace/path/type/value를 포함한다. string `"1"`, integer1, double1.0, boolean true, null, missing은 별도다.
- array group은 v1에서 지원하지 않는다. `array_contains` 필터만 지원. 숨은 Cartesian product를 만들지 않는다.
- task는 그룹을 local Top-K로 자르지 않는다. full local groups를 DuckDB partial table로 출력하고 reducer가 다시 GROUP BY한 뒤 최종 Top-K 적용.
- group bucket 최종 hard limit20,000, 중간 bytes hard limit64MiB/query를 초기값으로 둔다. local distinct20,001은 전역도 초과이므로 즉시 오류 가능. 여러 task 중복 그룹은 reducer에서 합친 뒤 최종 개수를 판단한다. 메모리 한도는 DuckDB spill을 사용하고 한도 초과 시 정확한 결과 대신 일부 count를 반환하지 않는다.

## 11. S3 읽기·캐시·DuckDB 격리

초기 의존성 검증 후보: DuckDB `1.5.5`, 공식 Go driver `github.com/duckdb/duckdb-go/v2`의 대응 `v2.10505.0`. 이는 공식 mapping 확인값이며 프로젝트에서 build/test된 lock은 아니다. G00에서 실제 release tag·checksums·amd64/arm64·CGO·extension ABI를 검증하고 lock한다. 2.0 alpha는 사용하지 않는다. Go toolchain은 기존 비교군의 1.26.5를 초기 기준으로 하고 지원성 검증 후 정확한 patch를 고정한다.

native process 초기값: `threads=1`, `memory_limit='256MiB'`, `max_temp_directory_size='2GiB'`, query concurrency1. supervisor 포함 RSS512MiB, CPU1, swap0. Go GOMEMLIMIT만으로 native 메모리가 제한된다고 주장하지 않는다. DuckDB OOM/segfault는 child failure로 처리하고 제한된 task split/retry 후 명시적 실패한다. native fault를 무한 재시도하지 않는다.

json/parquet/httpfs 및 필요한 extension은 build image에 버전 고정해 포함하고 시작 시 offline LOAD한다. runtime INSTALL/자동 외부 다운로드 금지. credentials는 child에 전달하지 않는다.

초기 원격 읽기 구현은 **supervisor의 localhost range gateway + DuckDB httpfs**다. child에는 작업에 허용된 객체만 opaque capability URL로 제공한다. gateway는 manifest의 key/size/checksum을 사용해 S3 Range GET을 수행하며 일반 proxy가 아니다. 임의 host/path를 받아 fetch하지 않는다. HEAD/GET/Range/Content-Range/206/416 동작과 DuckDB read_parquet list binding은 G00 검증 대상이다.

공개 object manifest에 전체 SHA-256과 1MiB fixed block별 SHA-256을 넣는다. gateway는 요청 range를 block 경계로 확장해 검증한 후 필요한 bytes를 전달한다. 마지막 block 길이·ETag 변경·short read를 검사한다. ETag를 content checksum으로 취급하지 않는다. S3 TLS와 object integrity를 구분한다. 추가 읽기량은 비용 계측에 포함한다.

local cache key는 `(installation,object_id,content_hash,block_index)`이며 disk-backed LRU, 동시 miss single-flight, 실행 중 block pin을 둔다. 캐시 path는 사용자 입력에서 만들지 않는다. 전체 파일 다운로드 모드는 fallback 실험으로 비교하되, 기본 partial read가 무의식적으로 full GET이 되면 G00 실패다.

cache/staging/spill의 총 disk limit 기본4GiB, reserve512MiB. worker별 query output·conversion output도 여기에 포함한다. cache가 가득 차면 unpinned부터 회수하고 여유가 없으면 새 task를 받지 않는다. 불변 checksum으로 cold/warm을 구분한다. affinity는 성능 힌트이며 특정 worker 부재가 정확성 실패를 만들면 안 된다.

## 12. 변환·병합 최적화

converter는 journal을 스트리밍 decode하고 accepted selection을 적용해 DuckDB Appender/검증된 bulk API로 넣는다. row마다 INSERT/CGO call 반복은 baseline만 허용하고 최종 구현은 batch append로 줄인다. attrs의 nested append와 decimal 정확성은 G00 필수다. 고정 row batch 초기2,048, large record에서는 byte 기준으로 줄인다.

변환 결과는 stable record_id·lane/seq를 보존한다. mutable Issue status/count는 Parquet에 복제하지 않는다. file statistics는 실제 출력에서 계산해 manifest에 넣으며 추정값을 pruning 근거로 쓰지 않는다.

compaction은 동일 tenant+lane+schema+event-day+kind의 공개 bundles를 대상으로 한다. 입력 목록과 generation을 예약하고 unbounded merge를 하지 않는다. analytics와 payload의 record 집합 동일성을 hash/count로 검증한다. snapshot 기준 입력이 여전히 현재인지 CAS한 뒤 old `valid_to_generation`, new `valid_from_generation`을 한 PG transaction에서 바꾼다. late 데이터가 추가돼도 예약하지 않은 파일을 retire하면 안 된다.

compaction은 published_seq를 바꾸지 않는다. 실패 결과는 orphan이며 정상 입력은 계속 검색 가능하다. small-file count만 보지 않고 크기·조회율·backlog·예상 GET 절감을 보고 제한된 maintenance budget에서 실행한다. ingest/query backlog가 높으면 중지한다. 모든 프로젝트를 주기적으로 재작성하지 않는다.

## 13. SDK 상세 정보와 UI

오류 상세에는 exception chain, mechanism/handled, SDK frame 순서(원본 순서 유지), in_app, context line, threads, breadcrumb, release/env, request/user/context, normalization warnings를 표시한다. stacktrace 없는 captureMessage도 정상 Issue다. event.time과 received.time을 구분한다.

transaction 내부 spans는 상세 JSON/기본 리스트에서 제공하고 span 통계와 로그 count에 자동 포함하지 않는다. Structured Log 상세에는 typed attrs/unit/template/parameters, severity 원문, trace/span, SDK 정보를 표시한다. body와 template를 합쳐 새 메시지를 재포맷하지 않는다.

관련 로그는 같은 권한 scope 안에서 trace_id를 우선하고, 없으면 project+service+좁은 시간으로 명시적 fallback한다. trace ID가 있다고 다른 프로젝트 권한을 획득하지 않는다. source map은 v1에서 해석하지 않고 원본 SDK 프레임을 표시한다. debug_meta는 보존한다.

UI 필수 화면: setup/login, projects/DSN/revoke, Issues 목록·상세·상태, Logs/Live, Explore/histogram, alert rules/deliveries, system(수신·공개 지연/SDK discard/unsupported/resource/storage/backup). SDK 설치 예는 실제 고정 버전 fixture와 일치해야 한다. 기능 미지원 메뉴를 동작하는 것처럼 표시하지 않는다.

React Query는 서버 상태, URL은 검색 상태, 로컬 state는 form/view만 담당한다. OpenAPI에서 DTO를 생성한다. i64는 string; raw/message는 text rendering, HTML 삽입 금지. rows와 histogram은 같은 read_token/absolute range를 재사용한다.

## 14. Issue grouping·순서·resolve

SDK ingestion 호환과 Sentry 서버 grouping의 bit-for-bit 동등성을 구분한다. v1은 `eventglass-grouping-v1`을 표시한다.

default fingerprint 계산: exception.values의 마지막 원소를 대표 exception으로 삼고 chain type은 전달된 배열 순서를 유지한다. scrubbed chain type들과 대표 exception의 frames 중 in_app=true를 우선해 마지막8개 `(module,function,filename)`를 원래 순서로 선택; 없으면 마지막8개 전체 frame. line/col/주소는 기본 fingerprint에서 제외. frame이 없으면 exception type+value, exception도 없으면 message_template → message 순서. path는 slash만 정규화하고 basename 절단·숫자 정규식 제거 같은 공격적 추측은 하지 않는다.

explicit fingerprint string[]는 그대로 사용하되 `{{ default }}`/`{{default}}` 위치에 계산된 default 구성요소를 확장한다. 빈 배열은 default. group canonical JSON bytes+version+project를 SHA-256. 알고리즘 변경은 새 version이며 기존 Issue를 재처리만으로 재분류하지 않는다.

group canonical 표현은 배열 기반 JSON으로 필드 순서를 고정하고 UTF-8·escape 규칙을 golden bytes로 잠근다. `issue_id`는 이 전체 SHA-256의 lowercase hex다. 변환 시 이미 계산 가능하며 Publish는 같은 ID로 upsert한다. auto-increment Issue ID를 Parquet 생성 후 뒤늦게 끼워 넣지 않는다.

Issue count는 unique error occurrence만 센다. first/last는 `(event_time_us,ns_remainder,record_id)` min/max, first/last release는 해당 record의 값(null 포함)이다. UI activity는 received-time을 사용한다.

resolve는 project의 모든 lane accepted_seq vector를 같은 PG transaction에서 캡처해 `resolved_cut`에 저장한다. 수신 Accept와 resolve는 관련 lane lock으로 선후를 정한다. publish 시 transaction 현재 Issue 상태를 읽고 `(record.lane,record.seq)`가 resolved_cut를 넘는 unique 오류일 때만 regression한다. resolve 전에 ACK된 backlog로 다시 열리지 않는다. ignored 상태는 자동 해제하지 않는다.

여러 lane의 같은 Issue는 Issue row lock으로 상태 변경을 직렬화한다. regression delivery는 상태 전이 revision을 unique key로 사용한다. occurrence 처리 순서와 event-time min/max를 혼동하지 않는다. 오류 이벤트가 신규인지 판단하는 dedupe는 accept 단계, Issue 반영의 재시도 방지는 occurrence UNIQUE 및 publish transaction으로 처리한다.

## 15. Live·경보·SDK 손실 진단

Live는 event-time이 아닌 received-time + lane position vector를 따른다. 변경 notification은 힌트이며 DB cut을 polling해도 누락 없이 따라잡아야 한다. 새로운 공개 cut까지 공통 filter로 조회하고 각 lane별 scan 완료 위치를 SSE resume token에 기록한다. 서로 다른 lane의 전역 시간 순서는 보장하지 않으며 UI 정렬과 resume completeness를 구분한다.

SSE buffer256KiB/connection, API당32 connections, heartbeat15s. initial catch-up15분, 최대10,000행/10초 초과는 `resync_required`. match0이어도 scan checkpoint를 보낸다. slow client 때문에 worker lease를 무한 연장하지 않는다.

경보는 new Issue/regression 또는 received-time threshold. threshold는 60초 aligned end E, `[E-window,E)`. 평가 예약 시 accepted_seq vector를 저장하고 모든 lane published_seq가 cut에 도달한 뒤 공통 query 실행. 지연된 lane을 0건으로 간주하지 않는다. 뒤처진 창은 bounded catch-up으로 순서대로 처리한다.

예약 transaction은 대상 lane을 정렬된 순서로 잠그고 E가 DB 현재 시각 이하인지 검증한다. cut을 읽는 동시에 각 lane의 last_received_time_us를 최소 E로 전진시킨다. 이후 Accept는 received>=E가 된다. 기존 Accept가 lock을 보유했다면 commit을 기다린 뒤 cut에 포함한다. 이 barrier와 §4.1의 시각 부여 없이 arrival-time만으로 창 완결성을 주장하지 않는다.

`(alert_id,revision,E)` unique evaluation, query 성공 뒤 revision 재검사·cooldown·outbox·last_completed_E를 transaction 확정. cooldown은 E 기준. 실패/부분 결과에는 진행 위치를 전진시키지 않는다. webhook은 stable delivery ID, at-least-once이며 exactly-once 외부 전달 보장 없음. 10초 timeout, response64KiB, redirect 없음, 로컬 테스트 수신기, SSRF/DNS pinning, 사용자 설정 목적지만 허용한다. 재시도는 network/408/429/5xx, 최대12회,5초..1시간 jitter.

client_reports는 SDK가 보내기 전에 잃은 데이터의 approximate 통계다. receipt/item 내 중복은 막되 서로 다른 HTTP 재전송의 client report를 정확히 dedupe한다고 하지 않는다. 서버 거절 수·SDK reported drop·accepted·published 수를 각각 표시하고 합계를 정확한 generated total이라고 부르지 않는다.

## 16. 보존·GC·백업·재난 복구

초기 이벤트 보존30일(received-time 기준), receipt/dedupe는 보존+7일. event-time이 오래됐다는 이유로 방금 ACK한 기록을 즉시 지우지 않는다. 혼합 retention 파일은 compaction에서 row별 만료를 적용하고 새 generation으로 교체한다. 모든 조회에는 snapshot에 고정된 retention cutoff를 적용한다. snapshot 기간 중 보이는 행이 바뀌면 안 된다.

GC 삭제 조건은 모두 만족해야 한다: 현재 catalog에서 미참조, active query/detail lease에서 미참조, 복구 가능 PG backup/PITR window에서 미참조, object intent/job에서 미참조, safety grace 만료. v1 PG PITR 목표7일이면 retired object의 최소 유예는8일이며 **journal에도 적용**한다. 더 긴 backup 보존을 설정하면 object 유예도 함께 늘린다. 이 추가 저장비를 계측한다. S3 lifecycle을 공개/복구 객체에 독립적으로 걸지 않는다.

PG는 base backup+WAL archive를 S3의 별도 backup prefix에 보관하는 검증된 도구(pgBackRest 등)를 사용한다. live prefix data 삭제 권한과 backup 권한을 분리한다. SDK public key는 백업 접근 권한 없음. 매일 격리 restore rehearsal, latest successful backup/WAL archive lag/restore age 진단.

worker loss: lease 만료 후 재시도, ACK 데이터는 S3+PG에서 복구. 전체 compute loss: 빈 cache로 재기동. PG loss: backup/PITR 복원 후 그 cut에 참조된 journals/catalog 검증, pending job 재시도. snapshot보다 새 S3 파일을 LIST해 자동 채택하지 않는다. **PG 마지막 durable recovery point 이후 ACK는 S3 객체가 있어도 완전한 Issue/권한/receipt 상태를 자동 복구한다고 주장할 수 없다.** 실제 backup RPO와 HA 복제 RPO를 별도 표시한다.

복원 시 storage_generation 증가, sessions와 cursor 무효화, 모든 임대 fence 증가, outgoing alert sender는 정합성 확인 전 정지. 사용자 role·scrub 정책·outbox·event dedupe도 복원 대상이다. 객체 missing/checksum mismatch는 unhealthy 및 repair이며 빈 결과로 숨기지 않는다.

물리 개인정보 삭제를 보존/PITR보다 우선하는 purge는 v1 범위 밖이다. 향후 추가 시 백업·journal·cache·Parquet까지 계약을 정의해야 한다. 단순 UI 숨김을 영구 삭제라 부르지 않는다.

## 17. 자동 운영·성능 예산

사용자 기본 입력은 DB DSN, S3 endpoint/bucket/prefix, public URL, 초기 관리자 bootstrap, 보존 기간, 실행 자원 상한이다. shard·file size·cache ratio를 사용자가 수동 튜닝해야만 정상 동작하면 완료가 아니다. advanced override는 진단용이며 기본값이 검증되어야 한다.

| 자원 | 초기 정책 |
|---|---|
| API/worker 단위 | 1 vCPU/512MiB, swap0 |
| API ingress admission | 총64MiB, 동시 decode2 |
| worker native 동시성 | 1 child task |
| worker PG connections | 최대2, API8, scheduler2 |
| 전체 DB pool budget | 배포당64 기본, autoscaler가 초과 replica 생성 금지 |
| lease/heartbeat | 60s/15s |
| native query deadline | interactive30s, async5분 |
| snapshot TTL | 15분, active 최대1시간 |
| shutdown grace | 30s, ACK 여부와 job completion을 구분 |

오토스케일러 입력은 작업 개수가 아니라 estimated bytes/CPU cost, oldest age, worker 서비스율 EWMA, 검색 queue delay다. startup 시 prior service rate를 사용하고 zero worker 상태에서 계산이 막히지 않게 한다. ingress와 대화형 query pool은 최소1을 기본으로 하며 burst loss/cold start를 줄인다. maintenance는0까지 축소 가능하다.

초기 제어식은 `desired = ceil(queued_work / (target_drain_seconds * measured_work_per_worker_second))`를 각 pool의 min/max로 clamp. oldest age/SLO breach는 별도 확장 신호. target drain ingest5초, search queue0.5초, maintenance300초부터 검증한다. 10초 관측, scale-out 두 연속 sample, scale-in300초 안정화, 한 번에25% 이내 축소. 구체 control policy는 측정으로 바꿀 수 있지만 oscillation·DB connection 폭증 방지 테스트는 필수다.

PG/S3 지연·429/5xx가 높으면 worker 추가로 증폭시키지 않고 dependency circuit/admission으로 제어한다. query fair queue는 tenant별 동시성/bytes budget, maintenance는 spare capacity의 최대20%를 초기값으로 둔다. 단일 고비용 tenant가 ingest를 막지 않도록 pool과 priority를 구분한다.

scale-in은 worker draining 표시→신규 claim 중지→작업 완료/안전한 포기→종료. SIGKILL은 동일 lease recovery로 안전해야 한다. 작업자 추가·삭제에 데이터 이동/재색인 절차가 필요하면 설계 위반이다. 캐시 재가열 비용은 감수하되 affinity/hysteresis로 최소화한다.

## 18. 배포·버전·보안 경계

AWS: S3, PostgreSQL managed HA/PITR, EC2 기반 컨테이너. Kubernetes 사용 시 HPA/KEDA+node provisioner, ECS 사용 시 별도 adapter를 통해 동일 desired capacity 지표를 노출한다. **v1 배포 자동화는 local Compose와 Kubernetes/KEDA 두 경로만 구현**하고 ECS adapter는 후속이다. 모든 provider adapter를 동시에 구현하지 않는다.

자체 서버: 같은 images/SQL/schema, PostgreSQL, S3 compatible storage, 기존 cluster나 Compose. Compose 단일 host는 수평 worker 확장 테스트는 가능하지만 host HA나 물리 서버 자동 증설을 제공하지 않는다. k3s 등 cluster의 자원 내에서 autoscale한다. 객체 저장소 자체 운영비·복제·disk를 총비용에 포함한다.

S3 호환 계약: SigV4, path-style/virtual-host-style 설정, GET/HEAD/Range, PUT, multipart complete/abort, paginated LIST, delete, TLS/CA, object read-after-write 검사. provider ETag/checksum 차이는 adapter에서 처리하고 correctness는 자체 SHA-256으로 판단한다. S3 conditional write를 분산 lease로 사용하지 않는다.

기존 repo의 pinned MinIO는 Rust 비교 fixture로 보존한다. Go 테스트도 그 호환 경로를 지원하되 archived community 배포를 신규 production 기본값으로 추천하지 않는다. 자체 호스팅 기본 후보는 Garage이며 G00 S3 계약·release provenance·복구 검증을 통과한 정확한 release/image digest만 배포 lock에 넣는다. 통과하지 못하면 MinIO로 몰래 대체해 production-ready라고 표시하지 않는다. 실제 AWS S3+최소1 self-host backend 통과가 출시 조건이다.

지원 build는 Linux amd64/arm64 우선. DuckDB CGO/native binary/extension ABI를 이미지 안에서 고정하고 SBOM/notice/checksum을 생성한다. runtime 인터넷 package download 금지. production user non-root, readonly root filesystem, scoped scratch, localhost child endpoint, private PG, narrow S3 prefix 권한.

관리 인증은 password hash(검증된 Argon2id 구현 및 메모리 budget), random sessions, Secure/HttpOnly/SameSite cookies, CSRF, login rate limit. setup은 one-time token, hash only. query worker는 공개 인터넷에 노출하지 않는다. executor job은 인증된 내부 API/DB claim에서만 생성 가능하다. query tenant scope와 gateway object allowlist를 이중 검사한다.

권한은 tenant admin, project operator, project viewer다. admin만 사용자·membership·프로젝트·키·보존·scrub 설정을 변경한다. operator는 배정 프로젝트 조회 및 Issue 상태/alert 변경, viewer는 조회만 한다. principal의 project_ids와 요청 project_ids의 교집합으로 조용히 축소하지 말고 미허가 프로젝트가 하나라도 있으면403. system 전체 진단은 admin 전용, alert 전송 destination 설정도 admin 전용이다. 모든 mutation에는 CSRF와 revision 검사를 적용하며 ingestion public key와 관리 session을 혼용하지 않는다.

초기 환경변수 계약: `EVENTGLASS_DATABASE_URL`, `EVENTGLASS_S3_ENDPOINT`(AWS면 생략), `EVENTGLASS_S3_REGION`, `EVENTGLASS_S3_BUCKET`, `EVENTGLASS_S3_PREFIX`, `EVENTGLASS_PUBLIC_URL`, `EVENTGLASS_ROLES`, `EVENTGLASS_SCRATCH_DIR`. S3 인증은 AWS default credential chain/workload identity 우선, 자체 호스팅 secret은 환경/secret mount로 공급한다. PG/S3 secret을 CLI 인자·문서 예제 실값으로 기록하지 않는다. 불변 installation ID와 topology/schema는 DB 정본을 따르며 잘못된 bucket/installation 조합이면 시작 실패한다.

## 19. API 최소 계약

신규 UI/API prefix는 `/v1/`, Sentry 수신 path는 §3 유지. OpenAPI를 구현 시작 때 생성하고 버전 관리한다.

| endpoint | 계약 |
|---|---|
| GET /livez, /readyz | 생존과 core 의존성 readiness 구분; 단순 backlog는 system 상태 |
| POST /v1/setup, /v1/sessions | 관리자 bootstrap/login |
| /v1/projects, /v1/projects/{id}/keys | 목록/생성/revoke; admin만 mutation |
| POST /v1/search | scope, expression/filter, projection, limit, cursor/read_token |
| POST /v1/aggregate | 동일 scope/token + metrics/group/histogram |
| GET /v1/records/{id} | snapshot 또는 권한 검증 후 현재 catalog detail |
| GET /v1/live | SSE, scope filter, vector resume token |
| /v1/issues, /v1/issues/{id} | query/detail/status mutation, revision conflict409 |
| /v1/alerts, /v1/deliveries | 규칙·결과·재시도 |
| GET /v1/query-jobs/{id} | async 상태·완료 결과; owner/scope 검증 |
| DELETE /v1/query-jobs/{id} | 취소 및 lease/output 정리 |
| GET /v1/system | accepted/published/backlog/SDK losses/limits/storage/backup |

SearchRequest는 project_ids[], absolute start/end, time_basis(event/received), kind[], expression 또는 filter, read_token?, cursor?, limit, mode(auto/sync/async). 기본시간은 UI가 absolute로 계산해 전달. mode=auto는 비용 추정이 interactive budget을 넘으면202/query_id. mode=sync 실패는429/503/timeout이며 부분 결과200 금지.

SearchResponse는 rows, read_token, next_cursor, complete, stats(scanned_bytes,objects,cache_bytes,elapsed_ms,cut,visibility_lag), warnings. Error DTO는 code/message/retryable/request_id, 내부 SQL·keys·raw 제외. 모든 i64/count/decimal integer string. runtime limit·unsupported vs empty를 UI에서 구분한다.

## 20. 필수 관측값과 성공 목표

필수 metrics: supported/unsupported items, SDK reported drops, server rejects, accepted unique/duplicate records, ACK latency, accepted→published lag, per-lane oldest backlog, converted/compacted bytes, S3 operations/bytes/retries, cache hits, scanned rows/bytes, result bytes, DuckDB spill/RSS/OOM, PG txn duration/deadlock/pool waits/WAL bytes, job attempts/fenced rejects, query incomplete/errors, backup lag, GC candidates/deletes.

고카디널리티 project/record/query ID를 Prometheus label로 넣지 않는다. tenant별 비용은 bounded rollup을 별도 관리한다. query body·message를 observability log에 남기지 않는다.

아래는 **달성했다고 주장할 수 없는 초기 검증 목표**다.

- worker1 CPU1/512MiB에서 정상 SDK workload 100 logs/s+5 errors/s,30분 지속 수집+검색 동안 backlog가 지속 상승하지 않을 것.
- S3 durable ACK p95는 네트워크와 microbatch 대기를 포함해 초기500ms 목표. 기존 Rust local SQLite ACK50ms와 동일 비용/내구성이라고 비교하지 않는다.
- 서버 ACK→검색 공개 p95 5초 목표. SDK 내부 batch 대기시간은 별도로 측정.
- 최근15분·프로젝트 범위 warm rows/히스토그램 p95 500ms 목표. cold 및 전기간 regex는 별도 workload로 보고하고 같은 SLA라 부르지 않는다.
- 1→2→4 worker에서 독립 batch 처리량이 증가해야 하며 공유 DB/S3 포화 전 효율을 보고한다. exact linear scaling은 보장하지 않는다.
- OOM/ACK 유실/무음 누락/권한 노출은 실패. 리소스 오류를 적절히 반환해도 목표 처리량 미달은 별도 실패다.

## 21. Rust와의 비교 계약

테스트 모드 A는 core normalize/query oracle 비교, 모드 B는 실제 end-to-end S3 durability 비교다. Rust 원본을 Go에 맞추기 위해 수정하지 않는다. 두 제품이 지원하는 공통 기능과 한쪽만 지원하는 기능을 분리한다.

- 기존 10k/100k/1m/10m seed와 실제 SDK fixture를 사용하고 ID·timestamp·records 크기·attribute cardinality·error 비율을 동일하게 만든다.
- Rust Tantivy phrase/token semantics와 Go literal substring이 다른 쿼리는 동일성 집계에 넣지 않고 별도 표로 표시. 공통 exact filters/time/count는 독립 oracle와 양쪽 비교.
- 오류 event duplicate와 ID 없는 log 재전송, same timestamp, 늦은 이벤트, 빈/missing/null, dotted/nested keys, mixed numeric types를 포함.
- release build를 별도 완료한 뒤 실행. 동일 Linux architecture/cgroup CPU/RAM/swap, filesystem 및 네트워크 지연, cache 조건, query concurrency, 수집 지속시간으로 비교.
- Go API/worker별512MiB와 전체 설치(추가 PostgreSQL·S3 compatible service)의 CPU/RAM을 둘 다 보고. Rust에 없는 공유 구성요소 비용을 제외한 숫자만으로 우위 주장 금지.
- 5분 warmup→30분 지속 수집+mixed queries→10분 drain. 1/2/4 worker, stop/start/SIGKILL, 저유량 idle, 대형 regex, cold cache를 별도 실행.
- 비용식은 compute-seconds+GB-month(storage/journal/backup/index)+S3 PUT/GET/LIST+전송+PG/PITR+자체 저장소 자원. 실제 가격·지역·날짜를 parameter로 기록; 임의 월 비용 확정 금지.
- 보고 JSON에는 revision/dirty state, toolchain/lock versions, native engine version, exact query semantics, dataset checksum, cgroup peak/OOM, ACK+visibility+query percentiles, scans/network/PG bytes, completeness를 포함.

기존 `go/benchmark/`는 SQLite FTS5 실험이며 신규 DuckDB 결과로 재사용하지 않는다. 기존 Rust 전역 seq는 Go vector position과 1:1 비교하지 말고 외부 record identity와 검색 결과로 대조한다.

## 22. 구현 순서와 완료 게이트

각 단계는 이전 계약을 유지하며 실제 검사 결과를 커밋 본문에 기록한다. 단계별 실행 일지 파일을 늘리지 않는다. 미구현 검사 명령이 PASS하도록 stub을 만들지 않는다.

### G00 — 격리·버전·엔진/S3 기술 계약

산출물: 독립 go.mod/go.sum, 고정 toolchain·native/extension·image lock, CLI skeleton, scripts/check, Compose PG+S3, fixtures manifest, baseline hashes.

필수 검증: Go driver build Linux amd64/arm64, nested attrs/DECIMAL exact roundtrip, memory cap+spill+cancel/kill, parameterized read_parquet list, localhost range gateway 실 Range GET 및 block checksum, Zstd/row group metadata, remote object permissions, network 없는 startup. PG migration/rollback/checksum. localhost S3와 실제 AWS 계약 검증을 구분.

실패하면 라이브러리 버전/지원 API를 확인하고 근거를 기록한다. 직접 codec/SQL parser로 대체하거나 성능 결과를 추측하지 않는다.

### G01 — SDK 계약·fixture·정규화

산출물: envelope/store adapters, typed canonical DTO, scrub, IDs, fixed normalization golden fixtures, SDK compatibility report.

Python2.69.0/Node+Browser10.73.0/Go0.49.0의 실제 HTTP를 localhost recorder/실제 Go API로 검증. 기존 Go fixture에는 errors만 있으므로 **새 Go structured logs fixture를 반드시 추가**. source inspection은 통과로 세지 않는다. request compression/versionless container/mixed logs+event/binary unknown/headers precedence/CORS/flush/rate-limit/client-report/fork/trace scope/large int/arrays/unit/unicode를 포함.

Java/.NET 등 추가 SDK는 정확한 버전·소스·wire fixture가 추가되기 전 미검증으로 표시한다. 전 언어 전체를 검증했다고 쓰지 않는다. 사용자에게 추가 SDK 우선순위를 다시 묻지 않고 먼저 위4 runtime의 필수 계약을 닫는다.

### G02 — S3 ACK·PG 수신·lease

산출물: intent/receipt/dedupe/lane/job SQL, Accept operation, byte admission, graceful drain.

필수 검증: object upload 직전/직후, Accept commit 직전/직후, reply loss, same source ID concurrent ingest, ID 없는 동일 로그 두 요청, revoke/scrub update 경합, PG down/S3 down, GC/late PUT race. 성공 ACK oracle는 부모 프로세스가 기록하고 복원 결과와 대조.

### G03 — Parquet 공개·Issue

산출물: conversion worker/native child, analytics/payload bundle, fenced publish, grouping/resolve/outbox.

필수 검증: out-of-order conversion, contiguous publication, poisoned batch, 중복 publish, analytics/payload mismatch, resolve와 backlog/새 오류 경합, multi-lane same Issue, received/event time 차이, 전체 local disk 삭제 후 재실행.

### G04 — 공통 검색·snapshot·집계

산출물: CEL subset/JSON filter validator, SQL lowering, scoped snapshot/cursor, rows/detail/histogram/group, query_tasks/reducer.

필수 검증: 독립 기대 결과, typed/missing/null·literal dotted vs nested·Unicode·LIKE escaping·regex 오류, predicate leaf NULL semantics, 권한 OR bypass, token 변경/revoke, local Top-K 밖 global winner, weighted avg,20,000/20,001 groups, empty metrics, timeout·partial failure·retry 중복 결과, compaction 중 pagination.

### G05 — UI·Live·alerts·SDK diagnostics

산출물: 독립 OpenAPI/UI, issue/log/Explore/system 화면, Live vector, threshold cut, local webhook harness.

필수 검증: 실제 browser SDK에서 오류·로그 수신 후 UI 표시, 로그 error가 Issue를 만들지 않음, breadcrumb 중복 row 없음,0-match Live progress, 느린 client/revoke, pending ACK backlog 때문에 false alert가 생기지 않음, HTML injection 및 CSRF.

### G06 — 병합·보존·복구

산출물: compact/retain/GC, object block cache, PostgreSQL backup/PITR runbook/자동 점검, restore command.

필수 검증: snapshot lease 등록 vs GC, 교체 전/후 프로세스 crash, mixed retention rewrite, last block checksum, late upload, pg snapshot+WAL+S3 full restore, 새로운 객체 잘못 채택하지 않음, backup window 내 journal 보호, 누락 파일 fail closed.

### G07 — 자원·확장·비교

산출물: Linux resource harness, reproducible comparison report, Kubernetes/KEDA 배포, bounded autoscale controller metrics, 최소 warm pool.

필수 검증:1 CPU/512MiB OOM 여부,1/2/4 worker mixed workload, deadline 내 cancel 후 native process 정리, scale-in/SIGKILL/PG pool budget, dependency slowdown에서 증설 폭주 없음, tenant fairness, cache rewarming 비용, fixed budget에서 명시적 거절. 목표 미달은 미달로 보고.

### G08 — 출시/운영 handoff

필수: 실제 AWS+S3 compatible 양쪽 smoke/restore, SDK 지원표와 미지원 기능 명시, reproducible amd64/arm64 image/SBOM, secret scanning, upgrade migration/backward schema read, rollback binary compatibility, admin guide, 샘플 DSN·retention/resource default, license notices. Rust 운영 교체는 별도 요청 전 수행하지 않는다.

구현할 명령 이름(현재 존재한다고 가정하지 말 것):

```text
./scripts/check unit
./scripts/check contracts
./scripts/check sdk
./scripts/check integration
./scripts/check crash
./scripts/check web
./scripts/check resource
./scripts/check scale
./scripts/check comparison
./scripts/check release
```

모든 명령은 이 디렉터리 기준. 부수 테스트 데이터는 OS 임시 디렉터리/임시 DB/임시 bucket-prefix, 실행 종료 시 검증된 owned targets만 회수한다. 실제 AWS gate만 외부 인프라를 사용하며 운영 bucket/alert 금지.

## 23. 실패 주입 필수 목록

| 지점/상황 | 반드시 관측할 결과 |
|---|---|
| journal upload만 성공 | ACK 없음, referenced receipt 없음, orphan 회수 가능 |
| Accept COMMIT 후 HTTP 연결 유실 | 내부 재조회는 동일 receipt, SDK 새 로그 재전송 중복 가능성은 명시 |
| worker lease 만료 후 결과 도착 | fence로 publish 거절 |
| conversion N+1 완료/N 실패 | N+1 prepared, lane public cut 정지, 다른 lane 정상 |
| Publish COMMIT 직후 crash | 결과·Issue·outbox·watermark 한 번 반영 |
| snapshot과 compaction/GC 경합 | 고정된 결과 유지 또는 명시적 expired, silent loss 없음 |
| reducer partition 중복 수신 | count 한 번만 반영 |
| query task missing/403/corrupt | complete=true 성공 반환 금지 |
| child OOM/segfault/timeout | API process 유지, query 실패 또는 bounded retry, permit 회수 |
| bucket/PG 인증 실패 | 빈 초기 설치로 오인 금지 |
| SDK429/network failure | 클라이언트 drop/재시도를 실제 캡처, server loss와 구분 |
| PG PITR 복구 | 복구 cut과 RPO 표시, generation/token 변경, object refs 일치 |

## 24. 변경 가능 정책과 고정 불변조건

튜닝 가능: batch 대기/크기, 파일/row-group 크기, compression level, cache/spill budget, worker 수, autoscale hysteresis, SLA 목표(측정 보고와 제품 정책 변경 필요).

튜닝 불가: ACK 이전 정본 확정, tenant 권한, SDK typed value 의미, 오류/로그 구분, 정확한 집계, snapshot completeness, fence/중복 공개 방지, 복구 가능한 객체 보호, 미지원 기능의 정직한 표기.

설계에서 승인되지 않은 자동 schema promotion/FTS 추가/엔진 교체/외부 broker 도입은 먼저 measured bottleneck과 비용을 문서화한다. 단순 느림을 이유로 결과 semantics를 바꾸지 않는다. 현재 기술 gate가 실패해 진행할 수 없다면 실패한 구체 계약을 보고하며, "DuckDB로 다 된다" 혹은 "DuckDB로 검색 불가" 같은 포괄적 결론으로 바꾸지 않는다.

## 25. 근거와 확인 수준

SDK의 고정 commit별 근거는 SDK-SOURCES.md. 라이브러리 문서는 아래 공식 자료를 기준으로 읽었다. 문서 지원과 프로젝트 실행 검증은 별개다.

- [DuckDB 공식 Go driver와 engine version mapping](https://github.com/duckdb/duckdb-go)
- [DuckDB Parquet projection/filter pushdown](https://duckdb.org/docs/stable/data/parquet/overview)
- [DuckDB Parquet row group 정책](https://duckdb.org/docs/current/data/parquet/tips)
- [DuckDB pattern/RE2 semantics](https://duckdb.org/docs/current/sql/functions/regular_expressions)
- [DuckDB 메모리 제한과 OOM](https://duckdb.org/docs/current/guides/performance/oom)
- [DuckDB FTS 자동 갱신 제한](https://duckdb.org/docs/lts/core_extensions/full_text_search)
- [PostgreSQL locking](https://www.postgresql.org/docs/17/explicit-locking.html)
- [S3 consistency](https://aws.amazon.com/s3/consistency/)
- [KEDA PostgreSQL scaler](https://keda.sh/docs/2.20/scalers/postgresql/)
- [Fargate CPU/RAM 조합](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-tasks-services.html)

최종 구현 완료는 G00–G08 검증 결과가 있을 때만 선언한다. 이 문서는 그 결과를 대체하지 않는다.
