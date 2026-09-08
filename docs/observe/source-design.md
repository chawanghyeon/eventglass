# Observe 개선 최종 설계서

작성 기준: 2026-09-08  
문서 상태: 구현용 설계안. 서버 구현·Sentry 종단 간 테스트·Tantivy 장애 주입·성능 검증을 완료했다는 뜻은 아니다.  
검토 원본: `붙여넣은 마크다운(1)(9).md`의 「Observe 최종 설계서」

> Sentry SDK를 연결하여 Error Tracking과 Application Log 검색을 제공한다. Rust 프로세스 하나, SQLite 하나, Tantivy, 선택적 S3로 구현한다. 검색 문법·검색 엔진·WAL·범용 집계 엔진은 새로 만들지 않는다.

---

## 1. 이번 개정에서 확정한 결정

기존 제품 목표와 v1 기능 범위를 유지한다. 변경하는 것은 주로 구현 방법과 잘못된 보장이다.

| 항목 | 확정 결정 |
|---|---|
| 배포 | Rust 바이너리 하나. 외부 DB·큐·검색 서버 없음 |
| 영속 데이터 | SQLite: 관리 상태·수신함·Error 식별자. Tantivy: 검색 가능한 Record. S3: 선택적 아카이브·복구 지점 |
| 검색 | **Tantivy QueryParser의 기존 문법 그대로 사용. Observe 자체 문법·lexer·AST·compiler 금지** |
| 일반 사용자 검색 | 검색창과 시간/서비스/레벨 등의 필터 UI. 복잡한 식은 기존 Tantivy 문법으로 입력 |
| 집계 | Tantivy의 집계 API와 intermediate result 병합 사용. 자체 집계 엔진 금지 |
| 수신 | 검증·정규화·마스킹 후 SQLite에 저장하고 ACK |
| 인덱싱 | 순차 Indexer 하나. Tantivy commit payload로 처리 경계를 기록 |
| 재처리 | 단일 진행 위치로 복구. Record마다 검색하여 중복을 확인하는 복구 절차 제거 |
| Shard | 생성부터 고정 디렉터리 사용. Active 디렉터리를 sealed 디렉터리로 옮기는 절차 제거 |
| 로컬 보관 | sealed 데이터와 cold cache를 같은 디렉터리·같은 회수 정책으로 관리 |
| S3 복구 | SQLite와 sealed shard가 일치하는 **완료된 복구 지점**만 자동 복구 |
| 성능 | 512 MiB는 검증 목표. 요청 개수가 아니라 실제 메모리·디스크 예산으로 제한 |

원문의 5분마다 독립적으로 SQLite를 올리는 정책은 변경한다. 연속 수집 중 전체 복구 지점은 shard seal 경계에서 만든다. 기본 seal 기준은 256 MiB 또는 1시간이다. 새로운 인덱싱이 없고 관리 정보만 바뀌었을 때는 5분 주기로 같은 데이터 경계의 관리 정보 백업을 갱신할 수 있다. 따라서 **항상 5분 RPO라고 설명하지 않는다.** 일관되지 않은 복구를 숨기지 않고, 별도의 복제·재구축 엔진을 추가하지 않기 위한 결정이다.

## 2. 만드는 제품과 만들지 않는 제품

Observe는 단일 노드 self-hosted Error Tracking + Application Log Debugging 서버다.

필수 기능은 공식 Sentry SDK의 Error/Event 및 Structured Log 수집, Issue grouping·상태·regression, JSON 내부 텍스트 검색, 구조화 필터, Boolean·phrase·prefix·range 검색, 시간 정렬과 페이지네이션, 집계와 histogram, Live Logs, Error와 관련 로그 연결, 경보, 자동 rollover, S3 장기 보관·cold search, 자동 디스크 관리, SQLite 백업·장애 복구, 관리 UI다.

전체 Elastic Stack이나 전체 Sentry 기능을 구현하는 것은 아니다. 원문과 동일하게 APM span 저장, distributed tracing UI, metrics TSDB, profiling, Session Replay, native minidump 처리, symbol server, 완전한 source-map 처리 파이프라인, 범용 Kibana dashboard builder, Elasticsearch API 호환, SQL 검색, 클러스터·HA·zero-RPO 복제는 v1 범위 밖이다. `trace_id`와 `span_id`는 상관관계를 위해 저장한다.

SDK에서 지원하지 않거나 애플리케이션이 생성하지 않은 로그를 서버가 만들어낼 수는 없다. 기존 logger의 레벨, SDK logging integration, worker 종료 시 flush는 애플리케이션 측 설정이다. 호스트의 모든 stdout이나 컨테이너 로그까지 자동 수집한다고 설명하지 않는다.

## 3. 기술 스택과 실행 구조

Backend는 Rust stable, Axum, Tokio, Serde/serde_json, Tantivy, rusqlite, AWS SDK for Rust, tar, zstd, tracing을 사용한다. Frontend는 React, TypeScript, Vite다. 빌드된 정적 파일은 바이너리에 포함한다.

검토한 Tantivy API 기준은 0.26.1이다. 실제 구현은 `Cargo.lock`으로 정확한 버전을 고정한다. 운영 환경에서 floating `latest`에 의존하지 않는다. Tokio와 라이브러리 내부 스레드는 허용하지만 서버 프로세스는 하나다.

```text
Sentry SDK
    │
    ▼
HTTP: 인증 → 크기 제한 → protocol 검증 → 정규화·마스킹
    │
    ▼
DbWorker: SQLite Inbox에 durable commit ──→ ACK
    │
    ▼
Indexer 한 개
    ├─ Tantivy commit + 처리 경계
    ├─ SQLite Issue/식별자/경보/진행 위치 확정
    └─ Reader 공개 + Live 알림
          │
          ▼
고정 경로의 Active → Sealed shard
          │
          ▼
Storage maintenance: 아카이브·복구 지점·디스크 회수
          │
          ▼
선택적 S3

API / UI / Alerts / Live catch-up → 공통 Search 함수 → Tantivy
```

독립적인 관리 단위는 HTTP, DbWorker, Indexer, Storage maintenance, Alert 작업 정도면 충분하다. 각 함수마다 actor나 service trait를 만들지 않는다. Alert 평가와 전송, 업로드와 백업은 필요에 따라 별도 task로 실행할 수 있지만 상태 저장·재시도 방식을 중복 구현하지 않는다.

## 4. 반드시 유지할 불변조건

1. ACK한 지원 Record는 마스킹된 형태로 SQLite에 commit되어 있다. 메모리 채널에 넣었다는 이유로 ACK하지 않는다.
2. 최종 인덱싱 순서를 결정하는 Indexer는 하나다. 이전 배치의 SQLite 확정 전에 다음 Tantivy 배치나 rollover를 시작하지 않는다.
3. Tantivy에 기록한 배치 경계와 SQLite에 적용한 경계를 비교하여 복구할 수 있다. 정상 처리에서 Record별 존재 검색을 하지 않는다.
4. 검색에 노출된 `ingest_seq` 이하의 Record는 Tantivy commit과 SQLite 확정을 모두 마쳤다.
5. `sealed`는 더 이상 파일이 바뀌지 않는다는 뜻이다. merge 중인 파일을 아카이브하지 않는다.
6. 유일한 로컬 사본은 그 데이터가 완료된 S3 복구 지점에서 복구 가능할 때만 자동 제거한다.
7. 시간 범위·프로젝트 권한 조건은 사용자 검색식 바깥에 강제로 결합한다.
8. 누락된 shard, bucket truncation, 검색 실패를 숨긴 채 정확한 0건이나 전체 집계를 반환하지 않는다.
9. 복구 실패나 포맷 불일치를 빈 데이터베이스로 자동 초기화하여 덮지 않는다.
10. 성능 제한에 도달하면 명시적으로 제한한다. 데이터를 몰래 버리거나 근사값으로 바꾸지 않는다.

## 5. Sentry SDK 연동 계약

### 5.1 Endpoint와 인증

```text
POST /api/{project_id}/envelope/
POST /api/{project_id}/store/          # legacy event 호환
```

DSN은 `https://PUBLIC_KEY@observe.example.com/{project_id}` 형식이다. Envelope header의 DSN, `X-Sentry-Auth`, query의 `sentry_key`를 공식 protocol과 fixture에 맞추어 읽는다. 서로 충돌하는 project/key가 있으면 거절한다. URL의 project, 인증 key의 project, Envelope DSN의 project가 일치해야 한다.

Ingestion public key는 브라우저에 노출될 수 있다. 관리자 인증이나 조회 권한으로 사용하지 않는다. project 비활성화·key revoke를 수신 전에 검사한다.

### 5.2 Protocol 처리

Error/Event와 Structured Log를 지원한다. 원문에서 제외한 transaction, profile, replay, session, attachment, client_report, metric과 알 수 없는 item은 길이·구조 제한을 지킨 뒤 건너뛴다. 같은 Envelope의 지원 item은 정상 처리하고 미지원 counter를 증가시킨다.

Envelope를 단순 `split('\n')`로 처리하지 않는다. item header의 바이트 길이, 마지막 newline 유무, binary item, UTF-8 다중 바이트, 복수 item을 실제 SDK fixture로 검증한다. HTTP 압축은 SDK가 실제 전송하는 gzip/deflate 등을 검증한 범위에서 지원한다. wire 크기와 압축 해제 후 크기를 각각 제한한다.

지원 item 자체가 malformed이면 ACK 전에 400으로 거절한다. valid item과 malformed 지원 item을 섞은 요청을 부분 성공으로 숨기지 않는다. 미지원 item만 있는 유효 Envelope는 성공 처리하되 미지원 수를 기록한다.

Envelope 성공 응답은 202로 할 수 있지만 각 SDK fixture로 확인한다. Legacy store 응답은 SDK가 기대하는 event ID 응답 형태까지 검증하여 반환한다. overload에는 429와 `Retry-After`를 제공한다. SDK별 영구 재전송은 보장하지 않는다.

### 5.3 SDK 호환성 범위

Python FastAPI, Python Celery/standard logging, JavaScript Browser, JavaScript Node, Go의 **명시적으로 고정한 SDK 버전**을 테스트한다. fixture에는 버전, 사용한 integration 옵션, 압축, 원본 HTTP header, sanitized Envelope, 기대 Record를 기록한다.

`error`, `capture_message`, 모든 지원 로그 레벨, extra/tags/user, request, breadcrumb, trace/span, Unicode, 다중 로그 batch, shutdown flush를 검증한다. 지원 SDK 버전 행렬 없이 “모든 최신 Sentry SDK와 호환”이라고 설명하지 않는다.

### 5.4 Python 예시의 수정

아래는 DEBUG 수집을 보여주기 위한 애플리케이션 예시다. 실제 프로젝트에서는 이미 설정한 logging 구성을 존중한다.

```python
import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

logging.basicConfig(level=logging.DEBUG)

sentry_sdk.init(
    dsn="https://PUBLIC_KEY@observe.example.com/1",
    enable_logs=True,
    integrations=[
        LoggingIntegration(
            level=logging.INFO,
            event_level=logging.ERROR,
            sentry_logs_level=logging.DEBUG,
        )
    ],
)

logger = logging.getLogger("erp-api")
logger.debug("SQL generated")
logger.info("Order created", extra={"order_id": 381})
logger.error("Payment failed")
```

Python logging의 Structured Log 수집 임계값 기본값은 INFO이므로 `enable_logs=True`만으로 DEBUG까지 전송된다고 설명하면 안 된다. 위 `event_level=ERROR`에서는 동일 logger.error가 Log와 Error/Event 양쪽 경로로 나타날 수 있다. 이는 한 Record의 중복 저장과 다른 의미다. Error 이벤트 생성을 원치 않는 프로젝트는 SDK에서 event_level을 조정한다. Browser console 수집도 SDK의 해당 integration이 필요하며, Python 옵션을 그대로 적용하지 않는다. [S8]

## 6. Record 모델과 식별자

공통 Record는 다음 정보를 가진다.

```text
record_id                 안정적인 Record 식별자
kind                      log | error
project_id                DSN의 프로젝트 ID
source_event_id           SDK event_id, 없을 수 있음
ingest_seq                SQLite에서 할당하는 전역 수신 순서
received_at_us            Observe의 수신 시각
timestamp_us              SDK의 발생 시각, 없거나 무효면 수신 시각
service / environment / release / level / logger
message
trace_id / span_id / request_id
user_id / user_email
issue_id / fingerprint_version / fingerprint     Error/Event만
attributes                타입을 보존한 동적 JSON
search_text               제한된 전체 텍스트 투영
raw_json                  마스킹된 원래 event/log 구조
normalizer_version
indexing_warnings         truncation, timestamp fallback 등
```

`kind=error`는 Issue Tracking용 Event를 뜻한다. 예외가 없는 `capture_message`도 SDK가 Event로 보냈다면 여기에 포함될 수 있다. 반대로 level=ERROR인 Structured Log를 서버가 자동으로 새로운 Error Event로 복제하지 않는다.

`record_id`는 다음처럼 만든다. SDK event_id가 있는 Error는 project와 event_id에서 결정한다. 그 외 Record는 서버가 해당 수신 요청에 부여한 무작위 acceptance UUID와 item/record ordinal로 결정한다. 구분자를 쓰는 경우 모호하지 않은 정규화 형식을 고정한다.

`ingest_seq`는 SQLite signed 64-bit 범위의 단조 증가 정수이며 Record별로 할당한다. 같은 timestamp를 가진 서로 다른 Record에도 값이 다르다. 중복 Error를 제거하면 sequence에 빈 구간이 생길 수 있고 이는 정상이다. 외부 JSON에서는 정밀도 손실을 피하기 위해 sequence를 10진 문자열로 보낸다.

`issue_id`는 `(project_id, fingerprint_version, fingerprint_hash)`로 결정되는 안정적인 문자열 ID다. 이로써 인덱싱 전에 Issue row를 미리 INSERT하거나 auto-increment ID를 예약할 필요가 없다. 원문의 issue_id 숫자형은 이 내부 식별 방식으로 변경한다.

## 7. 정규화·민감정보 처리

### 7.1 저장 전에 처리한다

인증된 요청을 검증하고, 지원 item을 Record로 변환하고, 민감정보를 마스킹한 뒤에만 SQLite에 저장한다. 원본 Envelope를 먼저 영속화하지 않는다.

이 단계에는 JSON decode, bounded traversal, 공통 필드 추출, fingerprint 계산 같은 순수 계산이 포함된다. Issue DB 갱신·Tantivy indexing·S3 전송은 HTTP 요청 처리에 포함하지 않는다. CPU 작업도 admission permit을 획득한 bounded blocking 실행 경로에서 수행한다.

기본적으로 authorization/proxy-authorization, cookie/set-cookie, password/passwd, access_token/refresh_token, api_key/apikey, secret/client_secret 등을 대소문자와 header 표현을 고려하여 `[Filtered]`로 바꾼다. request URL의 민감 query parameter, header 배열/객체, 중첩 JSON도 처리한다.

마스킹 전 내용은 SQLite Inbox, WAL, 임시 파일, internal error log, S3 backup에 기록하지 않는다. 하지만 이름을 알 수 없는 임의 문자열 속 비밀값까지 완벽하게 탐지한다고 보장하지 않는다. 프로젝트별 추가 마스킹 key 설정과 SDK 측 scrubbing을 허용한다. raw_json은 **마스킹된 원본 구조**이지 wire byte 원본이 아니다.

### 7.2 공통 필드

service 우선순위는 structured `service.name`, Event의 명시된 service, project.slug다. SDK의 표준 service/environment/release 표현은 fixture로 매핑한다. `trace_id`/`span_id`는 알려진 SDK 위치에서 추출하고, request_id는 명시된 attribute/tag 등에서 가져온다. 없는 식별자를 임의 생성하여 실제 correlation인 것처럼 표시하지 않는다.

Timestamp는 UTC 마이크로초로 정규화한다. 시간 역전·지연 전송은 허용한다. 지나치게 잘못된 형식이나 표현 범위 초과만 fallback 또는 거절하고 그 사실을 표시한다. 오래된 발생 시각이라는 이유만으로 서버 수신 시각으로 덮지 않는다.

### 7.3 JSON 투영과 검색 한계

마스킹된 message, exception, breadcrumb message, extra, contexts, tags, user, request URL, log attributes의 scalar string/number/boolean을 `search_text`에 포함한다. null은 missing과 구분하여 raw에는 남긴다.

검색용 투영의 초기 제한은 depth 16, scalar 1,000개, 텍스트 64 KiB다. 원본의 hard validation depth 한도는 별도로 두고 64를 넘는 입력을 거절한다. 개별 Record의 JSON node 수는 20,000으로 제한하고 초과 시 ACK 전에 거절한다. Structured Log batch는 전체를 하나의 거대한 DOM으로 만드는 대신 제한된 Record 단위로 decode한다. 정규화된 요청 전체의 직렬화 크기도 20 MiB를 넘으면 거절하여 입력 확장으로 수신 예산을 우회하지 못하게 한다. 검색용 제한을 초과하면 Record 전체를 버리지 않고 `indexing_warnings`와 counter를 기록한다. raw_json은 허용된 Record 크기 내에서 보존한다.

`search_text`는 scalar를 결정적인 순서로 이어 만든 문서 수준 텍스트다. JSON key 이름 자체까지 모두 검색되는 기능으로 오해하게 만들지 않는다. Phrase는 이 투영의 token 순서 기준이며 JSON 객체 간의 관계를 검사하는 기능이 아니다. 정확한 message 구문은 `message:"..."`, 정확한 attribute 값은 해당 구조화 필드로 검색한다.

자연어 검색이라는 표현은 JSON 위치를 몰라도 텍스트를 찾을 수 있다는 의미다. 동의어 추론·벡터 검색·LLM·한국어 형태소 분석을 자동 제공한다는 의미가 아니다. v1은 Tantivy 내장 tokenizer와 filter를 조합하고 자체 tokenizer framework를 만들지 않는다. Unicode와 한국어 실제 fixture를 포함해 검색 특성을 문서화한다.

## 8. 수신 제한과 ACK

기본 내부 예산은 다음 값으로 시작하고 벤치마크로 조정한다. 운영자가 처음 설치할 때 직접 조절해야 하는 옵션은 아니다.

| 항목 | 초기값 또는 규칙 |
|---|---|
| 압축된 HTTP body | 최대 20 MiB |
| 압축 해제된 Envelope | 최대 20 MiB |
| 개별 정규화 Record | 최대 1 MiB; 초과는 ACK 전에 413 |
| 지원 Record 수/Envelope | 최대 10,000 |
| 전역 수신·정규화 메모리 예약 | 64 MiB 예산, 실제 최대 작업 크기 기준으로 선예약 |
| 정규화 동시 작업 | 1, 측정 후 최대 2 검토 |
| Inbox group commit | 최대 128 요청, 4 MiB 또는 5 ms |
| Indexer batch | 최대 1,000 Record 또는 4 MiB |
| 인덱스 commit 시간 | 작업이 있을 때 최대 약 1초 대기 |

크기가 알려지지 않은 요청도 먼저 최대 작업 크기 permit을 예약한다. raw bytes, JSON 객체의 메모리 증폭, 정규화 결과, response 대기 메모리를 합산한다. “2048개 요청까지” 같은 개수 제한만으로 RAM을 제어하지 않는다. 큰 요청 때문에 배치 한도를 넘으면 더 작은 배치로 처리하며 요청의 원자적 수신 의미를 유지한다.

HTTP 수신 요청의 지원 Record는 크기가 제한된 Inbox chunk들로 나눌 수 있다. **한 요청에 속한 모든 chunk와 sequence 할당은 하나의 SQLite transaction에서 commit**한다. group commit을 쓰더라도 각 요청의 ACK는 실제 commit 성공 후에만 보낸다.

- malformed: 400. 잘못된 key: 401/403. body 초과: 413.
- 메모리·수신함·디스크 예산 초과: 429 + Retry-After.
- DB/스토리지 장애: 503. 응답 전에 commit 실패가 확실하면 ACK하지 않는다.
- ACK가 유실되어 SDK가 재전송할 수 있다. SDK별 재시도 정책을 서버가 보장하지 않는다.

## 9. SQLite 구성

운영 DB는 `data/meta.db` 하나다. SQLite Backup API가 만드는 일시적인 snapshot 파일은 두 번째 운영 DB가 아니다.

```sql
-- auto_vacuum은 최초 테이블 생성 전에 적용한다.
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

연결별 설정이 필요한 PRAGMA는 각 연결에 적용한다. 한 DbWorker가 쓰기 연결을 소유한다. 읽기 연결은 소수만 유지한다. SQLite 호출을 Tokio worker thread에서 장시간 blocking하지 않는다. 백업에는 독립 read connection을 사용한다.

DbWorker는 수신함 INSERT뿐 아니라 Issue 확정·사용자 설정 변경·경보 outbox 등 짧은 쓰기 transaction을 처리한다. 네트워크 요청·Tantivy commit을 SQLite transaction 안에서 기다리지 않는다. 일반 조회는 parameter binding을 사용한다.

WAL은 checkpoint 지연과 크기를 관찰한다. 장시간 read transaction을 무제한 유지하지 않는다. 평상시 checkpoint는 짧게 수행하고 실패 시 재시도한다. 매 요청마다 checkpoint, 매 시작마다 전체 VACUUM, 상시 전체 integrity_check를 실행하지 않는다. 시작은 quick_check, 상세 검사는 doctor와 백업 검증에 배치한다. [S9][S10]

## 10. 핵심 SQLite 스키마

다음 DDL은 최초 스키마의 기준이다. JSON payload는 버전이 있는 application DTO이며 새로운 저장 엔진 형식이 아니다. 로그 전체를 검색하기 위한 `events` 테이블은 만들지 않는다.

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'member')),
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    expires_at_us INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL,
    last_seen_at_us INTEGER NOT NULL
);
CREATE INDEX sessions_user ON sessions(user_id);

CREATE TABLE projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    platform TEXT,
    default_environment TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE project_keys (
    id INTEGER PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    public_key TEXT NOT NULL UNIQUE,
    created_at_us INTEGER NOT NULL,
    revoked_at_us INTEGER
);

CREATE TABLE shards (
    id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    format_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'local', 'remote_verified', 'remote_only')
    ),
    min_timestamp_us INTEGER,
    max_timestamp_us INTEGER,
    min_ingest_seq INTEGER,
    max_ingest_seq INTEGER,
    last_applied_inbox_id INTEGER NOT NULL DEFAULT 0,
    record_count INTEGER NOT NULL DEFAULT 0,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    remote_archive_key TEXT,
    archive_sha256 TEXT,
    recovery_checkpoint_id TEXT,
    created_at_us INTEGER NOT NULL,
    sealed_at_us INTEGER,
    last_accessed_at_us INTEGER
);
CREATE UNIQUE INDEX shards_one_active ON shards(state) WHERE state = 'active';
CREATE INDEX shards_time_range ON shards(min_timestamp_us, max_timestamp_us);

CREATE TABLE runtime_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    installation_id TEXT NOT NULL,
    storage_generation TEXT NOT NULL,
    next_ingest_seq INTEGER NOT NULL CHECK (next_ingest_seq > 0),
    last_applied_inbox_id INTEGER NOT NULL DEFAULT 0,
    last_applied_ingest_seq INTEGER NOT NULL DEFAULT 0,
    active_shard_id TEXT REFERENCES shards(id)
);

CREATE TABLE inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    acceptance_id TEXT NOT NULL,
    chunk_no INTEGER NOT NULL,
    first_ingest_seq INTEGER NOT NULL,
    last_ingest_seq INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    received_at_us INTEGER NOT NULL,
    normalizer_version INTEGER NOT NULL,
    payload BLOB NOT NULL,
    UNIQUE (acceptance_id, chunk_no)
);

CREATE TABLE issues (
    id TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    fingerprint TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    title TEXT NOT NULL,
    culprit TEXT,
    level TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('unresolved', 'resolved', 'ignored')),
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    occurrence_count INTEGER NOT NULL,
    first_release TEXT,
    last_release TEXT,
    resolved_at_us INTEGER,
    resolved_through_ingest_seq INTEGER,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    UNIQUE(project_id, fingerprint_version, fingerprint)
);
CREATE INDEX issues_listing ON issues(project_id, status, last_seen_us DESC, id);

CREATE TABLE issue_occurrences (
    event_key TEXT PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    issue_id TEXT NOT NULL REFERENCES issues(id),
    record_id TEXT NOT NULL,
    shard_id TEXT NOT NULL REFERENCES shards(id),
    source_event_id TEXT,
    ingest_seq INTEGER NOT NULL UNIQUE,
    occurred_at_us INTEGER NOT NULL
);
CREATE INDEX occurrence_listing
    ON issue_occurrences(issue_id, occurred_at_us DESC, ingest_seq DESC);

CREATE TABLE alerts (
    id INTEGER PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id),
    name TEXT NOT NULL,
    condition_type TEXT NOT NULL,
    condition_json TEXT NOT NULL,
    destination_type TEXT NOT NULL,
    destination_json TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_evaluated_at_us INTEGER,
    last_triggered_at_us INTEGER,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE alert_deliveries (
    id TEXT PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(id),
    dedupe_key TEXT NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at_us INTEGER NOT NULL,
    created_at_us INTEGER NOT NULL,
    last_error TEXT
);
CREATE INDEX delivery_due ON alert_deliveries(state, next_retry_at_us);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE backup_snapshots (
    id TEXT PRIMARY KEY,
    cut_ingest_seq INTEGER NOT NULL,
    cut_inbox_id INTEGER NOT NULL,
    object_key TEXT,
    sha256 TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed')),
    created_at_us INTEGER NOT NULL,
    completed_at_us INTEGER,
    last_error TEXT
);

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_us INTEGER NOT NULL
);
```

v1의 관리 권한은 단순하게 정의한다. admin은 설정·사용자·프로젝트·경보를 관리하며 member는 활성 프로젝트 데이터를 조회한다. 원문이 요구하지 않은 조직별 복잡한 RBAC를 미리 만들지 않는다. 이후 프로젝트별 권한을 추가하더라도 검색 외부의 authorization filter 경계는 유지한다.

`inbox`에 processing/lease/attempt 상태를 두지 않는다. 단일 순차 Indexer가 처리하며 처리 완료된 행만 삭제한다. 영구적인 입력 오류는 ACK 전에 걸러낸다. ACK 후 발견된 처리 결함은 Indexer를 멈추고 상태를 노출한다. 재시도 횟수를 넘겼다는 이유로 ACK한 Record를 버리지 않는다.

`issue_occurrences`는 로그 검색 테이블이 아니라 Error 식별자·직접 조회·중복 제거용이다. 그러나 Error마다 행이 추가되므로 작다고 가정해서는 안 된다. 5 errors/sec가 계속되면 하루 432,000행이다. 오류율이 높고 보관 기간이 길면 SQLite 크기·백업 시간·남은 디스크가 실제 제한 요소가 된다. 이 설계는 그 비용을 숨기거나 임의의 dedupe TTL로 보장을 줄이지 않는다.

## 11. Indexer: 처리 순서와 commit 경계

### 11.1 정상 처리

Indexer는 `inbox.id` 오름차순으로 아직 적용되지 않은 chunk를 읽는다. sequence가 연속된 정수여야 한다는 뜻이 아니라 **앞선 수신함 작업을 건너뛰지 않는다**는 뜻이다.

```text
1. 다음 Inbox batch 읽기
2. 이미 확정된 Error event_key와 batch 내부 중복 제거
3. 최종 Record/Issue 변화 계획 계산
4. 같은 active shard에 Record 추가
5. Tantivy prepare_commit → payload 설정 → commit
6. SQLite 한 transaction:
     issues 갱신
     issue_occurrences INSERT
     new issue / regression 경보 outbox INSERT
     shard 통계 갱신
     runtime_state.last_applied_* 갱신
     적용한 Inbox 행 DELETE
7. Tantivy reader 명시적 reload
8. 공개 가능한 ingest_seq 갱신 + Live notification
9. 다음 batch 또는 rollover
```

Issue를 갱신한 뒤 Tantivy 성공을 기대하는 순서는 사용하지 않는다. Issue ID는 순수 계산으로 이미 알 수 있으므로 Tantivy를 먼저 commit하고, 아직 남아 있는 Inbox로 SQLite 확정을 재실행할 수 있다.

Tantivy의 `prepare_commit`, `PreparedCommit::set_payload`, persisted commit metadata를 사용한다. commit payload는 다음과 같은 작은 JSON이다. 별도의 WAL 파일이나 복제 로그를 만들지 않는다. [S2][S3]

```json
{
  "version": 1,
  "shard_id": "shard-uuid",
  "last_inbox_id": 1200,
  "last_ingest_seq": "98765"
}
```

직전 SQLite 적용 경계 이후부터 이 payload 경계까지가 아직 확정하지 않았을 수 있는 유일한 batch다. 로컬 shard manifest에도 최종 경계를 복사한다. Tantivy opstamp를 Record sequence로 사용하지 않는다. C를 읽는 대상은 현 active, 또는 active 생성 전이면 가장 최근 sealed shard다. 오래된 sealed shard의 과거 경계를 현재 A와 비교하여 오류로 판단하지 않는다.

같은 Error event_key가 다른 HTTP 요청으로 다시 들어오면 이미 저장된 occurrence를 확인하여 **검색 document와 occurrence_count 둘 다** 중복 생성하지 않는다. 같은 batch 내부 중복도 제거한다. 모두 중복인 batch도 진행 위치를 확정해야 하므로 빈 document commit의 payload 갱신을 테스트한다.

로그는 안정적인 SDK 전역 log ID가 없으면 별도 HTTP 재전송까지 정확히 중복 제거할 수 없다. 같은 수신함의 재처리는 중복 없이 처리하지만, 서로 다른 요청의 동일한 메시지가 같은 이벤트라고 가정하지 않는다. payload hash를 dedupe key로 사용하여 정상 반복 로그를 지우지 않는다.

### 11.2 장애 복구

`A = SQLite.last_applied_inbox_id`, `C = 해당 진행 경계를 담은 Tantivy committed payload`로 둔다.

| 상태 | 처리 |
|---|---|
| Tantivy commit 전 중단 | 아직 Inbox에 있는 batch를 다시 인덱싱 |
| C > A | Tantivy에는 성공한 batch가 있음. Inbox로 **SQLite 확정만** 재실행 |
| C = A | 두 저장소의 적용 경계가 일치. 다음 Inbox부터 진행 |
| C < A 또는 경계 payload 유실·불일치 | 디스크/포맷/복구 일관성 오류. 정상으로 간주하지 않고 중단·진단 |
| SQLite 확정 후 reader reload 실패 | 검색을 비정상 상태로 표시하고 reload 재시도. Inbox를 다시 만들어 재인덱싱하지 않음 |

각 Record를 모든 shard에서 TermQuery로 찾아보는 복구를 하지 않는다. uncertain commit은 writer를 계속 사용하면서 추정하지 않고 reopen 후 실제 persisted payload로 판단한다. 원인의 성격에 따라 즉시 재시도하지 말고 Indexer를 정지시키고 수신함 backpressure를 적용한다.

Tantivy commit 뒤 SQLite 확정에 실패하면 다음 batch를 받지 않는다. 이 규칙이 없으면 하나의 진행 경계로 복구하는 단순한 방식이 성립하지 않는다.

코드 배포 후에도 Inbox에 저장된 정규화 결과·fingerprint·record_id를 재사용한다. 과거 Inbox를 새로운 normalizer 규칙으로 다시 해석하지 않는다. 호환하지 못하는 Inbox 버전이 있으면 upgrade를 중단한다.

### 11.3 공개 시점

자동 reader reload에 의존하지 않는다. SQLite 확정 후 명시적으로 reader를 reload한 뒤 `published_ingest_seq`를 올린다. 조회는 이 watermark 이하로 제한한다. 저장은 끝났으나 아직 검색이 공개되지 않은 짧은 구간을 API가 구별할 수 있어야 한다.

같은 디스크가 유지되는 프로세스 종료·재시작에서는 성공한 SQLite/Tantivy commit을 이용하여 복구한다. 저장장치가 fsync 계약을 위반하거나 파일이 손상되는 상황까지 절대 유실 없음을 보장하지 않는다.

## 12. Issue grouping과 lifecycle

Fingerprint 우선순위는 SDK 명시 fingerprint, Observe 기본 fingerprint 순이다. SDK fingerprint의 `{{ default }}`는 Observe의 기본 fingerprint로 확장한다. Sentry 서버와 완전히 동일한 grouping 결과를 보장하지 않는다. [S11]

기본 canonical input은 exception type, 정규화한 message, 선택한 in-app frame 최대 5개의 module/function/filename이다. frame 순서를 고정하고 가장 관련 있는 말단 frame을 결정적으로 선택한다. in-app frame이 없으면 일반 stack frame을 사용하고, stack도 없으면 logger·message 기반으로 grouping한다.

기본적으로 line number를 제외한다. UUID, 긴 hex token, 명백한 동적 숫자 ID, 메모리 주소, URL query 값을 제한된 규칙으로 정규화한다. 모든 숫자를 무조건 지워 의미가 다른 HTTP status나 오류 코드를 합치지 않는다. 정규화 false-merge와 false-split fixture를 둔다. 규칙 변경은 fingerprint_version 변경이다.

Issue 상태는 unresolved/resolved/ignored다. 새 Error occurrence의 event_key가 처음 확정될 때만 count를 증가시킨다. first_seen은 발생 시각 min, last_seen은 max로 계산하고 지연 도착으로 시간이 뒤로 돌아가지 않게 한다. first_release/last_release도 대응 시각과 결정적인 tie-break를 기준으로 정의한다.

Resolve 요청을 처리할 때 그 시점의 **최대 수신 sequence**를 `resolved_through_ingest_seq`로 저장한다. 이미 들어와 처리 대기 중이던 Error 때문에 즉시 regression이 발생하지 않도록 한다. 이후 수신된 실제 새 occurrence가 들어오면 unresolved로 변경하고 regression 경보를 한 번 생성한다. ignored는 새 occurrence가 있어도 자동으로 unresolved로 바꾸지 않는다.

경보 outbox와 Issue 상태 변경은 같은 SQLite 확정 transaction에 포함한다. 동일 event를 재처리해 count나 경보가 두 번 늘어나면 안 된다.

## 13. Tantivy schema

필드 이름은 사용자 검색창에 그대로 노출되는 실제 이름이다. Observe 별칭 언어를 만들지 않는다. [S4]

| 필드 | 타입·인덱싱·보관 |
|---|---|
| record_id | keyword, indexed, stored |
| kind / service / level / environment / release / logger | keyword, indexed, fast, stored |
| project_id | integer, indexed, fast, stored |
| timestamp | DateTime, indexed, fast, stored. Record의 timestamp_us에서 변환 |
| ingest_seq | integer, indexed, fast, stored |
| received_at | DateTime, indexed, fast, stored |
| trace_id / span_id / request_id / user_id / user_email | keyword, indexed, stored |
| issue_id / fingerprint | keyword, indexed, stored. 필요한 집계 필드는 fast |
| message | tokenized text, indexed with positions, stored |
| search_text | tokenized text, indexed with positions, not stored |
| attributes | JSON, indexed + fast. 문자열은 raw tokenizer, 숫자·boolean 타입 보존 |
| raw_json | stored only, Zstd document store로 보관 |
| normalizer_version / indexing_warnings | stored |

message와 search_text에 같은 내장 tokenizer 구성을 쓴다. 문자열 정확 일치는 keyword/JSON raw 필드에서 처리한다. 긴 식별자나 URL을 text tokenizer로만 검색하게 만들지 않는다.

`attributes`의 JSON fast field가 없는데 numeric aggregation을 지원한다고 쓰지 않는다. Record마다 새 schema field를 추가하지 않는다. raw_json과 attributes를 별도 압축 파일로 중복 저장하는 custom payload store를 만들지 않는다. 상세 화면은 stored raw_json을 기준으로 원본 구조를 보여주며 attributes를 추가 stored할지는 중복 비용을 측정해 정한다.

### JSON key와 타입 의미

원본 JSON key를 보존하기 위해 dot expansion은 비활성화한다. 중첩 `{"http":{"status_code":500}}`와 문자 그대로 점을 포함한 `{"http.status_code":500}`는 다른 경로다. 후자는 Tantivy가 정한 escape 문법으로 검색한다. 필드 패널이 올바른 표현을 생성하고 직접 새로운 escaping 규칙을 만들지 않는다.

숫자 500과 문자열 "500"을 데이터 저장 단계에서 하나로 변환하지 않는다. 동일 path에서 타입이 섞이면 필드 패널과 집계에서 알려준다. 숫자 metric은 숫자 값만 대상으로 하고 제외된 값의 존재를 알린다. 숫자로 취급할 문자열을 서버가 임의 강제 변환하지 않는다.

배열은 문서의 다중 값이다. 객체 배열에서 `a=1 AND b=2`는 같은 배열 원소에 대한 nested join을 보장하지 않는다. 별도의 nested document engine을 구현하지 않고 이 의미를 명시한다.

## 14. 검색: 자체 언어를 만들지 않는다

### 14.1 구현 규칙

검색창은 Tantivy QueryParser를 사용한다. 기본 검색 필드는 `message`, `search_text`이며 `set_conjunction_by_default()`로 기본 AND를 적용한다. 문법 오류는 strict parsing 오류로 반환한다. lenient parsing으로 틀린 부분을 버리고 성공한 것처럼 조회하지 않는다. [S1]

```rust
// 핵심 호출 형태. 함수 전체의 컴파일 가능한 구현 예제는 아니다.
let mut parser = tantivy::query::QueryParser::for_index(
    &index,
    vec![message_field, search_text_field],
);
parser.set_conjunction_by_default();
let user_query = parser.parse_query(input)?;
```

사용하는 문법은 library 문법이다. `attr.`를 다른 경로로 재작성하거나 `field:>10`을 custom range로 바꾸는 문자열 치환기를 만들지 않는다. 공백, 괄호, quotation mark를 직접 파싱하지 않는다. regex 허용 옵션과 자동 fuzzy 설정은 켜지 않는다.

비용 검사를 위해 구조를 볼 필요가 있으면 Tantivy가 제공하는 query grammar와 **라이브러리 AST**를 사용한다. 필요한 field/절 수/깊이 검증 후 library의 build API에 그대로 전달한다. Observe 전용 AST 타입이나 visitor 기반 검색 컴파일러를 추가하지 않는다. [S1]

### 14.2 검색 예시

```text
database timeout
message:"connection timeout"
service:erp-api level:error
(service:erp-api OR service:gw-api) level:error
* -healthcheck
attributes.tenant_id:10
attributes.http.status_code:[500 TO 599]
trace_id:0123456789abcdef0123456789abcdef
message:"connection time"*
```

이는 고정한 Tantivy 버전의 문법을 보여주는 예시다. Boolean NOT 기능은 native exclusion으로 제공한다. prefix 기능도 native prefix 표현으로 제공하며 원문의 `foo*`나 `NOT` 철자를 별도의 compatibility parser로 유지하지 않는다. 숫자 크기 비교는 native range와 UI의 이상/이하 필터로 제공한다. 기능은 유지하되 별도 철자 체계를 만드는 일이 없다.

기본 검색 UI는 사용자가 문법을 외우지 않도록 시간·프로젝트·서비스·환경·레벨·릴리스 필터를 제공한다. 단순 고정 필터는 `TermQuery`/`RangeQuery` 등 native query object로 결합한다. 이 필터 DTO는 범용 재귀 AST나 두 번째 검색 언어가 아니다.

### 14.3 권한과 필터 결합

```text
최종 Query =
  허용 프로젝트 조건
  AND 활성 프로젝트 조건
  AND 시간 범위 조건
  AND published watermark 조건
  AND UI 고정 필터
  AND QueryParser가 만든 사용자 query
```

사용자 query 안에 `project_id` 또는 OR가 있어도 바깥 권한 조건을 제거할 수 없다. 사용자 문자열 앞뒤에 권한 문자열을 붙이는 방식은 금지한다. native BooleanQuery의 mandatory clause로 결합한다.

## 15. 검색 API

Logs·Explore·Alerts·Related Logs·Live catch-up은 동일한 검색 요청 검증과 query builder를 사용한다. UI가 편리한 얇은 endpoint는 여러 개여도 검색 구현은 하나다.

```http
POST /api/explore/search
Content-Type: application/json
```

```json
{
  "projects": [1],
  "start": "2026-09-08T03:00:00Z",
  "end": "2026-09-08T03:15:00Z",
  "query": "message:\"connection timeout\"",
  "filters": {
    "kinds": ["log", "error"],
    "services": ["erp-api"],
    "levels": ["error", "fatal"],
    "environments": ["prod"]
  },
  "limit": 100,
  "cursor": null
}
```

```json
{
  "items": [],
  "next_cursor": null,
  "read_watermark": "98765",
  "complete": true,
  "took_ms": 12,
  "searched_shards": 3,
  "hydrated_shards": 0,
  "warnings": []
}
```

`GET /api/logs`는 같은 로직의 GET wrapper다. 시간 범위는 `[start, end)`로 통일한다. API 시간은 UTC RFC3339이고 UI에서 사용자 timezone으로 표시한다. 로그 화면 기본값은 최근 15분이다.

query가 비어 있으면 전체 일치로 처리한다. limit 기본값은 100, 최대 1,000이다. query와 API 문자열 길이 제한을 둔다. 요청한 프로젝트 중 권한 없는 것이 있으면 조용히 일부만 조회하기보다 명시적 권한 오류를 반환한다.

날짜 범위에 필요한 shard를 읽지 못하면 정확한 전체 결과처럼 200을 반환하지 않는다. v1 기본 계약은 503 등 명확한 오류다. 나중에 부분 결과 모드를 넣더라도 `complete=false`, 실패한 shard 수, 집계 불완전 여부를 반드시 명시한다.

## 16. 정렬과 페이지네이션

기본 정렬은 `(timestamp DESC, ingest_seq DESC)`다. timestamp만으로 cursor를 만들지 않는다. Record sequence는 전역 unique이므로 동률이 완전히 결정된다. relevance 점수를 서로 다른 shard 간에 비교하지 않는다.

첫 페이지에서 검색 공개 watermark W를 잡는다. 모든 페이지는 `ingest_seq <= W` 조건을 유지한다. 다음 페이지는 다음 조건을 추가한다.

```text
timestamp < last_timestamp
OR
(timestamp = last_timestamp AND ingest_seq < last_ingest_seq)
```

cursor에는 version, storage_generation, 정규화한 요청 hash, 권한 범위 hash, W, 마지막 timestamp/sequence, 만료 시각을 담아 서명한다. 프로젝트 권한은 cursor를 믿지 않고 매 요청 검사한다. 복구로 storage_generation이 바뀌면 이전 cursor를 무효화한다.

이 방법은 첫 조회 후 늦게 도착한 옛 timestamp 로그가 후속 페이지 중간에 끼는 문제를 방지한다. 무제한 서버 측 PIT/session registry는 만들지 않는다. 중간에 프로젝트 비활성화나 명시적 삭제가 일어나면 cursor를 무효화하거나 오류를 반환한다.

각 shard에서는 native TopDocs의 fast-field sort key를 사용하여 필요한 수만 수집하고 결과를 전역 정렬 키로 병합한다. 날짜와 sequence의 tuple sort는 고정한 라이브러리 API로 구현한다. 임의의 OFFSET 기반 deep pagination은 사용하지 않는다. [S5]

## 17. Shard 선택과 multi-shard 검색

SQLite catalog의 **실제 Record 발생 시각** min/max로 time pruning한다. 생성 날짜나 S3 경로 날짜는 검색 범위를 대신하지 않는다.

```text
shard.max_timestamp >= start
AND shard.min_timestamp < end
```

Active shard에는 이전 시간의 로그가 뒤늦게 들어올 수 있다. 현재 v1은 위 조건에 맞는 모든 shard를 제한된 동시성으로 검색한다. “새 shard 몇 개에서 limit을 채웠다”는 이유로 나머지 shard를 건너뛰지 않는다. 정확한 최대 sort bound 기반 가지치기는 나중에 측정한 뒤 추가할 수 있다.

한 요청의 전체 candidate를 무제한 쌓지 않는다. 전체 top K를 유지하는 bounded heap 또는 bounded merge를 사용한다. shard handle cache도 LRU와 열린 handle 수 제한을 둔다. 오래된 shard 수천 개를 항상 열어 놓지 않는다.

검색 중인 shard는 pin/refcount를 유지한다. 디스크 회수는 active·업로드·백업·검색·다운로드 중인 shard를 삭제하지 않는다. 파일 존재 검사와 handle 취득 사이의 경쟁 조건도 같은 pin 경로에서 처리한다.

## 18. 집계와 정확성

필수 metric은 Record count, numeric sum/min/max/avg다. 필수 group-by는 service/level/environment/release다. 시간 histogram과 이 group의 metric 조합을 지원한다. 숫자형 JSON attribute도 fast field를 통해 metric 대상이 될 수 있다. 임의의 모든 Elasticsearch aggregation을 구현할 필요는 없다.

Tantivy Aggregation API를 사용한다. 여러 shard의 intermediate result를 native merge한 다음 최종 result로 만든다. 같은 프로세스 안이므로 intermediate를 JSON으로 직렬화하는 별도 protocol을 만들지 않는다. [S6]

```text
각 shard의 native aggregation intermediate
                  ↓
          native intermediate merge
                  ↓
             final aggregation
```

### 18.1 Top-K를 섞어 정확하다고 하지 않는다

각 shard/segment에서 Top 10만 가져온 뒤 이를 합쳐 global Top 10이라고 부르면 틀릴 수 있다. 예를 들어 X가 shard A에서 9번, shard B에서 9번 등장하고 A 전용 값과 B 전용 값이 각각 10번 등장하면, 각 shard Top 1에 없는 X가 전체에서는 18번으로 1위다.

정확 모드에서는 중간 후보 수를 UI의 표시 개수로 줄이지 않는다. native bucket 수집을 설정해 허용 범위 내 모든 bucket을 보존하고, 전체 병합 후 top K를 자른다. segment/shard와 전체 coordinator 양쪽에서 cardinality와 메모리 제한을 검사한다. 어떤 단계라도 bucket truncation이나 non-zero count error가 생기면 정확 모드 성공 응답을 금지한다.

정확 bucket 한도는 전체 20,000개로 시작한다. 20,001번째 bucket이 필요하면 명시적 오류를 반환한다. 높은 cardinality는 조용히 근사 결과로 바꾸지 않는다. 고정한 native terms collector가 중간 truncation을 일으키지 않는 설정과 한도 오류를 실제 adversarial fixture로 확인하는 것을 구현 초기 검증 조건으로 둔다.

native aggregation memory guard는 명시적으로 설정한다. 라이브러리 기본 메모리 한도가 저사양 서버에 적절하다고 가정하지 않는다. 쿼리당 16~32 MiB로 시작하며 동시에 실행 가능한 집계 수를 제한한다.

### 18.2 수치 의미

count는 Record 수다. avg는 모든 numeric value의 sum / numeric value count이며 shard 평균의 단순 평균이 아니다. 다중 값 attribute는 숫자 값 각각이 metric에 참여하므로 numeric value count와 Record count가 다를 수 있다.

missing 값은 0이 아니다. 숫자 metric의 대상 값이 없으면 null을 반환한다. 문자열 숫자, boolean, null은 자동으로 숫자로 바꾸지 않는다. group의 missing과 문자열 `"null"`을 같은 key로 만들지 않는다. 혼합 타입 bucket에는 타입 정보를 유지한다.

정확하다는 것은 sampling이나 bucket 누락이 없다는 의미다. f64 sum/avg는 임의 정밀도 회계 계산이 아니며 부동소수점 반올림 오차가 있다. 테스트의 count와 bucket membership은 정확 비교, float는 명시한 허용 오차와 overflow/non-finite 정책으로 검증한다.

### 18.3 API와 histogram

```json
{
  "projects": [1],
  "start": "2026-09-08T00:00:00Z",
  "end": "2026-09-09T00:00:00Z",
  "query": "service:erp-api",
  "metrics": [
    {"op": "count"},
    {"op": "avg", "field": "attributes.duration_ms"}
  ],
  "group_by": ["level"],
  "histogram": {"field": "timestamp", "interval": "auto"}
}
```

이는 고정된 endpoint DTO이며 새로운 검색 언어·범용 실행 AST가 아니다. op와 필드를 검증해 native 집계 request로 매핑한다. 처음부터 범용 analytics abstraction을 만들지 않는다.

histogram의 auto 간격은 1시간 이하 10초, 6시간 이하 1분, 하루 이하 5분, 7일 이하 1시간, 30일 이하 6시간, 그 이상 1일을 기본값으로 한다. 매우 긴 범위에는 bucket budget에 맞추어 더 큰 간격을 선택하고 실제 interval을 응답한다. UTC bucket 경계와 `[start,end)` 규칙을 유지한다. UI는 local time으로 라벨만 바꾼다.

정확 total/count/histogram은 모든 대상 shard를 검사한다. top K 검색을 일찍 끝낼 수 있는 상황이라도 같은 조건으로 집계를 조기 종료하면 안 된다. 로그 row API와 histogram API는 분리 호출할 수 있으므로 느린 cold 집계가 이미 도착한 최근 로그 표시까지 막지 않게 한다.

## 19. Live Logs와 correlation

### 19.1 Live Logs

`GET /api/logs/live`는 SSE를 사용한다. commit과 SQLite 확정·reader 공개가 끝난 뒤 `tokio::broadcast`에 **변경 알림과 sequence 경계**를 보낸다. broadcast를 durable queue로 취급하지 않는다.

실제 구독 데이터는 공통 search 경로로 읽는다. Live 전용으로 또 하나의 검색식 해석기나 Record predicate evaluator를 구현하지 않는다. resume 위치는 event timestamp가 아니라 `ingest_seq`다. 늦게 도착한 과거 로그도 새 수신 Record로 전달한다.

최초 연결은 먼저 알림을 구독하고 W를 캡처하여 W 이하의 catch-up을 완료한 다음 W 이후를 전달한다. 재연결은 Last-Event-ID의 sequence 이후를 조회한다. 클라이언트는 record_id로 중복 표시를 방지한다. broadcast lag가 생기면 DB/인덱스에서 catch-up하고, 따라잡기 예산을 넘으면 `resync_required`를 보내 일반 검색으로 재동기화한다.

SSE keepalive, 연결 수 제한, 느린 클라이언트 버퍼 제한, 취소 처리를 둔다. 화면의 Live 시간 범위는 수신 시각과 발생 시각 중 무엇을 제한하는지 명시한다. 기본 Live는 새 수신 Record를 기준으로 하고 event timestamp를 row에 표시한다.

### 19.2 Related Logs

우선순위는 동일 trace_id, 동일 request_id, 같은 project/service + user_id + 근접 시간, 마지막으로 같은 service의 error 시각 ±30초다. 모든 단계에서 조회 권한과 프로젝트 경계를 적용한다.

정확한 trace/request ID 기반과 시간 근접 추정을 UI에서 구분한다. ID가 있다고 무제한 기간을 스캔하지 않고 합리적인 시간 범위를 기본으로 하되 사용자가 넓힐 수 있다. 서비스 간 trace 연결은 사용자가 조회 권한을 가진 프로젝트에 한한다.

## 20. 로컬 파일 배치와 shard lifecycle

```text
data/
├── .lock
├── meta.db
├── meta.db-wal
├── meta.db-shm
├── shards/
│   ├── {shard_uuid_1}/
│   │   ├── Tantivy native files
│   │   └── observe-manifest.json     # sealed일 때만 존재
│   └── {shard_uuid_2}/
│       └── Tantivy native files      # 현재 active
├── snapshots/                       # 아직 업로드/검증 중인 임시 복구 snapshot
└── tmp/                             # 다운로드·압축·안전한 staging
```

Shard 경로는 생성 시 정해지고 active→sealed 전환에서 바뀌지 않는다. cold hydrate도 같은 `shards/{id}` 경로로 들어온다. 별도 warm/cold 파일 트리와 별도 cache janitor를 만들지 않는다. sealed 데이터의 로컬 존재 여부는 native index 디렉터리의 유효성으로 확인한다.

같은 `data_dir`를 두 프로세스가 열지 못하도록 OS 파일 잠금을 사용한다. 파일이 존재한다는 검사만으로 lock을 구현하지 않는다. 로컬 SSD와 fsync/atomic rename 계약을 가진 로컬 파일시스템을 전제로 하며 NFS 같은 공유 파일시스템은 v1에서 지원하지 않는다.

상태는 원문과 같이 네 개를 사용한다.

| 상태 | 의미 |
|---|---|
| active | 유일한 인덱싱 대상 |
| local | sealed, 로컬 사본이 있고 아직 완료된 원격 복구 지점의 일부가 아님 |
| remote_verified | 완료된 원격 복구 지점으로 복구 가능하고 로컬 사본도 있음 |
| remote_only | 완료된 원격 복구 지점으로 복구 가능하며 로컬 사본은 없음 |

다운로드 후 remote_only는 remote_verified가 될 수 있다. upload 중/다운로드 중/검색 중 여부는 task 상태와 pin으로 나타낸다. 불필요한 영속 lifecycle 상태를 추가하지 않는다. archive 객체만 업로드한 상태를 remote_verified라고 표시하지 않는 점이 원문과 다르다.

## 21. Rollover와 seal

Active는 하나다. 실제 파일 크기가 256 MiB에 도달하거나 **첫 Record 이후** 1시간이 지나면 다음 batch 경계에서 seal한다. 빈 shard를 매시간 생성·업로드하지 않는다. 처리 중인 마지막 batch와 merge 때문에 기준보다 조금 커질 수 있으므로 이 크기를 절대 상한으로 설명하지 않는다.

seal 순서는 다음과 같다.

```text
1. 직전 batch의 Tantivy commit + SQLite 확정 + reader 공개 완료 확인
2. 신규 indexing batch 시작 중단. HTTP Inbox 수신은 유지
3. 마지막 commit 경계 유지, merge 완료 대기, writer 종료
4. native index 파일과 디렉터리의 내구성 확인
5. observe-manifest.json.tmp 작성 → fsync → atomic rename → parent fsync
6. SQLite에서 기존 shard를 local로 변경하고 active_shard_id를 비움
7. 필요한 경우 이 경계의 SQLite backup read snapshot을 고정
8. 새 UUID의 active shard 생성, 현재 적용 경계를 native payload에 초기화
9. SQLite active_shard_id 등록, Indexer 재개
```

`wait_merging_threads` 등 라이브러리 API를 사용하여 sealed 파일이 더 이상 변경되지 않게 한다. active를 아카이브하면서 writer가 동시에 merge하게 두지 않는다. 모든 shard를 강제로 한 segment로 합치는 force-merge는 기본 정책으로 사용하지 않는다.

새 active 초기 payload는 이전 SQLite 적용 경계를 이어받는다. 새 shard에 아직 Record가 없어도 C=A 불변조건이 성립해야 한다. seal 과정에서 내용 없는 commit이 기존 payload를 지우지 않도록 테스트한다.

로컬 manifest는 다음 정보를 가진다.

```json
{
  "manifest_version": 1,
  "shard_id": "shard-uuid",
  "schema_version": 1,
  "normalizer_version": 1,
  "tantivy_version": "0.26.1",
  "index_format_version": "pinned-build-format",
  "tokenizer_version": 1,
  "min_timestamp_us": 0,
  "max_timestamp_us": 0,
  "min_ingest_seq": "1",
  "max_ingest_seq": "98765",
  "last_applied_inbox_id": 1200,
  "record_count": 90000,
  "sealed_at": "2026-09-08T04:00:00Z",
  "files": []
}
```

`files`에는 실제 native index에 필요한 파일의 상대 경로, 크기, checksum을 기록한다. 자기 자신인 observe-manifest.json과 런타임 lock/임시 파일은 그 checksum 목록에서 제외한다. 필요한 native metadata와 segment 파일의 집합은 고정 버전의 index format에 맞추어 검증한다. 예시의 포맷 값은 실제 고정 빌드에서 구체화하며 문자열 placeholder를 그대로 출시하지 않는다. SQLite가 손상돼도 shard를 식별하고 포맷·경계를 검증할 수 있어야 한다.

## 22. S3 archive

S3는 선택 사항이다. 설정이 없으면 local-only로 모든 기본 Error/Log/Search 기능을 제공한다.

```text
OBSERVE_S3_URL=s3://bucket/prefix
OBSERVE_S3_ENDPOINT=...       # 검증된 compatible endpoint에 한해 선택
```

인증은 AWS SDK default credential chain을 사용한다. 환경 변수, IAM Role, AWS_PROFILE 등 표준 경로를 쓰며 자체 credential 포맷·서명·HTTP S3 client를 만들지 않는다. credential을 SQLite에 저장하지 않는다.

```text
s3://bucket/prefix/
├── installation.json
├── shards/{shard_id}.tar.zst
├── shards/{shard_id}.manifest.json
├── metadata/{snapshot_id}.db.zst
├── checkpoints/{checkpoint_id}.json
└── latest.json
```

각 shard archive는 sealed native index와 local manifest를 담은 표준 tar.zst다. 자체 `.pack`/columnar/remote Tantivy Directory를 만들지 않는다. 동일한 immutable object key를 다른 내용으로 덮어쓰지 않는다.

업로드는 sealed shard부터 가능한 빨리 시작한다. archive 생성과 upload를 스트리밍 또는 제한된 임시 파일로 수행하고 전체 archive를 메모리에 읽지 않는다. SHA-256과 실제 크기를 계산하고 업로드 무결성을 확인한 뒤 remote manifest를 기록한다.

### 22.1 Checksum과 multipart

ETag를 MD5나 전체 파일 SHA-256이라고 취급하지 않는다. HEAD로 자신이 적은 `x-amz-meta-sha256`을 다시 읽는 것도 독립적인 검증이 아니다. [S12]

일반적인 bounded shard는 단일 PutObject로 먼저 구현할 수 있다. 100 MiB 이상이면 반드시 multipart여야 하는 것은 아니다. 큰 snapshot과 재전송 비용 때문에 multipart 경로도 제공하되 AWS SDK의 기능과 공식 checksum 계약을 사용한다.

단일 PUT은 S3가 검증하는 SHA-256 checksum을 사용한다. Multipart의 SHA-256은 composite checksum이므로 로컬 전체 파일 SHA-256과 직접 비교하지 않는다. 각 part checksum 검증과 완료된 object의 size/composite checksum을 검증한다. 사용할 SDK가 이를 어떤 응답 필드로 제공하는지 고정 버전 integration test로 확인한다.

다운로드 후에는 manifest에 저장한 **전체 archive SHA-256**을 다시 계산하여 비교한다. 이렇게 업로드 전송 검증과 전체 파일 복구 검증의 의미를 분리한다. 실패한 multipart는 abort하고, stale multipart 정리는 일관된 내부 정책으로 재시도한다. SDK 밖에서 새로운 multipart protocol을 설계하지 않는다.

### 22.2 Remote manifest

remote manifest는 shard identity·포맷·시간/sequence 경계·record count·archive key·archive 크기·전체 SHA-256을 포함한다. archive 검증이 끝나기 전에 manifest를 완료본으로 공개하지 않는다. 업로드 중 실패하면 동일한 immutable 내용으로 안전하게 재시도한다.

S3-compatible을 명시한 경우 checksum, conditional request, multipart, list pagination 등의 동작을 해당 제품과 실제 검증한다. MinIO 테스트만 통과했다고 AWS S3의 multipart checksum 계약까지 검증했다고 하지 않는다.

## 23. 일관된 S3 복구 지점

### 23.1 왜 SQLite 백업만으로는 부족한가

SQLite Online Backup은 SQLite 내부의 일관성을 보장하는 기능이지 Tantivy와의 원자적 백업 기능은 아니다. SQLite snapshot 시점보다 뒤의 shard를 아무렇게나 합치면 Issue count/occurrence와 실제 Record가 어긋날 수 있고, snapshot의 Inbox가 이미 원격 shard에 들어간 내용을 재처리할 수도 있다. [S9]

이 설계는 복구 때 그 불일치를 모두 다시 계산하는 엔진을 추가하지 않는다. 대신 **백업을 만들 때 데이터 경계를 맞춘다.**

### 23.2 복구 지점 생성 절차

seal 직후, SQLite가 적용한 마지막 Inbox/sequence 경계를 B라고 한다.

```text
1. Indexer가 B까지 완전히 적용하고 active shard를 seal
2. SQLite에 sealed 상태를 반영, 현재 active는 없음
3. 전용 read connection에서 BEGIN + SELECT로 B의 snapshot 고정
4. 새 active 생성 후 Indexer 재개
5. 고정된 read transaction에서 SQLite Online Backup 수행
6. 임시 snapshot의 integrity, installation ID, B, shard catalog 검증
7. read transaction 종료; snapshot fsync/압축/checksum
8. snapshot catalog가 참조하는 모든 sealed shard의 S3 archive 검증
9. SQLite snapshot을 S3에 업로드·검증
10. 위 파일을 참조하는 checkpoints/{id}.json을 마지막에 업로드
11. latest.json 갱신
12. 로컬 catalog에서 해당 shard들을 remote_verified로 표시
```

BEGIN만 실행하고 SELECT하지 않은 상태를 고정된 snapshot이라고 가정하지 않는다. WAL의 read transaction이 실제로 성립한 뒤 Indexer를 재개한다. snapshot read connection은 백업 중 다른 작업에 공유하지 않는다. destination DB에는 별도의 활성 transaction이 없어야 한다. [S9][S10]

B 이후 이미 수신된 Inbox 행이 snapshot에 들어 있을 수 있다. 이는 정상이다. snapshot의 적용 경계는 B이고 그 이후 Inbox는 복구 후 새 active에서 처리하면 된다. B 이후 생성된 다른 원격 shard를 함께 섞지 않는 것이 핵심이다.

Idle 관리 정보 백업에 빈 active row가 포함되는 경우에는 B까지 적용한 해당 active의 record_count가 0임을 검증한다. 백업 사본에서만 active_shard_id를 비우고 그 빈 row를 제거한다. 운영 DB와 파일에는 영향을 주지 않는다. 데이터가 들어 있는 active를 이 규칙으로 제외해서는 안 된다. 복구 시에는 새 active를 B로 초기화한다.

백업이 오래 걸리면 고정 read transaction 때문에 WAL이 커질 수 있다. 백업 deadline·WAL growth·임시 공간 예산을 관찰하여 초과하면 backup을 취소하고 read transaction을 해제한다. 실패한 candidate를 완료된 복구 지점으로 공개하지 않는다. HTTP 수신은 별도 예산 범위에서 계속할 수 있다.

### 23.3 Checkpoint manifest

checkpoint JSON에는 installation ID, checkpoint ID, snapshot key/전체 checksum, B의 Inbox/sequence 경계, 생성 시각, 필요한 모든 sealed shard의 ID와 remote manifest 정보가 들어간다. SQLite snapshot의 shard catalog와 정확하게 대응해야 한다. DB snapshot이 아직 `local`로 알고 있는 새 shard의 remote 위치는 이 checkpoint가 보완한다.

이는 작은 복구 목록이며 새로운 WAL·distributed transaction manager가 아니다. shard와 snapshot은 immutable, 최종 manifest만 완료 표시다. latest 포인터 갱신 전에 죽으면 완료된 checkpoint 목록을 검사해 복구할 수 있다.

snapshot이 참조하는 shard 하나라도 원격 검증을 마치지 못하면 checkpoint는 미완료다. **S3 archive 업로드 완료**와 **디스크를 잃어도 복구 가능한 상태**를 구분하여 Storage UI에 표시한다.

### 23.4 생성 주기와 queue

기본적으로 256 MiB 또는 1시간 seal 경계에서 candidate를 만든다. 고빈도 rollover 때 전체 SQLite snapshot을 매번 만드는 것은 피하고 최소 5분 간격으로 합친다. 새로운 indexing이 없는 idle 상태에서 관리 정보만 변했다면 5분 간격으로 같은 B의 snapshot을 갱신할 수 있다.

진행 중 backup 하나와 최신 candidate 정도만 유지한다. 업로드가 막힌 동안 모든 full snapshot을 무한히 쌓지 않는다. 아직 공개하지 않은 오래된 candidate는 더 최신의 일관된 candidate가 이를 포함할 때 정리할 수 있지만, 완료된 최신 checkpoint와 그 파일은 먼저 삭제하지 않는다.

연속 수집 중 복구 지점 간격은 기본적으로 seal 주기에 영향을 받는다. 원격 업로드가 지연되면 RPO는 더 길어진다. UI에 `last_recoverable_at`, `recoverable_through_ingest_seq`, `backup_lag`를 표시하고 “5분 백업이므로 항상 5분 이내 데이터만 잃는다”는 문구를 쓰지 않는다.

### 23.5 Retention

관측 Record의 S3 archive는 기본 무기한 보관이다. 사용자 요청 없이 삭제하지 않는다. metadata/checkpoint snapshot은 최근 48시간의 적절한 복구 지점, 최근 30일의 일 단위, 최근 12주의 주 단위를 유지하도록 정리할 수 있다. 실제 생성한 지점만 보관한다.

최신 정상 복구 지점과 직전 정상 지점은 항상 남긴다. snapshot을 삭제할 때 이를 참조하는 checkpoint도 함께 안전하게 정리한다. checkpoint 정리가 immutable Record archive 삭제로 이어져서는 안 된다. 외부 S3 lifecycle이 archive를 삭제하거나 즉시 읽을 수 없는 storage class로 이동시키면 이 제품의 cold search 계약이 깨지므로 운영 경고를 제공한다.

## 24. 자동 복구와 reconciliation

### 24.1 같은 디스크가 있는 재시작

```text
OS data-dir lock → SQLite quick_check → migration 호환성 확인
→ local shard/manifest 검사 → 실제 active와 catalog 일치 확인
→ native commit 경계와 SQLite 적용 경계 비교·복구
→ 필요 시 미완료 seal/new-active 정리
→ reader 공개 → task 시작 → ready
```

manifest는 있는데 SQLite row가 active인 경우 sealed 상태로 정리한다. 새 active 파일만 있고 catalog 등록 전이라면 identity와 초기 payload를 확인하여 안전하게 채택하거나 빈 미사용 디렉터리로 정리한다. 데이터가 들어 있는 미등록 index를 추정으로 삭제하지 않는다.

정상 DB가 있으면 S3 snapshot이 더 새롭게 보인다는 이유로 덮어쓰지 않는다. S3가 일시적으로 실패해도 로컬 복구가 완료되면 조회·수신을 계속할 수 있다. 매 정상 시작마다 모든 S3 archive를 다운로드하거나 전체 bucket을 대규모 스캔하지 않는다.

### 24.2 meta.db 또는 전체 디스크 유실

S3가 설정되어 있고 로컬 DB가 없거나 명백히 손상되었으면 완료된 checkpoint를 찾는다. 손상된 DB와 WAL/SHM을 quarantine한 뒤 새로운 경로에서 검증한다. 건강한 데이터가 남아 있는지 확인하지 않고 같은 파일 위에 restore하지 않는다.

```text
완료 checkpoint 선택
→ installation/format/snapshot checksum 검증
→ SQLite snapshot 다운로드·압축 해제·integrity 검증
→ checkpoint의 shard 목록과 DB의 B 경계 대조
→ 필요한 remote manifest 검증·catalog 보완
→ meta.db 안전 설치
→ storage_generation 갱신, 세션·옛 cursor 무효화
→ 새로운 active shard 생성
→ snapshot 안의 pending Inbox 처리
→ cold shard는 필요할 때 hydrate
→ ready
```

checkpoint가 참조하는 shard는 `remote_only`로 등록하면 되며 모든 archive를 시작 시 다운로드할 필요는 없다. manifest만 있다고 충분한 것은 아니므로 원격 object 존재·크기·checksum metadata를 검증하고 실제 다운로드 때 전체 checksum을 재검증한다.

완료 checkpoint보다 뒤의 S3 객체는 `unreferenced recovery candidates`로 보고한다. 자동 복구 데이터에 섞지 않고 삭제하지도 않는다. 이 객체는 완료된 복구 보장 범위 밖이다. 원문처럼 목록에 추가하기만 해서 Issue와 Inbox까지 복구된 것으로 간주하지 않는다. 향후 explicit salvage 도구를 만들 수 있으나 v1의 자동 복구 성공 조건에 의존하지 않는다.

S3가 설정되었는데 일시 장애로 checkpoint를 확인할 수 없고 로컬 DB도 없다면 빈 설치를 자동 생성하지 않는다. ready=false로 유지하여 잘못된 mount나 prefix 때문에 기존 설치를 덮는 일을 막는다.

### 24.3 복구 보장 범위

동일 디스크의 process crash는 local Inbox와 native commit 경계로 복구한다. 디스크 전체 유실은 최신 완료 checkpoint의 Record·관리 상태와 그 snapshot에 남은 Inbox까지 복구한다. 그 이후의 active/미완료 checkpoint 데이터는 잃을 수 있다.

백업 이후 변경한 Issue 상태, 사용자, project key revoke도 rollback될 수 있다. full restore 시 기존 session을 무효화하고 관리자에게 인증 정보·key 회전을 요구하는 경고를 표시한다. 기본 query 권한 검사를 생략하여 복구 편의성을 얻지 않는다.

같은 S3 prefix를 두 실행 중인 Observe 설치가 동시에 쓰는 것은 지원하지 않는다. installation ID와 최신 pointer의 조건부 갱신으로 잘못된 충돌을 감지하되 이것을 HA leader election으로 확대하지 않는다. restore 뒤 원래 노드를 동시에 켜지 않는 운영 계약을 명시한다.

## 25. 디스크 관리와 cold search

### 25.1 공간 예산

남은 공간 비율만 보지 않는다. meta.db/WAL 성장, 현재 indexing batch, merge 임시 파일, archive 생성, snapshot, download/extract 작업이 필요한 공간을 미리 예약한다.

기본 감시 기준은 free 20% 이하에서 회수, 10% 이하에서 수신 제한 검토, 회수 목표 30%다. 여기에 최소 512 MiB와 실제 최대 임시 작업의 요구량을 반영한 reserve를 적용한다. 작은 볼륨과 매우 큰 볼륨에서 비율만으로 판단하지 않는다.

회수 순서는 사용하지 않는 임시 파일, 다시 받을 수 있는 오래 미접근 remote_verified shard 순서다. active, local-only, 미완료 checkpoint 의존 shard, 검색·업로드·백업 중 pin된 shard는 제거하지 않는다. `last_accessed_at` 갱신은 매 hit마다 SQLite write하지 않고 일정 간격으로 합친다.

S3가 없거나 업로드가 막혀 안전하게 회수할 수 없으면 수신을 제한하고 저장 공간 부족을 표시한다. 무기한 보관은 무제한 로컬 디스크를 뜻하지 않는다. S3를 사용해도 SQLite·WAL·임시 공간의 최소 로컬 용량은 필요하다.

### 25.2 Cold hydrate

```text
Query planner가 remote_only 선택
→ 동일 shard download single-flight
→ 예상 archive/extracted 크기만큼 디스크 예약
→ tmp에 다운로드 + 전체 SHA-256 확인
→ tmp 디렉터리에 안전하게 해제
→ manifest 파일 목록·크기·checksum·index format 검사
→ atomic rename으로 shards/{id} 설치
→ pin 획득 후 native Tantivy open/search
```

동시에 같은 shard를 여러 번 받지 않는다. hydration 동시성은 1로 시작하고 자원이 충분한 경우 내부 정책으로 2까지 허용한다. 전체 archive를 RAM에 올리지 않는다. 아카이브의 절대경로, `..`, symlink/hardlink escape, device entry, 크기 초과, manifest에 없는 파일을 거절한다.

검증하지 않은 디렉터리는 정상 shard 경로로 공개하지 않는다. eviction과 hydration은 같은 pin/예약 관리자에서 경쟁을 제어한다. 취소된 다운로드와 해제 실패의 임시 파일을 정리한다.

S3 cold search는 전체 대상 shard를 hydrate하는 비용이 있다. 오래된 전 기간 검색이 warm 데이터처럼 빠르다고 약속하지 않는다. UI는 cold load 진행을 표시하며 API 제한 시간에 걸리면 명확한 timeout을 반환한다. 다운로드가 완료되어 캐시에 남은 경우 다음 요청이 이를 재사용할 수 있지만 지속적인 백그라운드 검색 job 시스템까지 만들지는 않는다.

## 26. 자원·timeout·동시성

1 vCPU, 512 MiB RAM, local SSD를 저사양 테스트 환경으로 유지한다. 이 크기에서 모든 최대 크기 요청·모든 동시 검색·모든 cold 작업이 동시에 성공한다고 보장하지 않는다.

Indexer logical writer는 1개다. Tantivy worker 수와 memory budget을 명시한다. writer 32~64 MiB, 수신·정규화 예약 64 MiB, 집계 16~32 MiB, foreground search 동시성 1~2, upload 1, hydration 1부터 시작한다. 나머지 RAM은 SQLite cache, native segment reader, stored document decompress, allocator, HTTP, page cache 영향에 필요하다.

숫자 예산의 단순 합이 RSS 보장은 아니다. mmap/page cache와 temporary copies까지 실제 cgroup memory와 RSS로 측정한다. 기본 로그 목록에는 축약 message와 공통 필드만 보내고, 최대 1 MiB raw_json은 상세 조회로 분리한다. 1,000건 목록 요청이 1,000개 raw payload를 동시에 보유하게 하지 않는다.

query 길이 8 KiB, Boolean clause 64, nesting 16, exact bucket 20,000, 기본 HTTP 검색 제한 30초로 시작한다. regex와 leading wildcard는 활성화하지 않는다. prefix도 library query 구조를 통해 폭발적인 확장을 제한하고 자원 초과를 반환한다.

`tokio::time::timeout`이나 `spawn_blocking` handle 취소만으로 실행 중인 동기 검색 CPU가 중단된다고 가정하지 않는다. semaphore permit은 HTTP 응답이 끝난 시점이 아니라 **실제 blocking 작업이 끝날 때까지** 보유한다. 취소 신호를 지원하는 경로에서는 shard 사이에서 중단하고 native collector의 자원 제한을 적용한다. timeout된 작업이 남아 있는데 새 작업을 무제한 쌓는 동작은 금지한다.

수신을 우선하지만 query와 backup을 무조건 기아 상태로 만들지 않는다. 대기열 길이·작업 종류·최대 수행량을 관찰하고 짧은 bounded 작업으로 나눈다. 저사양 목표를 위해 정확성이나 durability를 무단 완화하지 않는다.

## 27. 경보

필수 조건은 new issue, regression, Error count threshold, Log query count threshold다. 전송 대상은 webhook 필수, SMTP email 선택이다. API·UI에서 경보 이름, 대상 프로젝트, query, window, threshold, cooldown, destination을 설정한다.

New issue와 regression은 Indexer의 SQLite 확정 transaction에서 `alert_deliveries`에 기록한다. 예를 들어 `(alert_id, condition_kind, issue_id, trigger_ingest_seq)`로 결정적인 dedupe_key를 만든다. record 저장과 경보 기록이 따로 성공하지 않도록 한다.

threshold는 기본 60초마다 공통 search/aggregation 경로로 평가한다. evaluation window의 기준 시각과 마지막 완료 경계를 기록하고 반복 평가가 같은 delivery를 중복 생성하지 않도록 한다. 작은 jitter나 실패 뒤 catch-up이 같은 경보를 여러 번 보내지 않게 dedupe key와 cooldown을 적용한다.

경보는 기본적으로 최근 수신된 데이터의 received_at window로 평가하고 UI에 그 의미를 표시한다. 발생 시각 기반 window가 필요하면 명시적으로 선택하게 한다. 지연 전송된 Error가 과거 시간으로 들어왔다고 경보에서 조용히 사라지는 일을 피한다.

필요한 shard를 읽지 못한 incomplete query나 timeout은 정상 0건이 아니다. 경보 평가 실패 상태를 저장하고 재시도한다. “오류 없음”으로 처리하지 않는다. 임계값 비교에서 아직 공개되지 않은 ingest를 포함하지 않는다.

전송은 SQLite outbox에서 읽고 exponential backoff와 최대 간격, 운영자가 확인할 수 있는 failed 상태를 둔다. 프로세스 종료 전에 외부 webhook이 수신했지만 SQLite sent 갱신 전일 수 있으므로 **at-least-once 전송**이다. 안정적인 delivery ID를 header/body에 제공하여 수신 측이 중복 제거할 수 있게 한다. 외부 webhook exactly-once는 주장하지 않는다.

Webhook에는 TLS 검증·응답 크기·timeout·redirect 제한을 적용한다. destination은 관리자만 변경한다. 기본은 private/link-local/metadata endpoint 등 위험한 목적지를 차단하고, 사내 webhook이 필요한 경우 명시적인 허용 설정을 제공한다. IP 검증 후 redirect/DNS 재해석으로 우회하지 않도록 같은 HTTP 정책을 사용한다.

## 28. UI와 조회 기능

다음 화면을 구현한다.

| 화면 | 필수 기능 |
|---|---|
| Setup / Login | 일회성 setup, 관리자 생성, 로그인/로그아웃 |
| Dashboard | 수집량, Issue 요약, 최근 Error/Log histogram, storage health |
| Issues | 상태·프로젝트·레벨·기간·제목 검색, last_seen 정렬 |
| Issue detail | 상태 변경, first/last seen, count, release/environment, occurrence 목록 |
| Occurrence detail | exception, stacktrace, request, user, tags, contexts/extra, breadcrumbs, raw |
| Related Logs | trace/request 연결 또는 시간 기반 추정과 사용한 연결 기준 |
| Logs | 검색창, 시간, 레벨/서비스/환경/릴리스 필터, histogram, Live, 행 상세 |
| Explore | 공통 query + metric + group-by + histogram, table/bar 표시 |
| Projects | 생성·수정·비활성화, DSN/key 발급·폐기, SDK 예시 |
| Alerts | 조건/대상/cooldown 설정, 최근 평가와 전송 실패 |
| Storage / System | 로컬·S3·backup·Inbox·resource 상태, 제한과 실패 원인 |

Logs field panel은 기본 필드와 선택한 결과의 JSON path/type/value를 보여준다. 샘플에서 발견한 attribute 목록을 전체 데이터의 완전한 schema처럼 표시하지 않는다. 전역 field discovery를 위해 모든 Record를 SQLite에 다시 저장하지 않는다.

stacktrace는 SDK가 보낸 frame과 context를 표시한다. source map이나 native symbol이 없는 minified stack을 원래 소스로 복원한 것처럼 표시하지 않는다. Raw JSON, log message, exception, stack source line은 전부 untrusted text로 렌더링하고 HTML을 실행하지 않는다.

Error 상세 직접 조회는 SQLite occurrence의 shard_id/record_id를 사용한다. Log 검색 결과에는 shard_id와 record_id를 담은 서명된 opaque detail token을 포함한다. 세부 조회를 위해 모든 shard를 검색하지 않는다. 토큰 검증 후에도 project 조회 권한을 다시 확인한다.

목록 response에는 전체 raw_json을 넣지 않고 축약 message를 사용한다. 펼친 상세에서 stored payload를 읽는다. message가 길면 잘렸음을 표시하고 원문 상세로 연결한다.

## 29. REST API와 보안

```text
Health
  GET  /healthz
  GET  /readyz

Setup / Auth
  POST /api/setup
  POST /api/auth/login
  POST /api/auth/logout
  GET  /api/auth/me

Projects
  GET   /api/projects
  POST  /api/projects
  GET   /api/projects/{id}
  PATCH /api/projects/{id}
  POST  /api/projects/{id}/keys
  DELETE /api/projects/{id}/keys/{key_id}

Issues
  GET   /api/issues
  GET   /api/issues/{id}
  PATCH /api/issues/{id}
  GET   /api/issues/{id}/events
  GET   /api/issues/{id}/related-logs

Records / Search
  GET  /api/logs
  GET  /api/records/{detail_token}
  GET  /api/logs/live
  POST /api/explore/search
  POST /api/explore/aggregate

Alerts
  GET    /api/alerts
  POST   /api/alerts
  PATCH  /api/alerts/{id}
  DELETE /api/alerts/{id}

System
  GET  /api/system/status
  GET  /api/system/storage
  POST /api/system/storage/test
```

UI와 API는 same-origin을 기본으로 한다. 브라우저 SDK ingest에 필요한 CORS와 OPTIONS는 별도로 제공하며 admin API에 같은 광범위 CORS를 적용하지 않는다. Ingest는 cookie credential이 필요하지 않다.

관리자는 Argon2id password hash와 session cookie를 사용한다. Session 원문 token은 DB에 저장하지 않고 hash만 저장한다. cookie는 HttpOnly, SameSite=Lax, 운영 HTTPS에서 Secure다. 상태 변경에는 CSRF 방어와 Origin 검증을 적용한다. 로그인·setup endpoint에 별도 시도 제한을 둔다. 실제 hashing 메모리 비용과 동시성을 512 MiB 예산에 포함한다.

첫 설치는 랜덤한 일회성 setup token을 생성하고 hash·만료를 저장한다. 최초 관리자 생성은 하나의 transaction으로 처리하여 두 요청이 각각 관리자 초기화를 완료할 수 없게 한다. setup URL의 token을 access log/referrer에 흘리지 않는다.

ingest 공개 key로 admin 또는 조회 API에 접근하지 못하게 한다. 프로젝트 비활성화는 key 사용을 막고 모든 query에서 제외한다. immutable shard를 즉시 재작성하는 물리 purge는 v1 관리 API의 비활성화와 구분한다. “프로젝트 비활성화”를 “모든 저장 사본의 영구 삭제”라고 표시하지 않는다. explicit project purge/reindex는 원문의 향후 기능 범위로 남긴다.

## 30. 운영 상태·CLI·설정

`/healthz`는 프로세스 생존 확인이다. `/readyz`는 SQLite 사용 가능, Indexer/reader 복구 완료, 기본 조회·수신 경로의 안전성을 확인한다. S3 단독 실패로 local-only 가능한 전체 서버를 죽이지 않는다. 디스크 압력에 의한 ingest 거절과 조회 가능 상태를 system status에서 구분한다.

Storage UI에는 disk used/free/reserved, meta.db/WAL 크기, active 크기, local/remote shard 수, 다운로드된 로컬 사본 크기, pending upload, archive 검증 상태, 마지막 완료 checkpoint, 실제 복구 가능 시각, backup WAL hold 시간, Inbox Record/bytes/oldest age를 표시한다.

내부 counter는 ingest 요청/bytes/거절 사유, normalized/indexed log/error 수, unsupported item, rejected Record, truncation, Inbox lag, query latency/timeouts/incomplete, bucket limit, open handles, archive/checkpoint 성공·실패, cold hydration bytes/time, 경보 실패를 포함한다. ACK한 Record의 silent drop은 정상 counter로 허용하지 않는다.

자체 로그는 `tracing`으로 stderr에 쓴다. 자기 Sentry endpoint로 자동 전송하지 않는다. raw body·secret·세션 token을 기록하지 않는다. Prometheus 등 외부 모니터링 도구는 운영에 유용할 수 있으나 필수 dependency가 아니다.

```text
observe                      # serve가 기본
observe serve
observe version
observe doctor
observe admin reset-password
```

Doctor는 SQLite integrity/migration, directory 권한/lock, disk reserve, native index format, local manifest, commit 경계, S3 연결, checkpoint 참조 무결성을 검사한다. 전체 archive 다운로드 검증은 비용이 크므로 명시적인 deep 모드로 분리할 수 있다. 정상 진단과 데이터 수정은 분리하고 doctor가 추측으로 파일을 지우지 않는다.

필수 또는 일반 설정은 다음 정도다.

```text
OBSERVE_ADDR=0.0.0.0:8080
OBSERVE_DATA_DIR=/data
OBSERVE_BASE_URL=https://observe.example.com
OBSERVE_S3_URL=s3://bucket/prefix          # 선택
OBSERVE_S3_ENDPOINT=...                   # 선택, 검증된 호환 서버용
OBSERVE_LOG=info
```

AWS credentials와 region은 SDK 표준 설정을 사용한다. 실제 외부 endpoint URL은 administrator가 설정한다. 처음 사용자가 shard 크기, warm_days, checkpoint 주기, merge tuning, 여러 cache limit을 튜닝하게 만들지 않는다. 데이터 마스킹·경보 조건 같은 제품 기능 설정은 이러한 내부 자원 튜닝과 다른 것이다.

Docker production image에는 바이너리와 필요한 CA certificate·런타임 파일만 포함한다. Node.js는 build-time dependency다. `/data` volume을 반드시 안내한다. HTTPS는 reverse proxy/load balancer가 종료할 수 있다. root 실행을 요구하지 않으며 정상 종료 때 수신 중단, Inbox commit, 현재 batch 확정, 제한된 seal/backup 시도를 수행한다. 종료 시 S3가 무한히 응답할 때까지 기다리지 않는다.

## 31. 업그레이드와 포맷 호환성

SQLite migration은 SQL 파일을 embed하고 순서대로 적용한다. 다운그레이드로 더 높은 schema_version을 읽을 수 없으면 중단한다. 파괴적 migration 전에는 검증 가능한 백업을 요구한다.

Tantivy application schema_version, native index format, tokenizer_version, normalizer_version은 서로 다른 문제다. app field schema adapter만 있다고 새로운 Tantivy 버전이 모든 과거 native 파일을 읽을 수 있는 것은 아니다.

기존 index를 읽을 수 있는 라이브러리 버전만 직접 upgrade한다. 지원하지 않는 native format이면 시작 시 명확히 거절하고 호환 바이너리로 수행하는 명시적인 reindex/migration 경로를 마련한다. raw_json이 있다고 읽지 못하는 native store를 마술처럼 변환할 수 있다고 설명하지 않는다.

동일 native format에서 field schema만 바뀌면 기존 active를 seal하고 새 schema의 active를 연다. 기존 shard에 없는 필드는 no-match 또는 명시적인 field-unavailable 의미로 처리한다. 이를 위해 필요한 버전별 소규모 field mapping만 두고 범용 query translation framework를 만들지 않는다.

v1은 자동 background reindex 서비스가 없다. 그러나 유지보수 release에서 오래된 schema를 읽을 수 있는지, pinned SDK parser와 Inbox 버전을 처리할 수 있는지 fixture로 지속 검증한다. 무기한 S3 보관은 포맷 호환성 비용도 무기한이라는 뜻이므로 해당 정책을 릴리스 문서에 남긴다.

## 32. 코드 구조와 금지 사항

```text
observe/
├── Cargo.toml
├── Cargo.lock
├── src/
│   ├── main.rs
│   ├── app.rs
│   ├── config.rs
│   ├── http/             # auth, ingest, projects, issues, search, alerts, system
│   ├── sentry/           # protocol + normalization + scrubbing
│   ├── db/               # SQLite commands, migrations
│   ├── indexer.rs        # 순차 batch 처리와 commit 경계
│   ├── issue.rs          # fingerprint, lifecycle
│   ├── search/           # schema, native query 결합, native aggregation, paging
│   ├── storage/          # shards, archive, backup/restore, disk reservations
│   └── alerts.rs
├── migrations/
├── tests/
│   ├── fixtures/sentry/
│   ├── integration/
│   ├── crash/
│   └── benchmarks/
└── web/
```

이 경로는 역할을 보여주는 기준이며 파일마다 struct/trait/service를 하나씩 만들라는 의미가 아니다. 하나의 구현만 있는 RepositoryInterface/Impl, GenericDatabaseProvider, AbstractShardManager 등을 만들지 않는다. concrete struct와 함수 호출을 우선한다.

금지되는 추가 구성은 Redis/Kafka/PostgreSQL/ClickHouse/Elasticsearch/OpenSearch/RabbitMQ, 별도 worker executable, microservices, custom WAL, custom inverted index/document store/columnar DB/payload pack, 자체 query lexer/AST/compiler, 자체 범용 집계 엔진, S3 protocol/credential resolver다.

테스트에 필요한 Clock, ObjectStore/AlertSender 같은 작은 경계는 허용한다. 아직 다른 구현이 없는데 미래 확장을 위해 모든 I/O를 추상화하지 않는다. 현재 실제로 검증할 경계만 둔다.

## 33. 테스트 계약

### 33.1 SDK와 민감정보

고정한 SDK별 실제 HTTP fixture로 정상 Error/Message/Logs, 모든 레벨, 압축, 다중 item, unsupported item, 마지막 newline 없음, UTF-8 길이, 잘못된 length, 길이 초과, malformed 지원 item을 테스트한다.

DEBUG는 SDK 기본 설정과 명시적 DEBUG 설정을 각각 테스트한다. ERROR Log + Event 동시 전송은 기대된 두 kind로 구별한다. Celery fork/worker 종료, FastAPI 종료, Browser integration 설정을 실제 전송으로 검증한다.

민감한 sentinel 값을 header/query/nested JSON에 넣고 meta.db·WAL·snapshot·stored raw·search_text·internal log에 남지 않았는지 검사한다. 이 테스트는 알려진 마스킹 규칙의 보장이지 모든 가능한 비밀값 검출의 증거가 아니다.

### 33.2 Indexer crash matrix

다음 각 지점에서 SIGKILL 또는 동등한 crash injection을 수행한다.

```text
Inbox transaction 전/후
Tantivy add 전/후
prepare_commit 전/후
Tantivy commit 완료 후 SQLite 확정 전
SQLite 확정 transaction 도중
SQLite 확정 후 reader reload 전
reader reload 후 Live publish 전
중복 Error만 들어 있는 batch의 metadata commit
seal manifest rename 전/후
SQLite sealed 변경 전/후
새 active 생성 전/후 및 catalog 등록 전/후
```

결과는 ACK한 지원 Record의 복구, 동일 Inbox 재처리의 중복 0, event_id 중복 Error의 검색 document/count 중복 0, 경보 outbox 중복 0, C/A 경계 일치다. 예외가 있는 로그의 별도 HTTP 재전송 중복을 무조건 0이라고 주장하지 않는다.

### 33.3 Query·pagination·aggregation

native full text/phrase/field/AND/OR/exclusion/parentheses/range/prefix, 동적 JSON, literal dotted key와 nested key 차이, 문자열 숫자와 실제 숫자, missing/null, Unicode, time 경계, 같은 timestamp의 다수 Record를 검증한다.

수신 순서와 발생 시각을 반대로 섞고 여러 shard에 분산한 결과가 단일 데이터셋 기준 결과와 같아야 한다. 첫 페이지 뒤 새로 수신된 과거 timestamp Record가 같은 cursor의 후속 페이지에 나타나지 않아야 한다.

다중 segment·다중 shard에서 global winner가 모든 local Top-K 밖에 있는 fixture를 만든다. 정확 group-by, count, sum/min/max/weighted avg, histogram을 메모리 기준 계산과 비교한다. count/bucket은 exact, float는 명시한 허용 오차다. 20,001 bucket과 메모리 한도에서 조용한 truncation이 아니라 오류가 발생해야 한다.

권한 filter는 사용자 query의 OR, negation, project_id 값, query parse 오류, detail token 변경으로 우회할 수 없어야 한다. 모든 검색 endpoint와 Live/Alerts/Related Logs가 같은 규칙을 지키는지 확인한다.

### 33.4 S3와 일관된 복구

```text
B까지 indexing → seal → B의 SQLite read snapshot 고정
→ B 이후 수신/인덱싱을 계속 수행
→ B checkpoint 완료
→ B 이후 shard archive만 업로드하고 다음 checkpoint는 미완료
→ local data_dir 전체 제거
→ B checkpoint 자동 복구
```

이때 B의 Issue/occurrence/Record가 일치해야 하고, B snapshot의 pending Inbox만 한 번 적용되어야 한다. B 이후 미참조 archive를 자동으로 섞어 중복 생성하지 않아야 한다.

또한 archive 업로드 중 종료, archive 완료 후 remote manifest 전 종료, snapshot upload 후 checkpoint 전 종료, checkpoint 완료 후 latest 갱신 전 종료, latest 손상, checksum 불일치, 누락된 object, S3 접근 실패, 잘못된 installation prefix, cold download 중단, tar traversal을 테스트한다.

복구 완료 후 B의 Issue count/status, project/key, query 결과, pending Inbox 처리, cold 상세 조회를 검증한다. 단순히 “로그 몇 건이 검색된다”만으로 복구 테스트를 통과시키지 않는다.

### 33.5 자원·Live·경보

20 MiB 요청 여러 개, 압축 bomb, 넓고 깊은 JSON, 거대한 log batch, disk full, WAL growth, 오래 걸리는 backup, pinned shard eviction 경쟁, 동시 동일 shard hydration, 느린 SSE client, reconnect 중 late event, broadcast lag를 검증한다.

검색 HTTP timeout 후에도 실제 blocking 작업 permit이 해제되지 않아 무제한 작업이 추가되지 않는지 검사한다. 경보 전송 완료 직후 crash 시 동일 delivery ID 재전송이 가능함을 확인한다. 대상을 읽지 못한 alert query가 정상 0건이 되면 실패다.

## 34. 벤치마크와 완료 기준

100K, 1M, 10M Record dataset을 사용한다. 현실적인 message 길이, 10~20개 attribute, 높은 cardinality, stacktrace/breadcrumb가 있는 Error, 지연 도착, 모든 레벨을 포함한다. 작은 문자열만 반복한 테스트는 부족하다.

기준 환경은 1 vCPU / 512 MiB / local SSD다. 초기 목표는 100 logs/sec + 5 errors/sec를 지속 수집하면서 OOM과 무제한 Inbox 증가가 없는 것이다. ACK p95 50 ms, 선택적인 warm 구조화 검색 p95 200 ms, 선택적인 warm text 검색 p95 500 ms, 최근 15분 histogram p95 500 ms는 **달성 여부를 측정할 목표**다.

모든 측정에는 Record 크기, 활성/전체 shard 수, time range, hit ratio, 동시성, cold/warm cache, CPU, RSS/cgroup memory, SQLite/WAL 크기, index 크기, ACK/index visibility lag, S3 전송량, backup 소요 시간, p50/p95/p99를 함께 기록한다. 10M Record라는 총량만으로 응답 시간 보장을 주장하지 않는다.

512 MiB에서 검색·집계·백업·cold hydrate를 함께 실행한 stress 결과를 별도로 남긴다. 달성하지 못하면 실제 측정에 따라 자원 목표를 수정하거나 내부 정책을 조정한다. 아직 실행하지 않은 benchmark를 제품 성능으로 기재하지 않는다.

## 35. 구현 순서

| 단계 | 구현 | 통과 조건 |
|---|---|---|
| 1 | 라이브러리 spike: native parser, typed JSON fast fields, commit payload, sort, exact bucket 수집, pinned SQLite backup | 작은 실행 테스트에서 핵심 API·불변조건 검증 |
| 2 | 바이너리/config/lock/SQLite migration/setup/project/key | 외부 서비스 없이 로그인·DSN 발급 |
| 3 | SDK fixture parser + 검증/마스킹 + durable Inbox | 지원 payload만 안전하게 ACK |
| 4 | 순차 Indexer + commit 경계 + reader 공개 | crash matrix에서 중복·누락 방지 |
| 5 | Issue grouping/lifecycle/occurrence + Error UI | 동일 오류 grouping, resolve/regression 정확 |
| 6 | Structured Logs + native QueryParser + JSON 필터 + cursor | 검색 언어 구현 없이 요구 검색 지원 |
| 7 | native aggregation + histogram + Logs/Explore UI | 단일/다중 shard 기준 결과 일치 |
| 8 | rollover + 고정 경로 + handle/pin/디스크 예산 | seal 장애·late timestamp 검색 정확 |
| 9 | S3 archive + checkpoint backup/restore + cold search | 전체 디스크 삭제 후 일관된 복구 |
| 10 | Live + correlation + Alerts + 운영 UI | 재연결·권한·경보 재시도 테스트 통과 |
| 11 | SDK 확장 fixture + 보안/자원 stress + benchmark | 아래 완료 조건 충족 |

마스킹, body limit, durability, permission은 마지막 hardening 단계까지 미루지 않는다. 저장이 시작되는 단계부터 포함한다. 구현을 위해 별도 검색 언어를 만들자는 변경은 허용하지 않는다.

## 36. Definition of Done

| 영역 | 완료 조건 |
|---|---|
| 설치 | single binary/container, 외부 DB 없음, S3 없이 실행 |
| Sentry | 지원 SDK 버전별 실제 Error/Message/Structured Log fixture 통과 |
| Error | grouping, occurrence, stacktrace/detail, 상태 변경, regression |
| Logs | 지원 레벨 수집, JSON 상세, 필터, Live 및 reconnect |
| Search | native text/phrase/prefix/Boolean/range, JSON field, 시간, cursor |
| 정확성 | late/out-of-order + 다중 shard 결과 일치, 권한 우회 불가 |
| Analytics | count/sum/min/max/avg, 필수 group-by, histogram, bucket 누락 없음 |
| Correlation | trace/request/시간 기반 Related Logs 및 방식 표시 |
| Storage | 자동 non-empty rollover, archive, 안전한 eviction, cold hydration |
| Recovery | C/A crash recovery, 일관된 SQLite/S3 checkpoint, 전체 디스크 유실 복구 |
| Alert | new/regression/query threshold, webhook, durable retry/실패 표시 |
| 운영 | setup/auth, projects/keys, health, doctor, storage/backup lag 표시 |
| 품질 | security, crash, disk pressure, S3 failure, 10M benchmark 결과 문서 |

이 표의 기능을 누락한 상태를 “아키텍처가 단순하다”는 이유로 완료 처리하지 않는다. 동시에 원문이 제외한 HA/APM/Replay 등의 기능까지 확대하여 단순성을 잃지 않는다.

## 37. 원문 대비 변경 추적

| 원문 영역 | 유지한 기능 | 제거·수정한 구현 |
|---|---|---|
| §7, §11~15 | durable ACK, 오류 dedupe, crash recovery | raw-first 저장, lease, Record별 복구 lookup 제거. sanitized Inbox + 순차 commit 경계 |
| §16~20 | 정규화, 마스킹, fingerprint, lifecycle | 저장 전 마스킹, 결정적 Issue ID, backlog에 의한 가짜 regression 방지 |
| §23 | typed JSON, full text, raw detail, aggregation | JSON fast field·keyword 의미·포맷 버전 명시 |
| §24~29 | active 하나, 자동 seal | active 디렉터리 rename 제거. 고정 경로와 완료 manifest |
| §30~44 | S3 archive, backup, auto restore, catalog 복구 | 독립 DB snapshot과 임의 shard 조합 대신 일관된 checkpoint |
| §35~37 | 자동 공간 회수, cold cache/search | cache와 sealed 로컬 사본 통합. pin/공간 예약 추가 |
| §46~50 | full text, phrase, Boolean, JSON, range, prefix | **Observe DSL·parser·AST·compiler 전부 제거** |
| §54~55 | cursor, 다중 shard merge | 전역 sequence + read watermark. 잘못된 최신 shard 조기 종료 제거 |
| §56~59 | exact metrics/group-by/histogram | native aggregation 사용, 중간 Top-K 누락과 float 의미 교정 |
| §60~61 | Live, Related Logs | event-time-only resume와 별도 filter evaluator 제거 |
| §66~67 | 경보와 retry | transaction outbox, at-least-once 명시, incomplete 평가 오류 |
| §70, §88~90 | 저사양 목표와 query protection | bytes 예산, 실행 종료까지 permit 유지, handle/pin 제한 |
| §83~85 | schema upgrade | native index format/tokenizer 호환성까지 구분 |
| §100~114 | fixture, crash/restore/disk/cold/performance tests | checkpoint 경계·late data·exact bucket·timeout 누수 검증 추가 |

## 38. 검증 근거와 남은 검증 범위

원본의 제품 목적과 v1 기능 목록은 업로드 문서를 기준으로 유지했다. 이 문서의 처리 순서·예산·복구 지점 설계는 그 원문을 검토하여 제안한 수정안이다. 다음은 라이브러리·protocol 확인에 사용한 공식 자료다. 문서 전체를 기존 라이브러리가 자동 구현해 준다는 의미는 아니다.

- [S1] Tantivy 0.26.1 QueryParser: 기존 문법, strict parse, 기본 AND 설정, native AST inspection.
  `https://docs.rs/tantivy/0.26.1/tantivy/query/struct.QueryParser.html`
- [S2] Tantivy IndexWriter: prepare/commit, rollback, merge 종료 API.
  `https://docs.rs/tantivy/latest/tantivy/indexer/struct.IndexWriter.html`
- [S3] Tantivy PreparedCommit와 IndexMeta: persisted commit payload.
  `https://docs.rs/tantivy/latest/tantivy/indexer/struct.PreparedCommit.html`
  `https://docs.rs/tantivy/latest/src/tantivy/index/index_meta.rs.html`
- [S4] Tantivy JsonObjectOptions: indexed JSON, fast fields, dot expansion.
  `https://docs.rs/tantivy/latest/tantivy/schema/struct.JsonObjectOptions.html`
- [S5] Tantivy TopDocs: fast-field와 sort key 기반 수집.
  `https://docs.rs/tantivy/latest/tantivy/collector/struct.TopDocs.html`
- [S6] Tantivy Aggregations: native collectors, intermediate merge, memory/bucket 제한.
  `https://docs.rs/tantivy/latest/tantivy/aggregation/index.html`
- [S7] Sentry Python Envelope 공식 구현: item header와 바이트 단위 payload 처리.
  `https://getsentry.github.io/sentry-python/_modules/sentry_sdk/envelope.html`
- [S8] Sentry Python Logging Integration: structured log capture threshold와 integration 설정.
  `https://docs.sentry.io/platforms/python/integrations/logging/`
- [S9] SQLite Online Backup API와 C API 계약.
  `https://www.sqlite.org/backup.html`
  `https://sqlite.org/c3ref/backup_finish.html`
- [S10] SQLite WAL: read snapshot, concurrent writer, checkpoint 특성.
  `https://www.sqlite.org/wal.html`
- [S11] Sentry SDK fingerprint customization.
  `https://docs.sentry.io/platforms/python/usage/sdk-fingerprinting/`
- [S12] AWS S3 object integrity: full/composite checksum, multipart의 차이.
  `https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html`

이 검토 과정에서는 로컬 SQLite 3.46.1로 WAL read transaction을 고정하고 다른 connection에서 값을 변경한 다음 Online Backup을 수행했을 때 백업이 이전 경계를 유지하는 작은 실험을 통과했다. 이는 backup 경계 방식의 제한적인 확인이다. Rust/rusqlite 구현, Tantivy 양 저장소 crash protocol, 실제 SDK 전송, S3 checkpoint, 512 MiB 성능은 별도 구현·통합 검증 대상이며 완료되었다고 주장하지 않는다.

## 39. 구현 에이전트에 대한 최종 지시

**기능을 줄여 단순하게 만들지 말고, 라이브러리가 제공하는 기능을 다시 구현하지 않아 단순하게 만든다.**

검색은 Tantivy QueryParser, 집계는 Tantivy Aggregation, 수신 내구성과 관리 정보는 SQLite, 오래된 immutable 데이터와 일관된 복구 지점은 S3로 해결한다. 제품에 필요한 접착 코드만 작성한다.

구현 도중 필요한 기능이 생기면 먼저 기존 라이브러리 API로 표현한다. 자체 검색 언어·범용 AST·검색 엔진·WAL·분산 조정기를 추가하지 않는다. 데이터 유실·중복·권한 누수·불완전한 집계를 숨기는 것은 단순화가 아니다.
