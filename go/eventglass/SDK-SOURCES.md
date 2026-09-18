# SDK 소스 조사와 검증 근거

조사일: 2026-09-18. 이 문서는 DESIGN.md의 근거 부록이다. 구현 계약은 [DESIGN.md](DESIGN.md)에 있으며, 소스 확인을 실행 테스트 통과로 간주하지 않는다.

## 1. 고정한 조사 대상

| 대상 | 태그 | 확인한 commit |
|---|---|---|
| sentry-python | 2.69.0 | f186a62b34ea44eb6d0db1d20f535817991c32fd |
| sentry-javascript | 10.73.0 | f109d922f5971e2ade549b6c755524168b101818 |
| sentry-go | v0.49.0 | 78b09d19307aafb162cd57838bd5c72055b14c8c |
| getsentry/develop 공식 명세 | 조사 시 master snapshot | 26cabd61bbd94ac8cffd05f1cacd03950a5576c7 |

SDK 버전은 기존 `tools/sdk-fixtures/`의 Python requirements.lock, Node/Browser package locks, Go go.mod와 대조했다. 새 구현에서는 이 파일들을 수정하지 않고 독립 fixture와 lock을 만든다. 기존 Rust SDK 테스트의 성공은 새 Go 서버의 성공 증거가 아니다.

## 2. Python

확인한 소스:

- [_log_batcher.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/_log_batcher.py): 로그 transport 변환, typed attributes, severity와 trace/span, lost-event category.
- [_batcher.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/_batcher.py): version=2 container, 배치·대기·overflow·fork buffer 처리.
- [integrations/logging.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/integrations/logging.py): breadcrumb/event/log의 별도 경로, 템플릿·parameter·code/logger metadata.
- [utils.py](https://github.com/getsentry/sentry-python/blob/f186a62b34ea44eb6d0db1d20f535817991c32fd/sentry_sdk/utils.py): serialize_attribute 관련 구현 확인; bool/int 구분, 배열 및 fallback 문자열화.

설계에 반영한 사실:

- 기본 배치 기준 100개, 버퍼 상한 1,000개, 주기 약5초라는 SDK 정책이 있다. 서버 visibility만 빠르게 만들어도 SDK 내부 대기는 사라지지 않는다. 테스트는 명시적 flush와 전송 완료를 기다려야 한다.
- severity_number/text가 typed attribute로 전달될 수 있어 top-level 숫자만 읽으면 정보가 빠진다.
- 동일 logging 호출의 breadcrumb·log·error event는 각각 의미가 다르다. 메시지 hash dedupe는 정상 데이터를 잃는다.
- SDK에서 문자열화한 객체를 서버가 다시 객체로 해석하면 wire 의미가 바뀐다.

## 3. JavaScript — Node와 Browser

확인한 소스:

- [logs/envelope.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/logs/envelope.ts): type/content_type/item_count, version2, browser ingest_settings, tunnel DSN.
- [logs/internal.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/logs/internal.ts): scope metadata, severity, trace, parent span attribute, template parameters, sequence, beforeSendLog, lone surrogate 처리.
- [attributes.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/attributes.ts): typed values/arrays/unit와 fallback 처리.
- [transports/base.ts](https://github.com/getsentry/sentry-javascript/blob/f109d922f5971e2ade549b6c755524168b101818/packages/core/src/transports/base.ts): transport buffer, rate-limit filtering, network/queue loss outcomes, 응답 처리.

설계에 반영한 사실:

- 로그 배치 전체가 같은 trace에 속한다고 가정할 수 없다. envelope trace를 모든 log에 복사하지 않는다.
- parent span이 `sentry.trace.parent_span_id` attribute일 수 있다.
- `sentry.message.parameter.N`과 순서 attribute는 원문대로 보존한다. sequence는 글로벌 중복 제거 ID가 아니다.
- Browser의 infer_ip/infer_user_agent 요구와 서버 개인정보 수집 정책은 별개다. 제품이 자동 추론하지 않는다는 사실을 명시한다.
- 조사한 base transport에서 일반적인 무손실 재시도 큐를 보장하지 않는다. 별도 offline transport 같은 선택 기능까지 조사·검증한 것으로 확장하지 않는다.

## 4. Go

확인한 소스(태그 소스 및 동일 버전 로컬 module cache 대조):

- [internal/protocol/item_container.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/internal/protocol/item_container.go): version 없는 items container.
- [interfaces.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/interfaces.go): Log의 time.Time/trace/span/severity/body/attributes wire 필드.
- [log.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/log.go): release/environment/server/SDK attrs, 템플릿의 복수형 `sentry.message.parameters.N` 경로.
- [log_batch_processor.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/log_batch_processor.go): legacy event wrapper를 사용하는 로그 flush 경로.
- [transport.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/transport.go) 및 [transport_test.go](https://github.com/getsentry/sentry-go/blob/78b09d19307aafb162cd57838bd5c72055b14c8c/transport_test.go): envelope log item과 version 없는 payload 예시.

설계에 반영한 사실:

- version 필수 검사로 Go 로그를 일괄 거절하면 안 된다. items container에 wrapper metadata가 더 있어도 알려진 wire 경로로 해석한다.
- timestamp는 RFC3339 time.Time 직렬화 경로도 필요하다. epoch float만 허용하지 않는다.
- 기존 저장소의 Go fixture는 CaptureException/CaptureMessage 중심이다. 이것을 Go structured logs 검증으로 세지 않고 새 fixture를 추가한다.

## 5. 공식 프로토콜

- [Envelopes](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/envelopes.mdx): length는 bytes, 마지막 LF 선택, envelope event_id 우선, event/transaction 합쳐 최대 하나, binary unknown item의 경계.
- [Rate limiting](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/rate-limiting.mdx): X-Sentry-Rate-Limits 문법, 빈 category는 전체, 200에서도 quota header 가능, 429/Retry-After.
- [Event payload](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/event-payloads/index.mdx): event 공통 필드, 시각 표현, fingerprint 등.
- [Client reports](https://github.com/getsentry/develop/blob/26cabd61bbd94ac8cffd05f1cacd03950a5576c7/src/docs/sdk/client-reports.mdx): discarded_events category/reason/quantity, best-effort loss reporting.

제품은 Relay가 아니므로 unknown item의 forwarding/storage를 구현하지 않는다. 혼합 envelope의 지원 항목 처리와 unsupported 진단은 의도적으로 제한한 제품 계약이다. Sentry 서버 grouping 재현·symbolication·APM·Replay 지원은 위 프로토콜을 읽었다는 사실만으로 성립하지 않는다.

## 6. 반드시 추가할 실행 증거

| 구분 | 최소 fixture / oracle |
|---|---|
| Python | exception, captureMessage, logging의 세 경로, enable_logs, 템플릿/배열/큰 정수, trace, flush/fork, before_send/log drop |
| Node | exception, log levels, template parameters, 여러 trace 한 배치, scope attrs, flush, rate-limit/network outcome |
| Browser | 실제 Playwright 페이지, CORS/preflight, exception+logs, typed attrs, envelope DSN/tunnel 형식, 개인정보 추론 비활성 |
| Go | exception/message 외에 실제 logger API 로그, versionless container, RFC3339/nanosecond, parameters 복수형, flush |
| 수동 protocol | version1의 실제 역사적 근거, missing/unknown versions, byte length/EOF/binary, duplicate JSON keys, mixed 지원·미지원, wrong event multiplicity |
| 숫자·문자 | i64 경계/38자리 decimal/초과 정수, typed double1.0, null/missing/empty, arrays/unit, surrogate/Unicode, dotted vs nested paths |
| 응답·압축 | 실제 압축 decoder별 body와 limit, 400/413/415/429/503, category 헤더, 응답 유실·SDK 실제 재시도/drop |
| 분석 결과 | 독립 expected record 집합, SQL rows/count/avg/groups, raw-detail 대응, breadcrumb/log/error 분리, 같은 ID 재전송 |

fixture 결과에는 SDK lock/commit, runtime, config(PII/sampling/log enable/flush 포함), 실제 요청 wire의 hash, scrubbed golden, 기대 records/outcomes, 서버 revision을 남긴다. 테스트 wire 원본은 합성 비밀값만 포함하며 운영 payload를 fixture로 쓰지 않는다. 실패·지원 제외를 숨기기 위해 expected 결과를 서버 출력에서 자동 생성하지 않는다.

## 7. 확인 수준과 남은 범위

- 완료: 위 버전의 관련 수집·로그 소스/프로토콜 조사, 기존 fixture lock과의 대조, 설계 계약 반영.
- 미완료: 새 Go 서버의 SDK wire 실행, DuckDB/native/Parquet 실험, AWS 동작, 512MiB·수평 확장·비용 비교. DESIGN.md G00–G08에서 실행한다.
- 미조사/미보장: Java, .NET, PHP, Ruby, Rust, mobile/native SDK 전체; 과거·미래 모든 버전; 지원 제외된 Sentry 기능의 전체 의미.

새 SDK를 지원표에 넣는 조건은 버전 고정 → 관련 wire 소스 조사 → 실제 localhost 전송 캡처 → 정규화/검색/실패 oracle 통과다. 단순 HTTP200만으로 호환 완료를 선언하지 않는다.
