# 비용 효율 중심 제품 설계

상태: 2026-09-12 사용자 요청에 따른 목표 설계. 아래 C01–C07은 구현 완료를 뜻하지 않는다. 기존 기능의 검증 상태는 커밋과 실행 증거를 따른다.

## 1. 제품 결정

Eventglass는 Issue·Logs·Replay를 조사하는 관측 도구에 수집 정책과 비용 가시성을 결합한다. 사용자는 어떤 데이터를 보존하고, 그 데이터를 검색하는 데 어떤 자원이 쓰이는지 확인할 수 있어야 한다. 비용 절감률 자체보다 필요한 관측 기능을 유지하면서 총 운영 비용을 낮추는 것을 목표로 한다.

사용자가 제품 전반의 변경을 허용했으므로 현재 파일 구조나 화면 배치는 제약으로 취급하지 않는다. 다만 한 운영 SQLite DB, 한 순차 Indexer, durable ACK, commit/finalize/publish, 공통 검색 권한, 완전한 checkpoint 복구는 유지한다. source-design.md는 수정하지 않고 이 문서에서 범위 변경을 명시한다.

첫 제품 범위는 기존 Sentry 수집과 내장 검색이다. 범용 S3 proxy, 외부 검색 엔진별 connector, OTLP metrics/traces 전체 수용은 별도 확장이다. 처음부터 이들을 만들면 비용 효율을 검증하기 전에 운영 대상이 늘어난다.

최적화 순서는 다음과 같다.

1. 처리한 데이터와 사용한 자원을 측정한다.
2. 저장하지 않아도 되는 데이터를 사용자가 정책으로 정한다.
3. 필요한 검색만 실행하고 이미 가져온 shard를 재사용한다.
4. 동일 workload에서 총 비용과 정확성·지연·복구를 함께 비교한다.

## 2. 데이터 흐름과 소유권

```text
Sentry SDK
  → admission / 인증 / 전체 요청 검증 / scrub
  → 수집 정책 평가: off | shadow | enforce
  → DbWorker: 권한·정책 revision 재검사
       보존 Record + 정책 적용 집계 + Inbox를 한 transaction으로 확정
  → ACK
  → 한 Indexer → Tantivy → finalize → publish
  → immutable archive + 완료 checkpoint

권한 있는 검색
  → 공통 scope / 시간·catalog pruning / 원격 읽기 예산
  → 로컬 reader / 로컬 shard 재사용
  → 필요할 때만 cold hydration
  → 정확한 결과 + 실행 사용량
```

수집 정책은 구체적인 순수 evaluator이며 HTTP/DB/S3를 import하지 않는다. SQL transaction은 db의 수신 operation이 소유한다. 비용 가격 계산도 순수 함수로 두며 범용 policy DSL, repository 계층, 별도 WAL을 만들지 않는다.

프론트는 generated API contract, TanStack Query, URL 검색 상태, local form state를 분리한다. 비용 차트를 위해 클라이언트가 모든 이벤트를 다운로드하지 않는다.

## 3. 수집 정책

### 첫 정책 범위

프로젝트마다 활성 policy bundle 하나와 shadow draft 하나를 둔다. bundle당 규칙은 최대 32개다. bundle은 revision과 mode를, 규칙은 순서와 고정 DTO를 갖는다. 이는 초기 자원 상한이며 측정 후 변경할 수 있다. 조건은 명시한 원본 signal 종류, level, service, environment, 정확한 attribute 경로·scalar 값의 일치로 제한한다. 경로는 literal key와 nested key를 구분하는 segment 배열이다. 임의 정규식, 사용자 코드, 자체 query parser를 추가하지 않는다.

현재 Sentry transaction도 내부 RecordKind::Log로 저장되므로 RecordKind만으로 정책을 적용하지 않는다. ingress가 확정한 source signal을 structured_log / transaction / error / replay / legacy_unknown으로 구분하고 사용자 attribute로 위조할 수 없게 한다. 첫 enforce는 structured_log만 허용하며 기존 출처 불명 Record는 자동 재분류하지 않는다.

첫 action은 structured_log 제외와 지정 attribute 제거다. Error와 Replay 전체 제외, 확률 sampling, 메시지 template dedup은 첫 enforce 범위에 넣지 않는다. 원본 occurrence 수를 보존해야 하는 Error identity dedupe와 로그가 반복 발생한 것을 하나로 합치는 행위를 구분한다.

- 활성 bundle의 기본 mode는 off이며 새 bundle은 shadow draft로 생성한다.
- shadow는 정책을 적용했을 때의 차이를 계산하고 실제 Record는 보존한다.
- enforce는 admin의 명시적 활성화로 새 수신에만 적용한다.
- 규칙 순서대로 평가하고 최초 drop에서 종료한다. 필드 제거는 앞선 제거가 반영된 Record에 적용한다. 중복 매칭을 합산해 절감량을 부풀리지 않는다.
- 한 bundle 안에서 shadow와 enforce를 섞지 않는다. active bundle 결과가 실제 경로이고 draft bundle 전체를 대신 적용한 결과가 가상 경로다. 두 경로 모두 같은 scrubbed 입력에서 시작한다. 규칙별 수치는 해당 경로에서의 추가 효과다.
- identity, project, timestamp, level, message, trace/span 연결, issue fingerprint 등 핵심 필드는 제거 대상에서 제외한다.
- 제거한 속성이 raw나 search_text에 남지 않도록 파생 필드를 생성하기 전에 적용한다. scrub은 항상 정책 평가보다 먼저 적용한다.

### 수신·복구 계약

전체 지원 item 검증은 정책보다 먼저 끝낸다. drop 규칙이 malformed payload를 정상 입력으로 바꾸지 않는다. keep Record만 sequence를 할당받는다. 모두 제외된 정상 요청은 정책 집계 transaction이 durable하게 확정된 뒤 성공 처리한다. SDK 응답 호환성은 실제 fixture로 검증한다.

수신 operation은 평가한 정책 revision을 commit 직전에 재검사한다. 변경됐다면 오래된 결정을 저장하지 않고 503 ingest_policy_changed와 Retry-After로 반환한다. 서버 안에서 payload를 복제해 무한 재평가하지 않는다. 승인된 정책 결과와 집계는 동일 transaction에 저장한다. Indexer·복구는 정책을 다시 평가하지 않는다.

요청 재전송에 따른 입력량은 실제 전송 부하로서 재집계할 수 있다. 이것을 unique 이벤트 수라고 부르지 않는다. 동일 Error의 색인 중복 제거는 기존 event identity 계약을 유지한다.

보존하지 않은 데이터는 정책을 끄더라도 복원되지 않는다. 활성화 화면에는 영향 대상, shadow 관측 기간, 건수·bytes, 제외 후 검색·경보의 모집단 변화를 표시한다. 로그 threshold 알림은 보존 데이터 기준이며 원래 전체 유입량의 정확한 수치로 표시하지 않는다.

## 4. 사용량의 정의와 저장

다음 값은 별도 단위를 유지한다.

| 측정 | 의미 |
|---|---|
| 수신 body bytes | 앱이 실제 읽은 HTTP body. 헤더·TLS·SDK에서 이미 제외한 데이터는 포함하지 않음 |
| decoded bytes | 압축 해제한 지원 요청 body 크기 |
| normalized before/after bytes | 동일한 canonical serializer로 측정한 정책 전후 Record 크기 |
| indexed records | identity dedupe 후 Indexer가 반영한 Record 수 |
| archive bytes | 실제 생성한 압축 객체 크기 |
| local bytes / remote byte-hours | 현 시점 로컬 사용량 / 관측 구간의 원격 저장량 적분 |
| 원격 operation / attempt | 논리 SDK 호출과 실제 전송 시도. 재시도를 포함한 attempt를 별도 측정 |
| 원격 수신 bytes | 성공·실패·취소 중 실제 읽은 body bytes. Content-Length 예상치와 구분 |

수신 정책 집계는 운영 SQLite의 고정 시간 bucket에 원자적으로 누적한다. 초기 보존 정책은 시간별 30일, 일별 1년이다. project, source signal, mode, action 같은 제한된 차원만 사용한다. 사용자 message, trace ID, attribute value를 metric label로 쓰지 않는다. 정책 revision별 상세 내역에도 보존 상한을 둔다.

수신 transaction이 확정된 집계와 프로세스의 일시적 성능 카운터를 분리한다. accepted body bytes는 수신 transaction에 기록하지만 인증 실패·중간 body 취소의 관측량은 설치 전체 transport 카운터에만 기록한다. SDK 내부 재시도 attempt 계측이 아직 불가능한 backend는 logical operation만 제공하고 attempt를 unknown으로 둔다. 원격 I/O 집계는 bounded 메모리에서 주기적으로 flush할 수 있으나 crash 직전 누락 가능성, 시작·종료 시각, coverage를 노출한다. 이를 청구서와 일치하는 원장으로 광고하지 않는다. counter overflow와 clock rollback을 명시적으로 처리하고 빈 구간을 0 사용량으로 추정하지 않는다.

필드 분석은 scrub 이후 제한된 표본으로 수행한다. 상위 field 후보 수·깊이·메모리에 상한을 두고 표본율과 기타 항목을 노출한다. 조회 이력에는 임의 query 원문을 장기 보관하지 않는다. 필터에서 쓰인 필드와 상세 조회에서 필요했던 필드를 완전히 알아낼 수 없으므로 미조회만으로 삭제를 자동 추천하지 않는다.

## 5. 검색과 저장 비용

### 현재 형식에서 먼저 개선할 것

현재 archive는 shard 단위 tar.gz이며 검색은 전체 객체를 내려받고 checksum·manifest를 검증한 뒤 로컬 Tantivy에서 실행한다. cold 전역 semaphore는 동시 hydration을 1개로 제한한다. registry의 reader handle LRU와 로컬 shard 디스크 회수는 다른 자원이다.

첫 최적화는 다음 정보를 수집한 뒤 결정한다.

- 검색별 후보 shard 수, 로컬 hit/miss, 실제 다운로드 bytes, cold 대기 시간.
- search/detail/live/alert, backup/restore, replay별 원격 작업 목적.
- shard별 마지막 접근, 제한된 빈도 점수, 최근 회수·재다운로드 여부.
- 압축 크기와 설치 후 크기, download·verify·inflate의 CPU·시간.

동일 shard 수요는 하나의 in-flight hydration을 공유한다. key는 installation과 immutable archive identity를 포함한다. 병렬 hydration 수는 기존 1부터 유지하고 메모리·디스크 예산으로 제한한다. 여러 waiter에게 같은 다운로드 bytes를 중복 청구하지 않는다. 원격 작업의 실제 합계와 query attribution 합계의 관계를 명시한다.

취소된 waiter, 마지막 waiter, 검증 실패, 설치 후 pin 전달, eviction 경합은 기존 복구·pin 계약을 그대로 검증한다. refcount가 높다는 이유만으로 원격 복구 검증 없이 파일을 회수하지 않는다.

검색 화면은 기본적으로 최근의 좁은 시간 범위를 사용하고 절대 범위를 URL에 둔다. 긴 범위의 검색은 실행 전에 catalog가 아는 cold shard 수와 예상 압축 bytes를 보여준다. 예상 크기가 없는 shard는 unknown으로 표시한다. 원격 예산을 넘으면 좁은 범위를 제안하거나 명시적 확장 실행을 받는다. 결과 일부를 정확한 전체 결과로 반환하지 않는다. Alerts/Live에도 bounded 예산과 incomplete 상태가 있어야 한다.

회수 정책은 크기·재사용·재다운로드 비용을 함께 고려하되 먼저 기존 최근 접근 기준과 비교한다. shard 크기를 줄이면 cold 과다 다운로드가 줄 수 있지만 shard 수·GET·reader overhead가 늘므로 실제 workload로 선택한다. 전용 RAM block cache와 prefetch는 기본 비활성이다.

### 원격 형식 변경의 조건

Range GET은 현재 tar.gz 경로 위에 덧붙이지 않는다. 필요성이 입증되면 native immutable 파일/독립 압축 chunk와 버전 있는 manifest를 사용하는 실험을 별도로 한다. checksum 단위, Tantivy Directory 접근, random read, 메모리, checkpoint와 이전 archive 복구를 모두 검증해야 한다.

AWS S3는 한 GET에서 여러 범위를 지원하지 않는다. 병합은 중간의 불필요한 bytes까지 포함하는 연속 범위를 읽는 결정이며, 이득과 추가 전송·메모리를 비교해야 한다. [AWS GetObject 계약](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

## 6. 비용 화면과 가격 모델

탐색·장애 조사 기능을 유지하면서 Usage 화면을 추가한다. 첫 화면은 관측 기간, 입력과 보존 건수·bytes, 실제 원격 다운로드, 로컬 재사용, 정책으로 제외된 양을 보여준다. 프로젝트별 drill-down에서 shadow 정책 작성 → 영향 확인 → 활성화 → 적용 후 비교로 이어진다.

가격은 운영자가 provider/region/currency/적용 기간별 단가를 입력하는 버전 있는 설정이다. 고정된 공용 S3 가격을 모든 설치에 적용하지 않는다. 가격 미설정은 비용 unknown이며 0원이 아니다. 서로 다른 통화를 합산하지 않는다.

```text
추정 비용 = 관측한 저장 byte-hours × 저장 단가
          + 실제 요청 attempts × 종류별 단가
          + 과금 대상 전송 bytes × 경로별 단가
          + 명시한 compute / 로컬 disk 배분 비용
```

포함하지 못한 과금 항목과 coverage를 함께 표시한다. 같은 호스트의 고정 CPU 비용이 입력 감소율만큼 즉시 줄어든다고 계산하지 않는다. 프로젝트가 공유하는 archive/checkpoint 비용은 실제 압축 bytes의 직접 측정과 배분 추정치를 구분한다.

절감은 세 수준으로 표시한다.

1. 실측: 정책 전후 canonical bytes, 실제 drop 건수, 실제 원격 bytes.
2. 추정: shadow 적용 결과와 명시한 단가·압축 가정에 따른 비용.
3. 비교 실험: 동일 seed/workload/hardware에서 baseline 대비 총 비용과 지연.

캐시 hit를 무조건 avoided GET 한 번으로 환산하지 않는다. 절감률 분모와 counterfactual baseline을 명시한다. README에는 재현 가능한 benchmark 결과만 게시한다.

## 7. 구현 순서와 완료 기준

| 단계 | 구현 범위 | 완료 증거 |
|---|---|---|
| C01 | 계측 정의, durable 입력 bucket, 원격 I/O 카운터, Usage API·화면 | 재시작·실패·취소·재시도·동시 요청과 권한/시간 경계 검증. unknown·coverage 노출 |
| C02 | bounded 필드/레벨 분석, revision 있는 shadow 정책 | 원본 Record·검색·Issue·Replay 결과가 baseline과 동일. 겹치는 규칙의 중복 절감 집계 없음 |
| C03 | Structured Log drop/attribute remove enforce, 활성화·이력 UI | 정책/키 revoke race, 전체 drop ACK, malformed rollback, crash recovery, 파생 필드 제거, SDK fixture |
| C04 | query cold 사전 정보·예산, shard 공유 hydration·회수 개선 | 동시 요청 실제 다운로드 수, waiter 취소, pin/evict, 실패 전파, thrash workload 비교 |
| C05 | 단가 설정, 비용 추정·비교, compression/seal 튜닝 | 단위/통화/기간/unknown 계산, 동일 workload end-to-end 비용·성능 보고 |
| C06 | 명시적 보존 기간·논리 만료·checkpoint와 연동한 회수 | 만료 검색/상세 의미, mixed-project shard, 지원 checkpoint 전체 복구, mark/sweep crash·권한 검사 |
| C07 | 조건부 remote format 실험 | 현 구조 대비 비용·지연 개선과 이전 checkpoint 전체 복구 입증 시에만 채택 |

C01–C06의 각 단계는 backend/API/UI와 필요한 migration을 함께 끝내고 검증 후 commit·즉시 push한다. 단계별 일지를 만들지 않고 실제 명령·결과를 commit에 남긴다. C07은 C05의 병목 증거가 없으면 착수하지 않는다.

기존 P00–P12를 삭제하지 않는다. C01은 P03/P09/P10, C02–C03은 P03/P04/P11, C04는 P06/P08/P09/P10, C05–C07은 P12 및 storage 호환 계약과 함께 검증한다. 이미 배포된 migration을 수정하지 않고 새 migration과 이전 DB upgrade fixture를 추가한다. 새 usage·policy 상태도 같은 DB backup/checkpoint에 포함하고 복구 시점 후퇴를 표시한다.

성능 비교에는 수집 위주, 반복 warm 검색, 넓은 cold 검색, 작은 cache의 반복 퇴출, 높은 attribute cardinality, 중복 Error, Replay, S3 장애를 포함한다. 기존 1코어·1GiB gate와 512MiB RC 목표를 구분하고 ACK p95, 검색 p95/p99, backlog, peak memory, disk, CPU, 원격 requests/bytes를 함께 보고한다. 정량 overhead 상한은 C01 baseline 측정 후 고정하며 미측정 절감 목표를 PASS 조건으로 만들지 않는다.

## 8. 확장 판단

OTLP logs 도입 시 기존 normalized Record와 수집 정책 경계를 재사용한다. Collector에서 이미 제공하는 filter/transform 기능을 중복된 범용 DSL로 만들지 않는다. [OpenTelemetry processor 개요](https://opentelemetry.io/docs/collector/components/processor/).

Metrics/traces 전체 지원, 외부 backend gateway, 자동 sampling, 자동 필드 삭제는 별도 설계가 필요하다. C06의 원격 회수도 S3 lifecycle 설정만으로 대신하지 않는다. 비용 경고가 데이터 삭제나 외부 알림 발송 권한을 의미하지 않는다.

## 9. 구현 전에 확정한 세부 계약

### 정규화·정책 경계

현재 normalizer가 scrub와 파생 필드 생성을 연속 수행하므로 정책을 완성된 Record 뒤에 붙이는 방식은 채택하지 않는다. source adapter가 scrubbed 입력과 신뢰할 수 있는 signal metadata를 만들고, concrete policy evaluator가 지원하는 source attribute만 제거한 뒤 기존 normalizer가 immutable Record를 완성한다. source별 raw attribute 위치와 normalized attribute 경로의 매핑을 명시하며 안전하게 왕복 매핑할 수 없는 제거 규칙은 저장 시 거절한다. 임의의 raw 전체 JSON 경로 삭제는 제공하지 않는다.

기존 최대 요청·Record·node·depth 검증은 정책 전에도 적용한다. drop 예정이라는 이유로 입력 검증을 생략하거나 기존 최대 유효 입력을 갑자기 거절하지 않는다. 한 Record에 대해 baseline/actual/shadow projection을 순차 계산하고 크기를 counting serializer로 재며, 전체 요청 DOM을 세 벌 보유하지 않는다. 요청의 모든 item 검증이 끝나기 전에는 정책 집계를 DB에 반영하지 않는다. 정책 evaluator가 실패하면 해당 요청을 원자적으로 실패시킨다.

비교 bytes는 sequence를 0으로 놓은 canonical policy projection을 기준으로 정의한다. 실제 Inbox wrapper와 sequence 자리수는 별도 저장 overhead다. 이를 압축 디스크 절감량으로 표시하지 않는다. shadow 평가 자원이 부족하면 실제 active 정책의 수신은 유지할 수 있으나 해당 shadow window를 incomplete로 표시하며, 누락된 평가를 would_keep으로 집계하지 않는다. active enforce 경로는 생략할 수 없다.

### 데이터 모델

SQL migration 자체는 구현 단계에서 작성한다. 아래는 동일 운영 SQLite 내 소유권과 key 계약이다.

| 데이터 | key와 수명 | 원자성 |
|---|---|---|
| policy bundle/revision | project, immutable revision; active/draft pointer; actor/time 기록 | expected revision 조건부 수정, admin/CSRF 재검사 |
| ingress usage hour | project, UTC received hour, source signal, mode/action | accepted Inbox와 한 transaction; rejected transport는 별도 best-effort |
| policy usage hour | project, bundle revision, UTC hour, rule ordinal; 최대 32개 | 정책의 실제·가상 incremental 효과를 구분 |
| usage day | UTC day + 제한된 차원 | 시간 bucket rollup 완료와 원본 제거를 한 transaction으로 확정 |
| remote usage interval | process epoch, monotonic interval ID, purpose, operation | 주기 flush의 같은 ID는 재적용하지 않음; 유실 window 표시 |
| pricing revision | currency, 단위, 유효 기간, fixed-decimal rates | admin revision 관리; 과거 조회의 사용 단가 명시 |
| retention policy | project/signal, prospective effective time, 보존 기간 | 신규 설정은 기존 데이터에 자동 소급하지 않음 |

usage에는 본문·원본 query·secret·개별 trace ID를 저장하지 않는다. 집계 총합 overflow는 wrap하지 않고 saturated/incomplete 상태를 표시한다. 정책으로 제외된 건수처럼 ACK와 묶인 필수 집계가 안전하게 기록될 수 없으면 ACK를 성공시키지 않는다. retention 없는 policy audit도 revision 증가로 무한히 커지지 않도록 과거 상세를 archive/요약하는 별도 상한을 둔다.

### API와 화면 계약

새 API는 아래 route를 목표로 하며 기존 route와 데이터 조회를 유지한다. OpenAPI/generated TS는 구현 단계에 함께 추가한다. sequence/count/bytes는 10진 문자열, 시각은 기존 μs 계약, 통화는 fixed decimal 문자열이다.

| API | 권한·동작 |
|---|---|
| GET /api/usage | admin 전용 운영 비용 집계; start/end, project, resolution 제한; coverage와 단위 포함 |
| GET /api/projects/{id}/ingest-policy | admin; active/draft/revision과 영향 통계 |
| PUT /api/projects/{id}/ingest-policy/draft | admin+CSRF; expected revision; 검증된 DTO만 저장 |
| POST /api/projects/{id}/ingest-policy/activate | admin+CSRF; 검토한 draft revision을 active로 전환 |
| POST /api/projects/{id}/ingest-policy/disable | admin+CSRF; 새 수신부터 off, 이미 제외된 데이터는 복원 안 됨 |
| POST /api/search/estimate | 실제 search와 같은 권한/scope/parser; catalog만 조회, hydration 없음 |
| GET/PUT /api/cost-pricing | admin; revision 관리와 누락 단가 표시 |

검색 estimate는 짧은 TTL의 서명 토큰에 principal/권한 epoch/query hash/시간 범위/storage generation/예산을 묶는다. 실행 시 권한과 현재 cold 상태를 재확인한다. cache eviction으로 실제 필요량이 늘면 승인 예산을 초과하기 전에 중단하고 새 estimate를 요구한다. estimate를 사용하지 않는 기존 API client에는 서버 기본 예산을 적용한다. bytes/attempt 예약을 통해 동시 요청의 합산 초과도 제한하며, 이미 시작한 요청의 취소 지연만큼 생길 수 있는 초과 상한을 명시한다.

일반 사용자는 공유 shard의 다른 프로젝트 크기·ID·작업량을 보지 않는다. 물리 사용량 상세는 admin에만 노출하고, member에게는 권한 있는 검색의 실행 가능 여부와 예산 초과 상태를 제공한다. 회원의 임의 입력으로 설치 전체 한도를 올릴 수 없다.

화면은 Usage → 프로젝트 원인 분석 → policy draft/shadow → 변경 영향 → 활성화의 흐름이다. 활성화 시 기존 query alert의 모집단 변화와 되돌릴 수 없는 제외를 해당 화면에서 설명한다. 이 제품 동작에 필요한 확인과 코드 구현 승인 요청은 구분한다. Logs/Explore의 장기 검색에는 cold 예산 정보를 연결하고, 정책 적용 기간의 결과에는 보존 데이터 기준임을 표시한다.

## 10. 보존 기간과 원격 용량 회수

수집량을 줄여도 원격 데이터를 무기한 쌓으면 장기 저장 비용은 계속 증가한다. 따라서 명시적인 보존 기간을 C06의 제품 범위에 추가한다. 이는 기존 v1의 physical purge 제외 범위를 확장하는 결정이며, 아래 복구 조건을 충족하기 전에는 실제 삭제를 활성화하지 않는다.

- 기본값은 기존 데이터 보존이다. 프로젝트·signal별 보존 기간은 admin이 설정하며 신규 수신부터 적용한다. 과거 데이터에 소급 적용하는 기능은 별도 dry-run과 명시적 확정을 필요로 한다.
- 만료는 received_at 기준이다. late event의 event timestamp만 보고 수신 직후 삭제하지 않는다. budget 초과가 자동 retention 단축을 일으키지 않는다.
- shared search scope가 만료 경계를 반영한다. rows/aggregate/detail/live/alert가 동일한 가시성을 따른다. cursor/read token 수명 동안 필요한 데이터는 pin 또는 만료 유예로 보존하고, 만료 정책 변경은 generation을 바꿔 기존 토큰을 명시적으로 무효화한다.
- Issue의 역사적 발생 횟수·상태 요약은 보존할 수 있지만, 오래된 상세는 expired로 구분한다. 현재 검색 가능한 occurrence 수와 역사적 총 발생 수를 같은 숫자로 표시하지 않는다. Replay는 session/segment/blob 참조를 함께 계산한다.
- 첫 물리 회수는 shard 전체가 만료된 경우에만 한다. 여러 프로젝트·signal이 섞인 shard는 가장 늦게 만료되는 Record가 기준이다. 따라서 논리 만료량을 즉시 회수 가능한 bytes로 표시하지 않는다. 비용 화면에 expired-but-referenced bytes를 분리한다.
- 보존 기간별 shard 분리나 부분 재색인은 측정 전에는 추가하지 않는다. 한 Indexer/active shard 계약을 유지하며, mixed retention의 회수 지연을 한계로 노출한다.

원격 GC는 설치 내 자신이 생성한 namespace만 대상으로 mark/sweep한다. 지원 중인 모든 완료 checkpoint와 현재 catalog·진행 중 backup/upload/restore 참조를 live set에 포함한다. live set에 없는 immutable object만 후보가 된다. 최근 업로드 보호 기간과 최소 두 번의 분리된 inventory 확인을 거치고, 삭제 직전 보호된 checkpoint set의 generation이 변하지 않았는지 검사한다. checkpoint publication과 GC 보호 집합 변경은 한 소유자가 직렬화한다. 여러 호스트가 동일 namespace에 writer로 붙는 구성은 지원하지 않는다.

새 checkpoint는 논리적으로 만료되어 회수하기로 확정된 shard의 검색 catalog/occurrence 참조를 일관되게 제거한 snapshot을 포함해야 한다. 만료 전 checkpoint는 지원하는 복구 기간 동안 계속 원격 객체를 보호한다. 복구 보존 기간은 최소 2개의 검증된 완료 checkpoint와 운영자가 정한 시간 범위를 함께 충족해야 하며, 새 backup 실패 시 보호 기간을 단순 시각 경과로 줄이지 않는다. 해제할 checkpoint의 manifest를 먼저 지원 목록에서 제외하고 grace가 지난 뒤 객체를 회수한다. 목록·권한·checksum 오류가 있으면 sweep하지 않는다.

지원 checkpoint 목록은 운영 DB만이 아니라 remote에 버전 있는 manifest와 조건부 갱신 pointer로 보존한다. retirement가 durable하게 기록되기 전에 참조 객체를 지우지 않는다. 복구의 latest fallback도 이 목록을 검사하며, 임의 listing에서 발견한 지원 종료 checkpoint를 되살리지 않는다. GC 도입 이전 installation은 기존 모든 완료 checkpoint를 보호하는 목록을 만든 뒤에만 회수를 시작한다. pointer/manifest를 읽거나 검증할 수 없으면 복구·GC를 fail closed 처리하며 빈 설치로 재초기화하지 않는다.

복구는 지원 목록에 있는 checkpoint만 선택하고, 과거 checkpoint에서 복구된 retention 상태로 현재 시각 기준 만료를 다시 적용한다. 지원 종료 checkpoint는 복구 가능하다고 표시하지 않는다. GC 결과·실패·회수 bytes는 durable하게 기록한다. S3 lifecycle로 참조 객체를 독립 삭제하는 설정은 허용된 기본 구성에 포함하지 않는다. crash 각 경계와 old/new checkpoint 복구가 검증되지 않으면 C06은 완료가 아니다.

## 11. 채택 결정과 실험 항목

이 설계에서 확정한 것은 제품 범위, 정책/ACK 의미, 단일 소유권, 데이터 종류 보호, 계측 단위·권한, bounded state, API 흐름, 보존·복구 계약과 단계 순서다. C01–C06은 목표 구현이며 아직 제공 중인 기능으로 광고하지 않는다.

실험으로 결정할 값은 shard 크기·seal 간격, compression 설정, cache 점수와 예산, 원격 동시성, analyzer 표본율, 계측 overhead 상한이다. 이 값들은 baseline 없이 숫자를 고정하지 않는다. C07의 remote 형식은 채택하지 않은 실험 대안이다. 실제 운영 단가·보존 기간은 사용자가 설치별로 입력하는 제품 설정으로 남기며, 구현 착수 전에 다시 물어야 하는 미해결 아키텍처 항목으로 취급하지 않는다.

이번 설계 작업은 문서만 변경한다. 실행 코드, migration, generated API, 인프라·배포 설정은 변경하지 않는다. 구현과 배포는 설계 검토 후 별도 작업으로 진행한다.
