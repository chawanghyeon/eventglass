# 별도 관리 없이 동작하는 비용 효율 설계

상태: 2026-09-12 사용자가 구현과 100% 테스트 커버리지 검증을 승인한 목표 설계. 이전 C01–C07 및 A01–A03 계획을 대체한다.

## 1. 제품 기준

Eventglass의 기준은 **사람이 비용 정책·캐시·압축·검색 예산을 관리하지 않아도, 데이터를 믿고 맡기면 알아서 잘 동작하는 것**이다. 기존 코드는 필요하면 바꾼다. 기존 코드의 보존보다 데이터 신뢰성, 검색 품질, 적은 운영 부담을 우선한다.

사용자는 기존 설치와 SDK 연결을 끝내면 된다. 비용 최적화용 설정·정책 작성·shadow 승인·가격 입력·주기적 튜닝을 추가하지 않는다. 비용 관련 화면은 운영자가 궁금할 때 읽는 설명이지, 제품을 정상 사용하기 위한 작업 목록이 아니다.

자동화할 판단은 기계적으로 확인 가능한 자원 효율이다. 동일 객체 재사용, 중복 다운로드 억제, 복구 가능한 로컬 사본 회수, 압축, 제한된 작업 동시성은 시스템이 결정한다. 로그가 중요하지 않다는 추측으로 수집 데이터를 버리거나 임의의 보존 기간을 정하지 않는다.

다음 결과는 유지한다.

- 지원 입력의 durable ACK와 원본 의미·필드 보존, 기존 Error identity 중복 제거.
- 한 운영 SQLite·한 순차 Indexer, commit/finalize/publish, Issue count·상태·경보 의미.
- 공통 검색 권한과 정확한 전체 결과·집계·paging·Live·Replay.
- 완료 checkpoint를 통한 전체 유실 복구와 checksum·manifest 검증.

source-design.md는 바이트 그대로 유지하고, 실제 변경 계약은 이 문서와 구현 단계의 검증으로 관리한다.

## 2. 사용자가 겪는 동작

| 상황 | Eventglass가 자동으로 할 일 | 사용자에게 요구하지 않을 일 |
|---|---|---|
| 평소 수집·검색 | 기존 경로로 처리하며 가벼운 내부 계측 수행 | 수집 정책 작성·비용 단가 입력 |
| 같은 cold 데이터를 여러 요청이 조회 | 검증된 한 번의 hydration을 공유하고 로컬 사본 재사용 | 캐시 활성화·용량 튜닝 |
| 로컬 디스크 여유가 줄어듦 | 기존 복구 검증·pin 조건을 지켜 재다운로드 가능한 사본부터 회수 | shard를 찾아 직접 삭제 |
| 수집·검색과 압축이 경합 | 필수 업무와 checkpoint 진척을 보호하며 부가 분석·최적화 작업을 늦춤 | worker 수·압축 level 조절 |
| 반복 조회하는 데이터가 있음 | 제한된 재사용 통계로 그 사본을 더 오래 유지 | hot field/shard 지정 |
| S3가 잠시 실패함 | 로컬 검색 유지, 기존 bounded retry/backoff와 복구 상태 설명 | 캐시 정책 전환·반복 재시작 |
| 프로세스 재시작 | 기존 durable 상태로 복구하고 보수적인 기본 정책에서 자동 재개 | 최적화 통계 복원·재학습 실행 |

검색 중 비용 승인이나 범위 축소 확인을 요구하지 않는다. 비용 예산을 이유로 기존에 가능한 검색을 새로 막거나 일부 shard만 검색하고 전체 결과처럼 반환하지 않는다. 기존 자원 한도·timeout·권한 오류는 정직하게 유지한다.

## 3. 구조: 업무 경로와 자동 최적화 분리

```text
수집 → 기존 검증·정규화 → durable Inbox/ACK → 순차 Indexer → publish
검색 → 공통 scope → 로컬 reader 또는 공유 cold hydration → 정확한 결과
                           ↑
            기존 storage budget·registry·maintenance
                           ↑
            bounded 사용량 관측 + 구체적인 자동 선택 함수
```

HTTP는 새 정책 엔진을 호출하지 않는다. 수집 transaction에 비용 원장 쓰기를 추가하지 않는다. 구체적인 storage 선택 함수는 기존 app/storage의 소유권 안에 두고 별도 Gateway·서비스·DB·WAL·범용 scheduler/DSL을 만들지 않는다.

기존 disk reservation, query/indexer permit, registry pin, checkpoint 증거가 실행 가능 여부를 결정한다. 최적화 점수는 이 안전 조건을 덮어쓰지 못한다. 관측 통계는 correctness의 근거가 아니며 없어도 기존 경로로 동작한다.

초기 자동 선택은 이미 안전성이 검증된 대안 사이에서만 수행한다. 운영 중 임의의 cache 알고리즘이나 compression 조합을 탐색하지 않는다. 새로운 대안은 동일 workload benchmark와 장애 검증을 거친 뒤 후보로 편입한다.

## 4. 알아서 줄일 비용

### 4.1 같은 원격 데이터를 한 번만 가져오기

현재 ColdStorage는 전역 semaphore와 catalog 재확인으로 이미 중복 hydration을 억제한다. 이를 baseline으로 삼아 불필요한 중복이 실제 발생하는 경계를 먼저 검증한다. 필요하면 installation + immutable archive identity를 기준으로 in-flight 결과를 공유하되, 전체 hydration 동시성과 메모리·디스크 한도는 기존 안전한 값에서 시작한다.

한 waiter 취소가 다른 검색을 실패시키지 않아야 한다. 마지막 waiter가 사라진 경우의 취소, 취소 중 blocking 작업의 permit 수명, checksum/manifest 실패, 설치 후 pin 전달과 eviction 경합을 검증한다. 실패를 성공 결과로 cache하지 않는다. 이미 설치된 객체는 기존 catalog/manifest 계약으로 재사용하고, 권한 검사는 각 요청에서 유지한다.

조회 비용을 계측하기 위한 추가 S3 HEAD/GET/LIST는 만들지 않는다. 이미 진행하는 읽기에서 얻은 bytes·결과를 이용한다. 여러 query가 다운로드 하나를 공유한 경우 실제 원격 bytes를 query 수만큼 중복 합산하지 않는다.

### 4.2 캐시를 사용자가 관리하지 않게 하기

현재 disk budget의 안전 여유와 reservation을 기준으로 한다. 머신의 전체 RAM을 임의로 cache로 사용하거나 별도 RAM block cache를 기본 추가하지 않는다. 기존 OS page cache·Tantivy reader·로컬 shard를 활용한다.

회수 후보는 **완료 checkpoint로 복구가 검증된 사본이며 pin이 없는 shard**로 제한한다. active·복구 불가능 사본·진행 중 설치는 후보가 아니다. 업로드 성공만으로 회수를 허용하지 않는다.

후보 선택은 최근 접근을 기본으로 하고, 최근 반복 조회와 회수 후 재다운로드가 확인된 shard에는 제한된 보호를 더한다. 보호는 필수 disk reserve를 넘길 권한이 아니며, 공간 확보가 필요하면 안전한 후보 중 회수할 수 있다. 초기에 접근 증거가 없으면 기존 최근 접근 순서를 쓴다.

접근 이력은 메모리 상한을 갖는 제한된 집합으로 유지하고 초과 시 오래된 통계를 버린다. query마다 SQLite에 별도 쓰기를 하지 않는다. 통계를 잃으면 기존 정책으로 돌아갈 뿐 검색 결과나 복구 가능성은 바뀌지 않는다. scoring과 후보 탐색 비용에도 상한을 둔다.

prefetch는 기본으로 추가하지 않는다. 아직 요청되지 않은 객체를 가져와 전송·디스크·CPU를 소비하는 것은 자동 최적화의 선행조건이 아니다.

### 4.3 압축과 작업 경합을 내부에서 처리하기

첫 baseline은 기존 archive 형식과 compression 설정이다. 저장 bytes가 조금 줄더라도 ACK·검색 지연·checkpoint 진척을 악화시키면 더 나은 정책이 아니다.

첫 후보 비교는 실제 SDK Log 정규화·native shard로 만든 반복/다양 로그 각 4,000건과 빈 shard를 사용한다. 동일 tar 입력의 gzip level 1/6을 각각 3회 측정하고 모든 후보를 기존 hydrate로 복구한다. Linux 1코어·1GiB에서 두 비어 있지 않은 workload 모두 저장 bytes 5% 이상 감소, 압축 시간 중앙값 증가 10% 이내일 때만 후속 ACK/query/checkpoint 경합 검증 대상으로 채택한다. 시간은 파일 flush/sync를 포함하며 가격·총 운영비로 환산하지 않는다. 빈 shard의 비율 개선만으로 후보를 채택하지 않는다. 이 사전 기준을 통과하지 못하면 빠른 기본 압축을 유지한다. test profile 측정은 후보 선별용이며 production 지연이나 요금 절감의 증거로 쓰지 않는다. 채택하려면 release artifact의 경합 검증까지 통과해야 한다.

동일 tar.gz 형식을 읽을 수 있는 검증된 compression 후보만 비교한다. 후보는 raw 의미, native index, manifest/checksum 검증과 restore 호환성을 유지해야 한다. 실제 이득이 있는 후보가 확보되기 전에는 기존 설정을 사용한다. 새 archive에만 적용하며 과거 archive를 다시 내려받아 재압축하지 않는다.

선택 신호는 기존 backlog, 요청 대기, compression 소요 시간·출력 크기, 메모리/디스크 reservation이다. 보수적인 기본값에서 시작하고 신호가 부족하면 그대로 유지한다. 최대 압축률을 목표로 하지 않는다.

새 설정 선택은 archive 경계에서만 적용한다. 한 archive 처리 도중 설정을 바꾸지 않는다. 실패하거나 기대와 달리 자원 경합이 커지면 다음 작업부터 기본값으로 돌아간다. 이미 ACK한 데이터·성공한 archive를 rollback하지 않는다.

필수 indexing·query·checkpoint와 부가 분석을 구분한다. 부가 작업은 신규 업무가 대기할 때 시작하지 않는다. checkpoint는 부가 최적화로 취급해 무기한 미루지 않으며 기존 deadline·RPO 진척을 보존한다. 새 controller가 기존의 여러 자원 소유자를 우회하지 않도록 실행 판단은 기존 app/storage admission 경계에서 한다.

## 5. 자동 판단을 위한 최소 계측

설치 전체의 고정 operation/purpose counter와 bounded timing 요약부터 시작한다. message·query 원문·trace ID·임의 attribute를 label로 저장하지 않는다. 원본 body/Record를 계측용으로 복제·재파싱·재직렬화하지 않는다.

필요한 값은 수집/검색 부하, 실제 관측 원격 bytes와 logical operations, 로컬 재사용, compression 입출력 크기·소요 시간, 기존 backlog/reservation 상태다. 통계 수집을 위한 전체 디스크 스캔이나 추가 네트워크 I/O는 하지 않는다. SDK retry attempts와 부분 전송 bytes를 계측할 수 없는 backend는 unknown으로 남긴다.

통계는 프로세스 재시작 시 사라져도 된다. 기본 정책으로 즉시 동작하고 새 관측으로 이어간다. 새 DB migration·장기 cost history·price catalog는 첫 설계에 필요하지 않다. sequence·ACK·health·검색 성공 여부가 통계 기록 성공에 의존하지 않는다.

controller는 매 요청마다 복잡한 선택을 하지 않는다. 다음 유지보수/작업 경계에서 현재 통계 snapshot으로 판단하며, 초기 재평가 간격은 60초 이상으로 제한한다. 실제 작업이 없으면 평가를 위한 S3 접근이나 주기적 heavy scan을 하지 않는다. 설정 변경 후에는 최소 다음 재평가까지 유지하고, signal이 흔들릴 때는 기본값을 유지한다. 안전한 admission/예약 실패에는 간격과 무관하게 즉시 대응한다.

counter overflow·incomplete·관측 부족이면 공격적인 변경을 중지한다. 최적화가 새 장애 원인이 되지 않도록 후보 선택 실패는 기본값으로 처리한다. 감지 가능한 퇴행에는 자동 복귀하되, 운영 계측이 모든 회귀를 감지한다고 가정하지 않고 배포 전 검증을 필수로 한다.

## 6. 화면은 설명만 제공한다

기존 관리자 운영 화면에 읽기 전용 요약을 붙인다. 별도 비용 관리 작업 공간이나 설정 wizard를 만들지 않는다. 필요한 경우 관리자 전용 `GET /api/system/efficiency`로 노출하고 구현 시 OpenAPI/generated TS를 함께 추가한다. 기존 endpoint의 필수 계약을 교체하지 않는다.

예시 정보는 다음과 같다.

- 이번 서버 실행 이후 실제 관측한 원격 읽기 bytes·logical operations.
- 로컬 재사용과 최근 재다운로드 현황. 단순 hit를 avoided GET으로 환산하지 않음.
- 현재 적용한 기본/검증된 압축 후보와 선택 이유.
- 최근 로컬 사본 회수량, 현재 복구 가능 여부와 disk pressure.
- 관측 기간·재시작 초기화·측정 불가 항목.

설정 저장·최적화 시작·정책 승인 버튼은 없다. 패널 조회는 표시 중일 때만 하며 오류는 그 패널 안에서 끝난다. 이 화면을 열지 않아도 자동 동작은 동일하다. 공유 shard의 설치 전체 물리 통계는 admin에게만 보인다.

가격 정보를 알 수 없는데 원화/달러 절감액을 만들어내지 않는다. 첫 제품은 requests/bytes/CPU/지연으로 효율을 설명한다. README의 절감률은 재현 가능한 비교 실험이 있을 때만 사용한다.

## 7. 자동화로 숨길 수 없는 경계

디스크·S3·네트워크가 모두 소진되거나 불능인데 무한 수신을 보장할 수는 없다. 시스템은 안전한 로컬 회수와 기존 retry/recovery를 먼저 수행하고, 더 이상 durable하게 보존할 수 없을 때는 기존 backpressure/오류를 반환한다. 성공으로 ACK하고 몰래 버리지 않는다. 실제 저장 공간 부족·권한 오류·손상은 원인과 기존 진단 정보로 설명한다. 비용 정책을 사용자가 대신 조절하게 만들지 않는다.

데이터의 의미적 중요도와 원하는 보존 기간은 현재 입력만으로 확정할 수 없다. 따라서 자동 sampling/drop, attribute 제거, message dedup, retention 단축, remote purge는 추가하지 않는다. 사용자 설정 부담을 없애기 위해 데이터를 임의 삭제하는 기본값을 넣지 않는다. 원격 객체 저장량이 계속 증가하는 한계도 숨기지 않는다.

Range GET을 위한 원격 형식 전환, 별도 Gateway, OTLP 전체 지원, 새로운 query parser/aggregation engine은 자동 운영을 달성하기 위한 필수 변경이 아니므로 이번 범위에서 제외한다.

## 8. 구현 단계와 완료 기준

| 단계 | 범위 | 완료 조건 |
|---|---|---|
| U01 | 고정/bounded 관측, 기본값 복귀 계약, 읽기 전용 운영 요약 | 설치 후 설정 없이 동작, 기존 업무 결과 동일, 통계 실패 격리, 추가 원격 I/O 없음 |
| U02 | cold 공유·취소·pin 검증 및 확인된 중복 개선 | 동시 반복 검색의 실제 다운로드 증거, checksum/실패/재시도/복구 계약 통과 |
| U03 | disk budget 안에서 자동 로컬 사본 선택 | 반복 조회·퇴출·수집 압력 workload에서 비용·지연 개선, 무통계 fallback, 안전 reserve·checkpoint 증거 유지 |
| U04 | 검증된 압축 후보와 작업 경계 자동 선택 | 동일 형식 복구, ACK/query/checkpoint 진척 유지, 퇴행 시 기본값 복귀. 유리한 후보가 없으면 기존 설정으로 완료 |

U02도 기존 경로에 불필요한 중복이 없다면 검증만으로 완료할 수 있다. 자동화의 양이나 설정 수가 산출물이 아니라 사용자의 개입 없이 얻는 효과가 산출물이다. U03/U04의 후보 정책은 benchmark gate를 통과해야 하며 증거 없는 변경을 완료로 만들지 않는다.

공통 gate:

1. 동일 dataset의 ACK ledger·Record identity/내용·검색 순서/집계·Issue 상태/count·Alerts·Replay와 checkpoint 전체 복구를 baseline과 대조한다.
2. 통계 없음/overflow, 느린 S3, 취소, 디스크 압력, worker 실패, process restart에서도 안전한 기본값과 기존 오류 의미를 유지한다.
3. 1코어·1GiB 환경에서 수집 위주, warm 반복, cold 광범위, cache thrash, compression 경합을 비교한다. CPU/RSS/페이지 cache/디스크·원격 bytes/operations와 ACK/query p95/p99, backlog·checkpoint lag를 함께 기록한다. 512MiB RC 목표는 별도다.
4. baseline 반복 분산과 비교 허용 범위는 후보 실행 전에 고정한다. 기존 목표 실패, 지속 backlog/RPO 악화, 반복 측정의 의미 있는 퇴행이면 후보를 채택하지 않는다.
5. 새 설치·기존 DB 업그레이드·재시작 뒤 설정 입력이나 패널 방문 없이 자동 동작함을 실제 흐름으로 확인한다.

기존 P00–P12 gate는 유지한다. 실제 구현 시 각 단계 검증 후 commit·즉시 push하는 저장소 규칙을 따르며, 결과는 커밋에 실제 명령·실행 수·환경으로 기록한다. 사용자 승인에 따라 구현을 진행하며, 전체 구현·검증 완료 후 배포한다.
