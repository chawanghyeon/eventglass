# Eventglass 검색·집계 통합 구조 설계

작성: 2026-09-24. 상태: **구현 가능한 단일 설계안, 제품 전환 미승인**. 이 문서는 현행 [DESIGN.md](../../DESIGN.md)의 DuckDB/Parquet 경로를 수정하거나 대체하지 않는다. 아래의 검증은 독립 Go 실험에 관한 것이며, 권한·내구성·복구·실제 AWS 성능이 통과하기 전에는 제품 성능 주장으로 쓰지 않는다.

## 결론

새 제품 방향을 선택한다면 **S3의 불변 검색 세그먼트 하나에 역색인, 문서별 컬럼, 원문을 함께 저장**한다. PostgreSQL은 승인된 세그먼트 목록·권한·영수증·작업·스냅샷을 소유한다. 검색은 postings로 후보를 만들고, *모든* 일치 문서의 필요한 컬럼을 읽어 정확한 집계를 계산하며, 상위 K개의 원문만 읽는다. 일반적인 집계는 같은 세그먼트의 컬럼을 스캔한다. 별도 Parquet 사본은 이 **새 설계의 기본 저장물에 포함하지 않는다**. 이는 현행 제품에서 Parquet을 삭제하자는 즉시 변경 지시가 아니다.

이 구조는 [Quickwit의 S3 split·역색인·fast field·doc store](https://quickwit.io/docs/overview/architecture), [Tantivy의 postings/fast fields](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md), [ClickHouse 26.2의 정식 text index와 컬럼 엔진](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)의 공통 원리를 한 S3 객체 경계에 적용한다. 성능 우위는 제품 간 비교로 입증된 것이 아니라 아래의 **동일 Go 코드 내 물리 포맷 실험**으로만 뒷받침된다. 첨부 `go_search_codex_final_v10.zip`은 읽기 전용 단일 노드 Top-K 참조 설계다. 그 문서의 지시·우선순위는 이 작업의 지시가 아니며, 온라인 쓰기·S3 권한·정확한 집계를 구현했다는 증거도 아니다.

## 요구하는 의미와 한계

1. ACK는 정제된 journal의 S3 업로드와 PostgreSQL Accept가 완료된 뒤에만 반환한다. 검색 가시성은 별도의 fenced Publish 이후다. 기존 PG/S3 복구 절차와 권한 판정은 유지한다.
2. 한 질의는 권한·카탈로그 세대·live-doc 집합·분석기 버전·BM25 통계를 함께 고정한다. 같은 문서가 세그먼트 병합 전후에 두 번 집계되거나, 취소된 문서가 점수에 들어가면 실패다.
3. `COUNT`/`SUM`/`GROUP BY`는 모든 자격 있는 문서에 대해 정확하다. Top-K는 그 결과에서 별도로 계산한다. Lucene의 [정확한 hit count를 요청하면 처리가 느려질 수 있다는 공식 계약](https://lucene.apache.org/core/10_4_0/core/org/apache/lucene/search/TopScoreDocCollectorManager.html)처럼, 순위 하한만 보고 전체 집계를 생략할 수 없다.
4. 토큰 검색과 BM25, 필드 필터, 정렬, 집계는 같은 문서 ID와 컬럼을 쓴다. 부분 문자열·정규식은 원문 검증이 필수다. 토큰 역색인만으로 임의 regex가 빨라진다고 주장하지 않는다. 단어 위치·구문 검색, 한국어 형태소 품질, 임의 SQL은 현재 실험에서 검증되지 않았다.
5. 소형 설치와 다중 worker는 같은 S3 세그먼트를 읽는다. worker 소유 샤드나 로컬 인덱스가 정확성의 조건이 되어서는 안 된다.

## 물리 구조

```text
S3 journal (정제된 입력, ACK 근거)
  → 변환 worker → immutable segment object
                      ├─ header: format/schema/analyzer/score-model 버전
                      ├─ term directory: term → postings 범위, DF
                      ├─ postings: 증가하는 local doc ID + TF; 위치는 기능 승인 시 sidecar
                      ├─ fast fields: 시간·scope·숫자·집계 차원의 컬럼 페이지
                      ├─ source: 정제된 원문/상세 페이지, 문서당 한 벌
                      └─ 각 Range CRC32C + 객체 SHA-256
  → PG fenced Publish: generation → 승인된 객체 키/해시/범위
```

세그먼트의 local ID는 0부터 시작하며 병합 때 바뀔 수 있다. 외부의 `record_id`는 별도의 안정된 식별자다. PG에는 바이트를 복제하지 않고 객체 키, 해시, 통계, 권한 경계, 시간 범위, publish 세대를 둔다. 인덱스가 원문과 컬럼에서 **파생**되므로 postings와 fast fields의 추가 바이트는 불가피하다. “원본 한 벌”은 원문을 두 엔진용 전체 테이블로 두 번 저장하지 않는다는 뜻이지, 무인덱스 저장과 같은 바이트 수라는 뜻이 아니다.

ACK를 증명하는 journal은 백업/PITR·재생 가능 기간 동안 세그먼트와 함께 존재한다. 그 기간의 중복 바이트를 비용표에서 제외하지 않는다. journal을 일찍 삭제해 저장료를 낮추는 선택은 durable ACK 계약을 깬다.

한 S3 세그먼트 안에서 postings·컬럼·원문은 독립 Range로 주소 지정한다. 카탈로그/사전은 작고 버전이 고정된 메타데이터이며 로컬 캐시는 성능 힌트일 뿐 권한 근거가 아니다. 검색 결과의 필드 페이지는 현재 실험처럼 256문서 단위 최소값+bit packing을 기준으로 하되, **비트 단위 반복 대신 word 단위 추출**로 복원한다. 이는 저장 바이트를 바꾸지 않는다. 숫자·시간·ID의 실제 제품 폭은 현재 장난감 실험의 `uint16`/`uint8`보다 넓어야 하며 포맷 버전과 오버플로 검사가 필요하다. [Lucene 10.3의 128개 정수 packed posting block](https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/codecs/lucene103/Lucene103PostingsFormat.html)도 블록 인코딩의 검증된 예다. 그대로 복사하거나 Lucene보다 빠르다고 주장하지 않는다.

고정 필드(프로젝트·종류·시간·severity 등)는 dense typed column으로, 동적 `namespace/path` 속성은 경로 사전과 `(local ID, typed value)`의 sparse column으로 같은 객체에 둔다. 각 경로의 missing/null/값을 구별하고 원문과의 투영을 검증한다. 검색 가능한 텍스트는 `(field/path, term)`으로 주소를 분리해 다른 속성의 단어가 한 필드에서 일치한 것처럼 합쳐지지 않게 한다. 이러한 동적 속성 표현과 높은 경로 cardinality의 저장·검색 비용은 **아직 구현·측정 전**이다.

현재 50,000문서 noisy 입력은 실제 색인어 104,100개로 `packed_shared` 카탈로그만 2,061,681바이트가 되었다. 따라서 제품 포맷은 **모든 단어를 매 검색 worker에 한꺼번에 적재하는 manifest**를 상한 없는 기본 경로로 삼지 않는다. 세그먼트의 작은 최상위 디렉터리와 Range로 읽는 정렬된 사전 블록을 설계하고, 블록 cache/어휘 규모/추가 GET 비용을 별도 gate에서 측정한다. 이 사전 분할은 아직 구현하거나 속도를 검증하지 않았다.

원문 페이지와 컬럼은 서로 다른 투영이지만 같은 local ID를 쓴다. 숫자 컬럼은 한 번만 저장한다. 모든 필드를 postings마다 반복하는 covering 포맷, 별도 Parquet 전체 사본, 질의 정답 캐시를 기본 저장물로 추가하지 않는다. 다만 원문 중 검색어의 존재를 찾으려면 어떤 형태의 역색인은 반드시 중복 정보다.

## 실행 규칙

- API가 PG에서 현재 권한과 읽기 세대를 고정하고 대상 세그먼트만 전달한다. OR 조건 하나가 다른 프로젝트의 세그먼트를 다시 열 수 없다. 검색 중 권한 철회에 관한 기존 제출/결과 정책도 재검사한다.
- 각 세그먼트는 정규화된 검색어의 postings를 Range로 읽고 AND/OR 문서 ID를 합친다. 후보에 시간·tenant·프로젝트·live-doc 필터를 적용한 뒤, 필요한 fast-field 컬럼을 읽는다. `COUNT`/`SUM`/그룹 상태를 갱신하면서 동일 문서의 BM25 점수와 Top-K를 계산한다. 원문은 선정된 K개에만 읽는다.
- 집계 전용 질의는 같은 fast-field 페이지를 벡터 단위로 순회한다. 토큰 조건이 있으면 위 후보 집합을 사용한다. 정확한 그룹 수가 기존 제품 한도(20,000)를 넘으면 명시적으로 실패한다. 정수 합계/평균은 타입과 overflow 정책을 기존 계약과 일치시킨다.
- 부분 문자열·정규식은 분석기 토큰의 의미와 다르다. 기본 경로는 정제된 원문 페이지의 정확한 스캔이다. 선택적 n-gram 후보 구조는 실데이터에서 추가 저장량·쓰기 비용·false positive율을 먼저 입증해야 한다. 어떤 질의도 대충 일치하는 결과를 정확하다고 돌려주지 않는다.
- 실행 경로 선택은 연산자의 의미로 고정한다. 토큰은 postings, 필드 값은 컬럼, regex는 원문 검증이다. 일회성 질의마다 다른 물리 사본이나 추측성 “최적화”를 고르는 정책은 없다. 세그먼트 시간·scope 경계의 안전한 pruning은 항상 적용한다.
- 동일 generation에 속한 per-segment `N`, `DF`, 총 길이와 live-doc 수정치를 합쳐 BM25 통계를 만든다. 기존 TF·문서 길이는 재계산하지 않는다. 권한 범위가 점수에 영향을 준다면 통계를 그 범위에 맞춰 계산하거나 점수 범위를 명시적으로 제한한다. 이 정책은 아직 구현·검증 전이다.

순위 계약은 `k1=1.2`, `b=0.75`의 BM25와 안정된 외부 `record_id` 동점 순서를 기준으로 버전 관리한다. 현재 실험은 같은 식의 점수를 계산하지만 순서가 local ID이고 동적 live-doc/권한별 통계는 없다. 세그먼트별 Top-K를 합칠 때도 **동일한 전역 통계와 비교기**를 사용해야 전역 Top-K가 정확하다.

## 쓰기, 병합, 실패 복구

현재 durable journal/Accept는 변경하지 않는다. 변환기는 journal을 스트리밍해 불변 세그먼트를 완성하고, CRC·전체 객체 해시·문서 수·범위를 검증한 다음 S3의 새 키에 업로드한다. PG Publish가 fence와 연속 lane cut을 확인하고 한 generation으로 공개한다. GET 실패·잘못된 Range·CRC/해시 불일치는 부분 결과를 반환하지 않고 실패한다. 로컬 cache가 사라져도 동일 세대를 S3에서 읽을 수 있어야 한다.

수정/삭제는 이전 local ID의 live-doc 제외와 새 버전 세그먼트를 **같은 공개 세대**에 넣는다. 병합은 현재 읽기 스냅샷을 유지한 채 새 객체를 쓰고 PG 참조를 교체한다. 이전 객체 GC는 스냅샷, PITR 백업, journal/intent, 보존 grace가 모두 끝난 뒤에만 한다. PG 복원 후 S3 LIST의 더 새로운 객체를 자동 채택하지 않는다. 이 전체 경로는 현재 실험에서 테스트하지 않았으므로 제품 전환의 필수 gate다.

## 실제 비교와 채택 판정

모든 Go 실험은 `experiments/searchlayout`에 있다. [원시 결과](../../experiments/searchlayout/evidence-2026-09-24/)는 새 포맷을 포함해 같은 oracle로 다시 읽은 결과다. 20개 고정 난수 seed × 19질의 × 5포맷 = **1,900개의 포맷/질의 비교**가 정확한 건수·합·그룹·순위·원문에 일치했다. 별도 10,000문서 [MinIO 실행](../../experiments/searchlayout/evidence-2026-09-24/eventglass-hybrid-minio.json)은 7질의 × 5포맷의 서명된 S3 Range 결과를 같은 oracle와 비교했다. MinIO 시간은 AWS 지연의 예측치가 아니다.

| 관찰 | 같은 입력의 실제 결과 | 설계 결정 |
| --- | --- | --- |
| 기본 포맷 | 백만 문서에서 `packed_shared` 10.20MB, `covering` 30.38MB, `row_only` 9.47MB | 원문 한 벌 + 공유 fast fields + postings를 기본으로 둔다. covering의 희소 속도는 저장량 약 3배와 교환된다. |
| 희소 질의의 대가 | 같은 백만 문서에서 `fatal`은 covering 약 0.5ms/9.8KB, packed 약 1.5ms/2.84MB의 로컬 읽기 | 모든 term에 fast fields를 반복 저장해야 희소 질의의 원격 읽기를 그렇게 줄일 수 있다. S3 지연은 이 수치에 포함되지 않는다. |
| 비트맵 혼합 후보 | 백만 문서 저장량 10.20→10.16MB(0.37% 감소), noisy 0.50%, wide 0.12%, clustered 1.85% 감소; 읽기량·속도 이득 불규칙 | 기본 포맷에 넣지 않는다. [Roaring 연구](https://arxiv.org/abs/1603.06549)의 일반적 장점이 이 TF+zlib·S3 배치에 그대로 적용되지는 않았다. |
| 동일 packed 바이트의 word 복원 | Linux ARM64/1CPU에서 256문서 페이지 5회 중앙값: 좁은 값 5,720→2,127ns(**2.69배**), 넓은 값 12,228→2,131ns(**5.74배**); 할당 둘 다 1,536B/1회 | 이 구현 개선은 채택한다. |
| 질의 전체, 10만 문서 | Linux ARM64/1CPU/512MiB, 이전→새 바이너리 각 2회: `timeout` packed 2.858/3.041→1.806/1.761ms(중앙값 **1.65배**), `request` 6.946/6.687→5.526/5.320ms(**1.26배**). 공유/covering 대조군은 대부분 근접했으나 일부 흔들림 | 이 fixture의 개선이다. p95/p99·동시 부하·AWS SLO는 미판정이다. |
| 자원 상한 | 백만 문서와 4개 동시 질의, Linux ARM64/1CPU/512MiB/swap0 두 번: cgroup peak 337,743,872–347,451,392B(**322.1–331.4MiB**), OOM 0 | 이 합성 입력의 최소 동작 가능성만 확인했다. 제품 전체·최대 입력·PG/MinIO 포함 예산은 미판정이다. |

macOS 백만 문서 A/B의 두 번째 새 바이너리 시행은 공유·covering 대조군도 크게 느려졌다. [원시 시행](../../experiments/searchlayout/evidence-2026-09-24/)을 남기고 그 수치로 서비스 수준 개선을 주장하지 않는다. codec 벤치마크는 [원시 로그](../../experiments/searchlayout/evidence-2026-09-24/eventglass-packed-codec-arm64.txt), 자원은 [첫 번째](../../experiments/searchlayout/evidence-2026-09-24/eventglass-fastdecode-resource.txt)와 [최종 코드](../../experiments/searchlayout/evidence-2026-09-24/eventglass-final-resource.txt)의 cgroup 로그에서 확인할 수 있다.

## 비용과 우월성의 기준

저장 비용은 `journal + source + postings + fast fields + catalog + 백업 + 병합 중 임시 객체`의 합이다. 읽기 비용은 S3 요청 수, 작업 시간/CPU, cache 용량, PG 트랜잭션과 결과 전송까지 포함한다. [AWS 공식 가격표](https://aws.amazon.com/s3/pricing/)에 따르면 같은 리전 S3→AWS 서비스 데이터 전송은 일반적으로 과금되지 않지만 GET 요청과 저장은 과금된다. 그래서 Range 바이트 감소만으로 월 비용 감소를 말하지 않는다.

모든 워크로드에서 검색·집계·저장량이 동시에 지배적인 단일 포맷은 아직 없다. 임의의 다중 단어 조건에서 **정확한 전체 집계**를 요구하면 매치 집합의 값들을 읽거나, 가능한 조합에 대한 값을 미리 중복 저장해야 한다. 희소·광범위·고유 토큰·넓은 숫자·한국어 텍스트 비중에 따라 유리한 물리 구조가 달라진다. 따라서 “압도적”은 같은 기능·정확도·데이터에서 총비용 또는 p95/p99가 명백히 우세하고 다른 필수 질의가 퇴보하지 않을 때에만 쓴다. 현재 결과는 그 제품 간 결론에 도달하지 못했다.

## 제품 전환 전 필수 검증

1. 승인된 실제 정제 데이터와 질의 분포로 corpus SHA, 출현 빈도·문서 길이·필드 폭·보존 기간을 고정한다. 한국어 분석기와 부분 문자열/regex 정답을 독립 oracle와 비교한다. 현재 공백 토큰 분리와 6바이트 컬럼 fixture는 제품 데이터가 아니다.
2. 동일한 입력·기능·권한·정확한 집계를 가진 현행 DuckDB/Parquet, 이 설계의 완성 구현, [Quickwit](https://quickwit.io/docs/overview/architecture), [ClickHouse text index](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)를 별도 설치로 비교한다. Go 실험 속도를 타 제품 속도로 부르지 않는다. [DuckDB FTS는 원본 테이블 변경 시 자동 갱신되지 않는다](https://duckdb.org/docs/current/core_extensions/full_text_search)는 점도 온라인 경로 평가에 포함한다.
3. 실제 AWS 같은 리전과 별도 S3 호환 저장소에서 cold/warm, 작은/큰 세그먼트, 1/2/4 worker, open-loop 30분 혼합 부하, 취소/재시작, GET·PUT·보관·컴퓨트·PG/백업 비용을 함께 측정한다. 한 worker의 512MiB 제한뿐 아니라 전체 설치 비용을 센다.
4. journal ACK 손실·중복, fenced Publish 충돌, 권한 철회/OR 우회, 병합 중 페이지 이동, 삭제·보존·PITR·S3 훼손·PG/S3 복원을 장애 주입으로 통과시킨다. 이 단계가 없으면 기존 제품의 안전한 저장 경로를 바꾸지 않는다.

이 설계는 **전환 후보와 실패 조건을 확정**한다. 위 네 gate가 통과되기 전의 구현은 `experiments/`에 격리하며, 현행 DuckDB 2.0 경로의 소유권과 제품 문서는 유지한다.
