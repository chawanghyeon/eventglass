# Eventglass 검색·집계 통합 구조 설계

작성: 2026-09-24. 상태: **물리 구조 실험 진행 중, 완전 입증 및 제품 전환 미승인**. 이 문서는 현행 [DESIGN.md](../../DESIGN.md)의 DuckDB/Parquet 경로를 수정하거나 대체하지 않는다. 아래의 검증은 독립 Go 실험에 관한 것이며, 권한·내구성·복구·실제 AWS 성능이 통과하기 전에는 제품 성능 주장으로 쓰지 않는다.

## 결론

검증 중인 후보는 **S3의 불변 세그먼트 하나에 역색인, 문서별 컬럼, 원문을 함께 저장하고 모든 조건을 정렬된 local 문서 ID 집합으로 통일**한다. PostgreSQL은 승인된 세그먼트 목록·권한·영수증·작업·스냅샷을 소유한다. 희소한 조건은 postings, 값 조건은 typed column, 임의 문자열·정규식은 원문 확인에서 ID를 얻는다. 그 뒤에는 같은 경로에서 *모든* 일치 문서의 컬럼으로 정확한 집계를 계산하고 상위 K개의 원문만 읽는다. 별도 Parquet 사본은 이 **후보의 기본 저장물에 포함하지 않는다**. 동일 입력의 첫 DuckDB/Parquet 비교에서 이 물리 포맷은 저장량과 정규식 지연에서 열세였으므로 **제품 구조로 채택하지 않는다**. 현행 제품의 Parquet을 삭제하자는 지시도 아니다.

이 구조는 [Quickwit의 S3 split·역색인·fast field·doc store](https://quickwit.io/docs/overview/architecture), [Tantivy의 postings/fast fields](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md), [ClickHouse 26.2의 정식 text index와 컬럼 엔진](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)의 공통 원리를 한 S3 객체 경계에 적용한다. 아래의 **동일 Go 코드 내 물리 포맷 실험**은 일부 토큰 검색 이득과 저장량·regex 손실을 함께 보여준다. 제품 성능 우위는 입증되지 않았다. 첨부 `go_search_codex_final_v10.zip`은 읽기 전용 단일 노드 Top-K 참조 설계다. 그 문서의 지시·우선순위는 이 작업의 지시가 아니며, 온라인 쓰기·S3 권한·정확한 집계를 구현했다는 증거도 아니다.

**입증 기준:** 같은 정제 입력과 질의 결과를 독립 oracle 및 현행 DuckDB/Parquet와 비교하고, 새 토큰/BM25 기능은 동등 기능을 가진 별도 엔진과 비교한다. 1/2/4 worker의 실제 ACK→Publish→권한 검색→집계→상세 조회에서 정확도·스냅샷·장애 복구를 모두 통과해야 한다. 현재 제품의 [ACK p95≤500ms, 가시성 p95≤5s, warm rows/histogram p95≤500ms](../../DESIGN.md)와 p99, 전체 설치의 저장·GET·CPU·메모리·백업 비용을 실제 AWS 같은 리전 및 S3 호환 환경에서 함께 판정한다. 테스트하지 않은 항목은 성공으로 간주하지 않는다.

| 검증 축 | 현재 증거 | 판정 |
| --- | --- | --- |
| 단일 객체의 동적 필드·원문·정확한 집계 | 1만 문서 로컬/MinIO 72조합, 10만 문서 CPU1/512MiB 72조합 | 이 범위에서 통과 |
| 여러 객체와 세대 | 로컬 2세그먼트→병합→live 변경, 이전 객체 읽기 | 논리 결과만 통과; PG 공개 세대·GC 미검증 |
| 사전·원격 Range 비용 | 전량/블록 사전을 MinIO로 A/B, 단일 Parquet과 세그먼트를 같은 HTTP Range 서버에서 30회씩 2번 A/B | 요청·바이트는 이 fixture에서 확인; AWS 비용 우위 미검증 |
| BM25·분석기·구문·한국어 품질 | 고정 필드 BM25와 일부 한국어/RE2 실험만 별도 존재 | 통합 포맷 미검증 |
| durable ACK·권한·fenced Publish·PG/S3 복구 | 현행 제품 계약은 존재, 새 포맷과 연결하지 않음 | 미검증 |
| 동일 fixture의 pinned DuckDB/Parquet 직접 비교 | 1만/10만 문서 × 72조합 정확도 일치. 단일 Parquet 308,745/2,898,596B, 후보 세그먼트 711,792/6,384,202B. 로컬 regex p50 약 3.5배 차이 | 이 물리 포맷의 일관된 우위 반증; 제품 경로·원격 p95/p99 미검증 |
| 실제 제품 변환기·질의 child·S3 Range 게이트웨이 | 기본/동적 각 1만·10만 레코드에서 두 역할/단일 Parquet의 집계·상세 정답, 로컬 파일 및 실제 MinIO GET·바이트·p95/p99 대조 | 이 비교 범위 통과; 단일 Parquet의 10만 건 GET 증가. 실제 AWS·PG 권한·ACK·복구 미검증 |
| 실제 StageRecord를 넣은 통합 세그먼트 | 기본/동적 각 1만·10만 레코드에서 전체 canonical source 왕복, project scope·검색·동적 필드·정확한 집계·Top-K를 독립 oracle와 비교 | 의미 검증 범위 통과. 현 JSON/zlib 물리형은 현행 Parquet 저장량의 2.6~3.4배라 기각; 전체 제품 타입·PG/ACK 미검증 |
| 단일 Parquet + Parquet postings sidecar | 기본/동적 입력 각 1만·10만 레코드, 고정 DuckDB 2.0, ARM64 CPU1/512MiB에서 독립 건수·합 검증. 10만 건은 로컬 및 검증 HTTP Range로 각 질의 30회 | 로컬 희소어 일부 개선이 Range 경로에서 사라짐; 추가 GET·저장량 발생. 이 단순 결합은 기각 |
| 실제 StageRecord 압축 색인의 다중 세그먼트 | 고유 토큰·속성값 10만 건의 4→1 세그먼트와 이전 로컬 세대에서 6개 집계 질의를 독립 oracle와 비교 | 색인 부분의 정확성 통과. PG/S3 세대 공개·원문 병합·원격 fanout 비용은 미검증 |
| 외부 엔진·전체 설치 비용 | 같은 기능·권한의 제품 비교 없음 | 미검증 |

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

고정 필드(프로젝트·종류·시간·severity 등)는 dense typed column으로, 동적 `namespace/path` 속성은 경로 사전과 `(local ID, typed value)`의 sparse column으로 같은 객체에 둔다. 각 경로의 missing/null/값을 구별하고 원문과의 투영을 검증한다. 검색 가능한 텍스트는 `(field/path, term)`으로 주소를 분리해 다른 속성의 단어가 한 필드에서 일치한 것처럼 합쳐지지 않게 한다. 동적 경로 1,007개를 포함한 단일 객체 왕복·MinIO Range 실험은 통과했지만, 그 JSON/zlib codec과 공백 토큰화는 제품 포맷·분석기가 아니다.

현재 50,000문서 noisy 입력은 실제 색인어 104,100개로 `packed_shared` 카탈로그만 2,061,681바이트가 되었다. 새 10,000문서·10,007토큰·1,007경로 실험에서도 전량 적재하는 압축 디렉터리가 **136,754B**, 희소 토큰 한 질의가 **10 GET/184,371B**였다. 동일 객체 안의 128키 사전 블록으로 바꾼 뒤 디렉터리는 **2,652B**, 희소 토큰은 **12 GET/53,393B**가 됐다. 객체 총량은 715,490→711,792B다. 10만 문서에서는 디렉터리 23,413B, 객체 6,384,202B였다. 블록 추가 GET과 질의당 컬럼·원문 요청은 그대로 비용이다. 이 실험은 모든 단어를 적재하는 manifest를 배제할 근거지만, 128키 크기나 JSON/zlib 사전을 최종 포맷으로 확정할 근거는 아니다.

원문 페이지와 컬럼은 서로 다른 투영이지만 같은 local ID를 쓴다. 집계용 숫자 컬럼은 한 벌만 두되 상세 원문에도 해당 값이 있으면 그 바이트는 중복된다. 모든 필드를 postings마다 반복하는 covering 포맷, 별도 Parquet 전체 사본, 질의 정답 캐시를 기본 저장물로 추가하지 않는다. 원문 중 검색어의 존재를 찾으려면 어떤 형태의 역색인은 반드시 중복 정보다.

## 실행 규칙

### 범용 질의 계약

세그먼트의 유일한 조인 키는 `(공개 generation, segment ID, local doc ID)`다. 고정 필드는 dense typed vector, 동적 경로는 `(path ID, local ID, type, value)`의 정렬된 sparse vector에 둔다. 정렬된 결과 ID와 sparse column은 선형 병합하고, 필요한 dense column만 투영해 전체 매치에 집계한다. `missing`, 명시적 `null`, 숫자와 문자열을 구별한다. `(필드/경로, 분석기 버전, 토큰)`과 typed exact value의 사전은 이 값에서 **파생된 접근 경로**이며, 문서의 필드 전체를 postings에 반복하지 않는다. 정제된 원문과 각 `search_values` 스칼라 경계는 정확한 재검증과 상세 조회에 남긴다.

```text
토큰/정확한 값 postings ─┐
typed column 필터 ──────┼→ 정렬된 local ID → AND/OR/NOT → 권한·시간·live-doc 교집합
원문 확인/regex 스캔 ────┘                                ↓
                                       필요한 컬럼 → 정확한 COUNT/SUM/GROUP BY
                                       동일 ID → 점수/정렬 → Top-K → 원문 상세
```

이 공통 결과형은 [Lucene의 증가 doc-ID 반복자와 DocValues](https://lucene.apache.org/core/9_5_0/core/org/apache/lucene/index/package-summary.html), [DuckDB의 selection vector](https://duckdb.org/docs/lts/internals/vector), [ClickHouse의 객체 저장소용 text index가 row ID를 컬럼 필터에 전달하는 방식](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)과 같은 원리다. **모든 조건을 빠르게 하는 단일 정렬 순서**는 여기서 주장하지 않는다. 색인이 정확히 판정할 수 없는 정규식·임의 계산은 같은 ID 계약 아래에서 원문/컬럼을 스캔해 정확성을 보장한다. 지원하지 않는 SQL 연산·조인은 별도 기능 범위이지 이 실험으로 구현되었다고 하지 않는다.

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
| 단순 비트맵 혼합 후보 | 백만 문서 저장량 10.20→10.16MB(0.37% 감소), noisy 0.50%, wide 0.12%, clustered 1.85% 감소; 읽기량·속도 이득 불규칙 | 기본 포맷에 넣지 않는다. 이 후보는 **dense bitset + TF**이며 Roaring이 아니므로 [Roaring](https://roaringbitmap.org/publications/)의 성능을 판정하지 않는다. |
| 동일 packed 바이트의 word 복원 | Linux ARM64/1CPU에서 256문서 페이지 5회 중앙값: 좁은 값 5,720→2,127ns(**2.69배**), 넓은 값 12,228→2,131ns(**5.74배**); 할당 둘 다 1,536B/1회 | 이 구현 개선은 채택한다. |
| 질의 전체, 10만 문서 | Linux ARM64/1CPU/512MiB, 이전→새 바이너리 각 2회: `timeout` packed 2.858/3.041→1.806/1.761ms(중앙값 **1.65배**), `request` 6.946/6.687→5.526/5.320ms(**1.26배**). 공유/covering 대조군은 대부분 근접했으나 일부 흔들림 | 이 fixture의 개선이다. p95/p99·동시 부하·AWS SLO는 미판정이다. |
| 자원 상한 | 백만 문서와 4개 동시 질의, Linux ARM64/1CPU/512MiB/swap0 두 번: cgroup peak 337,743,872–347,451,392B(**322.1–331.4MiB**), OOM 0 | 이 합성 입력의 최소 동작 가능성만 확인했다. 제품 전체·최대 입력·PG/MinIO 포함 예산은 미판정이다. |

macOS 백만 문서 A/B의 두 번째 새 바이너리 시행은 공유·covering 대조군도 크게 느려졌다. [원시 시행](../../experiments/searchlayout/evidence-2026-09-24/)을 남기고 그 수치로 서비스 수준 개선을 주장하지 않는다. codec 벤치마크는 [원시 로그](../../experiments/searchlayout/evidence-2026-09-24/eventglass-packed-codec-arm64.txt), 자원은 [첫 번째](../../experiments/searchlayout/evidence-2026-09-24/eventglass-fastdecode-resource.txt)와 [최종 코드](../../experiments/searchlayout/evidence-2026-09-24/eventglass-final-resource.txt)의 cgroup 로그에서 확인할 수 있다.

## 다른 계열의 연구와 추가 검증

[파티션 Elias-Fano](https://pages.di.unipi.it/rossano/assets/pdf/papers/SIGIR14.pdf)는 증가 ID 목록의 압축·탐색, [Roaring](https://arxiv.org/abs/1603.06549)은 배열·비트맵·run 컨테이너의 집합 연산, [Bit-Sliced Index](https://cse.usf.edu/~tuy/Literature/Bitmap-SIGMOD97.pdf)와 [BitWeaving](https://15721.courses.cs.cmu.edu/spring2016/papers/li-sigmod2013.pdf)은 숫자 필터·집계의 비트 병렬 처리, [Block-Max WAND](https://citeseerx.ist.psu.edu/document?doi=91a353974741cdcac274f8dfeabde87430fbc05b&repid=rep1&type=pdf)는 안전한 Top-K 점수 생략을 다룬다. [PostgreSQL의 trigram 구현](https://doxygen.postgresql.org/trgm__regexp_8c_source.html)은 부분 문자열/정규식 후보를 얻은 뒤 원문으로 거짓 양성을 제거한다. 가장 직접적인 최신 사례인 [ClickHouse의 2026년 객체 저장소 text index](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)는 정렬 사전 블록+작은 메모리 희소 색인, 길이에 따른 inline/varint/Roaring postings, 순차 병합을 사용한다. 이는 우리 사전 분할 방향의 **구현 가능성 근거**이지 Eventglass 성능 측정값은 아니다.

[추가 Go 실험](../../experiments/searchlayout/literature_test.go)의 [macOS ARM64 원시 로그](../../experiments/searchlayout/evidence-2026-09-24/literature-alternatives-darwin-arm64.txt)는 같은 생성 문서에서 별도 물리 후보를 왕복 검증했다. 아래 저장량은 **해당 부분만** 센다. 질의 전체·S3 GET·BM25·권한·병합 결과가 아니다.

공통 ID 계약의 [Go 시제품](../../experiments/searchlayout/unified_test.go)은 20개 고정 seed × 16개의 토큰/정확한 값/숫자 범위/존재/null/배열/부분 문자열/RE2/AND·OR·NOT 조건을 독립 원문 스캔과 비교했다. **320개 필터 결과와 4개 동적 그룹 경로별 1,280개 COUNT/SUM/GROUP BY/duration 정렬 Top-K 결과**가 일치했다. BM25는 앞선 세그먼트 실험의 별도 검증이며 이 범용 연산자 시제품의 정렬 기준은 아니다. tenant·시간·live-doc 경계, 한국어, 고유 ID 경로, `missing`/`null`/타입 불일치, `a.b`와 `a/b` 경로 분리, 스칼라 간 문자열 결합 금지를 포함한다. [100,000문서 ARM64 메모리 벤치마크](../../experiments/searchlayout/evidence-2026-09-24/unified-selection-darwin-arm64.txt) 3회 중앙값은 희소 `fatal`(100건) ID 경로 0.100µs 대 원문 순회 75.5µs, 광범위 `request`(99,900건) 57.9µs 대 192.3µs였다. 반대로 `contains("fatal")`의 공통 ID 경로는 618µs로 직접 원문 순회 429µs보다 느렸다. **범용성은 정확한 fallback을 제공하지만 모든 질의의 가속을 뜻하지 않는다.** 이 수치는 압축 해제·S3·집계 그룹·동시성·제품 권한 검사를 제외한 메모리 안의 연산자 비교다. 일반 엔진의 서비스 지연이나 비용 우위로 해석하지 않는다.

[저장·재읽기 시제품](../../experiments/searchlayout/unified_range_test.go)은 같은 공통 ID 모델을 **하나의 715,490B 불변 객체**에 기록했다. 10,000문서·10,007토큰·1,007동적 경로를 만들고 18개 조건 × 4개 그룹 경로 = **72개의 건수·합·그룹·Top-K·상세 원문 조합**을 로컬 파일과 [격리된 MinIO](../../experiments/searchlayout/evidence-2026-09-24/unified-range-minio-darwin-arm64.txt)에서 독립 원문 스캔 oracle과 비교해 모두 일치했다. 선택된 postings의 CRC 손상과 필수 컬럼의 짧은 Range 응답은 오류로 끝났다. 희소/광범위 토큰은 각각 10 GET/184,371B 및 10 GET/184,270B, regex는 83 GET/447,455B였다. [Linux ARM64 CPU1·512MiB·swap0 로그](../../experiments/searchlayout/evidence-2026-09-24/unified-range-linux-arm64-cpu1-512m.txt)는 네트워크를 끈 로컬 파일 경로에서 82,751,488B cgroup peak와 OOM 0을 기록했다. JSON/zlib은 실험용 codec이며, 희소 질의의 큰 디렉터리·원문 복제 바이트·다수 GET은 해결 전 비용이다. 이 검증은 단일 세그먼트의 실행 가능성이지 BM25·다중 세그먼트·권한·PG/S3 복구·실제 AWS·제품 대비 우위를 확인한 결과가 아니다.

후속 [블록 사전 단일 객체](../../experiments/searchlayout/unified_range_test.go)는 동일한 72조합을 [MinIO 원시 로그](../../experiments/searchlayout/evidence-2026-09-24/unified-directory-block-minio.txt)에서 다시 통과했고 사전 블록 CRC 손상도 검출했다. 희소/광범위 토큰은 각각 12 GET/53,393B 및 12 GET/53,292B, regex는 84 GET/314,923B다. 객체 711,792B의 내역은 원문 페이지 281,096B, 동적 필드 274,650B, postings 20,231B, 사전 블록 130,404B, 코어 2,734B, 최상위 디렉터리 2,652B, footer 25B다. 원문 단독 281KB에 비해 파생 접근 경로가 상당한 저장량을 더한다. [같은 측정 코드](../../experiments/searchlayout/unified_latency_test.go)의 `fb420e0` [전량 사전 기준](../../experiments/searchlayout/evidence-2026-09-24/unified-directory-baseline-ab.txt)과 [블록 사전](../../experiments/searchlayout/evidence-2026-09-24/unified-directory-block-ab.txt)을 로컬 MinIO에서 각각 30회 측정했지만, 당시 다른 제품 비교 컨테이너가 동시에 실행됐고 시행 간 지연이 약 2배 달라졌다. 따라서 GET/바이트 차이는 결정적이나 그 p95/p99를 성능 우위 또는 AWS 지연으로 해석하지 않는다. [1만 문서](../../experiments/searchlayout/evidence-2026-09-24/unified-directory-block-linux-arm64-cpu1-512m.txt)와 [10만 문서 Linux ARM64 CPU1·512MiB·swap0](../../experiments/searchlayout/evidence-2026-09-24/unified-directory-block-100k-linux-arm64.txt)은 각각 cgroup peak 76,869,632B/339,501,056B, OOM 0을 기록했다. 10만 문서 객체는 6,384,202B이고 동일 72조합이 일치했다. [두 객체의 병합·live 변경·이전 객체 읽기](../../experiments/searchlayout/evidence-2026-09-24/unified-generation-merge-darwin-arm64.txt)도 원문 oracle과 일치했으나 PG 카탈로그 트랜잭션이나 실제 복구는 수행하지 않았다.

[같은 문서 입력의 pinned DuckDB 2.0 직접 비교](../../experiments/searchlayout/unified_duckdb_test.go)는 1만·10만 문서 각각에서 18개 조건 × 4개 그룹의 건수·합·그룹·Top-5 원문을 독립 원문 스캔, 후보 세그먼트, 두 Parquet 역할의 DuckDB 질의로 대조해 모두 일치했다. DuckDB는 `v2.0.0-dev84020`, Linux ARM64 CPU1/512MiB/swap0에서 실행했다. [1만 문서 로그](../../experiments/searchlayout/evidence-2026-09-24/unified-pinned-duckdb-parquet-10k-linux-arm64.txt)의 분석/원문 Parquet은 135,534/192,484B(합 328,018B), 후보는 711,792B로 **2.17배**다. [10만 문서 로그](../../experiments/searchlayout/evidence-2026-09-24/unified-pinned-duckdb-parquet-100k-linux-arm64.txt)는 각각 1,167,545/1,870,179B(합 3,037,724B), 후보 6,384,202B로 **2.10배**다. 동일한 정제 journal·PG/S3 메타데이터·백업 바이트는 양쪽 수치에서 제외했으며, 이는 현행 제품의 실제 번들 파일 크기 비교가 아니라 **동일 fixture의 두 물리 저장 방식** 비교다. 10만 문서 통합 시험은 OOM 없이 끝났지만 cgroup peak가 512MiB 한계에 닿고 `memory.events max=97`이므로 이 *동일 프로세스 이중 엔진 시험*에는 메모리 여유가 없었다. 후보 단독 worker의 사용량으로 해석하지 않는다.

[로컬 파일 warm 반복 30회](../../experiments/searchlayout/evidence-2026-09-24/unified-pinned-duckdb-parquet-local-timing.txt)에서는 희소 토큰 p50/p95/p99가 Parquet 14.9/17.2/17.9ms, 후보 10.7/11.9/15.2ms; 광범위 토큰은 15.6/18.3/19.0ms 대 11.1/12.2/13.8ms였다. 반면 regex는 15.2/16.8/16.8ms 대 50.6/54.7/54.8ms였다. 두 질의 경로 모두 같은 결과를 내지만, 이 포맷은 **저장량과 regex에서 동시에 열세**다. 30회 표본의 p99는 한 최댓값이고 원격 S3·서비스 부하·동시 실행·제품 권한/ACK는 빠져 있어 이 숫자를 제품 SLO로 쓰지 않는다. 현재 물리 포맷의 일관된 우위 주장은 기각하며, 다음 후보는 이 반례를 동일 비교로 통과해야 한다.

같은 컬럼과 원문 JSON을 **단일 Parquet 객체**에 넣는 더 단순한 대안도 [1만](../../experiments/searchlayout/evidence-2026-09-24/unified-single-parquet-10k-linux-arm64.txt)·[10만 문서 분리 시험](../../experiments/searchlayout/evidence-2026-09-24/unified-single-parquet-100k-linux-arm64.txt)의 동일 72조합에서 독립 oracle과 일치했다. 크기는 308,745/2,898,596B로 위 두 역할 Parquet보다 5.9/4.6% 작고, 현재 세그먼트의 43.4/45.4%였다. [동일 1만 문서 세 방식 번갈아 30회](../../experiments/searchlayout/evidence-2026-09-24/unified-three-format-local-timing.txt)에서 단일 Parquet과 세그먼트의 p50은 희소 토큰 14.2/10.3ms, 광범위 토큰 15.3/10.8ms, regex 14.5/50.2ms였다. 세그먼트는 토큰 검색이 빠르지만 단일 Parquet은 저장량과 regex에서 낫다. [세 포맷을 한 512MiB 프로세스에 적재한 10만 문서 시험](../../experiments/searchlayout/evidence-2026-09-24/unified-single-parquet-combined-100k-oom.txt)은 OOM으로 실패했고, 단일 Parquet만 분리한 시험은 peak 523,108,352B, OOM 0으로 통과했다. 둘 다 제품 worker 메모리로 옮겨 해석할 수 없고, 분리 시험에도 약 13MiB 여유밖에 없었다. 단일 Parquet도 원문과 검색용 JSON의 중복을 포함하며 현행 제품 번들과 같지 않다. 토큰/BM25 품질, 원격 S3 GET, 권한과 내구 ACK를 아직 검증하지 않아 이 대안을 새 제품으로 승인하지 않는다.

이후 [같은 HTTP Range 서버 첫 실행](../../experiments/searchlayout/evidence-2026-09-24/unified-http-range-three-queries-run1.txt)과 [독립 재실행](../../experiments/searchlayout/evidence-2026-09-24/unified-http-range-three-queries-run2.txt)에서 1만 문서의 희소/광범위 토큰·regex 정답을 원문 oracle과 다시 대조하고, 각각 번갈아 30회씩 요청을 셌다. 단일 Parquet은 세 질의 모두 **8 Range GET+2 HEAD, 2,469,960B/질의**였다. 실제 헤더는 작은 308,745B 객체 전체 범위를 8번 요청했다. 세그먼트는 토큰마다 **12 GET, 약 53KB**, regex는 **84 GET, 314,923B**였다. 같은 리전의 무료 전송만 고려하면 세그먼트의 바이트 절감이 요청료 절감은 아니다. 두 실행의 p50은 단일 Parquet/세그먼트가 희소 토큰 약 31/13ms, regex 약 24/58ms였으나 이는 loopback HTTP, 워커 1개, 합성 입력의 진단값이다. 실제 S3·제품 gateway·캐시·동시성·HTTP/TLS·월간 요청 분포가 빠졌으므로 AWS p95/p99와 총비용으로 확대하지 않는다.

[실제 Eventglass 변환기와 질의 planner/child 비교](../../experiments/searchlayout/product_parquet_test.go)는 **정제 형식으로 직접 만든** 같은 1만 StageRecord를 현재 analytics/payload Parquet 두 역할로 변환한 뒤 두 파일의 컬럼과 원문을 하나의 Parquet으로 병합했다. SDK 수신·정규화·Accept를 거친 입력이라고 주장하지 않는다. 두 입력 모두 project 10/11, severity, 원문, 동적 속성과 검색 스칼라를 포함하며 두 번째 입력은 동적 경로 1,000개와 고유 검색값을 추가한다. 독립 Go 기대값에 대해 project 10의 검색 스칼라·regex·동적 정수 및 희소 사용자 정의 경로 필터 건수/합, 전체 1만 원문, 실제 query planner의 집계·상세 결과가 두 포맷에서 일치했다. [반복 입력 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-converter-single-parquet-10k.txt)는 두 역할 합계 243,321B/단일 230,684B(**5.2% 절감**), [동적 입력 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-converter-single-parquet-noisy-10k.txt)는 361,866B/349,229B(**3.5% 절감**)이다. 한 물리 파일의 두 로컬 hardlink를 서로 다른 역할 경로로 전달하면 기존 child가 동작한다. 같은 경로를 두 역할에 직접 지정하면 현재 입력 중복 검증이 거부하므로, 실제 S3에서는 한 객체 키에 대한 서로 다른 두 capability URL과 카탈로그 표현이 필요하다. 이 실험은 그 PG/S3 변경을 구현하지 않았다.

[제품 검증 Range 게이트웨이 반복 입력](../../experiments/searchlayout/evidence-2026-09-24/actual-product-gateway-pair-single-10k.txt)과 [동적 입력](../../experiments/searchlayout/evidence-2026-09-24/actual-product-gateway-pair-single-noisy-10k.txt)은 같은 물리 객체 키에 서로 다른 두 capability를 발급하고 실제 query planner/child의 집계·상세 작업을 교차 순서로 각각 30회 수행했다. 등록하지 않은 capability는 404였고 모든 결과가 독립 기대값과 일치했다. 검증 게이트웨이의 캐시 없는 RangeStore에서는 집계가 두 역할 **1 GET/107,369 또는 143,450B**, 단일 객체 **1 GET/230,684 또는 349,229B**였다. 상세는 두 역할 **2 GET/243,321 또는 361,866B**, 단일 **2 GET/461,368 또는 698,458B**였다. 작은 파일이므로 블록 검증이 객체 대부분을 읽어 단일 객체의 집계 읽기 바이트가 **2.1~2.4배**다. 30회 p95/p99도 원시 로그에 있지만 loopback·파일 RangeStore·워커 1개의 작업 지연이다. 실제 S3 요청 과금, 1/2/4 worker 경쟁, PG 권한·ACK, 백업·복구, 제품 API 지연을 입증하지 못한다. 따라서 단일 Parquet을 채택하지 않고 비교 대상으로 유지한다.

같은 제품 결과를 [기본 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-minio-pair-single-baseline-10k.txt)·[동적 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-minio-pair-single-noisy-10k.txt)·[기본 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-minio-pair-single-baseline-100k.txt)·[동적 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-minio-pair-single-noisy-100k.txt)에서 실제 `S3Store.PutStream`으로 격리 MinIO에 올리고 제품 검증 게이트웨이로 읽었다. 두 capability가 한 S3 객체 키를 가리키는 경우까지 포함해 집계·상세 각각 30회 교차 실행했고 모두 독립 정답과 일치했다. **10만 기본** 집계는 두 역할 1 GET/1,010,847B, 단일 2 GET/2,208,874B; 상세는 2 GET/2,315,870B 대 4 GET/4,417,748B였다. **10만 동적** 집계는 1 GET/1,256,703B 대 2 GET/3,236,254B, 상세는 2 GET/3,343,454B 대 4 GET/6,472,508B였다. 동적 입력 p95는 집계 51.9→61.4ms, 상세 76.2→90.2ms였지만 기본 입력의 집계 p95/p99는 단일 객체 쪽이 조금 낮아 지연의 일관된 우위까지 주장하지 않는다. 업로드 시 합계 3 PUT·3 HEAD·3 검증 GET을 별도로 기록했으며 후보별로는 두 역할이 각 2건, 단일 객체가 각 1건이다. 10만 건은 `GOMEMLIMIT=96MiB`를 적용해 OOM 없이 통과했으나 cgroup peak가 512MiB 한계에 닿았고, [이 설정이 없는 한 번의 시험](../../experiments/searchlayout/evidence-2026-09-24/actual-product-minio-pair-single-baseline-100k-oom-no-gomemlimit.txt)은 OOM이었다. 이는 loopback MinIO·한 워커의 물리 경로 증거이며 실제 AWS 지연·월간 비용·PG/ACK/복구를 입증하지 않는다.

[제품 StageRecord 단일 세그먼트 실험](../../experiments/searchlayout/product_unified_test.go)은 기존 범용 포맷에 project scope와 전체 canonical StageRecord source를 추가해 현행 변환기의 **동일 정제 입력**을 저장했다. [기본 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-unified-baseline-10k.txt)·[동적 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-unified-noisy-10k.txt)·[기본 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-unified-baseline-100k.txt)·[동적 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-unified-noisy-100k.txt)의 모든 source 레코드와 정제 원문 바이트를 왕복 확인하고, 토큰·regex·typed 동적 필드·집계·그룹·Top-K를 입력 생성 규칙의 독립 oracle와 대조했다. 범위 밖 프로젝트도 같은 객체에 넣고 project 10만 계산했다. 10만 동적 입력의 첫 정규식 실행은 원문 페이지 전체를 캐시해 [OOM](../../experiments/searchlayout/evidence-2026-09-24/actual-product-unified-noisy-100k-oom-cached-source.txt)이었으며, 페이지를 스트리밍하고 일치 ID만 보관한 뒤 같은 CPU1/512MiB에서 통과했다. **저장량은** 기본 1만/10만 640,120/6,391,588B 대 두 Parquet 243,321/2,315,870B, 동적 1만/10만 1,199,603/11,261,933B 대 361,866/3,343,454B였다. 동적 10만의 11.26MB 중 canonical source가 8.17MB, 동적 필드가 1.27MB, 사전이 1.27MB다. 이 JSON/zlib 물리형은 **제품 비용 후보에서 기각**한다. 실험은 수동으로 만든 정제 StageRecord, 정수·문자열 속성과 severity 집계에 한정되며 모든 제품 타입·한국어 분석기/BM25·제품 query child·실제 S3·PG/ACK/복구를 통과했다는 뜻이 아니다.

[제품 Parquet + postings 실험](../../experiments/searchlayout/product_postings_test.go)은 위 단일 Parquet의 `search_values`에서 `(term,record_id,tf)`를 별도 Parquet으로 만들고, 현행 DuckDB 2.0의 `read_parquet` 조인으로 정확한 `COUNT`/`SUM`을 계산했다. 같은 입력의 독립 Go oracle로 검색 결과를 검증한 뒤 스캔과 조인을 번갈아 30회 측정했다. [기본 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-baseline-10k.txt)·[동적 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-noisy-10k.txt)에서는 모든 색인 질의가 스캔보다 느렸다. [기본 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-baseline-100k.txt)에서는 희소 `fatal` 516건의 p50이 스캔 6.16ms→조인 5.32ms였지만, 흔한 `request` 49,484건은 4.46→9.17ms였다. [동적 10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-noisy-100k.txt)은 `fatal` 6.07→5.11ms, `request` 4.55→9.87ms, 고유 `trace` 6.07→5.18ms였다. 10만 건 총 저장량은 기본 2,539,237B 대 현행 두 역할 2,315,870B(**9.6% 증가**), 동적 3,770,669B 대 3,343,454B(**12.8% 증가**)였다. 테스트 cgroup의 OOM은 없었으나 메모리 peak는 512MiB 한계에 도달했다. 이는 **공백 단어 분리 + Parquet postings 조인**의 반례다. 형태소 분석기·BM25·최적화된 postings 실행기나 실제 S3 비용을 반증한 결과는 아니다.

이 구조를 [기본 10만 Range 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-gateway-baseline-100k.txt)와 [동적 10만 Range 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-gateway-noisy-100k.txt)에서 같은 검증 게이트웨이의 별도 capability 두 개로 다시 실행했다. 질의마다 새 DuckDB 연결을 열어 파일 캐시가 GET을 0으로 만드는 측정 오류를 제거했다. 기본 입력 `fatal` p50은 스캔 23.02ms/2 GET/2,208,874B, 색인 28.09ms/3 GET/2,539,237B였다. 동적 입력의 고유 `trace`도 25.96ms/2 GET/3,236,254B 대 27.22ms/3 GET/3,770,669B였다. 흔한 `request` 역시 두 입력 모두 색인이 느렸다. 로컬에서 확인된 희소어 이득은 이 Range 경로에서 사라졌다. [기본 1만 Range 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-product-postings-gateway-baseline-10k.txt)는 1 GET→2 GET을 보인다. 이는 loopback·파일 기반 RangeStore·새 엔진 연결을 포함한 진단값이며 실제 S3/TLS, 비용, 다중 worker p95/p99가 아니다. 10만 건은 oracle 메모리 보유를 제거한 뒤 OOM 없이 통과했지만 cgroup peak가 512MiB에 닿아 운영 여유를 증명하지도 못한다.

[제품 입력의 압축 어휘 실험](../../experiments/searchlayout/product_compact_terms_test.go)은 Parquet postings의 저장 부담이 컬럼형 포맷 때문인지 분리해 확인했다. `StageRecord.SearchValues`에서 공백 분리 토큰의 `(문서 ID, TF)`를 만들고, 정렬 어휘의 공통 접두어와 문서 ID 차분을 인코딩한 뒤 전체를 Zstd로 압축했다. 모든 어휘·postings를 복원해 원본과 비교하고 `fatal`/`request`/고유 `trace`의 프로젝트 범위 `COUNT`·severity `SUM`을 독립 생성 규칙과 대조했다. 동일 변환기의 단일 Parquet에 압축 블록을 더해 두 Parquet 총량과 비교했다.

| 입력 | 문서 | 압축 어휘+postings | 단일 Parquet+색인 | 현행 두 Parquet | 차이 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 기본 | 10,000 | 138B | 230,822B | 243,321B | −5.1% |
| 순번형 고유 토큰 | 10,000 | 9,704B | 358,933B | 361,866B | −0.8% |
| 무작위 128비트 고유 토큰 | 10,000 | 210,775B | 966,985B | 768,847B | +25.8% |
| 무작위 256비트 고유 토큰 | 10,000 | 386,315B | 1,503,735B | 1,130,037B | +33.1% |
| 기본 | 100,000 | 214B | 2,209,088B | 2,315,870B | −4.6% |
| 순번형 고유 토큰 | 100,000 | 77,839B | 3,314,093B | 3,343,454B | −0.9% |
| 무작위 128비트 고유 토큰 | 100,000 | 2,087,199B | 9,385,574B | 7,405,690B | +26.7% |
| 무작위 256비트 고유 토큰 | 100,000 | 3,871,718B | 14,774,280B | 11,009,907B | +34.2% |

[기본/순번형 1만·10만 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-terms-noisy-100k.txt), [128비트 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-terms-random-128-10k.txt)·[10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-terms-random-128-100k.txt), [256비트 1만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-terms-random-trace-10k.txt)·[10만](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-terms-random-trace-100k.txt)이 원시 증거다. 무작위 토큰은 고정 SHA-256 입력에서 생성해 재현 가능하고 실제 변환기에 투입했다. 순번형의 극단적 압축은 일반 고유 토큰의 증거가 아니며, 128비트 결과만으로도 작은 색인이 저장량을 반드시 절감한다는 주장을 반박한다. 128비트 10만 실행의 cgroup peak는 536,879,104B로 512MiB 한계에 닿았으나 OOM 이벤트는 0이었다. 이 블록은 **전체를 읽어야 하는 저장량 중심 시제품**이다. S3 선택 Range, 위치/구문 검색, BM25, 범용 Unicode 분석기, 병합과 제품 경로 비용을 포함한 색인이 아니다.

압축 블록에 실제 StageRecord의 `project_id`·`severity_number`·`service`를 같은 객체의 컬럼 구간으로 더하고, `COUNT`·`SUM`·`GROUP BY service`를 세 검색어에 대해 독립 oracle와 실험용 단일 Parquet에 대조했다. [기본](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-complete-gateway-baseline-100k.txt), [순번형](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-complete-gateway-noisy-100k.txt), [무작위 128비트](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-complete-gateway-random-128-100k.txt), [무작위 256비트](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-complete-gateway-random-256-100k.txt) 10만 건을 ARM64 1 CPU/512MiB에서 각각 30회 교차 실행했다. 검증된 loopback HTTP Range 게이트웨이에서 압축 블록은 모두 1 GET, 단일 Parquet은 2/2/3/4 GET이었다. 세 검색어 중 가장 느린 p95는 압축 블록이 각각 **8.73/11.32/22.98/28.97ms**, 단일 Parquet은 **41.56/44.38/37.25/43.80ms**였다. 실제 집계 컬럼을 넣은 단일 Parquet+블록 저장량은 각각 2,219,659/3,324,664/9,396,145/14,784,851B이고, 현행 두 Parquet은 2,315,870/3,343,454/7,405,690/11,009,907B다. 즉 요청·지연 이점과 고유 토큰의 **+26.9%/+34.3% 저장 손해**가 동시에 확인됐다. 네 실행 모두 OOM은 없지만 cgroup peak가 512MiB 한계에 닿았다.

이 비교는 제품 query child의 동일한 구현 두 개가 아니다. Parquet은 질의마다 새 DuckDB 연결을 열고, 압축 블록은 테스트 프로세스의 Go 조회기에서 실행한다. 검색어도 공백 토큰 세 개와 정형 집계에 한정되고, 압축 컬럼은 반복되는 프로젝트·service 값 덕분에 10,563B로 작다. 동적 속성, 원문/regex, 권한 철회, 병합·스냅샷, ACK·복구 및 실제 S3/TLS는 검증하지 않았다. [로컬 반복 조회](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-lookup-random-128-100k.txt)에서는 128비트 입력의 흔한 단어와 고유 토큰 p95가 Parquet보다 느린 실행도 있었다. 따라서 이 결과는 **원격 Range에서 정형 토큰+집계가 가능한 경로**의 증거이지 범용 제품 구조의 채택 판정이 아니다.

색인 객체에 StageRecord의 **전체 동적 속성**도 복제해 각 레코드의 타입·경로·값을 [무작위 토큰·속성 10만 건 왕복 검사](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-attr-roundtrip-100k.txt)로 확인하고 `fatal/request AND latency>=500`, `request AND attributes/custom/236 EXISTS`의 정확한 그룹별 건수·severity 합을 독립 oracle와 대조했다. 앞의 단일 Parquet 기준 대신 **현행 두 역할 중 analytics Parquet**을 검증 Range 게이트웨이의 비교 대상으로 다시 실행했다. [기본](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-baseline-pair-100k.txt), [순번형 고유 토큰](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-noisy-pair-100k.txt), [무작위 128비트 토큰](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-random-128-pair-100k.txt), [무작위 토큰+속성값](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-random-trace-attr-pair-100k.txt) 10만 건의 30회 교차 실행 결과다.

| 입력 | 단일 Parquet+전체 색인 | 현행 두 Parquet | 추가 저장량 | `request AND latency>=500` p95: 색인 / 현행 |
| --- | ---: | ---: | ---: | ---: |
| 기본 | 2,256,120B | 2,315,870B | −2.6% | 37.95 / 235.08ms |
| 순번형 고유 토큰·동적 경로 | 3,548,368B | 3,343,454B | +6.1% | 63.69 / 340.94ms |
| 무작위 128비트 토큰·동적 경로 | 9,619,849B | 7,405,690B | +29.9% | 75.37 / 333.79ms |
| 무작위 128비트 토큰·속성값·동적 경로 | 15,778,052B | 11,273,200B | +40.0% | 89.06 / 349.12ms |

마지막 입력의 색인은 1 Range GET/4,612,212B, 현행 analytics는 3 GET/4,739,744B였다. `request AND custom/236 EXISTS` p95도 87.01/204.03ms였지만, 토큰만 검색하는 `request` p50은 색인 28.58ms 대 현행 25.12ms였다. 같은 입력의 [제품 집계·상세 fallback](../../experiments/searchlayout/evidence-2026-09-24/actual-product-random-trace-attr-pair-single-gateway-100k.txt)은 단일 Parquet이 현행 두 역할보다 집계 p95 37.21/33.42ms, 상세 48.09/43.79ms로 느렸다. 따라서 색인 질의만 빠르다는 수치로 **전체 제품 경로가 압도적으로 우수하다**고 판정하지 않는다. 속성 블록은 JSON/Zstd이고 프로젝트·service·severity는 반복 패턴이므로 다른 분포에 대한 비용 보장도 아니다. 두 객체의 PUT·백업·병합 비용, 제품 query child와 권한·ACK·복구는 여전히 미검증이다.

[격리 MinIO 기본 입력](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-baseline-minio-100k.txt)과 [무작위 검색어·속성값 입력](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-random-trace-attr-minio-100k.txt)도 실제 `S3Store.PutStream`으로 analytics와 색인 객체를 올리고 제품 검증 게이트웨이의 S3 Range로 같은 집계를 30회씩 다시 비교했다. 두 입력 모두 2 PUT·2 HEAD·2 업로드 검증 GET 후 정답이 일치했고 OOM은 없었다. 기본 입력에서 `request AND latency>=500` p95는 색인 51.98ms/1 GET/47,246B, 현행 analytics 264.04ms/1 GET/1,010,847B였다. 무작위 검색어·속성값 입력은 색인 87.88ms/1 GET/4,612,212B, 현행 analytics 348.11ms/3 GET/4,739,744B였다. 같은 무작위 입력의 토큰 단독 `request` p50은 색인 31.09ms, 현행 27.67ms로 일관된 지연 우위가 아니다. 이 MinIO 실행에서는 후보 원문용 단일 Parquet을 업로드하지 않았으므로 **전체 후보 PUT·보관·복구 비용**은 측정하지 않았다. localhost MinIO와 서로 다른 Go/DuckDB 실행기 결과를 실제 AWS p95/p99나 제품 승인 근거로 확대하지 않는다.

[같은 MinIO 입력의 CPU 계측 재실행](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-random-trace-attr-minio-cpu-100k.txt)은 `getrusage(RUSAGE_SELF)`로 테스트 프로세스·DuckDB·게이트웨이의 작업별 CPU를 기록했다. `request AND latency>=500` p50 CPU는 현행 360.62ms, 색인 90.06ms였지만 토큰 단독 `request`는 현행 32.47ms, 색인 36.07ms였다. 이 값은 별도 MinIO 프로세스의 CPU, PG, 제품 워커의 다중 요청 경쟁을 포함하지 않는다. [파일 Range CPU 재실행](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-dynamic-random-trace-attr-cpu-100k.txt)도 결합 질의의 CPU 이득과 단독 검색의 불일치를 보였다.

[동일 압축 색인의 분할·병합 검사](../../experiments/searchlayout/product_compact_generations_test.go)는 앞의 `StageRecord` 입력과 같은 색인 생성 함수를 사용했다. 무작위 128비트 고유 토큰·무작위 속성값 10만 건을 4개 세그먼트로 나눈 뒤 단일 병합 세그먼트와 비교했다. `fatal`, `request`, `request AND latency>=500`, `request AND custom/236 EXISTS`, 고유 `trace`, 부재어의 그룹별 정확한 건수·severity 합이 입력 레코드를 직접 읽는 독립 oracle와 일치했다. 한 세그먼트의 검색값을 바꿔 새 세대를 만든 후 이전 세그먼트를 다시 읽어 이전 결과가 유지되는 것도 확인했다. [ARM64 CPU1/512MiB 원시 로그](../../experiments/searchlayout/evidence-2026-09-24/actual-product-compact-generations-random-128-attr-100k.txt)에서 4개 색인 객체 합계는 4,603,914B, 병합 색인 하나는 4,612,212B였고 OOM은 0이었다. cgroup peak는 536,875,008B로 한계에 닿아 메모리 여유를 입증하지 못한다. 이 검사는 **색인 부분의 로컬 논리적 세대**만 다룬다. 원문 Parquet의 병합, PG 카탈로그 원자 교체·권한·ACK, S3에서 고정된 객체 키 읽기와 삭제 지연, 실제 다중 세그먼트 GET·p95/p99는 검증하지 않았다.

[고정 가격 시나리오](../../tests/comparison/pricing.json)의 2026-09-21 `us-east-1` S3 Standard 보관 단가 USD0.023/GB-month와 GET USD0.0004/1,000건을 프로젝트 보고서의 `bytes/2^30` 환산으로 대입하면, 무작위 검색어·속성 입력 10만 건의 추가 **4,504,852B**는 월 약 **USD0.0000965**이고 3→1 GET 절감은 해당 세그먼트를 읽는 질의당 **USD0.0000008**이다. **약 121질의/세그먼트·월**이 두 항목만의 교차점이다. 이는 [AWS S3 가격](https://aws.amazon.com/s3/pricing/)을 넣은 조건부 산식이지 비용 절감 판정이 아니다. GET 차이가 없는 기본 입력에는 이 산식이 적용되지 않는다. 고정 Fargate 워커는 처리 시간이 줄어도 작업 수·가동 시간이 줄지 않으면 청구액이 줄지 않으며, journal·백업·병합 중 중복 객체·PG·MinIO 자체 자원·실제 질의 분포는 이 산식 밖이다. 따라서 저장량 +40%만으로 총비용 패배라고 단정하거나 CPU p50만으로 승리라고 단정하지 않는다.

| 후보 | 확인한 결과 | 판정 |
| --- | --- | --- |
| 128개씩 고정 폭 delta ID + TF | 10만 문서 baseline에서 postings 저장 바이트 355,792→291,196(**18.2% 감소**), clustered 356,879→309,686(**13.2% 감소**); 5만 noisy에서는 518,137→637,766(**23.1% 증가**). `request` 99,000건 복원 438→442µs, `timeout` 990건 3.97→7.81µs. 모든 목록 왕복 일치 | 블록 압축은 실행 가능한 대안이나 이 단순 구현은 일관된 우위가 없다. **Partitioned Elias-Fano나 SIMD/PForDelta를 구현·검증한 결과로 해석하지 않는다.** 기본 codec 변경 보류. |
| 16-bit duration 비트 슬라이스 SUM | 10만 baseline에서 `request` 99,000건의 ID 순회 38.3µs, 비트 슬라이스 15.7µs; `fatal` 10건은 3ns 대 17.0µs. 16개 plane 원시 200,064B 대 일반 uint16 200,000B. 같은 입력의 전체 zlib은 1,048B 대 2,744B지만 clustered는 18,326B 대 8,189B, wide는 200,092B 대 200,028B. 세 term의 합 모두 일치 | 넓은 SUM만 이득. 그룹별 집계·희소 검색까지 같은 구조로 압도하지 못하고 16 plane의 S3 접근 비용도 미측정. 기본 컬럼을 대체하지 않는다. |
| 원문 byte 3-gram 후보 + 재검증 | 2만 noisy에서 gram postings 1,267,698B, 압축 원문 512,404B(서로 다른 저장 항목); `aabb` 후보 121건→정확 4건. `service`/`trace`는 후보 20,000건 전부. baseline postings 59,881B/원문 54,049B. 테스트 literal의 누락 0 | 임의 부분 문자열을 빠르게 만드는 기본 저장물로 넣지 않는다. 3바이트 미만·한국어 문자 경계·regex 일반형은 이 실험 범위 밖이다. |
| 정렬 사전 블록 + 6건 이하 postings inline | 5만 문서 단일 세그먼트의 사전+postings: baseline 199,520→188,015B(**5.8% 감소**), noisy(104,100어휘 중 100,001개 inline) 2,412,249→1,446,352B(**40.0% 감소**). 512어휘/블록, 블록 CRC, 희소 위치표를 포함했고 모든 term·DF·inline postings·범위를 왕복 검증 | 지금까지의 추가 후보 중 **고유 토큰 비용을 가장 직접적으로 낮췄다**. S3에서는 작은 희소표+해당 블록만 읽을 수 있다. 다만 한 세그먼트의 바이트 결과이며 실제 Range GET·지연·병합은 미검증이다. |
| Top-K 블록 생략 | 점수 100/1/1에서 마지막 두 문서는 Top-1에 절대 못 들지만 정확한 건수는 3, SUM은 31이고 Top-1만 세면 1/1 | Top-K 점수 계산만 생략 가능. 같은 질의의 정확한 전체 집계까지 생략하면 오답이다. |

사전 수치는 [시제품 코드](../../experiments/searchlayout/dictionary_test.go)와 [ARM64 원시 로그](../../experiments/searchlayout/evidence-2026-09-24/dictionary-blocks-darwin-arm64.txt)에 있다. 두 포맷 모두 동일한 postings 내용, CRC와 포함/제외 기준(원문·컬럼 제외)으로 셌다. 시제품에는 제품 포맷 버전, 카탈로그 세대, S3 실패 처리 및 병합이 없으므로 위 절감률은 **이 부분 구조의 측정치**다.

따라서 **공통 문서 ID를 공유하는 postings+컬럼+원문**은 의미 모델의 후보로만 유지하고, 현재 세그먼트와 전체 압축 어휘 블록의 물리 포맷은 채택하지 않는다. 정렬 사전 블록+짧은 postings inline은 사전의 우선 실험 구조이며, 새 codec·BSI·3-gram을 기본 저장 포맷에 추가하지 않는다. Roaring과 파티션 Elias-Fano는 논문/실사용 구현을 확인했지만 이 TF 포함 세그먼트에서 독립 A/B를 수행하지 않았으므로 우열 미판정이다. 블록 사전도 실제 S3 GET·메모리·다중 세그먼트 병합을 재기 전에는 제품 포맷으로 확정하지 않는다.

## 비용과 우월성의 기준

저장 비용은 `journal + source + postings + fast fields + catalog + 백업 + 병합 중 임시 객체`의 합이다. 읽기 비용은 S3 요청 수, 작업 시간/CPU, cache 용량, PG 트랜잭션과 결과 전송까지 포함한다. [AWS 공식 가격표](https://aws.amazon.com/s3/pricing/)에 따르면 같은 리전 S3→AWS 서비스 데이터 전송은 일반적으로 과금되지 않지만 GET 요청과 저장은 과금된다. 그래서 Range 바이트 감소만으로 월 비용 감소를 말하지 않는다.

모든 워크로드에서 검색·집계·저장량이 동시에 지배적인 단일 포맷은 아직 없다. 임의의 다중 단어 조건에서 **정확한 전체 집계**를 요구하면 매치 집합의 값들을 읽거나, 가능한 조합에 대한 값을 미리 중복 저장해야 한다. 희소·광범위·고유 토큰·넓은 숫자·한국어 텍스트 비중에 따라 유리한 물리 구조가 달라진다. 따라서 “압도적”은 같은 기능·정확도·데이터에서 총비용 또는 p95/p99가 명백히 우세하고 다른 필수 질의가 퇴보하지 않을 때에만 쓴다. 현재 결과는 그 제품 간 결론에 도달하지 못했다.

## 제품 전환 전 필수 검증

1. 승인된 실제 정제 데이터와 질의 분포로 corpus SHA, 출현 빈도·문서 길이·필드 폭·보존 기간을 고정한다. 한국어 분석기와 부분 문자열/regex 정답을 독립 oracle와 비교한다. 현재 공백 토큰 분리와 6바이트 컬럼 fixture는 제품 데이터가 아니다.
2. 동일한 입력·기능·권한·정확한 집계를 가진 현행 DuckDB/Parquet, 이 설계의 완성 구현, [Quickwit](https://quickwit.io/docs/overview/architecture), [ClickHouse text index](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)를 별도 설치로 비교한다. Go 실험 속도를 타 제품 속도로 부르지 않는다. [DuckDB FTS는 원본 테이블 변경 시 자동 갱신되지 않는다](https://duckdb.org/docs/current/core_extensions/full_text_search)는 점도 온라인 경로 평가에 포함한다.
3. 실제 AWS 같은 리전과 별도 S3 호환 저장소에서 cold/warm, 작은/큰 세그먼트, 1/2/4 worker, open-loop 30분 혼합 부하, 취소/재시작, GET·PUT·보관·컴퓨트·PG/백업 비용을 함께 측정한다. 한 worker의 512MiB 제한뿐 아니라 전체 설치 비용을 센다.
4. journal ACK 손실·중복, fenced Publish 충돌, 권한 철회/OR 우회, 병합 중 페이지 이동, 삭제·보존·PITR·S3 훼손·PG/S3 복원을 장애 주입으로 통과시킨다. 이 단계가 없으면 기존 제품의 안전한 저장 경로를 바꾸지 않는다.

이 문서는 **검증할 의미 모델과 실패 조건**을 기록한다. 현 물리 포맷은 동일 fixture 비교에서 열세가 확인되어 전환 대상으로 확정하지 않는다. 위 네 gate가 통과되기 전의 구현은 `experiments/`에 격리하며, 현행 DuckDB 2.0 경로의 소유권과 제품 문서는 유지한다.
