> Historical Rust reference only. Active Go scope and architecture are defined in
> [DESIGN.md](../../DESIGN.md) and [ARCHITECTURE.md](../../ARCHITECTURE.md).
> This document does not add requirements to the Go implementation.

# 0.25코어 운영 성능 개선 설계

## 목표와 측정 기준

단일 0.25 CPU, 1 GiB RAM, 스왑 없음, 로컬 디스크 예산 50 GB를 기본 운영 조건으로 둔다. 사용자에게 중요한 시간은 별도로 기록한다: 요청의 durable ACK, ACK부터 해당 Record의 검색 공개까지, 색인이 한창일 때의 검색 지연, 유휴 상태의 검색 지연. 모든 시간에 p50/p95/p99와 최대값을 남긴다. 10만 건 전체 완료 시간은 비교 자료이지만 단독 성공 기준이 아니다.

현재 `check-benchmark`의 `seed_elapsed_ms`에는 데이터 생성, SQLite 수락, 동시에 진행되는 Indexer 작업이 함께 들어간다. `visibility_lag_ms`는 적재가 끝난 시점에 남은 backlog만 재므로 순수 색인 시간이 아니다. 2026-09-15의 동일 10만 건 debug test executable 실행에서 이전 버전은 seed 270초/남은 공개 323초, 구조 비교 실험은 seed 225초/남은 공개 507초였다. 후자 실험은 전체 시간이 593초에서 758초로 악화되어 폐기했지만 두 수치 모두 실제 release 운영 성능의 기준으로 사용하지 않는다. benchmark는 배포와 같은 release 최적화 executable만 허용하고 보고서에 build profile을 기록한다.

release 기준선은 동일한 0.25 CPU/1 GiB/swap 0/50 GB 예산에서 1만 건 seed 1.231초와 남은 공개 2.900초였다. 10만 건 두 번의 실행은 공개 완료 41.751–53.566초, 전체 benchmark 42.37–54.39초, peak memory 253,825,024–259,289,088 bytes, data directory 162,603,059–165,643,947 bytes, OOM 0이었다. debug 수치 593초와 release 공개 완료의 차이는 약 11.1–14.2배다. 이후 성능 후보는 이 release 기준선을 반복 측정해 비교한다.

두 번째 10만 건 실행 전에는 Tantivy merge가 directory listing 이후 파일을 제거해 active size 측정의 metadata 조회가 `NotFound`로 끝나는 경쟁이 한 번 재현됐다. active size는 rollover를 위한 반복 관측값이므로 조회 전에 사라진 entry만 제외하고, root 소실·다른 I/O 오류·non-regular entry는 계속 실패시킨다. 수정 후 동일 release 10만 건 실행이 정상 완료됐다.

후속 Indexer 구조 변경에 앞서 동일 seed와 컨테이너 제한 아래 수락의 정규화·크기 계산·chunk 직렬화·SQLite commit, Indexer의 Inbox 읽기/검증·Error 중복 조회·native 문서 변환/commit·SQLite finalize·reader reload·병합 대기, 검색의 CPU 시간과 지연을 분리한다. Indexer는 배치 수와 Record 수, prepare/native commit/SQLite finalize/reader publish/active size 측정의 누적 시간, 최대 배치 시간과 크기를 원자 카운터로 기록한다. benchmark 보고서는 이 snapshot과 native segment 수, `cpu.stat`의 throttling 증거를 함께 남기고 seed 공개가 끝나는 즉시 중간 진행값을 출력한다. payload나 비밀값은 기록하지 않는다. 같은 바이너리와 seed를 반복 실행해 편차를 먼저 확인한다.

## 우선 변경: 정규화 Record의 직렬화 횟수 줄이기

수락 경로는 현재 Record마다 `serde_json::to_vec`로 크기를 확인한 뒤, 같은 Record가 들어간 Inbox chunk 전체를 다시 직렬화한다. 각 Record를 한 번 직렬화한 byte slice로 크기를 확인하고, 이 slice들을 canonical Inbox wrapper에 순서대로 이어 붙이면 수락 트랜잭션의 의미와 저장 바이트를 유지하며 중복 직렬화를 없앨 수 있다. 기존 serializer가 만든 chunk와 byte-identical한지 다양한 JSON 값, Unicode, 경계 크기, chunk 분할, overflow에서 대조한다. 요청 전체를 추가 복사하거나 더 큰 메모리 버퍼에 모으지 않는다.

Indexer finalize의 재직렬화를 원본 bytes 직접 비교로 바꾼 debug 후보는 finalize 누적을 줄였지만 background merge와 검색의 CPU 경쟁을 키웠다. 이 후보와 아래 native 정책 후보는 production 변경에 포함하지 않았으며, debug 결과를 release 성능 향상의 증거로 재사용하지 않는다. 저장 payload 재직렬화와 Error dedupe 재계산을 유지한다.

## 그다음 변경: native commit과 검색 간 CPU 경쟁 제어

계측 결과 1만 건/10개 배치의 누적 79초 중 native commit은 40.8초, Inbox 준비는 17.9초, SQLite finalize는 19.1초였고 reader reload와 active 크기 측정은 합계 0.03초 미만이었다. 배치 확대는 1만 건에서 빨랐지만 10만 건에서 후반 native 병합 비용이 커졌다. 3,000 Record/6 MiB는 10만 건 전체 725초와 최대 배치 45.8초로 기존 1,000 Record/4 MiB의 593초보다 악화되어 폐기했다.

debug executable에서 LogMergePolicy 최소 segment 변경, LZ4 document store, Tantivy `UserOperation` 묶음 전달은 반복 우위가 없었다. release 기준선이 10만 건을 53.566초에 공개하고 개인용 목표 처리량을 충분히 넘으므로 스키마나 저장 형식 migration은 진행하지 않는다. complete chunk와 1,000 Record/4 MiB, 기본 LogMergePolicy, Zstd document store, Record별 writer 전달, native commit → SQLite finalize → reader publish 순서를 유지한다. 새 DB, 추가 서버, 별도 검색 엔진은 도입하지 않는다.

## 통과 조건

각 변경은 원본 byte parity, ACK/중복 Error/검색 결과 parity, SQLite FULL/WAL, native commit·finalize·publish·seal의 crash/restart matrix, 0.25 CPU/1 GiB/스왑 없음, ENOSPC, 50 GB 데이터 예산을 통과해야 한다. 10만 건은 단일 실행의 우연한 승리로 판단하지 않는다. 반복 실행에서 durable ACK와 검색 공개 p95가 모두 악화되지 않고, 전체 CPU 시간 또는 운영 조건의 10만 건 완료 시간이 일관되게 줄어든 경우에만 채택한다. S3의 실제 유료 요청은 이 성능 개선의 필요 조건으로 삼지 않는다.
