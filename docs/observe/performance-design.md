# 0.25코어 운영 성능 개선 설계

## 목표와 측정 기준

단일 0.25 CPU, 1 GiB RAM, 스왑 없음, 로컬 디스크 예산 50 GB를 기본 운영 조건으로 둔다. 사용자에게 중요한 시간은 별도로 기록한다: 요청의 durable ACK, ACK부터 해당 Record의 검색 공개까지, 색인이 한창일 때의 검색 지연, 유휴 상태의 검색 지연. 모든 시간에 p50/p95/p99와 최대값을 남긴다. 10만 건 전체 완료 시간은 비교 자료이지만 단독 성공 기준이 아니다.

현재 `check-benchmark`의 `seed_elapsed_ms`에는 데이터 생성, SQLite 수락, 동시에 진행되는 Indexer 작업이 함께 들어간다. `visibility_lag_ms`는 적재가 끝난 시점에 남은 backlog만 재므로 순수 색인 시간이 아니다. 2026-09-15의 동일 10만 건 실행에서 이전 버전은 seed 270초/남은 공개 323초, 구조 비교 실험은 seed 225초/남은 공개 507초였다. 이 수치로 개별 함수의 비용이나 병합 비용을 단정하지 않는다. 후자 실험은 전체 시간이 593초에서 758초로 악화되어 폐기했다.

후속 Indexer 구조 변경에 앞서 동일 seed와 컨테이너 제한 아래 수락의 정규화·크기 계산·chunk 직렬화·SQLite commit, Indexer의 Inbox 읽기/검증·Error 중복 조회·native 문서 변환/commit·SQLite finalize·reader reload·병합 대기, 검색의 CPU 시간과 지연을 분리한다. 적어도 각 단계의 누적 CPU/실행 시간, 호출 수, 처리 Record/byte 수, native segment 수, cgroup throttling을 보고서에 남긴다. 측정용 계측은 payload나 비밀값을 로그에 쓰지 않고, 운영 경로의 할당·동기화를 늘리지 않도록 검사 전용으로 둔다. 같은 바이너리와 seed를 반복 실행해 편차를 먼저 확인한다.

## 우선 변경: 정규화 Record의 직렬화 횟수 줄이기

수락 경로는 현재 Record마다 `serde_json::to_vec`로 크기를 확인한 뒤, 같은 Record가 들어간 Inbox chunk 전체를 다시 직렬화한다. 각 Record를 한 번 직렬화한 byte slice로 크기를 확인하고, 이 slice들을 canonical Inbox wrapper에 순서대로 이어 붙이면 수락 트랜잭션의 의미와 저장 바이트를 유지하며 중복 직렬화를 없앨 수 있다. 기존 serializer가 만든 chunk와 byte-identical한지 다양한 JSON 값, Unicode, 경계 크기, chunk 분할, overflow에서 대조한다. 요청 전체를 추가 복사하거나 더 큰 메모리 버퍼에 모으지 않는다.

Indexer는 현재 저장된 chunk를 역직렬화한 다음 canonical 여부와 Record별 크기를 검사하고, finalize에서 저장된 바이트와 준비된 payload를 다시 직렬화해 비교한다. 이 구간은 안전성 계약을 분리해서 최적화한다. prepare/recover 때 저장된 원본 bytes와 metadata를 검증하고, 원본 bytes를 준비된 batch가 소유하게 한다. native commit에는 검증된 Record의 불변 참조만 전달한다. finalize는 같은 SQLite 트랜잭션에서 저장된 원본 bytes와 준비된 원본 bytes를 직접 비교한다. `PreparedBatch`를 외부에서 수정할 수 없게 캡슐화해야 재직렬화 제거가 안전하다. 기존의 Error dedupe 결과 재계산을 없애려면 단일 Indexer 외에 `issue_occurrences`를 쓰는 경로가 없는지 먼저 증명하고, 관련 경쟁·복구 테스트를 통과해야 한다. 이 조건을 충족하지 못하면 해당 재계산은 유지한다.

## 그다음 변경: native commit과 검색 간 CPU 경쟁 제어

색인 1,000 Record/4 MiB, complete chunk, native commit → SQLite finalize → reader publish 순서는 우선 유지한다. 각 경계의 실제 비용과 segment/merge 지표로 commit 횟수가 지배적인지 확인한 후에만 batch 상한이나 merge policy를 조정한다. 큰 batch가 이득이라도 저부하의 공개 지연, 장애 시 `(A, C]` 복구 한도, 롤백 바이너리 호환성, 메모리 상한을 함께 검증해야 한다. 검색이 진행되는 동안 병합 CPU가 지연을 늘린다면 현행 Tantivy 설정 안에서 병합 동시성·시점의 유효한 선택지를 별도 실험한다. 새 DB, 추가 서버, 별도 검색 엔진은 도입하지 않는다.

## 통과 조건

각 변경은 원본 byte parity, ACK/중복 Error/검색 결과 parity, SQLite FULL/WAL, native commit·finalize·publish·seal의 crash/restart matrix, 0.25 CPU/1 GiB/스왑 없음, ENOSPC, 50 GB 데이터 예산을 통과해야 한다. 10만 건은 단일 실행의 우연한 승리로 판단하지 않는다. 반복 실행에서 durable ACK와 검색 공개 p95가 모두 악화되지 않고, 전체 CPU 시간 또는 운영 조건의 10만 건 완료 시간이 일관되게 줄어든 경우에만 채택한다. S3의 실제 유료 요청은 이 성능 개선의 필요 조건으로 삼지 않는다.
