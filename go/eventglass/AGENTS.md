# Eventglass Go 독립 구현 지침

이 하위 디렉터리는 사용자에게 명시적으로 승인된 신규 Go 제품 영역이다. 루트 Rust 구현의 `SQLite 하나 + 순차 Indexer 하나`, Tantivy QueryParser 고정이라는 구현 선택은 이 영역에 적용하지 않는다. 대신 [DESIGN.md](DESIGN.md)의 PostgreSQL·S3·DuckDB 및 분산 공개 계약을 따른다. durable ACK·권한·정확성·복구·마스킹 의무는 유지한다.

## 범위

- 시작 전 DESIGN.md 전체를 읽는다. 소스 조사 근거가 필요하면 SDK-SOURCES.md를 읽는다.
- 신규 코드·migration·UI·테스트·배포·의존성은 이 디렉터리 아래에 둔다. 루트 `src/`, `migrations/`, `web/`, `Cargo.*`, `go/benchmark/`, 기존 fixture는 비교 기준으로 읽기만 한다.
- `docs/observe/source-design.md`를 수정하지 않는다. 기존 루트 AGENTS.md의 사용자 변경을 커밋에 섞지 않는다.
- 원안과 달라지는 정책은 DESIGN.md에서만 명시한다. 루트 Rust를 이 설계에 맞춰 바꾸지 않는다.
- 최초 구현은 G00 기술 계약 검증부터다. DESIGN.md의 게이트를 통과하지 않은 기능을 지원한다고 표시하지 않는다.
- 다른 에이전트·서브에이전트를 사용하지 않는다.

## 구현 및 검증

- 기존 엔진의 SQL·Parquet·정규식·집계 기능을 사용한다. SQL parser, Parquet codec, 범용 분산 SQL 실행기를 새로 만들지 않는다.
- 검색 표현식은 DESIGN.md에 정의한 제한된 CEL 문법을 기존 CEL parser로 파싱하고 허용된 AST만 SQL로 변환한다. 임의 SQL 실행은 제공하지 않는다.
- native DuckDB는 격리된 자식 프로세스에서 사용한다. 로컬 DuckDB 파일을 공유 운영 DB나 영구 정본으로 사용하지 않는다.
- 테스트는 임시 PostgreSQL DB·S3 bucket/prefix·디렉터리와 localhost SDK 대상만 사용한다. 운영 DSN, 운영 버킷, 외부 webhook을 사용하지 않는다.
- 새 Go 측 명령은 DESIGN.md §22에서 구현하기 전에는 존재한다고 보고하지 않는다. 검사 스크립트는 미구현 필수 target을 skip/success로 처리하지 않는다.
- source inspection, 실행한 fixture, 리소스 측정, 실제 AWS 검증을 구분해 보고한다. 모든 SDK/언어의 완전 호환을 주장하지 않는다.
- 성능 비교는 같은 의미의 요청·데이터·결과 및 CPU/RAM/디스크/네트워크 조건에서 한다. Rust의 유리한 조건만 제거하거나 Go의 공유 DB 비용을 빼지 않는다.

## 커밋과 배포

루트 CONTRIBUTING.md의 검증·커밋·push 절차를 따른다. 훅을 우회하지 않는다. 기존 pre-push는 Rust 배포 artifact를 staging한다. 새 제품용 배포 정의는 이 디렉터리에서 별도로 만들고, 운영 Rust 교체·데이터 이관은 별도 사용자 요청 전에는 수행하지 않는다. 환경 때문에 push가 막히면 커밋과 실패 원인을 보고한다.
