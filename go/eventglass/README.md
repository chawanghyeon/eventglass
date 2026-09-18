# Eventglass Go 구현 시작점

이 디렉터리는 2026-09-18 사용자 요청으로 시작하는 **Go + DuckDB + PostgreSQL + S3 호환 저장소** 제품의 독립 구현 영역이다. 현재는 설계 인계 단계이며 실행 가능한 Go 제품은 아직 없다.

새 세션에서는 [AGENTS.md](AGENTS.md)를 읽고, [DESIGN.md](DESIGN.md)를 처음부터 끝까지 읽은 다음 §22의 G00부터 구현한다. DESIGN.md는 대화 이력 없이 사용할 수 있는 규범 명세다. [SDK-SOURCES.md](SDK-SOURCES.md)는 실제로 조사한 SDK 소스와 증거 수준을 기록한다.

- 제품 목표: 낮은 전체 운영비, Sentry SDK Error/Event 및 Structured Logs 수집, 자유로운 조건 검색·분석, S3 중심 영구 보관, 교체 가능한 작업자의 자동 확장.
- 기존 루트 Rust 제품과 `go/benchmark/`의 Go/SQLite FTS5 실험은 **변경하지 않는 비교군**이다. 둘을 이 구현으로 간주하지 않는다.
- 새 Go module의 예정 경로는 `eventglass/go/eventglass`, 바이너리 이름은 `eventglass-go`다. 의존성 및 실행 코드는 G00에서 실제 검증과 함께 추가한다.
- 문서의 수치는 초기 정책 또는 검증 목표다. SDK 호환성·512MiB 동작·처리량·비용 우위가 이미 입증됐다는 의미가 아니다.
- 기존 main 배포는 Rust 제품용이다. 이 디렉터리의 코드가 자동으로 운영 Rust 서버를 대체하지 않도록 한다.

## 새 에이전트에 줄 요청

> `go/eventglass/AGENTS.md`와 `go/eventglass/DESIGN.md`를 모두 읽고 G00부터 단계별로 구현하라. Rust 제품 및 기존 벤치마크는 비교군으로 보존하라. 실제 SDK wire fixture, 정확성 oracle, 장애 주입, Linux 자원 제한 검증을 통과한 범위만 완료로 표시하라. 필수 검색·내구성 계약을 성능 때문에 축소하지 말고, 실패한 가정은 측정 근거와 함께 명시하라. 테스트에서는 임시 DB·버킷·로컬 수신기만 사용하라. 현재 요청이 설계 인계 단계였다면 구현 완료로 오인하지 말라.
