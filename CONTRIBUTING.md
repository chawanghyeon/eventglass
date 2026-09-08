# Eventglass 개발 안내

Rust 1.97.1, Node 22.22.2/npm 10.9.7, Python 3.12를 사용합니다. `./scripts/bootstrap`은 버전을 확인하고 잠긴 의존성·저장소 내부 검사 도구·Git hooks를 준비합니다. 사용자 전역 Git 설정은 변경하지 않습니다.

```sh
./scripts/bootstrap
./scripts/check quick         # 형식·구문·secret·workflow
./scripts/check rust          # Rust check/clippy/unit/doc
./scripts/check contracts     # native 라이브러리·SQLite 계약
./scripts/check integration   # 구현된 서버 통합 테스트
./scripts/check web           # API 생성물·typecheck·lint·unit·build
./scripts/check crash-smoke   # failpoint 프로세스 복구
./scripts/check embed-smoke   # UI 내장 바이너리
./tools/sdk-fixtures/bootstrap-live.sh
./scripts/check sdk-live      # 실제 Python/Node SDK 전송
./scripts/check resource      # Linux 실행: CPU 1 / 1GiB / swap 0
```

`resource`는 실행 가능한 Docker가 필요합니다. Linux 빌드는 2 CPU/4GiB에서 수행한 뒤, 생성된 테스트 프로그램만 1 CPU/1GiB 제한 컨테이너에서 실행합니다. cgroup 제한값과 OOM 여부를 검사하고 `.tools/resource/report.json`을 생성합니다. 첫 검사 범위에서 제외된 대규모 workload·전체 복구 등은 보고서에 명시합니다. 512MiB 추가 검사는 `./scripts/check-resource --memory-bytes 536870912`로 실행합니다.

Hook은 check-only입니다. 필요할 때 `cargo fmt --all`과 `npm --prefix web exec prettier -- --write .`을 실행하고 diff를 검토합니다. `SKIP`·`--no-verify`로 실패를 숨기지 않습니다.

완료하고 검증한 단위로 `main`에 직접 커밋하고 즉시 `git push origin main`을 실행합니다. 커밋 제목은 game-uridogu-com처럼 `feat: 한국어 변경 요약` 형식을 사용하며, 변경에 맞게 `fix`, `perf`, `test`, `docs`, `chore`를 선택합니다. 범위가 필요한 경우 `chore(deploy): …`처럼 붙입니다. 본문에는 필요한 문제 설명, 실제 변경, 실행한 검사와 남은 제한을 간결하게 기록합니다. 이미 공개한 커밋을 메시지 통일만을 위해 재작성하거나, 과거 단계를 소급한 가짜 커밋이나 단계별 Markdown 실행 일지를 만들지 않습니다. CI의 최종 `required` job과 저장소 branch protection 설정은 별개이며 후자는 적용 여부를 확인해야 합니다.

설계 기준은 [원안](docs/observe/source-design.md), [구현 계약](docs/observe/implementation.md), [아키텍처](docs/observe/architecture.md), [검증 기준](docs/observe/quality.md), [단계별 완료 조건](docs/observe/work-packages.md)에 있습니다. 첨부 원안은 그대로 보존합니다.
