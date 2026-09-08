# 백엔드·프론트엔드 아키텍처

이 문서는 구현의 의존 방향과 변경 단위를 고정한다. 2026-09-08 사용자 지시에 따라 기능 완료와 함께 아키텍처 품질을 검증한다. 진행 중 코드는 이 구조로 점진적으로 정리하며 파일 생성만으로 준수 완료라고 표시하지 않는다.

## 백엔드

단일 Cargo package를 유지한다. `main → app → 구체 기능 모듈`로 조립하며, domain/data 모듈은 HTTP를 import하지 않는다.

```text
main / app                       실행·조립·작업 수명
  ├── http                       HTTP DTO·인증 경계·응답·라우팅
  │     ├── auth                 비밀번호·세션·관리 작업의 정책
  │     ├── sentry               protocol·정규화·scrub
  │     ├── search               native query·페이지·집계
  │     └── db management API    짧은 관리 transaction
  ├── indexer                    순차 native commit·DB finalize·공개
  │     ├── issue                순수 grouping/lifecycle
  │     ├── db                   원자적 수신·확정·진행 위치
  │     └── storage              native shard 사용·seal
  ├── storage maintenance        archive·checkpoint·hydrate·회수
  └── alerts                     공통 search·outbox·전송

model / config                   하위 공통 값과 제한, 상위 모듈 의존 없음
```

- HTTP handler는 입력 decode/검증, 인증된 principal 생성, 구체 operation 호출, response 변환을 담당한다. 여러 SQL 문으로 이뤄진 정책/transaction은 `db/` 또는 `auth`의 구체 함수로 이동한다.
- DB의 `call` closure는 한 write thread로 넘기는 내부 실행 경계다. 기능별 공개 operation은 `accept_request`, `finalize_batch`, `resolve_issue`, `create_project`처럼 의도가 보이는 함수다. HTTP가 임의 SQL과 durable 상태 전이를 조립하지 않게 한다.
- Normalizer는 DB/HTTP 응답/S3를 모른다. 마스킹된 immutable Record를 만들고 ingest 단계가 ID/sequence/정책을 검사한다.
- Issue grouping/lifecycle는 입력과 현재 상태로 결과를 계산한다. SQL 저장, webhook 전송, Tantivy commit을 내부에서 호출하지 않는다.
- Indexer만 최종 indexing 순서를 소유한다. Storage maintenance는 요청을 전달하며 active writer를 직접 변경하지 않는다.
- Search의 권한·time basis·W 검사는 모든 소비자가 공유한다. Live/Alerts의 요구 때문에 별도 parser/evaluator를 만들지 않는다.
- 모듈별 에러는 의미를 보존하는 enum으로 표현하고 HTTP에서 매핑한다. unique 충돌을 503으로 처리하거나 사용자 입력을 내부 error string에 담는 구현은 완료 전에 제거한다.
- 런타임 task 수명과 shutdown은 app에 모은다. detached task를 곳곳에서 생성하지 않는다. worker 종료/실패를 관측할 수 있어야 한다.
- trait는 Clock/ObjectStore/AlertSender처럼 실제 테스트 대체가 필요한 경계만 허용한다. 폴더 수를 늘리기 위한 Service/Repository/Impl 쌍을 만들지 않는다.

## 프론트엔드

React + TypeScript + Vite, npm lock 하나. TypeScript strict 모드와 React의 단방향 데이터 흐름을 사용한다. 서버 상태는 TanStack Query, 화면 이동은 React Router의 고정된 routing으로 관리한다. 버전은 실제 resolve와 테스트 후 lockfile로 고정한다. 새로운 전역 상태 라이브러리는 시작 단계에서 추가하지 않는다.

```text
web/src/
  app/                    router, providers, shell, error boundary
  api/                    generated types, 공통 fetch/error/CSRF 처리
  features/
    auth/                 setup/login/session
    projects/             project/key 관리
    issues/               list/detail/status/occurrence
    logs/                 query/filter/rows/detail/live
    explore/              metrics/group/histogram
    alerts/               rules/deliveries
    system/               storage/backup/resource
  components/             Button/Dialog/Table 등 재사용 UI primitives
  lib/                    시간·ID·표현 보조 함수, domain 정책 없음
```

- page/component 안에서 직접 fetch를 복제하지 않는다. `api/client.ts`가 credentials, CSRF, typed error, AbortSignal을 처리한다. 타입 생성 결과를 수동 편집하지 않는다.
- Query key는 projects/filters/absolute time/read token을 포함한다. 같은 필터의 이전 데이터가 다른 권한 사용자에게 보이지 않도록 logout/권한 변경에서 query cache를 비운다.
- 서버 상태: TanStack Query. 공유 가능한 검색 상태: URL. 폼 초안/펼침/선택: local component state. 이 세 가지를 양방향 동기화하는 거대한 global store를 만들지 않는다.
- 상대 시간 `최근 15분`은 요청 시작 때 absolute bounds로 계산하고 rows/histogram이 같은 bounds와 read token을 공유한다. refetch 시점의 다른 now로 각각 요청하지 않는다.
- sequence와 i64 ID/count는 string 유지. 정렬·페이지네이션은 서버 계약을 따른다. 프론트에서 전체 로그를 모아 정렬/집계하지 않는다.
- 로그 상세 raw는 선택했을 때 요청한다. 목록 response와 query cache에 모든 raw payload를 쌓지 않는다.
- Live 연결 lifecycle은 feature hook 하나가 소유한다. Abort/unsubscribe/reconnect/dedupe/resync를 처리하고 UI unmount에 자원을 정리한다.
- 화면은 loading/empty/error/forbidden/stale/timeout 상태를 구분한다. API 실패를 빈 표로 표시하지 않는다. 지원 안 된 기능은 실제 기능처럼 버튼/차트를 꾸미지 않는다.
- raw JSON/message/stack은 text. `dangerouslySetInnerHTML` 사용 금지. 키보드·focus·label·상세 close 복귀를 실제 테스트한다.
- 기능 폴더는 다른 기능의 내부 파일을 직접 import하지 않는다. 공통 API/types/components 또는 명시적 feature 공개 경계만 사용한다.

## 변경 검토 기준

PR/단계 검증에서 다음을 확인한다: 책임이 적절한 위치에 있는가, transaction/permit/task의 소유자가 하나인가, 같은 정책을 두 군데 구현하지 않았는가, 테스트가 구현 내부를 그대로 반복하지 않고 외부 계약을 확인하는가, 아직 필요한 이유가 없는 추상화를 추가하지 않았는가.

초기 vertical slice를 만든 뒤 관리 SQL/에러를 분리하고, auth/project/search/storage 각 경계가 안정될 때 독립 리뷰를 수행한다. 줄 수만으로 파일을 자르지 않는다. 복구·권한·정확성 변경은 경계 테스트와 함께 검토한다.
