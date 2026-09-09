# Sentry SDK Replay 계약

기준: 2026-09-09, 공식 `@sentry/browser@10.73.0`의 실제 Chromium 전송. 고객 코드는 공식 Sentry SDK 설정만 사용한다. 테스트 페이지의 일반 UI 동작은 Playwright로 실행하며 별도 recorder/listener/tracking API를 추가하지 않는다.

## 기존 구조와 추가 경계

- 공개 `/api/{project_id}/envelope/`와 `/store/`: URL project ID, query/header public key, 선택적인 envelope DSN이 일치해야 한다. commit 시 프로젝트 활성 상태와 키를 재검사한다.
- Error/log는 sanitized Record → SQLite Inbox durable ACK → 단일 Tantivy Indexer로 처리된다. transaction/span은 기존에 미지원이다. breadcrumbs는 Record raw 안에 보관하며 별도 복제 테이블은 없다.
- 운영 SQLite 하나, 로컬 shard와 선택적인 S3-compatible 백업·hydration을 사용한다. 기존 Error/log archive는 자동 영구 삭제하지 않는다. Replay는 별도 크기·만료 정책이 필요하다.
- React/TanStack Query/URL search state와 OpenAPI 생성 타입을 재사용한다. Replay를 Tantivy Record로 위장하거나 별도 검색 언어를 만들지 않는다.

## 확인한 wire 형식

[createReplayEnvelope](https://github.com/getsentry/sentry-javascript/blob/10.73.0/packages/replay-internal/src/util/createReplayEnvelope.ts), [sendReplayRequest](https://github.com/getsentry/sentry-javascript/blob/10.73.0/packages/replay-internal/src/util/sendReplayRequest.ts), [Compressor](https://github.com/getsentry/sentry-javascript/blob/10.73.0/packages/replay-worker/src/Compressor.ts).

한 envelope에 `replay_event` JSON과 byte-length가 있는 `replay_recording`이 온다. recording은 `{"segment_id":N}\n` 다음에 JSON event array 또는 **zlib** 바이트가 온다. HTTP 전체 압축과 별개다. replay_id/event_id는 32 hex, segment_id는 0부터 증가한다. 메타데이터 timestamp/replay_start_timestamp는 초, rrweb timestamp는 밀리초다. SDK custom performanceSpan의 timestamp/startTimestamp는 초이므로 구분한다. `urls`, `error_ids`, `trace_ids`는 해당 segment에서 관측한 집합이다. 전체 세션의 누적값으로 오해하지 않는다. 세션 종료 확정 마커는 없다. finished_at은 마지막 관측 시각이다.

`tests/fixtures/replay/*.envelope`는 `tools/sdk-fixtures/browser-app/replay.mjs`가 SDK transport HTTP 요청에서 캡처했다. 비압축/압축 각각 3 segment, DOM/masking, click/movement/scroll/resize/navigation/console/network/error/trace/feedback을 포함한다. 생성 명령:

```sh
PLAYWRIGHT_BROWSERS_PATH=tools/sdk-fixtures/.playwright-browsers node tools/sdk-fixtures/browser-app/replay.mjs
```

검증한 버전은 10.73.0이다. 같은 wire contract의 다른 버전을 버전 문자열만으로 거부하지 않지만, 7/8/9 전 버전·native/mobile Replay 호환을 주장하지 않는다. gzip/raw-deflate recording, 독립 metadata/recording envelope, canvas/video 변환은 검증 없이 추정 지원하지 않는다. 전체 envelope의 gzip/deflate 지원은 기존 계약을 유지한다.

## A/B/C 분류

| 기능 | 근거/분류 | 처리 기준 |
|---|---|---|
| DOM replay | A: rrweb snapshot/mutation | 관리 UI에서 기존 replayer 사용, 영상 변환 없음 |
| URL/journey | A: Meta href, navigation breadcrumb/span; B: 시간 차 | 관측 구간만 표시, 누락 segment를 체류로 확정하지 않음 |
| 좌표 click | A: IncrementalSnapshot source=2, interaction type=2 | viewport 크기가 확인된 좌표만 정규화; iframe 좌표는 상위 좌표와 섞지 않음 |
| element click | A: ui.click breadcrumb message/nodeId | SDK selector 문자열만 사용, DOM에서 새 selector engine 구현하지 않음; breadcrumb throttling으로 원시 클릭과 수가 다를 수 있음 |
| movement | A: source=1 positions + timeOffset | 실제 기본 desktop payload로 확인. iOS는 SDK가 mousemove=false로 비활성화하므로 데이터 없음 |
| scroll | A: source=3 x/y | 관측 스크롤 위치 제공. 전체 문서 높이/동적 레이아웃의 정확한 백분율은 SDK가 직접 제공하지 않음. viewport 높이를 문서 높이로 오인하지 않음 |
| slow/dead/rage/multi | A: ui.slowClickDetected/ui.multiClick; B: 공식 predicate | slow timeout의 a/button/input을 dead, 이때 clickCount>=5면 dead-rage; multiClick>=5면 rage. 새로운 scoring 없음 |
| network/performance | A: performanceSpan | method/status/duration과 실제 id가 있는 연결만. 시간 인접성을 인과관계로 표시하지 않음 |
| error/trace | A: error_ids/trace_ids, event contexts | 수신한 기존 Error에 연결. transaction/span 저장을 지원하지 않는 동안 backend trace를 재구성했다고 표시하지 않음 |
| browser/os/device | A: contexts에 있을 때 | 기본 Browser fixture에는 UA만 있고 이 context들은 없음. 없는 정보를 가짜 값으로 채우지 않음 |
| feedback | A: feedback item, contexts.feedback | 공식 captureFeedback/feedbackIntegration의 메시지·replay association; survey builder 아님 |
| exact scroll reach, custom funnels/attribution/form capture | C 또는 데이터 부족 | 추가 계측·입력값 추출·추측 금지 |

Frustration 기준은 [Sentry replay type predicates](https://github.com/getsentry/sentry/blob/master/static/app/utils/replays/types.tsx)의 isDeadClick/isDeadRageClick/isRageClick과 [SDK click detector](https://github.com/getsentry/sentry-javascript/blob/10.73.0/packages/replay-internal/src/coreHandlers/handleClick.ts)를 대조한다. 코드 경로는 upstream 변경 가능하며 fixture와 predicate 회귀 테스트가 이 구현의 기준이다.

## 자원과 privacy

recording 해제 상한 20 MiB, JSON 노드 200,000, 깊이 64, segment ID 0–10,000. 기존 일반 Event의 20,000 노드 제한은 유지한다. 압축 해제 중 상한을 적용하고 실패한 지원 item은 요청 전체를 거부한다. 과도한 segment는 413이며 ACK 후 버리지 않는다.

SDK의 masked text/input과 blocked node는 그대로 유지한다. 기존 프로젝트 secret scrub도 적용한다. 분석은 input/키 입력/폼 값/DOM 텍스트를 별도 추출하지 않는다. SDK에서 unmask 옵션을 설정한 고객 데이터의 원본을 새로 추측하거나 복원하지 않는다. raw envelope와 recording을 이중 저장하지 않는다.

## 저장·API·운영 계약

SQLite schema 2의 `replays`는 검색용 metadata·파생 count, `replay_segments`는 `(project_id,replay_id,segment_id)` unique reference, `feedback`은 공식 Feedback 메시지를 보관한다. DB에는 recording을 넣지 않는다. canonical scrubbed rrweb 배열을 zlib로 저장한 `replay-blobs/<sha256>.zlib`를 fsync한 후 기존 ingest 트랜잭션이 reference를 커밋한다. 동일 내용 재전송은 no-op, 같은 segment ID의 다른 내용은 409이며 혼합 envelope의 Error/Log도 함께 롤백한다. 도착 순서와 무관하게 segment metadata 집합을 병합한다. 가장 큰 segment ID보다 앞에 빈 번호가 있으면 Partial replay다. 종료 마커가 없으므로 마지막 segment 자체가 도착하지 않은 경우는 탐지할 수 없다.

S3 설정 시 기존 체크포인트가 정확한 Replay blob 집합·hash·size·metadata revision을 함께 포함한다. 복구 후보 하나의 SQLite와 모든 blob이 일치해야 한다. 체크포인트 제어 문서의 기존 4 MiB 한도를 넘으면 완료로 게시하지 않고 backup lag를 유지한다. 최신 포인터는 최적화이며 손상되면 완료 후보 listing으로 복구한다. 이미 완료 체크포인트가 검증한 동일 blob은 재업로드하지 않는다. Replay/Feedback만 들어와도 기존 단일 Indexer가 봉인 경계를 만들어 백업한다. metadata 변경 시 최소 5분 간격으로 시도하며 고정 RPO는 보장하지 않는다. `doctor`도 Replay 파일 무결성을 검사하고 backup 상태는 아직 백업되지 않은 Replay를 반영한다.

조회 가능 기간은 최초 수신부터 30일이다. 만료 metadata와 로컬 미참조 blob은 시작 시와 실행 중 제한된 배치로 정리한다. 실행 중에는 기존 reader/ingest/backup gate로 충돌을 막으며 부하가 지속되면 물리적 삭제가 지연될 수 있다. S3의 과거 완료 checkpoint와 blob은 기존 백업 보존 정책을 따르며 자동 영구 삭제하지 않는다. 따라서 30일은 S3 개인정보 영구 삭제 SLA가 아니다. S3 lifecycle은 복구 후보가 참조하는 파일을 임의로 먼저 지우지 않도록 전체 백업 보존 정책으로 운영해야 한다. Replay 파일은 현재 로컬 조회하며 Error shard처럼 S3 cold eviction하지 않는다.

관리자 session과 기존 project 검색 권한을 사용하는 API:

- `GET /api/replays`: project 필수, environment/release/URL/user/error/rage/dead/duration, 50개 cursor 페이지.
- `GET /api/replays/{project}/{replay}`: metadata, segment 목록, 수신된 Error/Feedback association.
- `GET /api/replays/{project}/{replay}/segments/{segment}`: rrweb 배열.
- `GET /api/replays/{project}/{replay}/analysis`: timeline/journey/페이지 활동.
- `GET /api/replay-pages`: 같은 필터의 최근 최대 20 replay 집계.
- `GET /api/feedback?project_id=…`: 최근 20개 공식 text Feedback. 첨부 screenshot은 지원하지 않는다.

분석은 동일 query permit으로 직렬 제한하고 요청당 64 MiB/10초, timeline 5,000개, page 500개에서 명시적으로 잘린 결과를 표시한다. 세션 저장 한도는 256 MiB다. 재생 버퍼는 64 MiB/100,000 event이며 segment를 순차 로드한다. 전체 기간·전체 사용자의 통계라고 표시하지 않는다. 좌표는 20×20 viewport grid이며 문서 전체 위치가 아니다. movement는 batched sample의 timeOffset과 당시 viewport/navigation을 사용한다. element 표는 SDK selector label 집계로 DOM identity를 보장하지 않는다. URL은 query/fragment를 제거한 실제 origin/path이며 임의 route template 추론은 없다.

스크롤은 상위 document의 관측 y/viewport 높이와 0/1/2/3/5 화면 높이 이상 도달한 sampled replay 수를 표시한다. iframe/개별 container 스크롤을 문서 스크롤로 합치지 않는다. dynamic layout/resize에 따른 best-effort 값이며 사람 수·문서 백분율이 아니다.

관리 UI `features/replays`는 URL 필터, TanStack 서버 상태, 로컬 재생 상태를 분리하고 OpenAPI 생성 타입을 사용한다. `@sentry/rrweb@2.43.2` replayer는 상세 화면에서만 lazy load한다. scripts 없는 sandbox iframe과 CSP로 원격 이미지·폰트·프레임·폼 제출을 차단한다. canvas render hint는 부모 document image loader를 차단하기 위해 재생 버퍼에서만 제거한다. 이로 인한 외부 asset/canvas 시각 차이는 UI에 표시하며 원본 녹화나 masking을 복원하지 않는다.

schema 2 도입 전 배포한 `ad3cdf1`은 additive schema 2를 checksum 검증 후 읽는 롤백 호환 릴리스다. 이전 릴리스로 돌아가야 한다면 이 버전 이상을 사용한다. 해당 bridge는 Replay 기능을 노출하지 않으며 Replay 참조를 이해하지 못하는 불완전한 checkpoint 생성도 거절한다. schema 1을 schema 2로 자동 업그레이드하는 회귀 검사를 유지한다.

## 고객 설정

```sh
npm install @sentry/browser
```

```js
import * as Sentry from '@sentry/browser';

Sentry.init({
  dsn: 'https://PUBLIC_KEY@eventglass.uridogu.com/PROJECT_ID',
  integrations: [Sentry.replayIntegration()],
  replaysSessionSampleRate: 0.1,
  replaysOnErrorSampleRate: 1.0,
});
```

DSN은 관리 화면에서 발급한 값을 복사한다. 비율은 서비스 트래픽에 맞게 선택한다. React/Vue/Next.js는 각각 공식 프레임워크 SDK의 정상 client 설정에 같은 Replay integration을 추가한다. 별도 recorder/tracker/snippet은 필요하지 않다. Feedback이 필요하면 공식 `Sentry.feedbackIntegration({ enableScreenshot: false })`만 선택적으로 추가한다. Replay sampling 밖의 방문과 iOS mouse movement는 이 서버가 새로 계측하지 않는다.

## 상품 상세페이지에서 사용하기

Replay 목록에서 프로젝트, `URL=/products/linen-shirt`, 세션 시작 기간, environment/release를 지정하고 `페이지별 Heatmaps`를 연다. 실제 origin/path 단위이므로 상품별 비교가 가능하며 임의 상품 ID나 conversion taxonomy를 만들지 않는다. 경로 전체를 모으려면 `/products/`로 URL을 좁힐 수 있다.

페이지별 sampled Replay 수, 재방문을 포함한 관측 방문, timestamp로 계산한 평균 관측 시간, 직접 연결된 이전/다음 URL, 마지막 관측 페이지 수, 공식 rage/dead 신호를 확인한다. 지연 도착한 frustration breadcrumb는 수신 시점의 페이지가 아니라 SDK가 기록한 원래 클릭 시각의 페이지로 연결한다. 관련 Replay 최대 3개로 바로 이동해 실제 화면과 network/error timeline을 대조한다. 페이지 진입 viewport가 768px 미만/이상인 Replay 수는 화면 크기 분포이며 브라우저·기기 추론이 아니다. 비활성 시간은 관측 시간에 포함된다. 마지막 관측 페이지를 이탈·bounce로, `/cart` 이동을 장바구니 담기 성공·결제로 표시하지 않는다.

긴 페이지는 화면 상단의 scroll y / 당시 viewport 높이를 1화면 단위(0–99)로 나눠 해당 구간의 좌표 클릭 수와 SDK selector breadcrumb를 보여준다. 리뷰 펼치기나 고정 구매 버튼이 어느 스크롤 위치에서 클릭됐는지 비교할 수 있다. 고정 요소를 문서의 절대 위치로 오인하지 않으며 DOM 구역명을 자동 추측하지 않는다. 구간당 selector 상한 50개와 기존 요청 전체 한도를 유지한다. 정확한 document-height 백분율·고유 사람 수·매출·구매율을 생성하지 않는다.

실제 SDK 테스트 `tools/sdk-fixtures/browser-app/shopping.mjs`는 responsive 상품 페이지에서 데스크톱(1280×800)과 모바일 크기(390×844)의 리뷰 이동·내용 펼치기·스크롤·고정 장바구니 버튼·URL 이동을 캡처한다. 저장된 `shopping-*.envelope`는 손으로 작성한 rrweb 데이터가 아니다. Product UI E2E는 이 데이터를 수집해 집계, 실제 보이는 첫 snapshot, 관련 Replay 이동, 390px 관리 화면의 가로 넘침을 검증한다. 고객 서비스에는 여전히 공식 Sentry SDK만 설치한다.

## 화면 크기 비교와 관측 시점 링크

Page maps의 `viewport=narrow|wide|mixed|unknown`은 최근 최대 20개 Replay를 읽은 뒤 적용한다. 전체 기록에서 확인된 viewport 폭이 모두 768px 미만이면 narrow, 모두 이상이면 wide, 경계를 넘으면 mixed다. 실제 휴대폰·데스크톱 종류로 추정하지 않는다. 미관측 구간은 복원하지 않으며 partial/truncated 경고는 유지한다. 목록 자체의 메타데이터 필터와 별개인 분석 필터이고 URL 및 query cache key에 보존한다.

히트맵 cell, SDK selector, 스크롤 구간·도달 지점, frustration에는 실제 timestamp의 대표 관측을 페이지당 최대 128개 붙인다. 움직임이 많아도 selector/frustration이 밀려나지 않도록 우선한다. 원본 이벤트를 DB에 복제하지 않는다. `/replays/{project}/{replay_id}?t={epoch_ms}`로 진입·새로고침·뒤로 가기해도 공식 replayer의 해당 시점을 표시한다. 최초 full snapshot 이전과 처리 범위 이후는 재생 가능한 범위로 제한한다.

## 실행 중 보존 정리와 진단

기존 ingest/query permit과 BackupCoordinator의 snapshot/upload gate를 모두 즉시 획득할 수 있을 때만 60초 주기로 로컬 Replay를 정리한다. 세션은 최대 16개/256 segments, 큰 세션은 1개(최대 10,001 segments), Feedback은 256개씩 만료 처리한다. DB commit 후 디렉터리를 최대 256개씩 순회해 참조 없는 압축 파일만 unlink/fsync한다. 순회 커서는 재사용하고 끝나거나 실패하면 다시 연다. DB commit 이후 중단·삭제 실패로 남은 orphan은 다음 순회 또는 시작 시 정리된다. 다른 프로젝트가 참조하는 동일 blob은 유지한다. 원격 completed checkpoint 객체는 건드리지 않는다. 읽기·업로드·수집 중에는 다음 주기로 미루며 지속적 부하에서는 정리가 지연될 수 있다.

취소된 비동기 요청도 실제 DB 작업이 끝날 때까지 permit을 보유한다. graceful shutdown은 보존 작업 종료 후 Indexer/backup을 종료한다. 관리자 System 화면에는 active/partial/expired Replay, 참조 segment/중복 제거 압축 크기, 백업 변경분, 정리 최근 성공·실패/유휴 대기와 프로세스 시작 이후 삭제량을 제공한다. Sentry 수집 응답 카운터는 모든 완료된 transport 요청의 고정된 상태 분류이며, 재시도·무시된 item도 포함하고 재시작 시 초기화된다. Replay 수집 성공률·실제 방문자 수로 오인하지 않는다. 클라이언트 코드는 바뀌지 않는다.
