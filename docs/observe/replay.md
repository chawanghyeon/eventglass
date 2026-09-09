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
