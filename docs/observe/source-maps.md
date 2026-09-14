# Source Map 도입 기준

현재 Eventglass는 SDK가 보낸 scrubbed stack frame과 코드 문맥을 그대로 표시한다. JavaScript Source Map으로 원본 위치를 복원하려면 배포된 minified 파일과 해당 map이 동일한 build에 속한다는 증거가 필요하다. Sentry의 [Source Map 점검 문서](https://docs.sentry.io/platforms/javascript/guides/hono/sourcemaps/troubleshooting_js)는 `debug_meta`의 debug ID와 frame `abs_path` 일치, 배포 전 artifact 업로드, minified 파일과 map의 동시 제공을 요구한다. 서버가 오류에 실린 임의 URL을 fetch하거나 release 이름만으로 추측해서 적용하면 다른 코드 위치를 원인으로 표시할 수 있다.

도입할 때의 계약은 다음과 같다.

1. 빌드 단계에서 발급된 별도 프로젝트 CI 자격증명으로 minified JavaScript와 map을 함께 업로드한다. 브라우저에 공개되는 DSN key나 관리자 cookie를 artifact 쓰기 권한으로 재사용하지 않는다. 업로드가 끝난 뒤에만 해당 release를 배포한다.
2. 프로젝트·debug ID·code file의 정확한 연결과 artifact checksum/size를 검증한다. mismatch, 손상, map 부재는 기존 frame을 유지하고 잘못된 원본 위치를 보여주지 않는다. URL 기준 대체 매칭은 별도 검증 전까지 사용하지 않는다.
3. Artifact는 불변 content address로 저장하고 SQLite에는 권한·identity·checksum·크기·상태만 둔다. checkpoint가 참조 파일을 같은 cut에 포함해야 전체 유실 복구가 가능하다. 만료·용량 상한·S3 GET/cache budget을 실제 사용량에 따라 자동 적용한다.
4. 수집 ACK·Indexer commit/finalize에는 symbolication을 넣지 않는다. 상세 조회에서 권한을 확인한 뒤 제한된 map을 재사용하고, 실패하면 원본 scrubbed frame을 반환한다. SDK 원문과 immutable Record는 변경하지 않는다.
5. 실제 browser/Node SDK debug ID fixture, 배포 순서 오류, 중복 파일명, 잘못된 release, 권한 우회, malformed·oversized map, checkpoint 복구, CPU 0.25 core/1 GiB/50 GB 디스크에서의 조회 비용을 통과해야 UI에 기능을 노출한다.

현재는 CI 업로드 자격증명과 artifact/checkpoint 참조 계약이 없으므로 Source Map 기능을 노출하지 않는다. 빌드 파일을 수동 업로드해야만 동작하는 절반짜리 기능은 사람의 운영 개입 없이 동작한다는 제품 기준에 맞지 않는다.
