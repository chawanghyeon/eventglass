import { lazy, Suspense, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { Notice } from "../../components/Notice";
import { Button } from "../../components/Button";
import { useSession } from "../auth";
import { replayQuery, analysisQuery, recordingQuery } from "./queries";
const ReplayPlayer = lazy(() =>
  import("./ReplayPlayer").then((module) => ({ default: module.ReplayPlayer })),
);
import { PageMaps } from "./PageMaps";
import { feedbackMessage } from "./feedback";
import { duration, userLabel } from "./presentation";

export function ReplayDetailPage() {
  const { project = "", id = "" } = useParams();
  const session = useSession();
  const user = session.data;
  const detail = useQuery({
    ...replayQuery(user?.id ?? "unknown", project, id),
    enabled: !!user && !!project && !!id,
  });
  const recording = useQuery({
    ...recordingQuery(
      user?.id ?? "unknown",
      project,
      id,
      detail.data?.segments ?? [],
    ),
    enabled: !!user && !!detail.data,
  });
  const analysis = useQuery({
    ...analysisQuery(user?.id ?? "unknown", project, id),
    enabled: !!user && !!detail.data && !recording.isFetching,
  });
  const [seek, setSeek] = useState<{ time: number; request: number } | null>(
    null,
  );
  const seekTo = (time: number) =>
    setSeek((previous) => ({ time, request: (previous?.request ?? 0) + 1 }));
  const [currentTime, setCurrentTime] = useState(0);
  if (detail.isPending) return <Notice>Replay를 불러오는 중…</Notice>;
  if (detail.isError)
    return <Notice tone="error">{describeApiError(detail.error)}</Notice>;
  const { replay, associations } = detail.data;
  const m = replay.metadata;
  const currentUrl =
    analysis.data?.journey
      .filter((visit) => visit.started_at_ms <= currentTime)
      .at(-1)?.url ??
    analysis.data?.journey[0]?.url ??
    "확인되지 않음";
  return (
    <section className="page-stack">
      <header className="page-heading">
        <div>
          <Link to={`/replays?project=${project}`}>← Replays</Link>
          <h1>{userLabel(m.user)}</h1>
          <p>
            {new Date(m.started_at_ms).toLocaleString()} ·{" "}
            {duration(m.finished_at_ms - m.started_at_ms)} ·{" "}
            {m.environment ?? "—"} · {m.release ?? "—"}
          </p>
        </div>
        <Button
          onClick={() => {
            void detail.refetch();
            void analysis.refetch();
          }}
        >
          새로고침
        </Button>
      </header>
      {(replay.partial || Boolean(recording.data?.gaps.length)) && (
        <Notice tone="warning">
          Partial replay: 누락된 segment가 있습니다. 화면 상태와 체류시간이
          불완전할 수 있습니다.
        </Notice>
      )}
      {(recording.data?.truncated || analysis.data?.truncated) && (
        <Notice tone="warning">
          큰 Replay의 처리 한도에 도달했습니다. 아래 재생·분석은 읽은 구간만
          포함합니다.
        </Notice>
      )}
      <p className="replay-current-url">
        Current URL: <code>{currentUrl}</code>
      </p>
      {recording.isPending && (
        <Notice>Recording segments를 불러오는 중…</Notice>
      )}
      {recording.isError && (
        <Notice tone="error">{describeApiError(recording.error)}</Notice>
      )}
      {recording.data && (
        <Suspense fallback={<Notice>Player를 준비하는 중…</Notice>}>
          <ReplayPlayer
            key={`${project}/${id}`}
            events={recording.data.events}
            seekTo={seek}
            onTime={setCurrentTime}
          />
        </Suspense>
      )}
      <div className="replay-columns">
        <div>
          <h2>Timeline</h2>
          {analysis.isPending && <Notice>행동 데이터를 분석하는 중…</Notice>}
          {analysis.isError && (
            <Notice tone="error">{describeApiError(analysis.error)}</Notice>
          )}
          <ol className="replay-timeline">
            {analysis.data?.timeline.map((event, index) => (
              <li key={index}>
                <button onClick={() => seekTo(event.timestamp_ms)}>
                  <time>{duration(event.timestamp_ms - m.started_at_ms)}</time>
                  <strong>{event.kind}</strong>
                  <span>{event.label}</span>
                  {event.duration_ms !== null && (
                    <span>{event.duration_ms.toFixed(0)} ms</span>
                  )}
                </button>
              </li>
            ))}
          </ol>
        </div>
        <div>
          <h2>User journey</h2>
          <ol>
            {analysis.data?.journey.map((visit, index) => (
              <li key={index}>
                <button
                  className="link-button"
                  onClick={() => seekTo(visit.started_at_ms)}
                >
                  {visit.url}
                </button>{" "}
                ·{" "}
                {visit.duration_ms === null
                  ? "불명"
                  : duration(visit.duration_ms)}
              </li>
            ))}
          </ol>
          <h2>Errors & traces</h2>
          <p>
            {m.error_ids.length} associated errors · {m.trace_ids.length} trace
            IDs
          </p>
          <ul>
            {associations.errors.map((error) => (
              <li key={error.event_id}>
                <Link to={`/issues/${error.issue_id}`}>
                  Error {error.event_id}
                </Link>
              </li>
            ))}
          </ul>
          {m.error_ids.length > associations.errors.length && (
            <p className="muted">
              일부 연결된 Error는 아직 수신·인덱싱되지 않았습니다.
            </p>
          )}
          <ul>
            {m.trace_ids.map((trace) => (
              <li key={trace}>
                <Link
                  to={`/logs?project=${project}&query=${encodeURIComponent(`trace_id:${trace}`)}&start=${encodeURIComponent(new Date(m.started_at_ms - 1000).toISOString())}&end=${encodeURIComponent(new Date(m.finished_at_ms + 1000).toISOString())}`}
                >
                  Trace {trace}
                </Link>
              </li>
            ))}
          </ul>
          <p className="muted">
            연결은 SDK 식별자를 사용합니다. 현재 transaction/span 저장은
            미지원이며, Trace 링크는 수신된 관련 로그를 검색합니다.
          </p>
          <h2>User feedback</h2>
          {associations.feedback.length ? (
            associations.feedback.map((feedback, index) => (
              <blockquote key={index}>{feedbackMessage(feedback)}</blockquote>
            ))
          ) : (
            <p className="muted">
              이 Replay에 연결된 Sentry Feedback이 없습니다.
            </p>
          )}
        </div>
      </div>
      <h2>Page activity</h2>
      {analysis.data && (
        <PageMaps pages={analysis.data.pages} project={project} />
      )}
    </section>
  );
}
