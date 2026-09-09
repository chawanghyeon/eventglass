import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { endpoints } from "../../api/endpoints";
import { describeApiError } from "../../api/client";
import { Notice } from "../../components/Notice";
import { feedbackMessage } from "./feedback";

function replayId(item: Record<string, unknown>): string | null {
  const contexts = item.contexts;
  if (!contexts || typeof contexts !== "object") return null;
  const feedback = (contexts as Record<string, unknown>).feedback;
  if (!feedback || typeof feedback !== "object") return null;
  const id = (feedback as Record<string, unknown>).replay_id;
  return typeof id === "string" && /^[a-f0-9]{32}$/i.test(id) ? id : null;
}
export function FeedbackPanel({
  user,
  project,
}: {
  user: string;
  project: string;
}) {
  const query = useQuery({
    queryKey: ["feedback", user, project],
    queryFn: ({ signal }) => endpoints.feedback(project, signal),
    staleTime: 15000,
  });
  return (
    <div>
      <h2>Sentry User Feedback</h2>
      <p>이 프로젝트의 최근 Feedback 최대 20개</p>
      {query.isPending && <Notice>Feedback을 불러오는 중…</Notice>}
      {query.isError && (
        <Notice tone="error">{describeApiError(query.error)}</Notice>
      )}
      {query.data?.items.map((item, i) => (
        <blockquote key={typeof item.event_id === "string" ? item.event_id : i}>
          <p>{feedbackMessage(item)}</p>
          {replayId(item) && (
            <Link to={`/replays/${project}/${replayId(item)}`}>
              관련 Replay
            </Link>
          )}
        </blockquote>
      ))}
      {query.data?.items.length === 0 && (
        <Notice>수신한 Sentry Feedback이 없습니다.</Notice>
      )}
    </div>
  );
}
