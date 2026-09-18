import { useQuery } from "@tanstack/react-query";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";

export function SystemReadiness({
  admin,
  userId,
}: {
  admin: boolean;
  userId: string;
}) {
  const status = useQuery({
    queryKey: ["system-status", userId],
    queryFn: ({ signal }) => endpoints.systemStatus(signal),
    enabled: admin,
    retry: false,
    staleTime: 30_000,
  });

  if (!admin) {
    return (
      <div className="readiness-card">
        <span className="readiness-dot readiness-dot--pending" />
        <div>
          <strong>관리 기능 제한됨</strong>
          <span>시스템 상태는 관리자에게만 표시됩니다.</span>
        </div>
      </div>
    );
  }
  if (status.isPending) {
    return (
      <div className="readiness-card readiness-card--skeleton">
        시스템 상태 확인 중…
      </div>
    );
  }
  if (status.isError) {
    return (
      <div className="readiness-card">
        <span className="readiness-dot readiness-dot--error" />
        <div>
          <strong>상태 확인 실패</strong>
          <span>{describeApiError(status.error)}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="readiness-card">
      <span
        className={`readiness-dot ${status.data.ready ? "readiness-dot--ready" : "readiness-dot--pending"}`}
      />
      <div>
        <strong>{status.data.ready ? "수집 준비됨" : "Indexer 준비 중"}</strong>
        <span>
          {status.data.ready
            ? `Eventglass ${status.data.version}`
            : "프로젝트 관리는 가능하지만 수집·검색은 아직 사용할 수 없습니다."}
        </span>
      </div>
    </div>
  );
}
