import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { Histogram } from "../../components/Histogram";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { aggregateQuery } from "../explore";
import { issuesQuery } from "../issues";
import { defaultBounds, type LogSearchState } from "../logs";
import { projectsQuery } from "../projects";
import { dashboardRowsQuery, dashboardSystemQuery } from "./queries";

export function DashboardPage() {
  const user = useSession().data;
  const [searchParams, setSearchParams] = useSearchParams();
  const [initialBounds] = useState(defaultBounds);
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: Boolean(user),
  });
  const activeProjects = useMemo(
    () => projects.data?.filter((project) => project.is_active) ?? [],
    [projects.data],
  );
  const requestedProject = searchParams.get("project");
  const project = activeProjects.find(
    (candidate) => candidate.id === requestedProject,
  );
  const start = searchParams.get("start") ?? "";
  const end = searchParams.get("end") ?? "";
  const validBounds =
    Number.isFinite(Date.parse(start)) &&
    Number.isFinite(Date.parse(end)) &&
    Date.parse(start) < Date.parse(end);
  const search: LogSearchState = {
    projects: project ? [project.id] : [],
    start,
    end,
    query: "",
    filters: {
      kinds: ["log", "error"],
      services: [],
      levels: [],
      environments: [],
      releases: [],
      loggers: [],
    },
  };
  const enabled = Boolean(user && project && validBounds);
  const rows = useQuery({
    ...dashboardRowsQuery(user?.id ?? "unknown", search),
    enabled,
    retry: false,
  });
  const histogram = useQuery({
    ...aggregateQuery(user?.id ?? "unknown", {
      projects: search.projects,
      start: search.start,
      end: search.end,
      filters: search.filters,
      read_token: rows.data?.read_token,
      metrics: [{ op: "count", name: "records" }],
      group_by: [],
      group_limit: 10,
      histogram: { field: "timestamp", interval: "auto" },
    }),
    enabled: enabled && Boolean(rows.data?.read_token),
    retry: false,
  });
  const issues = useQuery({
    ...issuesQuery(user?.id ?? "unknown", {
      projectId: project?.id ?? "unknown",
      status: "unresolved",
    }),
    enabled,
    retry: false,
  });
  const system = useQuery({
    ...dashboardSystemQuery(user?.id ?? "unknown"),
    enabled: Boolean(user?.role === "admin"),
    retry: false,
  });

  useEffect(() => {
    if (projects.isPending || activeProjects.length === 0) return;
    const next = new URLSearchParams(searchParams);
    let changed = false;
    if (!requestedProject) {
      next.set("project", activeProjects[0].id);
      changed = true;
    }
    if (!start && !end) {
      next.set("start", initialBounds.start);
      next.set("end", initialBounds.end);
      changed = true;
    }
    if (changed) setSearchParams(next, { replace: true });
  }, [
    activeProjects,
    end,
    initialBounds,
    projects.isPending,
    requestedProject,
    searchParams,
    setSearchParams,
    start,
  ]);

  function selectProject(id: string) {
    const next = new URLSearchParams(searchParams);
    next.set("project", id);
    setSearchParams(next);
  }

  const loading =
    projects.isPending || (enabled && (rows.isPending || issues.isPending));

  return (
    <div className="page-stack dashboard-page">
      <header className="page-heading">
        <div>
          <p className="eyebrow">개요</p>
          <h1>전체 현황</h1>
          <p>서비스에서 일어나는 오류와 수집 현황을 확인하세요.</p>
        </div>
        <label className="dashboard-project">
          프로젝트
          <select
            disabled={projects.isPending || activeProjects.length === 0}
            onChange={(event) => selectProject(event.target.value)}
            value={project?.id ?? ""}
          >
            {!project ? <option value="">프로젝트 선택</option> : null}
            {activeProjects.map((candidate) => (
              <option key={candidate.id} value={candidate.id}>
                {candidate.name}
              </option>
            ))}
          </select>
        </label>
      </header>

      {loading ? <Spinner label="Dashboard 불러오는 중" /> : null}
      {projects.isError ? (
        <Notice tone="error">{describeApiError(projects.error)}</Notice>
      ) : null}
      {projects.isSuccess && activeProjects.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state__mark" aria-hidden="true">
            00
          </span>
          <h2>활성 프로젝트가 없습니다.</h2>
          <p>웹사이트를 연결하면 오류와 방문 기록을 확인할 수 있습니다.</p>
          {user?.role === "admin" ? (
            <Link className="button button--primary" to="/projects">
              웹사이트 연결하기 →
            </Link>
          ) : (
            <p>관리자에게 프로젝트 연결을 요청해 주세요.</p>
          )}
        </div>
      ) : null}
      {requestedProject && !project && activeProjects.length > 0 ? (
        <Notice tone="warning">선택한 프로젝트가 없거나 중지되었습니다.</Notice>
      ) : null}
      {rows.isError ? (
        <Notice tone="error">{describeApiError(rows.error)}</Notice>
      ) : null}
      {issues.isError ? (
        <Notice tone="error">{describeApiError(issues.error)}</Notice>
      ) : null}

      {project && validBounds ? (
        <>
          <section className="dashboard-grid" aria-label="프로젝트 요약">
            <article className="dashboard-card">
              <span>최근 24시간 수집</span>
              <strong>{histogram.data?.record_count ?? "—"}</strong>
              <small>record</small>
            </article>
            <article className="dashboard-card">
              <span>미해결 오류</span>
              <strong>{issues.data?.items.length ?? "—"}</strong>
              <small>
                {issues.data?.next_cursor ? "다음 결과 있음" : "현재 목록"}
              </small>
            </article>
            <article className="dashboard-card">
              <span>저장소 상태</span>
              <strong>
                {user?.role !== "admin"
                  ? "제한됨"
                  : system.data?.ready
                    ? "정상"
                    : system.isError
                      ? "오류"
                      : "확인 중"}
              </strong>
              <small>
                {user?.role === "admin"
                  ? `Eventglass ${system.data?.version ?? ""}`
                  : "관리자 전용"}
              </small>
            </article>
          </section>

          <section
            className="snapshot-panel"
            aria-labelledby="dashboard-flow-heading"
          >
            <header>
              <div>
                <p className="eyebrow">최근 흐름</p>
                <h2 id="dashboard-flow-heading">오류·로그 추이</h2>
              </div>
              {rows.data ? <span>{rows.data.watermark} W</span> : null}
            </header>
            {histogram.isPending && rows.data ? (
              <Spinner label="수집량 집계 중" />
            ) : null}
            {histogram.isError ? (
              <Notice tone="error">{describeApiError(histogram.error)}</Notice>
            ) : null}
            {histogram.data?.buckets ? (
              <Histogram
                buckets={histogram.data.buckets}
                label="최근 Error와 Log 시간별 건수"
              />
            ) : null}
          </section>

          <section
            className="snapshot-panel"
            aria-labelledby="recent-records-heading"
          >
            <header>
              <div>
                <p className="eyebrow">같은 스냅샷</p>
                <h2 id="recent-records-heading">최근 기록</h2>
              </div>
              <Link to={`/logs?${searchParams}`}>로그 검색에서 보기</Link>
            </header>
            {rows.data?.rows.length === 0 ? (
              <p className="histogram__empty">최근 record가 없습니다.</p>
            ) : null}
            {rows.data && rows.data.rows.length > 0 ? (
              <ol className="recent-records">
                {rows.data.rows.map((row) => (
                  <li key={row.record_id}>
                    <span className={`log-kind log-kind--${row.kind}`}>
                      {row.kind}
                    </span>
                    <strong>{row.message || "(빈 메시지)"}</strong>
                    <time>
                      {new Date(row.timestamp).toLocaleString("ko-KR")}
                    </time>
                  </li>
                ))}
              </ol>
            ) : null}
          </section>
        </>
      ) : null}
    </div>
  );
}
