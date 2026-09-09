import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import type { IssueStatus } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { formatDecimal, formatTimestampUs } from "../../lib/decimal";
import { useSession } from "../auth";
import { projectsQuery } from "../projects";
import { IssueStatusBadge } from "./IssueStatusBadge";
import { isIssueStatus, issueStatusLabels } from "./presentation";
import { issuesQuery } from "./queries";

export function IssuesPage() {
  const session = useSession();
  const user = session.data;
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: Boolean(user),
  });
  const [searchParams, setSearchParams] = useSearchParams();
  const activeProjects =
    projects.data?.filter((project) => project.is_active) ?? [];
  const requestedProject = searchParams.get("project");
  const project =
    activeProjects.find((candidate) => candidate.id === requestedProject) ??
    (requestedProject ? undefined : activeProjects[0]);
  const statusParam = searchParams.get("status");
  const status: IssueStatus = isIssueStatus(statusParam)
    ? statusParam
    : "unresolved";
  const cursorLastSeenUs = searchParams.get("cursor_last_seen_us") ?? undefined;
  const cursorId = searchParams.get("cursor_id") ?? undefined;
  const hasCursor = Boolean(cursorLastSeenUs && cursorId);
  const filters = {
    projectId: project?.id ?? "unknown",
    status,
    query: searchParams.get("query") ?? "",
    cursorLastSeenUs: hasCursor ? cursorLastSeenUs : undefined,
    cursorId: hasCursor ? cursorId : undefined,
  };
  const issues = useQuery({
    ...issuesQuery(user?.id ?? "unknown", filters),
    enabled: Boolean(user && project),
  });

  function setFilter(name: "project" | "status" | "query", value: string) {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(name, value);
    else next.delete(name);
    next.delete("cursor_last_seen_us");
    next.delete("cursor_id");
    setSearchParams(next);
  }

  function nextPage() {
    if (!issues.data?.next_cursor || !project) return;
    const next = new URLSearchParams(searchParams);
    next.set("project", project.id);
    next.set("status", status);
    next.set("cursor_last_seen_us", issues.data.next_cursor.last_seen_us);
    next.set("cursor_id", issues.data.next_cursor.id);
    setSearchParams(next);
  }

  function firstPage() {
    const next = new URLSearchParams(searchParams);
    next.delete("cursor_last_seen_us");
    next.delete("cursor_id");
    setSearchParams(next);
  }

  const detailSearch = new URLSearchParams(searchParams);
  if (project) detailSearch.set("project", project.id);
  detailSearch.set("status", status);

  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <h1>오류 추적</h1>
          <p>같은 원인의 오류를 묶어 발생 횟수와 상태를 확인합니다.</p>
        </div>
        {issues.data ? (
          <span className="count-badge">{issues.data.items.length}개 표시</span>
        ) : null}
      </header>

      <div className="issue-filters search-toolbar" aria-label="Issue 필터">
        <form
          className="table-search"
          key={filters.query}
          onSubmit={(event) => {
            event.preventDefault();
            setFilter(
              "query",
              String(
                new FormData(event.currentTarget).get("query") ?? "",
              ).trim(),
            );
          }}
        >
          <label>
            오류 검색
            <input
              name="query"
              maxLength={256}
              defaultValue={filters.query}
              placeholder="오류 제목 검색"
            />
          </label>
          <Button type="submit">검색</Button>
        </form>
        <label>
          프로젝트
          <select
            disabled={projects.isPending || activeProjects.length === 0}
            onChange={(event) => setFilter("project", event.target.value)}
            value={project?.id ?? ""}
          >
            {!project && activeProjects.length > 0 ? (
              <option disabled value="">
                프로젝트 선택
              </option>
            ) : null}
            {activeProjects.length === 0 ? (
              <option value="">활성 프로젝트 없음</option>
            ) : null}
            {activeProjects.map((candidate) => (
              <option key={candidate.id} value={candidate.id}>
                {candidate.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          상태
          <select
            onChange={(event) => setFilter("status", event.target.value)}
            value={status}
          >
            {(Object.keys(issueStatusLabels) as IssueStatus[]).map((value) => (
              <option key={value} value={value}>
                {issueStatusLabels[value]}
              </option>
            ))}
          </select>
        </label>
      </div>

      {filters.query && (
        <div className="filter-chips" aria-label="적용된 필터">
          <button
            type="button"
            onClick={() => setFilter("query", "")}
            aria-label="오류 검색 조건 삭제"
          >
            검색: {filters.query} ×
          </button>
        </div>
      )}
      {projects.isPending ? <Spinner label="프로젝트 불러오는 중" /> : null}
      {projects.isError ? (
        <Notice tone="error">
          <span>{describeApiError(projects.error)}</span>
          <Button
            onClick={() => void projects.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}
      {projects.isSuccess && activeProjects.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state__mark" aria-hidden="true">
            00
          </span>
          <h2>활성 프로젝트가 없습니다.</h2>
          <p>Issue를 조회하려면 활성 프로젝트가 필요합니다.</p>
        </div>
      ) : null}
      {requestedProject && !project && activeProjects.length > 0 ? (
        <Notice tone="warning">선택한 프로젝트가 없거나 중지되었습니다.</Notice>
      ) : null}

      {project && issues.isPending ? (
        <Spinner label="Issues 불러오는 중" />
      ) : null}
      {issues.isError ? (
        <Notice tone="error">
          <span>{describeApiError(issues.error)}</span>
          <Button
            onClick={() => void issues.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}
      {issues.data?.items.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state__mark" aria-hidden="true">
            00
          </span>
          <h2>
            {filters.query
              ? "검색 조건에 맞는 오류가 없습니다."
              : `${issueStatusLabels[status]} Issue가 없습니다.`}
          </h2>
          <p>다른 상태나 프로젝트를 선택해 보세요.</p>
        </div>
      ) : null}
      {issues.data && issues.data.items.length > 0 ? (
        <div className="issue-table-wrap">
          <table className="issue-table">
            <caption className="sr-only">{project?.name} Issue 목록</caption>
            <thead>
              <tr>
                <th scope="col">Issue</th>
                <th scope="col">상태</th>
                <th scope="col">발생</th>
                <th scope="col">처음 / 최근</th>
              </tr>
            </thead>
            <tbody>
              {issues.data.items.map((issue) => (
                <tr key={issue.id}>
                  <th scope="row">
                    <Link
                      className="issue-title-link"
                      to={`/issues/${encodeURIComponent(issue.id)}?${detailSearch}`}
                    >
                      {issue.title}
                    </Link>
                    <span className="issue-level">{issue.level}</span>
                  </th>
                  <td>
                    <IssueStatusBadge status={issue.status} />
                  </td>
                  <td>{formatDecimal(issue.occurrence_count)}회</td>
                  <td className="issue-time-pair">
                    <span>{formatTimestampUs(issue.first_seen_us)}</span>
                    <strong>{formatTimestampUs(issue.last_seen_us)}</strong>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      {issues.data ? (
        <nav className="page-controls" aria-label="Issue 페이지 이동">
          <Button
            disabled={!hasCursor}
            onClick={firstPage}
            type="button"
            variant="quiet"
          >
            처음으로
          </Button>
          <Button
            disabled={!issues.data.next_cursor}
            onClick={nextPage}
            type="button"
          >
            다음 페이지
          </Button>
        </nav>
      ) : null}
    </div>
  );
}
