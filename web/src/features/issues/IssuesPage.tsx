import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function IssuesPage() {
  const { session, tenant } = useSession();
  const projects = useQuery({ queryKey: ["projects", session.user_id, tenant.tenant_id], queryFn: () => api.projects(tenant.tenant_id), enabled: tenant.role === "admin" });
  const projectIDs = (tenant.role === "admin" ? projects.data?.items.filter((project) => project.state === "active").map((project) => project.project_id) ?? [] : tenant.project_grants.map((grant) => grant.project_id)).sort();
  const issues = useQuery({
    queryKey: ["issues", session.user_id, tenant.tenant_id, projectIDs],
    queryFn: () => api.issues(tenant.tenant_id, projectIDs),
    enabled: projectIDs.length > 0 && (tenant.role !== "admin" || projects.isSuccess),
  });
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Live catalog state</p><h1>Issues</h1></div><p>Counts are lifetime published occurrences; retained detail may expire.</p></div>
    {projects.isPending && tenant.role === "admin" ? <p role="status">Loading project scope…</p> : null}
    <StatusPanel error={projects.error} onRetry={() => void projects.refetch()} />
    {!projectIDs.length && !projects.isPending ? <StatusPanel empty="No active projects are available." /> : null}
    {issues.isPending && projectIDs.length ? <p role="status">Loading issues…</p> : null}
    <StatusPanel error={issues.error} onRetry={() => void issues.refetch()} />
    {issues.data?.items.length === 0 ? <StatusPanel empty="No issues match this scope." /> : null}
    <div className="stack">{issues.data?.items.map((issue) => <Link className="issue-card" key={issue.issue_id} to={`/issues/${issue.issue_id}?project=${issue.project_id}`}>
      <div><span className={`badge ${issue.status}`}>{issue.status}</span><h2>{issue.title}</h2></div>
      <dl><div><dt>Occurrences</dt><dd>{formatInt64(issue.lifetime_occurrence_count)}</dd></div><div><dt>Project</dt><dd>{formatInt64(issue.project_id)}</dd></div></dl>
    </Link>)}</div>
  </section>;
}
