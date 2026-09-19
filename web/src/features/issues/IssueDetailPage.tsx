import { useQuery } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function IssueDetailPage() {
  const { id = "" } = useParams();
  const [params] = useSearchParams();
  const projectID = params.get("project") ?? "";
  const { session, tenant } = useSession();
  const issue = useQuery({
    queryKey: ["issue", session.user_id, tenant.tenant_id, projectID, id],
    queryFn: () => api.issue(tenant.tenant_id, projectID, id),
    enabled: /^[0-9a-f]{64}$/.test(id) && /^[1-9][0-9]*$/.test(projectID),
  });
  return <section>
    <Link to="/issues">← Issues</Link>
    {issue.isPending ? <p role="status">Loading issue…</p> : null}
    <StatusPanel error={issue.error} onRetry={() => void issue.refetch()} />
    {issue.data ? <article className="detail-card">
      <p className="eyebrow">Issue {issue.data.issue_id.slice(0, 12)}</p>
      <h1>{issue.data.title}</h1>
      <p><span className={`badge ${issue.data.status}`}>{issue.data.status}</span></p>
      <dl className="facts"><div><dt>Lifetime occurrences</dt><dd>{formatInt64(issue.data.lifetime_occurrence_count)}</dd></div><div><dt>Revision</dt><dd>{formatInt64(issue.data.revision)}</dd></div><div><dt>Grouping version</dt><dd>{issue.data.grouping_version}</dd></div></dl>
      <p className="muted">Occurrence detail older than retention can be unavailable even though this lifetime count remains.</p>
    </article> : null}
  </section>;
}
