import { useCursorPage } from "../../shared/query/useCursorPage";
import { CursorPager } from "../../shared/ui/CursorPager";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { api } from "../../api/client";
import type { Issue } from "../../api/types";
import { isApiFailure } from "../../api/errors";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function IssueDetailPage() {
  const { id = "" } = useParams();
  const [params] = useSearchParams();
  const projectID = params.get("project") ?? "";
  const { session, tenant } = useSession();
  const client = useQueryClient();
  const key = ["issue", session.user_id, tenant.tenant_id, projectID, id];
  const valid = /^[0-9a-f]{64}$/.test(id) && /^[1-9][0-9]*$/.test(projectID);
  const issue = useQuery({ queryKey: key, queryFn: () => api.issue(tenant.tenant_id, projectID, id), enabled: valid });
  const occurrences = useCursorPage(["issue-occurrences", ...key], (cursor, signal) => api.issueOccurrences(tenant.tenant_id, projectID, id, cursor, signal), valid);
  const operator = tenant.role === "admin" || tenant.project_grants.some((grant) => grant.project_id === projectID && grant.role === "operator");
  const update = useMutation({
    mutationFn: (status: Issue["status"]) => api.updateIssue(tenant.tenant_id, projectID, id, issue.data!.revision, status, session.csrf_token),
    onSuccess: (value) => client.setQueryData(key, value),
    onError: async (error) => { if (isApiFailure(error) && error.kind === "conflict") await issue.refetch(); },
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
      {operator ? <div className="button-row" aria-label="Issue status actions">
        <button type="button" disabled={update.isPending || issue.data.status === "unresolved"} onClick={() => update.mutate("unresolved")}>Reopen</button>
        <button type="button" disabled={update.isPending || issue.data.status === "resolved"} onClick={() => update.mutate("resolved")}>Resolve</button>
        <button className="secondary" type="button" disabled={update.isPending || issue.data.status === "ignored"} onClick={() => update.mutate("ignored")}>Ignore</button>
      </div> : null}
      {update.error ? <StatusPanel error={update.error} onRetry={() => void issue.refetch()} /> : null}
      <p className="muted">Occurrence detail older than retention can be unavailable even though this lifetime count remains. Revision conflicts refresh this page and are never retried blindly.</p>
    </article> : null}
    <h2>Retained occurrences</h2>
    {occurrences.isPending ? <p role="status">Loading occurrences…</p> : null}
    <StatusPanel error={occurrences.error} onRetry={() => void occurrences.refetch()} />
    {occurrences.data?.items.length === 0 ? <StatusPanel empty="No retained occurrence metadata is available." /> : null}
    <CursorPager paging={occurrences.paging} label="Occurrences" />
    <div className="stack">{occurrences.data?.items.map((occurrence) => occurrence.detail_available
      ? <Link className="record-card" key={occurrence.record_id} to={`/logs/${occurrence.record_id}?project=${occurrence.project_id}`}><strong>{occurrence.record_id.slice(0, 16)}</strong><footer><span>Event {formatInt64(occurrence.event_us)}.{occurrence.ns.toString().padStart(3, "0")}</span><span>{occurrence.release || "No release"}</span></footer></Link>
      : <article className="record-card muted" key={occurrence.record_id}><strong>{occurrence.record_id.slice(0, 16)}</strong><p>Detail expired by retention; lifetime Issue state is preserved.</p></article>)}</div>
  </section>;
}
