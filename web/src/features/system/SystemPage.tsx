import { useCursorPage } from "../../shared/query/useCursorPage";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CursorPager } from "../../shared/ui/CursorPager";
import { useState } from "react";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function SystemPage() {
  const { session, tenant } = useSession();
  const queryClient = useQueryClient();
  const system = useQuery({ queryKey: ["system", session.user_id, tenant.tenant_id], queryFn: ({ signal }) => api.system(tenant.tenant_id, signal), enabled: tenant.role === "admin", refetchInterval: 15_000 });
  const [retentionDays, setRetentionDays] = useState<number>();
  const retention = useMutation({ mutationFn: () => api.updateRetention(tenant.tenant_id, system.data!.retention.revision, retentionDays ?? system.data!.retention.days, session.csrf_token), onSuccess: async () => { setRetentionDays(undefined); await queryClient.invalidateQueries({ queryKey: ["system"] }); } });
  const [{ startUS, endUS }] = useState(() => {
    const end = BigInt(Date.now()) * 1000n;
    return { endUS: end.toString(), startUS: (end - 86_400_000_000n).toString() };
  });
  const outcomes = useCursorPage(["sdk-outcomes", session.user_id, tenant.tenant_id, startUS, endUS], (cursor, signal) => api.sdkOutcomes(tenant.tenant_id, startUS, endUS, cursor, signal), tenant.role === "admin");
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Installation diagnostics</p><h1>System</h1></div><p>Accepted and published cuts, bounded resources, backup degradation and SDK-reported loss are shown separately.</p></div>
    {tenant.role !== "admin" ? <StatusPanel empty="Tenant administrator access is required." /> : null}
    {outcomes.isPending && tenant.role === "admin" ? <p role="status">Loading SDK outcomes…</p> : null}
    <StatusPanel error={system.error ?? outcomes.error ?? retention.error} onRetry={() => { void system.refetch(); void outcomes.refetch(); }} />
    {system.data ? <>
      <div className="facts"><div><dt>Recovery</dt><dd>{system.data.recovery_state}</dd></div><div><dt>Backup</dt><dd>{system.data.backup.state}</dd></div><div><dt>Generation</dt><dd>{formatInt64(system.data.generation)}</dd></div><div><dt>Alerts</dt><dd>{system.data.alerts_paused ? "paused" : "active"}</dd></div></div>
      <h2>Retention</h2><form className="inline-form" onSubmit={(event) => { event.preventDefault(); retention.mutate(); }}><label>Installation-wide days<input type="number" min={1} max={3650} value={retentionDays ?? system.data.retention.days} onChange={(event) => setRetentionDays(Number(event.target.value))} /></label><button disabled={!session.is_installation_admin || retention.isPending}>Update policy</button></form>
      {!session.is_installation_admin ? <p className="muted">Only the installation administrator can change retention.</p> : null}
      <p>Current {system.data.retention.days} days · floor {formatInt64(system.data.retention.floor_us)} · revision {formatInt64(system.data.retention.revision)}</p>
      <h2>Lane cuts</h2><div className="table-wrap"><table><thead><tr><th>Lane</th><th>Accepted</th><th>Published</th><th>Backlog/error</th></tr></thead><tbody>{system.data.lanes.map((lane) => <tr key={lane.lane_id}><td>{lane.lane_id}</td><td>{formatInt64(lane.accepted_seq)}</td><td>{formatInt64(lane.published_seq)}</td><td>{lane.error_code ?? lane.oldest_pending_received_us ?? "clear"}</td></tr>)}</tbody></table></div>
      <h2>Resources and dependencies</h2><div className="table-wrap"><table><thead><tr><th>Resource</th><th>Used</th><th>Maximum</th></tr></thead><tbody>{system.data.resources.map((item) => <tr key={item.name}><td>{item.name}</td><td>{formatInt64(item.used)} {item.unit}</td><td>{formatInt64(item.max)} {item.unit}</td></tr>)}</tbody></table></div>
      <ul>{system.data.dependencies.map((item) => <li key={item.name}>{item.name}: <strong>{item.status}</strong></li>)}</ul>
      <h2>Ingest</h2><p>Accepted requests {formatInt64(system.data.ingest.accepted_requests)} · accepted records {formatInt64(system.data.ingest.accepted_records)} · published {formatInt64(system.data.ingest.published_records)} · duplicate {formatInt64(system.data.ingest.duplicate_records)} · conflicts {formatInt64(system.data.ingest.conflict_records)}</p>
    </> : null}
    <h2>SDK outcomes · last 24 hours</h2><p>Client-reported drops are approximate diagnostics, never generated or accepted event totals.</p>
    {outcomes.data?.items.length === 0 ? <StatusPanel empty="No SDK outcomes were reported in this interval." /> : null}
    <CursorPager paging={outcomes.paging} label="SDK outcomes" />
    {outcomes.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>SDK</th><th>Category</th><th>Reason</th><th>Count</th><th>Estimate</th></tr></thead><tbody>{outcomes.data.items.map((item) => <tr key={`${item.sdk_name}:${item.category}:${item.reason}`}><td>{item.sdk_name}</td><td>{item.category}</td><td>{item.reason}</td><td>{formatInt64(item.count)}</td><td>{item.approximate ? "Approximate" : "Exact"}</td></tr>)}</tbody></table></div> : null}
  </section>;
}
