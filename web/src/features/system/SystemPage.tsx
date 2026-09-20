import { useCursorPage } from "../../shared/query/useCursorPage";
import { CursorPager } from "../../shared/ui/CursorPager";
import { useState } from "react";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function SystemPage() {
  const { session, tenant } = useSession();
  const [{ startUS, endUS }] = useState(() => {
    const end = BigInt(Date.now()) * 1000n;
    return { endUS: end.toString(), startUS: (end - 86_400_000_000n).toString() };
  });
  const outcomes = useCursorPage(["sdk-outcomes", session.user_id, tenant.tenant_id, startUS, endUS], (cursor, signal) => api.sdkOutcomes(tenant.tenant_id, startUS, endUS, cursor, signal), tenant.role === "admin");
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Last 24 hours</p><h1>SDK outcomes</h1></div><p>Client-reported drops are approximate diagnostics, never generated or accepted event totals.</p></div>
    {tenant.role !== "admin" ? <StatusPanel empty="Tenant administrator access is required." /> : null}
    {outcomes.isPending && tenant.role === "admin" ? <p role="status">Loading SDK outcomes…</p> : null}
    <StatusPanel error={outcomes.error} onRetry={() => void outcomes.refetch()} />
    {outcomes.data?.items.length === 0 ? <StatusPanel empty="No SDK outcomes were reported in this interval." /> : null}
    <CursorPager paging={outcomes.paging} label="SDK outcomes" />
    {outcomes.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>SDK</th><th>Category</th><th>Reason</th><th>Count</th><th>Estimate</th></tr></thead><tbody>{outcomes.data.items.map((item) => <tr key={`${item.sdk_name}:${item.category}:${item.reason}`}><td>{item.sdk_name}</td><td>{item.category}</td><td>{item.reason}</td><td>{formatInt64(item.count)}</td><td>{item.approximate ? "Approximate" : "Exact"}</td></tr>)}</tbody></table></div> : null}
  </section>;
}
