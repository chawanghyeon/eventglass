import { useQuery } from "@tanstack/react-query";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function AlertsPage() {
  const { session, tenant } = useSession();
  const projects = useQuery({ queryKey: ["projects", session.user_id, tenant.tenant_id], queryFn: () => api.projects(tenant.tenant_id) });
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Delivery state</p><h1>Alerts</h1></div><p>Rules and attempts are durable. Eventglass never sends a test webhook from this screen.</p></div>
    {projects.isPending ? <p role="status">Loading alert projects…</p> : null}
    <StatusPanel error={projects.error} onRetry={() => void projects.refetch()} />
    <div className="stack">{projects.data?.items.filter((project) => project.state === "active").map((project) => <ProjectAlerts key={project.project_id} projectID={project.project_id} name={project.name} />)}</div>
  </section>;
}

function ProjectAlerts({ projectID, name }: { projectID: string; name: string }) {
  const { session, tenant } = useSession();
  const rules = useQuery({ queryKey: ["alerts", session.user_id, tenant.tenant_id, projectID], queryFn: () => api.alerts(tenant.tenant_id, projectID) });
  const deliveries = useQuery({ queryKey: ["deliveries", session.user_id, tenant.tenant_id, projectID], queryFn: () => api.deliveries(tenant.tenant_id, projectID) });
  return <article className="detail-card compact">
    <h2>{name}</h2>
    <StatusPanel error={rules.error ?? deliveries.error} onRetry={() => { void rules.refetch(); void deliveries.refetch(); }} />
    <h3>Rules</h3>
    {rules.data?.items.length ? <ul>{rules.data.items.map((rule) => <li key={rule.alert_id}><strong>{rule.name}</strong> · {rule.kind} · {rule.enabled ? "enabled" : "disabled"}{rule.last_completed_end_us ? ` · evaluated through ${formatInt64(rule.last_completed_end_us)}` : " · no completed window"}</li>)}</ul> : <p className="muted">No rules.</p>}
    <h3>Recent delivery attempts</h3>
    {deliveries.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>Delivery</th><th>State</th><th>Attempt</th><th>Result</th></tr></thead><tbody>{deliveries.data.items.map((delivery) => <tr key={delivery.delivery_id}><td><code>{delivery.delivery_id.slice(0, 12)}</code></td><td><span className={`badge ${delivery.state}`}>{delivery.state}</span></td><td>{delivery.attempt}</td><td>{delivery.error_code ?? delivery.last_http_status ?? "pending"}</td></tr>)}</tbody></table></div> : <p className="muted">No delivery attempts.</p>}
  </article>;
}
