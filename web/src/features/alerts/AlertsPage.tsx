import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useCursorPage } from "../../shared/query/useCursorPage";
import { CursorPager } from "../../shared/ui/CursorPager";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function AlertsPage() {
  const { session, tenant } = useSession();
  const projects = useCursorPage(["projects", session.user_id, tenant.tenant_id], (cursor, signal) => api.projects(tenant.tenant_id, cursor, signal));
  const [selected, setSelected] = useState("");
  const active = projects.data?.items.filter((project) => project.state === "active") ?? [];
  const project = active.find((project) => project.project_id === selected) ?? active[0];
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Delivery state</p><h1>Alerts</h1></div><p>Rules and attempts are durable. Eventglass never sends a test webhook from this screen.</p></div>
    {projects.isPending ? <p role="status">Loading alert projects…</p> : null}
    <StatusPanel error={projects.error} onRetry={() => void projects.refetch()} />
    {tenant.role === "admin" ? <DestinationEditor /> : null}
    <label>Project<select value={project?.project_id ?? ""} onChange={(event) => setSelected(event.target.value)}>{active.map((project) => <option key={project.project_id} value={project.project_id}>{project.name}</option>)}</select></label>
    <CursorPager paging={projects.paging} label="Projects" />
    {project ? <ProjectAlerts key={project.project_id} projectID={project.project_id} name={project.name} /> : null}
  </section>;
}

function DestinationEditor() {
  const { session, tenant } = useSession(); const queryClient = useQueryClient();
  const destinations = useQuery({ queryKey: ["destinations", session.user_id, tenant.tenant_id], queryFn: () => api.destinations(tenant.tenant_id) });
  const [name, setName] = useState(""); const [url, setURL] = useState(""); const [secret, setSecret] = useState("");
  const create = useMutation({ mutationFn: () => api.createDestination({ tenant_id: tenant.tenant_id, name, url, ...(secret ? { secret } : {}) }, session.csrf_token), onSuccess: async () => { setName(""); setURL(""); setSecret(""); await queryClient.invalidateQueries({ queryKey: ["destinations"] }); } });
  const toggle = useMutation({ mutationFn: (item: NonNullable<typeof destinations.data>["items"][number]) => api.updateDestination(item.destination_id, { tenant_id: tenant.tenant_id, revision: item.revision, enabled: !item.enabled }, session.csrf_token), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["destinations"] }) });
  return <article className="detail-card compact"><h2>Webhook destinations</h2><p className="muted">Secrets are write-only. No test webhook is sent from this editor.</p>
    <form className="editor-grid" onSubmit={(event) => { event.preventDefault(); create.mutate(); }}><label>Name<input value={name} onChange={(event) => setName(event.target.value)} /></label><label>HTTPS URL<input type="url" value={url} onChange={(event) => setURL(event.target.value)} /></label><label>Signing secret<input type="password" value={secret} onChange={(event) => setSecret(event.target.value)} /></label><button disabled={create.isPending || !name || !url.startsWith("https://")}>Add destination</button></form>
    <StatusPanel error={destinations.error ?? create.error ?? toggle.error} />
    {destinations.data?.items.length ? <ul>{destinations.data.items.map((item) => <li key={item.destination_id}><strong>{item.name}</strong> · {item.url} · {item.has_secret ? "signed" : "unsigned"} · {item.enabled ? "enabled" : "disabled"} <button className="secondary" onClick={() => toggle.mutate(item)}>{item.enabled ? "Disable" : "Enable"}</button></li>)}</ul> : <p className="muted">No destinations.</p>}
  </article>;
}

function ProjectAlerts({ projectID, name }: { projectID: string; name: string }) {
  const { session, tenant } = useSession();
  const queryClient = useQueryClient();
  const rules = useCursorPage(["alerts", session.user_id, tenant.tenant_id, projectID], (cursor, signal) => api.alerts(tenant.tenant_id, projectID, cursor, signal));
  const deliveries = useCursorPage(["deliveries", session.user_id, tenant.tenant_id, projectID], (cursor, signal) => api.deliveries(tenant.tenant_id, projectID, cursor, signal));
  const destinations = useQuery({ queryKey: ["destinations", session.user_id, tenant.tenant_id], queryFn: () => api.destinations(tenant.tenant_id) });
  const [ruleName, setRuleName] = useState(""); const [destinationID, setDestinationID] = useState("");
  const canOperate = tenant.role === "admin" || tenant.project_grants.some((grant) => grant.project_id === projectID && grant.role === "operator");
  const destination = destinationID || destinations.data?.items.find((item) => item.enabled)?.destination_id || "";
  const createRule = useMutation({ mutationFn: () => api.createAlert({ tenant_id: tenant.tenant_id, project_id: projectID, name: ruleName, kind: "issue", destination_id: destination, cooldown_seconds: 0, rule: { events: ["created", "regressed"] } }, session.csrf_token), onSuccess: async () => { setRuleName(""); await queryClient.invalidateQueries({ queryKey: ["alerts"] }); } });
  const toggleRule = useMutation({ mutationFn: (rule: NonNullable<typeof rules.data>["items"][number]) => api.updateAlert(rule.alert_id, { tenant_id: tenant.tenant_id, project_id: projectID, revision: rule.revision, enabled: !rule.enabled }, session.csrf_token), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["alerts"] }) });
  const retry = useMutation({ mutationFn: (delivery: NonNullable<typeof deliveries.data>["items"][number]) => api.retryDelivery(delivery.delivery_id, tenant.tenant_id, projectID, delivery.revision, session.csrf_token), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["deliveries"] }) });
  return <article className="detail-card compact">
    <h2>{name}</h2>
    <StatusPanel error={rules.error ?? deliveries.error ?? destinations.error ?? createRule.error ?? toggleRule.error ?? retry.error} onRetry={() => { void rules.refetch(); void deliveries.refetch(); void destinations.refetch(); }} />
    <h3>Rules</h3>
    {canOperate ? <form className="inline-form" onSubmit={(event) => { event.preventDefault(); createRule.mutate(); }}><label>New issue rule<input value={ruleName} onChange={(event) => setRuleName(event.target.value)} /></label><label>Destination<select value={destination} onChange={(event) => setDestinationID(event.target.value)}>{destinations.data?.items.filter((item) => item.enabled).map((item) => <option key={item.destination_id} value={item.destination_id}>{item.name}</option>)}</select></label><button disabled={!ruleName || !destination || createRule.isPending}>Create</button></form> : null}
    <CursorPager paging={rules.paging} label="Rules" />
    {rules.data?.items.length ? <ul>{rules.data.items.map((rule) => <li key={rule.alert_id}><strong>{rule.name}</strong> · {rule.kind} · {rule.enabled ? "enabled" : "disabled"}{rule.last_completed_end_us ? ` · evaluated through ${formatInt64(rule.last_completed_end_us)}` : " · no completed window"} {canOperate ? <button className="secondary" disabled={toggleRule.isPending} onClick={() => toggleRule.mutate(rule)}>{rule.enabled ? "Disable" : "Enable"}</button> : null}</li>)}</ul> : <p className="muted">No rules.</p>}
    <h3>Recent delivery attempts</h3>
    <CursorPager paging={deliveries.paging} label="Deliveries" />
    {deliveries.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>Delivery</th><th>State</th><th>Attempt</th><th>Result</th><th>Action</th></tr></thead><tbody>{deliveries.data.items.map((delivery) => <tr key={delivery.delivery_id}><td><code>{delivery.delivery_id.slice(0, 12)}</code></td><td><span className={`badge ${delivery.state}`}>{delivery.state}</span></td><td>{delivery.attempt}</td><td>{delivery.error_code ?? delivery.last_http_status ?? "pending"}</td><td>{delivery.state === "failed" && canOperate ? <button disabled={retry.isPending} onClick={() => retry.mutate(delivery)}>Retry</button> : "—"}</td></tr>)}</tbody></table></div> : <p className="muted">No delivery attempts.</p>}
  </article>;
}
