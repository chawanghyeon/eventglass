import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type FormEvent } from "react";

import { api } from "../../api/client";
import type { CreatedKey, Project } from "../../api/types";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function ProjectsPage() {
  const { session, tenant } = useSession();
  const client = useQueryClient();
  const admin = tenant.role === "admin";
  const projects = useQuery({ queryKey: ["projects", session.user_id, tenant.tenant_id], queryFn: () => api.projects(tenant.tenant_id) });
  const [name, setName] = useState("");
  const [service, setService] = useState("");
  const [origins, setOrigins] = useState("");
  const create = useMutation({
    mutationFn: () => api.createProject({ tenant_id: tenant.tenant_id, name, default_service: service, allowed_origins: parseOrigins(origins) }, session.csrf_token),
    onSuccess: async () => {
      setName(""); setService(""); setOrigins("");
      await client.invalidateQueries({ queryKey: ["projects", session.user_id, tenant.tenant_id] });
    },
  });
  const submit = (event: FormEvent) => { event.preventDefault(); create.mutate(); };
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Scope</p><h1>Projects</h1></div><p>Keys and DSNs are revealed once when an administrator creates them.</p></div>
    {admin ? <form className="editor-grid" onSubmit={submit}>
      <label>Name<input required maxLength={128} value={name} onChange={(event) => setName(event.target.value)} /></label>
      <label>Default service<input value={service} onChange={(event) => setService(event.target.value)} /></label>
      <label>Allowed browser origins<input value={origins} onChange={(event) => setOrigins(event.target.value)} placeholder="https://app.example.com" /></label>
      <button disabled={create.isPending} type="submit">Create project</button>
      {create.error ? <StatusPanel error={create.error} /> : null}
    </form> : null}
    {projects.isPending ? <p role="status">Loading projects…</p> : null}
    <StatusPanel error={projects.error} onRetry={() => void projects.refetch()} />
    {projects.data?.items.length === 0 ? <StatusPanel empty="No projects are assigned to this tenant." /> : null}
    <div className="stack">{projects.data?.items.map((project) => <ProjectCard key={project.project_id} project={project} admin={admin} />)}</div>
  </section>;
}

function ProjectCard({ project, admin }: { project: Project; admin: boolean }) {
  const { session, tenant } = useSession();
  const client = useQueryClient();
  const [label, setLabel] = useState("");
  const [revealed, setRevealed] = useState<CreatedKey | null>(null);
  const keys = useQuery({ queryKey: ["project-keys", session.user_id, tenant.tenant_id, project.project_id], queryFn: () => api.projectKeys(tenant.tenant_id, project.project_id), enabled: admin });
  const toggle = useMutation({
    mutationFn: () => api.updateProject(project.project_id, { tenant_id: tenant.tenant_id, revision: project.revision, state: project.state === "active" ? "disabled" : "active" }, session.csrf_token),
    onSettled: async () => client.invalidateQueries({ queryKey: ["projects", session.user_id, tenant.tenant_id] }),
  });
  const createKey = useMutation({
    mutationFn: () => api.createProjectKey(tenant.tenant_id, project.project_id, label, session.csrf_token),
    onSuccess: async (value) => { setRevealed(value); setLabel(""); await keys.refetch(); },
  });
  const revoke = useMutation({
    mutationFn: ({ keyID, revision }: { keyID: string; revision: string }) => api.revokeProjectKey(tenant.tenant_id, project.project_id, keyID, revision, session.csrf_token),
    onSuccess: async () => { setRevealed(null); await keys.refetch(); },
  });
  return <article className="detail-card compact">
    <div className="row-between"><div><span className={`badge ${project.state}`}>{project.state}</span><h2>{project.name}</h2></div><dl><div><dt>ID</dt><dd>{formatInt64(project.project_id)}</dd></div><div><dt>Revision</dt><dd>{formatInt64(project.revision)}</dd></div></dl></div>
    <p className="muted">Default service: {project.default_service || "not set"} · Browser origins: {project.allowed_origins.join(", ") || "none"}</p>
    {admin ? <>
      <button className="secondary" type="button" disabled={toggle.isPending} onClick={() => toggle.mutate()}>{project.state === "active" ? "Disable" : "Enable"} project</button>
      <StatusPanel error={toggle.error} />
      <form className="inline-form" onSubmit={(event) => { event.preventDefault(); createKey.mutate(); }}>
        <label>New key label<input maxLength={128} value={label} onChange={(event) => setLabel(event.target.value)} /></label>
        <button type="submit" disabled={createKey.isPending}>Create ingest key</button>
      </form>
      <StatusPanel error={createKey.error ?? keys.error ?? revoke.error} onRetry={() => void keys.refetch()} />
      {revealed ? <OneTimeKey value={revealed} /> : null}
      {keys.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>Label</th><th>Prefix</th><th>State</th><th>Action</th></tr></thead><tbody>{keys.data.items.map((key) => <tr key={key.key_id}><td>{key.label}</td><td><code>{key.key_prefix}</code></td><td>{key.state}</td><td>{key.state === "active" ? <button className="danger" type="button" onClick={() => { if (window.confirm(`Revoke key ${key.key_prefix}? This cannot be undone.`)) revoke.mutate({ keyID: key.key_id, revision: key.revision }); }}>Revoke</button> : "Revoked"}</td></tr>)}</tbody></table></div> : null}
    </> : null}
  </article>;
}

function OneTimeKey({ value }: { value: CreatedKey }) {
  return <aside className="one-time" role="status">
    <strong>Copy now. This key and DSN will not be shown again.</strong>
    <pre className="json-text">{value.dsn}</pre>
    <button type="button" onClick={() => void navigator.clipboard?.writeText(value.dsn)}>Copy DSN</button>
    <h3>SDK snippets</h3>
    <pre className="json-text">{`SENTRY_DSN=${value.dsn}\n# Go: sentry.Init(sentry.ClientOptions{Dsn: os.Getenv("SENTRY_DSN")})\n# Python: sentry_sdk.init(dsn=os.environ["SENTRY_DSN"])\n// Browser: Sentry.init({ dsn: import.meta.env.VITE_SENTRY_DSN })`}</pre>
  </aside>;
}

function parseOrigins(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}
