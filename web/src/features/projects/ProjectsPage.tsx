import { useQuery } from "@tanstack/react-query";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { formatInt64 } from "../../shared/format/int64";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function ProjectsPage() {
  const { session, tenant } = useSession();
  const projects = useQuery({ queryKey: ["projects", session.user_id, tenant.tenant_id], queryFn: () => api.projects(tenant.tenant_id) });
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Scope</p><h1>Projects</h1></div><p>Keys and DSNs are revealed once when an administrator creates them.</p></div>
    {projects.isPending ? <p role="status">Loading projects…</p> : null}
    <StatusPanel error={projects.error} onRetry={() => void projects.refetch()} />
    {projects.data?.items.length === 0 ? <StatusPanel empty="No projects are assigned to this tenant." /> : null}
    {projects.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>Name</th><th>ID</th><th>State</th><th>Revision</th></tr></thead><tbody>
      {projects.data.items.map((project) => <tr key={project.project_id}><td>{project.name}</td><td>{formatInt64(project.project_id)}</td><td><span className={`badge ${project.state}`}>{project.state}</span></td><td>{formatInt64(project.revision)}</td></tr>)}
    </tbody></table></div> : null}
  </section>;
}
