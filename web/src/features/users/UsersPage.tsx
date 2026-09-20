import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../../api/client";
import type { User } from "../../api/types";
import { useSession } from "../../app/providers";
import { useCursorPage } from "../../shared/query/useCursorPage";
import { CursorPager } from "../../shared/ui/CursorPager";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function UsersPage() {
  const { session, tenant } = useSession();
  const queryClient = useQueryClient();
  const users = useCursorPage(["users", session.user_id, tenant.tenant_id], (cursor, signal) => api.users(tenant.tenant_id, cursor, signal), tenant.role === "admin");
  const [email, setEmail] = useState(""); const [password, setPassword] = useState(""); const [role, setRole] = useState<"admin" | "member">("member");
  const create = useMutation({ mutationFn: () => api.createUser({ tenant_id: tenant.tenant_id, email, initial_password: password, role, project_grants: [] }, session.csrf_token), onSuccess: async () => { setEmail(""); setPassword(""); await queryClient.invalidateQueries({ queryKey: ["users"] }); } });
  if (tenant.role !== "admin") return <StatusPanel empty="Tenant administrator access is required." />;
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Tenant authority</p><h1>Users</h1></div><p>Membership and project grants take effect immediately and invalidate cached authorization.</p></div>
    <form className="editor-grid" onSubmit={(event) => { event.preventDefault(); create.mutate(); }}>
      <label>Email<input type="email" value={email} onChange={(event) => setEmail(event.target.value)} /></label>
      <label>Initial password<input type="password" minLength={12} value={password} onChange={(event) => setPassword(event.target.value)} /></label>
      <label>Role<select value={role} onChange={(event) => setRole(event.target.value as "admin" | "member")}><option value="member">Member</option><option value="admin">Admin</option></select></label>
      <button disabled={create.isPending || !email || password.length < 12}>Create user</button>
    </form>
    <StatusPanel error={create.error ?? users.error} onRetry={() => void users.refetch()} />
    <CursorPager paging={users.paging} label="Users" />
    <div className="stack">{users.data?.items.map((user) => <UserEditor key={user.user_id} user={user} />)}</div>
  </section>;
}

function UserEditor({ user }: { user: User }) {
  const { session, tenant } = useSession(); const queryClient = useQueryClient();
  const [role, setRole] = useState<"admin" | "member" | "none">(user.role ?? "none");
  const [state, setState] = useState<"active" | "disabled">(user.state);
  const [grants, setGrants] = useState(user.project_grants.map((grant) => `${grant.project_id}:${grant.role}`).join(","));
  const [newPassword, setNewPassword] = useState("");
  const save = useMutation({ mutationFn: () => api.updateUser(user.user_id, { tenant_id: tenant.tenant_id, revision: user.revision, state, role: role === "none" ? null : role, project_grants: role === "member" ? parseGrants(grants) : [], ...(newPassword ? { new_password: newPassword } : {}) }, session.csrf_token), onSuccess: async () => { setNewPassword(""); await queryClient.invalidateQueries({ queryKey: ["users"] }); } });
  return <article className="detail-card compact"><div className="row-between"><div><strong>{user.email}</strong><p className="muted">Revision {user.revision}</p></div><span className={`badge ${user.state}`}>{user.state}</span></div>
    <form className="editor-grid" onSubmit={(event) => { event.preventDefault(); save.mutate(); }}>
      <label>Membership<select value={role} onChange={(event) => setRole(event.target.value as typeof role)}><option value="member">Member</option><option value="admin">Admin</option><option value="none">Remove</option></select></label>
      <label>State<select value={state} onChange={(event) => setState(event.target.value as typeof state)}><option value="active">Active</option><option value="disabled">Disabled</option></select></label>
      <label>Project grants<input value={grants} disabled={role !== "member"} placeholder="123:viewer,456:operator" onChange={(event) => setGrants(event.target.value)} /></label>
      <label>Reset password<input type="password" minLength={12} value={newPassword} onChange={(event) => setNewPassword(event.target.value)} /></label>
      <button disabled={save.isPending}>Save</button>
    </form><StatusPanel error={save.error} /></article>;
}

function parseGrants(value: string) { return value.split(",").map((entry) => entry.trim()).filter(Boolean).map((entry) => { const [project_id, rawRole] = entry.split(":"); return { project_id, role: rawRole as "operator" | "viewer" }; }); }
