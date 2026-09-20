import { useQuery } from "@tanstack/react-query";
import { NavLink, Navigate, Outlet, useLocation, useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { isApiFailure } from "../api/errors";
import { SessionProvider, queryClient, useSession } from "./providers";

export function SessionBoundary() {
  const location = useLocation();
  const session = useQuery({ queryKey: ["session"], queryFn: api.session, retry: false });
  if (session.isPending) return <main className="center"><p role="status">Loading session…</p></main>;
  if (session.error && isApiFailure(session.error) && session.error.kind === "unauthenticated") {
    return <Navigate to="/login" state={{ from: location.pathname + location.search }} replace />;
  }
  if (session.error || !session.data) return <main className="center"><p role="alert">Session could not be loaded.</p></main>;
  return <SessionProvider session={session.data}><Outlet /></SessionProvider>;
}

export function AppShell() {
  const { session, tenant, setTenant } = useSession();
  const navigate = useNavigate();
  const logout = async () => {
    await api.logout(session.csrf_token);
    queryClient.clear();
    navigate("/login", { replace: true });
  };
  return <div className="app-shell">
    <header>
      <NavLink className="brand" to="/logs">Eventglass</NavLink>
      <label>Tenant
        <select value={tenant.tenant_id} onChange={(event) => setTenant(event.target.value)}>
          {session.tenants.map((item) => <option key={item.tenant_id} value={item.tenant_id}>{item.name}</option>)}
        </select>
      </label>
      <button type="button" onClick={() => void logout()}>Sign out</button>
    </header>
    <nav aria-label="Primary">
      <NavLink to="/logs">Logs</NavLink>
      <NavLink to="/issues">Issues</NavLink>
      <NavLink to="/explore">Explore</NavLink>
      <NavLink to="/projects">Projects</NavLink>
      <NavLink to="/alerts">Alerts</NavLink>
      {tenant.role === "admin" ? <NavLink to="/system">System</NavLink> : null}
    </nav>
    <main><Outlet /></main>
  </div>;
}
