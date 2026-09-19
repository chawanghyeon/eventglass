import { useMutation, useQuery } from "@tanstack/react-query";
import { type FormEvent, useState } from "react";
import { Navigate, useNavigate } from "react-router-dom";

import { api } from "../../api/client";
import { queryClient } from "../../app/providers";

export function SetupPage() {
  const state = useQuery({ queryKey: ["setup"], queryFn: api.setupState, refetchInterval: (query) => query.state.data?.state === "in_progress" ? 1000 : false });
  const [bootstrapToken, setBootstrapToken] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [tenantName, setTenantName] = useState("");
  const navigate = useNavigate();
  const setup = useMutation({
    mutationFn: () => api.setup({ bootstrap_token: bootstrapToken, email, password, tenant_name: tenantName }),
    onSuccess(session) {
      queryClient.setQueryData(["session"], session);
      navigate("/projects", { replace: true });
    },
  });
  if (state.data?.state === "complete") return <Navigate to="/login" replace />;
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setup.mutate();
  };
  return <main className="center"><form className="card auth" onSubmit={submit}>
    <p className="eyebrow">One-time installation</p><h1>Initial setup</h1>
    {state.data?.state === "in_progress" ? <p role="status">Another setup attempt is being recovered. Waiting…</p> : null}
    <label>Bootstrap token<input type="password" autoComplete="off" required pattern="[0-9a-f]{64}" value={bootstrapToken} onChange={(event) => setBootstrapToken(event.target.value)} /></label>
    <label>Administrator email<input type="email" autoComplete="username" required value={email} onChange={(event) => setEmail(event.target.value)} /></label>
    <label>Password<input type="password" autoComplete="new-password" required minLength={12} value={password} onChange={(event) => setPassword(event.target.value)} /></label>
    <label>Tenant name<input required maxLength={128} value={tenantName} onChange={(event) => setTenantName(event.target.value)} /></label>
    {setup.isError || state.isError ? <p role="alert">Setup could not be completed. The token and installation state were not changed silently.</p> : null}
    <button disabled={setup.isPending || state.data?.state === "in_progress"}>{setup.isPending ? "Creating installation…" : "Create installation"}</button>
  </form></main>;
}
