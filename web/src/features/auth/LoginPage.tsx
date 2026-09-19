import { useMutation } from "@tanstack/react-query";
import { type FormEvent, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { api } from "../../api/client";
import { queryClient } from "../../app/providers";

export function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const navigate = useNavigate();
  const location = useLocation();
  const login = useMutation({
    mutationFn: () => api.login(email, password),
    onSuccess(session) {
      queryClient.setQueryData(["session"], session);
      const target = (location.state as { from?: string } | null)?.from ?? "/logs";
      navigate(target, { replace: true });
    },
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    login.mutate();
  };
  return <main className="center">
    <form className="card auth" onSubmit={submit}>
      <p className="eyebrow">Eventglass operations</p>
      <h1>Sign in</h1>
      <label>Email<input type="email" autoComplete="username" required value={email} onChange={(event) => setEmail(event.target.value)} /></label>
      <label>Password<input type="password" autoComplete="current-password" required minLength={12} value={password} onChange={(event) => setPassword(event.target.value)} /></label>
      {login.isError ? <p role="alert">Email or password was not accepted.</p> : null}
      <button disabled={login.isPending}>{login.isPending ? "Signing in…" : "Sign in"}</button>
    </form>
  </main>;
}
