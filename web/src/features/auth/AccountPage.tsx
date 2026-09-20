import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function AccountPage() {
  const { session } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const change = useMutation({
    mutationFn: () => api.changePassword(currentPassword, newPassword, session.csrf_token),
    onSuccess: () => { queryClient.clear(); navigate("/login", { replace: true }); },
  });
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Credential security</p><h1>Account</h1></div><p>Changing your password revokes every active session, including this one.</p></div>
    <form className="card auth" onSubmit={(event) => { event.preventDefault(); change.mutate(); }}>
      <label>Current password<input type="password" autoComplete="current-password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} /></label>
      <label>New password<input type="password" autoComplete="new-password" minLength={12} value={newPassword} onChange={(event) => setNewPassword(event.target.value)} /></label>
      <button disabled={change.isPending || currentPassword.length === 0 || newPassword.length < 12}>Change password</button>
      <StatusPanel error={change.error} />
    </form>
  </section>;
}
