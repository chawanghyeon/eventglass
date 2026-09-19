import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createContext, type PropsWithChildren, useContext, useMemo, useState } from "react";

import { isApiFailure } from "../api/errors";
import type { Session, SessionTenant } from "../api/types";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: (count, error) => isApiFailure(error) ? error.body.retryable && count < 2 : count < 1,
      refetchOnWindowFocus: false,
    },
  },
});

interface SessionContextValue {
  session: Session;
  tenant: SessionTenant;
  setTenant(tenantID: string): void;
}

const SessionContext = createContext<SessionContextValue | null>(null);

export function Providers({ children }: PropsWithChildren) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}

export function SessionProvider({ session, children }: PropsWithChildren<{ session: Session }>) {
  const [tenantID, setTenantID] = useState(session.tenants[0]?.tenant_id ?? "");
  const tenant = session.tenants.find((candidate) => candidate.tenant_id === tenantID) ?? session.tenants[0];
  if (!tenant) throw new Error("Session has no tenant membership");
  const value = useMemo<SessionContextValue>(() => ({
    session,
    tenant,
    setTenant(next) {
      if (next === tenant.tenant_id) return;
      void queryClient.cancelQueries();
      queryClient.removeQueries({ predicate: (query) => query.queryKey[0] !== "session" });
      setTenantID(next);
    },
  }), [session, tenant]);
  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

export function useSession(): SessionContextValue {
  const value = useContext(SessionContext);
  if (!value) throw new Error("SessionProvider is missing");
  return value;
}
