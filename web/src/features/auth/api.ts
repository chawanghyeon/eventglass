import type { QueryClient } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import { setCsrfToken } from "../../api/client";
import type { Session } from "../../api/types";

export const sessionQueryKey = ["session"] as const;

export async function loadSession(
  signal?: AbortSignal,
): Promise<Session | null> {
  const session = await endpoints.session(signal);
  setCsrfToken(session.csrf_token);
  return session;
}

export function expireSession(queryClient: QueryClient): void {
  setCsrfToken(undefined);
  queryClient.removeQueries({
    predicate: (query) => query.queryKey[0] !== "session",
  });
  queryClient.getMutationCache().clear();
  queryClient.setQueryData(sessionQueryKey, null);
}

export async function finishLogin(
  queryClient: QueryClient,
  csrfToken: string,
): Promise<void> {
  await queryClient.cancelQueries();
  queryClient.clear();
  setCsrfToken(csrfToken);
  await queryClient.fetchQuery({
    queryKey: sessionQueryKey,
    queryFn: ({ signal }) => loadSession(signal),
  });
}

export async function logoutAndClear(queryClient: QueryClient): Promise<void> {
  try {
    await endpoints.logout();
  } finally {
    setCsrfToken(undefined);
    queryClient.clear();
  }
}
