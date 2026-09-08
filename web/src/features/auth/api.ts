import type { QueryClient } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import { setCsrfToken } from "../../api/client";

export const sessionQueryKey = ["session"] as const;

export async function loadSession(signal?: AbortSignal) {
  const session = await endpoints.session(signal);
  setCsrfToken(session.csrf_token);
  return session;
}

export async function logoutAndClear(queryClient: QueryClient): Promise<void> {
  try {
    await endpoints.logout();
  } finally {
    setCsrfToken(undefined);
    queryClient.clear();
  }
}
