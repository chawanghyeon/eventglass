import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { ApiError } from "../api/client";
import { expireSession } from "../features/auth";

function handleError(error: Error): void {
  if (error instanceof ApiError && error.status === 401)
    expireSession(queryClient);
}

export const queryClient = new QueryClient({
  queryCache: new QueryCache({ onError: handleError }),
  mutationCache: new MutationCache({ onError: handleError }),
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: (count, error) =>
        count < 1 && (!(error instanceof ApiError) || error.retryable),
    },
    mutations: { retry: false },
  },
});
