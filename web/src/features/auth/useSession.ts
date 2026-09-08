import { useQuery } from "@tanstack/react-query";
import { loadSession, sessionQueryKey } from "./api";

export function useSession() {
  return useQuery({
    queryKey: sessionQueryKey,
    queryFn: ({ signal }) => loadSession(signal),
    retry: false,
    staleTime: 30_000,
  });
}
