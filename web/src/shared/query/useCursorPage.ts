import { useQuery, type QueryKey } from "@tanstack/react-query";
import { useState } from "react";

// Keep only one page in the view; a new scope cannot inherit another scope's
// opaque cursor. Server data stays in Query, navigation state stays local.
export function useCursorPage<T extends { next_cursor: string | null }>(
  queryKey: QueryKey,
  load: (cursor: string | undefined, signal: AbortSignal) => Promise<T>,
  enabled = true,
) {
  const scope = JSON.stringify(queryKey);
  const [navigation, setNavigation] = useState<{ scope: string; cursor?: string }>({ scope });
  const cursor = navigation.scope === scope ? navigation.cursor : undefined;
  const result = useQuery({
    queryKey: [...queryKey, { cursor: cursor ?? null }],
    queryFn: ({ signal }) => load(cursor, signal),
    enabled,
    gcTime: 60_000,
  });
  return {
    ...result,
    paging: {
      canFirst: cursor !== undefined,
      canNext: Boolean(result.data?.next_cursor) && !result.isFetching,
      first: () => setNavigation({ scope }),
      next: () => { if (result.data?.next_cursor && !result.isFetching) setNavigation({ scope, cursor: result.data.next_cursor }); },
    },
  };
}
