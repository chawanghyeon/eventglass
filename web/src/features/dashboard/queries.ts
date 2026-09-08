import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import type { LogSearchState } from "../logs";

export const dashboardKeys = {
  rows: (userId: string, state: LogSearchState) =>
    ["dashboard", userId, "rows", state] as const,
  system: (userId: string) => ["dashboard", userId, "system"] as const,
};

export function dashboardRowsQuery(userId: string, state: LogSearchState) {
  return queryOptions({
    queryKey: dashboardKeys.rows(userId, state),
    queryFn: ({ signal }) => endpoints.logs({ ...state, limit: 8 }, signal),
  });
}

export function dashboardSystemQuery(userId: string) {
  return queryOptions({
    queryKey: dashboardKeys.system(userId),
    queryFn: ({ signal }) => endpoints.systemStatus(signal),
    staleTime: 30_000,
  });
}
