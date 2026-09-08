import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import type { LogSearchState } from "./state";

export const logKeys = {
  search: (userId: string, state: LogSearchState) =>
    [
      "logs",
      userId,
      state.projects,
      state.start,
      state.end,
      state.query,
      state.filters,
      state.readToken ?? null,
      state.cursor ?? null,
    ] as const,
  detail: (
    userId: string,
    projectId: string,
    readToken: string,
    detailToken: string,
  ) => ["logs", userId, projectId, readToken, "detail", detailToken] as const,
};

export function logsQuery(userId: string, state: LogSearchState) {
  return queryOptions({
    queryKey: logKeys.search(userId, state),
    queryFn: ({ signal }) =>
      endpoints.logs(
        {
          projects: state.projects,
          start: state.start,
          end: state.end,
          query: state.query,
          filters: state.filters,
          cursor: state.cursor,
          readToken: state.readToken,
          limit: 100,
        },
        signal,
      ),
  });
}

export function recordDetailQuery(
  userId: string,
  projectId: string,
  readToken: string,
  detailToken: string,
) {
  return queryOptions({
    queryKey: logKeys.detail(userId, projectId, readToken, detailToken),
    queryFn: ({ signal }) => endpoints.recordDetail(detailToken, signal),
  });
}
