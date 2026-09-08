import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import type { AggregateRequest } from "../../api/types";

export const aggregateKeys = {
  snapshot: (userId: string, request: AggregateRequest) =>
    ["aggregate", userId, request] as const,
};

export function aggregateQuery(userId: string, request: AggregateRequest) {
  return queryOptions({
    queryKey: aggregateKeys.snapshot(userId, request),
    queryFn: ({ signal }) => endpoints.aggregate(request, signal),
  });
}
