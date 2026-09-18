import { queryOptions } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import type { IssueStatus } from "../../api/types";

export interface IssueListFilters {
  projectId: string;
  status: IssueStatus;
  query?: string;
  cursorLastSeenUs?: string;
  cursorId?: string;
}

export const issueKeys = {
  project: (userId: string, projectId: string) =>
    ["issues", userId, projectId] as const,
  list: (userId: string, filters: IssueListFilters) =>
    [
      ...issueKeys.project(userId, filters.projectId),
      "list",
      filters.status,
      filters.query ?? "",
      filters.cursorLastSeenUs ?? null,
      filters.cursorId ?? null,
    ] as const,
  detail: (userId: string, projectId: string, issueId: string) =>
    [...issueKeys.project(userId, projectId), "detail", issueId] as const,
  occurrences: (
    userId: string,
    projectId: string,
    issueId: string,
    cursorOccurredAtUs?: string,
    cursorIngestSeq?: string,
  ) =>
    [
      ...issueKeys.detail(userId, projectId, issueId),
      "occurrences",
      cursorOccurredAtUs ?? null,
      cursorIngestSeq ?? null,
    ] as const,
  occurrenceDetail: (
    userId: string,
    projectId: string,
    issueId: string,
    recordId: string,
  ) =>
    [
      ...issueKeys.detail(userId, projectId, issueId),
      "occurrence-detail",
      recordId,
    ] as const,
};

export function issuesQuery(userId: string, filters: IssueListFilters) {
  return queryOptions({
    queryKey: issueKeys.list(userId, filters),
    queryFn: ({ signal }) => endpoints.issues(filters, signal),
    staleTime: 10_000,
  });
}

export function issueQuery(userId: string, projectId: string, issueId: string) {
  return queryOptions({
    queryKey: issueKeys.detail(userId, projectId, issueId),
    queryFn: ({ signal }) => endpoints.issue(issueId, signal),
    retry: false,
    staleTime: 10_000,
  });
}

export function occurrencesQuery(
  userId: string,
  projectId: string,
  issueId: string,
  cursorOccurredAtUs?: string,
  cursorIngestSeq?: string,
) {
  return queryOptions({
    queryKey: issueKeys.occurrences(
      userId,
      projectId,
      issueId,
      cursorOccurredAtUs,
      cursorIngestSeq,
    ),
    queryFn: ({ signal }) =>
      endpoints.issueOccurrences(
        issueId,
        { cursorOccurredAtUs, cursorIngestSeq },
        signal,
      ),
    retry: false,
    staleTime: 10_000,
  });
}

export function occurrenceDetailQuery(
  userId: string,
  projectId: string,
  issueId: string,
  recordId: string,
) {
  return queryOptions({
    queryKey: issueKeys.occurrenceDetail(userId, projectId, issueId, recordId),
    queryFn: ({ signal }) =>
      endpoints.issueOccurrenceDetail(issueId, recordId, signal),
    retry: false,
  });
}
