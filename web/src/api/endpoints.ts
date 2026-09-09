import { apiRequest } from "./client";
import type {
  AggregateRequest,
  AggregateResponse,
  Alert,
  AlertDelivery,
  AlertInput,
  AlertUpdate,
  Credentials,
  DoctorReport,
  Issue,
  IssuePage,
  IssueStatus,
  IssueUpdate,
  LoginResponse,
  OccurrencePage,
  RecordDetail,
  RelatedRecords,
  SearchFilters,
  SearchPage,
  Project,
  ProjectInput,
  ProjectKey,
  ProjectList,
  Session,
  SetupRequest,
  SystemStatus,
  User,
  UserList,
  CreateUserInput,
  UpdateUserInput,
} from "./types";

export const endpoints = {
  setup: (input: SetupRequest, signal?: AbortSignal) =>
    apiRequest<undefined>("/api/setup", {
      method: "POST",
      body: input,
      csrf: false,
      signal,
    }),
  login: (input: Credentials, signal?: AbortSignal) =>
    apiRequest<LoginResponse>("/api/auth/login", {
      method: "POST",
      body: input,
      csrf: false,
      signal,
    }),
  session: (signal?: AbortSignal) =>
    apiRequest<Session>("/api/auth/me", { signal }),
  logout: (signal?: AbortSignal) =>
    apiRequest<undefined>("/api/auth/logout", { method: "POST", signal }),
  issues: (
    input: {
      projectId: string;
      status?: IssueStatus;
      cursorLastSeenUs?: string;
      cursorId?: string;
      limit?: number;
    },
    signal?: AbortSignal,
  ) => {
    const query = new URLSearchParams({ project_id: input.projectId });
    if (input.status) query.set("status", input.status);
    if (input.cursorLastSeenUs)
      query.set("cursor_last_seen_us", input.cursorLastSeenUs);
    if (input.cursorId) query.set("cursor_id", input.cursorId);
    if (input.limit) query.set("limit", input.limit.toString());
    return apiRequest<IssuePage>(`/api/issues?${query}`, { signal });
  },
  issue: (id: string, signal?: AbortSignal) =>
    apiRequest<Issue>(`/api/issues/${encodeURIComponent(id)}`, { signal }),
  updateIssue: (id: string, input: IssueUpdate, signal?: AbortSignal) =>
    apiRequest<Issue>(`/api/issues/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: input,
      signal,
    }),
  issueOccurrences: (
    id: string,
    input: {
      cursorOccurredAtUs?: string;
      cursorIngestSeq?: string;
      limit?: number;
    },
    signal?: AbortSignal,
  ) => {
    const query = new URLSearchParams();
    if (input.cursorOccurredAtUs)
      query.set("cursor_occurred_at_us", input.cursorOccurredAtUs);
    if (input.cursorIngestSeq)
      query.set("cursor_ingest_seq", input.cursorIngestSeq);
    if (input.limit) query.set("limit", input.limit.toString());
    const suffix = query.size > 0 ? `?${query}` : "";
    return apiRequest<OccurrencePage>(
      `/api/issues/${encodeURIComponent(id)}/events${suffix}`,
      { signal },
    );
  },
  issueOccurrenceDetail: (
    issueId: string,
    recordId: string,
    signal?: AbortSignal,
  ) =>
    apiRequest<RecordDetail>(
      `/api/issues/${encodeURIComponent(issueId)}/events/${encodeURIComponent(recordId)}`,
      { signal },
    ),
  users: (signal?: AbortSignal) =>
    apiRequest<UserList>("/api/users", { signal }).then(
      (response) => response.items,
    ),
  createUser: (input: CreateUserInput, signal?: AbortSignal) =>
    apiRequest<{ id: string }>("/api/users", {
      method: "POST",
      body: input,
      signal,
    }),
  updateUser: (
    input: { id: User["id"]; update: UpdateUserInput },
    signal?: AbortSignal,
  ) =>
    apiRequest<undefined>(`/api/users/${encodeURIComponent(input.id)}`, {
      method: "PATCH",
      body: input.update,
      signal,
    }),
  projects: (signal?: AbortSignal) =>
    apiRequest<ProjectList>("/api/projects", { signal }).then(
      (response) => response.items,
    ),
  createProject: (input: ProjectInput, signal?: AbortSignal) =>
    apiRequest<{ id: string }>("/api/projects", {
      method: "POST",
      body: input,
      signal,
    }),
  updateProject: (
    project: Pick<Project, "id" | "is_active">,
    signal?: AbortSignal,
  ) =>
    apiRequest<undefined>(`/api/projects/${encodeURIComponent(project.id)}`, {
      method: "PATCH",
      body: { is_active: project.is_active },
      signal,
    }),
  projectKeys: (projectId: string, signal?: AbortSignal) =>
    apiRequest<{ items: ProjectKey[] }>(
      `/api/projects/${encodeURIComponent(projectId)}/keys`,
      { signal },
    ).then((result) => result.items),
  createProjectKey: (projectId: string, signal?: AbortSignal) =>
    apiRequest<ProjectKey>(
      `/api/projects/${encodeURIComponent(projectId)}/keys`,
      {
        method: "POST",
        signal,
      },
    ),
  revokeProjectKey: (projectId: string, keyId: string, signal?: AbortSignal) =>
    apiRequest<undefined>(
      `/api/projects/${encodeURIComponent(projectId)}/keys/${encodeURIComponent(keyId)}`,
      { method: "DELETE", signal },
    ),
  systemStatus: (signal?: AbortSignal) =>
    apiRequest<SystemStatus>("/api/system/status", { signal }),
  systemDoctor: (signal?: AbortSignal) =>
    apiRequest<DoctorReport>("/api/system/doctor", { signal }),
  alerts: (signal?: AbortSignal) =>
    apiRequest<{ items: Alert[] }>("/api/alerts", { signal }).then(
      (response) => response.items,
    ),
  createAlert: (input: AlertInput, signal?: AbortSignal) =>
    apiRequest<{ id: string }>("/api/alerts", {
      method: "POST",
      body: input,
      signal,
    }),
  updateAlert: (id: string, input: AlertUpdate, signal?: AbortSignal) =>
    apiRequest<Alert>(`/api/alerts/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: input,
      signal,
    }),
  deleteAlert: (id: string, revision: number, signal?: AbortSignal) =>
    apiRequest<undefined>(
      `/api/alerts/${encodeURIComponent(id)}?revision=${revision}`,
      { method: "DELETE", signal },
    ),
  alertDeliveries: (signal?: AbortSignal) =>
    apiRequest<{ items: AlertDelivery[] }>("/api/alert-deliveries", {
      signal,
    }).then((response) => response.items),
  retryAlertDelivery: (id: string, signal?: AbortSignal) =>
    apiRequest<undefined>(
      `/api/alert-deliveries/${encodeURIComponent(id)}/retry`,
      { method: "POST", signal },
    ),
  relatedRecords: (
    detailToken: string,
    windowSeconds = 3600,
    signal?: AbortSignal,
  ) =>
    apiRequest<RelatedRecords>(
      `/api/records/${encodeURIComponent(detailToken)}/related?window_seconds=${windowSeconds}`,
      { signal },
    ),
  logs: (
    input: {
      projects: string[];
      start: string;
      end: string;
      query: string;
      filters: SearchFilters;
      limit?: number;
      cursor?: string;
      readToken?: string;
    },
    signal?: AbortSignal,
  ) => {
    const query = new URLSearchParams({ start: input.start, end: input.end });
    if (input.projects.length > 0)
      query.set("projects", input.projects.join(","));
    if (input.query) query.set("query", input.query);
    if (Object.values(input.filters).some((values) => values?.length))
      query.set("filters", JSON.stringify(input.filters));
    if (input.limit) query.set("limit", input.limit.toString());
    if (input.cursor) query.set("cursor", input.cursor);
    if (input.readToken) query.set("read_token", input.readToken);
    return apiRequest<SearchPage>(`/api/logs?${query}`, { signal });
  },
  aggregate: (input: AggregateRequest, signal?: AbortSignal) =>
    apiRequest<AggregateResponse>("/api/explore/aggregate", {
      method: "POST",
      body: input,
      signal,
    }),
  recordDetail: (detailToken: string, signal?: AbortSignal) =>
    apiRequest<RecordDetail>(
      `/api/records/${encodeURIComponent(detailToken)}`,
      { signal },
    ),
};

export function liveLogsUrl(input: {
  projects: string[];
  start: string;
  end: string;
  query: string;
  filters: SearchFilters;
}): string {
  const query = new URLSearchParams({ start: input.start, end: input.end });
  if (input.projects.length > 0)
    query.set("projects", input.projects.join(","));
  if (input.query) query.set("query", input.query);
  if (Object.values(input.filters).some((values) => values?.length))
    query.set("filters", JSON.stringify(input.filters));
  return `/api/logs/live?${query}`;
}
