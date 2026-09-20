import createClient from "openapi-fetch";

import type { paths } from "../generated/api";
import { ApiFailure } from "./errors";
import type { AggregateRequest, AggregateResult, ApiErrorBody, CreatedKey, DeliveryList, DestinationList, Issue, IssueList, KeyList, OccurrenceList, Project, ProjectCreate, ProjectList, ProjectPatch, QueryJob, RecordDetail, RuleList, SDKOutcomeList, SearchRequest, SearchResult, Session, SetupRequest, SetupState } from "./types";

const transport = createClient<paths>({ baseUrl: "/", credentials: "include" });

function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.data !== undefined) return result.data;
  const fallback: ApiErrorBody = {
    code: "unexpected_response",
    message: "The server could not complete this request.",
    retryable: result.response.status >= 500,
    request_id: "00000000-0000-4000-8000-000000000000",
  };
  throw new ApiFailure(result.response.status, (result.error as ApiErrorBody | undefined) ?? fallback);
}

export const api = {
  async setupState(): Promise<SetupState> {
    return unwrap(await transport.GET("/v1/setup"));
  },
  async setup(body: SetupRequest): Promise<Session> {
    return unwrap(await transport.POST("/v1/setup", { body }));
  },
  async session(): Promise<Session> {
    return unwrap(await transport.GET("/v1/session"));
  },
  async login(email: string, password: string): Promise<Session> {
    return unwrap(await transport.POST("/v1/sessions", { body: { email, password } }));
  },
  async logout(csrf: string): Promise<void> {
    const result = await transport.DELETE("/v1/session", { params: { header: { "X-CSRF-Token": csrf } } });
    if (result.response.status !== 204) unwrap(result);
  },
  async projects(tenantID: string): Promise<ProjectList> {
    return unwrap(await transport.GET("/v1/projects", { params: { query: { tenant_id: tenantID, limit: 100 } } }));
  },
  async createProject(body: ProjectCreate, csrf: string): Promise<Project> {
    return unwrap(await transport.POST("/v1/projects", { params: { header: { "X-CSRF-Token": csrf } }, body }));
  },
  async updateProject(projectID: string, body: ProjectPatch, csrf: string): Promise<Project> {
    return unwrap(await transport.PATCH("/v1/projects/{id}", { params: { path: { id: projectID }, header: { "X-CSRF-Token": csrf } }, body }));
  },
  async projectKeys(tenantID: string, projectID: string): Promise<KeyList> {
    return unwrap(await transport.GET("/v1/projects/{id}/keys", { params: { path: { id: projectID }, query: { tenant_id: tenantID } } }));
  },
  async createProjectKey(tenantID: string, projectID: string, label: string, csrf: string): Promise<CreatedKey> {
    return unwrap(await transport.POST("/v1/projects/{id}/keys", { params: { path: { id: projectID }, header: { "X-CSRF-Token": csrf } }, body: { tenant_id: tenantID, label } }));
  },
  async revokeProjectKey(tenantID: string, projectID: string, keyID: string, revision: string, csrf: string): Promise<void> {
    const result = await transport.DELETE("/v1/projects/{id}/keys/{key_id}", { params: { path: { id: projectID, key_id: keyID }, query: { tenant_id: tenantID, revision }, header: { "X-CSRF-Token": csrf } } });
    if (result.response.status !== 204) unwrap(result);
  },
  async issues(tenantID: string, projectIDs: string[]): Promise<IssueList> {
    return unwrap(await transport.GET("/v1/issues", { params: { query: { tenant_id: tenantID, project_ids: projectIDs, limit: 100 } } }));
  },
  async issue(tenantID: string, projectID: string, issueID: string): Promise<Issue> {
    return unwrap(await transport.GET("/v1/issues/{id}", { params: { path: { id: issueID }, query: { tenant_id: tenantID, project_id: projectID } } }));
  },
  async issueOccurrences(tenantID: string, projectID: string, issueID: string): Promise<OccurrenceList> {
    return unwrap(await transport.GET("/v1/issues/{id}/occurrences", { params: { path: { id: issueID }, query: { tenant_id: tenantID, project_id: projectID, limit: 100 } } }));
  },
  async updateIssue(tenantID: string, projectID: string, issueID: string, revision: string, status: "unresolved" | "resolved" | "ignored", csrf: string): Promise<Issue> {
    return unwrap(await transport.PATCH("/v1/issues/{id}", { params: { path: { id: issueID }, header: { "X-CSRF-Token": csrf } }, body: { tenant_id: tenantID, project_id: projectID, revision, status, operation_id: crypto.randomUUID() } }));
  },
  async sdkOutcomes(tenantID: string, startUS: string, endUS: string): Promise<SDKOutcomeList> {
    return unwrap(await transport.GET("/v1/system/sdk-outcomes", { params: { query: { tenant_id: tenantID, start_us: startUS, end_us: endUS, limit: 100 } } }));
  },
  async alerts(tenantID: string, projectID: string): Promise<RuleList> {
    return unwrap(await transport.GET("/v1/alerts", { params: { query: { tenant_id: tenantID, project_id: projectID, limit: 100 } } }));
  },
  async deliveries(tenantID: string, projectID: string): Promise<DeliveryList> {
    return unwrap(await transport.GET("/v1/deliveries", { params: { query: { tenant_id: tenantID, project_id: projectID, limit: 100 } } }));
  },
  async destinations(tenantID: string): Promise<DestinationList> {
    return unwrap(await transport.GET("/v1/destinations", { params: { query: { tenant_id: tenantID } } }));
  },
  async search(body: SearchRequest, signal?: AbortSignal): Promise<SearchResult | QueryJob> {
    return unwrap(await transport.POST("/v1/search", { body, signal }));
  },
  async aggregate(body: AggregateRequest, signal?: AbortSignal): Promise<AggregateResult | QueryJob> {
    return unwrap(await transport.POST("/v1/aggregate", { body, signal }));
  },
  async queryJob(tenantID: string, queryID: string, signal?: AbortSignal): Promise<QueryJob> {
    return unwrap(await transport.GET("/v1/query-jobs/{id}", { params: { path: { id: queryID }, query: { tenant_id: tenantID } }, signal }));
  },
  async cancelQuery(tenantID: string, queryID: string, csrf: string): Promise<void> {
    const result = await transport.DELETE("/v1/query-jobs/{id}", { params: { path: { id: queryID }, query: { tenant_id: tenantID }, header: { "X-CSRF-Token": csrf } } });
    if (result.response.status !== 204) unwrap(result);
  },
  async record(tenantID: string, projectID: string, recordID: string, readToken?: string): Promise<RecordDetail> {
    return unwrap(await transport.GET("/v1/records/{id}", { params: { path: { id: recordID }, query: { tenant_id: tenantID, project_id: projectID, read_token: readToken } } }));
  },
  async releaseSnapshot(tenantID: string, snapshotID: string, csrf: string): Promise<void> {
    const result = await transport.DELETE("/v1/snapshots/{id}", { params: { path: { id: snapshotID }, query: { tenant_id: tenantID }, header: { "X-CSRF-Token": csrf } } });
    if (result.response.status !== 204) unwrap(result);
  },
  async renewSnapshot(tenantID: string, snapshotID: string, readToken: string, csrf: string): Promise<string> {
    const result = unwrap(await transport.POST("/v1/snapshots/{id}/heartbeat", { params: { path: { id: snapshotID }, header: { "X-CSRF-Token": csrf } }, body: { tenant_id: tenantID, read_token: readToken } }));
    return result.read_token;
  },
};
