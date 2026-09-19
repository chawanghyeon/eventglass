import createClient from "openapi-fetch";

import type { paths } from "../generated/api";
import { ApiFailure } from "./errors";
import type { AggregateRequest, AggregateResult, ApiErrorBody, Issue, IssueList, ProjectList, QueryJob, RecordDetail, SearchRequest, SearchResult, Session, SetupRequest, SetupState } from "./types";

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
  async issues(tenantID: string, projectIDs: string[]): Promise<IssueList> {
    return unwrap(await transport.GET("/v1/issues", { params: { query: { tenant_id: tenantID, project_ids: projectIDs, limit: 100 } } }));
  },
  async issue(tenantID: string, projectID: string, issueID: string): Promise<Issue> {
    return unwrap(await transport.GET("/v1/issues/{id}", { params: { path: { id: issueID }, query: { tenant_id: tenantID, project_id: projectID } } }));
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
};
