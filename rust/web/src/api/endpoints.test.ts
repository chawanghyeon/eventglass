import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { setCsrfToken } from "./client";
import { endpoints, liveLogsUrl } from "./endpoints";
import type { AlertInput } from "./types";

const signal = new AbortController().signal;
const special = "project /?&한글";
const encoded = encodeURIComponent(special);
const response = { items: [{ id: "9007199254740993" }], marker: "response" };
const fetchMock = vi.fn();
beforeEach(() => {
  setCsrfToken("csrf");
  fetchMock
    .mockReset()
    .mockImplementation(() => Promise.resolve(Response.json(response)));
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => {
  setCsrfToken(undefined);
  vi.unstubAllGlobals();
});

// Use the real transport to check cookie/CSRF/cancellation and URL boundaries together.
it.each([
  [
    "feedback",
    () => endpoints.feedback(special, signal),
    `/api/feedback?project_id=${encoded}`,
    false,
  ],
  [
    "replay pages",
    () => endpoints.replayMaps("project_id=1&cursor=a%2Fb", signal),
    "/api/replay-pages?project_id=1&cursor=a%2Fb",
    false,
  ],
  [
    "replays",
    () => endpoints.replays("project_id=1", signal),
    "/api/replays?project_id=1",
    false,
  ],
  [
    "replay",
    () => endpoints.replay(special, special, signal),
    `/api/replays/${encoded}/${encoded}`,
    false,
  ],
  [
    "analysis",
    () => endpoints.replayAnalysis(special, special, signal),
    `/api/replays/${encoded}/${encoded}/analysis`,
    false,
  ],
  [
    "recording",
    () => endpoints.replayRecording(special, special, 7, signal),
    `/api/replays/${encoded}/${encoded}/segments/7`,
    false,
  ],
  ["session", () => endpoints.session(signal), "/api/auth/me", false],
  [
    "issue",
    () => endpoints.issue(special, signal),
    `/api/issues/${encoded}`,
    false,
  ],
  [
    "occurrence detail",
    () => endpoints.issueOccurrenceDetail(special, special, signal),
    `/api/issues/${encoded}/events/${encoded}`,
    false,
  ],
  ["users", () => endpoints.users(signal), "/api/users", true],
  ["projects", () => endpoints.projects(signal), "/api/projects", true],
  [
    "keys",
    () => endpoints.projectKeys(special, signal),
    `/api/projects/${encoded}/keys`,
    true,
  ],
  [
    "efficiency",
    () => endpoints.systemEfficiency(signal),
    "/api/system/efficiency",
    false,
  ],
  ["status", () => endpoints.systemStatus(signal), "/api/system/status", false],
  ["doctor", () => endpoints.systemDoctor(signal), "/api/system/doctor", false],
  ["alerts", () => endpoints.alerts(signal), "/api/alerts", true],
  [
    "deliveries",
    () => endpoints.alertDeliveries(signal),
    "/api/alert-deliveries",
    true,
  ],
  [
    "record",
    () => endpoints.recordDetail(special, signal),
    `/api/records/${encoded}`,
    false,
  ],
] as const)(
  "reads %s without leaking tokens into URLs",
  async (_, call, path, unwrap) => {
    expect(await call()).toEqual(unwrap ? response.items : response);
    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(path);
    expect(init.signal).toBe(signal);
    expect(init.credentials).toBe("include");
    expect(init.method).toBe("GET");
    expect(new Headers(init.headers).get("x-csrf-token")).toBeNull();
  },
);

const credentials = {
  email: "operator@example.test",
  password: "password-value", // pragma: allowlist secret (isolated test fixture)
};
const project = { slug: "checkout", name: "Checkout" };
const issueUpdate = {
  status: "resolved",
  expected_revision: "9007199254740993",
} as const;
const alert: AlertInput = {
  name: "Errors",
  project_id: "1",
  enabled: true,
  condition: {
    type: "error_count",
    query: "",
    threshold: 1,
    window_seconds: 60,
    cooldown_seconds: 60,
    time_basis: "received_at",
  },
  destination: { type: "webhook", url: "https://example.test/webhook" },
};
it.each([
  [
    "setup",
    () => endpoints.setup({ ...credentials, token: "setup" }, signal),
    "/api/setup",
    "POST",
    { ...credentials, token: "setup" },
    false,
  ],
  [
    "login",
    () => endpoints.login(credentials, signal),
    "/api/auth/login",
    "POST",
    credentials,
    false,
  ],
  [
    "logout",
    () => endpoints.logout(signal),
    "/api/auth/logout",
    "POST",
    undefined,
    true,
  ],
  [
    "update issue",
    () => endpoints.updateIssue(special, issueUpdate, signal),
    `/api/issues/${encoded}`,
    "PATCH",
    issueUpdate,
    true,
  ],
  [
    "create user",
    () => endpoints.createUser({ ...credentials, role: "member" }, signal),
    "/api/users",
    "POST",
    { ...credentials, role: "member" },
    true,
  ],
  [
    "update user",
    () =>
      endpoints.updateUser(
        { id: special, update: { is_active: false } },
        signal,
      ),
    `/api/users/${encoded}`,
    "PATCH",
    { is_active: false },
    true,
  ],
  [
    "create project",
    () => endpoints.createProject(project, signal),
    "/api/projects",
    "POST",
    project,
    true,
  ],
  [
    "update project",
    () => endpoints.updateProject({ id: special, is_active: false }, signal),
    `/api/projects/${encoded}`,
    "PATCH",
    { is_active: false },
    true,
  ],
  [
    "create key",
    () => endpoints.createProjectKey(special, signal),
    `/api/projects/${encoded}/keys`,
    "POST",
    undefined,
    true,
  ],
  [
    "revoke key",
    () => endpoints.revokeProjectKey(special, special, signal),
    `/api/projects/${encoded}/keys/${encoded}`,
    "DELETE",
    undefined,
    true,
  ],
  [
    "create alert",
    () => endpoints.createAlert(alert, signal),
    "/api/alerts",
    "POST",
    alert,
    true,
  ],
  [
    "update alert",
    () => endpoints.updateAlert(special, { ...alert, revision: 3 }, signal),
    `/api/alerts/${encoded}`,
    "PATCH",
    { ...alert, revision: 3 },
    true,
  ],
  [
    "delete alert",
    () => endpoints.deleteAlert(special, 3, signal),
    `/api/alerts/${encoded}?revision=3`,
    "DELETE",
    undefined,
    true,
  ],
  [
    "retry delivery",
    () => endpoints.retryAlertDelivery(special, signal),
    `/api/alert-deliveries/${encoded}/retry`,
    "POST",
    undefined,
    true,
  ],
  [
    "aggregate",
    () =>
      endpoints.aggregate(
        {
          start: "2026-09-01T00:00:00Z",
          end: "2026-09-02T00:00:00Z",
          group_limit: 10,
        },
        signal,
      ),
    "/api/explore/aggregate",
    "POST",
    {
      start: "2026-09-01T00:00:00Z",
      end: "2026-09-02T00:00:00Z",
      group_limit: 10,
    },
    true,
  ],
] as const)(
  "sends %s with the exact mutation body and authorization",
  async (_, call, path, method, body, csrf) => {
    if (!csrf) setCsrfToken(undefined);
    await call();
    expect(fetchMock).toHaveBeenCalledOnce();
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(path);
    expect(init.method).toBe(method);
    expect(init.signal).toBe(signal);
    expect(init.credentials).toBe("include");
    expect(init.body).toBe(
      body === undefined ? undefined : JSON.stringify(body),
    );
    expect(new Headers(init.headers).get("x-csrf-token")).toBe(
      csrf ? "csrf" : null,
    );
  },
);

function lastUrl(): URL {
  return new URL(
    fetchMock.mock.lastCall?.[0] as string,
    "https://example.test",
  );
}
it("preserves decimal cursors and query syntax without introducing parameters", async () => {
  await endpoints.issues({ projectId: special }, signal);
  expect([...lastUrl().searchParams]).toEqual([["project_id", special]]);
  await endpoints.issues(
    {
      projectId: special,
      status: "resolved",
      query: 'message:"a&b"',
      cursorLastSeenUs: "9007199254740993",
      cursorId: special,
      limit: 25,
    },
    signal,
  );
  expect(Object.fromEntries(lastUrl().searchParams)).toEqual({
    project_id: special,
    status: "resolved",
    query: 'message:"a&b"',
    cursor_last_seen_us: "9007199254740993",
    cursor_id: special,
    limit: "25",
  });
  await endpoints.issueOccurrences(special, {}, signal);
  expect(lastUrl().pathname).toBe(`/api/issues/${encoded}/events`);
  expect(lastUrl().search).toBe("");
  await endpoints.issueOccurrences(
    special,
    {
      cursorOccurredAtUs: "-9007199254740993",
      cursorIngestSeq: "9007199254740993",
      limit: 40,
    },
    signal,
  );
  expect(Object.fromEntries(lastUrl().searchParams)).toEqual({
    cursor_occurred_at_us: "-9007199254740993",
    cursor_ingest_seq: "9007199254740993",
    limit: "40",
  });
  await endpoints.relatedRecords(special);
  expect(lastUrl().searchParams.get("window_seconds")).toBe("3600");
  await endpoints.relatedRecords(special, 120, signal);
  expect(lastUrl().pathname).toBe(`/api/records/${encoded}/related`);
  expect(lastUrl().searchParams.get("window_seconds")).toBe("120");
});

it("uses identical selection filters for snapshot and live search", async () => {
  const base = {
    start: "2026-09-01T00:00:00Z",
    end: "2026-09-02T00:00:00Z",
    projects: [],
    query: "",
    filters: {},
  };
  for (const filters of [{}, { levels: [] }, { levels: undefined }]) {
    const input = { ...base, filters };
    await endpoints.logs(input, signal);
    expect(Object.fromEntries(lastUrl().searchParams)).toEqual({
      start: base.start,
      end: base.end,
    });
    expect(new URL(liveLogsUrl(input), "https://example.test").search).toBe(
      lastUrl().search,
    );
  }
  const input = {
    ...base,
    projects: ["1", "9007199254740993"],
    query: 'message:"a&b"',
    filters: { levels: ["error"], environments: ["a&b"] },
  };
  await endpoints.logs(
    { ...input, limit: 20, cursor: "a/b+==", readToken: "snapshot&value" },
    signal,
  );
  const params = lastUrl().searchParams;
  expect(params.get("cursor")).toBe("a/b+==");
  expect(params.get("read_token")).toBe("snapshot&value");
  expect(params.get("limit")).toBe("20");
  expect(params.get("projects")).toBe("1,9007199254740993");
  expect(JSON.parse(params.get("filters") ?? "null")).toEqual(input.filters);
  for (const key of ["cursor", "read_token", "limit"]) params.delete(key);
  expect(
    new URL(liveLogsUrl(input), "https://example.test").searchParams.toString(),
  ).toBe(params.toString());
});
