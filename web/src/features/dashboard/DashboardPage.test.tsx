import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type {
  AggregateResponse,
  IssuePage,
  Project,
  SearchPage,
  Session,
  SystemStatus,
} from "../../api/types";
import { sessionQueryKey } from "../auth";
import { DashboardPage } from "./DashboardPage";

const session: Session = {
  id: "11",
  email: "admin@example.test",
  role: "admin",
  csrf_token: "csrf",
};
const projects: Project[] = [
  { id: "7", slug: "api", name: "API", is_active: true },
];
const searchPage: SearchPage = {
  rows: [
    {
      record_id: "a".repeat(64),
      kind: "error",
      project_id: "7",
      ingest_seq: "91",
      timestamp: "2026-09-08T12:00:00Z",
      received_at: "2026-09-08T12:00:01Z",
      service: "api",
      level: "error",
      message: "database timeout",
      environment: "production",
      release: null,
      logger: null,
      trace_id: null,
      span_id: null,
      request_id: null,
      issue_id: null,
      fingerprint: null,
      user_id: null,
      user_email: null,
      detail_token: "detail",
    },
  ],
  next_cursor: null,
  read_token: "dashboard-snapshot",
  watermark: "91",
  complete: true,
  took_ms: "2",
  searched_shards: "1",
  hydrated_shards: "0",
};
const aggregateResponse: AggregateResponse = {
  record_count: "101",
  metrics: [
    { name: "records", op: "count", value: "101", numeric_value_count: null },
  ],
  buckets: {
    dimension: { histogram: { interval_ms: 3_600_000 } },
    buckets: [
      {
        key: { type: "timestamp", timestamp_us: "1788868800000000" },
        doc_count: "101",
        metrics: [],
        children: null,
      },
    ],
    has_more: false,
  },
  warnings: [],
  read_token: "dashboard-snapshot",
  watermark: "91",
  complete: true,
  took_ms: "3",
  searched_shards: "1",
  hydrated_shards: "0",
};
const issuePage: IssuePage = { items: [], next_cursor: null };
const systemStatus: SystemStatus = {
  replay: {
    active_replays: "0",
    partial_replays: "0",
    expired_replays: "0",
    segments: "0",
    referenced_bytes: "0",
    backup_pending: false,
  },
  replay_maintenance: null,
  sentry_ingest_since_start: {},
  version: "0.1.0",
  ready: true,
  ingest_accepting: true,
  installation_id: "installation",
  storage_generation: "generation",
  applied_inbox_id: "9",
  applied_ingest_seq: "91",
  inbox_records: "0",
  inbox_bytes: "0",
  database_bytes: "4096",
  wal_bytes: "0",
  disk: {
    total_bytes: "1073741824",
    free_bytes: "805306368",
    reserved_bytes: "0",
    minimum_free_bytes: "536870912",
    ingest_accepting: true,
  },
  shards: {
    active: "1",
    local: "0",
    remote_verified: "0",
    remote_only: "0",
    records: "91",
    catalog_bytes: "4096",
    recoverable_records: "0",
  },
  backup: {
    configured: false,
    state: "disabled",
    latest_checkpoint_id: null,
    recoverable_through_ingest_seq: null,
    lag_records: null,
  },
  alerts: {
    pending_deliveries: "0",
    failed_deliveries: "0",
    evaluation_failures: "0",
  },
};

function renderPage(
  initialPath = "/?project=7&start=2026-09-08T00%3A00%3A00Z&end=2026-09-09T00%3A00%3A00Z",
  currentSession: Session | null = session,
  availableProjects: Project[] | null = projects,
) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, currentSession);
  if (currentSession && availableProjects)
    client.setQueryData(["projects", currentSession.id], availableProjects);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[initialPath]}>
          <Routes>
            <Route path="/" element={children} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
  }
  return render(<DashboardPage />, { wrapper: Wrapper });
}

afterEach(() => vi.restoreAllMocks());

describe("DashboardPage", () => {
  it("shares the rows token and absolute scope with the volume histogram", async () => {
    const rows = vi.spyOn(endpoints, "logs").mockResolvedValue(searchPage);
    const aggregate = vi
      .spyOn(endpoints, "aggregate")
      .mockResolvedValue(aggregateResponse);
    vi.spyOn(endpoints, "issues").mockResolvedValue(issuePage);
    vi.spyOn(endpoints, "systemStatus").mockResolvedValue(systemStatus);
    renderPage();

    await waitFor(() =>
      expect(rows).toHaveBeenCalledWith(
        expect.objectContaining({
          projects: ["7"],
          start: "2026-09-08T00:00:00Z",
          end: "2026-09-09T00:00:00Z",
          limit: 8,
        }),
        expect.anything(),
      ),
    );
    await waitFor(() =>
      expect(aggregate).toHaveBeenCalledWith(
        expect.objectContaining({
          projects: ["7"],
          start: "2026-09-08T00:00:00Z",
          end: "2026-09-09T00:00:00Z",
          read_token: "dashboard-snapshot",
        }),
        expect.anything(),
      ),
    );
    await waitFor(() =>
      expect(
        screen.getByText("최근 24시간 수집").parentElement,
      ).toHaveTextContent("101"),
    );
    expect(screen.getByText("database timeout")).toBeInTheDocument();
    expect(screen.getByText("정상")).toBeInTheDocument();
    expect(screen.getByText("91 W")).toBeInTheDocument();
  });
});

it("automatically chooses the first active project and keeps absolute bounds when switching", async () => {
  const user = userEvent.setup();
  const rows = vi.spyOn(endpoints, "logs").mockResolvedValue({
    ...searchPage,
    rows: [{ ...searchPage.rows[0], message: "" }],
  });
  vi.spyOn(endpoints, "aggregate").mockResolvedValue(aggregateResponse);
  vi.spyOn(endpoints, "issues").mockResolvedValue({
    items: [],
    next_cursor: { last_seen_us: "100", id: "next" },
  });
  const system = vi.spyOn(endpoints, "systemStatus");
  renderPage("/", { ...session, role: "member" }, [
    ...projects,
    { id: "8", slug: "shop", name: "Shop", is_active: true },
    { id: "9", slug: "old", name: "Old", is_active: false },
  ]);
  await waitFor(() => expect(rows).toHaveBeenCalledTimes(1));
  const first = rows.mock.calls[0][0];
  expect(first.projects).toEqual(["7"]);
  expect(Date.parse(first.end) - Date.parse(first.start)).toBeGreaterThan(0);
  expect(await screen.findByText("(빈 메시지)")).toBeInTheDocument();
  expect(screen.getByText("다음 결과 있음")).toBeInTheDocument();
  expect(screen.getByText("제한됨")).toBeInTheDocument();
  expect(system).not.toHaveBeenCalled();
  expect(screen.queryByRole("option", { name: "Old" })).not.toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("프로젝트"), "8");
  await waitFor(() =>
    expect(rows).toHaveBeenLastCalledWith(
      expect.objectContaining({
        projects: ["8"],
        start: first.start,
        end: first.end,
      }),
      expect.anything(),
    ),
  );
});

it.each(["admin", "member"] as const)(
  "explains empty project access for %s without querying records",
  async (role) => {
    const rows = vi.spyOn(endpoints, "logs");
    vi.spyOn(endpoints, "systemStatus").mockResolvedValue(systemStatus);
    renderPage("/", { ...session, role }, []);
    expect(screen.getByText("활성 프로젝트가 없습니다.")).toBeInTheDocument();
    expect(screen.getByLabelText("프로젝트")).toBeDisabled();
    if (role === "admin")
      expect(
        screen.getByRole("link", { name: "웹사이트 연결하기 →" }),
      ).toHaveAttribute("href", "/projects");
    else
      expect(
        screen.getByText("관리자에게 프로젝트 연결을 요청해 주세요."),
      ).toBeInTheDocument();
    expect(rows).not.toHaveBeenCalled();
  },
);

it("does not silently replace unavailable project or malformed bounds", () => {
  const rows = vi.spyOn(endpoints, "logs");
  vi.spyOn(endpoints, "systemStatus").mockResolvedValue(systemStatus);
  const view = renderPage("/?project=99&start=bad&end=bad");
  expect(
    screen.getByText("선택한 프로젝트가 없거나 중지되었습니다."),
  ).toBeInTheDocument();
  expect(rows).not.toHaveBeenCalled();
  view.unmount();
  renderPage("/?project=7&start=2026-09-09&end=2026-09-08");
  expect(
    screen.queryByRole("region", { name: "프로젝트 요약" }),
  ).not.toBeInTheDocument();
  expect(rows).not.toHaveBeenCalled();
});

it("exposes independent record, issue and system errors without replacing them with zero", async () => {
  vi.spyOn(endpoints, "logs").mockRejectedValue(new Error("offline"));
  vi.spyOn(endpoints, "issues").mockRejectedValue(new Error("offline"));
  vi.spyOn(endpoints, "systemStatus").mockRejectedValue(new Error("offline"));
  const aggregate = vi.spyOn(endpoints, "aggregate");
  renderPage();
  expect(await screen.findByText("오류")).toBeInTheDocument();
  expect(screen.getAllByRole("alert")).toHaveLength(2);
  expect(screen.getByText("최근 24시간 수집").parentElement).toHaveTextContent(
    "—",
  );
  expect(aggregate).not.toHaveBeenCalled();
});

it("reports histogram failure separately from a successful empty record snapshot", async () => {
  vi.spyOn(endpoints, "logs").mockResolvedValue({ ...searchPage, rows: [] });
  vi.spyOn(endpoints, "issues").mockResolvedValue(issuePage);
  vi.spyOn(endpoints, "systemStatus").mockResolvedValue({
    ...systemStatus,
    ready: false,
  });
  vi.spyOn(endpoints, "aggregate").mockRejectedValue(new Error("offline"));
  renderPage();
  expect(
    await screen.findByText("최근 record가 없습니다."),
  ).toBeInTheDocument();
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(screen.getByText("확인 중")).toBeInTheDocument();
  expect(screen.getByText("91 W")).toBeInTheDocument();
});

it("shows project failures and makes no authenticated requests after session loss", async () => {
  vi.spyOn(endpoints, "projects").mockRejectedValue(new Error("offline"));
  vi.spyOn(endpoints, "systemStatus").mockResolvedValue(systemStatus);
  const rows = vi.spyOn(endpoints, "logs");
  const view = renderPage("/", session, null);
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(rows).not.toHaveBeenCalled();
  view.unmount();
  vi.clearAllMocks();
  renderPage("/", null);
  expect(endpoints.projects).not.toHaveBeenCalled();
  expect(endpoints.systemStatus).not.toHaveBeenCalled();
});
