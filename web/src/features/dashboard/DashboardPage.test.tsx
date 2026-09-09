import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
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

function renderPage() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  client.setQueryData(["projects", session.id], projects);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter
          initialEntries={[
            "/?project=7&start=2026-09-08T00%3A00%3A00Z&end=2026-09-09T00%3A00%3A00Z",
          ]}
        >
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
