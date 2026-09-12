import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type {
  AggregateResponse,
  Project,
  RelatedRecords,
  SearchPage,
  SearchRow,
  Session,
} from "../../api/types";
import { sessionQueryKey } from "../auth";
import { LogsPage } from "./LogsPage";

const session: Session = {
  id: "member-9007199254740993",
  email: "member@example.test",
  role: "member",
  csrf_token: "csrf",
};
const projects: Project[] = [
  { id: "9007199254740993", slug: "primary", name: "Primary", is_active: true },
  { id: "9007199254740995", slug: "other", name: "Other", is_active: true },
];
const basePath =
  "/logs?start=2026-09-07T00%3A00%3A00Z&end=2026-09-09T00%3A00%3A00Z";

function row(overrides: Partial<SearchRow> = {}): SearchRow {
  return {
    record_id: "a".repeat(64),
    kind: "log",
    project_id: projects[0].id,
    ingest_seq: "9007199254740993",
    timestamp: "2026-09-08T00:00:00.000001Z",
    received_at: "2026-09-08T00:00:01Z",
    service: "api",
    level: "info",
    message: "request finished",
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
    detail_token: "detail-token",
    ...overrides,
  };
}

function page(overrides: Partial<SearchPage> = {}): SearchPage {
  return {
    rows: [row()],
    next_cursor: null,
    read_token: "read-token",
    watermark: "9007199254740999",
    complete: true,
    took_ms: "12",
    searched_shards: "1",
    hydrated_shards: "0",
    ...overrides,
  };
}

function aggregate(): AggregateResponse {
  return {
    record_count: "7",
    metrics: [
      { name: "records", op: "count", value: "7", numeric_value_count: null },
    ],
    buckets: {
      dimension: { histogram: { interval_ms: 3_600_000 } },
      buckets: [
        {
          key: { type: "timestamp", timestamp_us: "1788825600000000" },
          doc_count: "7",
          metrics: [],
          children: null,
        },
      ],
      has_more: false,
    },
    warnings: [],
    read_token: "read-token",
    watermark: "9007199254740999",
    complete: true,
    took_ms: "3",
    searched_shards: "1",
    hydrated_shards: "0",
  };
}

function related(): RelatedRecords {
  return {
    reference_record_id: "a".repeat(64),
    strategy: "trace_id",
    exact: true,
    window_seconds: 3600,
    watermark: "9007199254740999",
    complete: true,
    truncated: false,
    rows: [
      row({
        record_id: "b".repeat(64),
        detail_token: "related-detail-token",
        message: "correlated request",
        trace_id: "trace-a",
      }),
    ],
  };
}

function LocationProbe() {
  const location = useLocation();
  return <output data-testid="location">{location.search}</output>;
}

function renderPage(
  path = basePath,
  authenticated = true,
  availableProjects: Project[] | null = projects,
) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, authenticated ? session : null);
  if (availableProjects)
    client.setQueryData(["projects", session.id], availableProjects);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route
              path="/logs"
              element={
                <>
                  {children}
                  <LocationProbe />
                </>
              }
            />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
  }
  return { client, ...render(<LogsPage />, { wrapper: Wrapper }) };
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
beforeEach(() => {
  vi.spyOn(endpoints, "aggregate").mockResolvedValue(aggregate());
  vi.spyOn(endpoints, "relatedRecords").mockResolvedValue(related());
});

describe("LogsPage", () => {
  it("applies quick time ranges only after the search is submitted", async () => {
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    renderPage();
    const user = userEvent.setup();
    await user.selectOptions(
      screen.getByRole("combobox", { name: "빠른 기간" }),
      "15",
    );
    expect(screen.getByTestId("location")).toHaveTextContent("2026-09-07");
    await user.click(screen.getByRole("button", { name: "검색" }));
    await waitFor(() => {
      const params = new URLSearchParams(
        screen.getByTestId("location").textContent ?? "",
      );
      expect(
        Date.parse(params.get("end") ?? "") -
          Date.parse(params.get("start") ?? ""),
      ).toBe(900000);
    });
  });

  it("subscribes with a native-search-compatible future date", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "EventSource",
      class {
        constructor(url: string) {
          calls.push(url);
        }
        addEventListener() {}
        close() {}
      },
    );
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    renderPage();
    await userEvent
      .setup()
      .click(await screen.findByRole("button", { name: "Live 시작" }));
    await waitFor(() => expect(calls).toHaveLength(1));
    const end =
      new URL(calls[0], "http://localhost").searchParams.get("end") ?? "";
    const nanos = BigInt(Date.parse(end)) * 1000000n;
    expect(nanos).toBeLessThanOrEqual(9223372036854775807n);
    expect(Date.parse(end)).toBeGreaterThan(Date.now());
  });

  it("shows the fixed fallback correlation window instead of an ineffective selector", async () => {
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    vi.spyOn(endpoints, "recordDetail").mockResolvedValue({
      record_id: "a".repeat(64),
      raw: {},
    });
    vi.mocked(endpoints.relatedRecords).mockResolvedValue({
      ...related(),
      exact: false,
      strategy: "project_service_error_time",
      window_seconds: 30,
    });
    renderPage();
    await userEvent
      .setup()
      .click(await screen.findByRole("button", { name: "상세 보기" }));
    expect(
      await screen.findByText("시간 범위 ±30초 (고정)"),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: "시간 범위" }),
    ).not.toBeInTheDocument();
  });

  it("uses the rows read token and exact URL scope for its histogram", async () => {
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    const aggregation = vi.mocked(endpoints.aggregate);
    const path = `${basePath}&project=${projects[0].id}&query=timeout&levels=error`;
    renderPage(path);

    await waitFor(() =>
      expect(aggregation).toHaveBeenCalledWith(
        expect.objectContaining({
          projects: [projects[0].id],
          start: "2026-09-07T00:00:00Z",
          end: "2026-09-09T00:00:00Z",
          query: "timeout",
          filters: expect.objectContaining({ levels: ["error"] }),
          read_token: "read-token",
          histogram: { field: "timestamp", interval: "auto" },
        }),
        expect.anything(),
      ),
    );
    expect(await screen.findByLabelText(/7건/)).toBeInTheDocument();
  });

  it("uses URL filters and keeps cursor pages in the same read snapshot and user cache", async () => {
    const user = userEvent.setup();
    const search = vi
      .spyOn(endpoints, "logs")
      .mockResolvedValue(
        page({ next_cursor: "next-token", read_token: "snapshot-token" }),
      );
    const path = `${basePath}&project=${projects[0].id}&query=timeout&kinds=error&levels=warning`;
    const { client } = renderPage(path);

    await waitFor(() =>
      expect(search).toHaveBeenCalledWith(
        expect.objectContaining({
          projects: [projects[0].id],
          query: "timeout",
          filters: expect.objectContaining({
            kinds: ["error"],
            levels: ["warning"],
          }),
          cursor: undefined,
          readToken: undefined,
        }),
        expect.anything(),
      ),
    );
    await user.click(
      await screen.findByRole("button", { name: "다음 페이지" }),
    );
    await waitFor(() => {
      expect(search).toHaveBeenLastCalledWith(
        expect.objectContaining({
          cursor: "next-token",
          readToken: "snapshot-token",
        }),
        expect.anything(),
      );
    });
    expect(screen.getByTestId("location")).not.toHaveTextContent(
      /cursor|read_token/,
    );
    expect(
      client
        .getQueryCache()
        .getAll()
        .some(
          (entry) =>
            entry.queryKey.includes(session.id) &&
            entry.queryKey.includes("snapshot-token"),
        ),
    ).toBe(true);
  });

  it("keeps form edits local until apply, writes absolute bounds, and resets the snapshot", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    renderPage(basePath);

    const query = screen.getByLabelText("검색어");
    await user.type(query, "database");
    expect(screen.getByTestId("location")).not.toHaveTextContent("database");
    await user.click(screen.getByText("프로젝트 · 전체"));
    await user.click(screen.getByLabelText("Primary"));
    await user.click(screen.getByRole("button", { name: "검색" }));

    await waitFor(() => {
      const location = screen.getByTestId("location").textContent ?? "";
      expect(location).toContain("start=2026-09-07T00%3A00%3A00Z");
      expect(location).toContain(`project=${projects[0].id}`);
      expect(location).toContain("query=database");
      expect(location).not.toContain("cursor=");
      expect(location).not.toContain("read_token=");
    });
  });

  it("distinguishes expired snapshots from empty results and offers a clean restart", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "logs")
      .mockResolvedValueOnce(page({ next_cursor: "expired-page" }))
      .mockRejectedValueOnce(
        new ApiError(410, {
          error: {
            code: "search_token_expired",
            message: "search_token_expired",
            request_id: "request-1",
            retryable: false,
          },
        }),
      )
      .mockResolvedValue(page());
    renderPage(basePath);

    await user.click(
      await screen.findByRole("button", { name: "다음 페이지" }),
    );
    expect(
      await screen.findByText(/검색 스냅샷이 만료되었습니다/),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("조건에 맞는 로그가 없습니다."),
    ).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "새 스냅샷" }));
    await waitFor(() => {
      const location = screen.getByTestId("location").textContent ?? "";
      expect(location).not.toContain("cursor=");
      expect(location).not.toContain("read_token=");
    });
    expect(await screen.findByText("request finished")).toBeInTheDocument();
  });

  it("hydrates raw JSON only on demand and renders hostile content as text", async () => {
    const user = userEvent.setup();
    const hostile = '<script>alert("unsafe")</script>';
    vi.spyOn(endpoints, "logs").mockResolvedValue(
      page({ rows: [row({ message: hostile })] }),
    );
    const detail = vi.spyOn(endpoints, "recordDetail").mockResolvedValue({
      record_id: "a".repeat(64),
      raw: { message: hostile, nested: { ok: true } },
    });
    const { container } = renderPage();

    expect(await screen.findByText(hostile)).toBeInTheDocument();
    expect(detail).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "상세 보기" }));
    await waitFor(() =>
      expect(detail).toHaveBeenCalledWith("detail-token", expect.anything()),
    );
    expect(
      (await screen.findAllByText(/<script>alert/)).length,
    ).toBeGreaterThanOrEqual(2);
    expect(container.querySelector("script")).toBeNull();
  });

  it("waits for record detail before starting its related-record search", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    const detail = vi
      .spyOn(endpoints, "recordDetail")
      .mockImplementation(() => new Promise(() => undefined));
    const relatedRequest = vi.mocked(endpoints.relatedRecords);
    renderPage();

    await user.click(await screen.findByRole("button", { name: "상세 보기" }));
    await waitFor(() => expect(detail).toHaveBeenCalled());
    expect(relatedRequest).not.toHaveBeenCalled();
  });

  it("shows the actual correlation strategy and lets the user widen its time window", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "logs").mockResolvedValue(page());
    vi.spyOn(endpoints, "recordDetail").mockResolvedValue({
      record_id: "a".repeat(64),
      raw: { message: "request finished" },
    });
    const relatedRequest = vi.mocked(endpoints.relatedRecords);
    renderPage();

    await user.click(await screen.findByRole("button", { name: "상세 보기" }));
    expect(
      await screen.findByText("정확한 ID 연결 · 동일 trace_id"),
    ).toBeInTheDocument();
    expect(screen.getByText("correlated request")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("시간 범위"), "21600");
    await waitFor(() =>
      expect(relatedRequest).toHaveBeenLastCalledWith(
        "detail-token",
        21600,
        expect.anything(),
      ),
    );
  });
});

it("retries each failed request and restores detail trigger focus", async () => {
  const user = userEvent.setup();
  const search = vi
    .spyOn(endpoints, "logs")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue(page());
  vi.mocked(endpoints.aggregate)
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue(aggregate());
  const detail = vi
    .spyOn(endpoints, "recordDetail")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue({ record_id: row().record_id, raw: {} });
  renderPage();
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  const trigger = await screen.findByRole("button", { name: "상세 보기" });
  await user.click(screen.getByText("시간별 분포"));
  await user.click(
    await screen.findByRole("button", { name: "집계 다시 시도" }),
  );
  expect(await screen.findByLabelText(/7건/)).toBeInTheDocument();
  await user.click(trigger);
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  expect(
    await screen.findByText("Exception 정보가 없습니다."),
  ).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "닫기" }));
  expect(trigger).toHaveFocus();
  await user.click(screen.getByRole("button", { name: "새 스냅샷" }));
  await waitFor(() => expect(search).toHaveBeenCalledTimes(3));
  expect(detail).toHaveBeenCalledTimes(2);
});

it("keeps table display changes local and applies clicked values as fresh filters", async () => {
  const user = userEvent.setup();
  const search = vi.spyOn(endpoints, "logs").mockResolvedValue(
    page({
      rows: [
        row({
          project_id: "77",
          message: "",
          environment: null,
          release: null,
          logger: null,
        }),
      ],
    }),
  );
  const { container } = renderPage();
  expect(await screen.findByText("프로젝트 77")).toBeInTheDocument();
  expect(screen.getByText("(빈 메시지)")).toBeInTheDocument();
  expect(screen.getByText("추가 메타데이터 없음")).toBeInTheDocument();
  await user.click(screen.getByLabelText("메시지 줄바꿈"));
  await user.click(screen.getByLabelText("메타데이터 열"));
  expect(container.querySelector(".log-table-wrap")).not.toHaveClass(
    "log-table--compact",
  );
  expect(container.querySelector(".log-table-wrap")).toHaveClass(
    "log-table--hide-metadata",
  );
  expect(search).toHaveBeenCalledTimes(1);
  await user.click(screen.getByRole("button", { name: "api" }));
  await waitFor(() =>
    expect(search).toHaveBeenLastCalledWith(
      expect.objectContaining({
        filters: expect.objectContaining({ services: ["api"] }),
        cursor: undefined,
      }),
      expect.anything(),
    ),
  );
});

it("adds initial bounds and distinguishes empty records, project failures and invalid criteria", async () => {
  vi.spyOn(endpoints, "projects").mockRejectedValue(new Error("offline"));
  const search = vi
    .spyOn(endpoints, "logs")
    .mockResolvedValue(page({ rows: [] }));
  const view = renderPage("/logs", true, null);
  expect(
    await screen.findByText("조건에 맞는 로그가 없습니다."),
  ).toBeInTheDocument();
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(screen.getByTestId("location")).toHaveTextContent("start=");
  view.unmount();
  vi.clearAllMocks();
  const invalid = renderPage("/logs?start=bad&end=bad");
  expect(screen.getByText(/시작과 종료를 올바른 RFC3339/)).toBeInTheDocument();
  expect(search).not.toHaveBeenCalled();
  invalid.unmount();
  renderPage(basePath, false);
  expect(search).not.toHaveBeenCalled();
});

it("renders real live connection transitions and opens live records on demand", async () => {
  const instances: TestSource[] = [];
  class TestSource extends EventTarget {
    static CLOSED = 2;
    readyState = 1;
    onopen?: () => void;
    close = vi.fn();
    constructor() {
      super();
      instances.push(this);
    }
    emit(type: string, data: unknown) {
      this.dispatchEvent(
        new MessageEvent(type, { data: JSON.stringify(data) }),
      );
    }
  }
  vi.stubGlobal("EventSource", TestSource);
  vi.spyOn(endpoints, "logs").mockResolvedValue(page({ rows: [] }));
  const detail = vi
    .spyOn(endpoints, "recordDetail")
    .mockResolvedValue({ record_id: row().record_id, raw: {} });
  const user = userEvent.setup();
  renderPage();
  await user.click(screen.getByRole("button", { name: "Live 시작" }));
  expect(screen.getByText("연결 중")).toBeInTheDocument();
  act(() => instances[0].onopen?.());
  expect(screen.getByText("연결됨 · seq 동기화 중")).toBeInTheDocument();
  expect(
    screen.getByText("새로 수신된 조건 일치 기록이 없습니다."),
  ).toBeInTheDocument();
  act(() => instances[0].emit("checkpoint", { scan_seq: "9007199254740999" }));
  expect(screen.getByText("연결됨 · seq 9007199254740999")).toBeInTheDocument();
  act(() => instances[0].emit("record", row({ message: "live record" })));
  expect(detail).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: /live record/ }));
  await waitFor(() =>
    expect(detail).toHaveBeenCalledWith("detail-token", expect.anything()),
  );
  await user.click(screen.getByRole("button", { name: "닫기" }));
  act(() => instances[0].dispatchEvent(new Event("error")));
  expect(screen.getByText("재연결 중")).toBeInTheDocument();
  act(() => instances[0].emit("resync_required", {}));
  expect(
    screen.getByText(/처리 한도를 넘어 연결을 닫았습니다/),
  ).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Live 중지" }));
  expect(
    screen.queryByRole("heading", { name: "Live Logs" }),
  ).not.toBeInTheDocument();
  expect(instances[0].close).toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Live 시작" }));
  act(() => instances[1].emit("record", row({ message: "" })));
  expect(screen.getByRole("button", { name: /빈 메시지/ })).toBeInTheDocument();
  act(() => instances[1].emit("error", { code: "search_access_denied" }));
  expect(
    screen.getByText(/Live 연결이 종료되었습니다.*search_access_denied/),
  ).toBeInTheDocument();
});
