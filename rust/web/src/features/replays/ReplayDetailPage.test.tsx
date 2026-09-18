import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { ReplayAnalysis, ReplayDetail } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { ReplayDetailPage } from "./ReplayDetailPage";

vi.mock("./ReplayPlayer", () => ({
  ReplayPlayer: ({
    seekTo,
    onTime,
  }: {
    seekTo: { time: number } | null;
    onTime: (time: number) => void;
  }) => (
    <div>
      <output aria-label="seek target">{seekTo?.time ?? "none"}</output>
      <button onClick={() => onTime(3000)}>advance playback</button>
    </div>
  ),
}));
const detail: ReplayDetail = {
  replay: {
    project_id: "1",
    segment_count: 1,
    max_segment_id: 0,
    recording_bytes: 20,
    partial: false,
    frustration: { rage: 0, dead: 0, slow: 0, multi: 0 },
    metadata: {
      replay_id: "session",
      segment_id: 0,
      started_at_ms: 1000,
      finished_at_ms: 5000,
      user: { id: "customer" },
      environment: "production",
      release: "v1",
      browser: null,
      os: null,
      device: null,
      urls: [],
      error_ids: ["received", "pending"],
      trace_ids: ["trace&value"],
      sdk_version: null,
      replay_type: null,
    },
  },
  segments: [{ segment_id: 0, compressed_bytes: 20 }],
  associations: {
    errors: [{ event_id: "received", record_id: "record", issue_id: "issue" }],
    feedback: [{ contexts: { feedback: { message: "Checkout failed" } } }],
  },
};
const analysis: ReplayAnalysis = {
  viewport_class: "unknown",
  timeline: [
    {
      timestamp_ms: 1500.5,
      kind: "click",
      label: "Buy",
      duration_ms: 40,
      url: null,
      event_id: null,
      trace_id: null,
      span_id: null,
    },
    {
      timestamp_ms: 1600,
      kind: "navigation",
      label: "Next",
      duration_ms: null,
      url: null,
      event_id: null,
      trace_id: null,
      span_id: null,
    },
  ],
  journey: [
    { url: "/first", started_at_ms: 1000, duration_ms: 1000 },
    { url: "/next", started_at_ms: 2000, duration_ms: null },
  ],
  pages: {},
  truncated: false,
  gaps: [],
};
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  vi.restoreAllMocks();
});
function show(
  search = "",
  state?: unknown,
  authenticated = true,
  missing = false,
) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  client.setQueryData(
    sessionQueryKey,
    authenticated
      ? {
          id: "7",
          role: "member",
          email: "member@example.test",
          csrf_token: "fixture",
        }
      : null,
  );
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter
        initialEntries={[
          {
            pathname: missing ? "/missing" : "/replays/1/session",
            search,
            state,
          },
        ]}
      >
        <Routes>
          <Route
            path={missing ? "/missing" : "/replays/:project/:id"}
            element={<ReplayDetailPage />}
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
function mockData(value = detail, report = analysis) {
  const replay = vi.spyOn(endpoints, "replay").mockResolvedValue(value);
  vi.spyOn(endpoints, "replayRecording").mockResolvedValue({ events: [] });
  const analyze = vi
    .spyOn(endpoints, "replayAnalysis")
    .mockResolvedValue(report);
  return { replay, analyze };
}
it.each(["anonymous", "missing parameters"])(
  "does not fetch private data with %s",
  (reason) => {
    const request = vi.spyOn(endpoints, "replay");
    show(
      "",
      undefined,
      reason !== "anonymous",
      reason === "missing parameters",
    );
    expect(screen.getByText("Replay를 불러오는 중…")).toBeVisible();
    expect(request).not.toHaveBeenCalled();
  },
);
it("shows detail failures instead of starting playback requests", async () => {
  vi.spyOn(endpoints, "replay").mockRejectedValue(new Error("offline"));
  const recording = vi.spyOn(endpoints, "replayRecording");
  show();
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(recording).not.toHaveBeenCalled();
});
it("connects exact timeline times, journey, errors, feedback, traces, and refresh", async () => {
  const user = userEvent.setup();
  const requests = mockData();
  show("?t=1500", {
    replaysReturnTo: "/replays?project=1&environment=production",
  });
  expect(
    await screen.findByRole("heading", { name: "customer" }),
  ).toBeVisible();
  expect(await screen.findByLabelText("seek target")).toHaveTextContent("1500");
  expect(screen.getByRole("link", { name: "← 방문 기록으로" })).toHaveAttribute(
    "href",
    "/replays?project=1&environment=production",
  );
  expect(await screen.findByText("Checkout failed")).toBeVisible();
  expect(screen.getByRole("link", { name: "Error received" })).toHaveAttribute(
    "href",
    "/issues/issue",
  );
  expect(screen.getByText(/일부 연결된 Error/)).toBeVisible();
  const trace = new URL(
    screen
      .getByRole("link", { name: "Trace trace&value" })
      .getAttribute("href") ?? "",
    "https://example.test",
  );
  expect(trace.searchParams.get("query")).toBe("trace_id:trace&value");
  expect(trace.searchParams.get("start")).toBe("1970-01-01T00:00:00.000Z");
  await user.click(await screen.findByRole("button", { name: /Buy/ }));
  expect(screen.getByLabelText("seek target")).toHaveTextContent("1500");
  await user.click(screen.getByRole("button", { name: "/next" }));
  expect(screen.getByLabelText("seek target")).toHaveTextContent("2000");
  await user.click(screen.getByRole("button", { name: "advance playback" }));
  expect(screen.getByText("Current URL:").closest("p")).toHaveTextContent(
    "/next",
  );
  await user.click(screen.getByRole("button", { name: "새로고침" }));
  await waitFor(() => expect(requests.replay).toHaveBeenCalledTimes(2));
  expect(requests.analyze).toHaveBeenCalledTimes(2);
  await user.click(screen.getByText("이 세션의 페이지 분석"));
  expect(
    screen.getByText("좌표·viewport가 함께 확인된 페이지 데이터가 없습니다."),
  ).toBeVisible();
});
it.each(["", "?t=-1", "?t=9007199254740992", "?t=12345678901234567"])(
  "rejects unsafe seek values and untrusted return URLs (%s)",
  async (search) => {
    mockData(
      {
        ...detail,
        replay: {
          ...detail.replay,
          metadata: {
            ...detail.replay.metadata,
            user: null,
            environment: null,
            release: null,
            error_ids: [],
            trace_ids: [],
          },
        },
        associations: { errors: [], feedback: [] },
      },
      { ...analysis, journey: [], timeline: [] },
    );
    show(search, { replaysReturnTo: "https://untrusted.test/" });
    expect(await screen.findByLabelText("seek target")).toHaveTextContent(
      "none",
    );
    expect(
      screen.getByRole("link", { name: "← 방문 기록으로" }),
    ).toHaveAttribute("href", "/replays?project=1");
    expect(screen.getByRole("heading", { name: "익명 방문자" })).toBeVisible();
    expect(screen.getByText("Current URL:").closest("p")).toHaveTextContent(
      "확인되지 않음",
    );
    expect(screen.getByText(/연결된 Sentry Feedback이 없습니다/)).toBeVisible();
  },
);
it("shows partial, pending, truncated, and failed analysis states independently", async () => {
  mockData({
    ...detail,
    replay: { ...detail.replay, partial: true },
    segments: [{ segment_id: 2, compressed_bytes: 20 }],
  });
  let resolve!: (value: { events: Record<string, unknown>[] }) => void;
  vi.mocked(endpoints.replayRecording).mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  vi.mocked(endpoints.replayAnalysis).mockResolvedValue({
    ...analysis,
    truncated: true,
  });
  show();
  expect(
    await screen.findByText("Recording segments를 불러오는 중…"),
  ).toBeVisible();
  expect(screen.getByText(/Partial replay/)).toBeVisible();
  await act(async () => resolve({ events: [] }));
  expect(await screen.findByText(/큰 Replay의 처리 한도/)).toBeVisible();
});
it("does not hide segment or analysis failures behind empty replay output", async () => {
  mockData();
  vi.mocked(endpoints.replayRecording).mockRejectedValue(
    new Error("segment unavailable"),
  );
  vi.mocked(endpoints.replayAnalysis).mockRejectedValue(
    new Error("analysis unavailable"),
  );
  show();
  await waitFor(() => expect(screen.getAllByRole("alert")).toHaveLength(2));
  expect(screen.queryByLabelText("seek target")).not.toBeInTheDocument();
});
