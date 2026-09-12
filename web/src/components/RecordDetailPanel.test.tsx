import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentProps } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../api/endpoints";
import { RecordDetailPanel } from "./RecordDetailPanel";

type Related = Awaited<ReturnType<typeof endpoints.relatedRecords>>;
function related(
  strategy: Related["strategy"] = "trace_id",
  exact = true,
): Related {
  return {
    reference_record_id: "source",
    strategy,
    exact,
    window_seconds: 30,
    watermark: "99",
    complete: true,
    truncated: false,
    rows: [],
  };
}
function mount(props: Partial<ComponentProps<typeof RecordDetailPanel>> = {}) {
  const onClose = vi.fn();
  const onRetry = vi.fn();
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const view = render(
    <QueryClientProvider client={client}>
      <RecordDetailPanel
        heading="Record detail"
        recordId="source"
        pending={false}
        onClose={onClose}
        onRetry={onRetry}
        {...props}
      />
    </QueryClientProvider>,
  );
  return { ...view, onClose, onRetry };
}
afterEach(() => vi.restoreAllMocks());

it("focuses close and keeps retry and keyboard dismissal independent of loading", async () => {
  const user = userEvent.setup();
  const query = vi.spyOn(endpoints, "relatedRecords");
  const view = mount({
    pending: true,
    error: "원문 오류",
    detailToken: "token",
  });
  expect(screen.getByRole("button", { name: "닫기" })).toHaveFocus();
  expect(screen.getByText("원문 불러오는 중")).toBeInTheDocument();
  await user.keyboard("x");
  expect(view.onClose).not.toHaveBeenCalled();
  await user.keyboard("{Escape}");
  expect(view.onClose).toHaveBeenCalledTimes(1);
  await user.click(screen.getByRole("button", { name: "다시 시도" }));
  expect(view.onRetry).toHaveBeenCalledTimes(1);
  await user.click(screen.getByRole("button", { name: "닫기" }));
  expect(view.onClose).toHaveBeenCalledTimes(2);
  expect(query).not.toHaveBeenCalled();
});

it("renders partial untrusted exception and breadcrumb values as text with fallbacks", () => {
  mount({
    raw: {
      exception: {
        values: [
          null,
          [],
          1,
          {},
          {
            type: 17,
            value: false,
            stacktrace: {
              frames: [
                null,
                [],
                {},
                { abs_path: "/source.ts", lineno: 4 },
                {
                  filename: "app.ts",
                  colno: 9,
                  function: "<script>bad()</script>",
                },
              ],
            },
          },
        ],
      },
      breadcrumbs: [
        null,
        [],
        {},
        { type: "navigation", message: false, timestamp: 12 },
        { category: "ui", message: "clicked", timestamp: "" },
      ],
    },
  });
  const exceptions = within(
    screen.getByRole("region", { name: "Exception chain" }),
  );
  expect(exceptions.getByText("종류 없음")).toBeInTheDocument();
  expect(exceptions.getByText("설명 없음")).toBeInTheDocument();
  expect(exceptions.getByText("Stack frame이 없습니다.")).toBeInTheDocument();
  for (const label of [
    "17",
    "false",
    "파일 없음",
    "/source.ts:4",
    "app.ts:9",
    "<script>bad()</script>",
  ])
    expect(exceptions.getByText(label)).toBeInTheDocument();
  expect(document.querySelector("script")).toBeNull();
  const breadcrumbs = within(
    screen.getByRole("region", { name: "Breadcrumbs" }),
  );
  for (const label of [
    "breadcrumb",
    "메시지 없음",
    "navigation",
    "false",
    "12",
    "ui",
    "clicked",
  ])
    expect(breadcrumbs.getByText(label)).toBeInTheDocument();
});

it.each([
  null,
  [],
  { exception: { values: "bad" }, breadcrumbs: { values: "bad" } },
])("handles missing or malformed raw collections: %j", (raw) => {
  mount({ raw });
  expect(screen.getByText("Exception 정보가 없습니다.")).toBeInTheDocument();
  expect(screen.getByText("Breadcrumb가 없습니다.")).toBeInTheDocument();
});

it("loads wrapped breadcrumbs and retries related search with the selected time window", async () => {
  const user = userEvent.setup();
  const query = vi
    .spyOn(endpoints, "relatedRecords")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue(related());
  mount({
    detailToken: "signed-token",
    raw: { breadcrumbs: { values: [{ category: "sdk", message: "ready" }] } },
  });
  expect(screen.getByText("연관 로그 검색 중")).toBeInTheDocument();
  expect(screen.getByText("ready")).toBeInTheDocument();
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  expect(
    await screen.findByText("정확한 ID 연결 · 동일 trace_id"),
  ).toBeInTheDocument();
  expect(
    screen.getByText("연결 기준에 맞는 다른 로그가 없습니다."),
  ).toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("시간 범위"), "21600");
  await waitFor(() =>
    expect(query).toHaveBeenLastCalledWith(
      "signed-token",
      21600,
      expect.any(AbortSignal),
    ),
  );
  expect(query).toHaveBeenCalledTimes(3);
});

it.each([
  ["request_id", true, "동일 request_id"],
  [
    "project_service_user_time",
    false,
    "같은 프로젝트·서비스·사용자와 근접 시각",
  ],
  ["project_service_error_time", false, "같은 프로젝트·서비스의 Error ±30초"],
] as const)(
  "distinguishes %s links and renders related messages",
  async (strategy, exact, label) => {
    const data = related(strategy, exact);
    data.rows = ["", "related message"].map((message, index) => ({
      record_id: `row-${index}`,
      kind: "log",
      project_id: "7",
      ingest_seq: String(index + 1),
      timestamp: "2026-09-08T00:00:00Z",
      received_at: "2026-09-08T00:00:00Z",
      service: "api",
      level: "info",
      message,
      environment: null,
      release: null,
      logger: null,
      trace_id: null,
      span_id: null,
      request_id: null,
      issue_id: null,
      fingerprint: null,
      user_id: null,
      user_email: null,
      detail_token: "related-token",
    }));
    vi.spyOn(endpoints, "relatedRecords").mockResolvedValue(data);
    mount({ raw: {}, detailToken: "token" });
    expect(await screen.findByText(new RegExp(label))).toBeInTheDocument();
    expect(screen.getByText("(빈 메시지)")).toBeInTheDocument();
    expect(screen.getByText("related message")).toBeInTheDocument();
    if (exact) expect(screen.getByLabelText("시간 범위")).toBeInTheDocument();
    else {
      expect(screen.queryByLabelText("시간 범위")).not.toBeInTheDocument();
      expect(screen.getByText("시간 범위 ±30초 (고정)")).toBeInTheDocument();
    }
  },
);
