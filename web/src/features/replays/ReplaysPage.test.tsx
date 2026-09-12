import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { sessionQueryKey } from "../auth";
import { ReplaysPage } from "./ReplaysPage";
import type { Project, ReplaySummary } from "../../api/types";

afterEach(() => vi.restoreAllMocks());
it("switches views without mixing results or sending view state to the API", async () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, {
    id: "1",
    email: "qa@example.invalid",
    role: "admin",
    csrf_token: "qa",
  });
  client.setQueryData(
    ["projects", "1"],
    [{ id: "1", name: "Shop", slug: "shop", is_active: true }],
  );
  const list = vi
    .spyOn(endpoints, "replays")
    .mockResolvedValue({ items: [], next_cursor: null });
  const maps = vi
    .spyOn(endpoints, "replayMaps")
    .mockResolvedValue({ pages: {}, replays_analyzed: 0, truncated: false });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/replays?project=1&environment=qa"]}>
        <ReplaysPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  expect(
    await screen.findByText(/조건에 맞는 Replay가 없습니다/),
  ).toBeInTheDocument();
  expect(
    screen.getByText("추가 필터 · 1개 적용 중").closest("details"),
  ).not.toHaveAttribute("open");
  const keyboard = userEvent.setup();
  await keyboard.type(
    screen.getByRole("textbox", { name: "페이지 검색" }),
    "/products",
  );
  expect(list.mock.calls).toHaveLength(1);
  await keyboard.keyboard("{Enter}");
  await waitFor(() => expect(list.mock.calls).toHaveLength(2));
  expect(list.mock.calls[1][0]).toContain("url=%2Fproducts");
  await userEvent
    .setup()
    .click(screen.getByRole("button", { name: "페이지 분석" }));
  await waitFor(() => expect(maps).toHaveBeenCalled());
  expect(
    screen.queryByText(/조건에 맞는 Replay가 없습니다/),
  ).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "페이지 분석" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  expect(maps.mock.calls[0][0]).toContain("environment=qa");
  expect(maps.mock.calls[0][0]).not.toContain("view=");
  expect(list.mock.calls).toHaveLength(2);
});

it("guides a first-time admin to setup instead of showing unusable replay filters", () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, {
    id: "first",
    email: "first@example.invalid",
    role: "admin",
    csrf_token: "qa",
  });
  client.setQueryData(["projects", "first"], []);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ReplaysPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  expect(
    screen.getByRole("link", { name: "웹사이트 연결하기 →" }),
  ).toHaveAttribute("href", "/projects");
  expect(
    screen.queryByRole("textbox", { name: "URL" }),
  ).not.toBeInTheDocument();
  expect(
    screen.queryByRole("button", { name: "새로고침" }),
  ).not.toBeInTheDocument();
});

const projects: Project[] = [
  { id: "1", slug: "shop", name: "Shop", is_active: true },
  { id: "2", slug: "api", name: "API", is_active: true },
];
function showList(
  path = "/replays?project=1",
  role: "admin" | "member" | null = "member",
  items = projects,
) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(
    sessionQueryKey,
    role
      ? { id: "7", email: "user@example.test", role, csrf_token: "fixture" }
      : null,
  );
  client.setQueryData(["projects", role ? "7" : "unknown"], items);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <ReplaysPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return client;
}
function summary(id: string, busy: boolean): ReplaySummary {
  return {
    project_id: "1",
    partial: busy,
    segment_count: 1,
    max_segment_id: 0,
    recording_bytes: 20,
    frustration: {
      rage: busy ? 1 : 0,
      dead: busy ? 2 : 0,
      slow: busy ? 3 : 0,
      multi: busy ? 4 : 0,
    },
    metadata: {
      replay_id: id,
      segment_id: 0,
      started_at_ms: 1000,
      finished_at_ms: 5000,
      user: { id },
      environment: null,
      release: null,
      browser: null,
      os: null,
      device: null,
      urls: busy ? ["https://example.test/checkout?secret=hidden"] : [],
      error_ids: busy ? ["error"] : [],
      trace_ids: [],
      sdk_version: null,
      replay_type: null,
    },
  };
}
it("preserves submitted filters, clears stale cursors, and resets only selection filters", async () => {
  const user = userEvent.setup();
  const list = vi
    .spyOn(endpoints, "replays")
    .mockResolvedValue({ items: [], next_cursor: null });
  showList(
    "/replays?project=1&before_id=old&before_started_ms=100&viewport=narrow&view=recordings",
  );
  await screen.findByText(/조건에 맞는 Replay가 없습니다/);
  await user.type(screen.getByLabelText("페이지 검색"), "/checkout");
  await user.clear(screen.getByLabelText("페이지 검색"));
  await user.type(screen.getByLabelText("페이지 검색"), "/cart");
  await user.selectOptions(screen.getByLabelText("오류 포함"), "true");
  await user.click(screen.getByText("추가 필터", { exact: true }));
  for (const [label, value] of [
    ["환경", "prod"],
    ["릴리스", "v1"],
    ["사용자", "customer"],
  ])
    await user.type(screen.getByLabelText(label), value);
  await user.selectOptions(screen.getByLabelText("Rage click"), "false");
  await user.selectOptions(screen.getByLabelText("Dead click"), "true");
  for (const label of [
    "세션 시작 이후 (현지 시간)",
    "세션 시작 이전 (현지 시간)",
  ]) {
    fireEvent.change(screen.getByLabelText(label), {
      target: { value: "2026-09-01T12:00" },
    });
    fireEvent.change(screen.getByLabelText(label), { target: { value: "" } });
    fireEvent.change(screen.getByLabelText(label), {
      target: { value: "2026-09-02T12:00" },
    });
  }
  fireEvent.change(screen.getByLabelText("최소 시간 (초)"), {
    target: { value: "5" },
  });
  fireEvent.change(screen.getByLabelText("최소 시간 (초)"), {
    target: { value: "" },
  });
  fireEvent.change(screen.getByLabelText("최소 시간 (초)"), {
    target: { value: "5" },
  });
  await user.click(screen.getByRole("button", { name: "검색" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
  const params = new URLSearchParams(list.mock.calls[1][0]);
  expect(Object.fromEntries(params)).toMatchObject({
    project_id: "1",
    url: "/cart",
    environment: "prod",
    release: "v1",
    user: "customer",
    rage_click: "false",
    dead_click: "true",
    has_error: "true",
    min_duration_ms: "5000",
    started_after_ms: String(new Date("2026-09-02T12:00").getTime()),
  });
  expect(params.has("before_id")).toBe(false);
  expect(params.has("viewport")).toBe(false);
  await user.click(screen.getByRole("button", { name: "환경 조건 삭제" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
  await user.click(screen.getByRole("button", { name: "필터 초기화" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(4));
  expect(
    Object.fromEntries(new URLSearchParams(list.mock.calls[3][0])),
  ).toEqual({ project_id: "1" });
});
it("pages through real replay summaries and returns to the first snapshot", async () => {
  const user = userEvent.setup();
  const list = vi.spyOn(endpoints, "replays").mockResolvedValue({
    items: [summary("customer-a", true), summary("customer-b", false)],
    next_cursor: { before_id: "customer-b", before_started_ms: 1000 },
  });
  showList();
  expect(
    await screen.findByRole("link", { name: /customer-a/ }),
  ).toHaveAttribute("href", "/replays/1/customer-a");
  expect(screen.getByText("일부 누락")).toBeVisible();
  expect(screen.getByText("페이지 정보 없음")).toBeVisible();
  expect(screen.getByText(/Rage 1/)).toHaveTextContent("Dead 2 Slow 3 Multi 4");
  await user.click(screen.getByRole("button", { name: "다음" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(2));
  expect(new URLSearchParams(list.mock.calls[1][0]).get("before_id")).toBe(
    "customer-b",
  );
  await user.click(await screen.findByRole("button", { name: "처음으로" }));
  await user.click(screen.getByRole("button", { name: "새로고침" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
  expect(new URLSearchParams(list.mock.calls[2][0]).has("before_id")).toBe(
    false,
  );
});
it("refreshes only the selected analysis or feedback view and applies viewport scope", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "replays").mockResolvedValue({
    items: [],
    next_cursor: null,
  });
  const maps = vi
    .spyOn(endpoints, "replayMaps")
    .mockResolvedValue({ pages: {}, replays_analyzed: 20, truncated: true });
  const feedback = vi
    .spyOn(endpoints, "feedback")
    .mockResolvedValue({ items: [] });
  showList("/replays?project=1&view=pages");
  expect(await screen.findByText(/처리 한도에 따라 일부 구간/)).toBeVisible();
  await user.selectOptions(screen.getByLabelText("분석 화면 크기"), "narrow");
  await waitFor(() => expect(maps).toHaveBeenCalledTimes(2));
  expect(new URLSearchParams(maps.mock.calls[1][0]).get("viewport")).toBe(
    "narrow",
  );
  await user.click(screen.getByRole("button", { name: "새로고침" }));
  await waitFor(() => expect(maps).toHaveBeenCalledTimes(3));
  await user.click(screen.getByRole("button", { name: "사용자 피드백" }));
  await screen.findByText("수신한 Sentry Feedback이 없습니다.");
  await user.click(screen.getByRole("button", { name: "새로고침" }));
  await waitFor(() => expect(feedback).toHaveBeenCalledTimes(2));
  await user.selectOptions(screen.getByLabelText("프로젝트"), "2");
  await waitFor(() =>
    expect(feedback).toHaveBeenCalledWith("2", expect.any(AbortSignal)),
  );
});
it("recovers a stale project selection and guides members when no projects exist", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "replays").mockResolvedValue({
    items: [],
    next_cursor: null,
  });
  showList("/replays?project=disabled");
  expect(screen.getByText("다른 프로젝트를 선택해 주세요")).toBeVisible();
  await user.selectOptions(screen.getByLabelText("프로젝트"), "1");
  expect(
    await screen.findByText(/조건에 맞는 Replay가 없습니다/),
  ).toBeVisible();
});
it("does not ask members to administer an empty installation", () => {
  showList("/replays", "member", []);
  expect(
    screen.getByText("관리자에게 프로젝트 연결을 요청해 주세요."),
  ).toBeVisible();
});
it("reports list and map failures and tolerates malformed time filters", async () => {
  vi.spyOn(endpoints, "replays").mockRejectedValue(new Error("list failed"));
  vi.spyOn(endpoints, "replayMaps").mockRejectedValue(new Error("maps failed"));
  showList("/replays?project=1&view=pages&started_after_ms=invalid");
  await waitFor(() => expect(screen.getAllByRole("alert")).toHaveLength(2));
  expect(screen.getByLabelText("세션 시작 이후 (현지 시간)")).toHaveValue("");
});
