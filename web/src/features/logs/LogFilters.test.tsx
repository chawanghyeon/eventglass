import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { LogFilters } from "./LogFilters";
import type { LogSearchState } from "./state";
const committed: LogSearchState = {
  start: "2026-09-01T00:00:00Z",
  end: "2026-09-02T00:00:00Z",
  query: "timeout",
  projects: ["1"],
  filters: { levels: ["error", "warn"] },
};
const projects = [
  { id: "1", slug: "shop", name: "Shop", is_active: true },
  { id: "2", slug: "api", name: "API", is_active: true },
  { id: "3", slug: "old", name: "Inactive", is_active: false },
];
afterEach(() => vi.useRealTimers());
it("submits a draft once with trimmed filters and the selected active projects", async () => {
  const user = userEvent.setup();
  const apply = vi.fn();
  render(
    <LogFilters committed={committed} projects={projects} onApply={apply} />,
  );
  await user.clear(screen.getByLabelText("검색어"));
  await user.type(screen.getByLabelText("검색어"), " payment ");
  await user.click(screen.getByText(/^시간 범위/));
  fireEvent.change(screen.getByLabelText("시작 (RFC3339)"), {
    target: { value: "2026-09-03T00:00:00Z" },
  });
  fireEvent.change(screen.getByLabelText("종료 (RFC3339)"), {
    target: { value: "2026-09-04T00:00:00Z" },
  });
  await user.click(screen.getByText(/^프로젝트 ·/));
  await user.click(screen.getByRole("checkbox", { name: "Shop" }));
  expect(screen.getByText("전체 프로젝트")).toBeVisible();
  await user.click(screen.getByRole("checkbox", { name: "API" }));
  expect(
    screen.queryByRole("checkbox", { name: "Inactive" }),
  ).not.toBeInTheDocument();
  await user.click(screen.getByText(/^메타데이터 필터/));
  await user.clear(screen.getByLabelText("레벨"));
  await user.type(screen.getByLabelText("레벨"), " error, warn, error, ");
  for (const [label, value] of [
    ["종류 (log, error)", "log"],
    ["서비스", "checkout"],
    ["환경", "prod"],
    ["릴리스", "v1"],
    ["로거", "logger"],
  ])
    await user.type(screen.getByLabelText(label), value);
  expect(apply).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "검색" }));
  expect(apply).toHaveBeenCalledOnce();
  expect(apply).toHaveBeenCalledWith({
    projects: ["2"],
    start: "2026-09-03T00:00:00Z",
    end: "2026-09-04T00:00:00Z",
    query: "payment",
    filters: {
      kinds: ["log"],
      services: ["checkout"],
      levels: ["error", "warn"],
      environments: ["prod"],
      releases: ["v1"],
      loggers: ["logger"],
    },
  });
});
it("removes one committed condition without dropping unrelated selections", async () => {
  const user = userEvent.setup();
  const apply = vi.fn();
  render(
    <LogFilters committed={committed} projects={projects} onApply={apply} />,
  );
  await user.click(
    screen.getByRole("button", { name: "레벨 error 조건 삭제" }),
  );
  expect(apply).toHaveBeenLastCalledWith({
    ...committed,
    filters: { levels: ["warn"] },
  });
  await user.click(screen.getByRole("button", { name: "검색어 조건 삭제" }));
  expect(apply).toHaveBeenLastCalledWith({ ...committed, query: "" });
});
it("replaces malformed bounds with a real quick period only on submit", async () => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(new Date("2026-09-12T12:00:00Z"));
  const user = userEvent.setup();
  const apply = vi.fn();
  render(
    <LogFilters
      committed={{
        ...committed,
        start: "bad",
        end: "bad",
        query: "",
        projects: [],
        filters: {},
      }}
      projects={projects}
      onApply={apply}
    />,
  );
  expect(screen.getByText(/시간 선택 – 시간 선택/)).toBeVisible();
  await user.selectOptions(screen.getByLabelText("빠른 기간"), "60");
  expect(apply).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "검색" }));
  expect(apply).toHaveBeenCalledWith(
    expect.objectContaining({
      start: "2026-09-12T11:00:00.000Z",
      end: "2026-09-12T12:00:00.000Z",
    }),
  );
});
