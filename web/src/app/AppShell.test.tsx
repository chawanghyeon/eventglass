import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { expect, it, vi } from "vitest";
import { sessionQueryKey } from "../features/auth";
import { AppShell } from "./AppShell";

vi.mock("../features/system", () => ({ SystemReadiness: () => null }));

function Location() {
  const location = useLocation();
  return (
    <output data-testid="location">
      {location.pathname + location.search}
    </output>
  );
}
function show(path: string, role = "admin") {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  });
  client.setQueryData(sessionQueryKey, {
    id: "1",
    email: "qa@example.invalid",
    role,
    csrf_token: "qa",
  });
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="*" element={<Location />} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
it("groups settings under one of five primary destinations and preserves direct URLs", async () => {
  show("/users");
  const main = within(screen.getByRole("navigation", { name: "주요 메뉴" }));
  expect(main.getAllByRole("link")).toHaveLength(5);
  expect(main.getByRole("link", { name: "설정" })).toHaveAttribute(
    "aria-current",
    "page",
  );
  const settings = within(
    screen.getByRole("navigation", { name: "설정 메뉴" }),
  );
  expect(settings.getByRole("link", { name: "사용자" })).toHaveAttribute(
    "aria-current",
    "page",
  );
  await userEvent
    .setup()
    .click(settings.getByRole("link", { name: "알림 규칙" }));
  expect(screen.getByTestId("location")).toHaveTextContent("/alerts");
  expect(main.getByRole("link", { name: "설정" })).toHaveAttribute(
    "aria-current",
    "page",
  );
});
it("carries shared search criteria between logs and metrics without carrying pagination tokens", async () => {
  show(
    "/logs?project=1&project=2&start=2026-09-01T00:00:00Z&end=2026-09-02T00:00:00Z&query=timeout&levels=error&cursor=old",
  );
  const tabs = within(
    screen.getByRole("navigation", { name: "로그 분석 메뉴" }),
  );
  const target = new URL(
    tabs.getByRole("link", { name: "수치 분석" }).getAttribute("href") ?? "",
    "https://example.invalid",
  );
  expect(target.searchParams.getAll("project")).toEqual(["1", "2"]);
  expect(target.searchParams.get("query")).toBe("timeout");
  expect(target.searchParams.get("levels")).toBe("error");
  expect(target.searchParams.get("start")).toBe("2026-09-01T00:00:00Z");
  expect(target.searchParams.has("cursor")).toBe(false);
  await userEvent.setup().click(tabs.getByRole("link", { name: "수치 분석" }));
  expect(screen.getByTestId("location")).toHaveTextContent("/explore?");
  expect(
    within(screen.getByRole("navigation", { name: "주요 메뉴" })).getByRole(
      "link",
      { name: "로그 분석" },
    ),
  ).toHaveAttribute("aria-current", "page");
});
it("only exposes project settings navigation to members", () => {
  show("/projects", "member");
  const settings = within(
    screen.getByRole("navigation", { name: "설정 메뉴" }),
  );
  expect(settings.getAllByRole("link")).toHaveLength(1);
  expect(
    settings.getByRole("link", { name: "프로젝트·SDK 연결" }),
  ).toBeInTheDocument();
  expect(
    screen.queryByRole("link", { name: "사용자" }),
  ).not.toBeInTheDocument();
});
