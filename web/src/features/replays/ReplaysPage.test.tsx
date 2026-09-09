import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { sessionQueryKey } from "../auth";
import { ReplaysPage } from "./ReplaysPage";

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
  ).toHaveAttribute("open");
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
  expect(list.mock.calls).toHaveLength(1);
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
