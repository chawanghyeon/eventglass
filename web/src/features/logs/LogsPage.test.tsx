import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Project, SearchPage, SearchRow, Session } from "../../api/types";
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

function LocationProbe() {
  const location = useLocation();
  return <output data-testid="location">{location.search}</output>;
}

function renderPage(path = basePath) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  client.setQueryData(["projects", session.id], projects);
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

afterEach(() => vi.restoreAllMocks());

describe("LogsPage", () => {
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
    await user.click(screen.getByLabelText("Primary"));
    await user.click(screen.getByRole("button", { name: "검색 적용" }));

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
});
