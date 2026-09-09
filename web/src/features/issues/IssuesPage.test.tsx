import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Issue, IssuePage, Project, Session } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { IssuesPage } from "./IssuesPage";

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

function makeIssue(overrides: Partial<Issue> = {}): Issue {
  return {
    id: "a".repeat(64),
    project_id: projects[0].id,
    fingerprint: "b".repeat(64),
    fingerprint_version: "1",
    title: "database failed",
    culprit: null,
    level: "error",
    status: "resolved",
    first_seen_us: "1788800000000000",
    last_seen_us: "1788800001000000",
    occurrence_count: "9007199254740993",
    first_release: null,
    last_release: "v1",
    resolved_at_us: "1788800002000000",
    resolved_through_ingest_seq: "7",
    revision: "3",
    created_at_us: "1788800000000000",
    updated_at_us: "1788800002000000",
    ...overrides,
  };
}

function LocationProbe() {
  const location = useLocation();
  return <output data-testid="location">{location.search}</output>;
}

function renderPage(path: string) {
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
              path="/issues"
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
  return { client, ...render(<IssuesPage />, { wrapper: Wrapper }) };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("IssuesPage", () => {
  it("keeps filters and stable cursor pagination in the URL and query key", async () => {
    const user = userEvent.setup();
    const page: IssuePage = {
      items: [makeIssue()],
      next_cursor: { last_seen_us: "1788799999000000", id: "c".repeat(64) },
    };
    const list = vi.spyOn(endpoints, "issues").mockResolvedValue(page);
    const { client } = renderPage(
      `/issues?project=${projects[0].id}&status=resolved&cursor_last_seen_us=1788800001000000&cursor_id=${"d".repeat(64)}`,
    );

    await waitFor(() =>
      expect(list).toHaveBeenCalledWith(
        {
          projectId: projects[0].id,
          status: "resolved",
          query: "",
          cursorLastSeenUs: "1788800001000000",
          cursorId: "d".repeat(64),
        },
        expect.anything(),
      ),
    );
    expect(
      client
        .getQueryCache()
        .getAll()
        .some(
          (query) =>
            query.queryKey[0] === "issues" &&
            query.queryKey[1] === session.id &&
            query.queryKey[2] === projects[0].id,
        ),
    ).toBe(true);

    await user.selectOptions(screen.getByLabelText("상태"), "ignored");
    await waitFor(() => {
      const search = screen.getByTestId("location").textContent ?? "";
      expect(search).toContain("status=ignored");
      expect(search).not.toContain("cursor_last_seen_us");
      expect(search).not.toContain("cursor_id");
    });
    await waitFor(() =>
      expect(list).toHaveBeenCalledWith(
        { projectId: projects[0].id, status: "ignored", query: "" },
        expect.anything(),
      ),
    );

    await user.click(screen.getByRole("button", { name: "다음 페이지" }));
    await waitFor(() =>
      expect(list).toHaveBeenCalledWith(
        {
          projectId: projects[0].id,
          status: "ignored",
          query: "",
          cursorLastSeenUs: page.next_cursor?.last_seen_us,
          cursorId: page.next_cursor?.id,
        },
        expect.anything(),
      ),
    );
  });

  it("submits title search on Enter, resets pagination, and removes its chip", async () => {
    const user = userEvent.setup();
    const list = vi
      .spyOn(endpoints, "issues")
      .mockResolvedValue({ items: [makeIssue()], next_cursor: null });
    renderPage(
      `/issues?project=${projects[0].id}&cursor_last_seen_us=100&cursor_id=${"d".repeat(64)}`,
    );
    await screen.findByText("database failed");
    const calls = list.mock.calls.length;
    await user.type(
      screen.getByRole("textbox", { name: "오류 검색" }),
      "database",
    );
    expect(list).toHaveBeenCalledTimes(calls);
    await user.keyboard("{Enter}");
    await waitFor(() =>
      expect(list).toHaveBeenLastCalledWith(
        expect.objectContaining({ query: "database", cursorId: undefined }),
        expect.anything(),
      ),
    );
    expect(screen.getByTestId("location")).not.toHaveTextContent("cursor_id");
    await user.click(
      screen.getByRole("button", { name: "오류 검색 조건 삭제" }),
    );
    await waitFor(() =>
      expect(screen.getByRole("textbox", { name: "오류 검색" })).toHaveValue(
        "",
      ),
    );
  });

  it("renders hostile titles as text and preserves large counts", async () => {
    const hostile = '<img src=x onerror="alert(1)">';
    vi.spyOn(endpoints, "issues").mockResolvedValue({
      items: [makeIssue({ title: hostile })],
      next_cursor: null,
    });
    const { container } = renderPage(`/issues?project=${projects[0].id}`);

    expect(await screen.findByText(hostile)).toBeInTheDocument();
    expect(screen.getByText("9,007,199,254,740,993회")).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
  });

  it("shows a member permission failure as an error rather than empty data", async () => {
    vi.spyOn(endpoints, "issues").mockRejectedValue(
      new ApiError(403, {
        error: {
          code: "issue_access_denied",
          message: "issue_access_denied",
          request_id: "request-1",
          retryable: false,
        },
      }),
    );
    renderPage(`/issues?project=${projects[0].id}`);

    expect(
      await screen.findByText("이 프로젝트의 Issue를 볼 권한이 없습니다."),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("미해결 Issue가 없습니다."),
    ).not.toBeInTheDocument();
  });
});
