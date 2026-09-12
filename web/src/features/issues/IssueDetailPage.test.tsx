import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type {
  Issue,
  Occurrence,
  OccurrencePage,
  RecordDetail,
  Session,
} from "../../api/types";
import { sessionQueryKey } from "../auth";
import { IssueDetailPage } from "./IssueDetailPage";

const issueId = "a".repeat(64);
const projectId = "9007199254740993";
const session: Session = {
  id: "member-1",
  email: "member@example.test",
  role: "member",
  csrf_token: "csrf",
};

function makeIssue(overrides: Partial<Issue> = {}): Issue {
  return {
    id: issueId,
    project_id: projectId,
    fingerprint: "b".repeat(64),
    fingerprint_version: "1",
    title: "payment timeout",
    culprit: null,
    level: "error",
    status: "unresolved",
    first_seen_us: "1788800000000000",
    last_seen_us: "1788800001000000",
    occurrence_count: "2",
    first_release: "v1",
    last_release: "v2",
    resolved_at_us: null,
    resolved_through_ingest_seq: null,
    revision: "9007199254740993",
    created_at_us: "1788800000000000",
    updated_at_us: "1788800001000000",
    ...overrides,
  };
}

function LocationProbe() {
  const location = useLocation();
  return <output data-testid="location">{location.search}</output>;
}

function renderPage(
  path = `/issues/${issueId}?project=${projectId}`,
  currentSession: Session | null = session,
) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, currentSession);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route
              path="/issues/:id"
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
  return { client, ...render(<IssueDetailPage />, { wrapper: Wrapper }) };
}

const emptyOccurrences: OccurrencePage = { items: [], next_cursor: null };
const occurrence: Occurrence = {
  record_id: "c".repeat(64),
  source_event_id: "source-event-1",
  ingest_seq: "9007199254740993",
  occurred_at_us: "1788800001000000",
};
const oneOccurrence: OccurrencePage = {
  items: [occurrence],
  next_cursor: null,
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("IssueDetailPage", () => {
  it("sends the exact string revision when a member resolves an Issue", async () => {
    const user = userEvent.setup();
    const initial = makeIssue();
    const resolved = makeIssue({
      status: "resolved",
      revision: "9007199254740994",
      resolved_at_us: "1788800002000000",
      resolved_through_ingest_seq: "9007199254741000",
    });
    vi.spyOn(endpoints, "issue").mockResolvedValue(initial);
    vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(emptyOccurrences);
    const update = vi
      .spyOn(endpoints, "updateIssue")
      .mockResolvedValue(resolved);
    renderPage();

    await user.click(await screen.findByRole("button", { name: "해결 처리" }));

    await waitFor(() =>
      expect(update).toHaveBeenCalledWith(issueId, {
        status: "resolved",
        expected_revision: "9007199254740993",
      }),
    );
    expect(await screen.findByText("해결됨")).toBeInTheDocument();
    expect(screen.getByText("9,007,199,254,741,000")).toBeInTheDocument();
  });

  it("refreshes after a revision conflict and retries with the new revision", async () => {
    const user = userEvent.setup();
    const initial = makeIssue({ revision: "7" });
    const fresh = makeIssue({ status: "resolved", revision: "8" });
    const ignored = makeIssue({ status: "ignored", revision: "9" });
    const getIssue = vi
      .spyOn(endpoints, "issue")
      .mockResolvedValueOnce(initial)
      .mockResolvedValueOnce(fresh);
    vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(emptyOccurrences);
    const update = vi
      .spyOn(endpoints, "updateIssue")
      .mockRejectedValueOnce(
        new ApiError(409, {
          error: {
            code: "issue_revision_conflict",
            message: "issue_revision_conflict",
            request_id: "request-2",
            retryable: false,
          },
        }),
      )
      .mockResolvedValueOnce(ignored);
    renderPage();

    await user.click(await screen.findByRole("button", { name: "해결 처리" }));
    expect(
      await screen.findByText(
        "다른 변경이 먼저 저장되어 최신 Issue 상태를 다시 불러왔습니다.",
      ),
    ).toBeInTheDocument();
    await waitFor(() => expect(getIssue).toHaveBeenCalledTimes(2));
    await user.click(await screen.findByRole("button", { name: "무시" }));

    await waitFor(() =>
      expect(update).toHaveBeenLastCalledWith(issueId, {
        status: "ignored",
        expected_revision: "8",
      }),
    );
  });

  it("moves occurrence pagination through URL cursor strings", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
    const occurrences = vi
      .spyOn(endpoints, "issueOccurrences")
      .mockResolvedValue({
        items: [
          {
            record_id: "c".repeat(64),
            source_event_id: null,
            ingest_seq: "9007199254740993",
            occurred_at_us: "1788800001000000",
          },
        ],
        next_cursor: {
          occurred_at_us: "1788800000000000",
          ingest_seq: "9007199254740991",
        },
      });
    renderPage();

    await user.click(
      await screen.findByRole("button", { name: "다음 페이지" }),
    );

    await waitFor(() =>
      expect(occurrences).toHaveBeenCalledWith(
        issueId,
        {
          cursorOccurredAtUs: "1788800000000000",
          cursorIngestSeq: "9007199254740991",
        },
        expect.anything(),
      ),
    );
    expect(screen.getByTestId("location")).toHaveTextContent(
      "occurrence_seq=9007199254740991",
    );
  });

  it("loads a selected scrubbed payload and renders exception frames and breadcrumbs as safe text", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
    vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(oneOccurrence);
    const hostile = '<img src=x onerror="alert(1)">';
    const detail = vi
      .spyOn(endpoints, "issueOccurrenceDetail")
      .mockResolvedValue({
        record_id: occurrence.record_id,
        raw: {
          exception: {
            values: [
              {
                type: `Outer${hostile}`,
                value: '<script>alert("exception")</script>',
                stacktrace: {
                  frames: [
                    {
                      filename: `/srv/${hostile}.rs`,
                      function: '<script>alert("frame")</script>',
                      lineno: 47,
                    },
                  ],
                },
              },
            ],
          },
          breadcrumbs: {
            values: [
              {
                category: `http${hostile}`,
                message: '<script>alert("breadcrumb")</script>',
                timestamp: "2026-09-08T00:00:00Z",
              },
            ],
          },
          extra: { password: "[Filtered]", safe: "retained" },
        },
      });
    const { client, container } = renderPage();

    expect(detail).not.toHaveBeenCalled();
    await user.click(await screen.findByRole("button", { name: "원문 보기" }));
    await waitFor(() =>
      expect(detail).toHaveBeenCalledWith(
        issueId,
        occurrence.record_id,
        expect.anything(),
      ),
    );
    expect(await screen.findByText(`Outer${hostile}`)).toBeInTheDocument();
    expect(
      screen.getByText('<script>alert("frame")</script>'),
    ).toBeInTheDocument();
    expect(
      screen.getByText('<script>alert("breadcrumb")</script>'),
    ).toBeInTheDocument();
    expect(screen.getByText(/\[Filtered\]/)).toBeInTheDocument();
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("script")).toBeNull();
    expect(
      screen.getByText("Ingest #9,007,199,254,740,993"),
    ).toBeInTheDocument();
    expect(
      client
        .getQueryCache()
        .getAll()
        .some(
          (entry) =>
            entry.queryKey.includes(session.id) &&
            entry.queryKey.includes(projectId) &&
            entry.queryKey.includes(issueId) &&
            entry.queryKey.includes(occurrence.record_id),
        ),
    ).toBe(true);
  });

  it("shows explicit absent structured sections and restores trigger focus on Escape", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
    vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(oneOccurrence);
    let aborted = false;
    vi.spyOn(endpoints, "issueOccurrenceDetail").mockImplementation(
      (_issueId, _recordId, signal) =>
        new Promise<RecordDetail>((_resolve, reject) => {
          signal?.addEventListener("abort", () => {
            aborted = true;
            reject(new Error("aborted"));
          });
        }),
    );
    renderPage();

    const trigger = await screen.findByRole("button", { name: "원문 보기" });
    await user.click(trigger);
    const close = await screen.findByRole("button", { name: "닫기" });
    await waitFor(() => expect(close).toHaveFocus());
    await user.keyboard("{Escape}");

    expect(
      screen.queryByRole("button", { name: "닫기" }),
    ).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
    await waitFor(() => expect(aborted).toBe(true));
  });

  it("states when the hydrated payload has no exception or breadcrumbs", async () => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
    vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(oneOccurrence);
    vi.spyOn(endpoints, "issueOccurrenceDetail").mockResolvedValue({
      record_id: occurrence.record_id,
      raw: { message: "plain event" },
    });
    renderPage();

    await user.click(await screen.findByRole("button", { name: "원문 보기" }));
    expect(
      await screen.findByText("Exception 정보가 없습니다."),
    ).toBeInTheDocument();
    expect(screen.getByText("Breadcrumb가 없습니다.")).toBeInTheDocument();
  });

  it.each([
    [
      403,
      "search_access_denied",
      "선택한 프로젝트의 로그를 볼 권한이 없습니다.",
    ],
    [
      404,
      "issue_occurrence_not_found",
      "이 Issue에서 발생 기록 원문을 찾을 수 없습니다.",
    ],
    [
      503,
      "search_unavailable",
      "검색 인덱스를 사용할 수 없습니다. 잠시 후 다시 시도해 주세요.",
    ],
    [
      429,
      "query_busy",
      "다른 검색을 처리하고 있습니다. 잠시 후 다시 시도해 주세요.",
    ],
  ])(
    "keeps %s detail failures distinct from an absent payload",
    async (status, code, message) => {
      const user = userEvent.setup();
      vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
      vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(oneOccurrence);
      vi.spyOn(endpoints, "issueOccurrenceDetail").mockRejectedValue(
        new ApiError(status, {
          error: {
            code,
            message: code,
            request_id: "request-detail",
            retryable: false,
          },
        }),
      );
      renderPage();

      await user.click(
        await screen.findByRole("button", { name: "원문 보기" }),
      );
      expect(await screen.findByText(message)).toBeInTheDocument();
      expect(
        screen.queryByText("Exception 정보가 없습니다."),
      ).not.toBeInTheDocument();
    },
  );
});

it("recovers issue, occurrence and raw loading failures independently", async () => {
  const user = userEvent.setup();
  const issue = vi
    .spyOn(endpoints, "issue")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue(
      makeIssue({
        first_release: null,
        last_release: null,
        culprit: "checkout",
      }),
    );
  const occurrences = vi
    .spyOn(endpoints, "issueOccurrences")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue(oneOccurrence);
  const detail = vi
    .spyOn(endpoints, "issueOccurrenceDetail")
    .mockRejectedValueOnce(new Error("offline"))
    .mockResolvedValue({ record_id: occurrence.record_id, raw: {} });
  renderPage(`/issues/${issueId}`);
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  expect(
    await screen.findByRole("heading", { name: "payment timeout" }),
  ).toBeInTheDocument();
  expect(screen.getByText("checkout")).toBeInTheDocument();
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  await user.click(await screen.findByRole("button", { name: "원문 보기" }));
  await user.click(await screen.findByRole("button", { name: "다시 시도" }));
  expect(
    await screen.findByText("Exception 정보가 없습니다."),
  ).toBeInTheDocument();
  expect(issue).toHaveBeenCalledTimes(2);
  expect(occurrences).toHaveBeenCalledTimes(2);
  expect(detail).toHaveBeenCalledTimes(2);
});

it("returns to the first occurrence page without losing project scope", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
  const occurrences = vi
    .spyOn(endpoints, "issueOccurrences")
    .mockResolvedValue(emptyOccurrences);
  renderPage(
    `/issues/${issueId}?project=${projectId}&occurrence_time=100&occurrence_seq=99`,
  );
  const first = await screen.findByRole("button", { name: "처음으로" });
  expect(first).toBeEnabled();
  expect(screen.getByRole("button", { name: "다음 페이지" })).toBeDisabled();
  await user.click(first);
  await waitFor(() =>
    expect(occurrences).toHaveBeenLastCalledWith(
      issueId,
      { cursorOccurredAtUs: undefined, cursorIngestSeq: undefined },
      expect.anything(),
    ),
  );
  expect(screen.getByTestId("location")).toHaveTextContent(
    `?project=${projectId}`,
  );
  expect(screen.getByTestId("location")).not.toHaveTextContent("occurrence_");
});

it.each([
  new Error("offline"),
  new ApiError(403, {
    error: {
      code: "forbidden",
      message: "forbidden",
      request_id: "r",
      retryable: false,
    },
  }),
])("retains the saved issue status after a failed update", async (failure) => {
  const user = userEvent.setup();
  const issue = vi.spyOn(endpoints, "issue").mockResolvedValue(makeIssue());
  vi.spyOn(endpoints, "issueOccurrences").mockResolvedValue(emptyOccurrences);
  vi.spyOn(endpoints, "updateIssue").mockRejectedValue(failure);
  renderPage();
  await user.click(await screen.findByRole("button", { name: "해결 처리" }));
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "해결 처리" })).toBeEnabled();
  expect(screen.queryByText("해결됨")).not.toBeInTheDocument();
  expect(issue).toHaveBeenCalledTimes(1);
});

it("does not fetch private issue data without a session", () => {
  const issue = vi.spyOn(endpoints, "issue");
  const occurrences = vi.spyOn(endpoints, "issueOccurrences");
  renderPage(`/issues/${issueId}`, null);
  expect(issue).not.toHaveBeenCalled();
  expect(occurrences).not.toHaveBeenCalled();
});
