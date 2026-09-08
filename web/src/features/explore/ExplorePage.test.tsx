import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { AggregateResponse, Project, Session } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { ExplorePage } from "./ExplorePage";

const session: Session = {
  id: "42",
  email: "member@example.test",
  role: "member",
  csrf_token: "csrf",
};
const projects: Project[] = [
  { id: "7", slug: "api", name: "API", is_active: true },
];
const path =
  "/explore?start=2026-09-08T00%3A00%3A00Z&end=2026-09-09T00%3A00%3A00Z&project=7";

function response(): AggregateResponse {
  return {
    record_count: "12",
    metrics: [
      { name: "result", op: "count", value: "12", numeric_value_count: null },
    ],
    buckets: {
      dimension: { histogram: { interval_ms: 3_600_000 } },
      buckets: [
        {
          key: { type: "timestamp", timestamp_us: "1788825600000000" },
          doc_count: "12",
          metrics: [],
          children: {
            dimension: { group: { field: "service" } },
            buckets: [
              {
                key: { type: "string", value: "api" },
                doc_count: "12",
                metrics: [],
                children: null,
              },
            ],
            has_more: false,
          },
        },
      ],
      has_more: false,
    },
    warnings: [],
    read_token: "aggregate-snapshot",
    watermark: "81",
    complete: true,
    took_ms: "4",
    searched_shards: "2",
    hydrated_shards: "0",
  };
}

function LocationProbe() {
  return <output data-testid="location">{useLocation().search}</output>;
}

function renderPage() {
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
              path="/explore"
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
  return render(<ExplorePage />, { wrapper: Wrapper });
}

afterEach(() => vi.restoreAllMocks());

describe("ExplorePage", () => {
  it("keeps metric drafts local and sends exact URL criteria after apply", async () => {
    const user = userEvent.setup();
    const aggregate = vi
      .spyOn(endpoints, "aggregate")
      .mockResolvedValue(response());
    renderPage();

    await waitFor(() =>
      expect(aggregate).toHaveBeenCalledWith(
        expect.objectContaining({
          projects: ["7"],
          start: "2026-09-08T00:00:00Z",
          end: "2026-09-09T00:00:00Z",
          metrics: [{ name: "result", op: "count" }],
          histogram: { field: "timestamp", interval: "auto" },
        }),
        expect.anything(),
      ),
    );
    await user.selectOptions(screen.getByLabelText("Metric"), "avg");
    await user.type(
      screen.getByLabelText("숫자 필드"),
      "attributes.duration_ms",
    );
    await user.click(screen.getAllByLabelText("서비스")[1]);
    expect(screen.getByTestId("location")).not.toHaveTextContent("duration_ms");
    await user.click(screen.getByRole("button", { name: "집계 적용" }));

    await waitFor(() => {
      expect(screen.getByTestId("location")).toHaveTextContent(
        "metric=avg&field=attributes.duration_ms&group=service",
      );
      expect(aggregate).toHaveBeenLastCalledWith(
        expect.objectContaining({
          metrics: [
            {
              name: "result",
              op: "avg",
              field: "attributes.duration_ms",
            },
          ],
          group_by: ["service"],
        }),
        expect.anything(),
      );
    });
    expect(
      await screen.findByRole("rowheader", { name: /api/ }),
    ).toBeInTheDocument();
    expect(screen.getByLabelText(/12건/)).toBeInTheDocument();
  });

  it("shows a permission failure instead of an empty result", async () => {
    vi.spyOn(endpoints, "aggregate").mockRejectedValue(
      new ApiError(403, {
        error: {
          code: "search_access_denied",
          message: "denied",
          request_id: "request-1",
          retryable: false,
        },
      }),
    );
    renderPage();

    expect(
      await screen.findByText("선택한 프로젝트의 로그를 볼 권한이 없습니다."),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("조건에 맞는 record가 없습니다."),
    ).not.toBeInTheDocument();
  });
});
