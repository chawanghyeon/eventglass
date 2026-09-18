import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { FeedbackPanel } from "./FeedbackPanel";
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  vi.restoreAllMocks();
});
function show() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <FeedbackPanel user="7" project="1" />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
it("shows pending feedback without claiming that none was received", () => {
  vi.spyOn(endpoints, "feedback").mockImplementation(
    () => new Promise(() => {}),
  );
  show();
  expect(screen.getByText("Feedback을 불러오는 중…")).toBeVisible();
  expect(
    screen.queryByText("수신한 Sentry Feedback이 없습니다."),
  ).not.toBeInTheDocument();
});
it("distinguishes failed and empty feedback", async () => {
  const feedback = vi
    .spyOn(endpoints, "feedback")
    .mockRejectedValueOnce(new Error("offline"));
  show();
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  feedback.mockResolvedValue({ items: [] });
  show();
  expect(
    await screen.findByText("수신한 Sentry Feedback이 없습니다."),
  ).toBeVisible();
});
it("links only validated replay identities and renders malformed contexts without crashing", async () => {
  const id = "a".repeat(32);
  const feedback = vi.spyOn(endpoints, "feedback").mockResolvedValue({
    items: [
      {},
      { contexts: "invalid" },
      { contexts: {} },
      { contexts: { feedback: "invalid" } },
      { contexts: { feedback: { replay_id: 7 } } },
      { contexts: { feedback: { replay_id: "../../other" } } },
      {
        event_id: "event1",
        contexts: { feedback: { replay_id: id, message: "Checkout failed" } },
      },
    ],
  });
  show();
  expect(await screen.findByText("Checkout failed")).toBeVisible();
  expect(screen.getAllByRole("link")).toHaveLength(1);
  expect(screen.getByRole("link", { name: "관련 Replay" })).toHaveAttribute(
    "href",
    `/replays/1/${id}`,
  );
  expect(feedback).toHaveBeenCalledWith("1", expect.any(AbortSignal));
});
