import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { Alert, AlertDelivery, Session } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { AlertsPage } from "./AlertsPage";

const session: Session = {
  id: "1",
  email: "admin@example.test",
  role: "admin",
  csrf_token: "csrf",
};

function renderPage() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
  }
  return render(<AlertsPage />, { wrapper: Wrapper });
}

afterEach(() => vi.restoreAllMocks());

it("creates rules and exposes failed delivery retry with evaluation state", async () => {
  const alert: Alert = {
    id: "1",
    name: "API errors",
    project_id: null,
    revision: 0,
    condition: {
      type: "error_count",
      query: "service:api",
      window_seconds: 300,
      threshold: 5,
      cooldown_seconds: 600,
      time_basis: "received_at",
    },
    destination: { type: "webhook", url: "https://hooks.example.test" },
    enabled: true,
    last_evaluation_error: "query_incomplete",
    last_evaluation_watermark: "9",
    pending_evaluation_end_us: "1788825600000000",
    pending_cut_seq: "10",
    last_evaluated_at_us: null,
    last_triggered_at_us: null,
    created_at_us: "1788825500000000",
    updated_at_us: "1788825500000000",
  };
  const delivery: AlertDelivery = {
    id: "a".repeat(64),
    alert_id: "1",
    payload: {},
    state: "failed",
    sent_at_us: null,
    last_status_code: 500,
    attempts: 12,
    next_retry_at_us: "1788825600000000",
    created_at_us: "1788825600000000",
    last_error: "webhook_retryable_status",
  };
  vi.spyOn(endpoints, "alerts").mockResolvedValue([alert]);
  vi.spyOn(endpoints, "alertDeliveries").mockResolvedValue([delivery]);
  vi.spyOn(endpoints, "projects").mockResolvedValue([]);
  const create = vi
    .spyOn(endpoints, "createAlert")
    .mockResolvedValue({ id: "2" });
  const update = vi.spyOn(endpoints, "updateAlert").mockResolvedValue(alert);
  const retry = vi
    .spyOn(endpoints, "retryAlertDelivery")
    .mockResolvedValue(undefined);
  const user = userEvent.setup();
  renderPage();

  expect(
    await screen.findByText("평가 실패: query_incomplete"),
  ).toBeInTheDocument();
  await user.type(screen.getByLabelText("이름"), "New issues");
  await user.type(
    screen.getByLabelText("HTTPS webhook"),
    "https://hooks.example.test/new",
  );
  await user.click(screen.getByRole("button", { name: "경보 추가" }));
  await waitFor(() =>
    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "New issues",
        condition: { type: "new_issue" },
      }),
    ),
  );
  await user.click(screen.getByRole("button", { name: "같은 ID로 재시도" }));
  expect(retry).toHaveBeenCalledWith(delivery.id);
  await user.click(screen.getByRole("button", { name: "수정" }));
  expect(screen.getByLabelText("이름")).toHaveValue("API errors");
  await user.clear(screen.getByLabelText("이름"));
  await user.type(screen.getByLabelText("이름"), "Updated errors");
  await user.click(screen.getByRole("button", { name: "변경 저장" }));
  await waitFor(() =>
    expect(update).toHaveBeenCalledWith(
      alert.id,
      expect.objectContaining({
        name: "Updated errors",
        revision: alert.revision,
        condition: alert.condition,
        enabled: true,
      }),
    ),
  );
});
