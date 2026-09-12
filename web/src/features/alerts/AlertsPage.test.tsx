import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
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

function renderPage(user: Session | null = session) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, user);
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

const storedAlert: Alert = {
  id: "1",
  name: "Existing",
  project_id: "2",
  revision: 3,
  condition: { type: "regression" },
  destination: { type: "webhook", url: "https://example.test/hook" },
  enabled: true,
  last_evaluation_error: null,
  last_evaluation_watermark: "8",
  pending_evaluation_end_us: null,
  pending_cut_seq: null,
  last_evaluated_at_us: "1788825500000000",
  last_triggered_at_us: "1788825500000000",
  created_at_us: "1788825500000000",
  updated_at_us: "1788825500000000",
};
function loadAlerts(items: Alert[] = []) {
  const alerts = vi.spyOn(endpoints, "alerts").mockResolvedValue(items);
  vi.spyOn(endpoints, "alertDeliveries").mockResolvedValue([]);
  vi.spyOn(endpoints, "projects").mockResolvedValue([
    { id: "2", slug: "api", name: "API", is_active: true },
  ]);
  return alerts;
}
it.each([null, { ...session, role: "member" as const }])(
  "does not load alert configuration for unauthorized users",
  (user) => {
    const alerts = vi.spyOn(endpoints, "alerts");
    renderPage(user);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "관리자만 볼 수 있습니다",
    );
    expect(alerts).not.toHaveBeenCalled();
  },
);
it("preserves all threshold settings and reports failed saves without discarding the form", async () => {
  const user = userEvent.setup();
  loadAlerts();
  let reject!: (error: Error) => void;
  const create = vi.spyOn(endpoints, "createAlert").mockImplementationOnce(
    () =>
      new Promise((_, fail) => {
        reject = fail;
      }),
  );
  renderPage();
  expect(await screen.findByText("설정된 경보가 없습니다.")).toBeVisible();
  await user.selectOptions(screen.getByLabelText("프로젝트"), "2");
  await user.selectOptions(screen.getByLabelText("프로젝트"), "");
  await user.selectOptions(screen.getByLabelText("프로젝트"), "2");
  await user.selectOptions(screen.getByLabelText("조건"), "error_count");
  await user.selectOptions(screen.getByLabelText("조건"), "regression");
  expect(screen.queryByLabelText("Query")).not.toBeInTheDocument();
  await user.selectOptions(screen.getByLabelText("조건"), "log_count");
  await user.type(screen.getByLabelText("이름"), "Failures");
  await user.type(
    screen.getByLabelText("HTTPS webhook"),
    "https://example.test/hook",
  );
  await user.type(screen.getByLabelText("Query"), "level:error");
  fireEvent.change(screen.getByLabelText("Window (초)"), {
    target: { value: "120" },
  });
  fireEvent.change(screen.getByLabelText("임계값"), { target: { value: "5" } });
  fireEvent.change(screen.getByLabelText("Cooldown (초)"), {
    target: { value: "60" },
  });
  await user.selectOptions(screen.getByLabelText("시간 기준"), "timestamp");
  await user.click(screen.getByRole("button", { name: "경보 추가" }));
  expect(
    await screen.findByRole("button", { name: "저장 중…" }),
  ).toBeDisabled();
  expect(create).toHaveBeenCalledWith(
    expect.objectContaining({
      project_id: "2",
      condition: {
        type: "log_count",
        query: "level:error",
        window_seconds: 120,
        threshold: 5,
        cooldown_seconds: 60,
        time_basis: "timestamp",
      },
    }),
  );
  await act(async () => reject(new Error("offline")));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.getByLabelText("Query")).toHaveValue("level:error");
});
it("cancels edits and applies revision-checked stop, restart, and delete", async () => {
  const user = userEvent.setup();
  const alerts = loadAlerts([storedAlert]);
  const update = vi
    .spyOn(endpoints, "updateAlert")
    .mockResolvedValue(storedAlert);
  const remove = vi
    .spyOn(endpoints, "deleteAlert")
    .mockResolvedValue(undefined);
  renderPage();
  await user.click(await screen.findByRole("button", { name: "수정" }));
  await user.type(screen.getByLabelText("이름"), " modified");
  await user.click(screen.getByRole("button", { name: "수정 취소" }));
  expect(screen.getByLabelText("이름")).toHaveValue("");
  expect(update).not.toHaveBeenCalled();
  alerts.mockResolvedValue([{ ...storedAlert, enabled: false, revision: 4 }]);
  await user.click(screen.getByRole("button", { name: "중지" }));
  expect(await screen.findByRole("button", { name: "활성화" })).toBeEnabled();
  expect(update).toHaveBeenLastCalledWith(
    "1",
    expect.objectContaining({ enabled: false, revision: 3 }),
  );
  alerts.mockResolvedValue([{ ...storedAlert, revision: 5 }]);
  await user.click(screen.getByRole("button", { name: "활성화" }));
  expect(await screen.findByRole("button", { name: "중지" })).toBeEnabled();
  expect(update).toHaveBeenLastCalledWith(
    "1",
    expect.objectContaining({ enabled: true, revision: 4 }),
  );
  alerts.mockResolvedValue([]);
  await user.click(screen.getByRole("button", { name: "삭제" }));
  expect(await screen.findByText("설정된 경보가 없습니다.")).toBeVisible();
  expect(remove).toHaveBeenCalledWith("1", 5);
});
it.each(["alerts", "deliveries", "projects"])(
  "reports an unavailable %s collection",
  async (target) => {
    loadAlerts();
    if (target === "alerts")
      vi.mocked(endpoints.alerts).mockRejectedValue(new Error("offline"));
    if (target === "deliveries")
      vi.mocked(endpoints.alertDeliveries).mockRejectedValue(
        new Error("offline"),
      );
    if (target === "projects")
      vi.mocked(endpoints.projects).mockRejectedValue(new Error("offline"));
    renderPage();
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "연결 상태를 확인해 주세요",
    );
  },
);
