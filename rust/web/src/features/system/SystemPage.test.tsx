import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren, ReactNode } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { Session, SystemStatus } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { SystemPage } from "./SystemPage";
import { SystemReadiness } from "./SystemReadiness";

const session: Session = {
  id: "1",
  email: "admin@example.test",
  role: "admin",
  csrf_token: "csrf",
};
const status: SystemStatus = {
  replay: {
    active_replays: "0",
    partial_replays: "0",
    expired_replays: "17",
    segments: "0",
    referenced_bytes: "0",
    backup_pending: false,
  },
  replay_maintenance: {
    state: "busy",
    last_success_us: 1788951797000000,
    deleted_files: 2,
    deleted_bytes: 1024,
  },
  sentry_ingest_since_start: { accepted: "12", too_large: "3" },
  version: "0.1.0",
  ready: true,
  ingest_accepting: true,
  installation_id: "installation",
  storage_generation: "generation",
  applied_inbox_id: "9",
  applied_ingest_seq: "100",
  inbox_records: "3",
  inbox_bytes: "4096",
  database_bytes: "8192",
  wal_bytes: "2048",
  disk: {
    total_bytes: "1073741824",
    free_bytes: "805306368",
    reserved_bytes: "0",
    minimum_free_bytes: "536870912",
    ingest_accepting: true,
  },
  shards: {
    active: "1",
    local: "2",
    remote_verified: "3",
    remote_only: "4",
    records: "100",
    catalog_bytes: "8192",
    recoverable_records: "70",
  },
  backup: {
    configured: true,
    state: "lagging",
    latest_checkpoint_id: "checkpoint",
    recoverable_through_ingest_seq: "70",
    lag_records: "30",
  },
  alerts: {
    pending_deliveries: "1",
    failed_deliveries: "2",
    evaluation_failures: "0",
  },
};

afterEach(() => vi.restoreAllMocks());

it("distinguishes local archives from recoverable shards and runs doctor", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "systemEfficiency").mockRejectedValue(
    new Error("optional observation unavailable"),
  );
  vi.spyOn(endpoints, "systemStatus").mockResolvedValue(status);
  const doctor = vi.spyOn(endpoints, "systemDoctor").mockResolvedValue({
    ok: true,
    schema_version: "1",
    installation_id: "installation",
    storage_generation: "generation",
    checked_local_shards: "6",
    checked_remote_only_shards: "4",
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
  }
  render(<SystemPage />, { wrapper: Wrapper });

  expect(await screen.findByText("lagging")).toBeInTheDocument();
  expect(screen.getByText(/만료 정리 대기 17/)).toBeInTheDocument();
  expect(screen.getByText(/로컬 보존 정리: busy/)).toBeInTheDocument();
  expect(screen.getByText("too_large")).toBeInTheDocument();
  expect(screen.getByText("로컬 전용 archive")).toBeInTheDocument();
  expect(screen.getByText("원격 복구 검증 + 로컬")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "검사 실행" }));
  expect(
    await screen.findByText(/local 6, remote-only 4 검사 통과/),
  ).toBeInTheDocument();
  expect(doctor).toHaveBeenCalled();
});

function showSystem(content: ReactNode, user: Session | null = session) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, user);
  render(<QueryClientProvider client={client}>{content}</QueryClientProvider>);
  return client;
}
it.each([null, { ...session, role: "member" as const }])(
  "does not request admin status for an unauthorized session",
  (user) => {
    const request = vi.spyOn(endpoints, "systemStatus");
    showSystem(<SystemPage />, user);
    expect(screen.getByRole("alert")).toHaveTextContent(
      "관리자만 볼 수 있습니다",
    );
    expect(request).not.toHaveBeenCalled();
  },
);
it("distinguishes status errors, an unready local server, and failed doctor diagnostics", async () => {
  const user = userEvent.setup();
  const request = vi
    .spyOn(endpoints, "systemStatus")
    .mockRejectedValueOnce(new Error("offline"));
  vi.spyOn(endpoints, "systemEfficiency").mockImplementation(
    () => new Promise(() => {}),
  );
  showSystem(<SystemPage />);
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인",
  );
  request.mockResolvedValue({
    ...status,
    ready: false,
    ingest_accepting: false,
    inbox_bytes: "unknown",
    replay: { ...status.replay, backup_pending: true },
    replay_maintenance: null,
    backup: {
      ...status.backup,
      recoverable_through_ingest_seq: null,
      lag_records: null,
    },
  });
  await user.click(screen.getByRole("button", { name: "새로고침" }));
  expect(await screen.findByText("unavailable")).toBeVisible();
  expect(screen.getByText("완료된 복구 지점 없음")).toBeVisible();
  expect(screen.getByText("unknown")).toBeVisible();
  expect(screen.getByText(/로컬 보존 정리: 중지/)).toBeVisible();
  let reject!: (error: Error) => void;
  vi.spyOn(endpoints, "systemDoctor").mockImplementationOnce(
    () =>
      new Promise((_, fail) => {
        reject = fail;
      }),
  );
  await user.click(screen.getByRole("button", { name: "검사 실행" }));
  expect(await screen.findByText("DB와 shard 검사 중")).toBeVisible();
  await act(async () => reject(new Error("doctor failed")));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.queryByText(/검사 통과/)).not.toBeInTheDocument();
});
it("shows readiness as admin-only information", () => {
  const request = vi.spyOn(endpoints, "systemStatus");
  showSystem(<SystemReadiness admin={false} userId="7" />);
  expect(screen.getByText("관리 기능 제한됨")).toBeVisible();
  expect(request).not.toHaveBeenCalled();
});
it("does not label a pending or failed readiness request as ready", async () => {
  let reject!: (error: Error) => void;
  vi.spyOn(endpoints, "systemStatus").mockImplementationOnce(
    () =>
      new Promise((_, fail) => {
        reject = fail;
      }),
  );
  showSystem(<SystemReadiness admin userId="7" />);
  expect(screen.getByText("시스템 상태 확인 중…")).toBeVisible();
  await act(async () => reject(new Error("offline")));
  expect(await screen.findByText("상태 확인 실패")).toBeVisible();
  expect(screen.queryByText("수집 준비됨")).not.toBeInTheDocument();
});
it.each([true, false])(
  "reflects server readiness without requiring a setup action (ready=%s)",
  async (ready) => {
    vi.spyOn(endpoints, "systemStatus").mockResolvedValue({ ...status, ready });
    showSystem(<SystemReadiness admin userId="7" />);
    expect(
      await screen.findByText(ready ? "수집 준비됨" : "Indexer 준비 중"),
    ).toBeVisible();
  },
);
