import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { Session, SystemStatus } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { SystemPage } from "./SystemPage";

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
