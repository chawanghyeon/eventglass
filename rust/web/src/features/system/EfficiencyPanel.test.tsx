import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { EfficiencySnapshot, Session } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { EfficiencyPanel } from "./EfficiencyPanel";

const snapshot: EfficiencySnapshot = {
  process_epoch: "process",
  uptime_ms: "100",
  incomplete: false,
  observed_body_bytes: "300",
  observed_decoded_bytes: "600",
  local_reuses: "2",
  hydrated_shards: "1",
  evicted_shards: "0",
  remote: {
    download: {
      started: "1",
      succeeded: "1",
      failed: "0",
      cancelled: "0",
      completed_read_bytes: "12345",
    },
  },
};

function mount(role?: "admin" | "member") {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  if (role)
    client.setQueryData<Session>(sessionQueryKey, {
      id: "1",
      email: "user@example.test",
      role,
      csrf_token: "csrf",
    });
  else client.setQueryData(sessionQueryKey, null);
  const result = render(
    <QueryClientProvider client={client}>
      <EfficiencyPanel />
    </QueryClientProvider>,
  );
  return { ...result, client };
}

afterEach(() => vi.restoreAllMocks());

it("loads automatically without configuration and preserves exact byte strings", async () => {
  const request = vi
    .spyOn(endpoints, "systemEfficiency")
    .mockResolvedValue(snapshot);
  const mounted = mount("admin");
  expect(screen.getByText("효율 확인 중")).toBeInTheDocument();
  expect(await screen.findByText("12345")).toBeInTheDocument();
  expect(screen.getByText(/입력 body 300 bytes/)).toBeInTheDocument();
  expect(screen.getByText(/로컬 경로 선택 2/)).toBeInTheDocument();
  expect(screen.queryByRole("button")).not.toBeInTheDocument();
  expect(screen.queryByText(/상한에 도달/)).not.toBeInTheDocument();
  expect(request).toHaveBeenCalledTimes(1);
  mounted.unmount();
  mounted.client.clear();
});

it("keeps an efficiency failure inside its panel", async () => {
  vi.spyOn(endpoints, "systemEfficiency").mockRejectedValue(
    new Error("offline"),
  );
  mount("admin");
  expect(await screen.findByRole("alert")).toBeInTheDocument();
  expect(screen.queryByRole("table")).not.toBeInTheDocument();
});

it("displays saturated counters as incomplete", async () => {
  vi.spyOn(endpoints, "systemEfficiency").mockResolvedValue({
    ...snapshot,
    incomplete: true,
    remote: { unknown_operation: snapshot.remote.download },
  });
  mount("admin");
  expect(await screen.findByText(/상한에 도달/)).toBeInTheDocument();
});

it.each(["member", undefined] as const)(
  "does not request installation statistics for %s",
  (role) => {
    const request = vi.spyOn(endpoints, "systemEfficiency");
    const { container } = mount(role);
    expect(container).toBeEmptyDOMElement();
    expect(request).not.toHaveBeenCalled();
  },
);

it("aborts pending observations when the panel leaves the screen", () => {
  let signal: AbortSignal | undefined;
  vi.spyOn(endpoints, "systemEfficiency").mockImplementation((input) => {
    signal = input;
    return new Promise(() => {});
  });
  const result = mount("admin");
  expect(signal?.aborted).toBe(false);
  result.unmount();
  expect(signal?.aborted).toBe(true);
  result.client.clear();
});
