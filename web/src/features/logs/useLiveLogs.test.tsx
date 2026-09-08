import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { useLiveLogs } from "./useLiveLogs";

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  onopen: (() => void) | null = null;
  closed = false;
  readonly listeners = new Map<string, Array<(event: MessageEvent) => void>>();

  constructor(
    readonly url: string,
    readonly options: EventSourceInit,
  ) {
    FakeEventSource.instances.push(this);
  }

  addEventListener(name: string, listener: EventListenerOrEventListenerObject) {
    const callback = listener as (event: MessageEvent) => void;
    this.listeners.set(name, [...(this.listeners.get(name) ?? []), callback]);
  }

  emit(name: string, data: unknown = {}) {
    const event = new MessageEvent(name, { data: JSON.stringify(data) });
    for (const listener of this.listeners.get(name) ?? []) listener(event);
  }

  close() {
    this.closed = true;
  }
}

beforeEach(() => {
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
});

afterEach(() => vi.unstubAllGlobals());

it("owns connection cleanup, record dedupe, checkpoints, and resync closure", () => {
  const { result, unmount } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=fixed"),
  );
  const source = FakeEventSource.instances[0];
  expect(source.options.withCredentials).toBe(true);
  expect(result.current.status).toBe("connecting");

  act(() => source.onopen?.());
  const row = {
    record_id: "a".repeat(64),
    kind: "log",
    project_id: "1",
    ingest_seq: "7",
    timestamp: "2026-09-08T00:00:00Z",
    received_at: "2026-09-08T00:00:01Z",
    service: "api",
    level: "info",
    message: "received",
    environment: null,
    release: null,
    logger: null,
    trace_id: null,
    span_id: null,
    request_id: null,
    issue_id: null,
    fingerprint: null,
    user_id: null,
    user_email: null,
    detail_token: "detail",
  };
  act(() => {
    source.emit("record", row);
    source.emit("record", row);
    source.emit("checkpoint", { scan_seq: "7" });
  });
  expect(result.current.rows).toHaveLength(1);
  expect(result.current.checkpoint).toBe("7");

  act(() => source.emit("resync_required"));
  expect(result.current.status).toBe("resync_required");
  expect(source.closed).toBe(true);
  unmount();
  expect(source.closed).toBe(true);
});
