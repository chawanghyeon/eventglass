import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { useLiveLogs } from "./useLiveLogs";

class FakeEventSource {
  static CLOSED = 2;
  readyState = 0;
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

it("reports permanently closed HTTP streams as errors instead of endless reconnecting", () => {
  const { result } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=closed"),
  );
  const source = FakeEventSource.instances[0];
  source.readyState = FakeEventSource.CLOSED;
  act(() => {
    for (const listener of source.listeners.get("error") ?? [])
      listener(new Event("error") as MessageEvent);
  });
  expect(result.current.status).toBe("error");
  expect(result.current.errorCode).toBe("live_connection_closed");
  expect(source.closed).toBe(true);
});

const validRow = {
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
function raw(source: FakeEventSource, name: string, data: string) {
  for (const listener of source.listeners.get(name) ?? [])
    listener(new MessageEvent(name, { data }));
}
function networkError(source: FakeEventSource) {
  for (const listener of source.listeners.get("error") ?? [])
    listener(new Event("error") as MessageEvent);
}
it("stays idle without a URL and closes an active stream when Live is disabled", () => {
  const { result, rerender } = renderHook(
    ({ url }: { url?: string }) => useLiveLogs(url),
    { initialProps: {} },
  );
  expect(result.current).toEqual({ status: "idle", rows: [] });
  expect(FakeEventSource.instances).toHaveLength(0);
  rerender({ url: "/api/logs/live?scope=one" });
  const source = FakeEventSource.instances[0];
  act(() => source.onopen?.());
  rerender({});
  expect(source.closed).toBe(true);
  expect(result.current).toEqual({ status: "idle", rows: [] });
});
it("retains only 500 rows and preserves them across native EventSource reconnects", () => {
  const { result } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=bounded"),
  );
  const source = FakeEventSource.instances[0];
  act(() => {
    source.onopen?.();
    for (let seq = 0; seq < 501; seq++)
      source.emit("record", {
        ...validRow,
        record_id: seq.toString(16).padStart(64, "0"),
        ingest_seq: String(seq),
      });
    source.emit("checkpoint", { scan_seq: "9007199254740993" });
  });
  expect(result.current.rows).toHaveLength(500);
  expect(result.current.rows[0].ingest_seq).toBe("1");
  act(() => networkError(source));
  expect(result.current.status).toBe("reconnecting");
  expect(source.closed).toBe(false);
  act(() => source.onopen?.());
  expect(result.current.status).toBe("open");
  expect(result.current.rows).toHaveLength(500);
  expect(result.current.checkpoint).toBe("9007199254740993");
});
it.each([
  null,
  "invalid",
  {},
  { ...validRow, record_id: 7 },
  { ...validRow, ingest_seq: 7 },
  { ...validRow, timestamp: 7 },
  { ...validRow, received_at: 7 },
])(
  "closes malformed record streams instead of presenting invalid rows (%j)",
  (value) => {
    const { result } = renderHook(() =>
      useLiveLogs("/api/logs/live?scope=invalid"),
    );
    const source = FakeEventSource.instances[0];
    act(() => source.emit("record", value));
    expect(result.current.errorCode).toBe("invalid_live_record");
    expect(source.closed).toBe(true);
    expect(result.current.rows).toEqual([]);
  },
);
it.each(["record", "checkpoint"])("reports non-JSON %s events", (name) => {
  const { result } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=bad-json"),
  );
  const source = FakeEventSource.instances[0];
  act(() => raw(source, name, "not JSON"));
  expect(result.current.errorCode).toBe(`invalid_live_${name}`);
  expect(source.closed).toBe(true);
});
it("rejects a numeric checkpoint rather than losing decimal precision", () => {
  const { result } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=checkpoint"),
  );
  const source = FakeEventSource.instances[0];
  act(() => source.emit("checkpoint", { scan_seq: 7 }));
  expect(result.current.errorCode).toBe("invalid_live_checkpoint");
  expect(result.current.checkpoint).toBeUndefined();
});
it.each([
  { data: '{"code":"search_access_denied"}', code: "search_access_denied" },
  { data: '{"code":7}', code: "live_unavailable" },
  { data: "bad JSON", code: "live_unavailable" },
])("retains server stream errors (%s)", ({ data, code }) => {
  const { result } = renderHook(() =>
    useLiveLogs("/api/logs/live?scope=error"),
  );
  const source = FakeEventSource.instances[0];
  act(() => raw(source, "error", data));
  expect(result.current.errorCode).toBe(code);
  expect(result.current.status).toBe("error");
  expect(source.closed).toBe(true);
});
it("does not accept queued events from a closed scope into the new scope", () => {
  const { result, rerender } = renderHook(({ url }) => useLiveLogs(url), {
    initialProps: { url: "/api/logs/live?scope=old" },
  });
  const old = FakeEventSource.instances[0];
  act(() => {
    old.onopen?.();
    old.emit("record", validRow);
  });
  rerender({ url: "/api/logs/live?scope=new" });
  const current = FakeEventSource.instances[1];
  expect(result.current.rows).toEqual([]);
  act(() => current.emit("record", { ...validRow, record_id: "b".repeat(64) }));
  const before = result.current;
  act(() => {
    old.onopen?.();
    old.emit("record", validRow);
    old.emit("checkpoint", { scan_seq: "999" });
    old.emit("resync_required");
    networkError(old);
    raw(old, "error", "bad JSON");
  });
  expect(result.current).toBe(before);
  expect(result.current.rows[0].record_id).toBe("b".repeat(64));
  expect(current.closed).toBe(false);
});
