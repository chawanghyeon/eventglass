import { QueryClient } from "@tanstack/react-query";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import {
  analysisQuery,
  mapsQuery,
  recordingQuery,
  replayQuery,
  replaysQuery,
} from "./queries";

const clients: QueryClient[] = [];
function client() {
  const value = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(value);
  return value;
}
afterEach(() => {
  clients.splice(0).forEach((value) => value.clear());
  vi.restoreAllMocks();
});

it("separates private replay results by user and forwards request cancellation", async () => {
  const store = client();
  const list = vi
    .spyOn(endpoints, "replays")
    .mockResolvedValue({ items: [], next_cursor: null });
  await store.fetchQuery(replaysQuery("alice", "project_id=1"));
  await store.fetchQuery(replaysQuery("alice", "project_id=1"));
  await store.fetchQuery(replaysQuery("bob", "project_id=1"));
  expect(list).toHaveBeenCalledTimes(2);
  expect(list).toHaveBeenCalledWith("project_id=1", expect.any(AbortSignal));
  const detail = vi
    .spyOn(endpoints, "replay")
    .mockRejectedValue(new Error("detail denied"));
  const analysis = vi
    .spyOn(endpoints, "replayAnalysis")
    .mockRejectedValue(new Error("analysis denied"));
  const maps = vi
    .spyOn(endpoints, "replayMaps")
    .mockRejectedValue(new Error("maps denied"));
  await expect(
    store.fetchQuery(replayQuery("alice", "1", "session")),
  ).rejects.toThrow("detail denied");
  await expect(
    store.fetchQuery(analysisQuery("alice", "1", "session")),
  ).rejects.toThrow("analysis denied");
  await expect(
    store.fetchQuery(mapsQuery("alice", "project_id=1")),
  ).rejects.toThrow("maps denied");
  expect(detail).toHaveBeenCalledWith("1", "session", expect.any(AbortSignal));
  expect(analysis).toHaveBeenCalledWith(
    "1",
    "session",
    expect.any(AbortSignal),
  );
  expect(maps).toHaveBeenCalledWith("project_id=1", expect.any(AbortSignal));
});

it("assembles one ordered playback buffer, reports segment gaps, and discards malformed events", async () => {
  const recording = vi.spyOn(endpoints, "replayRecording");
  recording
    .mockResolvedValueOnce({
      events: [
        { type: 5, timestamp: 20, data: {} },
        { type: "3", timestamp: 1, data: {} },
        { type: -1, timestamp: 1, data: {} },
        { type: 6, timestamp: 1, data: {} },
        { type: 3, timestamp: "1", data: {} },
        { type: 3, timestamp: NaN, data: {} },
        { type: 3, timestamp: 1, data: null },
        { type: 3, timestamp: 1, data: "invalid" },
      ],
    })
    .mockResolvedValueOnce({ events: [{ type: 0, timestamp: 10, data: {} }] });
  const store = client();
  const options = recordingQuery("alice", "1", "session", [
    { segment_id: 0 },
    { segment_id: 2 },
  ]);
  const result = await store.fetchQuery(options);
  expect(result.events.map((event) => event.timestamp)).toEqual([10, 20]);
  expect(result.gaps).toEqual([2]);
  expect(result.truncated).toBe(false);
  expect(recording.mock.calls.map((call) => call[2])).toEqual([0, 2]);
  expect(options.gcTime).toBe(0);
  expect(store.getQueryCache().getAll()).toHaveLength(1);
  expect(
    await store.fetchQuery(recordingQuery("alice", "1", "empty", [])),
  ).toEqual({ events: [], gaps: [], truncated: false });
});

it.each(["events", "bytes"])(
  "stops fetching at the playback %s bound and reports truncation",
  async (bound) => {
    const oversized =
      bound === "events"
        ? Array.from({ length: 100001 }, () => ({}))
        : [{ body: "x".repeat(64 * 1024 * 1024) }];
    const recording = vi
      .spyOn(endpoints, "replayRecording")
      .mockResolvedValue({ events: oversized });
    const result = await client().fetchQuery(
      recordingQuery("alice", "1", "session", [
        { segment_id: 0 },
        { segment_id: 1 },
      ]),
    );
    expect(result).toEqual({ events: [], gaps: [], truncated: true });
    expect(recording).toHaveBeenCalledOnce();
  },
);

it("cancels an in-flight segment without requesting later segments or caching success", async () => {
  const store = client();
  let signal: AbortSignal | undefined;
  const recording = vi
    .spyOn(endpoints, "replayRecording")
    .mockImplementation((_, __, ___, requestSignal) => {
      signal = requestSignal;
      return new Promise((_, reject) =>
        requestSignal?.addEventListener("abort", () =>
          reject(new DOMException("cancelled", "AbortError")),
        ),
      );
    });
  const options = recordingQuery("alice", "1", "session", [
    { segment_id: 0 },
    { segment_id: 1 },
  ]);
  const pending = store.fetchQuery(options);
  const failure = expect(pending).rejects.toBeDefined();
  await store.cancelQueries({ queryKey: options.queryKey });
  await failure;
  expect(signal?.aborted).toBe(true);
  expect(recording).toHaveBeenCalledOnce();
  expect(store.getQueryData(options.queryKey)).toBeUndefined();
});
