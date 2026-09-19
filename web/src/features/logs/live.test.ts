import { afterEach, describe, expect, it, vi } from "vitest";

import type { SearchResult } from "../../api/types";
import { mergeLiveRows, parseSSE, streamLive } from "./live";

type Row = SearchResult["rows"][number];
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

function row(id: string, received: string): Row {
  return { record_id: id.repeat(64), project_id: "1", kind: "log", event_time_us: received, event_time_ns_remainder: 0, received_time_us: received, level: "info", message: id, message_truncated: false };
}

function body(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
}

describe("live stream", () => {
  it("backs off retryable HTTP failures without losing the checkpoint", async () => {
    vi.useFakeTimers();
    const mock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(body(["id: retained\nevent: checkpoint\ndata: {}\n\n"])))
      .mockResolvedValueOnce(new Response("unavailable", { status: 503 }))
      .mockResolvedValueOnce(new Response(body(["event: error\ndata: {\"code\":\"forbidden\"}\n\n"])));
    const running = streamLive({ tenantID: "1", projectIDs: ["2"], kinds: ["log"], expression: "" }, new AbortController().signal, () => undefined);
    await vi.advanceTimersByTimeAsync(1251); await running;
    expect(mock).toHaveBeenCalledTimes(3);
    expect(mock.mock.calls[2][1]?.headers).toEqual({ "Last-Event-ID": "retained" });
  });
  it("reconnects using the last emitted checkpoint, then stops on resync", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(body(["id: resume-inside-batch\nevent: rows\ndata: {\"rows\":[]}\n\n"])))
      .mockResolvedValueOnce(new Response(body(["event: resync_required\ndata: {\"code\":\"checkpoint_expired\"}\n\n"])));
    const running = streamLive({ tenantID: "1", projectIDs: ["2"], kinds: ["log"], expression: "" }, new AbortController().signal, () => undefined);
    await vi.advanceTimersByTimeAsync(251);
    await running;
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1][1]?.headers).toEqual({ "Last-Event-ID": "resume-inside-batch" });
  });

  it("rejects an unterminated oversized frame and cancels an idle reader", async () => {
    const read = async () => { for await (const _ of parseSSE(body(["x".repeat(256 * 1024 + 1)]))) { /* consume */ } };
    await expect(read()).rejects.toThrow("buffer limit");
    const cancel = vi.fn();
    const controller = new AbortController();
    const stream = new ReadableStream<Uint8Array>({ cancel });
    const iterator = parseSSE(stream, controller.signal);
    const pending = iterator.next();
    controller.abort();
    expect((await pending).done).toBe(true);
    expect(cancel).toHaveBeenCalledOnce();
  });
  it("parses events split across transport chunks", async () => {
    const events = [];
    for await (const event of parseSSE(body(["id: token\nevent: ro", "ws\ndata: {\"rows\":[]}\n\n"]))) events.push(event);
    expect(events).toEqual([{ type: "rows", id: "token", data: { rows: [] } }]);
  });

  it("deduplicates replayed records, sorts across lanes, and keeps a bounded list", () => {
    const merged = mergeLiveRows([row("a", "1"), row("b", "2")], [row("a", "1"), row("c", "3")], 2);
    expect(merged.map((item) => item.message)).toEqual(["c", "b"]);
  });

  it("does not reconnect after a forbidden terminal event", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(body(["event: error\ndata: {\"code\":\"forbidden\",\"retryable\":false}\n\n"]), { status: 200 }));
    const received: string[] = [];
    await streamLive({ tenantID: "1", projectIDs: ["2"], kinds: ["log"], expression: "" }, new AbortController().signal, (event) => received.push(event.type));
    expect(received).toEqual(["error"]);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
