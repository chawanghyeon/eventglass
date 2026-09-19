import { describe, expect, it, vi } from "vitest";

import type { SearchResult } from "../../api/types";
import { mergeLiveRows, parseSSE, streamLive } from "./live";

type Row = SearchResult["rows"][number];

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
