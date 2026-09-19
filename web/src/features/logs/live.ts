import type { Kind, SearchResult } from "../../api/types";

export type LiveRow = SearchResult["rows"][number];

export interface LiveRequest {
  tenantID: string;
  projectIDs: string[];
  kinds: Kind[];
  expression: string;
  catchupStartUS?: string;
}

export interface LiveEvent {
  type: "rows" | "checkpoint" | "heartbeat" | "resync_required" | "error";
  id?: string;
  data: unknown;
}

const terminalEvents = new Set<LiveEvent["type"]>(["resync_required", "error"]);

export async function streamLive(request: LiveRequest, signal: AbortSignal, receive: (event: LiveEvent) => void): Promise<void> {
  let resume = "";
  while (!signal.aborted) {
    const params = new URLSearchParams({ tenant_id: request.tenantID });
    for (const projectID of request.projectIDs) params.append("project_ids", projectID);
    for (const kind of request.kinds) params.append("kinds", kind);
    if (request.expression.trim()) params.set("expression", request.expression.trim());
    if (request.catchupStartUS && !resume) params.set("catchup_start_us", request.catchupStartUS);
    const response = await fetch(`/v1/live?${params}`, {
      credentials: "include",
      headers: resume ? { "Last-Event-ID": resume } : undefined,
      signal,
    });
    if (!response.ok || !response.body) throw new Error(`Live stream failed (${response.status})`);
    let terminal = false;
    for await (const event of parseSSE(response.body, signal)) {
      if (event.id) resume = event.id;
      receive(event);
      if (terminalEvents.has(event.type)) {
        terminal = true;
        break;
      }
    }
    if (terminal || signal.aborted) return;
    await abortableDelay(250, signal);
  }
}

export async function* parseSSE(stream: ReadableStream<Uint8Array>, signal?: AbortSignal): AsyncGenerator<LiveEvent> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (!signal?.aborted) {
      const { done, value } = await reader.read();
      buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
      let boundary: number;
      while ((boundary = buffer.indexOf("\n\n")) >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const event = decodeSSEBlock(block);
        if (event) yield event;
      }
      if (done) return;
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function decodeSSEBlock(block: string): LiveEvent | null {
  let type = "", id: string | undefined;
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (!line || line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    const value = separator < 0 ? "" : line.slice(separator + 1).replace(/^ /, "");
    if (field === "event") type = value;
    else if (field === "id" && !value.includes("\0")) id = value;
    else if (field === "data") data.push(value);
  }
  if (!isLiveEventType(type) || data.length === 0) return null;
  return { type, id, data: JSON.parse(data.join("\n")) as unknown };
}

function isLiveEventType(value: string): value is LiveEvent["type"] {
  return value === "rows" || value === "checkpoint" || value === "heartbeat" || value === "resync_required" || value === "error";
}

export function rowsFromLiveEvent(event: LiveEvent): LiveRow[] {
  if (event.type !== "rows" || typeof event.data !== "object" || event.data === null || !("rows" in event.data) || !Array.isArray(event.data.rows)) return [];
  return event.data.rows as LiveRow[];
}

export function mergeLiveRows(current: LiveRow[], incoming: LiveRow[], limit = 1000): LiveRow[] {
  const unique = new Map<string, LiveRow>();
  for (const row of [...current, ...incoming]) {
    if (typeof row.record_id === "string" && !unique.has(row.record_id)) unique.set(row.record_id, row);
  }
  return [...unique.values()].sort((left, right) => {
    const leftTime = BigInt(left.received_time_us), rightTime = BigInt(right.received_time_us);
    if (leftTime !== rightTime) return leftTime > rightTime ? -1 : 1;
    return left.record_id < right.record_id ? -1 : left.record_id > right.record_id ? 1 : 0;
  }).slice(0, limit);
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      window.clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    }, { once: true });
  });
}
