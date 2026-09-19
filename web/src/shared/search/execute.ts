import { api } from "../../api/client";
import type { QueryJob, SearchRequest, SearchResult } from "../../api/types";

export async function executeSearch(request: SearchRequest, csrf: string, signal: AbortSignal): Promise<SearchResult> {
  let job: QueryJob | undefined;
  try {
    const submitted = await api.search(request, signal);
    if (isSearchResult(submitted)) return submitted;
    job = submitted;
    let delay = Math.max(100, job.poll_after_ms);
    while (!signal.aborted) {
      await abortableDelay(delay, signal);
      job = await api.queryJob(request.tenant_id, job.query_id, signal);
      if (job.state === "succeeded" && isSearchResult(job.result)) return job.result;
      if (job.state === "failed" || job.state === "canceled") throw new Error(job.error?.message ?? "Query failed");
      delay = Math.min(2000, Math.ceil(delay * 1.5));
    }
    throw signal.reason;
  } finally {
    if (signal.aborted && job && !["succeeded", "failed", "canceled"].includes(job.state)) {
      void api.cancelQuery(request.tenant_id, job.query_id, csrf).catch(() => undefined);
    }
  }
}

export function isSearchResult(value: unknown): value is SearchResult {
  return typeof value === "object" && value !== null && Array.isArray((value as SearchResult).rows) && typeof (value as SearchResult).read_token === "string";
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = window.setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      window.clearTimeout(timer);
      reject(signal.reason);
    }, { once: true });
  });
}
