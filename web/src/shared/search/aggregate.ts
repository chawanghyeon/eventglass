import { api } from "../../api/client";
import type { AggregateRequest, AggregateResult, QueryJob, SearchRequest } from "../../api/types";
import { abortableDelay } from "./execute";

export function histogramRequest(rows: SearchRequest, readToken: string): AggregateRequest {
  const { cursor: _cursor, limit: _limit, projection: _projection, sort: _sort, ...dataset } = rows;
  return {
    ...dataset,
    read_token: readToken,
    metrics: [{ name: "events", op: "count" }],
    group_by: [],
    histogram: { interval: "1m", empty_buckets: true },
    top: 1000,
    order: { metric: "count", direction: "desc" },
    mode: "auto",
  };
}

export async function executeAggregate(request: AggregateRequest, csrf: string, signal: AbortSignal): Promise<AggregateResult> {
  let job: QueryJob | undefined;
  try {
    const submitted = await api.aggregate(request, signal);
    if (isAggregateResult(submitted)) return submitted;
    job = submitted;
    let delay = Math.max(100, job.poll_after_ms);
    while (!signal.aborted) {
      await abortableDelay(delay, signal);
      job = await api.queryJob(request.tenant_id, job.query_id, signal);
      if (job.state === "succeeded" && isAggregateResult(job.result)) return job.result;
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

export function isAggregateResult(value: unknown): value is AggregateResult {
  return typeof value === "object" && value !== null && Array.isArray((value as AggregateResult).groups) && typeof (value as AggregateResult).read_token === "string";
}
