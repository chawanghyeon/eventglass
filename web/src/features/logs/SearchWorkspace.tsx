import { useQuery, useQueryClient } from "@tanstack/react-query";
import { type FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { useSession } from "../../app/providers";
import type { Kind, SearchResult } from "../../api/types";
import { formatInt64 } from "../../shared/format/int64";
import { datasetKey, decodeSearchURL, defaultSearchState, encodeSearchURL, searchRequest, type SearchURLState } from "../../shared/search/url";
import { executeSearch } from "../../shared/search/execute";
import { executeAggregate, histogramRequest } from "../../shared/search/aggregate";
import { cancelDataset, datasetQueryPrefix } from "../../shared/search/lifetime";
import { StatusPanel } from "../../shared/ui/StatusPanel";

export function SearchWorkspace({ title, defaultKinds, histogram = false }: { title: string; defaultKinds: Kind[]; histogram?: boolean }) {
  const { session, tenant } = useSession();
  const [params, setParams] = useSearchParams();
  const fallback = useMemo(() => defaultSearchState(Date.now(), defaultKinds), [defaultKinds]);
  let state: SearchURLState;
  try {
    state = decodeSearchURL(params, fallback);
  } catch {
    state = fallback;
  }
  const [draft, setDraft] = useState(state.expression);
  const [projectDraft, setProjectDraft] = useState(state.projectIDs.join(","));
  const key = datasetKey(tenant.tenant_id, state);
  const previousKey = useRef(key);
  const queryClient = useQueryClient();
  useEffect(() => {
    if (previousKey.current === key) return;
    void cancelDataset(queryClient, session.user_id, tenant.tenant_id, previousKey.current);
    previousKey.current = key;
    setDraft(state.expression);
    setProjectDraft(state.projectIDs.join(","));
  }, [key, queryClient, session.user_id, tenant.tenant_id]);
  const result = useQuery<SearchResult>({
    queryKey: [...datasetQueryPrefix(session.user_id, tenant.tenant_id, key), "new", "rows"],
    queryFn: ({ signal }) => executeSearch(searchRequest(tenant.tenant_id, state), session.csrf_token, signal),
    enabled: state.projectIDs.length > 0,
  });
  const aggregate = useQuery({
    queryKey: [...datasetQueryPrefix(session.user_id, tenant.tenant_id, key), result.data?.read_token ?? "waiting", "histogram"],
    queryFn: ({ signal }) => executeAggregate(histogramRequest(searchRequest(tenant.tenant_id, state), result.data!.read_token), session.csrf_token, signal),
    enabled: histogram && Boolean(result.data?.read_token),
  });
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setParams(encodeSearchURL({ ...state, projectIDs: projectDraft.split(",").map((value) => value.trim()).filter(Boolean), expression: draft }));
  };
  return <section>
    <div className="page-heading"><div><p className="eyebrow">Snapshot-backed query</p><h1>{title}</h1></div><p>URL owns scope and absolute time. Snapshot and cursor tokens stay in memory.</p></div>
    <form className="search-bar" onSubmit={submit}>
      <label>Project IDs<input value={projectDraft} onChange={(event) => setProjectDraft(event.target.value)} inputMode="numeric" pattern="[0-9]+(,[0-9]+)*" placeholder="1,2" /></label>
      <label>Filter expression<input value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="service == 'api'" /></label>
      <button>Search</button>
    </form>
    {!state.projectIDs.length ? <StatusPanel empty="Choose at least one authorized project." /> : null}
    {result.isPending && state.projectIDs.length ? <p role="status">Running query…</p> : null}
    <StatusPanel error={result.error} onRetry={() => void result.refetch()} />
    {result.data?.rows.length === 0 ? <StatusPanel empty="No records match this snapshot." /> : null}
    {result.data ? <QuerySummary result={result.data} /> : null}
    {histogram && aggregate.isPending && result.data ? <p role="status">Loading snapshot histogram…</p> : null}
    {histogram ? <StatusPanel error={aggregate.error} onRetry={() => void aggregate.refetch()} /> : null}
    {histogram && aggregate.data ? <div className="histogram" aria-label="Event histogram">{aggregate.data.groups.map((group, index) => <div key={group.bucket_start_us ?? index} title={`${String(group.bucket_start_us ?? "bucket")}: ${String(group.metrics.events?.value ?? "0")}`} style={{ height: `${barHeight(group.metrics.events?.value)}px` }} />)}</div> : null}
    <div className="stack">{result.data?.rows.map((row) => <article className="record-card" key={row.record_id}>
      <div><span className={`badge level-${row.level}`}>{row.level}</span><span className="muted">{row.kind} · project {formatInt64(row.project_id)}</span></div>
      <p>{row.message}</p>
      <footer><code>{row.service ?? "no service"}</code><Link to={`/logs/${row.record_id}?project=${row.project_id}`}>View detail</Link></footer>
    </article>)}</div>
  </section>;
}

function QuerySummary({ result }: { result: SearchResult }) {
  return <p className="query-summary" aria-live="polite">{result.rows.length} rows · {formatInt64(result.stats.scanned_bytes)} bytes scanned · {formatInt64(result.stats.elapsed_ms)} ms</p>;
}

function barHeight(value: unknown): number {
  try {
    const count = BigInt(String(value ?? 0));
    if (count <= 0n) return 2;
    return Math.min(96, 2 + count.toString(2).length * 6);
  } catch {
    return 2;
  }
}
