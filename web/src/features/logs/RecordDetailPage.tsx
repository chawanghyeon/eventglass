import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { api } from "../../api/client";
import { useSession } from "../../app/providers";
import { StatusPanel } from "../../shared/ui/StatusPanel";
import { SearchSession } from "../../shared/search/session";

export function RecordDetailPage() {
  const { id = "" } = useParams();
  const [params] = useSearchParams();
  const projectID = params.get("project") ?? "";
  const { session, tenant } = useSession();
  const client = useQueryClient();
  const owner = useMemo(() => new SearchSession(tenant.tenant_id, session.csrf_token, (ownerID) => {
    client.removeQueries({ queryKey: ["record", session.user_id, tenant.tenant_id, projectID, id, ownerID] });
  }), [client, session.user_id, session.csrf_token, tenant.tenant_id, projectID, id]);
  useEffect(() => owner.retain(), [owner]);
  const detail = useQuery({
    queryKey: ["record", session.user_id, tenant.tenant_id, projectID, id, owner.id],
    queryFn: async () => {
      const value = await api.record(tenant.tenant_id, projectID, id, owner.readToken());
      owner.observe(value);
      return value;
    },
    enabled: /^[0-9a-f]{64}$/.test(id) && /^[1-9][0-9]*$/.test(projectID),
  });
  return <section>
    <Link to="/logs">← Logs</Link>
    {detail.isPending ? <p role="status">Loading record…</p> : null}
    <StatusPanel error={detail.error} onRetry={() => void detail.refetch()} />
    {detail.data ? <article className="detail-card">
      <p className="eyebrow">Record {id.slice(0, 12)}</p>
      <h1>{stringField(detail.data.record, "message") || "Record detail"}</h1>
      <h2>Canonical record</h2><SafeJSON value={detail.data.record} />
      <h2>Scrubbed raw payload</h2><SafeJSON value={detail.data.raw} />
      <h2>Envelope SDK</h2>{detail.data.envelope_sdk ? <SafeJSON value={detail.data.envelope_sdk} /> : <p className="muted">No envelope SDK metadata.</p>}
    </article> : null}
  </section>;
}

function SafeJSON({ value }: { value: unknown }) {
  return <pre className="json-text">{JSON.stringify(value, null, 2)}</pre>;
}

function stringField(value: Record<string, unknown>, name: string): string | undefined {
  return typeof value[name] === "string" ? value[name] : undefined;
}
