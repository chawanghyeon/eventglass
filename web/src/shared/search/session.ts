import { api } from "../../api/client";
import type { SearchRequest, SearchResult } from "../../api/types";
import { executeSearch } from "./execute";

type SnapshotResource = { snapshot_id?: string; read_token?: string; query_id?: string; state?: string };

// One dataset owns one snapshot, independent of row sort/projection. Observe
// late submissions too: an unmounted view must not orphan accepted server work.
export class SearchSession {
	readonly id = crypto.randomUUID();
  private snapshots = new Set<string>();
  private jobs = new Set<string>();
  private token?: string;
  private snapshot?: string;
  private closed = false;
  private references = 0;
  private timer?: ReturnType<typeof setInterval>;
  private renewing = false;
  private failure?: unknown;
  private serial: Promise<unknown> = Promise.resolve();

  constructor(private tenant: string, private csrf: string, private onClose: (id: string) => void = () => undefined) {}

  retain(): () => void {
    this.references++;
    if (!this.timer) this.timer = setInterval(() => { void this.heartbeat(); }, 30_000);
    return () => {
      this.references--;
      // StrictMode cleanup/setup must not release a still-mounted dataset.
      queueMicrotask(() => { if (this.references === 0) void this.close(); });
    };
  }

  observe = (value: SnapshotResource): void => {
    if (value.snapshot_id) { this.snapshots.add(value.snapshot_id); this.snapshot = value.snapshot_id; }
    if (value.read_token) this.token = value.read_token;
    if (value.query_id) {
      if (value.state === "succeeded" || value.state === "failed" || value.state === "canceled") this.jobs.delete(value.query_id);
      else this.jobs.add(value.query_id);
    }
    if (this.closed) void this.release();
  };

  readToken(): string | undefined {
    if (this.failure) throw this.failure;
    return this.token;
  }

  search(request: SearchRequest, signal: AbortSignal): Promise<SearchResult> {
    const result = this.serial.then(async () => {
      if (this.closed || signal.aborted) throw new DOMException("Search closed", "AbortError");
      if (this.failure) throw this.failure;
      const value = await executeSearch({ ...request, read_token: this.token }, this.csrf, signal, this.observe);
      this.observe(value);
      if (this.closed || signal.aborted) throw new DOMException("Search closed", "AbortError");
      return value;
    });
    this.serial = result.catch(() => undefined);
    return result;
  }

  async heartbeat(): Promise<void> {
    if (this.closed || this.renewing || !this.snapshot || !this.token) return;
    this.renewing = true;
    try { this.token = await api.renewSnapshot(this.tenant, this.snapshot, this.token, this.csrf); }
    catch (error) { this.failure = error; }
    finally { this.renewing = false; }
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      clearInterval(this.timer);
      this.onClose(this.id);
    }
    await this.release();
  }

  private async release(): Promise<void> {
    const jobs = [...this.jobs]; this.jobs.clear();
    const snapshots = [...this.snapshots]; this.snapshots.clear();
    await Promise.allSettled(jobs.map((id) => api.cancelQuery(this.tenant, id, this.csrf)));
    await Promise.allSettled(snapshots.map((id) => api.releaseSnapshot(this.tenant, id, this.csrf)));
  }
}
