import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import type { SearchRequest, SearchResult } from "../../api/types";
import { SearchSession } from "./session";

vi.mock("../../api/client", () => ({ api: { search: vi.fn(), renewSnapshot: vi.fn(), releaseSnapshot: vi.fn(), cancelQuery: vi.fn(), queryJob: vi.fn() } }));
const request = { tenant_id: "1", project_ids: ["2"], sort: "event_desc" } as SearchRequest;
const result = { snapshot_id: "snapshot", read_token: "token", rows: [] } as unknown as SearchResult;
afterEach(() => { vi.clearAllMocks(); vi.useRealTimers(); });

describe("server snapshot lifetime", () => {
  it("releases six successive datasets rather than filling the four-pin user quota", async () => {
    for (let i = 0; i < 6; i++) {
      vi.mocked(api.search).mockResolvedValue({ ...result, snapshot_id: `snapshot-${i}` });
      const owner = new SearchSession("1", "csrf");
      await owner.search(request, new AbortController().signal);
      await owner.close();
    }
    expect(api.releaseSnapshot).toHaveBeenCalledTimes(6);
    expect(new Set(vi.mocked(api.releaseSnapshot).mock.calls.map((call) => call[1])).size).toBe(6);
  });
  it("reuses one snapshot for a changed row operation and releases it once", async () => {
    vi.mocked(api.search).mockResolvedValue(result);
    const owner = new SearchSession("1", "csrf");
    const signal = new AbortController().signal;
    await owner.search(request, signal);
    await owner.search({ ...request, sort: "received_desc" }, signal);
    expect(api.search).toHaveBeenLastCalledWith(expect.objectContaining({ sort: "received_desc", read_token: "token" }));
    await owner.close(); await owner.close();
    expect(api.releaseSnapshot).toHaveBeenCalledExactlyOnceWith("1", "snapshot", "csrf");
  });

  it("cleans up an async submission arriving after unmount", async () => {
    let resolve!: (value: Awaited<ReturnType<typeof api.search>>) => void;
    vi.mocked(api.search).mockReturnValue(new Promise((done) => { resolve = done; }));
    const owner = new SearchSession("1", "csrf");
    const controller = new AbortController();
    const pending = owner.search(request, controller.signal);
    const rejection = expect(pending).rejects.toBeDefined();
    await Promise.resolve();
    controller.abort(); await owner.close();
    resolve({ snapshot_id: "late", query_id: "job", state: "queued", expires_at: "", poll_after_ms: 500 });
    await rejection;
    await Promise.resolve();
    expect(api.cancelQuery).toHaveBeenCalledWith("1", "job", "csrf");
    expect(api.releaseSnapshot).toHaveBeenCalledWith("1", "late", "csrf");
  });

  it("renews at 30 seconds and stops after disposal", async () => {
    vi.useFakeTimers();
    vi.mocked(api.search).mockResolvedValue(result);
    vi.mocked(api.renewSnapshot).mockResolvedValue("renewed");
    const owner = new SearchSession("1", "csrf");
    const release = owner.retain();
    await owner.search(request, new AbortController().signal);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(api.renewSnapshot).toHaveBeenCalledExactlyOnceWith("1", "snapshot", "token", "csrf");
    await owner.search(request, new AbortController().signal);
    expect(api.search).toHaveBeenLastCalledWith(expect.objectContaining({ read_token: "renewed" }));
    release(); await Promise.resolve();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(api.renewSnapshot).toHaveBeenCalledOnce();
  });

  it("does not dispose during StrictMode cleanup/setup", async () => {
    const owner = new SearchSession("1", "csrf");
    const first = owner.retain(); first();
    const second = owner.retain();
    owner.observe(result);
    await Promise.resolve();
    expect(api.releaseSnapshot).not.toHaveBeenCalled();
    second(); await Promise.resolve(); await Promise.resolve();
    expect(api.releaseSnapshot).toHaveBeenCalledOnce();
  });
});
