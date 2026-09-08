import { useEffect, useState } from "react";
import type { SearchRow } from "../../api/types";

export type LiveStatus =
  "idle" | "connecting" | "open" | "reconnecting" | "resync_required" | "error";

interface LiveState {
  status: LiveStatus;
  rows: SearchRow[];
  checkpoint?: string;
  errorCode?: string;
}

const initial: LiveState = { status: "idle", rows: [] };
const maxRows = 500;

function record(value: unknown): value is SearchRow {
  if (!value || typeof value !== "object") return false;
  const row = value as Partial<SearchRow>;
  return (
    typeof row.record_id === "string" &&
    typeof row.ingest_seq === "string" &&
    typeof row.received_at === "string" &&
    typeof row.timestamp === "string"
  );
}

export function useLiveLogs(url?: string): LiveState {
  const [state, setState] = useState<LiveState & { sourceUrl?: string }>(
    initial,
  );

  useEffect(() => {
    if (!url) return;
    const source = new EventSource(url, { withCredentials: true });
    let firstOpen = true;
    let active = true;
    const fail = (errorCode: string) => {
      source.close();
      if (active) {
        setState((current) => ({
          ...current,
          sourceUrl: url,
          status: "error",
          errorCode,
        }));
      }
    };

    source.onopen = () => {
      if (!active) return;
      setState((current) =>
        firstOpen
          ? { sourceUrl: url, status: "open", rows: [] }
          : { ...current, sourceUrl: url, status: "open" },
      );
      firstOpen = false;
    };
    source.addEventListener("record", (event) => {
      try {
        const value: unknown = JSON.parse(event.data);
        if (!record(value)) throw new Error("invalid_live_record");
        if (!active) return;
        setState((current) => {
          const base =
            current.sourceUrl === url
              ? current
              : { sourceUrl: url, status: "connecting" as const, rows: [] };
          if (base.rows.some((row) => row.record_id === value.record_id))
            return base;
          return {
            ...base,
            status: "open",
            rows: [...base.rows, value].slice(-maxRows),
          };
        });
      } catch {
        fail("invalid_live_record");
      }
    });
    source.addEventListener("checkpoint", (event) => {
      try {
        const value = JSON.parse(event.data) as { scan_seq?: unknown };
        if (typeof value.scan_seq !== "string")
          throw new Error("invalid checkpoint");
        const checkpoint = value.scan_seq;
        if (!active) return;
        setState((current) => ({
          ...current,
          sourceUrl: url,
          status: "open",
          checkpoint,
        }));
      } catch {
        fail("invalid_live_checkpoint");
      }
    });
    source.addEventListener("resync_required", () => {
      source.close();
      if (active)
        setState((current) => ({
          ...current,
          sourceUrl: url,
          status: "resync_required",
        }));
    });
    source.addEventListener("error", (event) => {
      if (event instanceof MessageEvent) {
        source.close();
        let errorCode = "live_unavailable";
        try {
          const value = JSON.parse(event.data) as { code?: unknown };
          if (typeof value.code === "string") errorCode = value.code;
        } catch {
          errorCode = "live_unavailable";
        }
        fail(errorCode);
      } else {
        if (active)
          setState((current) => ({
            ...current,
            sourceUrl: url,
            status: "reconnecting",
          }));
      }
    });

    return () => {
      active = false;
      source.close();
    };
  }, [url]);

  if (!url) return initial;
  if (state.sourceUrl !== url) return { status: "connecting", rows: [] };
  return state;
}
