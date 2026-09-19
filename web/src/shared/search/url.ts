import type { Kind, SearchRequest } from "../../api/types";
import { microsFromMilliseconds, parseInt64 } from "../format/int64";

export interface SearchURLState {
  projectIDs: string[];
  kinds: Kind[];
  expression: string;
  timeBasis: "event" | "received";
  startUS: string;
  endUS: string;
  sort: "event_desc" | "received_desc";
}

const kinds = new Set<Kind>(["error", "log", "transaction"]);

export function defaultSearchState(now = Date.now(), defaultKinds: Kind[] = ["log"]): SearchURLState {
  return {
    projectIDs: [],
    kinds: defaultKinds,
    expression: "",
    timeBasis: "event",
    startUS: microsFromMilliseconds(now - 15 * 60 * 1000),
    endUS: microsFromMilliseconds(now),
    sort: "event_desc",
  };
}

export function decodeSearchURL(params: URLSearchParams, fallback: SearchURLState): SearchURLState {
  const projectIDs = canonicalIDs(params.get("projects")?.split(",") ?? fallback.projectIDs);
  const selectedKinds = canonicalKinds(params.get("kinds")?.split(",") ?? fallback.kinds);
  const startUS = params.get("start") ?? fallback.startUS;
  const endUS = params.get("end") ?? fallback.endUS;
  if (parseInt64(startUS) >= parseInt64(endUS)) throw new Error("invalid absolute time range");
  const basis = params.get("basis") ?? fallback.timeBasis;
  const sort = params.get("sort") ?? fallback.sort;
  if (basis !== "event" && basis !== "received") throw new Error("invalid time basis");
  if (sort !== "event_desc" && sort !== "received_desc") throw new Error("invalid sort");
  return { projectIDs, kinds: selectedKinds, expression: params.get("q") ?? fallback.expression, timeBasis: basis, startUS, endUS, sort };
}

export function encodeSearchURL(state: SearchURLState): URLSearchParams {
  const result = new URLSearchParams();
  if (state.projectIDs.length) result.set("projects", canonicalIDs(state.projectIDs).join(","));
  result.set("kinds", canonicalKinds(state.kinds).join(","));
  result.set("basis", state.timeBasis);
  result.set("start", parseInt64(state.startUS).toString());
  result.set("end", parseInt64(state.endUS).toString());
  result.set("sort", state.sort);
  if (state.expression.trim()) result.set("q", state.expression.trim());
  return result;
}

export function datasetKey(tenantID: string, state: SearchURLState): string {
  return [tenantID, canonicalIDs(state.projectIDs).join(","), canonicalKinds(state.kinds).join(","), state.timeBasis, parseInt64(state.startUS), parseInt64(state.endUS), state.expression.trim()].join("|");
}

export function searchRequest(tenantID: string, state: SearchURLState, readToken?: string): SearchRequest {
  return {
    tenant_id: tenantID,
    project_ids: canonicalIDs(state.projectIDs),
    kinds: canonicalKinds(state.kinds),
    time_basis: state.timeBasis,
    start_us: parseInt64(state.startUS).toString(),
    end_us: parseInt64(state.endUS).toString(),
    expression: state.expression.trim() || undefined,
    limit: 100,
    sort: state.sort,
    mode: "auto",
    read_token: readToken,
    projection: "list",
  };
}

function canonicalIDs(values: string[]): string[] {
  const unique = new Set(values.filter(Boolean).map((value) => {
    const parsed = parseInt64(value);
    if (parsed <= 0n) throw new Error("project IDs must be positive");
    return parsed.toString();
  }));
  return [...unique].sort((left, right) => (BigInt(left) < BigInt(right) ? -1 : BigInt(left) > BigInt(right) ? 1 : 0));
}

function canonicalKinds(values: string[]): Kind[] {
  const unique = new Set(values.filter((value): value is Kind => kinds.has(value as Kind)));
  if (unique.size === 0) throw new Error("at least one kind is required");
  return [...unique].sort();
}
