import type { SearchFilters } from "../../api/types";

export const filterFields = [
  "kinds",
  "services",
  "levels",
  "environments",
  "releases",
  "loggers",
] as const;

export type FilterField = (typeof filterFields)[number];

export interface LogSearchState {
  projects: string[];
  start: string;
  end: string;
  query: string;
  filters: SearchFilters;
  cursor?: string;
  readToken?: string;
}

export function defaultBounds(now = new Date()): {
  start: string;
  end: string;
} {
  return {
    start: new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString(),
    end: now.toISOString(),
  };
}

function values(params: URLSearchParams, field: FilterField): string[] {
  return [
    ...new Set(
      params
        .getAll(field)
        .map((value) => value.trim())
        .filter(Boolean),
    ),
  ];
}

export function readLogSearch(params: URLSearchParams): LogSearchState {
  return {
    projects: [...new Set(params.getAll("project").filter(Boolean))],
    start: params.get("start") ?? "",
    end: params.get("end") ?? "",
    query: params.get("query") ?? "",
    filters: Object.fromEntries(
      filterFields.map((field) => [field, values(params, field)]),
    ),
  };
}

export function isValidLogSearch(state: LogSearchState): boolean {
  const start = Date.parse(state.start);
  const end = Date.parse(state.end);
  return (
    Number.isFinite(start) &&
    Number.isFinite(end) &&
    start < end &&
    state.projects.every((id) => /^[1-9][0-9]*$/.test(id)) &&
    (state.filters.kinds ?? []).every(
      (kind) => kind === "log" || kind === "error",
    )
  );
}

export function writeLogSearch(
  state: Omit<LogSearchState, "cursor" | "readToken">,
): URLSearchParams {
  const params = new URLSearchParams({ start: state.start, end: state.end });
  for (const project of state.projects) params.append("project", project);
  if (state.query) params.set("query", state.query);
  for (const field of filterFields) {
    for (const value of state.filters[field] ?? []) params.append(field, value);
  }
  return params;
}

export function splitFilter(value: string): string[] {
  return [
    ...new Set(
      value
        .split(",")
        .map((part) => part.trim())
        .filter(Boolean),
    ),
  ];
}
