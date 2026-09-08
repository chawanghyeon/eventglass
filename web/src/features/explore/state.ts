import {
  isValidLogSearch,
  readLogSearch,
  writeLogSearch,
  type LogSearchState,
} from "../logs";

export const metricOps = ["count", "sum", "min", "max", "avg"] as const;
export const groupFields = [
  "service",
  "level",
  "environment",
  "release",
] as const;
export const histogramIntervals = [
  "auto",
  "10s",
  "1m",
  "5m",
  "1h",
  "6h",
  "1d",
] as const;

export type MetricOp = (typeof metricOps)[number];
export type GroupField = (typeof groupFields)[number];
export type HistogramInterval = (typeof histogramIntervals)[number];

export interface ExploreState {
  search: LogSearchState;
  metric: MetricOp;
  field: string;
  groups: GroupField[];
  interval: HistogramInterval | "none";
}

function member<T extends readonly string[]>(
  values: T,
  value: string | null,
): value is T[number] {
  return value !== null && values.includes(value);
}

export function readExploreState(params: URLSearchParams): ExploreState {
  const metricParam = params.get("metric");
  const intervalParam = params.get("interval");
  const groups = [
    ...new Set(
      params
        .getAll("group")
        .filter((value): value is GroupField => member(groupFields, value)),
    ),
  ].slice(0, 2);
  return {
    search: readLogSearch(params),
    metric: member(metricOps, metricParam) ? metricParam : "count",
    field: params.get("field") ?? "",
    groups,
    interval:
      intervalParam === "none"
        ? "none"
        : member(histogramIntervals, intervalParam)
          ? intervalParam
          : "auto",
  };
}

export function writeExploreState(state: ExploreState): URLSearchParams {
  const params = writeLogSearch(state.search);
  if (state.metric !== "count") params.set("metric", state.metric);
  if (state.field) params.set("field", state.field);
  for (const group of state.groups) params.append("group", group);
  if (state.interval !== "auto") params.set("interval", state.interval);
  return params;
}

export function isValidExploreState(state: ExploreState): boolean {
  return (
    isValidLogSearch(state.search) &&
    (state.metric === "count" || /^attributes\..{1,512}$/.test(state.field))
  );
}
