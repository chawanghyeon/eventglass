import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import type { AggregateRequest } from "../../api/types";
import { Button } from "../../components/Button";
import { Histogram } from "../../components/Histogram";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { defaultBounds, LogFilters } from "../logs";
import { projectsQuery } from "../projects";
import { AggregateTable } from "./AggregateTable";
import { ExploreControls } from "./ExploreControls";
import { aggregateQuery } from "./queries";
import {
  isValidExploreState,
  readExploreState,
  writeExploreState,
} from "./state";

export function ExplorePage() {
  const user = useSession().data;
  const [searchParams, setSearchParams] = useSearchParams();
  const [initialBounds] = useState(defaultBounds);
  const state = useMemo(() => readExploreState(searchParams), [searchParams]);
  const bothBoundsMissing = !state.search.start && !state.search.end;
  const valid = isValidExploreState(state);
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: Boolean(user),
  });
  const metric: AggregateRequest["metrics"] =
    state.metric === "count"
      ? [{ op: "count", name: "result" }]
      : [{ op: state.metric, field: state.field, name: "result" }];
  const request: AggregateRequest = {
    projects: state.search.projects,
    start: state.search.start,
    end: state.search.end,
    query: state.search.query || undefined,
    filters: state.search.filters,
    metrics: metric,
    group_by: state.groups,
    group_limit: 20,
    histogram:
      state.interval === "none"
        ? undefined
        : { field: "timestamp", interval: state.interval },
  };
  const aggregate = useQuery({
    ...aggregateQuery(user?.id ?? "unknown", request),
    enabled: Boolean(user) && valid && !bothBoundsMissing,
    retry: false,
  });

  useEffect(() => {
    if (!bothBoundsMissing) return;
    const next = new URLSearchParams(searchParams);
    next.set("start", initialBounds.start);
    next.set("end", initialBounds.end);
    setSearchParams(next, { replace: true });
  }, [bothBoundsMissing, initialBounds, searchParams, setSearchParams]);

  function commit(next: typeof state) {
    setSearchParams(writeExploreState(next));
  }

  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <p className="eyebrow">분석</p>
          <h1>수치 분석</h1>
          <p>로그의 건수와 수치를 비교하고 추이를 확인하세요.</p>
        </div>
        {aggregate.data ? (
          <span className="count-badge">{aggregate.data.record_count}건</span>
        ) : null}
      </header>
      <LogFilters
        committed={state.search}
        key={`search-${writeExploreState(state)}`}
        onApply={(search) => commit({ ...state, search })}
        projects={projects.data ?? []}
      />
      <ExploreControls
        committed={state}
        key={`aggregate-${writeExploreState(state)}`}
        onApply={commit}
      />
      {!bothBoundsMissing && !valid ? (
        <Notice tone="error">
          검색 시간과 필터를 확인하고 숫자 metric에는 attributes 경로를 입력해
          주세요.
        </Notice>
      ) : null}
      {projects.isError ? (
        <Notice tone="error">{describeApiError(projects.error)}</Notice>
      ) : null}
      {aggregate.isPending && valid ? <Spinner label="집계 중" /> : null}
      {aggregate.isError ? (
        <Notice tone="error">
          <span>{describeApiError(aggregate.error)}</span>
          <Button
            onClick={() => void aggregate.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}
      {aggregate.data ? (
        <section
          className="snapshot-panel"
          aria-labelledby="explore-result-heading"
        >
          <header>
            <div>
              <p className="eyebrow">정확한 결과</p>
              <h2 id="explore-result-heading">
                {aggregate.data.record_count}개 record
              </h2>
            </div>
            <span>{aggregate.data.watermark} W</span>
          </header>
          {aggregate.data.metrics.map((value) => (
            <div className="metric-card" key={value.name}>
              <span>{value.op}</span>
              <strong>{value.value ?? "값 없음"}</strong>
            </div>
          ))}
          {aggregate.data.buckets &&
          "histogram" in aggregate.data.buckets.dimension ? (
            <Histogram
              buckets={aggregate.data.buckets}
              label="Explore 시간별 건수"
            />
          ) : null}
          {aggregate.data.buckets ? (
            <AggregateTable buckets={aggregate.data.buckets} />
          ) : null}
          {aggregate.data.record_count === "0" ? (
            <p className="histogram__empty">조건에 맞는 record가 없습니다.</p>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
