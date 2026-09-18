import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { ApiError, describeApiError } from "../../api/client";
import { liveLogsUrl } from "../../api/endpoints";
import type { SearchRow } from "../../api/types";
import { Button } from "../../components/Button";
import { Histogram } from "../../components/Histogram";
import { Notice } from "../../components/Notice";
import { RecordDetailPanel } from "../../components/RecordDetailPanel";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { projectsQuery } from "../projects";
import { LogFilters } from "./LogFilters";
import { logsHistogramQuery, logsQuery, recordDetailQuery } from "./queries";
import {
  defaultBounds,
  isValidLogSearch,
  readLogSearch,
  writeLogSearch,
} from "./state";
import { useLiveLogs } from "./useLiveLogs";

const resettableCodes = new Set([
  "invalid_search_token",
  "search_token_expired",
  "storage_generation_changed",
  "search_authorization_changed",
]);

function projectName(row: SearchRow, names: Map<string, string>): string {
  return names.get(row.project_id) ?? `프로젝트 ${row.project_id}`;
}

export function LogsPage() {
  const session = useSession();
  const user = session.data;
  const [searchParams, setSearchParams] = useSearchParams();
  const [initialBounds] = useState(defaultBounds);
  const [wrapMessages, setWrapMessages] = useState(false);
  const [showMetadata, setShowMetadata] = useState(true);
  const [selected, setSelected] = useState<SearchRow>();
  const [liveStart, setLiveStart] = useState<string>();
  const selectedTrigger = useRef<HTMLButtonElement>(null);
  const state = useMemo(() => readLogSearch(searchParams), [searchParams]);
  const liveUrl = useMemo(
    () =>
      liveStart
        ? liveLogsUrl({
            projects: state.projects,
            start: liveStart,
            // The native search index stores dates as signed i64 nanoseconds.
            end: "2262-04-11T23:47:16.854Z",
            query: state.query,
            filters: state.filters,
          })
        : undefined,
    [liveStart, state.filters, state.projects, state.query],
  );
  const live = useLiveLogs(liveUrl);
  const criteriaKey = writeLogSearch(state).toString();
  const [paging, setPaging] = useState<{
    criteriaKey: string;
    cursor?: string;
    readToken?: string;
  }>({ criteriaKey });
  const currentPaging: { cursor?: string; readToken?: string } =
    paging.criteriaKey === criteriaKey ? paging : {};
  const requestState = {
    ...state,
    cursor: currentPaging.cursor,
    readToken: currentPaging.readToken,
  };
  const bothBoundsMissing = !state.start && !state.end;
  const valid = isValidLogSearch(state);
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: Boolean(user),
  });
  const logs = useQuery({
    ...logsQuery(user?.id ?? "unknown", requestState),
    enabled: Boolean(user) && valid && !bothBoundsMissing,
    retry: false,
  });
  const histogramRequest = {
    projects: state.projects,
    start: state.start,
    end: state.end,
    query: state.query || undefined,
    filters: state.filters,
    read_token: logs.data?.read_token,
    metrics: [{ op: "count" as const, name: "records" }],
    group_by: [],
    group_limit: 10,
    histogram: { field: "timestamp" as const, interval: "auto" as const },
  };
  const histogram = useQuery({
    ...logsHistogramQuery(user?.id ?? "unknown", histogramRequest),
    enabled: Boolean(user && logs.data?.read_token && valid),
    retry: false,
  });
  const detail = useQuery({
    ...recordDetailQuery(
      user?.id ?? "unknown",
      selected?.project_id ?? "unknown",
      logs.data?.read_token ?? currentPaging.readToken ?? "unknown",
      selected?.detail_token ?? "unknown",
    ),
    enabled: Boolean(user && selected),
    retry: false,
  });

  useEffect(() => {
    if (!bothBoundsMissing) return;
    const next = new URLSearchParams(searchParams);
    next.set("start", initialBounds.start);
    next.set("end", initialBounds.end);
    setSearchParams(next, { replace: true });
  }, [bothBoundsMissing, initialBounds, searchParams, setSearchParams]);

  const names = useMemo(
    () => new Map(projects.data?.map((project) => [project.id, project.name])),
    [projects.data],
  );
  const error = logs.error;
  const canReset = error instanceof ApiError && resettableCodes.has(error.code);

  function freshSnapshot() {
    if (!currentPaging.cursor && !currentPaging.readToken) {
      void logs.refetch();
      return;
    }
    setSelected(undefined);
    setPaging({ criteriaKey });
  }

  function nextPage() {
    if (!logs.data?.next_cursor) return;
    setSelected(undefined);
    setPaging({
      criteriaKey,
      cursor: logs.data.next_cursor,
      readToken: logs.data.read_token,
    });
  }

  function closeDetail() {
    setSelected(undefined);
    selectedTrigger.current?.focus({ preventScroll: true });
  }

  return (
    <div className="page-stack logs-page">
      <header className="page-heading">
        <div>
          <h1>로그 검색</h1>
          <p>메시지를 검색하고 오류의 원인을 확인하세요.</p>
        </div>
        {logs.data ? (
          <span className="count-badge">{logs.data.rows.length}개 표시</span>
        ) : null}
        <Button
          onClick={() => {
            if (liveStart) {
              setLiveStart(undefined);
            } else {
              setLiveStart(new Date(Date.now() - 15 * 60 * 1000).toISOString());
            }
          }}
          type="button"
          variant={liveStart ? "quiet" : "primary"}
        >
          {liveStart ? "Live 중지" : "Live 시작"}
        </Button>
      </header>

      {liveStart ? (
        <section className="snapshot-panel" aria-labelledby="live-heading">
          <header>
            <div>
              <p className="eyebrow">수신 시각 기준 · 최근 15분부터</p>
              <h2 id="live-heading">Live Logs</h2>
            </div>
            <span>
              {live.status === "open"
                ? `연결됨 · seq ${live.checkpoint ?? "동기화 중"}`
                : live.status === "reconnecting"
                  ? "재연결 중"
                  : live.status === "connecting"
                    ? "연결 중"
                    : "연결 종료"}
            </span>
          </header>
          {live.status === "resync_required" ? (
            <Notice tone="error">
              처리 한도를 넘어 연결을 닫았습니다. Live를 다시 시작해 현재
              범위에서 동기화해 주세요.
            </Notice>
          ) : null}
          {live.status === "error" ? (
            <Notice tone="error">
              Live 연결이 종료되었습니다 ({live.errorCode ?? "live_unavailable"}
              ).
            </Notice>
          ) : null}
          {live.rows.length === 0 && live.status === "open" ? (
            <p>새로 수신된 조건 일치 기록이 없습니다.</p>
          ) : null}
          {live.rows.length > 0 ? (
            <ol className="live-list" aria-label="Live 수신 기록">
              {live.rows.map((row) => (
                <li key={row.record_id}>
                  <button onClick={() => setSelected(row)} type="button">
                    <strong>{row.message || "(빈 메시지)"}</strong>
                    <span>
                      수신 {new Date(row.received_at).toLocaleString("ko-KR")} ·
                      발생 {new Date(row.timestamp).toLocaleString("ko-KR")} ·
                      seq {row.ingest_seq}
                    </span>
                  </button>
                </li>
              ))}
            </ol>
          ) : null}
        </section>
      ) : null}

      <LogFilters
        committed={state}
        key={writeLogSearch(state).toString()}
        onApply={(next) => {
          setSelected(undefined);
          setPaging({ criteriaKey: writeLogSearch(next).toString() });
          setSearchParams(writeLogSearch(next));
        }}
        projects={projects.data ?? []}
      />

      {!bothBoundsMissing && !valid ? (
        <Notice tone="error">
          시작과 종료를 올바른 RFC3339 절대 시각으로 입력하고 프로젝트와 종류
          필터를 확인해 주세요.
        </Notice>
      ) : null}
      {projects.isError ? (
        <Notice tone="error">{describeApiError(projects.error)}</Notice>
      ) : null}
      {logs.isPending && valid ? <Spinner label="로그 검색 중" /> : null}
      {logs.isError ? (
        <Notice tone="error">
          <span>{describeApiError(error)}</span>
          {canReset ? (
            <Button onClick={freshSnapshot} type="button" variant="quiet">
              새 스냅샷
            </Button>
          ) : (
            <Button
              onClick={() => void logs.refetch()}
              type="button"
              variant="quiet"
            >
              다시 시도
            </Button>
          )}
        </Notice>
      ) : null}
      {logs.data ? (
        <details className="disclosure">
          <summary>시간별 분포</summary>
          <header>
            <div>
              <p className="eyebrow">동일 스냅샷</p>
              <h2 id="logs-volume-heading">시간별 로그</h2>
            </div>
            <span>{logs.data.watermark} W</span>
          </header>
          {histogram.isPending ? <Spinner label="히스토그램 집계 중" /> : null}
          {histogram.isError ? (
            <Notice tone="error">
              <span>{describeApiError(histogram.error)}</span>
              <Button
                onClick={() => void histogram.refetch()}
                type="button"
                variant="quiet"
              >
                집계 다시 시도
              </Button>
            </Notice>
          ) : null}
          {histogram.data?.buckets ? (
            <Histogram
              buckets={histogram.data.buckets}
              label="시간별 로그 건수"
            />
          ) : null}
        </details>
      ) : null}
      {logs.data?.rows.length === 0 ? (
        <div className="empty-state">
          <span className="empty-state__mark" aria-hidden="true">
            00
          </span>
          <h2>조건에 맞는 로그가 없습니다.</h2>
          <p>시간 범위, 프로젝트, 검색어 또는 메타데이터 필터를 바꿔 보세요.</p>
        </div>
      ) : null}
      {logs.data && logs.data.rows.length > 0 ? (
        <div
          className={`log-table-wrap ${wrapMessages ? "" : "log-table--compact"} ${showMetadata ? "" : "log-table--hide-metadata"}`}
        >
          <div className="table-options" aria-label="표 표시 설정">
            <label>
              <input
                type="checkbox"
                checked={wrapMessages}
                onChange={(e) => setWrapMessages(e.target.checked)}
              />
              메시지 줄바꿈
            </label>
            <label>
              <input
                type="checkbox"
                checked={showMetadata}
                onChange={(e) => setShowMetadata(e.target.checked)}
              />
              메타데이터 열
            </label>
          </div>
          <table className="log-table">
            <thead>
              <tr>
                <th scope="col">시각</th>
                <th scope="col">메시지</th>
                <th scope="col">메타데이터</th>
                <th scope="col">원문</th>
              </tr>
            </thead>
            <tbody>
              {logs.data.rows.map((row) => (
                <tr
                  key={row.record_id}
                  className={
                    selected?.record_id === row.record_id
                      ? "is-selected"
                      : undefined
                  }
                >
                  <td className="log-time">
                    <strong>
                      {new Date(row.timestamp).toLocaleString("ko-KR")}
                    </strong>
                    <span>seq {row.ingest_seq}</span>
                  </td>
                  <th scope="row">
                    <span className={`log-kind log-kind--${row.kind}`}>
                      {row.kind}
                    </span>
                    <strong className="log-message">
                      {row.message || "(빈 메시지)"}
                    </strong>
                    <span className="log-service">
                      {(
                        [
                          ["services", row.service],
                          ["levels", row.level],
                        ] as const
                      )
                        .filter(([, value]) => value)
                        .map(([field, value]) => (
                          <button
                            className="cell-filter"
                            key={field}
                            type="button"
                            title={`${value} 값으로 필터`}
                            onClick={() =>
                              setSearchParams(
                                writeLogSearch({
                                  ...state,
                                  filters: {
                                    ...state.filters,
                                    [field]: [value],
                                  },
                                }),
                              )
                            }
                          >
                            {value}
                          </button>
                        ))}
                    </span>
                  </th>
                  <td className="log-metadata">
                    <strong>{projectName(row, names)}</strong>
                    <span>
                      {[row.environment, row.release, row.logger]
                        .filter(Boolean)
                        .join(" · ") || "추가 메타데이터 없음"}
                    </span>
                  </td>
                  <td>
                    <Button
                      onClick={(event) => {
                        selectedTrigger.current = event.currentTarget;
                        setSelected(row);
                      }}
                      type="button"
                      variant="quiet"
                    >
                      상세 보기
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
      {logs.data ? (
        <nav className="page-controls" aria-label="로그 페이지 이동">
          <Button onClick={freshSnapshot} type="button" variant="quiet">
            새 스냅샷
          </Button>
          <Button
            disabled={!logs.data.next_cursor}
            onClick={nextPage}
            type="button"
          >
            다음 페이지
          </Button>
        </nav>
      ) : null}
      {selected ? (
        <RecordDetailPanel
          detailToken={selected.detail_token}
          error={detail.isError ? describeApiError(detail.error) : undefined}
          heading="로그 상세"
          onClose={closeDetail}
          onRetry={() => void detail.refetch()}
          pending={detail.isPending}
          raw={detail.data?.raw}
          recordId={selected.record_id}
        />
      ) : null}
    </div>
  );
}
