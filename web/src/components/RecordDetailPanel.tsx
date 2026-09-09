import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { describeApiError } from "../api/client";
import { endpoints } from "../api/endpoints";
import { Button } from "./Button";
import { Notice } from "./Notice";
import { Spinner } from "./Spinner";
import { RecordFields } from "./RecordFields";

type JsonObject = Record<string, unknown>;

function object(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;
}

function text(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean")
    return String(value);
  return undefined;
}

function array(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function exceptionValues(raw: unknown): JsonObject[] {
  const exception = object(object(raw)?.exception);
  return array(exception?.values).flatMap((value) => {
    const entry = object(value);
    return entry ? [entry] : [];
  });
}

function breadcrumbValues(raw: unknown): JsonObject[] {
  const breadcrumbs = object(raw)?.breadcrumbs;
  const values = Array.isArray(breadcrumbs)
    ? breadcrumbs
    : array(object(breadcrumbs)?.values);
  return values.flatMap((value) => {
    const entry = object(value);
    return entry ? [entry] : [];
  });
}

function frameLabel(frame: JsonObject): string {
  const location = text(frame.filename) ?? text(frame.abs_path) ?? "파일 없음";
  const line = text(frame.lineno);
  const column = text(frame.colno);
  return `${location}${line ? `:${line}` : ""}${column ? `:${column}` : ""}`;
}

export function RecordDetailPanel({
  detailToken,
  error,
  heading,
  onClose,
  onRetry,
  pending,
  raw,
  recordId,
}: {
  detailToken?: string;
  error?: string;
  heading: string;
  onClose: () => void;
  onRetry: () => void;
  pending: boolean;
  raw?: unknown;
  recordId: string;
}) {
  const closeButton = useRef<HTMLButtonElement>(null);
  const [relatedWindow, setRelatedWindow] = useState(3600);
  const exceptions = exceptionValues(raw);
  const breadcrumbs = breadcrumbValues(raw);
  const related = useQuery({
    queryKey: ["related-records", detailToken ?? "none", relatedWindow],
    queryFn: ({ signal }) =>
      endpoints.relatedRecords(detailToken ?? "", relatedWindow, signal),
    enabled: Boolean(detailToken) && raw !== undefined,
    retry: false,
  });

  useEffect(() => closeButton.current?.focus(), []);

  return (
    <section
      aria-labelledby="record-detail-heading"
      className="record-detail"
      onKeyDown={(event) => {
        if (event.key === "Escape") onClose();
      }}
    >
      <header className="section-heading">
        <div>
          <p className="eyebrow">저장된 원문</p>
          <h2 id="record-detail-heading">{heading}</h2>
        </div>
        <button
          className="button button--quiet"
          onClick={onClose}
          ref={closeButton}
          type="button"
        >
          닫기
        </button>
      </header>
      <code>{recordId}</code>
      {pending ? <Spinner label="원문 불러오는 중" /> : null}
      {error ? (
        <Notice tone="error">
          <span>{error}</span>
          <Button onClick={onRetry} type="button" variant="quiet">
            다시 시도
          </Button>
        </Notice>
      ) : null}
      {raw !== undefined ? (
        <>
          <RecordFields raw={raw} />
          <section
            className="record-section"
            aria-labelledby="exception-heading"
          >
            <h3 id="exception-heading">Exception chain</h3>
            {exceptions.length === 0 ? (
              <p className="inline-empty">Exception 정보가 없습니다.</p>
            ) : (
              <ol className="exception-list">
                {exceptions.map((exception, exceptionIndex) => {
                  const frames = array(
                    object(exception.stacktrace)?.frames,
                  ).flatMap((value) => {
                    const frame = object(value);
                    return frame ? [frame] : [];
                  });
                  return (
                    <li key={exceptionIndex}>
                      <strong>{text(exception.type) ?? "종류 없음"}</strong>
                      <span>{text(exception.value) ?? "설명 없음"}</span>
                      {frames.length === 0 ? (
                        <p className="inline-empty">Stack frame이 없습니다.</p>
                      ) : (
                        <ol className="frame-list">
                          {frames.map((frame, frameIndex) => (
                            <li key={frameIndex}>
                              <code>{frameLabel(frame)}</code>
                              <span>{text(frame.function) ?? "함수 없음"}</span>
                            </li>
                          ))}
                        </ol>
                      )}
                    </li>
                  );
                })}
              </ol>
            )}
          </section>
          <section
            className="record-section"
            aria-labelledby="breadcrumb-heading"
          >
            <h3 id="breadcrumb-heading">Breadcrumbs</h3>
            {breadcrumbs.length === 0 ? (
              <p className="inline-empty">Breadcrumb가 없습니다.</p>
            ) : (
              <ol className="breadcrumb-list">
                {breadcrumbs.map((breadcrumb, index) => (
                  <li key={index}>
                    <strong>
                      {text(breadcrumb.category) ??
                        text(breadcrumb.type) ??
                        "breadcrumb"}
                    </strong>
                    <span>{text(breadcrumb.message) ?? "메시지 없음"}</span>
                    {text(breadcrumb.timestamp) ? (
                      <time>{text(breadcrumb.timestamp)}</time>
                    ) : null}
                  </li>
                ))}
              </ol>
            )}
          </section>
          <section className="record-section" aria-labelledby="raw-heading">
            <h3 id="raw-heading">Scrubbed JSON</h3>
            <pre>{JSON.stringify(raw, null, 2)}</pre>
          </section>
          {detailToken ? (
            <section
              className="record-section"
              aria-labelledby="related-heading"
            >
              <div className="related-heading">
                <div>
                  <h3 id="related-heading">Related Logs</h3>
                  {related.data ? (
                    <p>
                      {related.data.exact ? "정확한 ID 연결" : "시간 기반 추정"}{" "}
                      · {strategyLabel(related.data.strategy)}
                    </p>
                  ) : null}
                </div>
                {related.data && !related.data.exact ? (
                  <span>시간 범위 ±{related.data.window_seconds}초 (고정)</span>
                ) : (
                  <label>
                    시간 범위
                    <select
                      onChange={(event) =>
                        setRelatedWindow(Number(event.target.value))
                      }
                      value={relatedWindow}
                    >
                      <option value={3600}>±1시간</option>
                      <option value={21600}>±6시간</option>
                      <option value={86400}>±24시간</option>
                    </select>
                  </label>
                )}
              </div>
              {related.isPending ? <Spinner label="연관 로그 검색 중" /> : null}
              {related.isError ? (
                <Notice tone="error">
                  <span>{describeApiError(related.error)}</span>
                  <Button
                    onClick={() => void related.refetch()}
                    type="button"
                    variant="quiet"
                  >
                    다시 시도
                  </Button>
                </Notice>
              ) : null}
              {related.data?.rows.length === 0 ? (
                <p className="inline-empty">
                  연결 기준에 맞는 다른 로그가 없습니다.
                </p>
              ) : null}
              {related.data?.rows.length ? (
                <ol className="related-list">
                  {related.data.rows.map((row) => (
                    <li key={row.record_id}>
                      <time>
                        {new Date(row.timestamp).toLocaleString("ko-KR")}
                      </time>
                      <strong>{row.message || "(빈 메시지)"}</strong>
                      <span>
                        {row.service} · {row.level} · {row.kind}
                      </span>
                    </li>
                  ))}
                </ol>
              ) : null}
            </section>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

function strategyLabel(strategy: string): string {
  switch (strategy) {
    case "trace_id":
      return "동일 trace_id";
    case "request_id":
      return "동일 request_id";
    case "project_service_user_time":
      return "같은 프로젝트·서비스·사용자와 근접 시각";
    default:
      return "같은 프로젝트·서비스의 Error ±30초";
  }
}
