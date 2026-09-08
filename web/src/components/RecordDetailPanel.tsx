import { useEffect, useRef } from "react";
import { Button } from "./Button";
import { Notice } from "./Notice";
import { Spinner } from "./Spinner";

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
  error,
  heading,
  onClose,
  onRetry,
  pending,
  raw,
  recordId,
}: {
  error?: string;
  heading: string;
  onClose: () => void;
  onRetry: () => void;
  pending: boolean;
  raw?: unknown;
  recordId: string;
}) {
  const closeButton = useRef<HTMLButtonElement>(null);
  const exceptions = exceptionValues(raw);
  const breadcrumbs = breadcrumbValues(raw);

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
        </>
      ) : null}
    </section>
  );
}
