import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { useRef, useState } from "react";
import { ApiError, describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Issue, IssueStatus, Occurrence } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { RecordDetailPanel } from "../../components/RecordDetailPanel";
import { Spinner } from "../../components/Spinner";
import { formatDecimal, formatTimestampUs } from "../../lib/decimal";
import { useSession } from "../auth";
import { IssueStatusBadge } from "./IssueStatusBadge";
import {
  issueKeys,
  issueQuery,
  occurrenceDetailQuery,
  occurrencesQuery,
} from "./queries";

const statuses: IssueStatus[] = ["unresolved", "resolved", "ignored"];
const statusActionLabels: Record<IssueStatus, string> = {
  unresolved: "미해결로 되돌리기",
  resolved: "해결 처리",
  ignored: "무시",
};

export function IssueDetailPage() {
  const { id = "" } = useParams();
  const session = useSession();
  const user = session.data;
  const [searchParams, setSearchParams] = useSearchParams();
  const projectId = searchParams.get("project") ?? "direct";
  const occurrenceTime = searchParams.get("occurrence_time") ?? undefined;
  const occurrenceSeq = searchParams.get("occurrence_seq") ?? undefined;
  const hasOccurrenceCursor = Boolean(occurrenceTime && occurrenceSeq);
  const queryClient = useQueryClient();
  const [selectedOccurrence, setSelectedOccurrence] = useState<Occurrence>();
  const selectedTrigger = useRef<HTMLButtonElement>(null);
  const issue = useQuery({
    ...issueQuery(user?.id ?? "unknown", projectId, id),
    enabled: Boolean(user && id),
  });
  const occurrences = useQuery({
    ...occurrencesQuery(
      user?.id ?? "unknown",
      projectId,
      id,
      hasOccurrenceCursor ? occurrenceTime : undefined,
      hasOccurrenceCursor ? occurrenceSeq : undefined,
    ),
    enabled: Boolean(user && issue.data),
  });
  const occurrenceDetail = useQuery({
    ...occurrenceDetailQuery(
      user?.id ?? "unknown",
      issue.data?.project_id ?? projectId,
      id,
      selectedOccurrence?.record_id ?? "unknown",
    ),
    enabled: Boolean(user && issue.data && selectedOccurrence),
  });
  const update = useMutation({
    mutationFn: (input: { status: IssueStatus; expectedRevision: string }) =>
      endpoints.updateIssue(id, {
        status: input.status,
        expected_revision: input.expectedRevision,
      }),
    onSuccess: (updated: Issue) => {
      queryClient.setQueryData(
        issueKeys.detail(user?.id ?? "unknown", projectId, id),
        updated,
      );
      void queryClient.invalidateQueries({
        queryKey: [
          ...issueKeys.project(user?.id ?? "unknown", updated.project_id),
          "list",
        ],
      });
    },
    onError: (error) => {
      if (error instanceof ApiError && error.status === 409) {
        void issue.refetch();
      }
    },
  });

  function updateStatus(status: IssueStatus) {
    if (issue.data) {
      update.mutate({ status, expectedRevision: issue.data.revision });
    }
  }

  function nextOccurrencePage() {
    const cursor = occurrences.data?.next_cursor;
    if (!cursor) return;
    const next = new URLSearchParams(searchParams);
    next.set("occurrence_time", cursor.occurred_at_us);
    next.set("occurrence_seq", cursor.ingest_seq);
    setSearchParams(next);
  }

  function firstOccurrencePage() {
    const next = new URLSearchParams(searchParams);
    next.delete("occurrence_time");
    next.delete("occurrence_seq");
    setSearchParams(next);
  }

  function closeOccurrenceDetail() {
    setSelectedOccurrence(undefined);
    selectedTrigger.current?.focus();
  }

  const backSearch = new URLSearchParams(searchParams);
  backSearch.delete("occurrence_time");
  backSearch.delete("occurrence_seq");

  return (
    <div className="page-stack">
      <div>
        <Link className="back-link" to={`/issues?${backSearch}`}>
          ← Issues로 돌아가기
        </Link>
      </div>

      {issue.isPending ? <Spinner label="Issue 불러오는 중" /> : null}
      {issue.isError ? (
        <Notice tone="error">
          <span>{describeApiError(issue.error)}</span>
          <Button
            onClick={() => void issue.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}

      {issue.data ? (
        <>
          <header className="issue-detail-heading">
            <div>
              <p className="eyebrow">Issue 상세</p>
              <h1>{issue.data.title}</h1>
              <code>{issue.data.id}</code>
            </div>
            <IssueStatusBadge status={issue.data.status} />
          </header>

          {update.error instanceof ApiError && update.error.status === 409 ? (
            <Notice tone="warning">
              다른 변경이 먼저 저장되어 최신 Issue 상태를 다시 불러왔습니다.
            </Notice>
          ) : update.isError ? (
            <Notice tone="error">{describeApiError(update.error)}</Notice>
          ) : null}

          <section
            className="metadata-panel"
            aria-labelledby="issue-metadata-title"
          >
            <div className="section-heading">
              <div>
                <p className="eyebrow">실제 저장 metadata</p>
                <h2 id="issue-metadata-title">요약</h2>
              </div>
              <div className="button-row">
                {statuses
                  .filter((status) => status !== issue.data.status)
                  .map((status) => (
                    <Button
                      disabled={update.isPending}
                      key={status}
                      onClick={() => updateStatus(status)}
                      type="button"
                      variant={status === "ignored" ? "quiet" : "primary"}
                    >
                      {statusActionLabels[status]}
                    </Button>
                  ))}
              </div>
            </div>
            <dl className="metadata-grid">
              <div>
                <dt>프로젝트 ID</dt>
                <dd>{issue.data.project_id}</dd>
              </div>
              <div>
                <dt>발생 횟수</dt>
                <dd>{formatDecimal(issue.data.occurrence_count)}회</dd>
              </div>
              <div>
                <dt>레벨</dt>
                <dd>{issue.data.level}</dd>
              </div>
              <div>
                <dt>처음 발생</dt>
                <dd>{formatTimestampUs(issue.data.first_seen_us)}</dd>
              </div>
              <div>
                <dt>최근 발생</dt>
                <dd>{formatTimestampUs(issue.data.last_seen_us)}</dd>
              </div>
              <div>
                <dt>첫 릴리스</dt>
                <dd>{issue.data.first_release ?? "없음"}</dd>
              </div>
              <div>
                <dt>최근 릴리스</dt>
                <dd>{issue.data.last_release ?? "없음"}</dd>
              </div>
              <div>
                <dt>Culprit</dt>
                <dd>{issue.data.culprit ?? "없음"}</dd>
              </div>
              <div>
                <dt>Fingerprint</dt>
                <dd className="mono">{issue.data.fingerprint}</dd>
              </div>
              <div>
                <dt>Revision</dt>
                <dd>{issue.data.revision}</dd>
              </div>
              <div>
                <dt>해결 시각</dt>
                <dd>
                  {issue.data.resolved_at_us
                    ? formatTimestampUs(issue.data.resolved_at_us)
                    : "없음"}
                </dd>
              </div>
              <div>
                <dt>Resolve 수신 경계</dt>
                <dd>
                  {issue.data.resolved_through_ingest_seq
                    ? formatDecimal(issue.data.resolved_through_ingest_seq)
                    : "없음"}
                </dd>
              </div>
              <div>
                <dt>최근 metadata 변경</dt>
                <dd>{formatTimestampUs(issue.data.updated_at_us)}</dd>
              </div>
            </dl>
          </section>

          <section
            className="metadata-panel"
            aria-labelledby="occurrence-title"
          >
            <div className="section-heading">
              <div>
                <p className="eyebrow">Occurrence metadata</p>
                <h2 id="occurrence-title">발생 기록</h2>
              </div>
            </div>
            {occurrences.isPending ? (
              <Spinner label="발생 기록 불러오는 중" />
            ) : null}
            {occurrences.isError ? (
              <Notice tone="error">
                <span>{describeApiError(occurrences.error)}</span>
                <Button
                  onClick={() => void occurrences.refetch()}
                  type="button"
                  variant="quiet"
                >
                  다시 시도
                </Button>
              </Notice>
            ) : null}
            {occurrences.data?.items.length === 0 ? (
              <p className="inline-empty">표시할 발생 기록이 없습니다.</p>
            ) : null}
            {occurrences.data?.items.map((occurrence) => (
              <article className="occurrence-row" key={occurrence.record_id}>
                <div>
                  <strong>
                    {formatTimestampUs(occurrence.occurred_at_us)}
                  </strong>
                  <span>Ingest #{formatDecimal(occurrence.ingest_seq)}</span>
                </div>
                <code>{occurrence.record_id}</code>
                {occurrence.source_event_id ? (
                  <span>Source {occurrence.source_event_id}</span>
                ) : null}
                <Button
                  onClick={(event) => {
                    selectedTrigger.current = event.currentTarget;
                    setSelectedOccurrence(occurrence);
                  }}
                  type="button"
                  variant="quiet"
                >
                  원문 보기
                </Button>
              </article>
            ))}
            {selectedOccurrence ? (
              <RecordDetailPanel
                error={
                  occurrenceDetail.isError
                    ? describeApiError(occurrenceDetail.error)
                    : undefined
                }
                heading="발생 기록 상세"
                onClose={closeOccurrenceDetail}
                onRetry={() => void occurrenceDetail.refetch()}
                pending={occurrenceDetail.isPending}
                raw={occurrenceDetail.data?.raw}
                recordId={selectedOccurrence.record_id}
              />
            ) : null}
            {occurrences.data ? (
              <nav className="page-controls" aria-label="발생 기록 페이지 이동">
                <Button
                  disabled={!hasOccurrenceCursor}
                  onClick={firstOccurrencePage}
                  type="button"
                  variant="quiet"
                >
                  처음으로
                </Button>
                <Button
                  disabled={!occurrences.data.next_cursor}
                  onClick={nextOccurrencePage}
                  type="button"
                >
                  다음 페이지
                </Button>
              </nav>
            ) : null}
          </section>
        </>
      ) : null}
    </div>
  );
}
