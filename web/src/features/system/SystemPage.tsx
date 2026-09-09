import { useQuery } from "@tanstack/react-query";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";

function bytes(value: string | number): string {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return String(value);
  return new Intl.NumberFormat("ko-KR", {
    style: "unit",
    unit: "megabyte",
    maximumFractionDigits: 1,
  }).format(amount / 1024 / 1024);
}

export function SystemPage() {
  const user = useSession().data;
  const status = useQuery({
    queryKey: ["system-status", user?.id ?? "unknown"],
    queryFn: ({ signal }) => endpoints.systemStatus(signal),
    enabled: user?.role === "admin",
    retry: false,
  });
  const doctor = useQuery({
    queryKey: ["system-doctor", user?.id ?? "unknown"],
    queryFn: ({ signal }) => endpoints.systemDoctor(signal),
    enabled: false,
    retry: false,
  });
  if (user?.role !== "admin") {
    return <Notice tone="error">시스템 상태는 관리자만 볼 수 있습니다.</Notice>;
  }
  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <p className="eyebrow">운영</p>
          <h1>시스템 상태</h1>
          <p>수집 가능 여부, 로컬 보관과 원격 복구 가능 범위를 구분합니다.</p>
        </div>
        <Button
          onClick={() => void status.refetch()}
          type="button"
          variant="quiet"
        >
          새로고침
        </Button>
      </header>
      {status.isPending ? <Spinner label="시스템 상태 확인 중" /> : null}
      {status.isError ? (
        <Notice tone="error">{describeApiError(status.error)}</Notice>
      ) : null}
      {status.data ? (
        <>
          <section className="system-grid" aria-label="운영 상태">
            <article className="panel">
              <span>Core / Ingest</span>
              <strong>{status.data.ready ? "ready" : "unavailable"}</strong>
              <p>{status.data.ingest_accepting ? "수집 허용" : "수집 차단"}</p>
              <small>적용 seq {status.data.applied_ingest_seq}</small>
            </article>
            <article className="panel">
              <span>Durable Inbox</span>
              <strong>{status.data.inbox_records} records</strong>
              <p>{bytes(status.data.inbox_bytes)}</p>
              <small>적용 inbox {status.data.applied_inbox_id}</small>
            </article>
            <article className="panel">
              <span>Disk</span>
              <strong>{bytes(status.data.disk.free_bytes)} free</strong>
              <p>예약 {bytes(status.data.disk.reserved_bytes)}</p>
              <small>
                보호 하한 {bytes(status.data.disk.minimum_free_bytes)}
              </small>
            </article>
            <article className="panel">
              <span>Backup</span>
              <strong>{status.data.backup.state}</strong>
              <p>
                {status.data.backup.recoverable_through_ingest_seq
                  ? `복구 가능 seq ${status.data.backup.recoverable_through_ingest_seq}`
                  : "완료된 복구 지점 없음"}
              </p>
              <small>lag {status.data.backup.lag_records ?? "—"} records</small>
            </article>
          </section>
          <section className="panel system-details">
            <h2>Replay 수집 / 보존</h2>
            <p>
              보관 중 {status.data.replay.active_replays} · Partial{" "}
              {status.data.replay.partial_replays} · 만료 정리 대기{" "}
              {status.data.replay.expired_replays}
            </p>
            <p>
              {status.data.replay.segments} segments · 참조 중인 압축 데이터{" "}
              {bytes(status.data.replay.referenced_bytes)}
            </p>
            <p>
              Replay 백업 변경분:{" "}
              {status.data.replay.backup_pending ? "미반영" : "없음"} · 백업
              구성 여부는 위 Backup 상태를 확인하세요.
            </p>
            <p>
              로컬 보존 정리: {status.data.replay_maintenance?.state ?? "중지"}{" "}
              · 최근 성공{" "}
              {status.data.replay_maintenance?.last_success_us
                ? new Date(
                    status.data.replay_maintenance.last_success_us / 1000,
                  ).toLocaleString()
                : "없음"}
            </p>
            <p>
              프로세스 시작 이후 정리:{" "}
              {status.data.replay_maintenance?.deleted_files ?? 0} files ·{" "}
              {bytes(status.data.replay_maintenance?.deleted_bytes ?? 0)}
            </p>
            <p className="muted">
              30일 만료 데이터는 즉시 조회에서 제외합니다. 매분 유휴 구간에서
              작은 세션 최대 16개(큰 세션 1개)·파일 최대 256개를 정리하며
              조회·수집·백업 중에는 다음 주기로 미룹니다. 원격 복구 지점의
              객체는 삭제하지 않습니다.
            </p>
            <h3>Sentry 수집 응답</h3>
            <dl>
              {Object.entries(status.data.sentry_ingest_since_start).map(
                ([code, count]) => (
                  <div key={code}>
                    <dt>{code}</dt>
                    <dd>{count}</dd>
                  </div>
                ),
              )}
            </dl>
            <p className="muted">
              현재 프로세스가 응답한 전체 Sentry 요청 수입니다. Replay 전용·고유
              세션 수가 아니며 재시도와 지원하지 않아 무시된 item도 포함됩니다.
              재시작 시 초기화됩니다.
            </p>
          </section>
          <section className="panel system-details">
            <h2>Shard 보관 상태</h2>
            <dl>
              <div>
                <dt>Active</dt>
                <dd>{status.data.shards.active}</dd>
              </div>
              <div>
                <dt>로컬 전용 archive</dt>
                <dd>{status.data.shards.local}</dd>
              </div>
              <div>
                <dt>원격 복구 검증 + 로컬</dt>
                <dd>{status.data.shards.remote_verified}</dd>
              </div>
              <div>
                <dt>원격 복구 전용</dt>
                <dd>{status.data.shards.remote_only}</dd>
              </div>
              <div>
                <dt>전체 records</dt>
                <dd>{status.data.shards.records}</dd>
              </div>
              <div>
                <dt>복구 가능한 records</dt>
                <dd>{status.data.shards.recoverable_records}</dd>
              </div>
            </dl>
          </section>
          <section className="panel system-details">
            <div className="section-heading">
              <div>
                <p className="eyebrow">읽기 전용 검사</p>
                <h2>Doctor</h2>
              </div>
              <Button onClick={() => void doctor.refetch()} type="button">
                검사 실행
              </Button>
            </div>
            {doctor.isFetching ? <Spinner label="DB와 shard 검사 중" /> : null}
            {doctor.isError ? (
              <Notice tone="error">{describeApiError(doctor.error)}</Notice>
            ) : null}
            {doctor.data ? (
              <Notice tone="success">
                schema v{doctor.data.schema_version}, local{" "}
                {doctor.data.checked_local_shards}, remote-only{" "}
                {doctor.data.checked_remote_only_shards} 검사 통과
              </Notice>
            ) : null}
          </section>
        </>
      ) : null}
    </div>
  );
}
