import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Alert, AlertCondition, AlertInput } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { projectsQuery } from "../projects";
import { alertDeliveriesQuery, alertKeys, alertsQuery } from "./queries";

type ConditionType = AlertCondition["type"];
type ThresholdCondition = Extract<AlertCondition, { query: string }>;

const initial: AlertInput = {
  name: "",
  project_id: null,
  condition: { type: "new_issue" },
  destination: { type: "webhook", url: "" },
  enabled: true,
};

function threshold(type: "error_count" | "log_count"): AlertCondition {
  return {
    type,
    query: "",
    window_seconds: 300,
    threshold: 1,
    cooldown_seconds: 600,
    time_basis: "received_at",
  };
}

function thresholdCondition(
  condition: AlertCondition,
): condition is ThresholdCondition {
  return condition.type === "error_count" || condition.type === "log_count";
}

function time(value: string | null): string {
  if (!value) return "없음";
  return new Date(Number(BigInt(value) / 1000n)).toLocaleString("ko-KR");
}

export function AlertsPage() {
  const session = useSession();
  const user = session.data;
  const userId = user?.id ?? "unknown";
  const client = useQueryClient();
  const [form, setForm] = useState<AlertInput>(initial);
  const [editing, setEditing] = useState<Pick<Alert, "id" | "revision">>();
  const alerts = useQuery({
    ...alertsQuery(userId),
    enabled: user?.role === "admin",
  });
  const deliveries = useQuery({
    ...alertDeliveriesQuery(userId),
    enabled: user?.role === "admin",
  });
  const projects = useQuery({
    ...projectsQuery(userId),
    enabled: user?.role === "admin",
  });
  const refresh = () =>
    Promise.all([
      client.invalidateQueries({ queryKey: alertKeys.all(userId) }),
      client.invalidateQueries({ queryKey: alertKeys.deliveries(userId) }),
    ]);
  const create = useMutation({
    mutationFn: (input: AlertInput) => endpoints.createAlert(input),
    onSuccess: async () => {
      setForm(initial);
      await refresh();
    },
  });
  const save = useMutation({
    mutationFn: (input: { id: string; revision: number; form: AlertInput }) =>
      endpoints.updateAlert(input.id, {
        ...input.form,
        revision: input.revision,
      }),
    onSuccess: async () => {
      setEditing(undefined);
      setForm(initial);
      await refresh();
    },
  });
  const update = useMutation({
    mutationFn: (alert: Alert) =>
      endpoints.updateAlert(alert.id, {
        name: alert.name,
        project_id: alert.project_id,
        condition: alert.condition,
        destination: alert.destination,
        enabled: !alert.enabled,
        revision: alert.revision,
      }),
    onSuccess: refresh,
  });
  const remove = useMutation({
    mutationFn: (alert: Alert) =>
      endpoints.deleteAlert(alert.id, alert.revision),
    onSuccess: refresh,
  });
  const retry = useMutation({
    mutationFn: (id: string) => endpoints.retryAlertDelivery(id),
    onSuccess: refresh,
  });
  const error =
    create.error ?? save.error ?? update.error ?? remove.error ?? retry.error;
  const loadingError = alerts.error ?? deliveries.error ?? projects.error;

  function updateThreshold(change: Partial<ThresholdCondition>) {
    setForm((current) =>
      thresholdCondition(current.condition)
        ? { ...current, condition: { ...current.condition, ...change } }
        : current,
    );
  }

  if (user?.role !== "admin") {
    return <Notice tone="error">경보 설정은 관리자만 볼 수 있습니다.</Notice>;
  }

  const conditionType = form.condition.type;
  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <p className="eyebrow">자동 감시</p>
          <h1>알림 규칙</h1>
          <p>
            수신 시각 window를 기본으로 평가하며 webhook은 at-least-once로
            전송합니다.
          </p>
        </div>
        {alerts.data ? (
          <span className="count-badge">{alerts.data.length}개</span>
        ) : null}
      </header>

      <form
        className="panel alert-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (editing) save.mutate({ ...editing, form });
          else create.mutate(form);
        }}
      >
        <div className="form-heading">
          <div>
            <p className="eyebrow">{editing ? "규칙 변경" : "새 규칙"}</p>
            <h2>{editing ? "경보 수정" : "경보 추가"}</h2>
          </div>
        </div>
        <div className="alert-form__grid">
          <label>
            이름
            <input
              maxLength={200}
              onChange={(event) =>
                setForm({ ...form, name: event.target.value })
              }
              required
              value={form.name}
            />
          </label>
          <label>
            프로젝트
            <select
              onChange={(event) =>
                setForm({ ...form, project_id: event.target.value || null })
              }
              value={form.project_id ?? ""}
            >
              <option value="">모든 활성 프로젝트</option>
              {projects.data?.map((project) => (
                <option key={project.id} value={project.id}>
                  {project.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            조건
            <select
              onChange={(event) => {
                const type = event.target.value as ConditionType;
                setForm({
                  ...form,
                  condition:
                    type === "error_count" || type === "log_count"
                      ? threshold(type)
                      : { type },
                });
              }}
              value={conditionType}
            >
              <option value="new_issue">새 Issue</option>
              <option value="regression">Regression</option>
              <option value="error_count">Error 수 임계값</option>
              <option value="log_count">Log query 수 임계값</option>
            </select>
          </label>
          <label>
            HTTPS webhook
            <input
              onChange={(event) =>
                setForm({
                  ...form,
                  destination: { type: "webhook", url: event.target.value },
                })
              }
              placeholder="https://hooks.example.com/eventglass"
              required
              type="url"
              value={form.destination.url}
            />
          </label>
          {thresholdCondition(form.condition) ? (
            <>
              <label>
                Query
                <input
                  maxLength={8192}
                  onChange={(event) =>
                    updateThreshold({ query: event.target.value })
                  }
                  value={form.condition.query}
                />
              </label>
              <label>
                Window (초)
                <input
                  min={60}
                  onChange={(event) =>
                    updateThreshold({
                      window_seconds: Number(event.target.value),
                    })
                  }
                  required
                  type="number"
                  value={form.condition.window_seconds}
                />
              </label>
              <label>
                임계값
                <input
                  min={1}
                  onChange={(event) =>
                    updateThreshold({ threshold: Number(event.target.value) })
                  }
                  required
                  type="number"
                  value={form.condition.threshold}
                />
              </label>
              <label>
                Cooldown (초)
                <input
                  min={0}
                  onChange={(event) =>
                    updateThreshold({
                      cooldown_seconds: Number(event.target.value),
                    })
                  }
                  required
                  type="number"
                  value={form.condition.cooldown_seconds}
                />
              </label>
              <label>
                시간 기준
                <select
                  onChange={(event) =>
                    updateThreshold({
                      time_basis: event.target.value as
                        "received_at" | "timestamp",
                    })
                  }
                  value={form.condition.time_basis}
                >
                  <option value="received_at">수신 시각</option>
                  <option value="timestamp">
                    발생 시각 (지연 도착 소급 없음)
                  </option>
                </select>
              </label>
            </>
          ) : null}
        </div>
        <div className="button-row">
          <Button disabled={create.isPending || save.isPending} type="submit">
            {create.isPending || save.isPending
              ? "저장 중…"
              : editing
                ? "변경 저장"
                : "경보 추가"}
          </Button>
          {editing ? (
            <Button
              type="button"
              variant="quiet"
              disabled={save.isPending}
              onClick={() => {
                setEditing(undefined);
                setForm(initial);
                save.reset();
              }}
            >
              수정 취소
            </Button>
          ) : null}
        </div>
      </form>

      {loadingError ? (
        <Notice tone="error">{describeApiError(loadingError)}</Notice>
      ) : null}
      {error ? <Notice tone="error">{describeApiError(error)}</Notice> : null}
      {alerts.isPending ? <Spinner label="경보 불러오는 중" /> : null}
      {alerts.data?.length === 0 ? (
        <Notice>설정된 경보가 없습니다.</Notice>
      ) : null}
      <div className="alert-grid">
        {alerts.data?.map((alert) => (
          <article className="panel alert-card" key={alert.id}>
            <header>
              <div>
                <p className="eyebrow">{alert.condition.type}</p>
                <h2>{alert.name}</h2>
              </div>
              <span className="count-badge">
                {alert.enabled ? "활성" : "중지"}
              </span>
            </header>
            <p>마지막 평가 {time(alert.last_evaluated_at_us)}</p>
            <p>마지막 발생 {time(alert.last_triggered_at_us)}</p>
            {alert.last_evaluation_error ? (
              <Notice tone="error">
                평가 실패: {alert.last_evaluation_error}
              </Notice>
            ) : null}
            <div className="page-controls">
              <Button
                type="button"
                variant="quiet"
                disabled={save.isPending}
                onClick={() => {
                  setEditing({ id: alert.id, revision: alert.revision });
                  setForm({
                    name: alert.name,
                    project_id: alert.project_id,
                    condition: alert.condition,
                    destination: alert.destination,
                    enabled: alert.enabled,
                  });
                  save.reset();
                }}
              >
                수정
              </Button>
              <Button
                disabled={update.isPending}
                onClick={() => update.mutate(alert)}
                type="button"
                variant="quiet"
              >
                {alert.enabled ? "중지" : "활성화"}
              </Button>
              <Button
                disabled={remove.isPending}
                onClick={() => remove.mutate(alert)}
                type="button"
                variant="danger"
              >
                삭제
              </Button>
            </div>
          </article>
        ))}
      </div>

      <section className="panel">
        <header className="form-heading">
          <div>
            <p className="eyebrow">Outbox</p>
            <h2>최근 전송</h2>
          </div>
        </header>
        {deliveries.isPending ? (
          <Spinner label="전송 기록 불러오는 중" />
        ) : null}
        {deliveries.data?.length === 0 ? <p>전송 기록이 없습니다.</p> : null}
        <ul className="delivery-list">
          {deliveries.data?.map((delivery) => (
            <li key={delivery.id}>
              <div>
                <strong>{delivery.state}</strong>
                <span>
                  시도 {delivery.attempts}회 · HTTP{" "}
                  {delivery.last_status_code ?? "-"} ·{" "}
                  {delivery.last_error ?? "오류 없음"}
                </span>
              </div>
              {delivery.state === "failed" ? (
                <Button
                  disabled={retry.isPending}
                  onClick={() => retry.mutate(delivery.id)}
                  type="button"
                  variant="quiet"
                >
                  같은 ID로 재시도
                </Button>
              ) : null}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
