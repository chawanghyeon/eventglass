import { useState, type FormEvent } from "react";
import { Button } from "../../components/Button";
import {
  groupFields,
  histogramIntervals,
  metricOps,
  type ExploreState,
  type GroupField,
  type HistogramInterval,
  type MetricOp,
} from "./state";

const metricLabels: Record<MetricOp, string> = {
  count: "건수",
  sum: "합계",
  min: "최솟값",
  max: "최댓값",
  avg: "평균",
};
const groupLabels: Record<GroupField, string> = {
  service: "서비스",
  level: "레벨",
  environment: "환경",
  release: "릴리스",
};

export function ExploreControls({
  committed,
  onApply,
}: {
  committed: ExploreState;
  onApply: (state: ExploreState) => void;
}) {
  const [metric, setMetric] = useState(committed.metric);
  const [field, setField] = useState(committed.field);
  const [groups, setGroups] = useState(committed.groups);
  const [interval, setInterval] = useState(committed.interval);

  function submit(event: FormEvent) {
    event.preventDefault();
    onApply({ ...committed, metric, field: field.trim(), groups, interval });
  }

  return (
    <form className="explore-controls" onSubmit={submit}>
      <label>
        Metric
        <select
          onChange={(event) => setMetric(event.target.value as MetricOp)}
          value={metric}
        >
          {metricOps.map((value) => (
            <option key={value} value={value}>
              {metricLabels[value]}
            </option>
          ))}
        </select>
      </label>
      <label>
        숫자 필드
        <input
          disabled={metric === "count"}
          maxLength={523}
          onChange={(event) => setField(event.target.value)}
          placeholder="attributes.duration_ms"
          required={metric !== "count"}
          value={field}
        />
      </label>
      <fieldset>
        <legend>Group by · 최대 2개</legend>
        <div>
          {groupFields.map((group) => (
            <label key={group}>
              <input
                checked={groups.includes(group)}
                disabled={!groups.includes(group) && groups.length >= 2}
                onChange={(event) =>
                  setGroups((current) =>
                    event.target.checked
                      ? [...current, group]
                      : current.filter((value) => value !== group),
                  )
                }
                type="checkbox"
              />
              {groupLabels[group]}
            </label>
          ))}
        </div>
      </fieldset>
      <label>
        Histogram
        <select
          onChange={(event) =>
            setInterval(event.target.value as HistogramInterval | "none")
          }
          value={interval}
        >
          <option value="none">사용 안 함</option>
          {histogramIntervals.map((value) => (
            <option key={value} value={value}>
              {value === "auto" ? "자동" : value}
            </option>
          ))}
        </select>
      </label>
      <Button type="submit">집계 적용</Button>
    </form>
  );
}
