import { FeedbackPanel } from "./FeedbackPanel";
import { useState } from "react";
import { PageMaps } from "./PageMaps";
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { Notice } from "../../components/Notice";
import { Button } from "../../components/Button";
import { useSession } from "../auth";
import { projectsQuery } from "../projects";
import { replaysQuery, mapsQuery } from "./queries";
import { duration, userLabel } from "./presentation";

export function ReplaysPage() {
  const session = useSession();
  const user = session.data;
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: !!user,
  });
  const [params, setParams] = useSearchParams();
  const active = projects.data?.filter((p) => p.is_active) ?? [];
  const selected = params.get("project");
  const project =
    active.find((p) => p.id === selected) ?? (selected ? undefined : active[0]);
  const query = new URLSearchParams(params);
  query.delete("project");
  if (project) query.set("project_id", project.id);
  const replays = useQuery({
    ...replaysQuery(user?.id ?? "unknown", query.toString()),
    enabled: !!user && !!project,
  });
  const [showMaps, setShowMaps] = useState(false);
  const [showFeedback, setShowFeedback] = useState(false);
  const maps = useQuery({
    ...mapsQuery(user?.id ?? "unknown", query.toString()),
    enabled: showMaps && !!user && !!project,
  });
  function change(name: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(name, value);
    else next.delete(name);
    next.delete("before_started_ms");
    next.delete("before_id");
    setParams(next);
  }
  return (
    <section className="page-stack">
      <header className="page-heading">
        <div>
          <h1>Replays</h1>
          <p>공식 Sentry SDK가 샘플링한 세션의 화면과 사용자 행동</p>
        </div>
        <Button onClick={() => void replays.refetch()}>새로고침</Button>
      </header>
      {projects.isError && (
        <Notice tone="error">{describeApiError(projects.error)}</Notice>
      )}
      <div className="replay-filters">
        <label>
          프로젝트
          <select
            value={project?.id ?? ""}
            onChange={(e) => change("project", e.target.value)}
          >
            <option value="">선택</option>
            {active.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </label>
        {[
          ["environment", "Environment"],
          ["release", "Release"],
          ["url", "URL"],
          ["user", "User"],
        ].map(([name, label]) => (
          <label key={name}>
            {label}
            <input
              value={params.get(name) ?? ""}
              onChange={(e) => change(name, e.target.value)}
            />
          </label>
        ))}
        {[
          ["has_error", "Error"],
          ["rage_click", "Rage click"],
          ["dead_click", "Dead click"],
        ].map(([name, label]) => (
          <label key={name}>
            {label}
            <select
              value={params.get(name) ?? ""}
              onChange={(e) => change(name, e.target.value)}
            >
              <option value="">전체</option>
              <option value="true">있음</option>
              <option value="false">없음</option>
            </select>
          </label>
        ))}
        <label>
          최소 시간 (초)
          <input
            type="number"
            min="0"
            value={
              params.has("min_duration_ms")
                ? Number(params.get("min_duration_ms")) / 1000
                : ""
            }
            onChange={(e) =>
              change(
                "min_duration_ms",
                e.target.value ? String(Number(e.target.value) * 1000) : "",
              )
            }
          />
        </label>
      </div>
      {!project && <Notice>활성 프로젝트를 선택해 주세요.</Notice>}
      {project && replays.isPending && <Notice>Replay를 불러오는 중…</Notice>}
      {replays.isError && (
        <Notice tone="error">{describeApiError(replays.error)}</Notice>
      )}
      {replays.data && (
        <>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>User</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th>URLs</th>
                  <th>Errors</th>
                  <th>Frustration</th>
                  <th>상태</th>
                </tr>
              </thead>
              <tbody>
                {replays.data.items.map((r) => (
                  <tr key={r.metadata.replay_id}>
                    <td>
                      <Link
                        to={`/replays/${r.project_id}/${r.metadata.replay_id}`}
                      >
                        {userLabel(r.metadata.user)}
                      </Link>
                    </td>
                    <td>
                      {new Date(r.metadata.started_at_ms).toLocaleString()}
                    </td>
                    <td>
                      {duration(
                        r.metadata.finished_at_ms - r.metadata.started_at_ms,
                      )}
                    </td>
                    <td>{r.metadata.urls.length}</td>
                    <td>{r.metadata.error_ids.length}</td>
                    <td>
                      {r.frustration.rage
                        ? `🔥 Rage ${r.frustration.rage} `
                        : ""}
                      {r.frustration.dead ? `Dead ${r.frustration.dead} ` : ""}
                      {r.frustration.slow ? `Slow ${r.frustration.slow}` : ""}
                      {!r.frustration.slow && !r.frustration.multi ? "—" : ""}
                    </td>
                    <td>
                      {r.partial
                        ? "Partial replay"
                        : `${r.segment_count} segments`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!replays.data.items.length && (
            <Notice>
              조건에 맞는 Replay가 없습니다. 프로젝트에 공식 replayIntegration과
              sampling 설정이 필요합니다.
            </Notice>
          )}
          {replays.data.next_cursor && (
            <Button
              onClick={() => {
                const next = new URLSearchParams(params);
                const cursor = replays.data.next_cursor;
                if (cursor) {
                  next.set(
                    "before_started_ms",
                    String(cursor.before_started_ms),
                  );
                  next.set("before_id", cursor.before_id);
                  setParams(next);
                }
              }}
            >
              다음
            </Button>
          )}
          {params.has("before_id") && (
            <Button onClick={() => change("before_id", "")}>처음으로</Button>
          )}
        </>
      )}
      <Button disabled={!project} onClick={() => setShowMaps(!showMaps)}>
        {showMaps ? "Page maps 닫기" : "페이지별 Heatmaps"}
      </Button>
      {showMaps && maps.isPending && (
        <Notice>페이지별 행동 데이터를 분석하는 중…</Notice>
      )}
      {showMaps && maps.isError && (
        <Notice tone="error">{describeApiError(maps.error)}</Notice>
      )}
      {showMaps && maps.data && (
        <>
          <h2>Page maps</h2>
          <Notice>
            현재 필터의 최근 Replay {maps.data.replays_analyzed}개를
            분석했습니다.
            {maps.data.truncated
              ? " 처리 한도에 따라 일부 구간만 포함합니다."
              : ""}{" "}
            전체 방문자의 집계가 아닙니다.
          </Notice>
          <PageMaps pages={maps.data.pages} />
        </>
      )}
      <Button
        disabled={!project}
        onClick={() => setShowFeedback(!showFeedback)}
      >
        Sentry User Feedback
      </Button>
      {showFeedback && project && user && (
        <FeedbackPanel user={user.id} project={project.id} />
      )}
      <p className="muted">
        Duration은 마지막 관측 시점까지입니다. 종료 확정·전체 방문자 수를
        의미하지 않습니다. 조회 가능 기간 30일.
      </p>
    </section>
  );
}
