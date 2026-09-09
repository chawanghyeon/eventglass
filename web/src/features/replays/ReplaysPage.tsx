import { useState } from "react";
import { FeedbackPanel } from "./FeedbackPanel";
import { PageMaps } from "./PageMaps";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useLocation, useSearchParams } from "react-router-dom";
import { describeApiError } from "../../api/client";
import { Notice } from "../../components/Notice";
import { Button } from "../../components/Button";
import { useSession } from "../auth";
import { projectsQuery } from "../projects";
import { replaysQuery, mapsQuery } from "./queries";
import { displayPage, duration, userLabel } from "./presentation";

export function ReplaysPage() {
  const session = useSession();
  const cache = useQueryClient();
  const user = session.data;
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: !!user,
  });
  const [params, setParams] = useSearchParams();
  const [draftState, setDraftState] = useState(() => ({
    source: params.toString(),
    values: new URLSearchParams(params),
  }));
  if (draftState.source !== params.toString()) {
    setDraftState({
      source: params.toString(),
      values: new URLSearchParams(params),
    });
  }
  const draft =
    draftState.source === params.toString() ? draftState.values : params;
  function editFilter(name: string, value: string) {
    const next = new URLSearchParams(draft);
    if (value) next.set(name, value);
    else next.delete(name);
    next.delete("before_started_ms");
    next.delete("before_id");
    setDraftState({ source: params.toString(), values: next });
  }
  const location = useLocation();
  const active = projects.data?.filter((p) => p.is_active) ?? [];
  const selected = params.get("project");
  const project =
    active.find((p) => p.id === selected) ?? (selected ? undefined : active[0]);
  const query = new URLSearchParams(params);
  query.delete("project");
  query.delete("viewport");
  query.delete("view");
  if (project) query.set("project_id", project.id);
  const replays = useQuery({
    ...replaysQuery(user?.id ?? "unknown", query.toString()),
    enabled: !!user && !!project,
  });
  const requestedView = params.get("view");
  const view =
    requestedView === "pages" || requestedView === "feedback"
      ? requestedView
      : "recordings";
  const showMaps = view === "pages";
  const showFeedback = view === "feedback";
  const extraFilters = [
    "environment",
    "release",
    "user",
    "rage_click",
    "dead_click",
    "started_after_ms",
    "started_before_ms",
    "min_duration_ms",
  ];
  const activeFilterCount = extraFilters.filter((key) =>
    params.has(key),
  ).length;
  const mapQuery = new URLSearchParams(query);
  const viewport = params.get("viewport");
  if (viewport) mapQuery.set("viewport", viewport);
  const maps = useQuery({
    ...mapsQuery(user?.id ?? "unknown", mapQuery.toString()),
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
  if (projects.isSuccess && !project) {
    return (
      <section className="page-stack">
        <header className="page-heading">
          <div>
            <h1>방문 분석</h1>
            <p>방문자의 화면과 행동을 확인하세요.</p>
          </div>
        </header>
        <div className="getting-started">
          <span className="setup-symbol" aria-hidden="true">
            ▷
          </span>
          <h2>
            {active.length
              ? "다른 프로젝트를 선택해 주세요"
              : "웹사이트를 연결해 주세요"}
          </h2>
          <p>
            {active.length
              ? "이전에 선택한 프로젝트가 없거나 중지되었습니다."
              : "공식 Sentry SDK가 보내는 방문 기록을 재생하고, 클릭과 스크롤을 분석합니다."}
          </p>
          {active.length ? (
            <label>
              프로젝트
              <select
                value=""
                onChange={(e) => change("project", e.target.value)}
              >
                <option value="" disabled>
                  프로젝트 선택
                </option>
                {active.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
            </label>
          ) : user?.role === "admin" ? (
            <>
              <ol className="setup-steps">
                <li>
                  <strong>프로젝트 만들기</strong>
                  <span>분석할 웹사이트를 등록합니다.</span>
                </li>
                <li>
                  <strong>Sentry SDK 연결</strong>
                  <span>연결 주소와 설정 예제를 복사합니다.</span>
                </li>
                <li>
                  <strong>방문 기록 확인</strong>
                  <span>수집된 세션을 재생합니다.</span>
                </li>
              </ol>
              <Link className="button button--primary" to="/projects">
                웹사이트 연결하기 →
              </Link>
            </>
          ) : (
            <p>관리자에게 프로젝트 연결을 요청해 주세요.</p>
          )}
        </div>
      </section>
    );
  }
  return (
    <section className="page-stack">
      <header className="page-heading">
        <div>
          <h1>방문 분석</h1>
          <p>방문을 재생하고, 페이지에서 막힌 지점을 찾으세요.</p>
        </div>
        <Button
          disabled={!project}
          onClick={() => {
            if (showMaps) void maps.refetch();
            else if (showFeedback)
              void cache.invalidateQueries({
                queryKey: ["feedback", user?.id, project?.id],
              });
            else void replays.refetch();
          }}
        >
          새로고침
        </Button>
      </header>
      <nav className="view-switcher" aria-label="Replay 보기">
        {[
          ["recordings", "방문 기록"],
          ["pages", "페이지 분석"],
          ["feedback", "사용자 피드백"],
        ].map(([value, label]) => (
          <Button
            key={value}
            variant={view === value ? "primary" : "quiet"}
            aria-pressed={view === value}
            onClick={() => change("view", value)}
          >
            {label}
          </Button>
        ))}
      </nav>
      {projects.isError && (
        <Notice tone="error">{describeApiError(projects.error)}</Notice>
      )}
      <form
        className="replay-search-form"
        onSubmit={(event) => {
          event.preventDefault();
          setParams(new URLSearchParams(draft));
        }}
      >
        <div className="replay-filters search-toolbar">
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
          {!showFeedback && (
            <>
              <label className="replay-url-search">
                페이지 검색
                <input
                  placeholder="/products/…"
                  value={draft.get("url") ?? ""}
                  onChange={(event) => editFilter("url", event.target.value)}
                />
              </label>

              <label>
                오류 포함
                <select
                  value={draft.get("has_error") ?? ""}
                  onChange={(e) => editFilter("has_error", e.target.value)}
                >
                  <option value="">전체</option>
                  <option value="true">있음</option>
                  <option value="false">없음</option>
                </select>
              </label>
            </>
          )}
          {!showFeedback && <Button type="submit">검색</Button>}
        </div>
        {!showFeedback && (
          <details className="disclosure">
            <summary>
              추가 필터
              {activeFilterCount ? ` · ${activeFilterCount}개 적용 중` : ""}
            </summary>
            <div className="replay-filters">
              {[
                ["environment", "환경"],
                ["release", "릴리스"],
                ["user", "사용자"],
              ].map(([name, label]) => (
                <label key={name}>
                  {label}
                  <input
                    value={draft.get(name) ?? ""}
                    onChange={(e) => editFilter(name, e.target.value)}
                  />
                </label>
              ))}
              {[
                ["rage_click", "Rage click"],
                ["dead_click", "Dead click"],
              ].map(([name, label]) => (
                <label key={name}>
                  {label}
                  <select
                    value={draft.get(name) ?? ""}
                    onChange={(e) => editFilter(name, e.target.value)}
                  >
                    <option value="">전체</option>
                    <option value="true">있음</option>
                    <option value="false">없음</option>
                  </select>
                </label>
              ))}
              {[
                ["started_after_ms", "세션 시작 이후"],
                ["started_before_ms", "세션 시작 이전"],
              ].map(([name, label]) => (
                <label key={name}>
                  {label} (현지 시간)
                  <input
                    type="datetime-local"
                    value={localInput(draft.get(name))}
                    onChange={(event) =>
                      editFilter(
                        name,
                        event.target.value
                          ? String(new Date(event.target.value).getTime())
                          : "",
                      )
                    }
                  />
                </label>
              ))}
              <label>
                최소 시간 (초)
                <input
                  type="number"
                  min="0"
                  value={
                    draft.has("min_duration_ms")
                      ? Number(draft.get("min_duration_ms")) / 1000
                      : ""
                  }
                  onChange={(e) =>
                    editFilter(
                      "min_duration_ms",
                      e.target.value
                        ? String(Number(e.target.value) * 1000)
                        : "",
                    )
                  }
                />
              </label>
            </div>
          </details>
        )}
      </form>
      {Array.from(params.keys()).some(
        (key) => !["project", "view", "viewport"].includes(key),
      ) && (
        <div className="button-row">
          <div className="filter-chips" aria-label="적용된 필터">
            {[
              ["url", "페이지"],
              ["has_error", "오류"],
              ["environment", "환경"],
              ["release", "릴리스"],
              ["user", "사용자"],
              ["rage_click", "Rage click"],
              ["dead_click", "Dead click"],
              ["started_after_ms", "시작 이후"],
              ["started_before_ms", "시작 이전"],
              ["min_duration_ms", "최소 시간"],
            ]
              .filter(([key]) => params.has(key))
              .map(([key, label]) => (
                <button
                  type="button"
                  key={key}
                  aria-label={`${label} 조건 삭제`}
                  onClick={() => change(key, "")}
                >
                  {label}:{" "}
                  {params.get(key) === "true"
                    ? "있음"
                    : params.get(key) === "false"
                      ? "없음"
                      : key === "min_duration_ms"
                        ? `${Number(params.get(key)) / 1000}초`
                        : key.startsWith("started_")
                          ? new Date(Number(params.get(key))).toLocaleString(
                              "ko-KR",
                            )
                          : params.get(key)}{" "}
                  ×
                </button>
              ))}
          </div>
          <Button
            variant="quiet"
            onClick={() => {
              const next = new URLSearchParams();
              for (const key of ["project", "view", "viewport"]) {
                const value = params.get(key);
                if (value) next.set(key, value);
              }
              setParams(next);
            }}
          >
            필터 초기화
          </Button>
        </div>
      )}
      {project && replays.isPending && <Notice>Replay를 불러오는 중…</Notice>}
      {replays.isError && (
        <Notice tone="error">{describeApiError(replays.error)}</Notice>
      )}
      {!showMaps && !showFeedback && replays.data && (
        <>
          <div className="table-scroll">
            <table className="replay-table">
              <thead>
                <tr>
                  <th>사용자</th>
                  <th>시작</th>
                  <th>재생 시간</th>
                  <th>페이지</th>
                  <th>오류</th>
                  <th>불편 신호</th>
                  <th>상태</th>
                </tr>
              </thead>
              <tbody>
                {replays.data.items.map((r) => (
                  <tr key={r.metadata.replay_id}>
                    <td>
                      <Link
                        to={`/replays/${r.project_id}/${r.metadata.replay_id}`}
                        state={{
                          replaysReturnTo: location.pathname + location.search,
                        }}
                        className="replay-entry"
                      >
                        <strong>{userLabel(r.metadata.user)}</strong>
                        <span>▶ 방문 기록 보기</span>
                      </Link>
                      <p
                        className="replay-entry-url"
                        title={r.metadata.urls[0]}
                      >
                        {r.metadata.urls[0]
                          ? displayPage(r.metadata.urls[0])
                          : "페이지 정보 없음"}
                      </p>
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
                      {r.frustration.slow ? `Slow ${r.frustration.slow} ` : ""}
                      {r.frustration.multi
                        ? `Multi ${r.frustration.multi}`
                        : ""}
                      {!r.frustration.rage &&
                      !r.frustration.dead &&
                      !r.frustration.slow &&
                      !r.frustration.multi
                        ? "—"
                        : ""}
                    </td>
                    <td title={`${r.segment_count}개 구간 수신`}>
                      {r.partial ? "일부 누락" : "수신됨"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!replays.data.items.length && (
            <Notice>
              조건에 맞는 Replay가 없습니다. 필터를 바꾸거나 웹사이트 연결
              설정을 확인해 주세요.
              {user?.role === "admin" && (
                <Link to="/projects">SDK 연결 설정 보기 →</Link>
              )}
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
      {showMaps && (
        <label>
          분석 화면 크기
          <select
            aria-label="분석 화면 크기"
            value={params.get("viewport") ?? ""}
            onChange={(e) => change("viewport", e.target.value)}
          >
            <option value="">전체</option>
            <option value="narrow">좁은 화면 · 768px 미만</option>
            <option value="wide">넓은 화면 · 768px 이상</option>
            <option value="mixed">크기 그룹 변경</option>
            <option value="unknown">크기 불명</option>
          </select>
        </label>
      )}
      {showMaps && maps.isPending && (
        <Notice>페이지별 행동 데이터를 분석하는 중…</Notice>
      )}
      {showMaps && maps.isError && (
        <Notice tone="error">{describeApiError(maps.error)}</Notice>
      )}
      {showMaps && maps.data && (
        <>
          <h2>페이지 분석</h2>
          <Notice>
            최근 샘플 세션 {maps.data.replays_analyzed}개 분석 · 최대 20개.
            {maps.data.truncated
              ? " 처리 한도에 따라 일부 구간만 포함합니다."
              : ""}{" "}
            전체 방문자의 집계가 아닙니다.
          </Notice>
          <PageMaps pages={maps.data.pages} project={project?.id} />
        </>
      )}
      {showFeedback && project && user && (
        <FeedbackPanel user={user.id} project={project.id} />
      )}
      <p className="muted">
        최근 30일의 샘플 세션입니다. 재생 시간은 마지막 관측 시점까지이며 전체
        방문자 통계가 아닙니다.
      </p>
    </section>
  );
}

function localInput(value: string | null): string {
  if (!value) return "";
  const date = new Date(Number(value));
  if (!Number.isFinite(date.getTime())) return "";
  return new Date(date.getTime() - date.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
}
