import { useState } from "react";
import { Link } from "react-router-dom";
import type { ReplayPageActivity } from "../../api/types";
import { Notice } from "../../components/Notice";
import { duration } from "./presentation";

export function PageMaps({
  pages,
  project,
}: {
  pages: Record<string, ReplayPageActivity>;
  project?: string;
}) {
  const urls = Object.keys(pages);
  const [selected, setSelected] = useState("");
  const [mode, setMode] = useState<"clicks" | "movement">("clicks");
  const url = urls.includes(selected) ? selected : urls[0];
  const page = url ? pages[url] : undefined;
  if (!page)
    return (
      <Notice>좌표·viewport가 함께 확인된 페이지 데이터가 없습니다.</Notice>
    );
  const example = (kind: string, key: string) => {
    const found = page.examples.find((e) => e.kind === kind && e.key === key);
    return found && project && found.replay_id
      ? `/replays/${project}/${found.replay_id}?t=${found.timestamp_ms}`
      : undefined;
  };
  const evidence = (kind: string, key: string, label: React.ReactNode) => {
    const to = example(kind, key);
    return to ? (
      <Link to={to} title="관측된 Replay 시점 보기">
        {label}
      </Link>
    ) : (
      label
    );
  };
  const cells = page[mode];
  const max = Math.max(1, ...Object.values(cells));
  return (
    <div className="page-stack">
      <h3>상품·페이지 분석</h3>
      <div className="table-scroll">
        <table className="replay-table">
          <thead>
            <tr>
              <th>페이지</th>
              <th>관측 방문</th>
              <th>평균 관측 시간</th>
              <th>클릭</th>
              <th>Rage / Dead</th>
            </tr>
          </thead>
          <tbody>
            {urls.map((pageUrl) => {
              const item = pages[pageUrl];
              return (
                <tr key={pageUrl}>
                  <td>
                    <button
                      className="link-button"
                      onClick={() => setSelected(pageUrl)}
                    >
                      {pageUrl}
                    </button>
                  </td>
                  <td>{item.visits}</td>
                  <td>
                    {item.timed_visits
                      ? duration(item.observed_time_ms / item.timed_visits)
                      : "불명"}
                  </td>
                  <td>
                    {Object.values(item.clicks).reduce((a, b) => a + b, 0)}
                  </td>
                  <td>
                    {item.frustration.rage} / {item.frustration.dead}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="replay-controls">
        <label>
          페이지
          <select value={url} onChange={(e) => setSelected(e.target.value)}>
            {urls.map((u) => (
              <option key={u} value={u}>
                {u}
              </option>
            ))}
          </select>
        </label>
        <label>
          Map
          <select
            aria-label="Map"
            value={mode}
            onChange={(e) => setMode(e.target.value as "clicks" | "movement")}
          >
            <option value="clicks">Click heatmap</option>
            <option value="movement">Movement heatmap</option>
          </select>
        </label>
      </div>
      <div className="replay-page-summary">
        <div>
          <strong>{page.sampled_replays}</strong>
          <span>관측된 Replay</span>
        </div>
        <div>
          <strong>{page.visits}</strong>
          <span>페이지 방문 · 재방문 포함</span>
        </div>
        <div>
          <strong>
            {page.timed_visits
              ? duration(page.observed_time_ms / page.timed_visits)
              : "불명"}
          </strong>
          <span>평균 관측 시간</span>
        </div>
        <div>
          <strong>{page.last_observed_replays}</strong>
          <span>마지막으로 관측된 페이지</span>
        </div>
      </div>
      <details className="disclosure">
        <summary>집계 기준</summary>
        <p className="muted">
          페이지 진입 화면 크기: 768px 미만 {page.narrow_replays} Replay · 768px
          이상 {page.wide_replays} Replay. 크기가 바뀐 재방문은 양쪽에 포함될 수
          있습니다. 관측 시간에는 비활성 시간이 포함되며, 마지막 페이지라는
          사실은 이탈이나 구매 완료를 뜻하지 않습니다.
        </p>
      </details>
      <details className="disclosure">
        <summary>이동 경로와 관련 세션</summary>
        <div className="replay-columns">
          {(
            [
              ["이전 관측 페이지", page.previous_pages],
              ["다음 관측 페이지", page.next_pages],
            ] as const
          ).map(([label, links]) => (
            <div key={label}>
              <h3>{label}</h3>
              <ul>
                {Object.entries(links)
                  .sort((a, b) => b[1] - a[1])
                  .slice(0, 10)
                  .map(([target, count]) => (
                    <li key={target}>
                      <button
                        className="link-button"
                        onClick={() => setSelected(target)}
                      >
                        {target}
                      </button>{" "}
                      · {count}회
                    </li>
                  ))}
              </ul>
              {!Object.keys(links).length && (
                <p className="muted">연속된 이동이 확인되지 않았습니다.</p>
              )}
            </div>
          ))}
        </div>
        {project && page.replay_ids.length > 0 && (
          <div>
            <h3>실제 세션 확인</h3>
            <ul>
              {page.replay_ids.map((id, index) => (
                <li key={id}>
                  <Link to={`/replays/${project}/${id}`}>
                    관련 Replay {index + 1}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        )}
      </details>
      <details className="disclosure">
        <summary>Replay 연결 기준</summary>
        <p className="muted">
          아래 링크는 집계 중 대표 관측 1건의 실제 시점을 엽니다. 대표 시점은
          페이지당 최대 128개이며 모든 클릭을 나열하지 않습니다.
        </p>
      </details>
      <p>
        {evidence("frustration", "rage", `Rage ${page.frustration.rage}`)} ·{" "}
        {evidence("frustration", "dead", `Dead ${page.frustration.dead}`)} ·{" "}
        {evidence("frustration", "slow", `Slow ${page.frustration.slow}`)}
      </p>
      <svg
        className="replay-heatmap"
        viewBox="0 0 400 300"
        role="img"
        aria-label={`${mode === "clicks" ? "Click" : "Movement"} heatmap: ${url}`}
      >
        <rect width="400" height="300" fill="#eef2f6" />
        {Object.entries(cells).map(([cell, count]) => {
          const [x, y] = cell.split(",").map(Number);
          const dot = (
            <circle
              key={cell}
              cx={(x + 0.5) * 20}
              cy={(y + 0.5) * 15}
              r={12}
              fill="#ed563a"
              opacity={0.15 + 0.8 * Math.sqrt(count / max)}
            >
              <title>
                {count} samples · viewport {x * 5}% / {y * 5}%
              </title>
            </circle>
          );
          const to = example(mode, cell);
          return to ? (
            <Link
              key={cell}
              to={to}
              aria-label={`${count} samples · ${cell} · Replay 시점 보기`}
            >
              {dot}
            </Link>
          ) : (
            dot
          );
        })}
      </svg>
      <p className="muted">
        좌표는 각 시점의 viewport를 20×20으로 정규화했습니다. 스크린샷 위 위치나
        문서 전체 좌표가 아닙니다. iframe 내부와 viewport를 확인할 수 없는
        구간은 제외합니다.
      </p>
      <details className="disclosure">
        <summary>요소별 클릭</summary>
        <h3>Element clicks</h3>
        <table className="replay-table">
          <thead>
            <tr>
              <th>SDK selector</th>
              <th>Breadcrumbs</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(page.elements)
              .sort((a, b) => b[1] - a[1])
              .slice(0, 30)
              .map(([selector, count]) => (
                <tr key={selector}>
                  <td>
                    {evidence("element", selector, <code>{selector}</code>)}
                  </td>
                  <td>{count}</td>
                </tr>
              ))}
          </tbody>
        </table>
      </details>
      <details className="disclosure">
        <summary>스크롤 분석</summary>
        <h3>Scroll</h3>
        <h4>긴 페이지의 스크롤 구간별 클릭</h4>
        <div className="table-scroll">
          <table className="replay-table">
            <thead>
              <tr>
                <th>화면 상단의 스크롤 위치</th>
                <th>좌표 클릭</th>
                <th>관측된 SDK selector</th>
              </tr>
            </thead>
            <tbody>
              {Array.from(
                new Set([
                  ...Object.keys(page.depth_clicks),
                  ...Object.keys(page.depth_elements),
                ]),
              )
                .sort((a, b) => Number(a) - Number(b))
                .map((depth) => (
                  <tr key={depth}>
                    <td>
                      {evidence(
                        "depth",
                        depth,
                        `${depth}–${Number(depth) + 1} 화면 높이`,
                      )}
                    </td>
                    <td>{page.depth_clicks[depth] ?? 0}</td>
                    <td>
                      {Object.entries(page.depth_elements[depth] ?? {})
                        .sort((a, b) => b[1] - a[1])
                        .slice(0, 5)
                        .map(([label, count]) => (
                          <div key={label}>
                            <code>{label}</code> · {count}
                          </div>
                        ))}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
        <p className="muted">
          설명·리뷰 등 긴 페이지의 어느 스크롤 위치에서 행동했는지 확인합니다.
          고정 구매 버튼도 당시 화면 위치에 집계하며, DOM 구역 이름이나 구매
          의도를 추정하지 않습니다. SDK breadcrumb와 좌표 클릭 수는 수집 방식에
          따라 다를 수 있습니다.
        </p>
        <p>
          최대 관측 이동: viewport 높이의{" "}
          <strong>{page.max_scroll_viewports.toFixed(2)}배</strong> ·{" "}
          {page.scroll_samples} samples
        </p>
        <table className="replay-table">
          <thead>
            <tr>
              <th>추가로 내려간 viewport 높이</th>
              <th>도달한 sampled replays</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(page.scroll_reach_replays).map(([depth, count]) => (
              <tr key={depth}>
                <td>{evidence("scroll", depth, `${depth}배 이상`)}</td>
                <td>{count}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div
          className="scroll-depth"
          role="img"
          aria-label={`최대 스크롤 ${page.max_scroll_viewports.toFixed(2)} viewport`}
        >
          <span
            style={{
              width: `${Math.min(100, (page.max_scroll_viewports / 5) * 100)}%`,
            }}
          />
        </div>
        <p className="muted">
          문서 높이를 SDK가 전송하지 않아 페이지의 25%·100% 도달률은 계산하지
          않습니다. 스크롤 값은 동적 레이아웃과 viewport 변화의 영향을 받습니다.
        </p>
      </details>
    </div>
  );
}
