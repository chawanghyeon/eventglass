import { useState } from "react";
import type { ReplayPageActivity } from "../../api/types";
import { Notice } from "../../components/Notice";

export function PageMaps({
  pages,
}: {
  pages: Record<string, ReplayPageActivity>;
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
  const cells = page[mode];
  const max = Math.max(1, ...Object.values(cells));
  return (
    <div className="page-stack">
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
      <svg
        className="replay-heatmap"
        viewBox="0 0 400 300"
        role="img"
        aria-label={`${mode === "clicks" ? "Click" : "Movement"} heatmap: ${url}`}
      >
        <rect width="400" height="300" fill="#eef2f6" />
        {Object.entries(cells).map(([cell, count]) => {
          const [x, y] = cell.split(",").map(Number);
          return (
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
        })}
      </svg>
      <p className="muted">
        좌표는 각 시점의 viewport를 20×20으로 정규화했습니다. 스크린샷 위 위치나
        문서 전체 좌표가 아닙니다. iframe 내부와 viewport를 확인할 수 없는
        구간은 제외합니다.
      </p>
      <h3>Element clicks</h3>
      <table>
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
                  <code>{selector}</code>
                </td>
                <td>{count}</td>
              </tr>
            ))}
        </tbody>
      </table>
      <h3>Scroll</h3>
      <p>
        최대 관측 이동: viewport 높이의{" "}
        <strong>{page.max_scroll_viewports.toFixed(2)}배</strong> ·{" "}
        {page.scroll_samples} samples
      </p>
      <table>
        <thead>
          <tr>
            <th>추가로 내려간 viewport 높이</th>
            <th>도달한 sampled replays</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(page.scroll_reach_replays).map(([depth, count]) => (
            <tr key={depth}>
              <td>{depth}배 이상</td>
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
    </div>
  );
}
