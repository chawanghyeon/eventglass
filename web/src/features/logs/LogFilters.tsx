import { useState, type FormEvent } from "react";
import type { Project, SearchFilters } from "../../api/types";
import { Button } from "../../components/Button";
import { filterFields, splitFilter, type LogSearchState } from "./state";

const labels = {
  kinds: "종류 (log, error)",
  services: "서비스",
  levels: "레벨",
  environments: "환경",
  releases: "릴리스",
  loggers: "로거",
} satisfies Record<(typeof filterFields)[number], string>;

export function LogFilters({
  committed,
  projects,
  onApply,
}: {
  committed: LogSearchState;
  projects: Project[];
  onApply: (state: Omit<LogSearchState, "cursor" | "readToken">) => void;
}) {
  const [start, setStart] = useState(committed.start);
  const [end, setEnd] = useState(committed.end);
  const [query, setQuery] = useState(committed.query);
  const [selectedProjects, setSelectedProjects] = useState(committed.projects);
  const [filters, setFilters] = useState<Record<string, string>>(() =>
    Object.fromEntries(
      filterFields.map((field) => [
        field,
        (committed.filters[field] ?? []).join(", "),
      ]),
    ),
  );

  function submit(event: FormEvent) {
    event.preventDefault();
    onApply({
      projects: selectedProjects,
      start,
      end,
      query: query.trim(),
      filters: Object.fromEntries(
        filterFields.map((field) => [field, splitFilter(filters[field] ?? "")]),
      ) as SearchFilters,
    });
  }

  return (
    <form className="log-filters search-toolbar" onSubmit={submit}>
      <div className="log-filters__primary">
        <label className="log-filters__query">
          검색어
          <input
            maxLength={8192}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="메시지 또는 검색 문법"
            value={query}
          />
        </label>
        <Button type="submit">검색</Button>
      </div>
      <div className="search-toolbar__options">
        <label className="period-select">
          빠른 기간
          <select
            aria-label="빠른 기간"
            value=""
            onChange={(event) => {
              const now = new Date();
              setStart(
                new Date(
                  now.getTime() - Number(event.target.value) * 60000,
                ).toISOString(),
              );
              setEnd(now.toISOString());
            }}
          >
            <option value="" disabled>
              기간 선택
            </option>
            <option value="15">최근 15분</option>
            <option value="60">최근 1시간</option>
            <option value="360">최근 6시간</option>
            <option value="1440">최근 24시간</option>
            <option value="10080">최근 7일</option>
          </select>
        </label>
        <details className="search-time">
          <summary>
            시간 범위 · {formatTime(start)} – {formatTime(end)}
          </summary>
          <div className="log-filters__vectors">
            <label>
              시작 (RFC3339)
              <input
                onChange={(event) => setStart(event.target.value)}
                required
                value={start}
              />
            </label>
            <label>
              종료 (RFC3339)
              <input
                onChange={(event) => setEnd(event.target.value)}
                required
                value={end}
              />
            </label>
          </div>
        </details>
        <details>
          <summary>
            프로젝트 ·{" "}
            {selectedProjects.length
              ? `${selectedProjects.length}개 선택`
              : "전체"}
          </summary>
          <fieldset className="log-projects">
            <legend>프로젝트</legend>
            <span className="muted">
              {selectedProjects.length
                ? `${selectedProjects.length}개 선택`
                : "전체 프로젝트"}
            </span>
            <div>
              {projects
                .filter((project) => project.is_active)
                .map((project) => (
                  <label key={project.id}>
                    <input
                      checked={selectedProjects.includes(project.id)}
                      onChange={(event) =>
                        setSelectedProjects((current) =>
                          event.target.checked
                            ? [...current, project.id]
                            : current.filter((id) => id !== project.id),
                        )
                      }
                      type="checkbox"
                    />
                    {project.name}
                  </label>
                ))}
            </div>
          </fieldset>
        </details>
        <details>
          <summary>
            메타데이터 필터
            {Object.values(filters).some(Boolean) ? " · 적용 조건 있음" : ""}
          </summary>
          <div className="log-filters__vectors">
            {filterFields.map((field) => (
              <label key={field}>
                {labels[field]}
                <input
                  onChange={(event) =>
                    setFilters((current) => ({
                      ...current,
                      [field]: event.target.value,
                    }))
                  }
                  placeholder="쉼표로 여러 값 구분"
                  value={filters[field] ?? ""}
                />
              </label>
            ))}
          </div>
        </details>
      </div>
      <div className="filter-chips" aria-label="적용된 필터">
        {committed.query && (
          <button
            type="button"
            aria-label="검색어 조건 삭제"
            onClick={() => onApply({ ...committed, query: "" })}
          >
            검색: {committed.query} ×
          </button>
        )}
        {filterFields.flatMap((field) =>
          (committed.filters[field] ?? []).map((value) => (
            <button
              type="button"
              key={`${field}:${value}`}
              aria-label={`${labels[field]} ${value} 조건 삭제`}
              onClick={() =>
                onApply({
                  ...committed,
                  filters: {
                    ...committed.filters,
                    [field]: committed.filters[field]?.filter(
                      (item) => item !== value,
                    ),
                  },
                })
              }
            >
              {labels[field]}: {value} ×
            </button>
          )),
        )}
      </div>
    </form>
  );
}

function formatTime(value: string): string {
  const time = new Date(value);
  return Number.isFinite(time.getTime())
    ? time.toLocaleString("ko-KR", {
        month: "numeric",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    : "시간 선택";
}
