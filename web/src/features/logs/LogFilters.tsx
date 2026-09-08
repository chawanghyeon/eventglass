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
    <form className="log-filters" onSubmit={submit}>
      <div className="log-filters__primary">
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
        <label className="log-filters__query">
          검색어
          <input
            maxLength={8192}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="메시지 또는 검색 문법"
            value={query}
          />
        </label>
      </div>
      <fieldset className="log-projects">
        <legend>프로젝트</legend>
        <p>선택하지 않으면 접근 가능한 활성 프로젝트를 모두 검색합니다.</p>
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
      <details>
        <summary>메타데이터 필터</summary>
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
      <div className="button-row">
        <Button type="submit">검색 적용</Button>
      </div>
    </form>
  );
}
