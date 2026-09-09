import { useQuery } from "@tanstack/react-query";
import { endpoints } from "../../api/endpoints";
import { describeApiError } from "../../api/client";
import type { Project, ProjectKey } from "../../api/types";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";

interface ProjectCardProps {
  admin: boolean;
  busy: boolean;
  userId: string;
  onCreateKey: () => void;
  onRevokeKey: (key: ProjectKey) => void;
  onToggle: () => void;
  project: Project;
}

export function ProjectCard({
  admin,
  busy,
  userId,
  onCreateKey,
  onRevokeKey,
  onToggle,
  project,
}: ProjectCardProps) {
  const keys = useQuery({
    queryKey: ["projectKeys", userId, project.id],
    queryFn: ({ signal }) => endpoints.projectKeys(project.id, signal),
    enabled: admin,
  });

  return (
    <article
      className={`project-card ${project.is_active ? "" : "project-card--inactive"}`}
    >
      <header>
        <div>
          <div className="title-row">
            <h3>{project.name}</h3>
            <span
              className={`status-pill ${project.is_active ? "status-pill--active" : ""}`}
            >
              {project.is_active ? "활성" : "중지됨"}
            </span>
          </div>
          <p className="mono">{project.slug}</p>
        </div>
        <span className="project-id">ID {project.id}</span>
      </header>

      {keys.isError ? (
        <Notice tone="error">{describeApiError(keys.error)}</Notice>
      ) : null}
      {admin &&
        keys.data?.map((issuedKey) => (
          <div className="dsn-panel" key={issuedKey.id}>
            <label>
              DSN
              <textarea readOnly rows={3} value={issuedKey.dsn} />
            </label>
            <div className="button-row">
              <Button
                onClick={() =>
                  void navigator.clipboard.writeText(issuedKey.dsn)
                }
                type="button"
              >
                DSN 복사
              </Button>
              <Button
                disabled={busy}
                onClick={() => onRevokeKey(issuedKey)}
                type="button"
                variant="danger"
              >
                이 키 폐기
              </Button>
            </div>
          </div>
        ))}
      <div className="project-card__actions">
        <p>
          {project.is_active
            ? "SDK 연결용 DSN을 새로 발급할 수 있습니다."
            : "중지된 프로젝트는 새 키를 발급할 수 없습니다."}
        </p>
        {admin ? (
          <div className="button-row">
            <Button
              disabled={busy || !project.is_active}
              onClick={onCreateKey}
              type="button"
            >
              새 DSN 발급
            </Button>
            <Button
              disabled={busy}
              onClick={onToggle}
              type="button"
              variant="quiet"
            >
              {project.is_active ? "프로젝트 중지" : "프로젝트 다시 활성화"}
            </Button>
          </div>
        ) : null}
      </div>
    </article>
  );
}
