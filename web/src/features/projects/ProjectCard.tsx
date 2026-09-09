import { useState } from "react";
import { Link } from "react-router-dom";
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
  const [copied, setCopied] = useState("");
  const [copyError, setCopyError] = useState(false);
  async function copy(value: string, key: string) {
    setCopied("");
    setCopyError(false);
    try {
      await navigator.clipboard.writeText(value);
      setCopied(key);
      setCopyError(false);
    } catch {
      setCopyError(true);
    }
  }
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
                onClick={() => void copy(issuedKey.dsn, issuedKey.id)}
                type="button"
              >
                {copied === issuedKey.id ? "복사됨 ✓" : "DSN 복사"}
              </Button>
            </div>
            <details className="disclosure">
              <summary>Sentry SDK 연결 방법</summary>
              <p>
                웹사이트에는 공식 SDK 하나만 설치하세요. React·Vue·Next.js에서는
                해당 공식 Sentry 패키지를 사용합니다.
              </p>
              <pre>npm install @sentry/browser</pre>
              <pre>{sdkExample(issuedKey.dsn)}</pre>
              <Button
                onClick={() =>
                  void copy(sdkExample(issuedKey.dsn), "code-" + issuedKey.id)
                }
              >
                {copied === "code-" + issuedKey.id
                  ? "설정 복사됨 ✓"
                  : "설정 복사"}
              </Button>
              <p className="muted">
                예제는 방문의 10%를 수집하며 오류 발생 시에는 Replay를
                수집합니다. 샘플링 비율은 서비스에 맞게 조정하세요. 텍스트와
                입력값은 SDK의 마스킹 설정을 유지합니다.
              </p>
            </details>
            <details className="disclosure">
              <summary>이 연결 키 관리</summary>
              <p>폐기하면 이 DSN으로 더 이상 데이터를 수집할 수 없습니다.</p>
              <Button
                disabled={busy}
                onClick={() => onRevokeKey(issuedKey)}
                variant="danger"
              >
                이 키 폐기
              </Button>
            </details>
          </div>
        ))}
      {copied && <p role="status">클립보드에 복사했습니다.</p>}
      {copyError && (
        <Notice tone="error">
          복사하지 못했습니다. 위 내용을 직접 선택해 복사해 주세요.
        </Notice>
      )}
      {project.is_active && (
        <div className="button-row">
          <Link
            className="button button--quiet"
            to={`/replays?project=${project.id}`}
          >
            방문 기록 보기 →
          </Link>
          <Link
            className="button button--quiet"
            to={`/logs?project=${project.id}`}
          >
            수집된 로그 보기 →
          </Link>
        </div>
      )}
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

function sdkExample(dsn: string): string {
  return `import * as Sentry from "@sentry/browser";

Sentry.init({
  dsn: ${JSON.stringify(dsn)},
  integrations: [Sentry.replayIntegration({
    maskAllText: true,
    blockAllMedia: true,
  })],
  replaysSessionSampleRate: 0.1,
  replaysOnErrorSampleRate: 1.0,
});`;
}
