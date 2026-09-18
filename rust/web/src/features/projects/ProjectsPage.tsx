import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { describeApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import { Button } from "../../components/Button";
import { Notice } from "../../components/Notice";
import { Spinner } from "../../components/Spinner";
import { useSession } from "../auth";
import { ProjectCard } from "./ProjectCard";
import { ProjectForm } from "./ProjectForm";
import { projectsQuery } from "./queries";

export function ProjectsPage() {
  const session = useSession();
  const queryClient = useQueryClient();
  const user = session.data;
  const projects = useQuery({
    ...projectsQuery(user?.id ?? "unknown"),
    enabled: Boolean(user),
  });

  const refreshProjects = () =>
    queryClient.invalidateQueries({ queryKey: ["projects", user?.id] });
  const create = useMutation({
    mutationFn: (input: { slug: string; name: string }) =>
      endpoints.createProject(input),
    onSuccess: refreshProjects,
  });
  const update = useMutation({
    mutationFn: (project: { id: string; is_active: boolean }) =>
      endpoints.updateProject(project),
    onSuccess: refreshProjects,
  });
  const issueKey = useMutation({
    mutationFn: (projectId: string) => endpoints.createProjectKey(projectId),
    onSuccess: (_, projectId) =>
      queryClient.invalidateQueries({
        queryKey: ["projectKeys", user?.id, projectId],
      }),
  });
  const revokeKey = useMutation({
    mutationFn: ({ projectId, keyId }: { projectId: string; keyId: string }) =>
      endpoints.revokeProjectKey(projectId, keyId),
    onSuccess: (_, input) =>
      queryClient.invalidateQueries({
        queryKey: ["projectKeys", user?.id, input.projectId],
      }),
  });

  const mutationError =
    create.error ?? update.error ?? issueKey.error ?? revokeKey.error;

  return (
    <div className="page-stack">
      <header className="page-heading">
        <div>
          <p className="eyebrow">수집 구성</p>
          <h1>프로젝트</h1>
          <p>분석할 서비스를 등록하고 Sentry SDK를 연결하세요.</p>
        </div>
        {projects.data ? (
          <span className="count-badge">{projects.data.length}개</span>
        ) : null}
      </header>

      {user?.role === "admin" ? (
        projects.data?.length ? (
          <details className="disclosure">
            <summary>새 프로젝트 추가</summary>
            <ProjectForm
              disabled={create.isPending}
              onCreate={(input) => create.mutate(input)}
            />
          </details>
        ) : (
          <ProjectForm
            key={create.data?.id ?? "new"}
            disabled={create.isPending}
            onCreate={(input) => create.mutate(input)}
          />
        )
      ) : (
        <Notice>프로젝트 설정은 관리자만 변경할 수 있습니다.</Notice>
      )}

      {mutationError ? (
        <Notice tone="error">{describeApiError(mutationError)}</Notice>
      ) : null}

      {projects.isPending ? <Spinner label="프로젝트 불러오는 중" /> : null}
      {projects.isError ? (
        <Notice tone="error">
          <span>{describeApiError(projects.error)}</span>
          <Button
            onClick={() => void projects.refetch()}
            type="button"
            variant="quiet"
          >
            다시 시도
          </Button>
        </Notice>
      ) : null}
      <div className="project-grid">
        {projects.data?.map((project) => (
          <ProjectCard
            admin={user?.role === "admin"}
            busy={update.isPending || issueKey.isPending || revokeKey.isPending}
            userId={user?.id ?? "unknown"}
            key={project.id}
            onCreateKey={() => issueKey.mutate(project.id)}
            onRevokeKey={(key) =>
              revokeKey.mutate({ projectId: project.id, keyId: key.id })
            }
            onToggle={() =>
              update.mutate({ id: project.id, is_active: !project.is_active })
            }
            project={project}
          />
        ))}
      </div>
    </div>
  );
}
