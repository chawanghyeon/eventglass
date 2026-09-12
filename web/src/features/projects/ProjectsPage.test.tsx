import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import type { Project, ProjectKey } from "../../api/types";
import { sessionQueryKey } from "../auth";
import { ProjectsPage } from "./ProjectsPage";

const project: Project = {
  id: "1",
  slug: "shop",
  name: "Shop",
  is_active: true,
};
const key: ProjectKey = {
  id: "key1",
  dsn: "https://public@example.test/1",
  public_key: "public",
};
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  vi.restoreAllMocks();
});
function show(role: "admin" | "member" | null = "admin") {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  clients.push(client);
  client.setQueryData(
    sessionQueryKey,
    role
      ? { id: "7", role, email: "operator@example.test", csrf_token: "fixture" }
      : null,
  );
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ProjectsPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return client;
}
it("does not request projects or collection keys without a session", () => {
  const projects = vi.spyOn(endpoints, "projects");
  const keys = vi.spyOn(endpoints, "projectKeys");
  show(null);
  expect(
    screen.getByText("프로젝트 설정은 관리자만 변경할 수 있습니다."),
  ).toBeVisible();
  expect(projects).not.toHaveBeenCalled();
  expect(keys).not.toHaveBeenCalled();
});
it("shows member projects without requesting admin collection keys", async () => {
  vi.spyOn(endpoints, "projects").mockResolvedValue([project]);
  const keys = vi.spyOn(endpoints, "projectKeys");
  show("member");
  expect(await screen.findByText("Shop")).toBeVisible();
  expect(
    screen.queryByRole("button", { name: "새 DSN 발급" }),
  ).not.toBeInTheDocument();
  expect(keys).not.toHaveBeenCalled();
});
it("retries a failed project load without losing the page", async () => {
  const user = userEvent.setup();
  const projects = vi
    .spyOn(endpoints, "projects")
    .mockRejectedValueOnce(new Error("offline"));
  show("member");
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  projects.mockResolvedValueOnce([]);
  await user.click(screen.getByRole("button", { name: "다시 시도" }));
  expect(await screen.findByText("0개")).toBeVisible();
});
it("creates the first project, prevents duplicate submits, and refreshes the list", async () => {
  const user = userEvent.setup();
  const projects = vi.spyOn(endpoints, "projects").mockResolvedValue([]);
  vi.spyOn(endpoints, "projectKeys").mockResolvedValue([]);
  let resolve!: (value: { id: string }) => void;
  const create = vi.spyOn(endpoints, "createProject").mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  show();
  await screen.findByText("0개");
  await user.type(screen.getByLabelText("표시 이름"), " Shop ");
  await user.type(screen.getByLabelText("프로젝트 식별자 (영문)"), "shop");
  await user.click(screen.getByRole("button", { name: "프로젝트 추가" }));
  expect(
    await screen.findByRole("button", { name: "추가 중…" }),
  ).toBeDisabled();
  expect(create).toHaveBeenCalledWith({ name: "Shop", slug: "shop" });
  projects.mockResolvedValue([project]);
  await act(async () => resolve({ id: "1" }));
  expect(await screen.findByText("Shop")).toBeVisible();
  expect(screen.getByText("새 프로젝트 추가")).toBeVisible();
});
it("keeps failed creation input available for retry in the additional-project form", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "projects").mockResolvedValue([project]);
  vi.spyOn(endpoints, "projectKeys").mockResolvedValue([]);
  vi.spyOn(endpoints, "createProject").mockRejectedValue(new Error("offline"));
  show();
  await user.click(await screen.findByText("새 프로젝트 추가"));
  await user.type(screen.getByLabelText("표시 이름"), "Second");
  await user.type(screen.getByLabelText("프로젝트 식별자 (영문)"), "second");
  await user.click(screen.getByRole("button", { name: "프로젝트 추가" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.getByLabelText("표시 이름")).toHaveValue("Second");
});
it("refreshes key and project state after issue, revoke, stop, and reactivation", async () => {
  const user = userEvent.setup();
  const projects = vi.spyOn(endpoints, "projects").mockResolvedValue([project]);
  const keys = vi.spyOn(endpoints, "projectKeys").mockResolvedValue([]);
  let resolve!: (value: ProjectKey) => void;
  const create = vi.spyOn(endpoints, "createProjectKey").mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const revoke = vi
    .spyOn(endpoints, "revokeProjectKey")
    .mockResolvedValue(undefined);
  const update = vi
    .spyOn(endpoints, "updateProject")
    .mockResolvedValue(undefined);
  show();
  await user.click(await screen.findByRole("button", { name: "새 DSN 발급" }));
  expect(
    await screen.findByRole("button", { name: "프로젝트 중지" }),
  ).toBeDisabled();
  keys.mockResolvedValue([key]);
  await act(async () => resolve(key));
  expect(create).toHaveBeenCalledWith("1");
  expect(await screen.findByLabelText("DSN")).toHaveValue(key.dsn);
  await user.click(screen.getByText("이 연결 키 관리"));
  keys.mockResolvedValue([]);
  await user.click(screen.getByRole("button", { name: "이 키 폐기" }));
  await waitFor(() =>
    expect(screen.queryByLabelText("DSN")).not.toBeInTheDocument(),
  );
  expect(revoke).toHaveBeenCalledWith("1", "key1");
  projects.mockResolvedValue([{ ...project, is_active: false }]);
  await user.click(screen.getByRole("button", { name: "프로젝트 중지" }));
  expect(await screen.findByText("중지됨")).toBeVisible();
  expect(screen.getByRole("button", { name: "새 DSN 발급" })).toBeDisabled();
  expect(update).toHaveBeenLastCalledWith({ id: "1", is_active: false });
  projects.mockResolvedValue([project]);
  await user.click(
    screen.getByRole("button", { name: "프로젝트 다시 활성화" }),
  );
  expect(await screen.findByText("활성")).toBeVisible();
  expect(update).toHaveBeenLastCalledWith({ id: "1", is_active: true });
});
it.each(["issue", "revoke", "update"])(
  "reports a failed %s operation and preserves the current project",
  async (action) => {
    const user = userEvent.setup();
    vi.spyOn(endpoints, "projects").mockResolvedValue([project]);
    vi.spyOn(endpoints, "projectKeys").mockResolvedValue([key]);
    vi.spyOn(endpoints, "createProjectKey").mockRejectedValue(
      new Error("offline"),
    );
    vi.spyOn(endpoints, "revokeProjectKey").mockRejectedValue(
      new Error("offline"),
    );
    vi.spyOn(endpoints, "updateProject").mockRejectedValue(
      new Error("offline"),
    );
    show();
    await screen.findByLabelText("DSN");
    if (action === "revoke")
      await user.click(screen.getByText("이 연결 키 관리"));
    await user.click(
      screen.getByRole("button", {
        name:
          action === "issue"
            ? "새 DSN 발급"
            : action === "revoke"
              ? "이 키 폐기"
              : "프로젝트 중지",
      }),
    );
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "연결 상태를 확인해 주세요",
    );
    expect(screen.getByText("활성")).toBeVisible();
    expect(screen.getByLabelText("DSN")).toHaveValue(key.dsn);
  },
);
it("keeps a project usable when its key list fails", async () => {
  vi.spyOn(endpoints, "projects").mockResolvedValue([project]);
  vi.spyOn(endpoints, "projectKeys").mockRejectedValue(new Error("offline"));
  show();
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.getByRole("button", { name: "프로젝트 중지" })).toBeEnabled();
});
