import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Session, User } from "../../api/types";
import { sessionQueryKey } from "../auth/api";
import { UsersPage } from "./UsersPage";

const adminSession: Session = {
  id: "admin-1",
  email: "owner@example.com",
  role: "admin",
  csrf_token: "csrf-value",
};

function renderPage(session: Session | null, users?: User[]) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  if (users && session) client.setQueryData(["users", session.id], users);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter>
          <Routes>
            <Route path="/" element={children} />
            <Route path="/login" element={<h1>Login destination</h1>} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    );
  }
  return { client, ...render(<UsersPage />, { wrapper: Wrapper }) };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("UsersPage", () => {
  it("denies a member route without requesting the admin user list", () => {
    const users = vi.spyOn(endpoints, "users");
    renderPage({ ...adminSession, role: "member" });

    expect(
      screen.getByText("사용자 관리는 관리자 계정에서만 열 수 있습니다."),
    ).toBeInTheDocument();
    expect(users).not.toHaveBeenCalled();
  });

  it("sends a keyboard-driven role change and shows last-admin protection", async () => {
    const user = userEvent.setup();
    const users: User[] = [
      {
        id: adminSession.id,
        email: adminSession.email,
        role: "admin",
        is_active: true,
      },
    ];
    vi.spyOn(endpoints, "updateUser").mockRejectedValue(
      new ApiError(409, {
        error: {
          code: "last_admin_required",
          message: "last_admin_required",
          request_id: "request-1",
          retryable: false,
        },
      }),
    );
    renderPage(adminSession, users);

    await user.selectOptions(
      screen.getByLabelText("owner@example.com 역할"),
      "member",
    );
    await user.click(screen.getByRole("button", { name: "변경 저장" }));

    await waitFor(() =>
      expect(endpoints.updateUser).toHaveBeenCalledWith({
        id: adminSession.id,
        update: { role: "member" },
      }),
    );
    expect(
      await screen.findByText("활성 관리자는 최소 한 명 필요합니다."),
    ).toBeInTheDocument();
  });
});

it("hides the user management page without a session", () => {
  const users = vi.spyOn(endpoints, "users");
  renderPage(null);
  expect(screen.queryByRole("heading")).not.toBeInTheDocument();
  expect(users).not.toHaveBeenCalled();
});
it("retries list failures and creates a user without duplicate submission", async () => {
  const user = userEvent.setup();
  const users = vi
    .spyOn(endpoints, "users")
    .mockRejectedValueOnce(new Error("offline"));
  renderPage(adminSession);
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  users.mockResolvedValue([]);
  await user.click(screen.getByRole("button", { name: "다시 시도" }));
  expect(await screen.findByText("등록된 사용자가 없습니다.")).toBeVisible();
  let resolve!: (value: { id: string }) => void;
  const create = vi.spyOn(endpoints, "createUser").mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  await user.type(screen.getByLabelText("이메일"), "member@example.test");
  await user.type(screen.getByLabelText("임시 비밀번호"), "fixture-password");
  await user.click(screen.getByRole("button", { name: "사용자 추가" }));
  expect(
    await screen.findByRole("button", { name: "추가 중…" }),
  ).toBeDisabled();
  const member: User = {
    id: "2",
    email: "member@example.test",
    role: "member",
    is_active: true,
  };
  users.mockResolvedValue([member]);
  await act(async () => resolve({ id: "2" }));
  expect(await screen.findByText(member.email)).toBeVisible();
  expect(create).toHaveBeenCalledTimes(1);
});
it("keeps failed creation input for retry", async () => {
  const user = userEvent.setup();
  vi.spyOn(endpoints, "users").mockResolvedValue([]);
  vi.spyOn(endpoints, "createUser").mockRejectedValue(new Error("offline"));
  renderPage(adminSession);
  await user.type(screen.getByLabelText("이메일"), "member@example.test");
  await user.type(screen.getByLabelText("임시 비밀번호"), "fixture-password");
  await user.click(screen.getByRole("button", { name: "사용자 추가" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.getByLabelText("이메일")).toHaveValue("member@example.test");
});
it.each([false, true])(
  "applies activation changes and revokes only the current session (self=%s)",
  async (self) => {
    const user = userEvent.setup();
    const target: User = {
      id: self ? adminSession.id : "2",
      email: "target@example.test",
      role: "admin",
      is_active: true,
    };
    const users = vi.spyOn(endpoints, "users").mockResolvedValue([target]);
    let resolve!: () => void;
    const update = vi.spyOn(endpoints, "updateUser").mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = () => done(undefined);
        }),
    );
    const { client } = renderPage(adminSession);
    client.setQueryData(["private"], "secret");
    await user.click(await screen.findByRole("checkbox", { name: "활성" }));
    await user.click(screen.getByRole("button", { name: "변경 저장" }));
    expect(
      await screen.findByRole("button", { name: "저장 중…" }),
    ).toBeDisabled();
    expect(update).toHaveBeenCalledWith({
      id: target.id,
      update: { is_active: false },
    });
    users.mockResolvedValue([{ ...target, is_active: false }]);
    await act(async () => resolve());
    if (self) {
      expect(
        await screen.findByRole("heading", { name: "Login destination" }),
      ).toBeVisible();
      expect(client.getQueryData(["private"])).toBeUndefined();
    } else {
      await waitFor(() =>
        expect(
          screen.getByRole("checkbox", { name: "활성" }),
        ).not.toBeChecked(),
      );
      expect(client.getQueryData(["private"])).toBe("secret");
    }
  },
);
it("does not revive a session that expires while a user update is in flight", async () => {
  const user = userEvent.setup();
  const target: User = {
    id: "2",
    email: "target@example.test",
    role: "member",
    is_active: true,
  };
  let resolve!: () => void;
  vi.spyOn(endpoints, "updateUser").mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = () => done(undefined);
      }),
  );
  const { client } = renderPage(adminSession, [target]);
  await user.click(screen.getByRole("checkbox", { name: "활성" }));
  await user.click(screen.getByRole("button", { name: "변경 저장" }));
  act(() => client.setQueryData(sessionQueryKey, null));
  await waitFor(() => expect(screen.queryAllByRole("heading")).toHaveLength(0));
  await act(async () => resolve());
  expect(screen.queryByRole("heading")).not.toBeInTheDocument();
  expect(client.getQueryData(sessionQueryKey)).toBeNull();
});

it("does not send an empty user update even if the form is submitted programmatically", () => {
  const update = vi.spyOn(endpoints, "updateUser");
  renderPage(adminSession, [
    { id: "2", email: "member@example.test", role: "member", is_active: true },
  ]);
  const form = screen
    .getByRole("button", { name: "변경 저장" })
    .closest("form");
  if (!form) throw new Error("missing user row form");
  fireEvent.submit(form);
  expect(update).not.toHaveBeenCalled();
});
