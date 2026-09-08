import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
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

function renderPage(session: Session, users?: User[]) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  client.setQueryData(sessionQueryKey, session);
  if (users) client.setQueryData(["users", session.id], users);
  function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <MemoryRouter>{children}</MemoryRouter>
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
