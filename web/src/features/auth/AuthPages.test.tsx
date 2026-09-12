import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError, setCsrfToken } from "../../api/client";
import { endpoints } from "../../api/endpoints";
import type { Session } from "../../api/types";
import { LoginPage } from "./LoginPage";
import { SetupPage } from "./SetupPage";
import { sessionQueryKey } from "./api";

const session: Session = {
  id: "1",
  role: "admin",
  email: "admin@example.test",
  csrf_token: "fresh",
};
const clients: QueryClient[] = [];
afterEach(() => {
  for (const client of clients.splice(0)) client.clear();
  setCsrfToken(undefined);
  vi.restoreAllMocks();
});
function Destination() {
  const location = useLocation();
  return (
    <div>
      Destination: {location.pathname}
      {location.state?.setupComplete ? " setup complete" : ""}
    </div>
  );
}
function show(page: "login" | "setup", initialSession: Session | null = null) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  clients.push(client);
  client.setQueryData(sessionQueryKey, initialSession);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/${page}`]}>
        <Routes>
          <Route
            path="/login"
            element={page === "login" ? <LoginPage /> : <Destination />}
          />
          <Route
            path="/setup"
            element={page === "setup" ? <SetupPage /> : <Destination />}
          />
          <Route path="/projects" element={<Destination />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return client;
}
it("redirects an existing session without submitting credentials", async () => {
  const login = vi.spyOn(endpoints, "login");
  show("login", session);
  expect(await screen.findByText("Destination: /projects")).toBeVisible();
  expect(login).not.toHaveBeenCalled();
});
it("keeps a rejected login on the form, then clears private cache on successful retry", async () => {
  const user = userEvent.setup();
  const login = vi.spyOn(endpoints, "login").mockRejectedValueOnce(
    new ApiError(401, {
      error: {
        code: "invalid_credentials",
        message: "invalid_credentials",
        retryable: false,
        request_id: "1",
      },
    }),
  );
  const client = show("login");
  client.setQueryData(["projects", "old-user"], [{ id: "private" }]);
  await user.type(screen.getByLabelText("이메일"), session.email);
  await user.type(screen.getByLabelText(/^비밀번호/), "correct-password");
  await user.click(screen.getByRole("button", { name: "로그인" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "이메일 또는 비밀번호가 올바르지 않습니다.",
  );
  expect(login).toHaveBeenCalledWith({
    email: session.email,
    password: "correct-password", // pragma: allowlist secret (isolated test fixture)
  });
  let resolve!: (value: { csrf_token: string }) => void;
  login.mockImplementationOnce(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  vi.spyOn(endpoints, "session").mockResolvedValue(session);
  await user.click(screen.getByRole("button", { name: "로그인" }));
  expect(
    await screen.findByRole("button", { name: "로그인 중…" }),
  ).toBeDisabled();
  await act(async () => resolve({ csrf_token: "fresh" }));
  expect(await screen.findByText("Destination: /projects")).toBeVisible();
  expect(client.getQueryData(["projects", "old-user"])).toBeUndefined();
  expect(client.getQueryData(sessionQueryKey)).toEqual(session);
});
it("submits a trimmed one-time setup token, reports failure, and routes after successful retry", async () => {
  const user = userEvent.setup();
  let reject!: (error: Error) => void;
  const setup = vi.spyOn(endpoints, "setup").mockImplementationOnce(
    () =>
      new Promise((_, fail) => {
        reject = fail;
      }),
  );
  show("setup");
  await user.type(screen.getByLabelText("설정 토큰"), "  one-time  ");
  await user.type(screen.getByLabelText("관리자 이메일"), session.email);
  await user.type(screen.getByLabelText(/^비밀번호/), "correct-password");
  await user.click(screen.getByRole("button", { name: "관리자 만들기" }));
  expect(
    await screen.findByRole("button", { name: "계정 만드는 중…" }),
  ).toBeDisabled();
  expect(setup).toHaveBeenCalledWith({
    token: "one-time",
    email: session.email,
    password: "correct-password", // pragma: allowlist secret (isolated test fixture)
  });
  await act(async () => reject(new Error("offline")));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  setup.mockResolvedValueOnce(undefined);
  await user.click(screen.getByRole("button", { name: "관리자 만들기" }));
  await waitFor(() =>
    expect(
      screen.getByText("Destination: /login setup complete"),
    ).toBeVisible(),
  );
});
