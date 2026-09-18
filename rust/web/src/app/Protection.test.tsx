import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError, setCsrfToken } from "../api/client";
import { endpoints } from "../api/endpoints";
import { sessionQueryKey } from "../features/auth/api";
import { ErrorBoundary } from "./ErrorBoundary";
import { ProtectedRoute } from "./ProtectedRoute";

const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  setCsrfToken(undefined);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
function LoginDestination() {
  const location = useLocation();
  return <div>Login from {location.state?.from}</div>;
}
function show(expired = false) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  if (expired) client.setQueryData(sessionQueryKey, null);
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/private"]}>
        <Routes>
          <Route path="/login" element={<LoginDestination />} />
          <Route element={<ProtectedRoute />}>
            <Route path="/private" element={<div>Private data</div>} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}
it("does not show private content before the session has loaded", () => {
  vi.spyOn(endpoints, "session").mockImplementation(
    () => new Promise(() => {}),
  );
  show();
  expect(screen.getByText("세션 확인 중")).toBeVisible();
  expect(screen.queryByText("Private data")).not.toBeInTheDocument();
});
it.each([false, true])(
  "routes rejected or expired sessions to login (cached=%s)",
  async (expired) => {
    vi.spyOn(endpoints, "session").mockRejectedValue(new ApiError(401));
    show(expired);
    expect(await screen.findByText("Login from /private")).toBeVisible();
    expect(screen.queryByText("Private data")).not.toBeInTheDocument();
  },
);
it("keeps transient session errors visible and permits an explicit retry", async () => {
  const user = userEvent.setup();
  const session = vi
    .spyOn(endpoints, "session")
    .mockRejectedValueOnce(new Error("offline"));
  show();
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "연결 상태를 확인해 주세요",
  );
  expect(screen.queryByText("Private data")).not.toBeInTheDocument();
  session.mockResolvedValueOnce({
    id: "1",
    role: "member",
    email: "member@example.test",
    csrf_token: "fresh",
  });
  await user.click(screen.getByRole("button", { name: "다시 시도" }));
  expect(await screen.findByText("Private data")).toBeVisible();
});
it("does not mistake a forbidden session response for an expired session", async () => {
  vi.spyOn(endpoints, "session").mockRejectedValue(new ApiError(403));
  show();
  expect(await screen.findByRole("alert")).toHaveTextContent("request_failed");
  expect(screen.queryByText("Login from /private")).not.toBeInTheDocument();
});
it("renders normally until a child fails, then offers an actual page reload", () => {
  const error = new Error("render failed");
  function Broken(): never {
    throw error;
  }
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const view = render(
    <ErrorBoundary>
      <span>Healthy content</span>
    </ErrorBoundary>,
  );
  expect(screen.getByText("Healthy content")).toBeVisible();
  view.rerender(
    <ErrorBoundary>
      <Broken />
    </ErrorBoundary>,
  );
  expect(
    screen.getByRole("heading", { name: "화면을 불러오지 못했습니다." }),
  ).toBeVisible();
  expect(log).toHaveBeenCalledWith(
    "Eventglass UI failed",
    error,
    expect.any(String),
  );
  const reload = vi.fn();
  vi.stubGlobal("window", { location: { reload } });
  fireEvent.click(screen.getByRole("button", { name: "새로 고침" }));
  expect(reload).toHaveBeenCalledOnce();
  vi.unstubAllGlobals();
});
