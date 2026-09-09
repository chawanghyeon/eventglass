import { QueryClient } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { apiRequest, setCsrfToken } from "../../api/client";
import {
  expireSession,
  finishLogin,
  logoutAndClear,
  sessionQueryKey,
} from "./api";

afterEach(() => {
  setCsrfToken(undefined);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("expires private cached data and aborts old requests when the server rejects a session", async () => {
  const client = new QueryClient();
  client.setQueryData(sessionQueryKey, { id: "7", role: "admin" });
  client.setQueryData(["projects", "7"], [{ id: "private" }]);
  let requestSignal: AbortSignal | undefined;
  const pending = client
    .fetchQuery({
      queryKey: ["logs", "7"],
      queryFn: ({ signal }) => {
        requestSignal = signal;
        return new Promise(() => {});
      },
    })
    .catch(() => undefined);
  setCsrfToken("old-token");
  expireSession(client);
  await pending;
  expect(requestSignal?.aborted).toBe(true);
  expect(client.getQueryData(["projects", "7"])).toBeUndefined();
  expect(client.getQueryData(sessionQueryKey)).toBeNull();
  await expect(apiRequest("/api/projects", { method: "POST" })).rejects.toThrow(
    "CSRF token is unavailable",
  );
});

it("starts a login with an empty cache even when the same user returns after role revocation", async () => {
  const client = new QueryClient();
  client.setQueryData(["users", "7"], [{ email: "private@example.test" }]);
  client.setQueryData(["projects", "7"], [{ id: "disabled-project" }]);
  vi.spyOn(endpoints, "session").mockResolvedValue({
    id: "7",
    email: "member@example.test",
    role: "member",
    csrf_token: "fresh-token",
  });
  await finishLogin(client, "fresh-token");
  expect(client.getQueryData(["users", "7"])).toBeUndefined();
  expect(client.getQueryData(["projects", "7"])).toBeUndefined();
  expect(client.getQueryData(sessionQueryKey)).toMatchObject({
    id: "7",
    role: "member",
  });
});

describe("logoutAndClear", () => {
  it("clears all user-scoped server state after logout", async () => {
    const client = new QueryClient();
    client.setQueryData(["projects", "7"], [{ id: "private" }]);
    setCsrfToken("csrf-value");
    vi.spyOn(endpoints, "logout").mockResolvedValue(undefined);

    await logoutAndClear(client);

    expect(client.getQueryCache().getAll()).toHaveLength(0);
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      apiRequest("/api/projects", { method: "POST" }),
    ).rejects.toThrow("CSRF token is unavailable");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("still clears cached authorization state when the logout request fails", async () => {
    const client = new QueryClient();
    client.setQueryData(["projects", "7"], [{ id: "private" }]);
    setCsrfToken("csrf-value");
    vi.spyOn(endpoints, "logout").mockRejectedValue(new Error("offline"));

    await expect(logoutAndClear(client)).rejects.toThrow("offline");

    expect(client.getQueryCache().getAll()).toHaveLength(0);
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(
      apiRequest("/api/projects", { method: "POST" }),
    ).rejects.toThrow("CSRF token is unavailable");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
