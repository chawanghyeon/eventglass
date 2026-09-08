import { QueryClient } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { endpoints } from "../../api/endpoints";
import { apiRequest, setCsrfToken } from "../../api/client";
import { logoutAndClear } from "./api";

afterEach(() => {
  setCsrfToken(undefined);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
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
