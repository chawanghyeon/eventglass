import { useQueryClient } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ApiError, apiRequest, setCsrfToken } from "../api/client";
import { sessionQueryKey } from "../features/auth/api";
import { AppProviders } from "./providers";
import { queryClient } from "./queryClient";

const defaults = queryClient.getDefaultOptions();
beforeEach(() => {
  queryClient.setDefaultOptions({
    ...defaults,
    queries: { ...defaults.queries, retryDelay: 0 },
  });
  queryClient.setQueryData(sessionQueryKey, { id: "1" });
  queryClient.setQueryData(["projects", "1"], [{ id: "private" }]);
  setCsrfToken("old-token");
});
afterEach(() => {
  queryClient.clear();
  queryClient.setDefaultOptions(defaults);
  setCsrfToken(undefined);
  vi.unstubAllGlobals();
});
it("provides the shared client to the application", () => {
  function Probe() {
    const client = useQueryClient();
    return (
      <div>{client === queryClient ? "shared client" : "wrong client"}</div>
    );
  }
  render(
    <AppProviders>
      <Probe />
    </AppProviders>,
  );
  expect(screen.getByText("shared client")).toBeVisible();
});
it.each([new Error("offline"), new ApiError(503)])(
  "retries transient reads only once and preserves the session",
  async (error) => {
    const request = vi.fn().mockRejectedValue(error);
    await expect(
      queryClient.fetchQuery({ queryKey: ["read"], queryFn: request }),
    ).rejects.toBe(error);
    expect(request).toHaveBeenCalledTimes(2);
    expect(queryClient.getQueryData(["projects", "1"])).toEqual([
      { id: "private" },
    ]);
  },
);
it("does not retry a non-retryable error", async () => {
  const error = new ApiError(409);
  const request = vi.fn().mockRejectedValue(error);
  await expect(
    queryClient.fetchQuery({ queryKey: ["read"], queryFn: request }),
  ).rejects.toBe(error);
  expect(request).toHaveBeenCalledOnce();
  expect(queryClient.getQueryData(sessionQueryKey)).toEqual({ id: "1" });
});
it.each(["query", "mutation"])(
  "expires all private state after an unauthorized %s",
  async (kind) => {
    const request = vi.fn().mockRejectedValue(new ApiError(401));
    if (kind === "query") {
      await expect(
        queryClient.fetchQuery({ queryKey: ["read"], queryFn: request }),
      ).rejects.toBeDefined();
    } else {
      await expect(
        queryClient
          .getMutationCache()
          .build(queryClient, { mutationFn: request })
          .execute(undefined),
      ).rejects.toBeDefined();
    }
    expect(request).toHaveBeenCalledOnce();
    expect(queryClient.getQueryData(sessionQueryKey)).toBeNull();
    expect(queryClient.getQueryData(["projects", "1"])).toBeUndefined();
    expect(queryClient.getMutationCache().getAll()).toHaveLength(0);
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    await expect(
      apiRequest("/api/projects", { method: "POST" }),
    ).rejects.toThrow("CSRF token is unavailable");
    expect(fetch).not.toHaveBeenCalled();
  },
);
it("does not retry mutations even when the server calls them retryable", async () => {
  const error = new ApiError(503);
  const request = vi.fn().mockRejectedValue(error);
  await expect(
    queryClient
      .getMutationCache()
      .build(queryClient, { mutationFn: request })
      .execute(undefined),
  ).rejects.toBe(error);
  expect(request).toHaveBeenCalledOnce();
  expect(queryClient.getQueryData(sessionQueryKey)).toEqual({ id: "1" });
});
