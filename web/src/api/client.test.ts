import { afterEach, describe, expect, it, vi } from "vitest";
import { apiRequest, setCsrfToken } from "./client";

afterEach(() => {
  setCsrfToken(undefined);
  vi.unstubAllGlobals();
});

describe("apiRequest", () => {
  it("sends cookies and the in-memory CSRF token for mutations", async () => {
    setCsrfToken("csrf-value");
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await apiRequest<undefined>("/api/projects/42", {
      method: "PATCH",
      body: { is_active: false },
    });

    expect(fetchMock).toHaveBeenCalledOnce();
    const [, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(request.credentials).toBe("include");
    expect(new Headers(request.headers).get("x-csrf-token")).toBe("csrf-value");
    expect(request.body).toBe('{"is_active":false}');
  });

  it("preserves typed server errors instead of turning them into empty data", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: {
              code: "project_exists",
              message: "project_exists",
              request_id: "req-7",
              retryable: false,
            },
          }),
          { status: 409, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    await expect(apiRequest("/api/projects")).rejects.toMatchObject({
      code: "project_exists",
      requestId: "req-7",
      retryable: false,
      status: 409,
    });
  });

  it("fails before an authenticated mutation when no CSRF token is available", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      apiRequest("/api/auth/logout", { method: "POST" }),
    ).rejects.toThrow("CSRF token is unavailable");
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
