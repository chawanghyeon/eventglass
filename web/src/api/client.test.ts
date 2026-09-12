import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, apiRequest, describeApiError, setCsrfToken } from "./client";

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

it("keeps read requests token-free and preserves response data and cancellation", async () => {
  setCsrfToken("private-token");
  const controller = new AbortController();
  const fetchMock = vi
    .fn()
    .mockResolvedValue(Response.json({ items: ["record"] }));
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    apiRequest("/api/logs", {
      signal: controller.signal,
      headers: { "x-request-id": "trace" },
    }),
  ).resolves.toEqual({ items: ["record"] });
  const request = fetchMock.mock.calls[0][1] as RequestInit;
  expect(request.signal).toBe(controller.signal);
  expect(request.method).toBe("GET");
  expect(request.body).toBeUndefined();
  expect(new Headers(request.headers).get("x-csrf-token")).toBeNull();
  expect(new Headers(request.headers).get("x-request-id")).toBe("trace");
  expect(new Headers(request.headers).get("content-type")).toBeNull();
});

it("allows setup before a session and handles explicit empty responses", async () => {
  const fetchMock = vi
    .fn()
    .mockResolvedValue(
      new Response(null, { headers: { "content-length": "0" } }),
    );
  vi.stubGlobal("fetch", fetchMock);
  await expect(
    apiRequest("/api/setup", {
      method: "post",
      csrf: false,
      body: { token: "setup" },
    }),
  ).resolves.toBeUndefined();
  const request = fetchMock.mock.calls[0][1] as RequestInit;
  expect(request.method).toBe("POST");
  expect(new Headers(request.headers).get("x-csrf-token")).toBeNull();
  expect(new Headers(request.headers).get("content-type")).toBe(
    "application/json",
  );
  await expect(
    apiRequest("/api/system/status", { method: "HEAD" }),
  ).resolves.toBeUndefined();
});

it.each([400, 503])(
  "retains HTTP status when a proxy returns non-JSON (%i)",
  async (status) => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response("<html>proxy error</html>", { status }),
        ),
    );
    await expect(apiRequest("/api/logs")).rejects.toMatchObject({
      name: "ApiError",
      status,
      code: "request_failed",
      message: `request_failed_${status}`,
      retryable: status >= 500,
      requestId: undefined,
    });
  },
);

it("does not disguise malformed success bodies or aborted requests as empty results", async () => {
  const fetchMock = vi.fn().mockResolvedValueOnce(new Response("not JSON"));
  vi.stubGlobal("fetch", fetchMock);
  await expect(apiRequest("/api/logs")).rejects.toBeInstanceOf(SyntaxError);
  const aborted = new DOMException("cancelled", "AbortError");
  fetchMock.mockRejectedValueOnce(aborted);
  await expect(apiRequest("/api/logs")).rejects.toBe(aborted);
});

it("explains known errors and safely describes unknown failures", () => {
  expect(
    describeApiError(
      new ApiError(401, {
        error: {
          code: "authentication_required",
          message: "internal",
          request_id: "r",
          retryable: false,
        },
      }),
    ),
  ).toBe("세션이 만료되었습니다. 다시 로그인해 주세요.");
  expect(describeApiError(new ApiError(502))).toBe(
    "요청에 실패했습니다 (request_failed).",
  );
  expect(describeApiError(null)).toBe(
    "요청을 완료하지 못했습니다. 연결 상태를 확인해 주세요.",
  );
  expect(new ApiError(503, {} as never).retryable).toBe(true);
});
