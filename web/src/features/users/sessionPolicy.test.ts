import { QueryClient } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { apiRequest, setCsrfToken } from "../../api/client";
import type { Session } from "../../api/types";
import { finishUserUpdate, updateRevokesCurrentSession } from "./sessionPolicy";

const session: Session = {
  id: "user-1",
  email: "admin@example.com",
  role: "admin",
  csrf_token: "csrf-value",
};

afterEach(() => {
  setCsrfToken(undefined);
  vi.unstubAllGlobals();
});

describe("user update session policy", () => {
  it("recognizes self demotion and self deactivation as session-revoking", () => {
    expect(
      updateRevokesCurrentSession(session, session.id, { role: "member" }),
    ).toBe(true);
    expect(
      updateRevokesCurrentSession(session, session.id, { is_active: false }),
    ).toBe(true);
    expect(
      updateRevokesCurrentSession(session, "user-2", { role: "member" }),
    ).toBe(false);
  });

  it("clears all scoped cache and CSRF state after a self demotion", async () => {
    const client = new QueryClient();
    client.setQueryData(["session"], session);
    client.setQueryData(["projects", session.id], [{ id: "private" }]);
    client.setQueryData(["users", session.id], [{ id: session.id }]);
    setCsrfToken(session.csrf_token);

    await expect(
      finishUserUpdate(client, session, session.id, { role: "member" }),
    ).resolves.toBe(true);

    expect(client.getQueryCache().getAll()).toHaveLength(0);
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    await expect(apiRequest("/api/users", { method: "POST" })).rejects.toThrow(
      "CSRF token is unavailable",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("invalidates only the current user's list after editing another user", async () => {
    const client = new QueryClient();
    client.setQueryData(["users", session.id], [{ id: "user-2" }]);
    client.setQueryData(["projects", session.id], [{ id: "project-1" }]);

    await expect(
      finishUserUpdate(client, session, "user-2", { is_active: false }),
    ).resolves.toBe(false);

    expect(client.getQueryState(["users", session.id])?.isInvalidated).toBe(
      true,
    );
    expect(client.getQueryData(["projects", session.id])).toEqual([
      { id: "project-1" },
    ]);
  });
});
