import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../api/client";
import type { Session } from "../api/types";
import { Providers, queryClient, SessionProvider } from "../app/providers";
import { LoginPage } from "./auth/LoginPage";
import { IssuesPage } from "./issues/IssuesPage";
import { SearchWorkspace } from "./logs/SearchWorkspace";
import { ProjectsPage } from "./projects/ProjectsPage";

const session: Session = {
  user_id: "9007199254740993",
  email: "operator@example.test",
  expires_at: "2026-09-21T00:00:00Z",
  csrf_token: "x".repeat(43),
  is_installation_admin: false,
  tenants: [{ tenant_id: "7", name: "Test tenant", role: "member", project_grants: [{ project_id: "11", role: "viewer" }] }],
};

afterEach(() => {
  queryClient.clear();
  vi.restoreAllMocks();
});

// Component contract tests with mocked APIs, not connected browser E2E evidence.
describe("operator component contracts", () => {
  it("signs in with a local password form and stores the generated session DTO", async () => {
    vi.spyOn(api, "login").mockResolvedValue(session);
    render(<Providers><MemoryRouter initialEntries={["/login"]}><Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/logs" element={<p>Log workspace</p>} />
    </Routes></MemoryRouter></Providers>);
    fireEvent.change(screen.getByLabelText("Email"), { target: { value: session.email } });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "a-safe-test-password" } });
    fireEvent.submit(screen.getByRole("button", { name: "Sign in" }).closest("form")!);
    expect(await screen.findByText("Log workspace")).toBeInTheDocument();
    expect(api.login).toHaveBeenCalledWith(session.email, "a-safe-test-password");
  });

  it("renders exact project identifiers from generated contracts", async () => {
    vi.spyOn(api, "projects").mockResolvedValue({ items: [{
      tenant_id: "7", project_id: "9007199254740993", name: "Payments", state: "active", default_service: "pay",
      allowed_origins: [], revision: "12", auth_revision: "3", scrub_revision: "4",
    }], next_cursor: null });
    render(<Providers><SessionProvider session={session}><ProjectsPage /></SessionProvider></Providers>);
    expect(await screen.findByText("Payments")).toBeInTheDocument();
    expect(screen.getByText("9,007,199,254,740,993")).toBeInTheDocument();
  });

  it("runs a URL-owned log query and renders server state as text", async () => {
    vi.spyOn(api, "search").mockResolvedValue({
      rows: [{ record_id: "a".repeat(64), project_id: "11", kind: "log", event_time_us: "5", event_time_ns_remainder: 0, received_time_us: "6", level: "info", message: "<script>not html</script>", message_truncated: false }],
      read_token: "opaque", next_cursor: null, complete: true,
      stats: { scanned_bytes: "123", objects: "1", cache_bytes: "0", elapsed_ms: "4", cut: [], visibility_lag_ms: null }, warnings: [],
    });
    render(<Providers><SessionProvider session={session}><MemoryRouter initialEntries={["/logs?projects=11&kinds=log&basis=event&start=1&end=10&sort=event_desc"]}>
      <Routes><Route path="/logs" element={<SearchWorkspace title="Logs" defaultKinds={["log"]} />} /></Routes>
    </MemoryRouter></SessionProvider></Providers>);
    expect(await screen.findByText("<script>not html</script>")).toBeInTheDocument();
    expect(document.querySelector("script:not([type='module'])")).toBeNull();
  });

  it("renders issue lifetime state without claiming retained detail", async () => {
    vi.spyOn(api, "issues").mockResolvedValue({ items: [{
      issue_id: "b".repeat(64), project_id: "11", status: "unresolved", revision: "2", title: "Payment failed",
      grouping_version: 1, lifetime_occurrence_count: "9007199254740993",
      first: { event_us: "1", ns: 0, record_id: "a".repeat(64), release: null },
      last: { event_us: "2", ns: 0, record_id: "b".repeat(64), release: null },
      last_received_us: "3", detail_retention_floor_us: "0",
    }], next_cursor: null });
    render(<Providers><SessionProvider session={session}><MemoryRouter><IssuesPage /></MemoryRouter></SessionProvider></Providers>);
    expect(await screen.findByText("Payment failed")).toBeInTheDocument();
    expect(screen.getByText("Counts are lifetime published occurrences; retained detail may expire.")).toBeInTheDocument();
  });
});
