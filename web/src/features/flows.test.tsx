import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../api/client";
import type { Session } from "../api/types";
import { Providers, queryClient, SessionProvider } from "../app/providers";
import { LoginPage } from "./auth/LoginPage";
import { IssuesPage } from "./issues/IssuesPage";
import { SearchWorkspace } from "./search/SearchWorkspace";
import { ProjectsPage } from "./projects/ProjectsPage";
import { AlertsPage } from "./alerts/AlertsPage";
import { UsersPage } from "./users/UsersPage";
import { SystemPage } from "./system/SystemPage";

const session: Session = {
  user_id: "9007199254740993",
  email: "operator@example.test",
  expires_at: "2026-09-21T00:00:00Z",
  csrf_token: "x".repeat(43),
  is_installation_admin: false,
  tenants: [{ tenant_id: "7", name: "Test tenant", role: "member", project_grants: [{ project_id: "11", role: "viewer" }] }],
};

afterEach(() => {
  cleanup();
  queryClient.clear();
  vi.restoreAllMocks();
});

// Component contract tests with mocked APIs, not connected browser E2E evidence.
describe("operator component contracts", () => {
  it("loads the next project page with its opaque cursor", async () => {
    const projects = vi.spyOn(api,"projects").mockResolvedValueOnce({items:[],next_cursor:"page-two"}).mockResolvedValueOnce({items:[{
      tenant_id:"7",project_id:"12",name:"Second page project",state:"active",default_service:"",allowed_origins:[],revision:"1",auth_revision:"1",scrub_revision:"1",
    }],next_cursor:null});
    render(<Providers><SessionProvider session={session}><ProjectsPage /></SessionProvider></Providers>);
    fireEvent.click(await screen.findByRole("button",{name:"Next page"}));
    expect(await screen.findByText("Second page project")).toBeInTheDocument();
    expect(projects).toHaveBeenLastCalledWith("7","page-two",expect.any(AbortSignal));
  });

  it("loads alerts only for the selected project, not every project", async () => {
    vi.spyOn(api,"projects").mockResolvedValue({items:[1,2,3].map((id)=>({tenant_id:"7",project_id:String(id),name:`Project ${id}`,state:"active" as const,default_service:"",allowed_origins:[],revision:"1",auth_revision:"1",scrub_revision:"1"})),next_cursor:null});
    const rules = vi.spyOn(api,"alerts").mockResolvedValue({items:[],next_cursor:null});
    const deliveries = vi.spyOn(api,"deliveries").mockResolvedValue({items:[],next_cursor:null});
    vi.spyOn(api,"destinations").mockResolvedValue({items:[],next_cursor:null});
    render(<Providers><SessionProvider session={session}><AlertsPage /></SessionProvider></Providers>);
    expect(await screen.findByRole("heading",{name:"Project 1"})).toBeInTheDocument();
    expect(rules).toHaveBeenCalledTimes(1);
    expect(deliveries).toHaveBeenCalledTimes(1);
    fireEvent.change(screen.getByLabelText("Project"),{target:{value:"3"}});
    expect(await screen.findByRole("heading",{name:"Project 3"})).toBeInTheDocument();
    expect(rules).toHaveBeenLastCalledWith("7","3",undefined,expect.any(AbortSignal));
  });

  it("allows an operator to retry a failed delivery with its revision", async () => {
    const operator = { ...session, tenants: [{ tenant_id: "7", name: "Test tenant", role: "member" as const, project_grants: [{ project_id: "11", role: "operator" as const }] }] };
    vi.spyOn(api, "projects").mockResolvedValue({ items: [{ tenant_id: "7", project_id: "11", name: "Payments", state: "active", default_service: "", allowed_origins: [], revision: "1", auth_revision: "1", scrub_revision: "1" }], next_cursor: null });
    vi.spyOn(api, "alerts").mockResolvedValue({ items: [], next_cursor: null });
    vi.spyOn(api, "destinations").mockResolvedValue({ items: [], next_cursor: null });
    vi.spyOn(api, "deliveries").mockResolvedValue({ items: [{ delivery_id: "00000000-0000-4000-8000-000000000001", alert_id: "00000000-0000-4000-8000-000000000002", state: "failed", revision: "4", attempt: 12, error_code: "network", created_at: "2026-09-20T00:00:00Z" }], next_cursor: null });
    const retry = vi.spyOn(api, "retryDelivery").mockResolvedValue({ delivery_id: "00000000-0000-4000-8000-000000000001", alert_id: "00000000-0000-4000-8000-000000000002", state: "queued", revision: "5", attempt: 0, created_at: "2026-09-20T00:00:00Z" });
    render(<Providers><SessionProvider session={operator}><AlertsPage /></SessionProvider></Providers>);
    fireEvent.click(await screen.findByRole("button", { name: "Retry" }));
    await waitFor(() => expect(retry).toHaveBeenCalledWith("00000000-0000-4000-8000-000000000001", "7", "11", "4", operator.csrf_token));
  });

  it("edits users with revision-bound membership state", async () => {
    const admin = { ...session, is_installation_admin: true, tenants: [{ tenant_id: "7", name: "Test tenant", role: "admin" as const, project_grants: [] }] };
    vi.spyOn(api, "users").mockResolvedValue({ items: [{ user_id: "19", email: "member@example.test", state: "active", revision: "3", role: "member", project_grants: [{ project_id: "11", role: "viewer" }] }], next_cursor: null });
    const update = vi.spyOn(api, "updateUser").mockResolvedValue({ user_id: "19", email: "member@example.test", state: "disabled", revision: "4", role: "member", project_grants: [] });
    render(<Providers><SessionProvider session={admin}><UsersPage /></SessionProvider></Providers>);
    expect(await screen.findByText("member@example.test")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("State"), { target: { value: "disabled" } });
    fireEvent.submit(screen.getAllByRole("button", { name: "Save" })[0].closest("form")!);
    await waitFor(() => expect(update).toHaveBeenCalledWith("19", expect.objectContaining({ tenant_id: "7", revision: "3", state: "disabled" }), admin.csrf_token));
  });

  it("shows backup degradation and sends installation retention revision", async () => {
    const admin = { ...session, is_installation_admin: true, tenants: [{ tenant_id: "7", name: "Test tenant", role: "admin" as const, project_grants: [] }] };
    vi.spyOn(api, "sdkOutcomes").mockResolvedValue({ items: [], next_cursor: null });
    vi.spyOn(api, "system").mockResolvedValue({ generation: "2", recovery_state: "verification_required", alerts_paused: true, backup: { state: "stale" }, dependencies: [{ name: "object_storage", status: "degraded" }], resources: [], retention: { days: 30, revision: "8", floor_us: "1" }, lanes: Array.from({ length: 16 }, (_, lane_id) => ({ lane_id, accepted_seq: "4", published_seq: "3" })), ingest: { since: "2026-09-20T00:00:00Z", scope: "tenant", accepted_requests: "1", accepted_records: "1", duplicate_records: "0", conflict_records: "0", published_records: "1", rejected_requests: "0", rejected_since: "2026-09-20T00:00:00Z", rejected_scope: "process" }, sdk: { since: "2026-09-20T00:00:00Z", scope: "tenant", reported_drops: "0", reported_drops_approximate: true, unsupported_items: "0", by_reason: [] } });
    const update = vi.spyOn(api, "updateRetention").mockResolvedValue({ days: 45, revision: "9", floor_us: "2" });
    render(<Providers><SessionProvider session={admin}><SystemPage /></SessionProvider></Providers>);
    expect(await screen.findByText("stale")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Installation-wide days"), { target: { value: "45" } });
    fireEvent.submit(screen.getByRole("button", { name: "Update policy" }).closest("form")!);
    await waitFor(() => expect(update).toHaveBeenCalledWith("7", "8", 45, admin.csrf_token));
  });
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
