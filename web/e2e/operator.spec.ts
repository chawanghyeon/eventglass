import { expect, test } from "@playwright/test";

test("SDK ACK becomes safe Issue UI while logs and breadcrumbs keep their roles", async ({ page, request }) => {
  const bootstrap = process.env.EVENTGLASS_E2E_BOOTSTRAP_TOKEN;
  if (!bootstrap) throw new Error("EVENTGLASS_E2E_BOOTSTRAP_TOKEN is required");

  await page.goto("/setup");
  await page.getByLabel("Bootstrap token").fill(bootstrap);
  await page.getByLabel("Administrator email").fill("browser-admin@example.invalid");
  await page.getByLabel("Password").fill("browser-only-integration-password");
  await page.getByLabel("Tenant name").fill("Browser tenant");
  await page.getByRole("button", { name: "Create installation" }).click();
  await expect(page.getByRole("heading", { name: "Projects" })).toBeVisible();

  await page.getByLabel("Name").fill("Browser project");
  await page.getByLabel("Default service").fill("browser-e2e");
  await page.getByLabel("Allowed browser origins").fill(page.url().replace(/\/projects$/, ""));
  await page.getByRole("button", { name: "Create project" }).click();
  const project = page.getByRole("article").filter({ hasText: "Browser project" });
  await expect(project).toBeVisible();
  await project.getByLabel("New key label").fill("Playwright SDK");
  await project.getByRole("button", { name: "Create ingest key" }).click();
  const oneTime = project.locator(".one-time");
  await expect(oneTime).toContainText("will not be shown again");
  const dsn = (await oneTime.locator("pre").first().textContent())!.trim();
  const parsed = new URL(dsn);
  const projectID = parsed.pathname.slice(1);
  const eventMessage = "<img src=x onerror=window.__eventglass_xss=1> payment failed";
  const breadcrumb = "breadcrumb-only-marker";
  const frame = "<script>frame-must-stay-text</script>";
  const now = new Date();
  const event = {
    event_id: "11111111111111111111111111111111", timestamp: now.toISOString(), level: "error", message: eventMessage,
    breadcrumbs: [{ timestamp: now.toISOString(), category: "ui.click", message: breadcrumb }],
    exception: { values: [{ type: "BrowserFailure", value: eventMessage, stacktrace: { frames: [{ function: frame, filename: "<svg/onload=window.__eventglass_xss=2>", lineno: 7, in_app: true }] } }] },
    sdk: { name: "playwright.fixture", version: "1" },
  };
  const logs = { version: 2, items: [{ timestamp: now.getTime() / 1000, level: "error", body: "error-level-log-must-not-be-issue", severity_number: 17, attributes: { "service.name": { value: "browser-e2e", type: "string" } } }] };
  const report = { timestamp: now.getTime() / 1000, discarded_events: [{ reason: "network_error", category: "error", quantity: 3 }] };
  const envelope = makeEnvelope(dsn, [
    { type: "event", value: event },
    { type: "log", value: logs, item_count: 1 },
    { type: "client_report", value: report },
  ]);
  const response = await request.post(`/api/${projectID}/envelope/`, {
    data: envelope,
    headers: { "Content-Type": "application/x-sentry-envelope", "X-Sentry-Auth": `Sentry sentry_version=7, sentry_key=${parsed.username}` },
  });
  expect(response.status()).toBe(200);

  await page.goto("/issues");
  await expect.poll(async () => {
    await page.reload();
    return page.locator(".issue-card").count();
  }, { timeout: 30_000 }).toBe(1);
  await expect(page.locator(".issue-card")).toContainText("payment failed");
  await expect(page.getByText("error-level-log-must-not-be-issue")).toHaveCount(0);
  await expect(page.getByText(breadcrumb)).toHaveCount(0);
  await expect(page.locator("img, script:not([type='module']), svg")).toHaveCount(0);

  await page.locator(".issue-card").click();
  const occurrence = page.locator(".record-card").first();
  await expect(occurrence).toBeVisible();
  await occurrence.click();
  await expect(page.getByText(breadcrumb, { exact: false }).first()).toBeVisible();
  await expect(page.getByText(frame, { exact: false }).first()).toBeVisible();
  await expect(page.locator("img, script:not([type='module']), svg")).toHaveCount(0);

  await page.goto("/system");
  await expect(page.getByText("network_error")).toBeVisible();
  await expect(page.getByText("3", { exact: true })).toBeVisible();
  await expect(page.getByText("missing", { exact: true })).toBeVisible();
  await page.getByLabel("Installation-wide days").fill("45");
  await page.getByRole("button", { name: "Update policy" }).click();
  await expect(page.getByText(/Current 45 days/)).toBeVisible();

  await page.goto("/users");
  await page.getByLabel("Email").fill("browser-viewer@example.invalid");
  await page.getByLabel("Initial password").fill("browser-viewer-password");
  await page.getByRole("button", { name: "Create user" }).click();
  const managedUser = page.getByRole("article").filter({ hasText: "browser-viewer@example.invalid" });
  await expect(managedUser).toBeVisible();
  await managedUser.getByLabel("Project grants").fill(`${projectID}:viewer`);
  await managedUser.getByRole("button", { name: "Save" }).click();
  await expect(managedUser.getByText(/Revision 2/)).toBeVisible();

  await page.goto("/alerts");
  await page.getByLabel("Name").fill("Browser webhook");
  await page.getByLabel("HTTPS URL").fill("https://example.invalid/eventglass");
  await page.getByLabel("Signing secret").fill("browser-secret-not-sent");
  await page.getByRole("button", { name: "Add destination" }).click();
  await expect(page.getByText("Browser webhook", { exact: false }).first()).toBeVisible();
  await page.getByLabel("New issue rule").fill("New browser issues");
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("New browser issues", { exact: false })).toBeVisible();

  await page.goto("/account");
  await page.getByLabel("Current password").fill("browser-only-integration-password");
  await page.getByLabel("New password").fill("browser-rotated-integration-password");
  await page.getByRole("button", { name: "Change password" }).click();
  await expect(page.getByRole("heading", { name: "Sign in" })).toBeVisible();
});

function makeEnvelope(dsn: string, items: Array<{ type: string; value: unknown; item_count?: number }>): string {
  const lines = [JSON.stringify({ dsn, sdk: { name: "playwright.fixture", version: "1" } })];
  for (const item of items) {
    const payload = JSON.stringify(item.value);
    lines.push(JSON.stringify({ type: item.type, length: new TextEncoder().encode(payload).byteLength, ...(item.item_count ? { item_count: item.item_count } : {}) }));
    lines.push(payload);
  }
  return `${lines.join("\n")}\n`;
}
