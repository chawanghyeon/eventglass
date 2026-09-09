#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";
import { checkReplayUi } from "./replay-ui.mjs";

const binary = process.env.EVENTGLASS_E2E_BIN;
if (!binary) throw new Error("EVENTGLASS_E2E_BIN is required");

const email = "product-ui@example.invalid";
const password = "product-ui-password-2026"; // pragma: allowlist secret -- disposable E2E fixture
const memberEmail = "product-ui-member@example.invalid";
const memberPassword = "product-ui-member-password-2026"; // pragma: allowlist secret -- disposable E2E fixture
const issueTitle = "Product UI regression sentinel";
const dataDir = await mkdtemp(join(tmpdir(), "eventglass-product-ui-"));
const port = await freePort();
const baseUrl = `http://127.0.0.1:${port}`;
const environment = {
  ...process.env,
  EVENTGLASS_ADDR: `127.0.0.1:${port}`,
  EVENTGLASS_BASE_URL: baseUrl,
  EVENTGLASS_DATA_DIR: dataDir,
  RUST_LOG: "eventglass=warn",
};
const setup = spawnSync(binary, ["admin", "setup-token"], {
  encoding: "utf8",
  env: environment,
  timeout: 15_000,
});
if (setup.status !== 0 || !setup.stdout.trim()) {
  await rm(dataDir, { force: true, recursive: true });
  throw new Error(`setup-token failed: ${setup.stderr}`);
}

const serverLog = [];
const server = spawn(binary, ["serve"], {
  env: environment,
  stdio: ["ignore", "ignore", "pipe"],
});
server.stderr.setEncoding("utf8");
server.stderr.on("data", (chunk) => serverLog.push(chunk));

let browser;
try {
  browser = await chromium.launch({ headless: true });
  await waitReady(baseUrl);
  const page = await browser.newPage();
  const browserErrors = [];
  const issueResponses = [];
  page.on("pageerror", (error) => browserErrors.push(String(error)));
  page.on("console", (message) => {
    if (
      message.type() === "error" &&
      !message.text().startsWith("Failed to load resource:")
    ) {
      browserErrors.push(message.text());
    }
  });
  page.on("requestfailed", (request) => {
    const errorText = request.failure()?.errorText ?? "request failed";
    if (errorText !== "net::ERR_ABORTED") {
      browserErrors.push(`${errorText} ${request.url()}`);
    }
  });
  page.on("response", (response) => {
    const url = new URL(response.url());
    const expectedAnonymousSession =
      response.status() === 401 && url.pathname === "/api/auth/me";
    const expectedMemberForbidden =
      response.status() === 403 && url.pathname === "/api/users";
    if (
      response.status() >= 400 &&
      !expectedAnonymousSession &&
      !expectedMemberForbidden
    ) {
      browserErrors.push(`${response.status()} ${response.url()}`);
    }
    if (response.url().includes("/api/issues?")) {
      void response
        .text()
        .then((body) => issueResponses.push({ url: response.url(), body }))
        .catch((error) =>
          issueResponses.push({ url: response.url(), body: String(error) }),
        );
    }
  });

  await page.goto(`${baseUrl}/setup`, { waitUntil: "networkidle" });
  await page.getByLabel("설정 토큰").fill(setup.stdout.trim());
  await page.getByLabel("관리자 이메일").fill(email);
  await page.getByLabel("비밀번호").fill(password);
  await Promise.all([
    page.waitForURL(`${baseUrl}/login`, { timeout: 15_000 }),
    page.getByRole("button", { name: "관리자 만들기" }).click(),
  ]);

  await page.getByLabel("이메일").fill(email);
  await page.getByLabel("비밀번호").fill(password);
  await Promise.all([
    page.waitForURL(`${baseUrl}/projects`, { timeout: 15_000 }),
    page.getByRole("button", { name: "로그인" }).click(),
  ]);

  await page.getByLabel("표시 이름").fill("Product UI E2E");
  await page.getByLabel("Slug").fill("product-ui-e2e");
  await page.getByRole("button", { name: "프로젝트 추가" }).click();
  const card = page.locator("article.project-card", {
    has: page.getByRole("heading", { name: "Product UI E2E" }),
  });
  await card.waitFor({ state: "visible", timeout: 15_000 });
  const idText = await card.locator(".project-id").innerText();
  const projectId = idText.match(/^ID (\d+)$/)?.[1];
  if (!projectId) throw new Error(`project ID was not rendered: ${idText}`);
  await card.getByRole("button", { name: "새 DSN 발급" }).click();
  const dsn = await card.getByLabel("DSN").inputValue();
  validateDsn(dsn, baseUrl, projectId);
  await page.reload({ waitUntil: "networkidle" });
  if ((await card.getByLabel("DSN").inputValue()) !== dsn) {
    throw new Error("Issued DSN disappeared after reload");
  }

  await ingest(page, dsn, "11111111111111111111111111111111");
  await waitForIssue(page, projectId, "unresolved", 1);
  await page.getByRole("link", { name: "Issues", exact: true }).click();
  await page.waitForURL((url) => url.pathname === "/issues", { timeout: 15_000 });
  const firstIssueLink = page.getByRole("link", { name: issueTitle });
  try {
    await firstIssueLink.waitFor({ state: "visible", timeout: 15_000 });
  } catch (error) {
    const api = await page.evaluate(async (projectId) => {
      const response = await fetch(
        `/api/issues?project_id=${projectId}&status=unresolved&limit=100`,
      );
      return { status: response.status, body: await response.text() };
    }, projectId);
    throw new Error(
      `Issue list did not render the indexed row: ${JSON.stringify({ api, issueResponses })}\n${await page.locator("body").innerText()}`,
      { cause: error },
    );
  }
  await firstIssueLink.click();
  await page.getByRole("heading", { name: issueTitle }).waitFor();
  await page.getByText("1회", { exact: true }).waitFor();
  await page.getByRole("button", { name: "원문 보기" }).click();
  await page.getByText("발생 기록 상세").waitFor();
  await page.locator("pre").filter({ hasText: "product-ui-raw-sentinel" }).waitFor();
  await page.getByText("선택한 기록의 필드", { exact: true }).click();
  await page.getByRole("cell", { name: '$["extra"]["detail"]', exact: true }).waitFor();
  await page.getByRole("button", { name: "닫기" }).click();
  await page.getByRole("button", { name: "해결 처리" }).click();
  await page.getByText("해결됨", { exact: true }).waitFor({ timeout: 15_000 });

  await ingest(page, dsn, "22222222222222222222222222222222");
  await waitForIssue(page, projectId, "unresolved", 2);
  await page.getByRole("link", { name: "Issues", exact: true }).click();
  await page.waitForURL((url) => url.pathname === "/issues", { timeout: 15_000 });
  await page.getByRole("link", { name: issueTitle }).click();
  await page.getByText("2회", { exact: true }).waitFor();
  await page.getByRole("table").getByText("미해결", { exact: true }).waitFor();

  await page.getByRole("link", { name: "Logs", exact: true }).click();
  await page.waitForURL((url) => url.pathname === "/logs", { timeout: 15_000 });
  const logTable = page.getByRole("table");
  await logTable.getByText(issueTitle, { exact: true }).first().waitFor();
  await page.getByLabel("시간별 로그 건수").waitFor();
  await logTable.getByRole("button", { name: "상세 보기" }).first().click();
  await page.getByRole("heading", { name: "로그 상세" }).waitFor();
  try {
    await page.locator("pre").filter({ hasText: "product-ui-raw-sentinel" }).waitFor();
  } catch (error) {
    throw new Error(
      `Log detail did not render raw JSON: ${JSON.stringify({ browserErrors })}\n${await page.locator("body").innerText()}`,
      { cause: error },
    );
  }
  await page.getByText("선택한 기록의 필드", { exact: true }).click();
  await page.getByRole("cell", { name: '$["extra"]["detail"]', exact: true }).waitFor();
  await page.getByRole("button", { name: "닫기" }).click();

  await checkReplayUi(page,baseUrl,dsn);

  await page.getByRole("link", { name: "사용자", exact: true }).click();
  await page.getByLabel("이메일").fill(memberEmail);
  await page.getByLabel("임시 비밀번호").fill(memberPassword);
  await page.getByRole("button", { name: "사용자 추가" }).click();
  await page.getByRole("table").getByText(memberEmail, { exact: true }).waitFor();
  await page.getByRole("button", { name: "로그아웃" }).click();
  await page.waitForURL(`${baseUrl}/login`, { timeout: 15_000 });
  await page.getByLabel("이메일").fill(memberEmail);
  await page.getByLabel("비밀번호").fill(memberPassword);
  await Promise.all([
    page.waitForURL(`${baseUrl}/projects`, { timeout: 15_000 }),
    page.getByRole("button", { name: "로그인" }).click(),
  ]);
  await page.goto(`${baseUrl}/users`, { waitUntil: "networkidle" });
  await page
    .getByText("사용자 관리는 관리자 계정에서만 열 수 있습니다.")
    .waitFor();
  const usersResponse = await page.evaluate(async () => {
    const response = await fetch("/api/users");
    return { status: response.status, body: await response.text() };
  });
  if (usersResponse.status !== 403) {
    throw new Error(`member user list was not forbidden: ${JSON.stringify(usersResponse)}`);
  }

  if (browserErrors.length > 0) {
    throw new Error(`browser errors: ${JSON.stringify(browserErrors)}`);
  }
  console.log(
    JSON.stringify({
      result: "pass",
      embedded_ui: true,
      browser: "chromium",
      setup_login_project_dsn: true,
      issue_detail_raw_resolve_regression: true,
      logs_detail_raw: true,
      member_permission: true,
      project_id: projectId,
    }),
  );
} finally {
  await browser?.close();
  let timer;
  try {
    if (server.exitCode === null && server.signalCode === null) {
      const stopped = new Promise((resolve) => server.once("exit", resolve));
      server.kill("SIGTERM");
      const exited = await Promise.race([
        stopped,
        new Promise((resolve) => { timer = setTimeout(() => resolve("timeout"), 30_000); }),
      ]);
      if (exited === "timeout") {
        server.kill("SIGKILL");
        await stopped;
      }
    }
  } finally {
    clearTimeout(timer);
  }
  if (server.exitCode && server.exitCode !== 0) {
    process.stderr.write(serverLog.join(""));
  }
  await rm(dataDir, { force: true, recursive: true });
}

async function freePort() {
  const listener = createServer();
  await new Promise((resolve, reject) => {
    listener.once("error", reject);
    listener.listen(0, "127.0.0.1", resolve);
  });
  const address = listener.address();
  if (!address || typeof address === "string") throw new Error("port allocation failed");
  await new Promise((resolve, reject) =>
    listener.close((error) => (error ? reject(error) : resolve())),
  );
  return address.port;
}

async function waitReady(origin) {
  const deadline = Date.now() + 15_000;
  let last = "not attempted";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${origin}/readyz`);
      last = `${response.status} ${await response.text()}`;
      if (response.ok) return;
    } catch (error) {
      last = String(error);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Eventglass did not become ready: ${last}`);
}

function validateDsn(dsn, origin, projectId) {
  const parsed = new URL(dsn);
  const expected = new URL(origin);
  if (
    parsed.protocol !== "http:" ||
    parsed.hostname !== "127.0.0.1" ||
    parsed.port !== expected.port ||
    parsed.pathname !== `/${projectId}` ||
    !parsed.username
  ) {
    throw new Error(`rendered DSN is invalid: ${dsn}`);
  }
}

async function ingest(page, dsn, eventId) {
  const parsed = new URL(dsn);
  const event = JSON.stringify({
    event_id: eventId,
    level: "error",
    message: issueTitle,
    extra: { detail: "product-ui-raw-sentinel" },
    exception: {
      values: [
        {
          type: "ProductUiFailure",
          value: issueTitle,
          stacktrace: {
            frames: [
              { filename: "product-ui.mjs", function: "exercise", lineno: 1, in_app: true },
            ],
          },
        },
      ],
    },
  });
  const header = JSON.stringify({ dsn });
  const item = JSON.stringify({ type: "event", length: Buffer.byteLength(event) });
  const body = `${header}\n${item}\n${event}\n`;
  const endpoint = `/api/${parsed.pathname.slice(1)}/envelope/?sentry_key=${encodeURIComponent(parsed.username)}`;
  const result = await page.evaluate(
    async ({ endpoint, body }) => {
      const response = await fetch(endpoint, {
        body,
        headers: { "content-type": "application/x-sentry-envelope" },
        method: "POST",
      });
      return { status: response.status, body: await response.text() };
    },
    { endpoint, body },
  );
  if (result.status !== 202) throw new Error(`ingest failed: ${JSON.stringify(result)}`);
}

async function waitForIssue(page, projectId, status, count) {
  const deadline = Date.now() + 30_000;
  let last = { status: 0, body: "not attempted" };
  while (Date.now() < deadline) {
    last = await page.evaluate(
      async ({ projectId, status }) => {
        const response = await fetch(
          `/api/issues?project_id=${projectId}&status=${status}&limit=100`,
        );
        return { status: response.status, body: await response.text() };
      },
      { projectId, status },
    );
    if (last.status === 200) {
      const body = JSON.parse(last.body);
      if (
        body.items?.some(
          (issue) =>
            issue.title === issueTitle &&
            Number(issue.occurrence_count) === count,
        )
      ) {
        return;
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(
    `Issue did not reach ${status}/${count}: ${JSON.stringify(last)}`,
  );
}
