#!/usr/bin/env node

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { chromium } from "playwright";

const dsn = process.env.SENTRY_FIXTURE_DSN;
const parsedDsn = dsn ? new URL(dsn) : null;
if (
  !parsedDsn ||
  parsedDsn.protocol !== "http:" ||
  parsedDsn.hostname !== "127.0.0.1"
) {
  throw new Error("browser fixture requires a loopback HTTP DSN");
}

const bundle = await readFile(new URL("./dist/app.js", import.meta.url));
const server = createServer((request, response) => {
  if (request.url?.startsWith("/app.js")) {
    response.writeHead(200, {
      "Content-Type": "text/javascript; charset=utf-8",
      "Content-Length": bundle.length,
      "Cache-Control": "no-store",
    });
    response.end(bundle);
    return;
  }
  const html = Buffer.from(
    '<!doctype html><meta charset="utf-8"><title>Eventglass browser SDK fixture</title><script src="/app.js"></script>',
  );
  response.writeHead(200, {
    "Content-Type": "text/html; charset=utf-8",
    "Content-Length": html.length,
    "Cache-Control": "no-store",
  });
  response.end(html);
});

await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(0, "127.0.0.1", resolve);
});

const address = server.address();
if (!address || typeof address === "string") {
  throw new Error("browser fixture server did not bind a TCP port");
}
const secret = process.env.SENTRY_FIXTURE_SECRET;
const pageUrl = new URL(`http://127.0.0.1:${address.port}/`);
pageUrl.searchParams.set("dsn", dsn);
if (secret) pageUrl.searchParams.set("secret", secret);
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(String(error)));
  page.on("console", (message) => {
    if (message.type() === "error" && !message.text().startsWith("Failed to load resource:")) {
      errors.push(message.text());
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 400) {
      errors.push(`${response.status()} ${response.url()}`);
    }
  });
  await page.goto(pageUrl.href, { waitUntil: "load", timeout: 15000 });
  const operations = {
    exception: "browser fixture exception",
    message: "browser fixture message",
    console: "browser fixture console info",
  };
  for (const [operation, expectedMessage] of Object.entries(operations)) {
    const [response, result] = await Promise.all([
      page.waitForResponse(
        (candidate) =>
          candidate.url().includes("/envelope/") &&
          candidate.request().postData()?.includes(expectedMessage),
        { timeout: 15000 },
      ),
      page.evaluate((name) => window.fixture[name](), operation),
    ]);
    if (response.status() !== 202 || !result?.flushed) {
      throw new Error(
        `browser fixture ${operation} failed: ${response.status()} ${JSON.stringify(result)}`,
      );
    }
  }
  if (errors.length > 0) {
    throw new Error(`browser fixture failed: ${JSON.stringify({ errors })}`);
  }
} finally {
  await browser.close();
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}
