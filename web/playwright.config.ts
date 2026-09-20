import { defineConfig } from "@playwright/test";

const port = Number(process.env.EVENTGLASS_WEB_PORT ?? "19080");
const productionURL = process.env.EVENTGLASS_E2E_BASE_URL;

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  expect: { timeout: 30_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: "line",
  use: {
    baseURL: productionURL ?? `http://127.0.0.1:${port}`,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: productionURL ? undefined : {
    command: `npm run dev -- --host 127.0.0.1 --port ${port}`,
    url: `http://127.0.0.1:${port}/setup`,
    reuseExistingServer: false,
    timeout: 30_000,
  },
  projects: [{ name: "chromium", use: { browserName: "chromium" } }],
});
