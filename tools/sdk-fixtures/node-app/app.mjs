#!/usr/bin/env node
/** Minimal real Sentry Node SDK sender used by the capture fixtures. */

import * as Sentry from "@sentry/node";

const liveSequential = process.env.SENTRY_FIXTURE_MODE === "live-sequential";

Sentry.init({
  dsn: process.env.SENTRY_FIXTURE_DSN,
  enableLogs: true,
  defaultIntegrations: false,
  environment: "fixture",
  release: "eventglass-sdk-fixture@1",
  serverName: "fixture-node-host",
  sendClientReports: false,
});

Sentry.setUser({ id: "fixture-user", email: "fixture@example.invalid" });
Sentry.setTag("service.name", "fixture-node");
Sentry.setExtra("fixture_unicode", "안녕하세요 👋");
if (process.env.SENTRY_FIXTURE_SECRET) {
  // The live Eventglass runner uses a synthetic sentinel to prove server-side
  // scrubbing. Offline capture generation deliberately leaves this unset.
  Sentry.setExtra("password", process.env.SENTRY_FIXTURE_SECRET);
}
Sentry.addBreadcrumb({ category: "fixture", message: "before exception" });

Sentry.captureException(new Error("node fixture exception"));
if (liveSequential && !(await Sentry.flush(5000))) {
  throw new Error("Sentry.flush() timed out after live exception");
}
Sentry.captureMessage("node fixture message", "warning");
if (liveSequential && !(await Sentry.flush(5000))) {
  throw new Error("Sentry.flush() timed out after live message");
}

const logAttributes = (fixtureLevel) => ({
  fixtureLevel,
  ...(process.env.SENTRY_FIXTURE_SECRET
    ? { password: process.env.SENTRY_FIXTURE_SECRET }
    : {}),
});
Sentry.logger.trace("node fixture trace", logAttributes("trace"));
Sentry.logger.debug("node fixture debug", logAttributes("debug"));
Sentry.logger.info("node fixture info", logAttributes("info"));
Sentry.logger.warn("node fixture warning", logAttributes("warning"));
Sentry.logger.error("node fixture error", logAttributes("error"));
Sentry.logger.fatal("node fixture fatal", logAttributes("fatal"));

if (!(await Sentry.flush(5000))) {
  throw new Error("Sentry.flush() timed out");
}
