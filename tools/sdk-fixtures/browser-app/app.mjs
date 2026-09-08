import * as Sentry from "@sentry/browser";

const parameters = new URLSearchParams(window.location.search);
const dsn = parameters.get("dsn");
const secret = parameters.get("secret");
if (!dsn) {
  throw new Error("browser fixture requires a DSN");
}

Sentry.init({
  dsn,
  enableLogs: true,
  defaultIntegrations: false,
  integrations: [
    Sentry.browserApiErrorsIntegration(),
    Sentry.globalHandlersIntegration(),
    Sentry.consoleLoggingIntegration({ levels: ["log", "warn", "error"] }),
  ],
  environment: "fixture",
  release: "eventglass-sdk-fixture@1",
  sendClientReports: false,
  autoSessionTracking: false,
});

Sentry.setUser({ id: "fixture-user", email: "fixture@example.invalid" });
Sentry.setTag("service.name", "fixture-browser");
Sentry.setExtra("fixture_unicode", "안녕하세요 👋");
if (secret) {
  Sentry.setExtra("password", secret);
}
Sentry.addBreadcrumb({ category: "fixture", message: "before browser exception" });

async function emitAndFlush(emit) {
  emit();
  if (!(await Sentry.flush(10000))) {
    throw new Error("browser Sentry.flush() timed out");
  }
  return { flushed: true };
}

window.fixture = {
  exception: () =>
    emitAndFlush(() =>
      Sentry.captureException(new Error("browser fixture exception")),
    ),
  message: () =>
    emitAndFlush(() =>
      Sentry.captureMessage("browser fixture message", "warning"),
    ),
  console: () =>
    emitAndFlush(() =>
      console.log("browser fixture console info"),
    ),
};
