package main

import (
	"errors"
	"os"
	"time"

	"github.com/getsentry/sentry-go"
)

func main() {
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:              os.Getenv("SENTRY_FIXTURE_DSN"),
		Environment:      "fixture",
		Release:          "eventglass-sdk-fixture@1",
		AttachStacktrace: true,
	}); err != nil {
		panic(err)
	}
	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("service.name", "fixture-go")
		extra := map[string]interface{}{"fixture_unicode": "안녕하세요 👋"}
		if secret := os.Getenv("SENTRY_FIXTURE_SECRET"); secret != "" {
			extra["password"] = secret // pragma: allowlist secret
		}
		scope.SetContext("fixture", extra)
	})
	sentry.CaptureException(errors.New("go fixture exception"))
	if !sentry.Flush(5 * time.Second) {
		panic("Sentry flush timed out after exception")
	}
	sentry.CaptureMessage("go fixture message")
	if !sentry.Flush(5 * time.Second) {
		panic("Sentry flush timed out after message")
	}
}
