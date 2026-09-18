package main

import (
	"context"
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
	logger := sentry.NewLogger(context.Background())
	logger.Info().
		String("logger.name", "fixture.go").
		Int64("order_id", 9223372036854775807).
		StringSlice("regions", []string{"ap-northeast-2", "eu-west-1"}).
		Emitf("go structured order %s", "order_fixture")
	if !sentry.Flush(5 * time.Second) {
		panic("Sentry flush timed out after structured log")
	}
}
