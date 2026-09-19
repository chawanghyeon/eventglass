package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
)

const version = "0.0.0-g02-i4"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usage(stderr)
	}

	switch args[0] {
	case "version":
		_, err := fmt.Fprintln(stdout, version)
		return err
	case "engine-child":
		return engine.RunChild(ctx, os.Stdin, stdout)
	case "migrate":
		fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
		fs.SetOutput(stderr)
		databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *databaseURL == "" {
			return errors.New("EVENTGLASS_DATABASE_URL or --database-url is required")
		}
		return control.ApplyMigrations(ctx, *databaseURL)
	case "migration-manifest":
		manifest, err := control.MigrationManifest()
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(manifest)
	case "run":
		config, err := app.LoadConfigFromEnv(os.LookupEnv)
		if err != nil {
			return err
		}
		return app.Run(ctx, config)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(w io.Writer) error {
	_, err := fmt.Fprintln(w, "usage: eventglass-go <version|engine-child|migrate|migration-manifest|run>")
	return err
}
