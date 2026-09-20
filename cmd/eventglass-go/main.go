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
	"time"

	"github.com/chawanghyeon/eventglass/internal/app"
	"github.com/chawanghyeon/eventglass/internal/control"
	"github.com/chawanghyeon/eventglass/internal/engine"
	"github.com/chawanghyeon/eventglass/internal/storage"
)

const version = "0.0.0-g06"

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
	case "doctor":
		fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
		fs.SetOutput(stderr)
		databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
		readOnly := fs.Bool("read-only", false, "confirm that no repair mutation is requested")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if !*readOnly || *databaseURL == "" || fs.NArg() != 0 {
			return errors.New("doctor requires --read-only and a database URL")
		}
		return app.Doctor(ctx, *databaseURL, stdout)
	case "backup":
		return runBackupCLI(ctx, args[1:], stdout, stderr)
	case "restore":
		return runRestoreCLI(ctx, args[1:], stdout, stderr)
	case "repair":
		return runRepairCLI(ctx, args[1:], stdout, stderr)
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
	_, err := fmt.Fprintln(w, "usage: eventglass-go <version|engine-child|migrate|migration-manifest|doctor|backup|restore|repair|run>")
	return err
}

func runBackupCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: eventglass-go backup <register|verify> [flags]")
	}
	if args[0] == "verify" {
		fs := flag.NewFlagSet("backup verify", flag.ContinueOnError)
		fs.SetOutput(stderr)
		databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
		report := fs.String("report", "", "private absolute verification report path")
		key := fs.String("attestation-key-file", "", "private absolute HMAC key path")
		expected := fs.Int64("expected-generation", 0, "live storage generation")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 || *databaseURL == "" {
			return errors.New("backup verify has invalid arguments")
		}
		return app.VerifyBackup(ctx, *databaseURL, *report, *key, *expected, stdout)
	}
	if args[0] != "register" {
		return errors.New("usage: eventglass-go backup <register|verify> [flags]")
	}
	fs := flag.NewFlagSet("backup register", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
	backupID := fs.String("backup-id", "", "Eventglass backup UUID")
	externalID := fs.String("external-tool-id", "", "pgBackRest backup label")
	installationID := fs.String("installation-id", "", "installation UUID")
	generation := fs.Int64("generation", 0, "storage generation")
	baseStart := fs.String("base-start", "", "base backup start RFC3339")
	baseEnd := fs.String("base-end", "", "base backup end RFC3339")
	earliest := fs.String("earliest-recoverable", "", "earliest recovery RFC3339")
	latest := fs.String("latest-recoverable", "", "latest recovery RFC3339")
	protected := fs.String("protected-until", "", "object protection RFC3339")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *databaseURL == "" {
		return errors.New("backup register has invalid arguments")
	}
	parse := func(value string) (time.Time, error) { return time.Parse(time.RFC3339, value) }
	start, err := parse(*baseStart)
	if err != nil {
		return err
	}
	end, err := parse(*baseEnd)
	if err != nil {
		return err
	}
	first, err := parse(*earliest)
	if err != nil {
		return err
	}
	last, err := parse(*latest)
	if err != nil {
		return err
	}
	until, err := parse(*protected)
	if err != nil {
		return err
	}
	return app.RegisterBackup(ctx, *databaseURL, control.BackupRegistration{BackupID: *backupID, ExternalToolID: *externalID, InstallationID: *installationID, StorageGeneration: *generation, BaseStart: start, BaseEnd: end, EarliestRecoverable: first, LatestRecoverable: last, ProtectedUntil: until})
}

func runRestoreCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: eventglass-go restore <verify|activate> [flags]")
	}
	fs := flag.NewFlagSet("restore "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
	expected := fs.Int64("expected-generation", 0, "restored storage generation")
	subcommand := args[0]
	switch subcommand {
	case "verify":
		backupID := fs.String("backup-id", "", "registered backup UUID")
		verificationID := fs.String("verification-id", "", "verification UUID")
		recoveryLSN := fs.String("recovery-lsn", "", "restored PostgreSQL recovery LSN")
		report := fs.String("report", "", "new absolute report path")
		key := fs.String("attestation-key-file", "", "private absolute HMAC key path")
		s3 := addS3Flags(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 || *databaseURL == "" {
			return errors.New("restore verify has invalid arguments")
		}
		return app.VerifyRestore(ctx, *databaseURL, *s3, *backupID, *expected, *verificationID, *recoveryLSN, *report, *key, stdout)
	case "activate":
		report := fs.String("report", "", "private absolute verification report path")
		key := fs.String("attestation-key-file", "", "private absolute HMAC key path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 || *databaseURL == "" {
			return errors.New("restore activate has invalid arguments")
		}
		return app.ActivateRestore(ctx, *databaseURL, *report, *key, *expected, stdout)
	default:
		return errors.New("usage: eventglass-go restore <verify|activate> [flags]")
	}
}

func runRepairCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "inspect" {
		return errors.New("usage: eventglass-go repair inspect [flags]")
	}
	fs := flag.NewFlagSet("repair inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", os.Getenv("EVENTGLASS_DATABASE_URL"), "PostgreSQL connection URL")
	tenantID := fs.Int64("tenant", 0, "tenant ID")
	laneID := fs.Int("lane", -1, "lane 0..15")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 || *databaseURL == "" {
		return errors.New("repair inspect has invalid arguments")
	}
	return app.InspectRepair(ctx, *databaseURL, *tenantID, *laneID, stdout)
}

func addS3Flags(fs *flag.FlagSet) *storage.S3Config {
	config := &storage.S3Config{}
	fs.StringVar(&config.Endpoint, "s3-endpoint", os.Getenv("EVENTGLASS_S3_ENDPOINT"), "S3 endpoint")
	fs.StringVar(&config.Region, "s3-region", os.Getenv("EVENTGLASS_S3_REGION"), "S3 region")
	fs.StringVar(&config.Bucket, "s3-bucket", os.Getenv("EVENTGLASS_S3_BUCKET"), "S3 bucket")
	fs.StringVar(&config.Prefix, "s3-prefix", os.Getenv("EVENTGLASS_S3_PREFIX"), "S3 live prefix")
	fs.BoolVar(&config.PathStyle, "s3-path-style", config.Endpoint != "", "use path-style S3 addressing")
	return config
}
