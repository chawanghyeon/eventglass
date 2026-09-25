# Backup verification and disaster recovery

Eventglass coordinates PostgreSQL authority with S3 live objects. A PostgreSQL
backup alone is not healthy: a rehearsal must restore base backup and continuous
WAL to an explicit cut and read every object referenced by that restored database.
Unknown objects found by S3 listing are never adopted.

## Required separation

Use PostgreSQL 17 and the source/checksum pin in `deploy/versions.lock`
(`pgBackRest 2.59.1`). Keep the pgBackRest repository in an S3 prefix distinct
from live journals/bundles. The backup writer must not have live-object delete
permission; the live GC role must not have backup-delete permission. Use verified
TLS and workload credentials in production. The local `deploy/recovery` fixture
uses generated TLS certificates and test credentials only.

Create an independent random attestation key of at least 32 bytes in a root-owned
0600 file. Mount it only into the isolated recovery runner and the narrowly scoped
production import command. Do not store it in PostgreSQL, S3, images, reports, or
this repository. A report is valid for 24 hours and cannot be replaced by a flag
or a hand-written attestation.

## Daily isolated rehearsal

1. Before the base backup, run `eventglass-go backup register` against the live
   database with a new backup UUID, the installation UUID/current generation,
   backup window, PITR window, and protection expiry. Registration precedes any
   GC pass that could mark an object needed by the backup.
2. Run pgBackRest `stanza-create`, `check`, and a full backup; keep continuous
   WAL archive enabled. Record the exact recovery LSN/time and verify archive
   continuity. A completed upload without continuous WAL is not a passing backup.
3. Stop or isolate the restore target from all Eventglass processes and external
   delivery networks. Restore into a new empty PGDATA with pgBackRest to the
   chosen LSN/time and promote it. Never point a rehearsal at the live database.
4. Run `eventglass-go restore verify` on the restored database with the registered
   backup ID, source generation, new verification UUID, actual recovery LSN,
   absolute new report path, attestation key, and read-only live-object S3
   credentials. It first makes the clone fail closed, revokes sessions/releases
   snapshots, then sequentially size/SHA-verifies the complete restored
   `uploaded`/`referenced` inventory. Any missing or corrupt object leaves
   `recovery_state=verification_required`, alerts paused, and no GC evidence.
5. Copy the private 0600 report through the controlled recovery channel. Run
   `eventglass-go backup verify --expected-generation <live-generation>
   --report <path> --attestation-key-file <path>` against the live database.
   HMAC, installation, generation, backup, inventory hash, report hash, LSN and
   freshness must match. This changes only backup/GC verification state; it does
   not change the live storage generation or resume alerts.
6. Confirm `GET /v1/system` reports a fresh backup/GC horizon. The scheduler
   freezes physical deletion automatically after 24 hours without another
   successful rehearsal. Preserve the report according to audit policy without
   preserving its key alongside it.

`./scripts/check recovery` executes this sequence on disposable ARM64 containers,
including two independent PGDATA restores, a WAL-only row, a referenced object,
a newer unreferenced object, signed live import, and a deleted-object failure.
For an actual AWS rehearsal, use a pre-created non-production scratch bucket and
short-lived credentials for a dedicated test role; the script checks the
expected account and bucket owner, uses a fresh
`eventglass-release-check-<random>/` prefix for both live objects and pgBackRest,
and deletes only that prefix on exit. The role needs `s3:ListBucket` constrained
to that prefix pattern and `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject`
on objects below it. The bucket must not contain production data. Endpoint and
profile overrides, long-lived credentials, and AWS China regions are rejected.

Supply values through a secure environment/credential manager, never inline on
the command line:

```sh
export EVENTGLASS_RECOVERY_BACKEND=aws
export EVENTGLASS_AWS_RESTORE_BUCKET='replace-with-dedicated-scratch-bucket'
export EVENTGLASS_AWS_ACCOUNT_ID='replace-with-expected-account-id'
export AWS_REGION='replace-with-standard-aws-region'
# Inject AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN from
# the configured short-lived credential manager; do not paste credentials here.
./scripts/check recovery
```

The AWS branch still restores into disposable local Colima PostgreSQL volumes;
it does not connect to a live database, create a bucket, send alerts, or deploy.
If cleanup fails, the check exits nonzero and prints the exact owned prefix that
requires manual cleanup.

## Disaster activation

Stop API/workers/schedulers, revoke old writers, and prove the old database cannot
be reached. Restore and verify as above. Then run `eventglass-go restore activate`
against the verified clone with the signed report and expected source generation.
Activation accepts only the report already committed by `restore verify`; it
increments `storage_generation`, fences/requeues unfinished durable work,
cancels transient queries, invalidates sessions and keeps alerts paused. Verified
old-generation objects remain readable. New writes use only the new generation.

Before routing traffic, inspect stalled lanes with `eventglass-go doctor
--read-only` and `eventglass-go repair inspect --tenant <id> --lane <0..15>`;
verify contiguous publication cuts and the known ACK oracle through the recovery
point. Explicitly resume external alert delivery only after an installation admin
accepts possible at-least-once repeats. If any reference, generation, signature,
LSN, checksum, or continuity check fails, keep the installation unhealthy and
repair or choose another recovery point—never reconstruct authority from S3 LIST.
