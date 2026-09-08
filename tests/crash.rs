#![cfg(feature = "failpoints")]

use std::{
    io::{Read, Write},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use eventglass::{
    db::{
        self,
        ingest::{self, IngestProject},
    },
    model::RecordKind,
    sentry::{self, ProjectContext},
};

struct Server(Child, u16);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(binary: &std::path::Path, path: &std::path::Path, point: Option<&str>) -> Result<Server> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let mut command = Command::new(binary);
    command
        .arg("serve")
        .env("EVENTGLASS_DATA_DIR", path)
        .env("EVENTGLASS_ADDR", format!("127.0.0.1:{port}"))
        .env("EVENTGLASS_BASE_URL", "http://127.0.0.1:8080")
        .env_remove("EVENTGLASS_S3_URL")
        .env_remove("EVENTGLASS_FAILPOINT")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    if let Some(point) = point {
        command.env("EVENTGLASS_FAILPOINT", point);
    }
    Ok(Server(command.spawn()?, port))
}

fn ready(port: u16) -> bool {
    let Ok(mut stream) = std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    if stream
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.1 200")
}

#[test]
fn process_crash_at_each_commit_phase_recovers_acknowledged_records_once() -> Result<()> {
    let binaries = tempfile::tempdir()?;
    let binary = binaries.path().join("eventglass-failpoints");
    std::fs::copy(env!("CARGO_BIN_EXE_eventglass"), &binary)?;
    for point in [
        "before_native_commit",
        "after_native_commit",
        "after_sqlite_finalize",
        "after_reader_reload",
    ] {
        let dir = tempfile::tempdir()?;
        let mut db = db::open(&dir.path().join("meta.db"))?;
        db.execute_batch("INSERT INTO runtime_state(singleton,installation_id,storage_generation,next_ingest_seq) VALUES(1,'crash-test','generation',1);
            INSERT INTO projects(id,slug,name,created_at_us,updated_at_us) VALUES(1,'test','Test',0,0);
            INSERT INTO project_keys(id,project_id,public_key,created_at_us) VALUES(1,1,'public',0)")?;
        let context = ProjectContext {
            project_id: 1,
            slug: "test".into(),
            public_key: "public".into(),
            scrub_keys: vec![],
        };
        let error = sentry::normalize_store(
            b"{\"event_id\":\"0123456789abcdef0123456789abcdef\",\"message\":\"crash ledger\"}",
            &context,
            uuid::Uuid::new_v4(),
            1788860000000000,
            &Default::default(),
        )?
        .records
        .remove(0);
        let mut log = error.clone();
        log.kind = RecordKind::Log;
        log.source_event_id = None;
        log.record_id = "a".repeat(64);
        log.issue_id = None;
        log.fingerprint = None;
        log.fingerprint_version = None;
        let mut log2 = log.clone();
        log2.record_id = "b".repeat(64);
        let receipt = ingest::accept(
            &mut db,
            IngestProject {
                id: 1,
                slug: "test".into(),
                public_key: "public".into(),
            },
            "crash-acceptance",
            vec![error.clone(), error, log, log2],
            &Default::default(),
        )?;
        assert_eq!(receipt.accepted, 4);
        assert!(eventglass::db::indexer::prepare(&db, &Default::default())?.is_some());
        drop(db);
        let mut first = start(&binary, dir.path(), Some(point))?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = first.0.try_wait()? {
                assert_eq!(status.code(), Some(86), "{point}");
                break;
            }
            if Instant::now() > deadline {
                bail!("failpoint child did not exit: {point}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(first);
        let mut recovered = start(&binary, dir.path(), None)?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = recovered.0.try_wait()? {
                bail!("recovery child exited early: {status}");
            }
            let db = db::open_reader(&dir.path().join("meta.db"))?;
            let applied = eventglass::db::indexer::applied(&db)?;
            if applied.ingest_seq == 4 && ready(recovered.1) {
                break;
            }
            if Instant::now() > deadline {
                bail!("recovery never finalized: {point}");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(recovered);
        let db = db::open_reader(&dir.path().join("meta.db"))?;
        let id: String = db.query_row("SELECT active_shard_id FROM runtime_state", [], |r| {
            r.get(0)
        })?;
        let index = tantivy::Index::open_in_dir(dir.path().join("shards").join(id))?;
        assert_eq!(
            index.reader()?.searcher().num_docs(),
            3,
            "{point}: native replay duplicated records"
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM issue_occurrences", [], |r| r
                .get::<_, i64>(0))?,
            1,
            "{point}"
        );
        assert_eq!(
            db.query_row("SELECT sum(occurrence_count) FROM issues", [], |r| r
                .get::<_, i64>(0))?,
            1,
            "{point}"
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))?,
            0,
            "{point}"
        );
    }
    Ok(())
}
