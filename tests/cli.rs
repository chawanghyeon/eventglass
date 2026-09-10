use std::{
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

fn command(data_dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_eventglass"));
    command
        .env("EVENTGLASS_DATA_DIR", data_dir)
        .env("EVENTGLASS_BASE_URL", "http://127.0.0.1:8080")
        .env_remove("RUST_LOG")
        .env_remove("EVENTGLASS_S3_URL")
        .env_remove("EVENTGLASS_S3_ENDPOINT")
        .env_remove("EVENTGLASS_S3_INITIALIZE");
    command
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    command(data_dir).args(args).output().unwrap()
}

#[test]
fn version_usage_setup_token_reset_and_doctor_commands_execute_the_real_binary() {
    let directory = tempfile::tempdir().unwrap();
    let version = run(directory.path(), &["version"]);
    assert!(version.status.success());
    assert!(
        String::from_utf8(version.stdout)
            .unwrap()
            .starts_with("eventglass 0.1.0 (")
    );

    let invalid = run(directory.path(), &["unknown"]);
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("usage: eventglass"));

    let setup = run(directory.path(), &["admin", "setup-token"]);
    assert!(
        setup.status.success(),
        "{}",
        String::from_utf8_lossy(&setup.stderr)
    );
    assert!(!String::from_utf8(setup.stdout).unwrap().trim().is_empty());

    let database = eventglass::db::open(&directory.path().join("meta.db")).unwrap();
    database
        .execute(
            "INSERT INTO users(email,password_hash,role,is_active,created_at_us,updated_at_us)
             VALUES('admin@example.test','x','admin',1,0,0)",
            [],
        )
        .unwrap();
    drop(database);
    let reset = run(
        directory.path(),
        &["admin", "reset-password", "admin@example.test"],
    );
    assert!(
        reset.status.success(),
        "{}",
        String::from_utf8_lossy(&reset.stderr)
    );
    assert!(!String::from_utf8(reset.stdout).unwrap().trim().is_empty());

    let doctor = run(directory.path(), &["doctor"]);
    assert!(
        doctor.status.success(),
        "{}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert!(report.is_object());
}

#[cfg(unix)]
fn serve_and_signal(signal_name: &str) {
    let directory = tempfile::tempdir().unwrap();
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = probe.local_addr().unwrap();
    drop(probe);

    let mut child = command(directory.path())
        .env("EVENTGLASS_ADDR", address.to_string())
        .arg("serve")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "server exited before binding"
        );
        assert!(
            Instant::now() < deadline,
            "server did not bind before deadline"
        );
        thread::sleep(Duration::from_millis(25));
    }

    let signal = Command::new("/bin/kill")
        .args([signal_name, &child.id().to_string()])
        .status()
        .unwrap();
    assert!(signal.success());
    let status = child.wait().unwrap();
    assert!(
        status.success(),
        "server did not shut down cleanly: {status}"
    );
}

#[cfg(unix)]
#[test]
fn serve_starts_and_exits_cleanly_on_sigint_and_sigterm() {
    serve_and_signal("-INT");
    serve_and_signal("-TERM");
}
