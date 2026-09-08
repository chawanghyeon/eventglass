use std::future::IntoFuture;

use eventglass::{app::AppState, config::Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if matches!(
        args.first().map(String::as_str),
        Some("version" | "--version" | "-V")
    ) {
        println!("eventglass {}", eventglass::VERSION);
        return Ok(());
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("eventglass=info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let config = Config::from_env()?;
    let app = AppState::open(config).await?;
    if args.iter().map(String::as_str).collect::<Vec<_>>() == ["admin", "setup-token"] {
        println!("{}", eventglass::http::issue_setup_token(&app).await?);
        return Ok(());
    }
    anyhow::ensure!(args.is_empty() || args == ["serve"], "unknown command");
    let app = app.start_core().await?;
    let indexer = app.indexer.clone().expect("started core has an indexer");
    let listener = tokio::net::TcpListener::bind(app.config.addr).await?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, eventglass::http::router(app))
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => { result?; indexer.shutdown().await?; },
        result = shutdown_signal() => {
            result?;
            let _ = stop.send(());
            let drain = async { server.await?; indexer.shutdown().await?; Ok::<_,anyhow::Error>(()) };
            if let Ok(result) = tokio::time::timeout(std::time::Duration::from_secs(30),drain).await {
                result?;
            } else {
                // A Tokio runtime drop can wait indefinitely for blocking I/O.
                // Durable Inbox/native C are reconciled on the next startup.
                eprintln!("eventglass: shutdown deadline exceeded; recovery required on restart");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

async fn shutdown_signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => {result?;}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }
    Ok(())
}
