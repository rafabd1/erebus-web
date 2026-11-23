mod api;
mod automation;
mod blob;
mod capture;
mod config;
mod error;
mod extract;
mod fingerprint;
mod graph;
mod model;
mod proxy;
mod replay;
mod scope;
mod store;

use std::io::Read;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::api::{AppState, router};
use crate::config::Config;
use crate::store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "erebus_webd=info".into()),
        )
        .init();

    let config = Config::parse()
        .prepare()
        .context("failed to prepare runtime configuration")?;
    let store = Arc::new(Store::open(&config.data_dir).context("failed to open traffic store")?);
    let state = AppState::new(config.clone(), store);
    let captures = state.captures();
    let api_shutdown = state.shutdown_token();
    let app = router(state).layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(config.bind)
        .await
        .context("failed to bind local API")?;
    let address = listener.local_addr()?;

    info!(%address, data_dir = %config.data_dir.display(), "erebus web runtime ready");
    println!("EREBUS_WEB_READY http://{address}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(api_shutdown, captures.clone()))
        .await
        .context("local API stopped unexpectedly")?;
    captures.shutdown_all().await;
    Ok(())
}

async fn shutdown_signal(
    api_shutdown: CancellationToken,
    captures: crate::capture::CaptureManager,
) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install terminate handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    let (parent_closed_tx, parent_closed) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("erebus-web-parent-watch".into())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut stdin = stdin.lock();
            let mut byte = [0_u8; 1];
            loop {
                match stdin.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = parent_closed_tx.send(());
        })
        .expect("failed to start parent process watcher");

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
        _ = parent_closed => {},
        _ = api_shutdown.cancelled() => {},
    }
    captures.shutdown_all().await;
}
