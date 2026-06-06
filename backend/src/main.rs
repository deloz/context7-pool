use anyhow::Result;
use contextpool::{
    admin, auth,
    config::Config,
    db,
    http::{self, AppState},
    pool, proxy, settings, stats,
};
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::load()?;
    let pool = db::connect(&cfg.database_url).await?;
    db::migrate(&pool).await?;

    let manager = pool::Manager::new(pool::Config {
        failure_threshold: cfg.failure_threshold,
        cooldown: cfg.cooldown,
        flush_interval: cfg.flush_interval,
        flush_batch_size: cfg.flush_batch_size,
    });
    let settings_service = settings::Service::new(pool.clone());
    let stats_service = stats::Service::new(
        pool.clone(),
        stats::Config {
            flush_interval: cfg.flush_interval,
            batch_size: cfg.flush_batch_size,
            queue_size: 2048,
        },
    );
    let auth_service = auth::Service::new(pool.clone());
    auth_service.bootstrap().await?;

    let admin_service = admin::Service::new(
        pool.clone(),
        manager.clone(),
        settings_service.clone(),
        stats_service.clone(),
    );
    admin_service.bootstrap().await?;

    let proxy_service = proxy::Service::new(
        manager.clone(),
        settings_service,
        stats_service.clone(),
        cfg.upstream_base_url.clone(),
    )?;

    let app = http::router(AppState {
        auth: auth_service,
        admin: admin_service.clone(),
        proxy: proxy_service,
        frontend_dist: cfg.frontend_dist.clone(),
    });

    let stats_task = {
        let stats_service = stats_service.clone();
        tokio::spawn(async move { stats_service.run().await })
    };
    let manager_task = {
        let manager = manager.clone();
        let admin_service = admin_service.clone();
        tokio::spawn(async move {
            manager
                .run(|states| {
                    let admin_service = admin_service.clone();
                    async move { admin_service.flush_runtime_states(states).await }
                })
                .await;
        })
    };

    let listener = TcpListener::bind(cfg.http_addr).await?;
    tracing::info!("contextpool listening on {}", cfg.http_addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    stats_task.abort();
    manager_task.abort();
    let _ = stats_service.flush().await;
    let mut flush = |states| {
        let admin_service = admin_service.clone();
        async move { admin_service.flush_runtime_states(states).await }
    };
    let _ = manager.flush_once(&mut flush).await;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
