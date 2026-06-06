use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgPoolOptions};

pub async fn connect(database_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(16)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
        .context("connect postgres")
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("run database migrations")
}
