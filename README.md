# ContextPool

## Overview

ContextPool is a self-hosted Context7 relay service. It keeps a pool of upstream Context7 API keys, distributes relay requests with simple round-robin scheduling, cools down unhealthy keys after failures, and exposes an operational dashboard for setup and monitoring.

![ContextPool dashboard](docs/images/contextpool-dashboard.png)

## Core Features

- Context7 API key pool with enable/disable controls and masked key display.
- Round-robin request forwarding across available keys.
- Automatic degraded and cooling states after upstream failures.
- Relay token management for MCP clients and other trusted callers.
- Dashboard summaries for key availability, request success rate, latency, status codes, minute buckets, and request logs.
- Docker Compose stack for the application and PostgreSQL.

## Architecture

- Backend: Rust service built with Axum and SQLx.
- Database: PostgreSQL, accessed through SQLx migrations and queries.
- Frontend: Vue 3, Vite, TypeScript, and Element Plus.
- Runtime: one application container serving the backend API and built frontend assets, plus one PostgreSQL container.
- Admin UI: served from `/admin/`.
- Health check: served from `/healthz`.
- Admin API: served under `/api/admin/...`.
- Context7 relay: served under `/relay/context7/...`.

## Quick Start

Requirements:

- Docker and Docker Compose.
- Node.js 24+ if you want to run frontend commands locally.
- Rust stable if you want to run backend checks locally.

Start the full local stack:

```bash
docker compose up --build
```

Open the dashboard:

```text
http://127.0.0.1:42421/admin/
```

Health check:

```text
http://127.0.0.1:42421/healthz
```

PostgreSQL is exposed on an uncommon local port to reduce conflicts:

```text
postgres://contextpool:contextpool@127.0.0.1:45432/contextpool
```

The Compose stack maps host port `42421` to the application port `42421`, and host port `45432` to PostgreSQL port `5432`.

## Configuration

Application configuration is read from environment variables. Empty values are ignored.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CONTEXTPOOL_HTTP_ADDR` | `:42421` | Backend listen address. A leading `:port` value binds on `0.0.0.0`. |
| `CONTEXTPOOL_DATABASE_URL` | `postgres://contextpool:contextpool@127.0.0.1:45432/contextpool` | Primary PostgreSQL connection URL. |
| `DATABASE_URL` | unset | Fallback PostgreSQL connection URL when `CONTEXTPOOL_DATABASE_URL` is not set. |
| `CONTEXTPOOL_UPSTREAM_BASE_URL` | unset | Optional generic upstream base URL for non-admin fallback proxy requests. |
| `CONTEXTPOOL_FAILURE_THRESHOLD` | `3` | Consecutive upstream failures before a key enters the cooling state. |
| `CONTEXTPOOL_COOLDOWN_SECONDS` | `30` | Cooling duration in seconds before a failed key can be selected again. |
| `CONTEXTPOOL_FLUSH_BATCH_SIZE` | `64` | Maximum number of buffered runtime state and stats updates flushed at once. |
| `CONTEXTPOOL_FLUSH_INTERVAL` | `100ms` | Runtime state flush interval. Values may use `ms`, `s`, or plain seconds. |
| `CONTEXTPOOL_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown timeout. Values may use `ms`, `s`, or plain seconds. |
| `CONTEXTPOOL_FRONTEND_DIST` | `../frontend/dist` | Directory containing the built frontend assets served under `/admin/`. |

## Using the Relay

Configure Context7 settings and upstream API keys in the dashboard, then create one or more relay bearer tokens from the dashboard or the admin API at `/api/admin/relay-tokens`.

Use the generated relay token in the `Authorization` header:

```text
Authorization: Bearer <relay-token>
```

The Context7 relay currently exposes these paths:

```text
GET /relay/context7/v2/libs/search
GET /relay/context7/v2/context
```

For local Compose runs, call them through the application host port:

```text
http://127.0.0.1:42421/relay/context7/v2/libs/search
http://127.0.0.1:42421/relay/context7/v2/context
```

Do not use an upstream Context7 API key as the relay bearer token. The relay bearer token authenticates the client to ContextPool; ContextPool selects and injects an upstream API key from the configured pool.

## Development

Install frontend dependencies when running frontend commands locally:

```bash
npm ci --prefix frontend
```

Backend tests include PostgreSQL integration coverage. Start the local database first, or set `DATABASE_URL` to another reachable PostgreSQL database:

```bash
docker compose up -d postgres
```

The default local test database URL is:

```text
postgres://contextpool:contextpool@127.0.0.1:45432/contextpool
```

Run quality checks:

```bash
cargo fmt --manifest-path backend/Cargo.toml --check
cargo clippy --manifest-path backend/Cargo.toml --locked -- -D warnings
cargo test --manifest-path backend/Cargo.toml --locked
npm run build --prefix frontend
```

Run the frontend dev server:

```bash
npm run dev --prefix frontend
```

The Vite dev server serves the admin UI under `/admin/`, listens on port `42422`, and proxies `/api` to `http://127.0.0.1:42421`.

## Release

The official release artifact is the GHCR Docker image:

```bash
docker pull ghcr.io/deloz/context7-pool:v0.1.0
```

Release rules:

- `main` must stay releasable.
- Official releases are created only from `vX.Y.Z` tags.
- `backend/Cargo.toml` `version` must match the tag without the leading `v`.
- Docker is the only official release artifact; the service is not published to crates.io.
- Release notes are generated by GitHub Releases.

Publish a release:

```bash
# 1. Update backend/Cargo.toml version, for example 0.1.0.
# Stage every intended release change, including new files.
git add backend/Cargo.toml
git commit -m "chore(release): prepare v0.1.0"

# 2. Create an annotated SemVer tag.
git tag -a v0.1.0 -m "v0.1.0"

# 3. Push the branch and the tag.
git push origin main
git push origin v0.1.0
```

Pushing a `vX.Y.Z` tag runs the release workflow. The workflow checks Rust formatting, runs clippy, runs backend tests, builds the frontend, verifies the Cargo package version, publishes GHCR image tags, and creates a GitHub Release.

For tag `v0.1.0`, the published image tags are:

```text
ghcr.io/deloz/context7-pool:v0.1.0
ghcr.io/deloz/context7-pool:0.1
ghcr.io/deloz/context7-pool:0
ghcr.io/deloz/context7-pool:latest
```

## Notes

- The initial scheduling strategy is intentionally simple round-robin.
- Request logs do not store request bodies, response bodies, Authorization headers, or upstream API keys.
- The dashboard screenshot uses isolated demo data only.
