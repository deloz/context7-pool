# ContextPool

ContextPool is a Context7 relay service that pools API keys, distributes requests with round-robin scheduling, cools down unhealthy keys, and exposes operational stats in a dashboard.

![ContextPool dashboard](docs/images/contextpool-dashboard.png)

## Core Features

- Context7 API key pool with enable/disable controls and masked key display.
- Round-robin request forwarding across available keys.
- Automatic degraded/cooling health states after upstream failures.
- Relay token management for MCP clients.
- Dashboard summaries for key availability, request success rate, latency, status codes, minute buckets, and request logs.
- Docker Compose setup for the application and PostgreSQL.

## Tech Stack

- Backend: Rust, Axum, SQLx, PostgreSQL.
- Frontend: Vue 3, Vite, TypeScript, Element Plus.
- Runtime: Docker Compose, PostgreSQL, static frontend served by the backend container.

## Local Start

Requirements:

- Docker and Docker Compose.
- Node.js 24+ if you want to run the frontend dev server.
- Rust stable if you want to run backend tests locally.

Start the full local stack:

```bash
docker compose up --build
```

Open the dashboard:

```text
http://127.0.0.1:42431/admin/
```

PostgreSQL is exposed on the uncommon local port `45432` to reduce conflicts:

```text
postgres://contextpool:contextpool@127.0.0.1:45432/contextpool
```

## Development

Run backend tests:

```bash
cargo test --manifest-path backend/Cargo.toml --locked
```

Run the frontend build:

```bash
cd frontend
npm run build
```

Run the frontend dev server:

```bash
cd frontend
npm run dev
```

The Vite dev server serves the admin UI under `/admin/` and proxies `/api` to the backend configured in `frontend/vite.config.ts`.

## Notes

- The initial scheduling strategy is intentionally simple round-robin.
- Request logs do not store request bodies, response bodies, Authorization headers, or upstream API keys.
- The dashboard screenshot uses isolated demo data only.
