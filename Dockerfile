FROM node:24-bookworm-slim AS frontend-builder

WORKDIR /src/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:1-slim-bookworm AS backend-builder

WORKDIR /src/backend
COPY backend/Cargo.toml ./
COPY backend/Cargo.lock ./
COPY backend/.sqlx ./.sqlx
COPY backend/migrations ./migrations
COPY backend/src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

ENV CONTEXTPOOL_HTTP_ADDR=:42421 \
    CONTEXTPOOL_DATABASE_URL=postgres://contextpool:contextpool@postgres:5432/contextpool \
    CONTEXTPOOL_FRONTEND_DIST=/app/frontend/dist

WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/frontend/dist
COPY --from=backend-builder /src/backend/target/release/contextpool /app/contextpool
COPY --from=frontend-builder /src/frontend/dist /app/frontend/dist

EXPOSE 42421

ENTRYPOINT ["/app/contextpool"]
