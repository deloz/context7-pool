# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "psycopg[binary]>=3.2",
# ]
# ///
# Run with:
# uv run --with 'psycopg[binary]>=3.2' python backend/scripts/migrate_sqlite_to_postgres.py
from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Iterable


DEFAULT_POSTGRES_URL = "postgres://contextpool:contextpool@127.0.0.1:45432/contextpool"

TABLE_ORDER = [
    "settings",
    "admin_users",
    "api_keys",
    "relay_tokens",
    "admin_sessions",
    "context7_request_logs",
    "context7_minute_stats",
]

TIME_COLUMNS = {
    "api_keys": {"cooldown_until", "last_success_at", "created_at", "updated_at"},
    "admin_users": {"created_at", "updated_at"},
    "admin_sessions": {"expires_at", "created_at", "last_used_at"},
    "relay_tokens": {"created_at", "last_used_at", "revoked_at"},
    "settings": {"created_at", "updated_at"},
    "context7_request_logs": {"started_at", "finished_at"},
    "context7_minute_stats": {"minute_at", "updated_at"},
}

POSTGRES_COLUMNS = {
    "settings": ["key", "value", "created_at", "updated_at"],
    "admin_users": ["id", "username", "password_hash", "created_at", "updated_at"],
    "api_keys": [
        "id",
        "name",
        "api_key",
        "enabled",
        "health_status",
        "failure_streak",
        "cooldown_until",
        "last_error",
        "last_status_code",
        "last_success_at",
        "created_at",
        "updated_at",
    ],
    "relay_tokens": [
        "id",
        "name",
        "token_hash",
        "token",
        "masked_token",
        "created_at",
        "last_used_at",
        "revoked_at",
    ],
    "admin_sessions": ["id", "token_hash", "admin_user_id", "expires_at", "created_at", "last_used_at"],
    "context7_request_logs": [
        "id",
        "api_key_id",
        "api_key_name",
        "method",
        "path",
        "query",
        "status_code",
        "success",
        "latency_ms",
        "error",
        "client_ip",
        "user_agent",
        "client_source",
        "client_ide",
        "client_version",
        "transport",
        "started_at",
        "finished_at",
    ],
    "context7_minute_stats": [
        "id",
        "api_key_id",
        "api_key_name",
        "minute_at",
        "total_requests",
        "success_requests",
        "failed_requests",
        "status_2xx",
        "status_4xx",
        "status_5xx",
        "network_errors",
        "total_latency_ms",
        "max_latency_ms",
        "last_status_code",
        "last_error",
        "updated_at",
    ],
}

IDENTITY_TABLES = [
    "api_keys",
    "admin_users",
    "admin_sessions",
    "relay_tokens",
    "context7_request_logs",
    "context7_minute_stats",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate ContextPool SQLite data to PostgreSQL.")
    parser.add_argument("--sqlite", default="backend/contextpool.db", help="source SQLite db path")
    parser.add_argument("--postgres-url", default=DEFAULT_POSTGRES_URL, help="target PostgreSQL URL")
    parser.add_argument("--no-backup", action="store_true", help="skip SQLite backup")
    parser.add_argument("--truncate", action="store_true", help="truncate target tables before inserting")
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite)
    if not sqlite_path.exists():
        raise SystemExit(f"SQLite file not found: {sqlite_path}")

    if not args.no_backup:
        backup_path = backup_sqlite(sqlite_path)
        print(f"backup: {backup_path}")

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_tables = table_names(sqlite_conn)
    sqlite_columns = {table: table_columns(sqlite_conn, table) for table in sqlite_tables}

    import psycopg

    with psycopg.connect(args.postgres_url) as pg_conn:
        with pg_conn.transaction():
            if args.truncate:
                truncate_tables(pg_conn)
            counts: dict[str, tuple[int, int]] = {}
            for table in TABLE_ORDER:
                source_count = count_rows(sqlite_conn, table) if table in sqlite_tables else 0
                inserted = migrate_table(pg_conn, sqlite_conn, sqlite_columns, table)
                counts[table] = (source_count, inserted)
            reset_sequences(pg_conn)

    for table, (source_count, inserted) in counts.items():
        print(f"{table}: source={source_count} inserted={inserted}")


def backup_sqlite(path: Path) -> Path:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    backup_path = path.with_name(f"{path.name}.{stamp}.bak")
    shutil.copy2(path, backup_path)
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, Path(str(backup_path) + suffix))
    return backup_path


def table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row["name"] for row in rows}


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return {row["name"] for row in rows}


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f'SELECT COUNT(*) AS count FROM "{table}"').fetchone()["count"])


def truncate_tables(conn: psycopg.Connection[Any]) -> None:
    joined = ", ".join(TABLE_ORDER)
    conn.execute(f"TRUNCATE TABLE {joined} RESTART IDENTITY CASCADE")


def migrate_table(
    pg_conn: psycopg.Connection[Any],
    sqlite_conn: sqlite3.Connection,
    sqlite_columns: dict[str, set[str]],
    table: str,
) -> int:
    if table not in sqlite_columns:
        return 0

    target_columns = POSTGRES_COLUMNS[table]
    rows = sqlite_conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid ASC').fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(target_columns))
    columns_sql = ", ".join(target_columns)
    insert_sql = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    values = [
        tuple(convert_value(table, column, row[column] if column in sqlite_columns[table] else None) for column in target_columns)
        for row in rows
    ]
    with pg_conn.cursor() as cur:
        cur.executemany(insert_sql, values)
    return len(values)


def convert_value(table: str, column: str, value: Any) -> Any:
    if value is None:
        return None
    if column in TIME_COLUMNS.get(table, set()):
        return parse_time(value)
    if column in {"enabled", "success"}:
        return bool(value)
    return value


def parse_time(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            raise ValueError("empty timestamp")
        raw = normalize_fraction(raw.split(" m=+", 1)[0])
        parsed = parse_go_time(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_go_time(raw: str) -> dt.datetime:
    formats = [
        "%Y-%m-%d %H:%M:%S.%f %z %Z",
        "%Y-%m-%d %H:%M:%S %z %Z",
        "%Y-%m-%d %H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    normalized = raw.replace("Z", "+0000")
    for fmt in formats:
        try:
            return dt.datetime.strptime(normalized, fmt)
        except ValueError:
            pass
    try:
        return dt.datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"unsupported timestamp format: {raw}") from exc


def normalize_fraction(raw: str) -> str:
    return re.sub(
        r"\.(\d{6})\d+",
        lambda match: "." + match.group(1),
        raw,
        count=1,
    )


def reset_sequences(conn: psycopg.Connection[Any]) -> None:
    for table in IDENTITY_TABLES:
        conn.execute(
            """
            SELECT setval(
                pg_get_serial_sequence(%s, 'id'),
                GREATEST(COALESCE((SELECT MAX(id) FROM """ + table + """), 0), 1),
                COALESCE((SELECT MAX(id) FROM """ + table + """), 0) > 0
            )
            """,
            (table,),
        )


if __name__ == "__main__":
    main()
