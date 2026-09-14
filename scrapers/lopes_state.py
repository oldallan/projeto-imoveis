from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LEASE_HOURS = 12
MAX_DAILY_ATTEMPTS = 5
RETRY_BACKOFF_DAYS = (1, 2, 4, 7)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def state_db_path(output_root: str | Path) -> Path:
    return Path(output_root) / "state" / "lopes" / "lopes_state.sqlite3"


def generation_fingerprint(records: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    pairs = sorted(
        (
            str(record.get("listing_url") or "").strip(),
            str(record.get("lastmod") or "").strip(),
        )
        for record in records
        if str(record.get("listing_url") or "").strip()
    )
    for listing_url, lastmod in pairs:
        digest.update(listing_url.encode("utf-8"))
        digest.update(b"\0")
        digest.update(lastmod.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _connect(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    _create_schema(connection)
    return connection


@contextmanager
def _database(path: str | Path):
    connection = _connect(path)
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS generations (
            generation_id TEXT PRIMARY KEY,
            discovered_at TEXT NOT NULL,
            full_rows INTEGER NOT NULL,
            new_rows INTEGER NOT NULL,
            refresh_rows INTEGER NOT NULL,
            unchanged_rows INTEGER NOT NULL,
            removed_rows INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'in_progress'
        );
        CREATE TABLE IF NOT EXISTS generation_items (
            generation_id TEXT NOT NULL,
            listing_url TEXT NOT NULL,
            priority TEXT NOT NULL,
            PRIMARY KEY (generation_id, listing_url),
            FOREIGN KEY (generation_id) REFERENCES generations(generation_id)
        );
        CREATE TABLE IF NOT EXISTS inventory (
            listing_url TEXT PRIMARY KEY,
            listing_id TEXT,
            business_type TEXT,
            lastmod TEXT,
            generation_id TEXT NOT NULL,
            first_discovered_at TEXT NOT NULL,
            last_discovered_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS queue (
            listing_url TEXT PRIMARY KEY,
            listing_id TEXT,
            business_type TEXT,
            lastmod TEXT,
            generation_id TEXT NOT NULL,
            priority TEXT NOT NULL CHECK(priority IN ('new', 'refresh')),
            status TEXT NOT NULL CHECK(status IN ('pending', 'claimed', 'completed', 'terminal')),
            attempts INTEGER NOT NULL DEFAULT 0,
            available_at TEXT NOT NULL,
            claim_token TEXT,
            claimed_at TEXT,
            last_error TEXT,
            discovered_at TEXT NOT NULL,
            completed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_queue_selection
            ON queue(status, priority, available_at, discovered_at, listing_url);
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )


def _normalize_records(records: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for raw in records:
        listing_url = str(raw.get("listing_url") or "").strip()
        if not listing_url:
            continue
        candidate = {
            "listing_url": listing_url,
            "listing_id": str(raw.get("listing_id") or raw.get("property_id") or "").strip() or None,
            "business_type": str(raw.get("business_type") or "").strip() or None,
            "lastmod": str(raw.get("lastmod") or "").strip() or None,
        }
        previous = normalized.get(listing_url)
        if previous is None or str(candidate["lastmod"] or "") > str(previous["lastmod"] or ""):
            normalized[listing_url] = candidate
    return normalized


def _inventory(connection: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    return {
        str(row["listing_url"]): row
        for row in connection.execute("SELECT * FROM inventory")
    }


def _set_metadata(connection: sqlite3.Connection, key: str, value: str | None) -> None:
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_metadata(db_path: str | Path, key: str) -> str | None:
    with _database(db_path) as connection:
        row = connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return None if row is None else row["value"]


def set_metadata(db_path: str | Path, key: str, value: str | None) -> None:
    with _database(db_path) as connection:
        _set_metadata(connection, key, value)


def _apply_generation(
    connection: sqlite3.Connection,
    records: Sequence[Mapping[str, Any]],
    *,
    discovered_at: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current = _normalize_records(records)
    if not current:
        raise ValueError("o sitemap Lopes precisa conter ao menos uma URL valida")
    generation_id = generation_fingerprint(list(current.values()))
    existing_generation = connection.execute(
        "SELECT * FROM generations WHERE generation_id=?", (generation_id,)
    ).fetchone()
    if existing_generation is not None:
        delta: list[dict[str, Any]] = []
        metrics = dict(existing_generation)
        metrics["delta_rows"] = 0
        metrics["new_rows"] = 0
        metrics["refresh_rows"] = 0
        metrics["updated_rows"] = 0
        metrics["unchanged_rows"] = len(current)
        metrics["removed_rows"] = 0
        metrics.update(queue_metrics_connection(connection))
        connection.execute(
            "UPDATE generations SET status=? WHERE generation_id=?",
            (metrics["generation_status"], generation_id),
        )
        metrics["idempotent_generation"] = True
        return delta, metrics

    previous = _inventory(connection)
    previous_urls = set(previous)
    current_urls = set(current)
    removed_urls = previous_urls - current_urls
    delta: list[dict[str, Any]] = []
    new_rows = refresh_rows = unchanged_rows = 0
    connection.execute(
        """
        INSERT INTO generations(
            generation_id, discovered_at, full_rows, new_rows, refresh_rows,
            unchanged_rows, removed_rows, status
        ) VALUES(?, ?, ?, 0, 0, 0, 0, 'in_progress')
        """,
        (generation_id, discovered_at, len(current)),
    )

    for listing_url, record in current.items():
        old = previous.get(listing_url)
        priority: str | None = None
        if old is None:
            priority = "new"
            new_rows += 1
        elif (old["lastmod"] or None) != record["lastmod"]:
            priority = "refresh"
            refresh_rows += 1
        else:
            unchanged_rows += 1

        first_discovered_at = old["first_discovered_at"] if old is not None else discovered_at
        connection.execute(
            """
            INSERT INTO inventory(
                listing_url, listing_id, business_type, lastmod, generation_id,
                first_discovered_at, last_discovered_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_url) DO UPDATE SET
                listing_id=excluded.listing_id,
                business_type=excluded.business_type,
                lastmod=excluded.lastmod,
                generation_id=excluded.generation_id,
                last_discovered_at=excluded.last_discovered_at
            """,
            (
                listing_url, record["listing_id"], record["business_type"], record["lastmod"],
                generation_id, first_discovered_at, discovered_at,
            ),
        )
        if priority:
            connection.execute(
                "INSERT INTO generation_items(generation_id, listing_url, priority) VALUES(?, ?, ?)",
                (generation_id, listing_url, priority),
            )
            connection.execute(
                """
                INSERT INTO queue(
                    listing_url, listing_id, business_type, lastmod, generation_id,
                    priority, status, attempts, available_at, discovered_at
                ) VALUES(?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
                ON CONFLICT(listing_url) DO UPDATE SET
                    listing_id=excluded.listing_id,
                    business_type=excluded.business_type,
                    lastmod=excluded.lastmod,
                    generation_id=excluded.generation_id,
                    priority=excluded.priority,
                    status='pending', attempts=0, available_at=excluded.available_at,
                    claim_token=NULL, claimed_at=NULL, last_error=NULL,
                    discovered_at=excluded.discovered_at, completed_at=NULL
                """,
                (
                    listing_url, record["listing_id"], record["business_type"], record["lastmod"],
                    generation_id, priority, discovered_at, discovered_at,
                ),
            )
            delta.append(
                {
                    **record,
                    "collection_priority": priority,
                    "sitemap_generation_id": generation_id,
                    "discovered_at": discovered_at,
                }
            )

    if removed_urls:
        connection.executemany("DELETE FROM inventory WHERE listing_url=?", ((url,) for url in removed_urls))
        connection.executemany("DELETE FROM queue WHERE listing_url=?", ((url,) for url in removed_urls))

    connection.execute(
        """
        UPDATE generations
        SET new_rows=?, refresh_rows=?, unchanged_rows=?, removed_rows=?
        WHERE generation_id=?
        """,
        (new_rows, refresh_rows, unchanged_rows, len(removed_urls), generation_id),
    )
    _set_metadata(connection, "active_generation_id", generation_id)
    metrics = {
        "generation_id": generation_id,
        "discovered_at": discovered_at,
        "full_rows": len(current),
        "delta_rows": len(delta),
        "new_rows": new_rows,
        "refresh_rows": refresh_rows,
        "updated_rows": refresh_rows,
        "unchanged_rows": unchanged_rows,
        "removed_rows": len(removed_urls),
        "idempotent_generation": False,
    }
    metrics.update(queue_metrics_connection(connection))
    connection.execute(
        "UPDATE generations SET status=? WHERE generation_id=?",
        (metrics["generation_status"], generation_id),
    )
    return delta, metrics


def reconcile_inventory(
    db_path: str | Path,
    records: Sequence[Mapping[str, Any]],
    *,
    discovered_at: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    timestamp = discovered_at or utc_now_iso()
    with _database(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        delta, metrics = _apply_generation(connection, records, discovered_at=timestamp)
        connection.commit()
        return delta, metrics


def bootstrap_inventory(
    db_path: str | Path,
    *,
    previous_records: Sequence[Mapping[str, Any]],
    current_records: Sequence[Mapping[str, Any]],
    discovered_at: str,
    completed_urls: set[str] | None = None,
) -> dict[str, Any]:
    with _database(db_path) as connection:
        if connection.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]:
            return {"bootstrapped": False, **queue_metrics_connection(connection)}
        connection.execute("BEGIN IMMEDIATE")
        previous = _normalize_records(previous_records)
        for record in previous.values():
            connection.execute(
                "INSERT INTO inventory VALUES(?, ?, ?, ?, 'legacy-baseline', ?, ?)",
                (
                    record["listing_url"], record["listing_id"], record["business_type"], record["lastmod"],
                    discovered_at, discovered_at,
                ),
            )
        _, metrics = _apply_generation(connection, current_records, discovered_at=discovered_at)
        completed = completed_urls or set()
        if completed:
            connection.executemany(
                "UPDATE queue SET status='completed', completed_at=? WHERE listing_url=?",
                ((discovered_at, listing_url) for listing_url in completed),
            )
        _set_metadata(connection, "legacy_bootstrap_completed", "1")
        connection.commit()
        metrics.update(queue_metrics_connection(connection))
        if metrics.get("generation_id"):
            connection.execute(
                "UPDATE generations SET status=? WHERE generation_id=?",
                (metrics["generation_status"], metrics["generation_id"]),
            )
        metrics["bootstrapped"] = True
        metrics["bootstrap_completed_rows"] = len(completed)
        return metrics


def queue_metrics_connection(connection: sqlite3.Connection) -> dict[str, Any]:
    counts = {
        (row["priority"], row["status"]): int(row["rows"])
        for row in connection.execute(
            "SELECT priority, status, COUNT(*) AS rows FROM queue GROUP BY priority, status"
        )
    }
    pending_new = sum(counts.get(("new", status), 0) for status in ("pending", "claimed"))
    pending_refresh = sum(counts.get(("refresh", status), 0) for status in ("pending", "claimed"))
    retry_pending = int(
        connection.execute(
            "SELECT COUNT(*) FROM queue WHERE status='pending' AND attempts > 0"
        ).fetchone()[0]
    )
    failed_terminal = int(
        connection.execute(
            "SELECT COUNT(*) FROM queue WHERE status='terminal' AND COALESCE(last_error, '') <> 'not_found'"
        ).fetchone()[0]
    )
    active = connection.execute("SELECT value FROM metadata WHERE key='active_generation_id'").fetchone()
    return {
        "generation_id": active["value"] if active else None,
        "pending_new": pending_new,
        "pending_refresh": pending_refresh,
        "retry_pending": retry_pending,
        "failed_terminal": failed_terminal,
        "backlog_remaining": pending_new + pending_refresh,
        "generation_status": "completed" if pending_new + pending_refresh == 0 else "in_progress",
    }


def queue_metrics(db_path: str | Path) -> dict[str, Any]:
    if not Path(db_path).exists():
        return {
            "generation_id": None, "pending_new": 0, "pending_refresh": 0,
            "retry_pending": 0, "failed_terminal": 0, "backlog_remaining": 0,
            "generation_status": "completed",
        }
    with _database(db_path) as connection:
        return queue_metrics_connection(connection)


def claim_daily_batch(
    db_path: str | Path,
    *,
    claim_token: str,
    daily_limit: int = 10_000,
    new_quota: int = 8_000,
    refresh_quota: int = 2_000,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    current = now or utc_now()
    current_iso = current.isoformat()
    stale_before = (current - timedelta(hours=LEASE_HOURS)).isoformat()
    with _database(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE queue SET status='pending', claim_token=NULL, claimed_at=NULL "
            "WHERE status='claimed' AND claimed_at < ?",
            (stale_before,),
        )
        existing = list(
            connection.execute(
                "SELECT * FROM queue WHERE status='claimed' AND claim_token=? ORDER BY priority, listing_url",
                (claim_token,),
            )
        )
        if existing:
            connection.commit()
            return [_queue_row_to_record(row) for row in existing]

        def select(priority: str, limit: int) -> list[sqlite3.Row]:
            if limit <= 0:
                return []
            rows = list(
                connection.execute(
                    "SELECT * FROM queue WHERE status='pending' AND priority=? AND available_at <= ? "
                    "ORDER BY available_at, discovered_at, listing_url LIMIT ?",
                    (priority, current_iso, limit),
                )
            )
            return rows

        available_new = select("new", daily_limit)
        available_refresh = select("refresh", daily_limit)
        selected = available_new[: min(new_quota, daily_limit)]
        selected += available_refresh[: min(refresh_quota, daily_limit - len(selected))]
        remaining = daily_limit - len(selected)
        if remaining > 0:
            selected += available_new[min(new_quota, daily_limit):][:remaining]
            remaining = daily_limit - len(selected)
        if remaining > 0:
            selected += available_refresh[min(refresh_quota, daily_limit):][:remaining]

        connection.executemany(
            "UPDATE queue SET status='claimed', claim_token=?, claimed_at=? WHERE listing_url=? AND status='pending'",
            ((claim_token, current_iso, row["listing_url"]) for row in selected),
        )
        connection.commit()
        return [_queue_row_to_record(row) for row in selected]


def _queue_row_to_record(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "listing_url": row["listing_url"],
        "listing_id": row["listing_id"],
        "business_type": row["business_type"],
        "lastmod": row["lastmod"],
        "sitemap_lastmod": row["lastmod"],
        "sitemap_generation_id": row["generation_id"],
        "collection_priority": row["priority"],
        "discovered_at": row["discovered_at"],
    }


def finalize_claim(
    db_path: str | Path,
    *,
    claim_token: str,
    successful_urls: set[str],
    terminal_urls: set[str],
    attempted_urls: set[str] | None = None,
    error: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or utc_now()
    current_iso = current.isoformat()
    with _database(db_path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        claimed = list(connection.execute("SELECT * FROM queue WHERE claim_token=? AND status='claimed'", (claim_token,)))
        terminal_today = failed_today = completed_today = 0
        for row in claimed:
            listing_url = str(row["listing_url"])
            if listing_url in successful_urls:
                connection.execute(
                    "UPDATE queue SET status='completed', completed_at=?, claim_token=NULL, claimed_at=NULL WHERE listing_url=?",
                    (current_iso, listing_url),
                )
                completed_today += 1
                continue
            if listing_url in terminal_urls:
                connection.execute(
                    "UPDATE queue SET status='terminal', completed_at=?, claim_token=NULL, claimed_at=NULL, last_error='not_found' WHERE listing_url=?",
                    (current_iso, listing_url),
                )
                terminal_today += 1
                continue
            if attempted_urls is not None and listing_url not in attempted_urls:
                connection.execute(
                    "UPDATE queue SET status='pending', claim_token=NULL, claimed_at=NULL WHERE listing_url=?",
                    (listing_url,),
                )
                continue
            attempts = int(row["attempts"]) + 1
            failed_today += 1
            if attempts >= MAX_DAILY_ATTEMPTS:
                connection.execute(
                    "UPDATE queue SET status='terminal', attempts=?, completed_at=?, claim_token=NULL, claimed_at=NULL, last_error=? WHERE listing_url=?",
                    (attempts, current_iso, error or "transient_failure", listing_url),
                )
                terminal_today += 1
            else:
                delay_days = RETRY_BACKOFF_DAYS[min(attempts - 1, len(RETRY_BACKOFF_DAYS) - 1)]
                available_at = (current + timedelta(days=delay_days)).isoformat()
                connection.execute(
                    "UPDATE queue SET status='pending', attempts=?, available_at=?, claim_token=NULL, claimed_at=NULL, last_error=? WHERE listing_url=?",
                    (attempts, available_at, error or "transient_failure", listing_url),
                )

        metrics = queue_metrics_connection(connection)
        if metrics["generation_id"]:
            connection.execute(
                "UPDATE generations SET status=? WHERE generation_id=?",
                (metrics["generation_status"], metrics["generation_id"]),
            )
        connection.commit()
        return {
            **metrics,
            "completed_today": completed_today,
            "terminal_today": terminal_today,
            "failed_today": failed_today,
        }


def mark_urls_completed(db_path: str | Path, urls: Iterable[str], *, completed_at: str | None = None) -> int:
    timestamp = completed_at or utc_now_iso()
    normalized = {str(url).strip() for url in urls if str(url).strip()}
    with _database(db_path) as connection:
        connection.executemany(
            "UPDATE queue SET status='completed', completed_at=?, claim_token=NULL, claimed_at=NULL WHERE listing_url=?",
            ((timestamp, url) for url in normalized),
        )
        return len(normalized)
