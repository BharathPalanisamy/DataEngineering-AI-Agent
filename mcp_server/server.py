from __future__ import annotations

import csv
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from psycopg2.extras import RealDictCursor


load_dotenv()
mcp = FastMCP("data-control-plane")

ROOT_DIR = Path(__file__).resolve().parents[1]
DRIFT_REPORT_CSV = ROOT_DIR / "reports" / "latest" / "schema_drift_attribute_analysis.csv"


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    return list(dict.fromkeys(items))


def load_latest_schema_drift(source: str | None = None) -> dict[str, Any]:
    requested_source = source.strip() if source else None

    if not DRIFT_REPORT_CSV.exists():
        return {
            "found": False,
            "source": requested_source,
            "report_path": str(DRIFT_REPORT_CSV),
            "message": "Schema drift report is not available yet. Run the monitor cycle to generate it.",
            "results": [],
        }

    with DRIFT_REPORT_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    available_sources = sorted({row["source"] for row in rows if row.get("source")})
    relevant_sources = [requested_source] if requested_source else available_sources
    results: list[dict[str, Any]] = []

    for source_name in relevant_sources:
        source_rows = [row for row in rows if row.get("source") == source_name]
        if not source_rows:
            continue

        latest_date = max(row["date"] for row in source_rows)
        latest_rows = [row for row in source_rows if row["date"] == latest_date]
        prev_day = latest_rows[0].get("prev_day", "Unknown")

        added = _dedupe_preserve_order(
            [row["attribute"] for row in latest_rows if row.get("change_type") == "ADDED"]
        )
        removed = _dedupe_preserve_order(
            [row["attribute"] for row in latest_rows if row.get("change_type") == "REMOVED"]
        )
        type_changed = _dedupe_preserve_order(
            [
                f"{row['attribute']} ({row['prev_type']} → {row['curr_type']})"
                for row in latest_rows
                if row.get("change_type") == "TYPE_CHANGE"
            ]
        )
        format_changed = _dedupe_preserve_order(
            [
                f"{row['attribute']} ({row['prev_type']} → {row['curr_type']})"
                for row in latest_rows
                if row.get("change_type") == "FORMAT_CHANGE"
            ]
        )

        results.append(
            {
                "source": source_name,
                "previous_day": prev_day,
                "current_day": latest_date,
                "added": added,
                "removed": removed,
                "type_changed": type_changed,
                "format_changed": format_changed,
                "changed": any([added, removed, type_changed, format_changed]),
                "change_counts": {
                    "ADDED": len(added),
                    "REMOVED": len(removed),
                    "TYPE_CHANGE": len(type_changed),
                    "FORMAT_CHANGE": len(format_changed),
                },
            }
        )

    return {
        "found": bool(results),
        "source": requested_source,
        "report_path": str(DRIFT_REPORT_CSV),
        "available_sources": available_sources,
        "results": results,
    }


def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set. Add it to .env before starting the MCP server.")
    return psycopg2.connect(database_url)


def make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: make_json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [make_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_all(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [make_json_safe(dict(row)) for row in cur.fetchall()]


def fetch_one(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return make_json_safe(dict(row)) if row else None


@mcp.tool()
def get_pipeline_overview() -> dict[str, Any]:
    """Return the latest pipeline health snapshot across all sources."""
    rows = fetch_all(
        """
        WITH latest AS (
            SELECT DISTINCT ON (source)
                source,
                status_color,
                summary,
                checked_at,
                details
            FROM control_plane_check_status
            WHERE check_name = 'PIPELINE_MONITOR'
            ORDER BY source, checked_at DESC
        )
        SELECT source, status_color, summary, checked_at, details
        FROM latest
        ORDER BY source
        """
    )

    counts = {"GREEN": 0, "YELLOW": 0, "RED": 0}
    for row in rows:
        counts[row["status_color"]] = counts.get(row["status_color"], 0) + 1

    overall_status = "HEALTHY"
    if counts.get("RED", 0) > 0:
        overall_status = "CRITICAL"
    elif counts.get("YELLOW", 0) > 0:
        overall_status = "WARNING"

    return {
        "overall_status": overall_status,
        "source_count": len(rows),
        "status_breakdown": counts,
        "sources": rows,
    }


@mcp.tool()
def get_source_status(source: str) -> dict[str, Any]:
    """Get the latest monitor result, ingestion run, and payload freshness for a single source."""
    source = source.strip()

    latest_status = fetch_one(
        """
        SELECT source, status_color, summary, details, checked_at
        FROM control_plane_check_status
        WHERE check_name = 'PIPELINE_MONITOR' AND source = %s
        ORDER BY checked_at DESC
        LIMIT 1
        """,
        (source,),
    )

    latest_run = fetch_one(
        """
        SELECT source, started_at, finished_at, status, rows_written, error_message
        FROM ingestion_runs
        WHERE source = %s
        ORDER BY COALESCE(finished_at, started_at) DESC, started_at DESC
        LIMIT 1
        """,
        (source,),
    )

    latest_payload = fetch_one(
        """
        SELECT MAX(fetched_at) AS latest_payload_at, COUNT(*) AS total_payloads
        FROM raw_api_events
        WHERE source = %s
        """,
        (source,),
    )

    found = any(
        [
            latest_status is not None,
            latest_run is not None,
            bool(latest_payload and latest_payload.get("total_payloads")),
        ]
    )

    return {
        "source": source,
        "found": found,
        "latest_monitor_status": latest_status,
        "latest_ingestion_run": latest_run,
        "latest_payload": latest_payload,
    }


@mcp.tool()
def list_open_incidents(limit: int = 10) -> list[dict[str, Any]]:
    """List unresolved control-plane incidents in newest-first order."""
    limit = max(1, min(limit, 50))
    return fetch_all(
        """
        SELECT incident_id, incident_type, severity, affected_asset, detected_at, status, evidence
        FROM control_plane_incidents
        WHERE status = 'OPEN'
        ORDER BY detected_at DESC
        LIMIT %s
        """,
        (limit,),
    )


@mcp.tool()
def get_recent_monitor_history(limit: int = 10) -> list[dict[str, Any]]:
    """Return recent persisted pipeline-monitor snapshots for demo and trend questions."""
    limit = max(1, min(limit, 50))
    return fetch_all(
        """
        SELECT source, status_color, summary, checked_at
        FROM control_plane_check_status
        WHERE check_name = 'PIPELINE_MONITOR'
        ORDER BY checked_at DESC
        LIMIT %s
        """,
        (limit,),
    )


@mcp.tool()
def get_latest_schema_drift(source: str | None = None) -> dict[str, Any]:
    """Return the latest schema drift details, including additions and removals versus the previous day."""
    return load_latest_schema_drift(source)


if __name__ == "__main__":
    mcp.run(transport="stdio")
