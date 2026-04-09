from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

HEALTH_RANK = {
    "HEALTHY": 0,
    "UNKNOWN": 1,
    "WARNING": 2,
    "CRITICAL": 3,
}


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def isoformat_or_none(value: datetime | None) -> str | None:
    value = ensure_utc(value)
    return value.strftime("%Y-%m-%d %H:%M:%S UTC") if value else None


def fetch_all_sources(conn) -> list[str]:
    query = """
        SELECT source FROM raw_api_events
        UNION
        SELECT source FROM ingestion_runs
        ORDER BY source
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def fetch_latest_runs(conn) -> dict[str, dict[str, Any]]:
    query = """
        SELECT DISTINCT ON (source)
            source,
            started_at,
            finished_at,
            status,
            rows_written,
            error_message
        FROM ingestion_runs
        ORDER BY source, COALESCE(finished_at, started_at) DESC, started_at DESC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return {row["source"]: dict(row) for row in cur.fetchall()}


def fetch_latest_events(conn) -> dict[str, dict[str, Any]]:
    query = """
        SELECT
            source,
            MAX(fetched_at) AS latest_fetched_at,
            COUNT(*) AS total_events
        FROM raw_api_events
        GROUP BY source
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return {row["source"]: dict(row) for row in cur.fetchall()}


def fetch_latest_payload_samples(conn) -> dict[str, list[dict[str, Any]]]:
    query = """
        SELECT source, payload, fetched_at
        FROM (
            SELECT
                source,
                payload,
                fetched_at,
                ROW_NUMBER() OVER (PARTITION BY source ORDER BY fetched_at DESC) AS rn
            FROM raw_api_events
        ) ranked
        WHERE rn <= 2
        ORDER BY source, fetched_at DESC
    """
    samples: dict[str, list[dict[str, Any]]] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        for row in cur.fetchall():
            samples.setdefault(row["source"], []).append(dict(row))
    return samples


def normalize_attribute_name(name: str) -> str:
    return re.sub(r"[_\-.\[\]{}]", "", name).lower()


def extract_paths(obj, prefix: str = "$") -> dict[str, tuple[str, str]]:
    attributes: dict[str, tuple[str, str]] = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            path = f"{prefix}.{key}"
            if isinstance(val, (dict, list)):
                attributes.update(extract_paths(val, path))
            else:
                if val is None:
                    value_type = "null"
                elif isinstance(val, bool):
                    value_type = "boolean"
                elif isinstance(val, int):
                    value_type = "integer"
                elif isinstance(val, float):
                    value_type = "number"
                else:
                    value_type = "string"
                attributes[f"{path}:{value_type}"] = (path, value_type)
    elif isinstance(obj, list):
        path = f"{prefix}[]"
        attributes[f"{path}:array"] = (path, "array")
        if obj:
            attributes.update(extract_paths(obj[0], path))
    return attributes


def classify_change_for_key(key: str, prev_attrs: dict[str, tuple[str, str]], curr_attrs: dict[str, tuple[str, str]]) -> str:
    in_prev = key in prev_attrs
    in_curr = key in curr_attrs

    if in_prev and in_curr:
        return "SAME"

    if not in_prev and in_curr:
        return "ADDED"

    if in_prev and not in_curr:
        prev_path, prev_type = prev_attrs[key]
        prev_norm = normalize_attribute_name(prev_path)
        for _curr_key, (curr_path, curr_type) in curr_attrs.items():
            if normalize_attribute_name(curr_path) == prev_norm:
                if curr_type != prev_type:
                    if prev_type == "null" or curr_type == "null":
                        return "SAME"
                    return "TYPE_CHANGE"
                return "FORMAT_CHANGE"
        return "REMOVED"

    return "SAME"


def compute_latest_drift_summary(previous_payload, current_payload) -> tuple[dict[str, int], str]:
    prev_attrs = extract_paths(previous_payload)
    curr_attrs = extract_paths(current_payload)
    counts = {"ADDED": 0, "REMOVED": 0, "TYPE_CHANGE": 0}

    for key in set(prev_attrs) | set(curr_attrs):
        change_type = classify_change_for_key(key, prev_attrs, curr_attrs)
        if change_type in counts:
            counts[change_type] += 1

    total_changes = sum(counts.values())
    if total_changes == 0:
        return counts, "No schema drift detected vs previous pull."

    parts = []
    if counts["ADDED"]:
        parts.append(f"{counts['ADDED']} added")
    if counts["REMOVED"]:
        parts.append(f"{counts['REMOVED']} removed")
    if counts["TYPE_CHANGE"]:
        parts.append(f"{counts['TYPE_CHANGE']} type changed")

    return counts, "Latest drift vs previous pull: " + ", ".join(parts) + "."


def fetch_open_incidents(conn) -> dict[str, dict[str, Any]]:
    query = """
        SELECT
            split_part(affected_asset, ':', 2) AS source,
            COUNT(*) FILTER (WHERE status = 'OPEN') AS open_incidents,
            COALESCE(
                MAX(
                    CASE severity
                        WHEN 'HIGH' THEN 3
                        WHEN 'MED' THEN 2
                        WHEN 'LOW' THEN 1
                        ELSE 0
                    END
                ) FILTER (WHERE status = 'OPEN'),
                0
            ) AS max_open_severity_rank
        FROM control_plane_incidents
        WHERE affected_asset LIKE 'raw_api_events:%'
        GROUP BY split_part(affected_asset, ':', 2)
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return {row["source"]: dict(row) for row in cur.fetchall()}


def fetch_latest_incident_details(conn) -> dict[str, dict[str, Any]]:
    query = """
        SELECT DISTINCT ON (split_part(affected_asset, ':', 2))
            split_part(affected_asset, ':', 2) AS source,
            incident_type,
            severity,
            status,
            detected_at,
            evidence
        FROM control_plane_incidents
        WHERE affected_asset LIKE 'raw_api_events:%'
        ORDER BY split_part(affected_asset, ':', 2), detected_at DESC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        return {row["source"]: dict(row) for row in cur.fetchall()}


def health_from_severity_rank(rank: int) -> str:
    if rank >= 3:
        return "CRITICAL"
    if rank >= 2:
        return "WARNING"
    return "HEALTHY"


def choose_recommendation(health: str, run_status: str | None, open_incidents: int) -> str:
    if run_status == "FAIL":
        return "Retry ingestion for this source and inspect the latest error message."
    if health == "CRITICAL":
        return "Investigate immediately before the next scheduled pull."
    if health == "WARNING":
        return "Monitor closely and confirm the source refreshes on the expected cadence."
    if open_incidents > 0:
        return "Latest pull looks healthy; older open incidents can be reviewed separately if needed."
    return "Continue normal monitoring."


def status_color_from_health(health: str) -> str:
    if health == "HEALTHY":
        return "GREEN"
    if health == "WARNING":
        return "YELLOW"
    return "RED"


def ensure_check_status_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS control_plane_check_status (
                status_id TEXT PRIMARY KEY,
                check_name TEXT NOT NULL,
                source TEXT NOT NULL,
                status_color TEXT NOT NULL,
                summary TEXT NOT NULL,
                details JSONB NOT NULL,
                checked_at TIMESTAMP NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_check_status_lookup
            ON control_plane_check_status(check_name, source, checked_at DESC)
            """
        )


def persist_monitoring_snapshot(snapshot: dict[str, Any]) -> None:
    conn = get_db_connection()
    try:
        ensure_check_status_table(conn)
        checked_at = datetime.fromisoformat(snapshot["checked_at"])

        with conn.cursor() as cur:
            for report in snapshot["sources"]:
                summary = (
                    f"Latest run {report['last_run_status']}. "
                    f"{report['latest_drift_summary']}"
                )
                details = {
                    "overall_status": snapshot["overall_status"],
                    **report,
                }
                cur.execute(
                    """
                    INSERT INTO control_plane_check_status (
                        status_id,
                        check_name,
                        source,
                        status_color,
                        summary,
                        details,
                        checked_at
                    ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        "PIPELINE_MONITOR",
                        report["source"],
                        status_color_from_health(report["health"]),
                        summary,
                        json.dumps(details),
                        checked_at,
                    ),
                )
        conn.commit()
    finally:
        conn.close()


def build_source_report(
    source: str,
    latest_runs: dict[str, dict[str, Any]],
    latest_events: dict[str, dict[str, Any]],
    latest_payload_samples: dict[str, list[dict[str, Any]]],
    open_incidents: dict[str, dict[str, Any]],
    latest_incident_details: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    run = latest_runs.get(source)
    event = latest_events.get(source, {})
    payload_samples = latest_payload_samples.get(source, [])
    incident_rollup = open_incidents.get(source, {})
    incident_detail = latest_incident_details.get(source)

    last_run_status = run["status"] if run else None
    last_started_at = ensure_utc(run["started_at"]) if run else None
    last_finished_at = ensure_utc(run["finished_at"]) if run else None
    latest_fetched_at = ensure_utc(event.get("latest_fetched_at"))
    open_count = int(incident_rollup.get("open_incidents", 0) or 0)

    health = "HEALTHY"
    notes: list[str] = []

    if not run:
        health = "UNKNOWN"
        notes.append("No ingestion run has been recorded yet.")
    elif last_run_status == "FAIL":
        health = "CRITICAL"
        error_message = (run.get("error_message") or "Unknown error").strip()
        notes.append(f"Latest ingestion run failed: {error_message}")
    else:
        rows_written = run.get("rows_written")
        rows_text = f"{rows_written} row(s) written" if rows_written is not None else "run completed"
        notes.append(f"Latest ingestion run succeeded ({rows_text}).")

    if latest_fetched_at:
        age_hours = (now - latest_fetched_at).total_seconds() / 3600
        notes.append(f"Latest payload snapshot is {age_hours:.1f} hour(s) old.")
        if age_hours > 24 and HEALTH_RANK[health] < HEALTH_RANK["WARNING"]:
            health = "WARNING"
            notes.append("Freshness is stale relative to a daily monitoring cadence.")
    else:
        notes.append("No raw payload snapshot found yet.")
        if HEALTH_RANK[health] < HEALTH_RANK["WARNING"]:
            health = "WARNING"

    latest_incident_summary = None
    if incident_detail:
        evidence = incident_detail.get("evidence") or {}
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except json.JSONDecodeError:
                evidence = {"summary": evidence}
        latest_incident_summary = evidence.get("summary") or evidence.get("recommendation")

    drift_counts = {"ADDED": 0, "REMOVED": 0, "TYPE_CHANGE": 0}
    if last_run_status == "FAIL":
        latest_drift_summary = "Unavailable because the latest pull failed."
    elif len(payload_samples) < 2:
        latest_drift_summary = "Not enough history yet to compare against a previous pull."
    else:
        drift_counts, latest_drift_summary = compute_latest_drift_summary(
            payload_samples[1]["payload"],
            payload_samples[0]["payload"],
        )

    recommendation = choose_recommendation(health, last_run_status, open_count)

    return {
        "source": source,
        "health": health,
        "last_run_status": last_run_status or "NO_RUN",
        "last_started_at": isoformat_or_none(last_started_at),
        "last_finished_at": isoformat_or_none(last_finished_at),
        "latest_fetched_at": isoformat_or_none(latest_fetched_at),
        "latest_drift_summary": latest_drift_summary,
        "drift_counts": drift_counts,
        "open_incidents": open_count,
        "latest_incident_summary": latest_incident_summary,
        "recommendation": recommendation,
        "notes": notes,
    }


def build_monitoring_snapshot() -> dict[str, Any]:
    conn = get_db_connection()
    try:
        sources = fetch_all_sources(conn)
        latest_runs = fetch_latest_runs(conn)
        latest_events = fetch_latest_events(conn)
        latest_payload_samples = fetch_latest_payload_samples(conn)
        open_incidents = fetch_open_incidents(conn)
        latest_incident_details = fetch_latest_incident_details(conn)
    finally:
        conn.close()

    source_reports = [
        build_source_report(
            source,
            latest_runs,
            latest_events,
            latest_payload_samples,
            open_incidents,
            latest_incident_details,
        )
        for source in sources
    ]

    overall_status = "HEALTHY"
    for report in source_reports:
        if HEALTH_RANK[report["health"]] > HEALTH_RANK[overall_status]:
            overall_status = report["health"]

    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "source_count": len(source_reports),
        "sources": source_reports,
    }


def print_human_summary(snapshot: dict[str, Any]) -> None:
    print("=" * 72)
    print("Data Engineering Control Plane Agent — Monitoring Summary")
    print(f"Checked at: {snapshot['checked_at']}")
    print(f"Overall status: {snapshot['overall_status']}")
    print("=" * 72)

    for report in snapshot["sources"]:
        print(f"\nSource: {report['source']}")
        print(f"Health: {report['health']}")
        print(f"Last run status: {report['last_run_status']}")
        if report["last_finished_at"]:
            print(f"Last finished at: {report['last_finished_at']}")
        if report["latest_fetched_at"]:
            print(f"Latest payload at: {report['latest_fetched_at']}")
        print(f"Latest drift: {report['latest_drift_summary']}")


def exit_code_for_status(status: str) -> int:
    if status == "CRITICAL":
        return 2
    if status in {"WARNING", "UNKNOWN"}:
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the first control-plane monitoring agent.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON instead of text.")
    args = parser.parse_args()

    snapshot = build_monitoring_snapshot()
    persist_monitoring_snapshot(snapshot)

    if args.json:
        print(json.dumps(snapshot, indent=2))
    else:
        print_human_summary(snapshot)

    return exit_code_for_status(snapshot["overall_status"])


if __name__ == "__main__":
    sys.exit(main())
