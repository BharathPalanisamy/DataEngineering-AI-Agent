"""
Schema drift monitor for raw API payloads.
Compares schema paths between the first and latest available day per source.
Stores GREEN/YELLOW/RED status snapshots in control_plane_incidents.
"""

import json
import os
import uuid
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from drift_status import classify_drift


load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def collect_schema_paths(value, prefix="$", paths=None):
    """
    Collect json-path-like schema paths with type markers.
    Arrays are sampled by using the first element for deeper traversal.
    """
    if paths is None:
        paths = set()

    if isinstance(value, dict):
        paths.add(f"{prefix}:object")
        for key, child in value.items():
            collect_schema_paths(child, f"{prefix}.{key}", paths)
    elif isinstance(value, list):
        paths.add(f"{prefix}:array")
        if value:
            collect_schema_paths(value[0], f"{prefix}[]", paths)
    elif isinstance(value, str):
        paths.add(f"{prefix}:string")
    elif isinstance(value, bool):
        paths.add(f"{prefix}:boolean")
    elif isinstance(value, int):
        paths.add(f"{prefix}:integer")
    elif isinstance(value, float):
        paths.add(f"{prefix}:number")
    elif value is None:
        paths.add(f"{prefix}:null")
    else:
        paths.add(f"{prefix}:unknown")

    return paths


def get_sources_with_two_days(conn):
    query = """
        SELECT
            source,
            MIN(fetched_at::date) AS first_day,
            MAX(fetched_at::date) AS latest_day,
            COUNT(DISTINCT fetched_at::date) AS day_count
        FROM raw_api_events
        GROUP BY source
        HAVING COUNT(DISTINCT fetched_at::date) >= 2
        ORDER BY source;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def get_day_payload(conn, source, day):
    query = """
        SELECT payload, fetched_at
        FROM raw_api_events
        WHERE source = %s AND fetched_at::date = %s
        ORDER BY fetched_at ASC
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(query, (source, day))
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]


def status_to_severity(status_color):
    if status_color == "RED":
        return "HIGH"
    if status_color == "YELLOW":
        return "MED"
    return "LOW"


def status_recommendation(status_color):
    if status_color == "RED":
        return "Breaking schema change detected. Validate downstream parsing and mappings now."
    if status_color == "YELLOW":
        return "Non-breaking schema additions detected. Review if new fields should be consumed."
    return "No schema change detected. Continue normal monitoring."


def insert_schema_status_record(
    conn,
    source,
    first_day,
    latest_day,
    first_ts,
    latest_ts,
    added,
    removed,
    drift_result,
):
    incident_id = str(uuid.uuid4())
    status_color = drift_result["status"]
    severity = status_to_severity(status_color)
    recommendation = status_recommendation(status_color)
    affected_asset = f"raw_api_events:{source}"
    incident_status = "RESOLVED" if status_color == "GREEN" else "OPEN"

    evidence = {
        "source": source,
        "first_day": str(first_day),
        "latest_day": str(latest_day),
        "first_snapshot_ts": first_ts.isoformat(),
        "latest_snapshot_ts": latest_ts.isoformat(),
        "status_color": status_color,
        "summary": drift_result["summary"],
        "recommendation": recommendation,
        "added_paths": sorted(added),
        "removed_paths": sorted(removed),
        "added_count": len(added),
        "removed_count": len(removed),
        "type_changes": drift_result.get("type_changes", []),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }

    query = """
        INSERT INTO control_plane_incidents (
            incident_id,
            incident_type,
            severity,
            affected_asset,
            detected_at,
            evidence,
            status
        ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s);
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                incident_id,
                "SCHEMA_DRIFT",
                severity,
                affected_asset,
                datetime.now(timezone.utc),
                json.dumps(evidence),
                incident_status,
            ),
        )
    return recommendation


def main():
    print("=" * 60)
    print("Control Plane - Schema Drift Check")
    print(f"Started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    conn = get_db_connection()
    status_records_written = 0

    try:
        sources = get_sources_with_two_days(conn)
        if not sources:
            print("No sources have data across at least two days yet.")
            return

        for source, first_day, latest_day, _day_count in sources:
            first_payload, first_ts = get_day_payload(conn, source, first_day)
            latest_payload, latest_ts = get_day_payload(conn, source, latest_day)

            if first_payload is None or latest_payload is None:
                print(f"[{source}] Skipped: missing payload on one of the comparison days")
                continue

            first_paths = collect_schema_paths(first_payload)
            latest_paths = collect_schema_paths(latest_payload)

            added = latest_paths - first_paths
            removed = first_paths - latest_paths
            drift_result = classify_drift(added, removed)
            status = drift_result["status"]
            summary = drift_result["summary"]

            recommendation = insert_schema_status_record(
                conn,
                source,
                first_day,
                latest_day,
                first_ts,
                latest_ts,
                added,
                removed,
                drift_result,
            )
            status_records_written += 1

            if added or removed:
                print(
                    f"[{source}] {status} - DRIFT detected between {first_day} ({first_ts}) "
                    f"and {latest_day} ({latest_ts}) | +{len(added)} / -{len(removed)}"
                )
            else:
                print(
                    f"[{source}] {status} - No drift between {first_day} ({first_ts}) "
                    f"and {latest_day} ({latest_ts})"
                )
            print(f"  Recommendation: {recommendation}")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("=" * 60)
    print(f"Schema drift check complete. Status records written: {status_records_written}")
    print("=" * 60)


if __name__ == "__main__":
    main()
