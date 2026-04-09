from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from mcp_server.server import (
    get_latest_schema_drift,
    get_pipeline_overview,
    get_recent_monitor_history,
    get_source_status,
    list_open_incidents,
)

CHAT_HTML = ROOT_DIR / "orchestrator" / "ui" / "chat_window.html"
KNOWN_SOURCES = [
    "amazon_products",
    "openfoodfacts_snacks",
    "product_search_details",
    "walmart_reviews",
]


def detect_source(question: str) -> str | None:
    lowered = question.lower()
    for source in KNOWN_SOURCES:
        if source.lower() in lowered:
            return source
    aliases = {
        "amazon": "amazon_products",
        "openfoodfacts": "openfoodfacts_snacks",
        "snacks": "openfoodfacts_snacks",
        "product search": "product_search_details",
        "walmart": "walmart_reviews",
    }
    for key, source in aliases.items():
        if key in lowered:
            return source
    return None


def format_pipeline_overview() -> str:
    overview = get_pipeline_overview()
    lines = [
        f"Overall pipeline status: {overview['overall_status']}",
        f"Sources monitored: {overview['source_count']}",
        (
            "Status breakdown: "
            f"GREEN={overview['status_breakdown'].get('GREEN', 0)}, "
            f"YELLOW={overview['status_breakdown'].get('YELLOW', 0)}, "
            f"RED={overview['status_breakdown'].get('RED', 0)}"
        ),
        "",
        "Latest source summary:",
    ]

    for row in overview["sources"]:
        lines.append(f"- {row['source']}: {row['summary']}")

    return "\n".join(lines)


def format_critical_reason() -> str:
    overview = get_pipeline_overview()
    critical_sources = [
        row for row in overview["sources"] if row.get("status_color") == "RED"
    ]
    if not critical_sources:
        return "The pipeline is not currently critical."

    lines = ["The pipeline is critical because these sources are currently RED:"]
    for row in critical_sources:
        details = row.get("details") or {}
        reason = "; ".join(details.get("notes", [])) or row.get("summary", "No details available.")
        lines.append(f"- {row['source']}: {reason}")
    return "\n".join(lines)


def format_open_incidents() -> str:
    incidents = list_open_incidents(limit=5)
    if not incidents:
        return "There are no open incidents right now."

    lines = [f"There are {len(incidents)} recent open incidents:"]
    for incident in incidents:
        lines.append(
            f"- {incident['incident_type']} | {incident['severity']} | "
            f"{incident['affected_asset']} | detected at {incident['detected_at']}"
        )
    return "\n".join(lines)


def format_source_status(source: str) -> str:
    result = get_source_status(source)
    if not result.get("found"):
        return f"I could not find any monitoring data for '{source}'."

    monitor = result.get("latest_monitor_status") or {}
    run = result.get("latest_ingestion_run") or {}
    payload = result.get("latest_payload") or {}
    details = monitor.get("details") or {}

    lines = [
        f"Source: {source}",
        f"Health: {details.get('health', monitor.get('status_color', 'UNKNOWN'))}",
        f"Summary: {monitor.get('summary', 'No summary available.')}",
    ]

    if run:
        lines.extend(
            [
                f"Last run status: {run.get('status', 'UNKNOWN')}",
                f"Last started: {run.get('started_at', 'Unknown')}",
                f"Last finished: {run.get('finished_at', 'Unknown')}",
            ]
        )
        if run.get("error_message"):
            lines.append(f"Latest error: {run['error_message']}")

    if payload:
        lines.append(f"Latest payload at: {payload.get('latest_payload_at', 'Unknown')}")

    recommendation = details.get("recommendation")
    if recommendation:
        lines.append(f"Recommendation: {recommendation}")

    return "\n".join(lines)


def format_recent_history() -> str:
    history = get_recent_monitor_history(limit=8)
    if not history:
        return "No recent monitor history is available yet."

    lines = ["Recent monitor history:"]
    for item in history:
        lines.append(
            f"- {item['checked_at']} | {item['source']} | {item['status_color']} | {item['summary']}"
        )
    return "\n".join(lines)


def build_change_lines(title: str, items: list[str]) -> list[str]:
    if not items:
        return []

    preview = items[:8]
    lines = [f"{title} ({len(items)}):"]
    lines.extend(f"  - {item}" for item in preview)
    if len(items) > len(preview):
        lines.append(f"  - ...and {len(items) - len(preview)} more")
    return lines


def format_latest_schema_drift(source: str | None = None) -> str:
    drift = get_latest_schema_drift(source)
    if not drift.get("found"):
        return drift.get("message") or f"I could not find schema drift rows for '{source}'."

    sections: list[str] = []
    for result in drift.get("results", []):
        section_lines = [
            f"{result['source']} | {result['previous_day']} → {result['current_day']}"
        ]
        if result.get("changed"):
            section_lines.extend(build_change_lines("Added", result.get("added", [])))
            section_lines.extend(build_change_lines("Removed", result.get("removed", [])))
            section_lines.extend(
                build_change_lines("Type changed", result.get("type_changed", []))
            )
            section_lines.extend(
                build_change_lines("Format changed", result.get("format_changed", []))
            )
        else:
            section_lines.append("No schema drift detected.")
        sections.append("\n".join(section_lines))

    header = "Latest schema drift from the previous day to the current day:"
    if source:
        header = f"Latest schema drift for {source}:"

    return (
        header
        + "\n\n"
        + "\n\n".join(sections)
        + f"\n\nInteractive chart: {drift.get('report_path', 'reports/latest/schema_drift_interactive.html').replace('schema_drift_attribute_analysis.csv', 'schema_drift_interactive.html')}"
    )


def answer_question(question: str) -> dict[str, Any]:
    cleaned = (question or "").strip()
    lowered = cleaned.lower()

    if not cleaned:
        return {
            "answer": "Ask about pipeline status, schema drift, failed sources, open incidents, or a specific source.",
            "suggestions": [
                "What is the latest pipeline status?",
                "Show me the latest schema drift changes",
                "Why is the pipeline critical?",
            ],
        }

    source = detect_source(lowered)
    is_schema_drift_question = any(
        phrase in lowered
        for phrase in [
            "schema drift",
            "added and removed",
            "additions and removals",
            "previous day",
            "current day",
        ]
    )

    if is_schema_drift_question:
        answer = format_latest_schema_drift(source)
    elif source and any(word in lowered for word in ["source", "status", "show", "details", "amazon", "walmart", "openfoodfacts", "product"]):
        answer = format_source_status(source)
    elif "why" in lowered and "critical" in lowered:
        answer = format_critical_reason()
    elif "open incident" in lowered or "incidents" in lowered:
        answer = format_open_incidents()
    elif "history" in lowered or "recent monitor" in lowered or "recent status" in lowered:
        answer = format_recent_history()
    elif "failed" in lowered or "failing" in lowered:
        overview = get_pipeline_overview()
        failed = [row for row in overview["sources"] if row.get("status_color") == "RED"]
        if failed:
            answer = "Currently failing source(s):\n" + "\n".join(
                f"- {row['source']}: {row['summary']}" for row in failed
            )
        else:
            answer = "No sources are currently failing."
    else:
        answer = format_pipeline_overview()

    return {
        "answer": answer,
        "suggestions": [
            "Show me the latest schema drift changes",
            "Show me the schema drift for walmart_reviews",
            "Which source failed today?",
            "Do we have any open incidents?",
        ],
    }


async def homepage(_: Request) -> HTMLResponse:
    return HTMLResponse(CHAT_HTML.read_text(encoding="utf-8"))


async def chat(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": "Invalid JSON payload."}, status_code=400)

    message = payload.get("message", "")
    response = answer_question(message)
    return JSONResponse(response)


app = Starlette(
    debug=True,
    routes=[
        Route("/", homepage),
        Route("/api/chat", chat, methods=["POST"]),
    ],
)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8501)
