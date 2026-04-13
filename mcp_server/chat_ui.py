from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import uvicorn
from openai import OpenAI
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
DRIFT_CHART_HTML = ROOT_DIR / "orchestrator" / "ui" / "schema_drift_chart.html"
KNOWN_SOURCES = [
    "amazon_products",
    "openfoodfacts_snacks",
    "product_search_details",
    "walmart_reviews",
]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")


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
        details = row.get("details") or {}
        last_pulled = details.get("latest_fetched_at") or details.get("last_finished_at") or "Unknown"
        lines.append(f"- {row['source']}: {row['summary']} (last pulled: {last_pulled})")

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


def format_next_step_recommendation() -> str:
    overview = get_pipeline_overview()
    failing_sources = [row for row in overview["sources"] if row.get("status_color") == "RED"]
    warning_sources = [row for row in overview["sources"] if row.get("status_color") == "YELLOW"]

    lines = ["Recommended next steps:"]

    if failing_sources:
        top_issue = failing_sources[0]
        details = top_issue.get("details") or {}
        notes = details.get("notes", [])
        lines.append(
            f"1. Fix `{top_issue['source']}` first because it is currently failing and is the main reason the pipeline is critical."
        )
        if notes:
            lines.append(f"   Reason: {notes[0]}")
        lines.append("   Action: retry the ingestion call and validate the API credentials, access rules, or endpoint availability.")

    if warning_sources:
        for idx, source in enumerate(warning_sources, start=2):
            lines.append(
                f"{idx}. Review `{source['source']}` for non-breaking schema drift and decide whether downstream mappings should be updated."
            )

    incidents = list_open_incidents(limit=5)
    if incidents:
        lines.append(f"- There are also {len(incidents)} open incidents worth reviewing after the active failure is addressed.")

    if not failing_sources and not warning_sources:
        lines.append("1. No urgent fixes are needed right now. Continue normal monitoring and review recent schema drift for potential downstream updates.")

    return "\n".join(lines)


CONVERSATIONAL_PATTERNS = [
    "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
    "how are you", "what are you", "who are you", "what can you do",
    "what do you do", "help me", "thanks", "thank you", "awesome", "great",
    "cool", "nice", "sounds good", "got it", "ok", "okay", "sure", "bye",
    "goodbye", "see you", "cheers", "appreciate",
]


def is_conversational(question: str) -> bool:
    lowered = question.lower().strip()
    return any(lowered.startswith(p) or lowered == p for p in CONVERSATIONAL_PATTERNS)


def should_use_llm(question: str) -> bool:
    return bool(OPENAI_API_KEY)


def build_llm_context(question: str, source: str | None, structured_answer: str) -> dict[str, Any]:
    context: dict[str, Any] = {
        "question": question,
        "structured_answer": structured_answer,
        "pipeline_overview": get_pipeline_overview(),
    }

    if source:
        context["source_status"] = get_source_status(source)

    lowered = question.lower()
    if "incident" in lowered:
        context["open_incidents"] = list_open_incidents(limit=5)

    if any(term in lowered for term in ["schema drift", "added", "removed", "previous day", "current day"]):
        context["schema_drift"] = get_latest_schema_drift(source)

    return context


def generate_llm_answer(question: str, structured_answer: str, source: str | None = None) -> str | None:
    if not OPENAI_API_KEY:
        return None

    context = build_llm_context(question, source, structured_answer)
    prompt = f"""
You are Pipeline Sentinel, a professional and friendly AI assistant for a data engineering control plane.
Your personality: confident, clear, and warm — like a senior data engineer who explains things simply.

Rules:
- For greetings or small talk, respond naturally and briefly. Mention you're a pipeline assistant and offer to help.
- For pipeline questions, answer using the monitoring context below. Be concise and specific.
- Preserve exact facts, source names, counts, and error messages from the context.
- For plain-English requests, summarize naturally for a non-technical person.
- For "what to fix" or "next steps", give a short numbered prioritized recommendation.
- For team/stakeholder updates, write a polished message they can share directly.
- Never invent data that is not in the context.
- Do not start every response with "I" — vary your openings.

User message:
{question}

Monitoring context (use only when relevant):
{json.dumps(context, indent=2)}
""".strip()

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        response = client.responses.create(
            model=OPENAI_MODEL,
            input=prompt,
        )
        answer = (response.output_text or "").strip()
        return answer or None
    except Exception:
        return None


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

    default_suggestions = [
        "What is the latest pipeline status?",
        "Show me the latest schema drift changes",
        "Why is the pipeline critical?",
        "What should I fix first?",
    ]

    if not cleaned:
        return {
            "answer": "Hey! I'm Pipeline Sentinel — your data pipeline assistant. Ask me about pipeline health, schema drift, incidents, or a specific source.",
            "suggestions": default_suggestions,
        }

    # Route conversational messages straight to the LLM
    if is_conversational(cleaned) and OPENAI_API_KEY:
        llm_answer = generate_llm_answer(cleaned, "", None)
        return {
            "answer": llm_answer or "Hey! Ask me about your pipeline health, schema drift, or open incidents.",
            "suggestions": default_suggestions,
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
    elif any(phrase in lowered for phrase in ["what should i fix first", "what should i do next", "recommend", "next step", "priority"]):
        answer = format_next_step_recommendation()
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

    if should_use_llm(cleaned):
        llm_answer = generate_llm_answer(cleaned, answer, source)
        if llm_answer:
            answer = llm_answer

    return {
        "answer": answer,
        "suggestions": [
            "What should I fix first?",
            "What do you recommend I do next?",
            "Write a short update for my team",
            "Do we have any open incidents?",
        ],
    }


async def homepage(_: Request) -> HTMLResponse:
    return HTMLResponse(CHAT_HTML.read_text(encoding="utf-8"))


async def drift_chart(_: Request) -> HTMLResponse:
    if not DRIFT_CHART_HTML.exists():
        return HTMLResponse("<p style='color:#9fb0d1;font-family:sans-serif;padding:24px'>Schema drift chart not yet generated. Run the monitor cycle first.</p>")
    return HTMLResponse(DRIFT_CHART_HTML.read_text(encoding="utf-8"))


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
        Route("/drift-chart", drift_chart),
        Route("/api/chat", chat, methods=["POST"]),
    ],
)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8501)
