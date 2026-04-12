# Pipeline Sentinel

Pipeline Sentinel is an AI-powered data engineering control plane for API-driven pipelines.
It ingests raw API payloads, monitors pipeline health, detects schema drift, persists incidents, and provides a chat interface for natural-language diagnostics and next-step recommendations.

## What this project does

- Ingests data from multiple external APIs into PostgreSQL as raw JSON.
- Tracks ingestion outcomes (success/failure), timestamps, and error messages.
- Detects schema drift (added/removed/type/format changes) day over day.
- Persists control-plane incidents and monitor snapshots.
- Generates drift reports (CSV, PNG, interactive HTML).
- Exposes data through MCP tools for AI interaction.
- Serves a browser chat UI with OpenAI-powered summaries and recommendations.

## Core capabilities

1. Pipeline monitoring
- Source-level health, freshness, and run status.
- Overall status classification: HEALTHY, WARNING, CRITICAL.

2. Schema drift proof
- Day-over-day comparison across sources.
- Evidence for Added, Removed, Type Changed, and Format Changed attributes.
- Interactive drift chart output for demos.

3. AI assistant behavior
- Plain-English pipeline summaries.
- Root-cause explanations.
- Prioritized next-step recommendations.
- Stakeholder/team-ready status updates.

4. Automation
- End-to-end monitor cycle script.
- Daily scheduler support via cron.

## Project structure

- ingest: API ingestion logic
- control_plane: drift checks and report generation
- orchestrator: monitoring summary logic and UI assets
- mcp_server: MCP tools and browser chat backend
- sql: database schema
- reports/latest: generated drift artifacts
- run_monitor_cycle.sh: full monitor pipeline runner
- run_chat_ui.sh: chat UI launcher

## Tech stack

- Python 3.14
- PostgreSQL 16 (Docker)
- psycopg2, requests, python-dotenv
- Matplotlib + Plotly
- FastMCP
- Starlette + Uvicorn
- OpenAI API

## Prerequisites

- macOS/Linux shell
- Docker
- Python virtual environment at .venv
- PostgreSQL container name: dcp_postgres

## Environment variables

Create a local .env (do not commit it):

- DATABASE_URL
- RAPIDAPI_KEY
- AMAZON_API_HOST
- PRODUCT_SEARCH_API_HOST
- WALMART_API_HOST
- OPENAI_API_KEY
- OPENAI_MODEL (optional, default gpt-4.1-mini)

## Setup

1. Start database

```bash
docker compose up -d
```

2. Initialize schema

```bash
psql "$DATABASE_URL" -f sql/schema.sql
```

3. Install Python deps in your virtual environment

```bash
.venv/bin/pip install -U pip requests psycopg2-binary python-dotenv matplotlib plotly mcp starlette uvicorn openai
```

## Run the full monitoring cycle

```bash
./run_monitor_cycle.sh
```

This executes:

1. ingestion
2. schema drift check
3. drift report generation
4. monitoring summary

## Run the chat assistant

```bash
./run_chat_ui.sh
```

Open:

http://127.0.0.1:8501/

## Useful chatbot prompts

- What is the latest pipeline status?
- Summarize today’s pipeline health in plain English
- Why is the pipeline critical right now?
- What should I fix first?
- Show me the schema drift for walmart_reviews
- Write a short update for my team

## Key outputs

- Drift CSV: reports/latest/schema_drift_attribute_analysis.csv
- Drift chart (interactive): reports/latest/schema_drift_interactive.html
- Drift chart (UI): orchestrator/ui/schema_drift_chart.html
- Monitor log (runtime): ~/Library/Logs/AI_agent_project/monitor.log

## Scheduling

Current daily schedule example:

```cron
0 14 * * * /bin/zsh /Users/bharathpalanisamy/AI_agent_project_runtime/run_monitor_cycle.sh
```

Note: If the laptop is asleep at trigger time, cron can miss the run.

## MCP tools exposed

- get_pipeline_overview
- get_source_status
- list_open_incidents
- get_recent_monitor_history
- get_latest_schema_drift

## Troubleshooting

1. Chat UI exits with code 137
- Port 8501 is likely already in use.
- Kill old process and relaunch:

```bash
(lsof -ti tcp:8501 | xargs kill -9) >/dev/null 2>&1 || true
./run_chat_ui.sh
```

2. OpenAI key test

```bash
.venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv
from openai import OpenAI
load_dotenv('.env')
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
r = client.responses.create(model=os.getenv('OPENAI_MODEL','gpt-4.1-mini'), input='Reply exactly with: OpenAI API is working.')
print(r.output_text)
PY
```

3. API source failure
- Check latest ingestion error in ingestion_runs.
- Validate API key/host/env values.

## Security notes

- Never commit .env.
- Rotate keys immediately if exposed.
- Keep generated caches and local artifacts out of git.

## Hiring-ready project summary

Pipeline Sentinel demonstrates end-to-end skills in:

- Data ingestion and reliability
- Monitoring and incident design
- Schema drift governance
- Automation and operational workflows
- AI/LLM integration for practical diagnostics

This project is designed as a portfolio-grade example of modern Data Engineering + AI operations.
