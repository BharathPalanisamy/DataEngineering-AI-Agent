# MCP Server

This folder exposes your control-plane data to AI through a simple Python MCP server.

## What it can answer

- `get_pipeline_overview()` — latest overall health across all sources
- `get_source_status(source)` — most recent status for one source
- `list_open_incidents(limit)` — unresolved incidents
- `get_recent_monitor_history(limit)` — recent monitoring summaries for demos

## Run it locally

```bash
cd /Users/bharathpalanisamy/Documents/AI_agent_project
source .venv/bin/activate
python mcp_server/server.py
```

## Connect it to an MCP client

Use this command in your MCP client config:

- **command:** `/Users/bharathpalanisamy/Documents/AI_agent_project/.venv/bin/python`
- **args:** `['/Users/bharathpalanisamy/Documents/AI_agent_project/mcp_server/server.py']`

## Good demo questions

- What is the latest pipeline status?
- Which source failed most recently?
- Do we have any open incidents?
- Show me the latest status for `amazon_products`.
