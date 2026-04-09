-- =============================================================================
-- Data Engineering Control Plane Schema
-- Real API data → PostgreSQL raw landing → Monitoring → Incidents → AI analysis
-- =============================================================================

-- =============================================================================
-- TABLE 1: raw_api_events
-- Purpose: Raw landing zone for API responses
-- Stores complete JSON payload as-is, no parsing or transformation
-- =============================================================================
CREATE TABLE raw_api_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,              
    fetched_at TIMESTAMP NOT NULL,     
    payload JSONB NOT NULL
);

-- Index for querying by source and fetch time (common monitoring queries)
CREATE INDEX idx_raw_api_events_source_fetched 
    ON raw_api_events(source, fetched_at DESC);

-- =============================================================================
-- TABLE 2: ingestion_runs
-- Purpose: Track health of ingestion jobs
-- Every time we run the API pull script, we create one row here
-- =============================================================================
CREATE TABLE ingestion_runs (
    run_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,              -- which API
    started_at TIMESTAMP NOT NULL,     -- when job started
    finished_at TIMESTAMP,             -- when job finished (NULL if still running)
    status TEXT NOT NULL,              -- 'SUCCESS' or 'FAIL'
    rows_written INT,                  -- how many rows inserted
    error_message TEXT                 -- if status='FAIL', error details
);

-- Index for recent runs queries
CREATE INDEX idx_ingestion_runs_source_finished 
    ON ingestion_runs(source, finished_at DESC);

-- =============================================================================
-- TABLE 3: control_plane_incidents
-- Purpose: Track detected data pipeline failures
-- Monitoring checks (freshness, volume, schema drift) insert rows here
-- =============================================================================
CREATE TABLE control_plane_incidents (
    incident_id TEXT PRIMARY KEY,
    incident_type TEXT NOT NULL,       -- 'FRESHNESS', 'VOLUME', 'SCHEMA_DRIFT'
    severity TEXT NOT NULL,            -- 'LOW', 'MED', 'HIGH'
    affected_asset TEXT NOT NULL,      -- what broke (e.g., 'raw_api_events')
    detected_at TIMESTAMP NOT NULL,    -- when problem was discovered
    evidence JSONB NOT NULL,           -- details: expected vs actual
    status TEXT NOT NULL DEFAULT 'OPEN' -- 'OPEN', 'RESOLVED'
);

-- Index for querying open incidents
CREATE INDEX idx_incidents_status 
    ON control_plane_incidents(status);

-- Index for severity-based alerting
CREATE INDEX idx_incidents_severity 
    ON control_plane_incidents(severity);

-- =============================================================================
-- TABLE 4: control_plane_audit_log
-- Purpose: Track all AI agent decisions and actions
-- Immutable log for compliance and auditability
-- =============================================================================
CREATE TABLE control_plane_audit_log (
    audit_id TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,         -- links to the incident being responded to
    decision TEXT NOT NULL,            -- 'RETRY', 'ESCALATE', 'IGNORE'
    confidence_score DOUBLE PRECISION, -- 0.0 to 1.0 (how sure is the AI)
    tools_used JSONB,                  -- JSON list of MCP tools called
    action_taken TEXT NOT NULL,        -- plain-English description of what happened
    logged_at TIMESTAMP NOT NULL,      -- when this decision was recorded
    FOREIGN KEY (incident_id) REFERENCES control_plane_incidents(incident_id)
);

-- Index for audit trail queries
CREATE INDEX idx_audit_log_incident 
    ON control_plane_audit_log(incident_id);

-- Index for recent decisions
CREATE INDEX idx_audit_log_logged_at 
    ON control_plane_audit_log(logged_at DESC);

-- =============================================================================
-- TABLE 5: control_plane_check_status
-- Purpose: Persist latest monitor status (GREEN/YELLOW/RED) per source and check
-- This stores healthy states too, unlike incidents which focus on problems
-- =============================================================================
CREATE TABLE control_plane_check_status (
    status_id TEXT PRIMARY KEY,
    check_name TEXT NOT NULL,          -- e.g., 'SCHEMA_DRIFT'
    source TEXT NOT NULL,              -- e.g., 'amazon_products'
    status_color TEXT NOT NULL,        -- 'GREEN', 'YELLOW', 'RED'
    summary TEXT NOT NULL,             -- short human-readable summary
    details JSONB NOT NULL,            -- structured evidence
    checked_at TIMESTAMP NOT NULL      -- when this status was computed
);

-- Fast lookup for morning "latest status" view
CREATE INDEX idx_check_status_lookup
    ON control_plane_check_status(check_name, source, checked_at DESC);
