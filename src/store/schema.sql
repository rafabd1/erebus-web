PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO schema_meta(key, value) VALUES ('version', '1')
ON CONFLICT(key) DO NOTHING;

CREATE TABLE IF NOT EXISTS scope_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scope_rules (
    id TEXT PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES scope_profiles(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK (action IN ('include', 'exclude')),
    matcher TEXT NOT NULL CHECK (matcher IN ('host', 'host_wildcard', 'url_wildcard', 'regex', 'cidr')),
    pattern TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    order_index INTEGER NOT NULL,
    UNIQUE(profile_id, order_index)
);

CREATE TABLE IF NOT EXISTS capture_sessions (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    source TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    scope_profile_id TEXT NOT NULL REFERENCES scope_profiles(id)
);
CREATE TRIGGER IF NOT EXISTS reserve_active_browser_source
BEFORE INSERT ON capture_sessions
WHEN NEW.ended_at IS NULL
  AND NEW.source GLOB 'browser:*'
  AND EXISTS (
      SELECT 1 FROM capture_sessions
      WHERE source = NEW.source AND ended_at IS NULL
  )
BEGIN
    SELECT RAISE(ABORT, 'active browser source already reserved');
END;

CREATE TABLE IF NOT EXISTS extension_pairings (
    token_hash TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1)),
    created_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_extension_pairings_session
ON extension_pairings(session_id, revoked);

CREATE TABLE IF NOT EXISTS blobs (
    hash TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    stored_size INTEGER NOT NULL,
    codec TEXT NOT NULL,
    mime TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS request_variants (
    id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE,
    method TEXT NOT NULL,
    url TEXT NOT NULL,
    scheme TEXT NOT NULL,
    host TEXT NOT NULL,
    port INTEGER,
    path TEXT NOT NULL,
    raw_query TEXT,
    headers_json TEXT NOT NULL,
    body_hash TEXT REFERENCES blobs(hash),
    body_state TEXT NOT NULL,
    body_reason TEXT,
    body_size INTEGER NOT NULL,
    body_original_size INTEGER NOT NULL,
    content_type TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS response_variants (
    id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE,
    status INTEGER NOT NULL,
    headers_json TEXT NOT NULL,
    body_hash TEXT REFERENCES blobs(hash),
    body_state TEXT NOT NULL,
    body_reason TEXT,
    body_size INTEGER NOT NULL,
    body_original_size INTEGER NOT NULL,
    content_type TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES capture_sessions(id) ON DELETE CASCADE,
    producer_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    captured_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    duration_ms INTEGER,
    request_id TEXT NOT NULL REFERENCES request_variants(id),
    response_id TEXT REFERENCES response_variants(id),
    status INTEGER,
    scope_state TEXT NOT NULL,
    scope_rule_id TEXT,
    scope_reason TEXT NOT NULL,
    error TEXT,
    tab_id TEXT,
    initiator TEXT,
    parent_sequence INTEGER,
    metadata_json TEXT NOT NULL,
    event_fingerprint TEXT NOT NULL,
    UNIQUE(producer_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_observations_session_cursor
ON observations(session_id, captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_observations_cursor
ON observations(captured_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_observations_scope ON observations(scope_state);
CREATE INDEX IF NOT EXISTS idx_request_host_path ON request_variants(host, path, method);
CREATE INDEX IF NOT EXISTS idx_response_status ON response_variants(status);

CREATE TABLE IF NOT EXISTS traffic_search_fallback (
    observation_id TEXT PRIMARY KEY REFERENCES observations(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    headers TEXT NOT NULL,
    body_preview TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS websocket_connections (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES capture_sessions(id) ON DELETE CASCADE,
    producer_id TEXT NOT NULL,
    source_connection_id TEXT NOT NULL,
    url TEXT,
    subprotocol TEXT,
    opened_at TEXT,
    closed_at TEXT,
    close_code INTEGER,
    close_reason TEXT,
    scope_state TEXT NOT NULL,
    scope_rule_id TEXT,
    scope_reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    UNIQUE(session_id, producer_id, source_connection_id)
);

CREATE TABLE IF NOT EXISTS websocket_events (
    id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL REFERENCES websocket_connections(id) ON DELETE CASCADE,
    producer_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    kind TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    direction TEXT,
    opcode INTEGER,
    payload_hash TEXT REFERENCES blobs(hash),
    payload_state TEXT NOT NULL,
    payload_reason TEXT,
    payload_size INTEGER NOT NULL,
    payload_original_size INTEGER NOT NULL,
    error TEXT,
    event_fingerprint TEXT NOT NULL,
    UNIQUE(producer_id, sequence)
);

CREATE INDEX IF NOT EXISTS idx_websocket_events_connection
ON websocket_events(connection_id, captured_at, sequence);

-- The startup orphan-blob sweep checks body hashes with correlated NOT EXISTS
-- subqueries. Without these indexes every blob scans the full variant tables,
-- which takes minutes on a large traffic store and makes the runtime miss its
-- startup timeout.
CREATE INDEX IF NOT EXISTS idx_request_variants_body_hash
ON request_variants(body_hash);
CREATE INDEX IF NOT EXISTS idx_response_variants_body_hash
ON response_variants(body_hash);
CREATE INDEX IF NOT EXISTS idx_websocket_events_payload_hash
ON websocket_events(payload_hash);

CREATE TABLE IF NOT EXISTS websocket_search_fallback (
    event_id TEXT PRIMARY KEY REFERENCES websocket_events(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS capture_gaps (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES capture_sessions(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    dropped_events INTEGER NOT NULL,
    reason TEXT NOT NULL,
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_capture_gaps_session
ON capture_gaps(session_id, observed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS annotations (
    id TEXT PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    body TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_annotations_target ON annotations(target_type, target_id);

CREATE TABLE IF NOT EXISTS graph_nodes (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    canonical_key TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_graph_nodes_kind_label ON graph_nodes(kind, label);

CREATE TABLE IF NOT EXISTS graph_edges (
    src_id TEXT NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
    dst_id TEXT NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(src_id, dst_id, kind)
);

CREATE TABLE IF NOT EXISTS graph_node_evidence (
    node_id TEXT NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES capture_sessions(id) ON DELETE CASCADE,
    evidence_kind TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    scope_state TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    PRIMARY KEY(node_id, evidence_kind, evidence_id)
);
CREATE INDEX IF NOT EXISTS idx_graph_node_evidence_session
ON graph_node_evidence(session_id, node_id, scope_state, captured_at);

CREATE TABLE IF NOT EXISTS graph_edge_evidence (
    src_id TEXT NOT NULL,
    dst_id TEXT NOT NULL,
    edge_kind TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES capture_sessions(id) ON DELETE CASCADE,
    evidence_kind TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    scope_state TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    PRIMARY KEY(src_id, dst_id, edge_kind, evidence_kind, evidence_id),
    FOREIGN KEY(src_id, dst_id, edge_kind)
        REFERENCES graph_edges(src_id, dst_id, kind) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_graph_edge_evidence_session
ON graph_edge_evidence(session_id, src_id, dst_id, edge_kind, scope_state, captured_at);

