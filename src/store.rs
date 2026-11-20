mod schema {
    pub const BASE: &str = include_str!("store/schema.sql");
    pub const FTS: &str = include_str!("store/fts.sql");
}

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{anyhow, bail};
use base64::Engine;
use rusqlite::types::{Type as SqlType, Value as SqlValue};
use rusqlite::{Connection, OptionalExtension, Transaction, params, params_from_iter};
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};
use url::Url;
use uuid::Uuid;

use crate::blob::{BlobStore, StoredBlob};
use crate::extract::body_parameters;
use crate::fingerprint::{content_type, request_fingerprint, response_fingerprint};
use crate::graph;
use crate::model::*;
use crate::scope::ScopeProfile;

const SCHEMA_VERSION: u32 = 1;
const MAX_BATCH_ITEMS: usize = 100;
pub const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_TEXT_INDEX_BYTES: usize = 256 * 1024;
const MAX_BODY_PREVIEW_BYTES: usize = 64 * 1024;
const ORPHAN_BLOB_GRACE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Error)]
#[error("{0}")]
pub struct InputError(String);

pub fn is_input_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.is::<InputError>())
}

pub(crate) fn invalid(message: impl Into<String>) -> anyhow::Error {
    InputError(message.into()).into()
}

pub struct Store {
    database_path: PathBuf,
    writer: Mutex<Connection>,
    blobs: BlobStore,
    fts_enabled: bool,
}

#[derive(Debug)]
struct DecodedBody {
    bytes: Option<Vec<u8>>,
    state: String,
    reason: Option<String>,
    stored: Option<StoredBlob>,
    original_size: u64,
}

#[derive(Clone, Debug)]
pub struct ReplaySourceRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<HeaderField>,
    pub body: ReplaySourceBody,
}

#[derive(Clone, Debug)]
pub struct ReplaySourceBody {
    pub bytes: Option<Vec<u8>>,
    pub state: String,
    pub reason: Option<String>,
    pub original_size: u64,
}

#[derive(Clone, Debug)]
pub struct ReplayRecord {
    pub observation_id: String,
    pub status: Option<u16>,
    pub error: Option<String>,
    pub metadata: serde_json::Value,
}

impl Store {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        let database_path = data_dir.join("traffic.sqlite");
        let mut connection = Connection::open(&database_path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA foreign_keys=ON;
             PRAGMA auto_vacuum=INCREMENTAL;",
        )?;
        let transaction = connection.transaction()?;
        transaction.execute_batch(schema::BASE)?;
        ensure_default_scope(&transaction)?;
        transaction.commit()?;
        let fts_enabled = match connection.execute_batch(schema::FTS) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, "FTS5 is unavailable; body search will use a bounded fallback");
                false
            }
        };
        let blobs = BlobStore::new(data_dir)?;
        maintain_blobs(&mut connection, &blobs, ORPHAN_BLOB_GRACE)?;
        Ok(Self {
            database_path,
            writer: Mutex::new(connection),
            blobs,
            fts_enabled,
        })
    }

    pub fn schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    pub fn create_session(&self, input: CreateSession) -> anyhow::Result<CaptureSession> {
        let label = input.label.trim();
        if label.is_empty() {
            return Err(invalid("session label cannot be empty"));
        }
        let scope_profile_id = input.scope_profile_id.unwrap_or_else(|| "default".into());
        let connection = self.writer()?;
        let exists = connection
            .query_row(
                "SELECT 1 FROM scope_profiles WHERE id = ?1",
                [&scope_profile_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            return Err(invalid(format!(
                "scope profile {scope_profile_id} does not exist"
            )));
        }
        let session = CaptureSession {
            id: Uuid::now_v7().to_string(),
            label: label.into(),
            source: input.source,
            started_at: now(),
            ended_at: None,
            scope_profile_id,
        };
        connection.execute(
            "INSERT INTO capture_sessions(id, label, source, started_at, scope_profile_id)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session.id,
                session.label,
                session.source,
                session.started_at,
                session.scope_profile_id
            ],
        )?;
        Ok(session)
    }

    pub fn list_sessions(&self) -> anyhow::Result<Vec<CaptureSession>> {
        let connection = self.reader()?;
        let mut statement = connection.prepare(
            "SELECT id, label, source, started_at, ended_at, scope_profile_id
             FROM capture_sessions ORDER BY started_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(CaptureSession {
                id: row.get(0)?,
                label: row.get(1)?,
                source: row.get(2)?,
                started_at: row.get(3)?,
                ended_at: row.get(4)?,
                scope_profile_id: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn session(&self, id: &str) -> anyhow::Result<Option<CaptureSession>> {
        let connection = self.reader()?;
        connection
            .query_row(
                "SELECT id, label, source, started_at, ended_at, scope_profile_id
                 FROM capture_sessions WHERE id = ?1",
                [id],
                row_to_capture_session,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn end_session(&self, id: &str) -> anyhow::Result<Option<CaptureSession>> {
        let connection = self.writer()?;
        let ended_at = now();
        connection.execute(
            "UPDATE capture_sessions SET ended_at = COALESCE(ended_at, ?2) WHERE id = ?1",
            params![id, ended_at],
        )?;
        connection
            .query_row(
                "SELECT id, label, source, started_at, ended_at, scope_profile_id
                 FROM capture_sessions WHERE id = ?1",
                [id],
                |row| {
                    Ok(CaptureSession {
                        id: row.get(0)?,
                        label: row.get(1)?,
                        source: row.get(2)?,
                        started_at: row.get(3)?,
                        ended_at: row.get(4)?,
                        scope_profile_id: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn delete_session(&self, id: &str) -> anyhow::Result<Option<SessionDeleteResult>> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let ended_at = transaction
            .query_row(
                "SELECT ended_at FROM capture_sessions WHERE id = ?1",
                [id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        let Some(ended_at) = ended_at else {
            return Ok(None);
        };
        if ended_at.is_none() {
            return Err(invalid(
                "active capture sessions must be ended before deletion",
            ));
        }

        let observations = transaction.query_row(
            "SELECT COUNT(*) FROM observations WHERE session_id = ?1",
            [id],
            |row| row.get::<_, i64>(0),
        )? as u64;
        let websocket_events = transaction.query_row(
            "SELECT COUNT(*) FROM websocket_events we
             JOIN websocket_connections wc ON wc.id = we.connection_id
             WHERE wc.session_id = ?1",
            [id],
            |row| row.get::<_, i64>(0),
        )? as u64;

        transaction.execute(
            "DELETE FROM annotations
             WHERE target_type = 'observation'
               AND target_id IN (SELECT id FROM observations WHERE session_id = ?1)",
            [id],
        )?;
        transaction.execute(
            "DELETE FROM annotations
             WHERE target_type = 'websocket_event'
               AND target_id IN (
                    SELECT we.id FROM websocket_events we
                    JOIN websocket_connections wc ON wc.id = we.connection_id
                    WHERE wc.session_id = ?1
               )",
            [id],
        )?;
        if self.fts_enabled {
            transaction.execute(
                "DELETE FROM traffic_fts
                 WHERE observation_id IN (SELECT id FROM observations WHERE session_id = ?1)",
                [id],
            )?;
            transaction.execute(
                "DELETE FROM websocket_fts
                 WHERE event_id IN (
                    SELECT we.id FROM websocket_events we
                    JOIN websocket_connections wc ON wc.id = we.connection_id
                    WHERE wc.session_id = ?1
                 )",
                [id],
            )?;
        }

        transaction.execute("DELETE FROM capture_sessions WHERE id = ?1", [id])?;
        transaction.execute(
            "DELETE FROM graph_edges
             WHERE NOT EXISTS (
                SELECT 1 FROM graph_edge_evidence gee
                WHERE gee.src_id = graph_edges.src_id
                  AND gee.dst_id = graph_edges.dst_id
                  AND gee.edge_kind = graph_edges.kind
             )",
            [],
        )?;
        transaction.execute_batch(
            "UPDATE graph_edges
             SET first_seen = (
                    SELECT MIN(captured_at) FROM graph_edge_evidence gee
                    WHERE gee.src_id = graph_edges.src_id
                      AND gee.dst_id = graph_edges.dst_id
                      AND gee.edge_kind = graph_edges.kind
                 ),
                 last_seen = (
                    SELECT MAX(captured_at) FROM graph_edge_evidence gee
                    WHERE gee.src_id = graph_edges.src_id
                      AND gee.dst_id = graph_edges.dst_id
                      AND gee.edge_kind = graph_edges.kind
                 ),
                 evidence_count = (
                    SELECT COUNT(*) FROM graph_edge_evidence gee
                    WHERE gee.src_id = graph_edges.src_id
                      AND gee.dst_id = graph_edges.dst_id
                      AND gee.edge_kind = graph_edges.kind
                 );
             DELETE FROM graph_nodes
             WHERE NOT EXISTS (
                SELECT 1 FROM graph_node_evidence ge WHERE ge.node_id = graph_nodes.id
             );
             UPDATE graph_nodes
             SET first_seen = (
                    SELECT MIN(captured_at) FROM graph_node_evidence ge
                    WHERE ge.node_id = graph_nodes.id
                 ),
                 last_seen = (
                    SELECT MAX(captured_at) FROM graph_node_evidence ge
                    WHERE ge.node_id = graph_nodes.id
                 ),
                 evidence_count = (
                    SELECT COUNT(*) FROM graph_node_evidence ge
                    WHERE ge.node_id = graph_nodes.id
                 );
             DELETE FROM annotations
             WHERE target_type = 'graph_node'
               AND NOT EXISTS (
                    SELECT 1 FROM graph_nodes n WHERE n.id = annotations.target_id
               );
             DELETE FROM request_variants
             WHERE NOT EXISTS (
                    SELECT 1 FROM observations o WHERE o.request_id = request_variants.id
             );
             DELETE FROM response_variants
             WHERE NOT EXISTS (
                    SELECT 1 FROM observations o WHERE o.response_id = response_variants.id
             );
             DELETE FROM blobs
             WHERE NOT EXISTS (
                    SELECT 1 FROM request_variants r WHERE r.body_hash = blobs.hash
             ) AND NOT EXISTS (
                    SELECT 1 FROM response_variants r WHERE r.body_hash = blobs.hash
             ) AND NOT EXISTS (
                    SELECT 1 FROM websocket_events we WHERE we.payload_hash = blobs.hash
             );",
        )?;
        if self.fts_enabled {
            transaction.execute("DELETE FROM graph_fts", [])?;
            transaction.execute(
                "INSERT INTO graph_fts(node_id, label, metadata)
                 SELECT id, label, metadata_json FROM graph_nodes",
                [],
            )?;
        }
        transaction.commit()?;

        if let Err(error) = maintain_blobs(&mut connection, &self.blobs, Duration::ZERO) {
            tracing::warn!(%error, %id, "session data was deleted but blob cleanup will retry later");
        }
        if let Err(error) = connection.execute_batch("PRAGMA incremental_vacuum(256);") {
            tracing::warn!(%error, %id, "session data was deleted but database compaction was deferred");
        }
        Ok(Some(SessionDeleteResult {
            id: id.to_owned(),
            observations,
            websocket_events,
        }))
    }

    pub fn save_scope_profile(&self, input: SaveScopeProfile) -> anyhow::Result<String> {
        validate_scope_input(&input)?;

        let id = Uuid::now_v7().to_string();
        let timestamp = now();
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO scope_profiles(id, name, version, created_at, updated_at)
             VALUES (?1, ?2, 1, ?3, ?3)",
            params![id, input.name.trim(), timestamp],
        )?;
        for (index, rule) in input.rules.into_iter().enumerate() {
            transaction.execute(
                "INSERT INTO scope_rules(id, profile_id, action, matcher, pattern, enabled, order_index)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    Uuid::now_v7().to_string(),
                    id,
                    rule.action.as_str(),
                    rule.matcher.as_str(),
                    rule.pattern,
                    rule.enabled,
                    index as i64
                ],
            )?;
        }
        transaction.commit()?;
        Ok(id)
    }

    pub fn scope_profiles(&self) -> anyhow::Result<Vec<ScopeProfileView>> {
        let connection = self.reader()?;
        let ids = {
            let mut statement = connection.prepare(
                "SELECT id FROM scope_profiles
                 ORDER BY CASE WHEN id = 'default' THEN 0 ELSE 1 END, name, id",
            )?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        ids.into_iter()
            .map(|id| {
                load_scope_profile_view(&connection, &id)?
                    .ok_or_else(|| anyhow!("scope profile disappeared while being listed"))
            })
            .collect()
    }

    pub fn scope_profile(&self, id: &str) -> anyhow::Result<Option<ScopeProfileView>> {
        let connection = self.reader()?;
        load_scope_profile_view(&connection, id)
    }

    pub fn update_scope_profile(
        &self,
        id: &str,
        input: SaveScopeProfile,
    ) -> anyhow::Result<Option<ScopeProfileView>> {
        validate_scope_input(&input)?;
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let timestamp = now();
        let updated = transaction.execute(
            "UPDATE scope_profiles
             SET name = ?2, version = version + 1, updated_at = ?3
             WHERE id = ?1",
            params![id, input.name.trim(), timestamp],
        )?;
        if updated == 0 {
            return Ok(None);
        }
        transaction.execute("DELETE FROM scope_rules WHERE profile_id = ?1", [id])?;
        for (index, rule) in input.rules.into_iter().enumerate() {
            transaction.execute(
                "INSERT INTO scope_rules(
                    id, profile_id, action, matcher, pattern, enabled, order_index
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    Uuid::now_v7().to_string(),
                    id,
                    rule.action.as_str(),
                    rule.matcher.as_str(),
                    rule.pattern,
                    rule.enabled,
                    index as i64
                ],
            )?;
        }
        transaction.commit()?;
        load_scope_profile_view(&connection, id)
    }

    pub fn delete_scope_profile(&self, id: &str) -> anyhow::Result<bool> {
        if id == "default" {
            return Err(invalid("the default scope profile cannot be deleted"));
        }
        let connection = self.writer()?;
        let session_count = connection.query_row(
            "SELECT COUNT(*) FROM capture_sessions WHERE scope_profile_id = ?1",
            [id],
            |row| row.get::<_, i64>(0),
        )?;
        if session_count > 0 {
            return Err(invalid("scope profile is still used by capture sessions"));
        }
        Ok(connection.execute("DELETE FROM scope_profiles WHERE id = ?1", [id])? > 0)
    }

    pub fn ingest_http(&self, batch: HttpIngestBatch) -> anyhow::Result<Vec<IngestResult>> {
        validate_batch(&batch.producer_id, &batch.session_id, batch.events.len())?;
        let mut connection = self.writer()?;
        let profile_id = session_scope_profile(&connection, &batch.session_id)?;
        let scope = load_scope_profile(&connection, &profile_id)?;
        let mut results = Vec::with_capacity(batch.events.len());
        for event in batch.events {
            let sequence = event.sequence;
            match self.ingest_http_event(
                &mut connection,
                &batch.producer_id,
                &batch.session_id,
                &scope,
                event,
            ) {
                Ok(result) => results.push(result),
                Err(error) => results.push(IngestResult {
                    sequence,
                    status: IngestStatus::Rejected,
                    observation_id: None,
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(results)
    }

    pub fn ingest_websocket(
        &self,
        batch: WebSocketIngestBatch,
    ) -> anyhow::Result<Vec<IngestResult>> {
        validate_batch(&batch.producer_id, &batch.session_id, batch.frames.len())?;
        let mut connection = self.writer()?;
        let profile_id = session_scope_profile(&connection, &batch.session_id)?;
        let scope = load_scope_profile(&connection, &profile_id)?;
        let mut results = Vec::with_capacity(batch.frames.len());
        for event in batch.frames {
            let sequence = event.sequence;
            match self.ingest_websocket_event(
                &mut connection,
                &batch.producer_id,
                &batch.session_id,
                &scope,
                event,
            ) {
                Ok(result) => results.push(result),
                Err(error) => results.push(IngestResult {
                    sequence,
                    status: IngestStatus::Rejected,
                    observation_id: None,
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(results)
    }

    pub fn traffic(&self, query: TrafficQuery) -> anyhow::Result<TrafficPage> {
        let connection = self.reader()?;
        let limit = query.limit.unwrap_or(50).clamp(1, 200) as usize;
        let mut sql = String::from(
            "SELECT o.id, o.captured_at, req.method, req.url, req.host, req.path,
                    res.status, o.duration_ms, o.scope_state, req.body_size,
                    COALESCE(res.body_size, 0)
             FROM observations o
             JOIN request_variants req ON req.id = o.request_id
             LEFT JOIN response_variants res ON res.id = o.response_id
             WHERE 1=1",
        );
        let mut values = Vec::<SqlValue>::new();
        if let Some(session_id) = query.session_id {
            sql.push_str(" AND o.session_id = ?");
            values.push(session_id.to_owned().into());
        }
        if let Some(cursor) = query.cursor {
            let (captured_at, id) = parse_cursor(&cursor)?;
            sql.push_str(" AND (o.captured_at < ? OR (o.captured_at = ? AND o.id < ?))");
            values.push(captured_at.clone().into());
            values.push(captured_at.into());
            values.push(id.into());
        }
        if let Some(host) = query.host {
            sql.push_str(" AND req.host = ?");
            values.push(host.to_ascii_lowercase().into());
        }
        if let Some(method) = query.method {
            sql.push_str(" AND req.method = ?");
            values.push(method.to_ascii_uppercase().into());
        }
        if let Some(path) = query.path {
            sql.push_str(" AND req.path LIKE ? ESCAPE '\\'");
            values.push(format!("%{}%", escape_like(&path)).into());
        }
        if let Some(status) = query.status {
            sql.push_str(" AND res.status = ?");
            values.push((status as i64).into());
        }
        if let Some(scope) = query.scope {
            sql.push_str(" AND o.scope_state = ?");
            values.push(scope.into());
        }
        if let Some(text) = query.text.filter(|value| !value.trim().is_empty()) {
            if self.fts_enabled && text.chars().count() >= 3 {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1 FROM traffic_fts f
                        WHERE f.observation_id = o.id AND traffic_fts MATCH ?
                    )",
                );
                values.push(format!("\"{}\"", text.replace('"', "\"\"")).into());
            } else if self.fts_enabled {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1 FROM traffic_fts f
                        WHERE f.observation_id = o.id
                          AND (f.url LIKE ? ESCAPE '\\'
                            OR f.headers LIKE ? ESCAPE '\\'
                            OR f.body_preview LIKE ? ESCAPE '\\')
                    )",
                );
                let like = format!("%{}%", escape_like(&text));
                values.push(like.clone().into());
                values.push(like.clone().into());
                values.push(like.into());
            } else {
                sql.push_str(
                    " AND (req.url LIKE ? ESCAPE '\\' OR req.headers_json LIKE ? ESCAPE '\\')",
                );
                let like = format!("%{}%", escape_like(&text));
                values.push(like.clone().into());
                values.push(like.into());
            }
        }
        sql.push_str(" ORDER BY o.captured_at DESC, o.id DESC LIMIT ?");
        values.push(((limit + 1) as i64).into());

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_summary)?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                items
                    .last()
                    .map(|item| make_cursor(&item.captured_at, &item.id))
            })
            .flatten();
        Ok(TrafficPage { items, next_cursor })
    }

    pub fn websocket_events(&self, query: WebSocketQuery) -> anyhow::Result<WebSocketPage> {
        let connection = self.reader()?;
        let limit = query.limit.unwrap_or(50).clamp(1, 200) as usize;
        let mut sql = String::from(
            "SELECT we.id, we.connection_id, we.captured_at, we.kind, we.direction,
                    we.opcode, wc.url, wc.subprotocol, wc.state, wc.close_code,
                    wc.close_reason, wc.scope_state, we.payload_size,
                    we.payload_original_size, we.error
             FROM websocket_events we
             JOIN websocket_connections wc ON wc.id = we.connection_id
             WHERE 1=1",
        );
        let mut values = Vec::<SqlValue>::new();
        if let Some(session_id) = query.session_id {
            sql.push_str(" AND wc.session_id = ?");
            values.push(session_id.into());
        }
        if let Some(connection_id) = query.connection_id {
            sql.push_str(" AND we.connection_id = ?");
            values.push(connection_id.into());
        }
        if let Some(cursor) = query.cursor {
            let (captured_at, id) = parse_cursor(&cursor)?;
            sql.push_str(" AND (we.captured_at < ? OR (we.captured_at = ? AND we.id < ?))");
            values.push(captured_at.clone().into());
            values.push(captured_at.into());
            values.push(id.into());
        }
        if let Some(url) = query.url.filter(|value| !value.trim().is_empty()) {
            sql.push_str(" AND wc.url LIKE ? ESCAPE '\\'");
            values.push(format!("%{}%", escape_like(&url)).into());
        }
        if let Some(kind) = query.kind.filter(|value| !value.trim().is_empty()) {
            sql.push_str(" AND we.kind = ?");
            values.push(kind.to_ascii_lowercase().into());
        }
        if let Some(direction) = query.direction.filter(|value| !value.trim().is_empty()) {
            sql.push_str(" AND we.direction = ?");
            values.push(direction.to_ascii_lowercase().into());
        }
        if let Some(opcode) = query.opcode {
            sql.push_str(" AND we.opcode = ?");
            values.push((opcode as i64).into());
        }
        if let Some(scope) = query.scope.filter(|value| !value.trim().is_empty()) {
            sql.push_str(" AND wc.scope_state = ?");
            values.push(scope.to_ascii_lowercase().into());
        }
        if let Some(text) = query.text.filter(|value| !value.trim().is_empty()) {
            if self.fts_enabled && text.chars().count() >= 3 {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1 FROM websocket_fts wf
                        WHERE wf.event_id = we.id AND websocket_fts MATCH ?
                    )",
                );
                values.push(format!("\"{}\"", text.replace('"', "\"\"")).into());
            } else if self.fts_enabled {
                sql.push_str(
                    " AND EXISTS (
                        SELECT 1 FROM websocket_fts wf
                        WHERE wf.event_id = we.id AND wf.payload LIKE ? ESCAPE '\\'
                    )",
                );
                values.push(format!("%{}%", escape_like(&text)).into());
            } else {
                sql.push_str(" AND wc.url LIKE ? ESCAPE '\\'");
                values.push(format!("%{}%", escape_like(&text)).into());
            }
        }
        sql.push_str(" ORDER BY we.captured_at DESC, we.id DESC LIMIT ?");
        values.push(((limit + 1) as i64).into());
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_websocket_summary)?;
        let mut items = rows.collect::<Result<Vec<_>, _>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more
            .then(|| {
                items
                    .last()
                    .map(|item| make_cursor(&item.captured_at, &item.id))
            })
            .flatten();
        Ok(WebSocketPage { items, next_cursor })
    }

    pub fn websocket_event(&self, id: &str) -> anyhow::Result<Option<WebSocketEventDetail>> {
        let connection = self.reader()?;
        let row = connection
            .query_row(
                "SELECT we.id, we.connection_id, we.captured_at, we.kind, we.direction,
                        we.opcode, wc.url, wc.subprotocol, wc.state, wc.close_code,
                        wc.close_reason, wc.scope_state, we.payload_size,
                        we.payload_original_size, we.error, we.payload_hash,
                        we.payload_state, we.payload_reason,
                        CASE WHEN we.opcode = 1 THEN 'text/plain' ELSE NULL END
                 FROM websocket_events we
                 JOIN websocket_connections wc ON wc.id = we.connection_id
                 WHERE we.id = ?1",
                [id],
                |row| {
                    Ok((
                        row_to_websocket_summary(row)?,
                        BodyRow {
                            hash: row.get(15)?,
                            state: row.get(16)?,
                            reason: row.get(17)?,
                            original_size: row.get::<_, i64>(13)? as u64,
                            mime: row.get(18)?,
                        },
                    ))
                },
            )
            .optional()?;
        let Some((summary, payload)) = row else {
            return Ok(None);
        };
        Ok(Some(WebSocketEventDetail {
            summary,
            payload: self.body_detail(&connection, payload)?,
        }))
    }

    pub fn record_gap(&self, input: CaptureGapInput) -> anyhow::Result<CaptureGap> {
        let source = input.source.trim();
        let reason = input.reason.trim();
        if source.is_empty() || source.len() > 64 {
            return Err(invalid("capture gap source must contain 1 to 64 bytes"));
        }
        if reason.is_empty() || reason.len() > 2_048 {
            return Err(invalid("capture gap reason must contain 1 to 2048 bytes"));
        }
        let observed_at = normalized_time(input.observed_at.as_deref())?;
        let connection = self.writer()?;
        let session_exists = connection
            .query_row(
                "SELECT 1 FROM capture_sessions WHERE id = ?1",
                [&input.session_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !session_exists {
            return Err(invalid(format!(
                "capture session {} does not exist",
                input.session_id
            )));
        }
        let gap = CaptureGap {
            id: Uuid::now_v7().to_string(),
            session_id: input.session_id,
            source: source.into(),
            dropped_events: input.dropped_events,
            reason: reason.into(),
            observed_at,
        };
        connection.execute(
            "INSERT INTO capture_gaps(
                id, session_id, source, dropped_events, reason, observed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                gap.id,
                gap.session_id,
                gap.source,
                gap.dropped_events as i64,
                gap.reason,
                gap.observed_at
            ],
        )?;
        Ok(gap)
    }

    pub fn capture_gaps(&self, query: CaptureGapQuery) -> anyhow::Result<Vec<CaptureGap>> {
        let connection = self.reader()?;
        let mut sql = String::from(
            "SELECT id, session_id, source, dropped_events, reason, observed_at
             FROM capture_gaps WHERE 1=1",
        );
        let mut values = Vec::<SqlValue>::new();
        if let Some(session_id) = query.session_id {
            sql.push_str(" AND session_id = ?");
            values.push(session_id.into());
        }
        sql.push_str(" ORDER BY observed_at DESC, id DESC LIMIT ?");
        values.push((query.limit.unwrap_or(100).clamp(1, 500) as i64).into());
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), |row| {
            Ok(CaptureGap {
                id: row.get(0)?,
                session_id: row.get(1)?,
                source: row.get(2)?,
                dropped_events: row.get::<_, i64>(3)? as u64,
                reason: row.get(4)?,
                observed_at: row.get(5)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn exchange(&self, id: &str) -> anyhow::Result<Option<ExchangeDetail>> {
        let connection = self.reader()?;
        let row = connection
            .query_row(
                "SELECT o.id, o.captured_at, req.method, req.url, req.host, req.path,
                        res.status, o.duration_ms, o.scope_state, req.body_size,
                        COALESCE(res.body_size, 0), req.headers_json,
                        COALESCE(res.headers_json, '[]'), req.body_hash, req.body_state,
                        req.body_reason, req.body_original_size, req.content_type,
                        res.body_hash, res.body_state, res.body_reason,
                        COALESCE(res.body_original_size, 0), res.content_type,
                        o.error, o.metadata_json
                 FROM observations o
                 JOIN request_variants req ON req.id = o.request_id
                 LEFT JOIN response_variants res ON res.id = o.response_id
                 WHERE o.id = ?1",
                [id],
                |row| {
                    let summary = row_to_summary(row)?;
                    Ok((
                        summary,
                        row.get::<_, String>(11)?,
                        row.get::<_, String>(12)?,
                        BodyRow {
                            hash: row.get(13)?,
                            state: row.get(14)?,
                            reason: row.get(15)?,
                            original_size: row.get::<_, i64>(16)? as u64,
                            mime: row.get(17)?,
                        },
                        BodyRow {
                            hash: row.get(18)?,
                            state: row
                                .get::<_, Option<String>>(19)?
                                .unwrap_or_else(|| "unavailable".into()),
                            reason: row.get(20)?,
                            original_size: row.get::<_, i64>(21)? as u64,
                            mime: row.get(22)?,
                        },
                        row.get::<_, Option<String>>(23)?,
                        row.get::<_, String>(24)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            summary,
            request_headers,
            response_headers,
            request_body,
            response_body,
            error,
            metadata,
        )) = row
        else {
            return Ok(None);
        };
        Ok(Some(ExchangeDetail {
            summary,
            request_headers: serde_json::from_str(&request_headers)?,
            response_headers: serde_json::from_str(&response_headers)?,
            request_body: self.body_detail(&connection, request_body)?,
            response_body: self.body_detail(&connection, response_body)?,
            error,
            metadata: serde_json::from_str(&metadata)?,
        }))
    }

    pub fn body(&self, hash: &str) -> anyhow::Result<Option<StoredBody>> {
        let connection = self.reader()?;
        let metadata = connection
            .query_row(
                "SELECT b.size, b.stored_size, b.codec, b.mime
                 FROM blobs b
                 WHERE b.hash = ?1
                   AND (
                        EXISTS (SELECT 1 FROM request_variants r WHERE r.body_hash = b.hash)
                     OR EXISTS (SELECT 1 FROM response_variants r WHERE r.body_hash = b.hash)
                     OR EXISTS (SELECT 1 FROM websocket_events we WHERE we.payload_hash = b.hash)
                   )",
                [hash],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((size, stored_size, codec, mime)) = metadata else {
            return Ok(None);
        };
        if size > MAX_BODY_BYTES as u64 {
            return Err(anyhow!("stored body exceeds the runtime body limit"));
        }
        let bytes = self.blobs.read(hash, &codec, MAX_BODY_BYTES + 1)?;
        if bytes.len() as u64 != size {
            return Err(anyhow!("stored body size does not match its metadata"));
        }
        Ok(Some(StoredBody {
            hash: hash.to_owned(),
            mime,
            captured_size: size,
            stored_size,
            codec,
            data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        }))
    }

    pub fn replay_source(&self, id: &str) -> anyhow::Result<Option<ReplaySourceRequest>> {
        let connection = self.reader()?;
        let row = connection
            .query_row(
                "SELECT req.method, req.url, req.headers_json, req.body_hash,
                        req.body_state, req.body_reason, req.body_original_size,
                        blobs.size, blobs.codec
                 FROM observations o
                 JOIN request_variants req ON req.id = o.request_id
                 LEFT JOIN blobs ON blobs.hash = req.body_hash
                 WHERE o.id = ?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)? as u64,
                        row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()?;
        let Some((method, url, headers, hash, state, reason, original_size, size, codec)) = row
        else {
            return Ok(None);
        };
        let bytes = match (hash, size, codec) {
            (Some(hash), Some(size), Some(codec)) => {
                if size > MAX_BODY_BYTES as u64 {
                    bail!("stored request body exceeds the replay limit");
                }
                let bytes = self.blobs.read(&hash, &codec, MAX_BODY_BYTES + 1)?;
                if bytes.len() as u64 != size {
                    bail!("stored request body size does not match blob metadata");
                }
                Some(bytes)
            }
            (None, None, None) => None,
            _ => bail!("stored request body metadata is incomplete"),
        };
        Ok(Some(ReplaySourceRequest {
            method,
            url,
            headers: serde_json::from_str(&headers)?,
            body: ReplaySourceBody {
                bytes,
                state,
                reason,
                original_size,
            },
        }))
    }

    pub fn ensure_active_session(&self, id: &str) -> anyhow::Result<()> {
        let connection = self.reader()?;
        session_scope_profile(&connection, id).map(|_| ())
    }

    pub fn replay_record(
        &self,
        producer_id: &str,
        sequence: u64,
    ) -> anyhow::Result<Option<ReplayRecord>> {
        let connection = self.reader()?;
        connection
            .query_row(
                "SELECT id, status, error, metadata_json
                 FROM observations
                 WHERE producer_id = ?1 AND sequence = ?2",
                params![producer_id, sequence as i64],
                |row| {
                    let metadata: String = row.get(3)?;
                    Ok(ReplayRecord {
                        observation_id: row.get(0)?,
                        status: row.get::<_, Option<i64>>(1)?.map(|value| value as u16),
                        error: row.get(2)?,
                        metadata: serde_json::from_str(&metadata).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn graph(&self, query: GraphQuery) -> anyhow::Result<GraphPage> {
        let connection = self.reader()?;
        let limit = query.limit.unwrap_or(100).clamp(1, 500) as i64;
        let mut sql = String::from(
            "SELECT n.id, n.kind, n.canonical_key, n.label, n.metadata_json,
                    MIN(ge.captured_at), MAX(ge.captured_at), COUNT(*)
             FROM graph_nodes n
             JOIN graph_node_evidence ge ON ge.node_id = n.id
             WHERE 1=1",
        );
        let mut values = Vec::<SqlValue>::new();
        if let Some(session_id) = query.session_id.as_deref() {
            sql.push_str(" AND ge.session_id = ?");
            values.push(session_id.to_owned().into());
        }
        if let Some(kind) = query
            .kind
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            sql.push_str(" AND n.kind = ?");
            values.push(kind.to_owned().into());
        }
        if let Some(text) = query
            .text
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            sql.push_str(" AND (n.label LIKE ? ESCAPE '\\' OR n.canonical_key LIKE ? ESCAPE '\\')");
            let like = format!("%{}%", escape_like(text));
            values.push(like.clone().into());
            values.push(like.into());
        }
        if query.in_scope_only {
            sql.push_str(" AND ge.scope_state = 'in_scope'");
        }
        sql.push_str(
            " GROUP BY n.id, n.kind, n.canonical_key, n.label, n.metadata_json
              ORDER BY MAX(ge.captured_at) DESC, n.id DESC LIMIT ?",
        );
        values.push(limit.into());
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), row_to_graph_node)?;
        Ok(GraphPage {
            nodes: rows.collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub fn graph_neighborhood(
        &self,
        id: &str,
        session_id: Option<&str>,
        in_scope_only: bool,
    ) -> anyhow::Result<Option<GraphNeighborhood>> {
        let connection = self.reader()?;
        let mut center_sql = String::from(
            "SELECT n.id, n.kind, n.canonical_key, n.label, n.metadata_json,
                    MIN(ge.captured_at), MAX(ge.captured_at), COUNT(*)
             FROM graph_nodes n
             JOIN graph_node_evidence ge ON ge.node_id = n.id
             WHERE n.id = ?",
        );
        let mut center_values = vec![SqlValue::from(id.to_owned())];
        if let Some(session_id) = session_id {
            center_sql.push_str(" AND ge.session_id = ?");
            center_values.push(session_id.to_owned().into());
        }
        if in_scope_only {
            center_sql.push_str(" AND ge.scope_state = 'in_scope'");
        }
        center_sql.push_str(" GROUP BY n.id, n.kind, n.canonical_key, n.label, n.metadata_json");
        let center = connection
            .query_row(
                &center_sql,
                params_from_iter(center_values),
                row_to_graph_node,
            )
            .optional()?;
        let Some(center) = center else {
            return Ok(None);
        };
        let mut neighbor_sql = String::from(
            "SELECT n.id, n.kind, n.canonical_key, n.label, n.metadata_json,
                    MIN(ge.captured_at), MAX(ge.captured_at), COUNT(*)
             FROM graph_nodes n
             JOIN graph_node_evidence ge ON ge.node_id = n.id
             WHERE n.id <> ?
               AND EXISTS (
                    SELECT 1 FROM graph_edges e
                    JOIN graph_edge_evidence gee
                      ON gee.src_id = e.src_id
                     AND gee.dst_id = e.dst_id
                     AND gee.edge_kind = e.kind
                    WHERE ((e.src_id = ? AND e.dst_id = n.id)
                        OR (e.dst_id = ? AND e.src_id = n.id))
                   ",
        );
        let mut neighbor_values = vec![
            SqlValue::from(id.to_owned()),
            SqlValue::from(id.to_owned()),
            SqlValue::from(id.to_owned()),
        ];
        if let Some(session_id) = session_id {
            neighbor_sql.push_str(" AND gee.session_id = ?");
            neighbor_values.push(session_id.to_owned().into());
        }
        if in_scope_only {
            neighbor_sql.push_str(" AND gee.scope_state = 'in_scope'");
        }
        neighbor_sql.push(')');
        if let Some(session_id) = session_id {
            neighbor_sql.push_str(" AND ge.session_id = ?");
            neighbor_values.push(session_id.to_owned().into());
        }
        if in_scope_only {
            neighbor_sql.push_str(" AND ge.scope_state = 'in_scope'");
        }
        neighbor_sql.push_str(
            " GROUP BY n.id, n.kind, n.canonical_key, n.label, n.metadata_json
              ORDER BY n.kind, n.label LIMIT 500",
        );
        let mut statement = connection.prepare(&neighbor_sql)?;
        let neighbors = statement
            .query_map(params_from_iter(neighbor_values), row_to_graph_node)?
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = Vec::with_capacity(neighbors.len() + 1);
        nodes.push(center);
        nodes.extend(neighbors);
        let node_ids = nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut edge_sql = String::from(
            "SELECT e.src_id, e.dst_id, e.kind,
                    MIN(gee.captured_at), MAX(gee.captured_at), COUNT(*)
             FROM graph_edges e
             JOIN graph_edge_evidence gee
               ON gee.src_id = e.src_id
              AND gee.dst_id = e.dst_id
              AND gee.edge_kind = e.kind
             WHERE (e.src_id = ? OR e.dst_id = ?)",
        );
        let mut edge_values = vec![SqlValue::from(id.to_owned()), SqlValue::from(id.to_owned())];
        if let Some(session_id) = session_id {
            edge_sql.push_str(" AND gee.session_id = ?");
            edge_values.push(session_id.to_owned().into());
        }
        if in_scope_only {
            edge_sql.push_str(" AND gee.scope_state = 'in_scope'");
        }
        edge_sql.push_str(
            " GROUP BY e.src_id, e.dst_id, e.kind
              ORDER BY MIN(gee.captured_at) LIMIT 1000",
        );
        let mut edge_statement = connection.prepare(&edge_sql)?;
        let edges = edge_statement
            .query_map(params_from_iter(edge_values), |row| {
                Ok(GraphEdge {
                    source_id: row.get(0)?,
                    target_id: row.get(1)?,
                    kind: row.get(2)?,
                    first_seen: row.get(3)?,
                    last_seen: row.get(4)?,
                    evidence_count: row.get::<_, i64>(5)? as u64,
                })
            })?
            .filter_map(|row| match row {
                Ok(edge)
                    if node_ids.contains(edge.source_id.as_str())
                        && node_ids.contains(edge.target_id.as_str()) =>
                {
                    Some(Ok(edge))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(GraphNeighborhood { nodes, edges }))
    }

    pub fn annotate(&self, input: AnnotationInput) -> anyhow::Result<Annotation> {
        let (body, tags) = normalize_annotation(input.body, input.tags)?;
        let connection = self.writer()?;
        let table = annotation_target_table(&input.target_type)?;
        let target_exists = connection
            .query_row(
                &format!("SELECT 1 FROM {table} WHERE id = ?1"),
                [&input.target_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !target_exists {
            return Err(invalid("annotation target does not exist"));
        }
        let timestamp = now();
        let annotation = Annotation {
            id: Uuid::now_v7().to_string(),
            target_type: input.target_type,
            target_id: input.target_id,
            body,
            tags,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        connection.execute(
            "INSERT INTO annotations(id, target_type, target_id, body, tags_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                annotation.id,
                annotation.target_type,
                annotation.target_id,
                annotation.body,
                serde_json::to_string(&annotation.tags)?,
                annotation.created_at,
                annotation.updated_at
            ],
        )?;
        Ok(annotation)
    }

    pub fn annotations(&self, query: AnnotationQuery) -> anyhow::Result<Vec<Annotation>> {
        if query.target_id.is_some() && query.target_type.is_none() {
            return Err(invalid("targetType is required when targetId is supplied"));
        }
        if let Some(target_type) = query.target_type.as_deref() {
            annotation_target_table(target_type)?;
        }
        let limit = query.limit.unwrap_or(200).clamp(1, 500) as i64;
        let connection = self.reader()?;
        let mut statement = connection.prepare(
            "SELECT id, target_type, target_id, body, tags_json, created_at, updated_at
             FROM annotations
             WHERE (?1 IS NULL OR target_type = ?1)
               AND (?2 IS NULL OR target_id = ?2)
             ORDER BY updated_at DESC, id DESC
             LIMIT ?3",
        )?;
        statement
            .query_map(
                params![query.target_type, query.target_id, limit],
                row_to_annotation,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn annotation(&self, id: &str) -> anyhow::Result<Option<Annotation>> {
        let connection = self.reader()?;
        connection
            .query_row(
                "SELECT id, target_type, target_id, body, tags_json, created_at, updated_at
                 FROM annotations WHERE id = ?1",
                [id],
                row_to_annotation,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn update_annotation(
        &self,
        id: &str,
        input: UpdateAnnotation,
    ) -> anyhow::Result<Option<Annotation>> {
        let (body, tags) = normalize_annotation(input.body, input.tags)?;
        let connection = self.writer()?;
        let updated_at = now();
        let updated = connection.execute(
            "UPDATE annotations
             SET body = ?2, tags_json = ?3, updated_at = ?4
             WHERE id = ?1",
            params![id, body, serde_json::to_string(&tags)?, updated_at],
        )?;
        if updated == 0 {
            return Ok(None);
        }
        connection
            .query_row(
                "SELECT id, target_type, target_id, body, tags_json, created_at, updated_at
                 FROM annotations WHERE id = ?1",
                [id],
                row_to_annotation,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn delete_annotation(&self, id: &str) -> anyhow::Result<bool> {
        let connection = self.writer()?;
        Ok(connection.execute("DELETE FROM annotations WHERE id = ?1", [id])? > 0)
    }

    fn ingest_http_event(
        &self,
        connection: &mut Connection,
        producer_id: &str,
        session_id: &str,
        scope: &ScopeProfile,
        event: HttpCaptureEvent,
    ) -> anyhow::Result<IngestResult> {
        let event_fingerprint = event_fingerprint(&event)?;
        if let Some((existing, existing_fingerprint)) =
            existing_event(connection, "observations", producer_id, event.sequence)?
        {
            if existing_fingerprint != event_fingerprint {
                return Err(invalid(format!(
                    "sequence {} was already used for different HTTP content",
                    event.sequence
                )));
            }
            return Ok(IngestResult {
                sequence: event.sequence,
                status: IngestStatus::Duplicate,
                observation_id: Some(existing),
                error: None,
            });
        }
        let url = Url::parse(&event.request.url)
            .map_err(|error| invalid(format!("request URL is invalid: {error}")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid("request URL must use http or https"));
        }
        let host = url
            .host_str()
            .ok_or_else(|| invalid("request URL has no host"))?
            .to_ascii_lowercase();
        let request_body = self.decode_body(event.request.body.as_ref())?;
        let response_body = self.decode_body(
            event
                .response
                .as_ref()
                .and_then(|response| response.body.as_ref()),
        )?;
        let captured_at = normalized_time(event.captured_at.as_deref())?;
        let received_at = now();
        let decision = scope.evaluate(&url);
        let request_content_type = content_type(&event.request.headers);
        let request_parameters = body_parameters(
            request_content_type.as_deref(),
            request_body.bytes.as_deref(),
        );
        let request_fingerprint = request_fingerprint(
            &event.request.method,
            event.request.url.as_str(),
            &event.request.headers,
            request_body.stored.as_ref().map(|blob| blob.hash.as_str()),
            &request_body.state,
            request_body.original_size,
        );
        let response_fingerprint = event.response.as_ref().map(|response| {
            response_fingerprint(
                response.status,
                &response.headers,
                response_body.stored.as_ref().map(|blob| blob.hash.as_str()),
                &response_body.state,
                response_body.original_size,
            )
        });
        let observation_id = Uuid::now_v7().to_string();
        let transaction = connection.transaction()?;
        insert_blob_metadata(
            &transaction,
            request_body.stored.as_ref(),
            request_content_type.as_deref(),
            &received_at,
        )?;
        insert_blob_metadata(
            &transaction,
            response_body.stored.as_ref(),
            event
                .response
                .as_ref()
                .and_then(|response| content_type(&response.headers))
                .as_deref(),
            &received_at,
        )?;
        let request_id = upsert_request(
            &transaction,
            &request_fingerprint,
            &event.request,
            &url,
            &host,
            &request_body,
            &received_at,
        )?;
        let response_id = match (&event.response, response_fingerprint) {
            (Some(response), Some(fingerprint)) => Some(upsert_response(
                &transaction,
                &fingerprint,
                response,
                &response_body,
                &received_at,
            )?),
            _ => None,
        };
        transaction.execute(
            "INSERT INTO observations(
                id, session_id, producer_id, sequence, captured_at, received_at,
                duration_ms, request_id, response_id, status, scope_state,
                scope_rule_id, scope_reason, error, tab_id, initiator,
                parent_sequence, metadata_json, event_fingerprint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
            params![
                observation_id,
                session_id,
                producer_id,
                event.sequence as i64,
                captured_at,
                received_at,
                event.duration_ms.map(|value| value as i64),
                request_id,
                response_id,
                event.response.as_ref().map(|response| response.status as i64),
                decision.state.as_str(),
                decision.rule_id,
                decision.reason,
                event.error,
                event.tab_id,
                event.initiator,
                event.parent_sequence.map(|value| value as i64),
                serde_json::to_string(&event.metadata)?,
                event_fingerprint
            ],
        )?;
        if self.fts_enabled {
            let body_preview = index_preview(&request_body, &response_body);
            transaction.execute(
                "INSERT INTO traffic_fts(observation_id, url, headers, body_preview)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    observation_id,
                    event.request.url,
                    format!(
                        "{}\n{}",
                        headers_text(&event.request.headers),
                        event
                            .response
                            .as_ref()
                            .map(|response| headers_text(&response.headers))
                            .unwrap_or_default()
                    ),
                    body_preview
                ],
            )?;
        }
        graph::project_http(
            &transaction,
            &observation_id,
            session_id,
            &captured_at,
            &event.request.method,
            &url,
            &request_parameters,
            decision.state.as_str(),
        )?;
        transaction.commit()?;
        Ok(IngestResult {
            sequence: event.sequence,
            status: IngestStatus::Accepted,
            observation_id: Some(observation_id),
            error: None,
        })
    }

    fn ingest_websocket_event(
        &self,
        connection: &mut Connection,
        producer_id: &str,
        session_id: &str,
        scope: &ScopeProfile,
        event: WebSocketFrameEvent,
    ) -> anyhow::Result<IngestResult> {
        let event_fingerprint = event_fingerprint(&event)?;
        if let Some((existing, existing_fingerprint)) =
            existing_event(connection, "websocket_events", producer_id, event.sequence)?
        {
            if existing_fingerprint != event_fingerprint {
                return Err(invalid(format!(
                    "sequence {} was already used for different WebSocket content",
                    event.sequence
                )));
            }
            return Ok(IngestResult {
                sequence: event.sequence,
                status: IngestStatus::Duplicate,
                observation_id: Some(existing),
                error: None,
            });
        }
        if matches!(event.kind, WebSocketEventKind::Frame)
            && (event.direction.is_none() || event.opcode.is_none())
        {
            return Err(invalid("WebSocket frames require direction and opcode"));
        }
        let payload = self.decode_body(event.payload.as_ref())?;
        let captured_at = normalized_time(event.captured_at.as_deref())?;
        let parsed_url = event
            .url
            .as_deref()
            .map(Url::parse)
            .transpose()
            .map_err(|error| invalid(format!("WebSocket URL is invalid: {error}")))?;
        if parsed_url
            .as_ref()
            .is_some_and(|url| !matches!(url.scheme(), "ws" | "wss"))
        {
            return Err(invalid("WebSocket URL must use ws or wss"));
        }
        let decision = parsed_url
            .as_ref()
            .map(|url| scope.evaluate(url))
            .unwrap_or(ScopeDecision {
                state: ScopeState::Unknown,
                rule_id: None,
                reason: "connection URL has not been observed yet".into(),
            });
        let event_id = Uuid::now_v7().to_string();
        let connection_id = Uuid::now_v7().to_string();
        let connection_state = if matches!(
            event.kind,
            WebSocketEventKind::Close | WebSocketEventKind::Error
        ) {
            event.kind.as_str()
        } else {
            "open"
        };
        let closed_at = matches!(
            event.kind,
            WebSocketEventKind::Close | WebSocketEventKind::Error
        )
        .then_some(captured_at.as_str());
        let transaction = connection.transaction()?;
        insert_blob_metadata(&transaction, payload.stored.as_ref(), None, &captured_at)?;
        transaction.execute(
            "INSERT INTO websocket_connections(
                id, session_id, producer_id, source_connection_id, url,
                subprotocol, opened_at, closed_at, close_code, close_reason,
                scope_state, scope_rule_id, scope_reason, state
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(session_id, producer_id, source_connection_id) DO UPDATE SET
                url = COALESCE(excluded.url, websocket_connections.url),
                subprotocol = COALESCE(excluded.subprotocol, websocket_connections.subprotocol),
                opened_at = CASE
                    WHEN websocket_connections.opened_at IS NULL THEN excluded.opened_at
                    WHEN julianday(excluded.opened_at) < julianday(websocket_connections.opened_at)
                    THEN excluded.opened_at ELSE websocket_connections.opened_at END,
                closed_at = CASE
                    WHEN excluded.closed_at IS NULL THEN websocket_connections.closed_at
                    WHEN websocket_connections.closed_at IS NULL THEN excluded.closed_at
                    WHEN julianday(excluded.closed_at) > julianday(websocket_connections.closed_at)
                    THEN excluded.closed_at ELSE websocket_connections.closed_at END,
                close_code = COALESCE(excluded.close_code, websocket_connections.close_code),
                close_reason = COALESCE(excluded.close_reason, websocket_connections.close_reason),
                scope_state = CASE
                    WHEN excluded.scope_state <> 'unknown' THEN excluded.scope_state
                    ELSE websocket_connections.scope_state END,
                scope_rule_id = CASE
                    WHEN excluded.scope_state <> 'unknown' THEN excluded.scope_rule_id
                    ELSE websocket_connections.scope_rule_id END,
                scope_reason = CASE
                    WHEN excluded.scope_state <> 'unknown' THEN excluded.scope_reason
                    ELSE websocket_connections.scope_reason END,
                state = CASE
                    WHEN excluded.state IN ('close', 'error') THEN excluded.state
                    ELSE websocket_connections.state END",
            params![
                connection_id,
                session_id,
                producer_id,
                event.connection_id,
                event.url,
                event.subprotocol,
                captured_at,
                closed_at,
                event.close_code.map(i64::from),
                event.close_reason,
                decision.state.as_str(),
                decision.rule_id,
                decision.reason,
                connection_state
            ],
        )?;
        let (internal_connection_id, connection_url, connection_scope): (
            String,
            Option<String>,
            String,
        ) = transaction.query_row(
            "SELECT id, url, scope_state FROM websocket_connections
             WHERE session_id = ?1 AND producer_id = ?2 AND source_connection_id = ?3",
            params![session_id, producer_id, event.connection_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        transaction.execute(
            "INSERT INTO websocket_events(
                id, connection_id, producer_id, sequence, kind, captured_at,
                direction, opcode, payload_hash, payload_state, payload_reason,
                payload_size, payload_original_size, error, event_fingerprint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                event_id,
                internal_connection_id,
                producer_id,
                event.sequence as i64,
                event.kind.as_str(),
                captured_at,
                event.direction.map(|direction| direction.as_str()),
                event.opcode.map(i64::from),
                payload.stored.as_ref().map(|blob| blob.hash.as_str()),
                payload.state,
                payload.reason,
                payload
                    .bytes
                    .as_ref()
                    .map_or(0_i64, |bytes| bytes.len() as i64),
                payload.original_size as i64,
                event.error,
                event_fingerprint
            ],
        )?;
        if self.fts_enabled {
            let text = payload
                .bytes
                .as_deref()
                .and_then(|bytes| safe_utf8_prefix(bytes, MAX_TEXT_INDEX_BYTES))
                .unwrap_or_default();
            transaction.execute(
                "INSERT INTO websocket_fts(event_id, connection_id, payload) VALUES (?1, ?2, ?3)",
                params![event_id, internal_connection_id, text],
            )?;
        }
        if let Some(url) = connection_url.as_deref().map(Url::parse).transpose()? {
            graph::project_websocket(
                &transaction,
                &event_id,
                session_id,
                &captured_at,
                &url,
                &connection_scope,
            )?;
        }
        transaction.commit()?;
        Ok(IngestResult {
            sequence: event.sequence,
            status: IngestStatus::Accepted,
            observation_id: Some(event_id),
            error: None,
        })
    }

    fn decode_body(&self, body: Option<&CapturedBody>) -> anyhow::Result<DecodedBody> {
        let Some(body) = body else {
            return Ok(DecodedBody {
                bytes: None,
                state: "unavailable".into(),
                reason: None,
                stored: None,
                original_size: 0,
            });
        };
        let bytes = match body.encoding {
            BodyEncoding::Utf8 => body.data.as_bytes().to_vec(),
            BodyEncoding::Base64 => base64::engine::general_purpose::STANDARD
                .decode(&body.data)
                .map_err(|_| invalid("body is not valid base64"))?,
        };
        if bytes.len() > MAX_BODY_BYTES {
            return Err(invalid(format!(
                "body exceeds the {MAX_BODY_BYTES} byte ingest limit"
            )));
        }
        let original_size = body.original_size.unwrap_or(bytes.len() as u64);
        if original_size < bytes.len() as u64 {
            return Err(invalid(
                "body originalSize cannot be smaller than the captured body",
            ));
        }
        if body.state == BodyState::Complete && original_size != bytes.len() as u64 {
            return Err(invalid(
                "a complete body must report its exact captured size",
            ));
        }
        let stored = (!bytes.is_empty())
            .then(|| self.blobs.put(&bytes))
            .transpose()?;
        Ok(DecodedBody {
            original_size,
            bytes: Some(bytes),
            state: body.state.as_str().into(),
            reason: body.reason.clone(),
            stored,
        })
    }

    fn body_detail(
        &self,
        connection: &Connection,
        row: BodyRow,
    ) -> anyhow::Result<Option<BodyDetail>> {
        let Some(hash) = row.hash else {
            return Ok(
                (row.state != "unavailable" || row.reason.is_some()).then_some(BodyDetail {
                    state: row.state,
                    reason: row.reason,
                    captured_size: 0,
                    original_size: row.original_size,
                    mime: row.mime,
                    text_preview: None,
                    hash: None,
                }),
            );
        };
        let (size, codec): (i64, String) = connection.query_row(
            "SELECT size, codec FROM blobs WHERE hash = ?1",
            [&hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let bytes = self.blobs.read(&hash, &codec, MAX_BODY_PREVIEW_BYTES)?;
        let text_preview = is_textual(row.mime.as_deref())
            .then(|| String::from_utf8(bytes).ok())
            .flatten();
        Ok(Some(BodyDetail {
            state: row.state,
            reason: row.reason,
            captured_size: size as u64,
            original_size: row.original_size,
            mime: row.mime,
            text_preview,
            hash: Some(hash),
        }))
    }

    fn writer(&self) -> anyhow::Result<MutexGuard<'_, Connection>> {
        self.writer
            .lock()
            .map_err(|_| anyhow!("writer lock is poisoned"))
    }

    fn reader(&self) -> anyhow::Result<Connection> {
        let connection = Connection::open(&self.database_path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA query_only=ON;")?;
        Ok(connection)
    }
}

fn maintain_blobs(
    connection: &mut Connection,
    blobs: &BlobStore,
    orphan_grace: Duration,
) -> anyhow::Result<()> {
    let removed_metadata = connection.execute(
        "DELETE FROM blobs
         WHERE NOT EXISTS (
             SELECT 1 FROM request_variants WHERE request_variants.body_hash = blobs.hash
         ) AND NOT EXISTS (
             SELECT 1 FROM response_variants WHERE response_variants.body_hash = blobs.hash
         ) AND NOT EXISTS (
             SELECT 1 FROM websocket_events WHERE websocket_events.payload_hash = blobs.hash
         )",
        [],
    )?;
    let referenced_hashes = {
        let mut statement = connection.prepare("SELECT hash FROM blobs")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<HashSet<_>, _>>()?
    };
    let stats = blobs.sweep_orphans(&referenced_hashes, orphan_grace)?;
    if removed_metadata > 0 || stats.removed_orphans > 0 || stats.removed_parts > 0 {
        tracing::info!(
            removed_metadata,
            removed_orphans = stats.removed_orphans,
            removed_parts = stats.removed_parts,
            "cleaned stale web capture blobs"
        );
    }
    Ok(())
}

struct BodyRow {
    hash: Option<String>,
    state: String,
    reason: Option<String>,
    mime: Option<String>,
    original_size: u64,
}

fn validate_scope_input(input: &SaveScopeProfile) -> anyhow::Result<()> {
    let name = input.name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(invalid("scope profile name must contain 1 to 128 bytes"));
    }
    if input.rules.len() > 512 {
        return Err(invalid("scope profile cannot contain more than 512 rules"));
    }
    for rule in &input.rules {
        if rule.pattern.trim().is_empty() || rule.pattern.len() > 4_096 {
            return Err(invalid("scope rule patterns must contain 1 to 4096 bytes"));
        }
    }
    let validation_rules = input
        .rules
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, rule)| (format!("validation-{index}"), rule));
    ScopeProfile::compile(validation_rules)
        .map_err(|error| invalid(format!("invalid scope rule: {error}")))?;
    Ok(())
}

fn load_scope_profile_view(
    connection: &Connection,
    id: &str,
) -> anyhow::Result<Option<ScopeProfileView>> {
    let profile = connection
        .query_row(
            "SELECT id, name, version, created_at, updated_at
             FROM scope_profiles WHERE id = ?1",
            [id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?;
    let Some((id, name, version, created_at, updated_at)) = profile else {
        return Ok(None);
    };
    let rules = {
        let mut statement = connection.prepare(
            "SELECT id, action, matcher, pattern, enabled, order_index
             FROM scope_rules WHERE profile_id = ?1 ORDER BY order_index",
        )?;
        statement
            .query_map([&id], |row| {
                let action = match row.get::<_, String>(1)?.as_str() {
                    "include" => ScopeAction::Include,
                    "exclude" => ScopeAction::Exclude,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                let matcher = match row.get::<_, String>(2)?.as_str() {
                    "host" => ScopeMatcher::Host,
                    "host_wildcard" => ScopeMatcher::HostWildcard,
                    "url_wildcard" => ScopeMatcher::UrlWildcard,
                    "regex" => ScopeMatcher::Regex,
                    "cidr" => ScopeMatcher::Cidr,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                Ok(ScopeRule {
                    id: row.get(0)?,
                    action,
                    matcher,
                    pattern: row.get(3)?,
                    enabled: row.get(4)?,
                    order_index: row.get::<_, i64>(5)? as u32,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok(Some(ScopeProfileView {
        id,
        name,
        version,
        created_at,
        updated_at,
        rules,
    }))
}

fn annotation_target_table(target_type: &str) -> anyhow::Result<&'static str> {
    match target_type {
        "observation" => Ok("observations"),
        "websocket_event" => Ok("websocket_events"),
        "graph_node" => Ok("graph_nodes"),
        _ => Err(invalid("unsupported annotation target type")),
    }
}

fn normalize_annotation(body: String, tags: Vec<String>) -> anyhow::Result<(String, Vec<String>)> {
    let body = body.trim().to_owned();
    if body.len() > 65_536 {
        return Err(invalid("annotation text cannot exceed 65536 bytes"));
    }
    if tags.len() > 64 {
        return Err(invalid("annotation cannot contain more than 64 tags"));
    }
    let mut seen = HashSet::new();
    let mut normalized_tags = Vec::with_capacity(tags.len());
    for tag in tags {
        let tag = tag.trim();
        if tag.is_empty() || tag.len() > 128 {
            return Err(invalid("annotation tags must contain 1 to 128 bytes"));
        }
        if seen.insert(tag.to_owned()) {
            normalized_tags.push(tag.to_owned());
        }
    }
    if body.is_empty() && normalized_tags.is_empty() {
        return Err(invalid("annotation must contain text or tags"));
    }
    Ok((body, normalized_tags))
}

fn ensure_default_scope(transaction: &Transaction<'_>) -> anyhow::Result<()> {
    let timestamp = now();
    transaction.execute(
        "INSERT INTO scope_profiles(id, name, version, created_at, updated_at)
         VALUES ('default', 'Default', 1, ?1, ?1)
         ON CONFLICT(id) DO NOTHING",
        [timestamp],
    )?;
    Ok(())
}

fn session_scope_profile(connection: &Connection, session_id: &str) -> anyhow::Result<String> {
    let session = connection
        .query_row(
            "SELECT scope_profile_id, ended_at FROM capture_sessions WHERE id = ?1",
            [session_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
        .ok_or_else(|| invalid(format!("capture session {session_id} does not exist")))?;
    if session.1.is_some() {
        return Err(invalid(format!("capture session {session_id} has ended")));
    }
    Ok(session.0)
}

fn load_scope_profile(connection: &Connection, profile_id: &str) -> anyhow::Result<ScopeProfile> {
    let mut statement = connection.prepare(
        "SELECT id, action, matcher, pattern, enabled
         FROM scope_rules WHERE profile_id = ?1 ORDER BY order_index",
    )?;
    let rows = statement.query_map([profile_id], |row| {
        let action: String = row.get(1)?;
        let matcher: String = row.get(2)?;
        Ok((
            row.get::<_, String>(0)?,
            ScopeRuleInput {
                action: match action.as_str() {
                    "include" => ScopeAction::Include,
                    "exclude" => ScopeAction::Exclude,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
                matcher: match matcher.as_str() {
                    "host" => ScopeMatcher::Host,
                    "host_wildcard" => ScopeMatcher::HostWildcard,
                    "url_wildcard" => ScopeMatcher::UrlWildcard,
                    "regex" => ScopeMatcher::Regex,
                    "cidr" => ScopeMatcher::Cidr,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                },
                pattern: row.get(3)?,
                enabled: row.get(4)?,
            },
        ))
    })?;
    ScopeProfile::compile(rows.collect::<Result<Vec<_>, _>>()?)
}

fn existing_event(
    connection: &Connection,
    table: &str,
    producer_id: &str,
    sequence: u64,
) -> anyhow::Result<Option<(String, String)>> {
    if !matches!(table, "observations" | "websocket_events") {
        bail!("unsupported event table");
    }
    connection
        .query_row(
            &format!(
                "SELECT id, event_fingerprint FROM {table} WHERE producer_id = ?1 AND sequence = ?2"
            ),
            params![producer_id, sequence as i64],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
}

fn event_fingerprint(event: &impl serde::Serialize) -> anyhow::Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(event)?)
        .to_hex()
        .to_string())
}

fn insert_blob_metadata(
    transaction: &Transaction<'_>,
    blob: Option<&StoredBlob>,
    mime: Option<&str>,
    created_at: &str,
) -> anyhow::Result<()> {
    let Some(blob) = blob else { return Ok(()) };
    transaction.execute(
        "INSERT INTO blobs(hash, size, stored_size, codec, mime, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(hash) DO UPDATE SET mime = COALESCE(blobs.mime, excluded.mime)",
        params![
            blob.hash,
            blob.size as i64,
            blob.stored_size as i64,
            blob.codec,
            mime,
            created_at
        ],
    )?;
    Ok(())
}

fn upsert_request(
    transaction: &Transaction<'_>,
    fingerprint: &str,
    request: &CapturedRequest,
    url: &Url,
    host: &str,
    body: &DecodedBody,
    created_at: &str,
) -> anyhow::Result<String> {
    let id = Uuid::now_v7().to_string();
    transaction.execute(
        "INSERT INTO request_variants(
            id, fingerprint, method, url, scheme, host, port, path, raw_query,
            headers_json, body_hash, body_state, body_reason, body_size,
            body_original_size, content_type, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
         ON CONFLICT(fingerprint) DO NOTHING",
        params![
            id,
            fingerprint,
            request.method.to_ascii_uppercase(),
            request.url,
            url.scheme(),
            host,
            url.port(),
            if url.path().is_empty() {
                "/"
            } else {
                url.path()
            },
            url.query(),
            serde_json::to_string(&request.headers)?,
            body.stored.as_ref().map(|blob| blob.hash.as_str()),
            body.state,
            body.reason,
            body.bytes
                .as_ref()
                .map_or(0_i64, |bytes| bytes.len() as i64),
            body.original_size as i64,
            content_type(&request.headers),
            created_at
        ],
    )?;
    transaction
        .query_row(
            "SELECT id FROM request_variants WHERE fingerprint = ?1",
            [fingerprint],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn upsert_response(
    transaction: &Transaction<'_>,
    fingerprint: &str,
    response: &CapturedResponse,
    body: &DecodedBody,
    created_at: &str,
) -> anyhow::Result<String> {
    let id = Uuid::now_v7().to_string();
    transaction.execute(
        "INSERT INTO response_variants(
            id, fingerprint, status, headers_json, body_hash, body_state,
            body_reason, body_size, body_original_size, content_type, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(fingerprint) DO NOTHING",
        params![
            id,
            fingerprint,
            response.status,
            serde_json::to_string(&response.headers)?,
            body.stored.as_ref().map(|blob| blob.hash.as_str()),
            body.state,
            body.reason,
            body.bytes
                .as_ref()
                .map_or(0_i64, |bytes| bytes.len() as i64),
            body.original_size as i64,
            content_type(&response.headers),
            created_at
        ],
    )?;
    transaction
        .query_row(
            "SELECT id FROM response_variants WHERE fingerprint = ?1",
            [fingerprint],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn validate_batch(producer_id: &str, session_id: &str, len: usize) -> anyhow::Result<()> {
    if producer_id.trim().is_empty() || session_id.trim().is_empty() {
        return Err(invalid("producerId and sessionId are required"));
    }
    if len == 0 || len > MAX_BATCH_ITEMS {
        return Err(invalid(format!(
            "batch must contain between 1 and {MAX_BATCH_ITEMS} items"
        )));
    }
    Ok(())
}

fn normalized_time(value: Option<&str>) -> anyhow::Result<String> {
    match value {
        Some(value) => {
            let parsed = OffsetDateTime::parse(value, &Rfc3339)
                .map_err(|_| invalid("timestamp must be RFC 3339"))?;
            parsed
                .to_offset(UtcOffset::UTC)
                .format(&Rfc3339)
                .map_err(|_| invalid("timestamp could not be normalized"))
        }
        None => Ok(now()),
    }
}

fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("RFC 3339 formatting cannot fail")
}

fn headers_text(headers: &[HeaderField]) -> String {
    headers
        .iter()
        .map(|header| format!("{}: {}", header.name, header.value))
        .collect::<Vec<_>>()
        .join("\n")
}

fn index_preview(request: &DecodedBody, response: &DecodedBody) -> String {
    [request, response]
        .into_iter()
        .filter_map(|body| body.bytes.as_deref())
        .filter_map(|bytes| safe_utf8_prefix(bytes, MAX_TEXT_INDEX_BYTES))
        .collect::<Vec<_>>()
        .join("\n")
}

fn safe_utf8_prefix(bytes: &[u8], limit: usize) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    if text.len() <= limit {
        return Some(text);
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Some(&text[..end])
}

fn row_to_capture_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<CaptureSession> {
    Ok(CaptureSession {
        id: row.get(0)?,
        label: row.get(1)?,
        source: row.get(2)?,
        started_at: row.get(3)?,
        ended_at: row.get(4)?,
        scope_profile_id: row.get(5)?,
    })
}

fn row_to_annotation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Annotation> {
    let tags_json = row.get::<_, String>(4)?;
    let tags = serde_json::from_str(&tags_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, SqlType::Text, Box::new(error))
    })?;
    Ok(Annotation {
        id: row.get(0)?,
        target_type: row.get(1)?,
        target_id: row.get(2)?,
        body: row.get(3)?,
        tags,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn row_to_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrafficSummary> {
    Ok(TrafficSummary {
        id: row.get(0)?,
        captured_at: row.get(1)?,
        method: row.get(2)?,
        url: row.get(3)?,
        host: row.get(4)?,
        path: row.get(5)?,
        status: row.get(6)?,
        duration_ms: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
        scope: row.get(8)?,
        request_body_size: row.get::<_, i64>(9)? as u64,
        response_body_size: row.get::<_, i64>(10)? as u64,
    })
}

fn row_to_websocket_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<WebSocketEventSummary> {
    Ok(WebSocketEventSummary {
        id: row.get(0)?,
        connection_id: row.get(1)?,
        captured_at: row.get(2)?,
        kind: row.get(3)?,
        direction: row.get(4)?,
        opcode: row.get::<_, Option<i64>>(5)?.map(|value| value as u8),
        url: row.get(6)?,
        subprotocol: row.get(7)?,
        connection_state: row.get(8)?,
        close_code: row.get::<_, Option<i64>>(9)?.map(|value| value as u16),
        close_reason: row.get(10)?,
        scope: row.get(11)?,
        payload_size: row.get::<_, i64>(12)? as u64,
        payload_original_size: row.get::<_, i64>(13)? as u64,
        error: row.get(14)?,
    })
}

fn row_to_graph_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<GraphNode> {
    let metadata: String = row.get(4)?;
    Ok(GraphNode {
        id: row.get(0)?,
        kind: row.get(1)?,
        canonical_key: row.get(2)?,
        label: row.get(3)?,
        metadata: serde_json::from_str(&metadata).unwrap_or(serde_json::Value::Null),
        first_seen: row.get(5)?,
        last_seen: row.get(6)?,
        evidence_count: row.get::<_, i64>(7)? as u64,
    })
}

fn make_cursor(captured_at: &str, id: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{captured_at}\n{id}"))
}

fn parse_cursor(cursor: &str) -> anyhow::Result<(String, String)> {
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid("cursor is invalid"))?;
    let decoded = String::from_utf8(decoded).map_err(|_| invalid("cursor is invalid"))?;
    let (captured_at, id) = decoded
        .split_once('\n')
        .ok_or_else(|| invalid("cursor is invalid"))?;
    Ok((captured_at.into(), id.into()))
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn is_textual(mime: Option<&str>) -> bool {
    mime.is_some_and(|mime| {
        mime.starts_with("text/")
            || mime.contains("json")
            || mime.contains("xml")
            || mime.contains("javascript")
            || mime.contains("x-www-form-urlencoded")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(url: &str) -> CapturedRequest {
        CapturedRequest {
            method: "GET".into(),
            url: url.into(),
            headers: vec![],
            body: None,
        }
    }

    fn event(sequence: u64, url: &str, captured_at: &str) -> HttpCaptureEvent {
        HttpCaptureEvent {
            sequence,
            captured_at: Some(captured_at.into()),
            duration_ms: Some(12),
            request: request(url),
            response: Some(CapturedResponse {
                status: 200,
                headers: vec![],
                body: None,
            }),
            error: None,
            tab_id: None,
            initiator: None,
            parent_sequence: None,
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn ingest_is_idempotent_and_keeps_distinct_occurrences() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let session = store
            .create_session(CreateSession {
                label: "test".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let event = HttpCaptureEvent {
            sequence: 1,
            captured_at: None,
            duration_ms: Some(12),
            request: request("https://example.com/users?id=1"),
            response: Some(CapturedResponse {
                status: 200,
                headers: vec![],
                body: None,
            }),
            error: None,
            tab_id: None,
            initiator: None,
            parent_sequence: None,
            metadata: serde_json::json!({}),
        };
        let first = store
            .ingest_http(HttpIngestBatch {
                producer_id: "browser-1".into(),
                session_id: session.id.clone(),
                events: vec![event.clone()],
            })
            .unwrap();
        let duplicate = store
            .ingest_http(HttpIngestBatch {
                producer_id: "browser-1".into(),
                session_id: session.id.clone(),
                events: vec![event.clone()],
            })
            .unwrap();
        let second = store
            .ingest_http(HttpIngestBatch {
                producer_id: "browser-1".into(),
                session_id: session.id,
                events: vec![HttpCaptureEvent {
                    sequence: 2,
                    ..event
                }],
            })
            .unwrap();
        assert!(matches!(first[0].status, IngestStatus::Accepted));
        assert!(matches!(duplicate[0].status, IngestStatus::Duplicate));
        assert!(matches!(second[0].status, IngestStatus::Accepted));
        assert_eq!(
            store.traffic(TrafficQuery::default()).unwrap().items.len(),
            2
        );
    }

    #[test]
    fn cursor_is_stable_and_round_trips() {
        let cursor = make_cursor("2026-11-20T10:00:00Z", "id");
        assert_eq!(
            parse_cursor(&cursor).unwrap(),
            ("2026-11-20T10:00:00Z".into(), "id".into())
        );
    }

    #[test]
    fn session_filters_isolate_traffic_and_graph_aggregates() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let first = store
            .create_session(CreateSession {
                label: "first".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let second = store
            .create_session(CreateSession {
                label: "second".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();

        store
            .ingest_http(HttpIngestBatch {
                producer_id: "producer-first".into(),
                session_id: first.id.clone(),
                events: vec![
                    event(
                        1,
                        "https://shared.example/items?first=1",
                        "2026-11-20T10:00:00Z",
                    ),
                    event(
                        2,
                        "https://shared.example/items?first=2",
                        "2026-11-20T10:01:00Z",
                    ),
                ],
            })
            .unwrap();
        store
            .ingest_http(HttpIngestBatch {
                producer_id: "producer-second".into(),
                session_id: second.id.clone(),
                events: vec![event(
                    1,
                    "https://shared.example/items?second=1",
                    "2026-11-20T11:00:00Z",
                )],
            })
            .unwrap();

        let first_traffic = store
            .traffic(TrafficQuery {
                session_id: Some(first.id.clone()),
                ..TrafficQuery::default()
            })
            .unwrap();
        let second_traffic = store
            .traffic(TrafficQuery {
                session_id: Some(second.id.clone()),
                ..TrafficQuery::default()
            })
            .unwrap();
        assert_eq!(first_traffic.items.len(), 2);
        assert_eq!(second_traffic.items.len(), 1);

        let first_graph = store
            .graph(GraphQuery {
                session_id: Some(first.id.clone()),
                kind: Some("host".into()),
                ..GraphQuery::default()
            })
            .unwrap();
        let second_graph = store
            .graph(GraphQuery {
                session_id: Some(second.id.clone()),
                kind: Some("host".into()),
                ..GraphQuery::default()
            })
            .unwrap();
        assert_eq!(first_graph.nodes.len(), 1);
        assert_eq!(first_graph.nodes[0].evidence_count, 2);
        assert_eq!(first_graph.nodes[0].first_seen, "2026-11-20T10:00:00Z");
        assert_eq!(first_graph.nodes[0].last_seen, "2026-11-20T10:01:00Z");
        assert_eq!(second_graph.nodes.len(), 1);
        assert_eq!(second_graph.nodes[0].evidence_count, 1);
        assert_eq!(second_graph.nodes[0].first_seen, "2026-11-20T11:00:00Z");
        assert_eq!(second_graph.nodes[0].last_seen, "2026-11-20T11:00:00Z");

        let operation = store
            .graph(GraphQuery {
                session_id: Some(first.id.clone()),
                kind: Some("operation".into()),
                ..GraphQuery::default()
            })
            .unwrap()
            .nodes
            .pop()
            .unwrap();
        let first_neighbors = store
            .graph_neighborhood(&operation.id, Some(&first.id), true)
            .unwrap()
            .unwrap();
        let second_neighbors = store
            .graph_neighborhood(&operation.id, Some(&second.id), true)
            .unwrap()
            .unwrap();
        assert!(
            first_neighbors
                .nodes
                .iter()
                .any(|node| node.kind == "parameter" && node.label == "first")
        );
        assert!(
            first_neighbors
                .nodes
                .iter()
                .all(|node| node.kind != "parameter" || node.label != "second")
        );
        assert!(
            second_neighbors
                .nodes
                .iter()
                .any(|node| node.kind == "parameter" && node.label == "second")
        );
        assert!(
            second_neighbors
                .nodes
                .iter()
                .all(|node| node.kind != "parameter" || node.label != "first")
        );
    }

    #[test]
    fn ending_a_session_is_idempotent_and_blocks_new_events() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let session = store
            .create_session(CreateSession {
                label: "ended".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();

        let ended = store.end_session(&session.id).unwrap().unwrap();
        let ended_again = store.end_session(&session.id).unwrap().unwrap();
        assert!(ended.ended_at.is_some());
        assert_eq!(ended.ended_at, ended_again.ended_at);

        let error = store
            .ingest_http(HttpIngestBatch {
                producer_id: "late-producer".into(),
                session_id: session.id,
                events: vec![event(1, "https://example.com/late", "2026-11-20T12:00:00Z")],
            })
            .unwrap_err();
        assert!(is_input_error(&error));
        assert!(error.to_string().contains("has ended"));
    }

    #[test]
    fn graph_extracts_json_body_shape_without_storing_values_in_nodes() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let session = store
            .create_session(CreateSession {
                label: "json".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let body = r#"{"user":{"email":"secret@example.com"},"enabled":true}"#;
        let mut capture = event(1, "https://example.com/users", "2026-11-20T12:00:00Z");
        capture.request.method = "POST".into();
        capture.request.headers = vec![HeaderField {
            name: "content-type".into(),
            value: "application/json".into(),
        }];
        capture.request.body = Some(CapturedBody {
            encoding: BodyEncoding::Utf8,
            data: body.into(),
            state: BodyState::Complete,
            reason: None,
            original_size: Some(body.len() as u64),
        });
        store
            .ingest_http(HttpIngestBatch {
                producer_id: "json-producer".into(),
                session_id: session.id.clone(),
                events: vec![capture],
            })
            .unwrap();

        let parameters = store
            .graph(GraphQuery {
                session_id: Some(session.id),
                kind: Some("parameter".into()),
                text: Some("user.email".into()),
                ..GraphQuery::default()
            })
            .unwrap();
        assert_eq!(parameters.nodes.len(), 1);
        assert_eq!(parameters.nodes[0].label, "user.email");
        assert_eq!(parameters.nodes[0].metadata["location"], "body");
        assert!(
            !parameters.nodes[0]
                .metadata
                .to_string()
                .contains("secret@example.com")
        );
    }

    #[test]
    fn reused_sequence_with_different_content_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let session = store
            .create_session(CreateSession {
                label: "collision".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let first = event(7, "https://example.com/one", "2026-11-20T12:00:00Z");
        let second = event(7, "https://example.com/two", "2026-11-20T12:00:01Z");
        store
            .ingest_http(HttpIngestBatch {
                producer_id: "same-producer".into(),
                session_id: session.id.clone(),
                events: vec![first],
            })
            .unwrap();
        let result = store
            .ingest_http(HttpIngestBatch {
                producer_id: "same-producer".into(),
                session_id: session.id,
                events: vec![second],
            })
            .unwrap();
        assert!(matches!(result[0].status, IngestStatus::Rejected));
        assert!(
            result[0]
                .error
                .as_deref()
                .is_some_and(|error| error.contains("different HTTP content"))
        );
    }

    #[test]
    fn websocket_history_handles_out_of_order_events_and_text_search() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let session = store
            .create_session(CreateSession {
                label: "websocket".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let frames = vec![
            WebSocketFrameEvent {
                sequence: 3,
                connection_id: "tab-1:request-1".into(),
                captured_at: Some("2026-11-20T12:03:00Z".into()),
                kind: WebSocketEventKind::Close,
                direction: None,
                opcode: None,
                payload: None,
                url: None,
                subprotocol: None,
                close_code: Some(1000),
                close_reason: Some("done".into()),
                error: None,
            },
            WebSocketFrameEvent {
                sequence: 2,
                connection_id: "tab-1:request-1".into(),
                captured_at: Some("2026-11-20T12:02:00Z".into()),
                kind: WebSocketEventKind::Frame,
                direction: Some(WebSocketDirection::ServerToClient),
                opcode: Some(1),
                payload: Some(CapturedBody {
                    encoding: BodyEncoding::Utf8,
                    data: "hello websocket".into(),
                    state: BodyState::Complete,
                    reason: None,
                    original_size: Some(15),
                }),
                url: None,
                subprotocol: None,
                close_code: None,
                close_reason: None,
                error: None,
            },
            WebSocketFrameEvent {
                sequence: 1,
                connection_id: "tab-1:request-1".into(),
                captured_at: Some("2026-11-20T12:01:00Z".into()),
                kind: WebSocketEventKind::Open,
                direction: None,
                opcode: None,
                payload: None,
                url: Some("wss://example.com/socket".into()),
                subprotocol: Some("graphql-ws".into()),
                close_code: None,
                close_reason: None,
                error: None,
            },
        ];
        let results = store
            .ingest_websocket(WebSocketIngestBatch {
                producer_id: "ws-producer".into(),
                session_id: session.id.clone(),
                frames,
            })
            .unwrap();
        assert!(
            results
                .iter()
                .all(|result| matches!(result.status, IngestStatus::Accepted))
        );

        let page = store
            .websocket_events(WebSocketQuery {
                session_id: Some(session.id),
                text: Some("hello websocket".into()),
                ..WebSocketQuery::default()
            })
            .unwrap();
        assert_eq!(page.items.len(), 1);
        let summary = &page.items[0];
        assert_eq!(summary.url.as_deref(), Some("wss://example.com/socket"));
        assert_eq!(summary.connection_state, "close");
        assert_eq!(summary.close_code, Some(1000));
        assert_eq!(summary.scope, "in_scope");
        let detail = store.websocket_event(&summary.id).unwrap().unwrap();
        assert_eq!(
            detail
                .payload
                .and_then(|payload| payload.text_preview)
                .as_deref(),
            Some("hello websocket")
        );
    }

    #[test]
    fn scope_profiles_and_annotations_have_complete_crud() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let profile_id = store
            .save_scope_profile(SaveScopeProfile {
                name: "Target".into(),
                rules: vec![ScopeRuleInput {
                    action: ScopeAction::Include,
                    matcher: ScopeMatcher::HostWildcard,
                    pattern: "*.example.test".into(),
                    enabled: true,
                }],
            })
            .unwrap();
        let profile = store.scope_profile(&profile_id).unwrap().unwrap();
        assert_eq!(profile.version, 1);
        assert_eq!(profile.rules.len(), 1);
        assert!(
            store
                .scope_profiles()
                .unwrap()
                .iter()
                .any(|profile| profile.id == profile_id)
        );
        let updated = store
            .update_scope_profile(
                &profile_id,
                SaveScopeProfile {
                    name: "Target updated".into(),
                    rules: vec![ScopeRuleInput {
                        action: ScopeAction::Exclude,
                        matcher: ScopeMatcher::Regex,
                        pattern: r"/logout$".into(),
                        enabled: true,
                    }],
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated.version, 2);
        assert_eq!(updated.rules[0].action, ScopeAction::Exclude);

        let session = store
            .create_session(CreateSession {
                label: "scoped".into(),
                source: "extension".into(),
                scope_profile_id: Some(profile_id.clone()),
            })
            .unwrap();
        assert!(store.delete_scope_profile(&profile_id).is_err());
        assert!(store.delete_scope_profile("default").is_err());

        let result = store
            .ingest_http(HttpIngestBatch {
                producer_id: "annotation-producer".into(),
                session_id: session.id.clone(),
                events: vec![event(
                    1,
                    "https://api.example.test/items",
                    "2026-11-20T12:00:00Z",
                )],
            })
            .unwrap();
        let observation_id = result[0].observation_id.clone().unwrap();
        let annotation = store
            .annotate(AnnotationInput {
                target_type: "observation".into(),
                target_id: observation_id.clone(),
                body: "  investigate  ".into(),
                tags: vec!["auth".into(), "auth".into()],
            })
            .unwrap();
        assert_eq!(annotation.body, "investigate");
        assert_eq!(annotation.tags, vec!["auth"]);
        let updated_annotation = store
            .update_annotation(
                &annotation.id,
                UpdateAnnotation {
                    body: "confirmed".into(),
                    tags: vec!["high".into()],
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated_annotation.body, "confirmed");
        assert_eq!(
            store
                .annotations(AnnotationQuery {
                    target_type: Some("observation".into()),
                    target_id: Some(observation_id),
                    limit: None,
                })
                .unwrap()
                .len(),
            1
        );
        assert!(store.delete_annotation(&annotation.id).unwrap());
        assert!(store.annotation(&annotation.id).unwrap().is_none());

        store.end_session(&session.id).unwrap();
        store.delete_session(&session.id).unwrap();
        assert!(store.delete_scope_profile(&profile_id).unwrap());
    }

    #[test]
    fn deleting_an_ended_session_preserves_shared_evidence_and_purges_orphans() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let first = store
            .create_session(CreateSession {
                label: "first".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let second = store
            .create_session(CreateSession {
                label: "second".into(),
                source: "extension".into(),
                scope_profile_id: None,
            })
            .unwrap();

        let body_event = |sequence: u64, path: &str, body: &str, captured_at: &str| {
            let mut value = event(
                sequence,
                &format!("https://shared.example/{path}"),
                captured_at,
            );
            value.request.method = "POST".into();
            value.request.headers = vec![HeaderField {
                name: "content-type".into(),
                value: "text/plain".into(),
            }];
            value.request.body = Some(CapturedBody {
                encoding: BodyEncoding::Utf8,
                data: body.into(),
                state: BodyState::Complete,
                reason: None,
                original_size: Some(body.len() as u64),
            });
            value
        };
        let first_result = store
            .ingest_http(HttpIngestBatch {
                producer_id: "first-producer".into(),
                session_id: first.id.clone(),
                events: vec![body_event(
                    1,
                    "first-only",
                    "first-secret",
                    "2026-11-20T10:00:00Z",
                )],
            })
            .unwrap();
        store
            .ingest_http(HttpIngestBatch {
                producer_id: "second-producer".into(),
                session_id: second.id.clone(),
                events: vec![body_event(
                    1,
                    "second-only",
                    "second-secret",
                    "2026-11-20T11:00:00Z",
                )],
            })
            .unwrap();
        let first_observation = first_result[0].observation_id.clone().unwrap();
        let first_hash = store
            .exchange(&first_observation)
            .unwrap()
            .unwrap()
            .request_body
            .unwrap()
            .hash
            .unwrap();
        let raw = store.body(&first_hash).unwrap().unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(raw.data_base64)
                .unwrap(),
            b"first-secret"
        );
        let host = store
            .graph(GraphQuery {
                kind: Some("host".into()),
                ..GraphQuery::default()
            })
            .unwrap()
            .nodes
            .pop()
            .unwrap();
        let graph_annotation = store
            .annotate(AnnotationInput {
                target_type: "graph_node".into(),
                target_id: host.id.clone(),
                body: "shared host".into(),
                tags: vec![],
            })
            .unwrap();
        store
            .annotate(AnnotationInput {
                target_type: "observation".into(),
                target_id: first_observation,
                body: "first only".into(),
                tags: vec![],
            })
            .unwrap();

        assert!(store.delete_session(&first.id).is_err());
        store.end_session(&first.id).unwrap();
        let deleted = store.delete_session(&first.id).unwrap().unwrap();
        assert_eq!(deleted.observations, 1);
        assert!(store.body(&first_hash).unwrap().is_none());
        assert!(
            store
                .traffic(TrafficQuery {
                    text: Some("first-only".into()),
                    ..TrafficQuery::default()
                })
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(
            store
                .traffic(TrafficQuery {
                    session_id: Some(second.id.clone()),
                    ..TrafficQuery::default()
                })
                .unwrap()
                .items
                .len(),
            1
        );
        let remaining_host = store
            .graph(GraphQuery {
                kind: Some("host".into()),
                ..GraphQuery::default()
            })
            .unwrap()
            .nodes
            .pop()
            .unwrap();
        assert_eq!(remaining_host.id, host.id);
        assert_eq!(remaining_host.evidence_count, 1);
        assert!(store.annotation(&graph_annotation.id).unwrap().is_some());
        assert!(
            store
                .annotations(AnnotationQuery::default())
                .unwrap()
                .iter()
                .all(|annotation| annotation.body != "first only")
        );

        store.end_session(&second.id).unwrap();
        store.delete_session(&second.id).unwrap();
        assert!(store.graph(GraphQuery::default()).unwrap().nodes.is_empty());
        assert!(store.annotation(&graph_annotation.id).unwrap().is_none());
    }
}
