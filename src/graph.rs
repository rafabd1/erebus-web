use rusqlite::{Transaction, params};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use crate::extract::BodyParameter;

#[allow(clippy::too_many_arguments)]
pub fn project_http(
    transaction: &Transaction<'_>,
    observation_id: &str,
    session_id: &str,
    captured_at: &str,
    method: &str,
    url: &Url,
    body_parameters: &[BodyParameter],
    scope_state: &str,
) -> anyhow::Result<()> {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host.is_empty() {
        return Ok(());
    }
    let origin_label = origin_label(url);
    let host_id = upsert_node(
        transaction,
        "host",
        &format!("host:{host}"),
        &host,
        json!({}),
        captured_at,
    )?;
    let origin_id = upsert_node(
        transaction,
        "origin",
        &format!("origin:{origin_label}"),
        &origin_label,
        json!({ "scheme": url.scheme(), "port": url.port_or_known_default() }),
        captured_at,
    )?;
    let path = normalized_path(url.path());
    let route_id = upsert_node(
        transaction,
        "route",
        &format!("route:{origin_label}{path}"),
        &path,
        json!({ "origin": origin_label }),
        captured_at,
    )?;
    let normalized_method = method.to_ascii_uppercase();
    let operation_key = format!("operation:{normalized_method}:{origin_label}{path}");
    let operation_id = upsert_node(
        transaction,
        "operation",
        &operation_key,
        &format!("{normalized_method} {path}"),
        json!({ "method": normalized_method, "path": path }),
        captured_at,
    )?;

    upsert_edge(
        transaction,
        &host_id,
        &origin_id,
        "serves",
        captured_at,
        session_id,
        "http",
        observation_id,
        scope_state,
    )?;
    upsert_edge(
        transaction,
        &origin_id,
        &route_id,
        "contains",
        captured_at,
        session_id,
        "http",
        observation_id,
        scope_state,
    )?;
    upsert_edge(
        transaction,
        &route_id,
        &operation_id,
        "accepts",
        captured_at,
        session_id,
        "http",
        observation_id,
        scope_state,
    )?;

    attach_evidence(
        transaction,
        &host_id,
        session_id,
        "http",
        observation_id,
        scope_state,
        captured_at,
    )?;
    attach_evidence(
        transaction,
        &origin_id,
        session_id,
        "http",
        observation_id,
        scope_state,
        captured_at,
    )?;
    attach_evidence(
        transaction,
        &route_id,
        session_id,
        "http",
        observation_id,
        scope_state,
        captured_at,
    )?;
    attach_evidence(
        transaction,
        &operation_id,
        session_id,
        "http",
        observation_id,
        scope_state,
        captured_at,
    )?;

    let mut seen = std::collections::BTreeSet::new();
    for (name, _) in url.query_pairs() {
        if !seen.insert(name.to_string()) {
            continue;
        }
        let key = format!("parameter:{operation_key}:query:{name}");
        let parameter_id = upsert_node(
            transaction,
            "parameter",
            &key,
            &name,
            json!({ "location": "query", "name": name }),
            captured_at,
        )?;
        upsert_edge(
            transaction,
            &operation_id,
            &parameter_id,
            "has_parameter",
            captured_at,
            session_id,
            "http",
            observation_id,
            scope_state,
        )?;
        attach_evidence(
            transaction,
            &parameter_id,
            session_id,
            "http",
            observation_id,
            scope_state,
            captured_at,
        )?;
    }
    for parameter in body_parameters {
        let key = format!("parameter:{operation_key}:body:{}", parameter.path);
        let parameter_id = upsert_node(
            transaction,
            "parameter",
            &key,
            &parameter.path,
            json!({
                "location": "body",
                "name": parameter.name,
                "path": parameter.path,
                "valueType": parameter.value_type,
            }),
            captured_at,
        )?;
        upsert_edge(
            transaction,
            &operation_id,
            &parameter_id,
            "has_parameter",
            captured_at,
            session_id,
            "http",
            observation_id,
            scope_state,
        )?;
        attach_evidence(
            transaction,
            &parameter_id,
            session_id,
            "http",
            observation_id,
            scope_state,
            captured_at,
        )?;
    }
    Ok(())
}

pub fn project_websocket(
    transaction: &Transaction<'_>,
    event_id: &str,
    session_id: &str,
    captured_at: &str,
    url: &Url,
    scope_state: &str,
) -> anyhow::Result<()> {
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host.is_empty() {
        return Ok(());
    }
    let origin_label = origin_label(url);
    let host_id = upsert_node(
        transaction,
        "host",
        &format!("host:{host}"),
        &host,
        json!({}),
        captured_at,
    )?;
    let origin_id = upsert_node(
        transaction,
        "origin",
        &format!("origin:{origin_label}"),
        &origin_label,
        json!({ "scheme": url.scheme(), "port": url.port_or_known_default() }),
        captured_at,
    )?;
    let path = normalized_path(url.path());
    let channel_id = upsert_node(
        transaction,
        "websocket",
        &format!("websocket:{origin_label}{path}"),
        &path,
        json!({ "origin": origin_label, "path": path }),
        captured_at,
    )?;
    upsert_edge(
        transaction,
        &host_id,
        &origin_id,
        "serves",
        captured_at,
        session_id,
        "websocket",
        event_id,
        scope_state,
    )?;
    upsert_edge(
        transaction,
        &origin_id,
        &channel_id,
        "upgrades_to",
        captured_at,
        session_id,
        "websocket",
        event_id,
        scope_state,
    )?;
    attach_evidence(
        transaction,
        &host_id,
        session_id,
        "websocket",
        event_id,
        scope_state,
        captured_at,
    )?;
    attach_evidence(
        transaction,
        &origin_id,
        session_id,
        "websocket",
        event_id,
        scope_state,
        captured_at,
    )?;
    attach_evidence(
        transaction,
        &channel_id,
        session_id,
        "websocket",
        event_id,
        scope_state,
        captured_at,
    )?;
    Ok(())
}

fn upsert_node(
    transaction: &Transaction<'_>,
    kind: &str,
    canonical_key: &str,
    label: &str,
    metadata: Value,
    seen_at: &str,
) -> anyhow::Result<String> {
    let id = Uuid::now_v7().to_string();
    let metadata = serde_json::to_string(&metadata)?;
    transaction.execute(
        "INSERT INTO graph_nodes(id, kind, canonical_key, label, metadata_json, first_seen, last_seen, evidence_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, 1)
         ON CONFLICT(canonical_key) DO UPDATE SET
             first_seen = CASE
                 WHEN julianday(excluded.first_seen) < julianday(graph_nodes.first_seen)
                 THEN excluded.first_seen ELSE graph_nodes.first_seen END,
             last_seen = CASE
                 WHEN julianday(excluded.last_seen) > julianday(graph_nodes.last_seen)
                 THEN excluded.last_seen ELSE graph_nodes.last_seen END,
             evidence_count = graph_nodes.evidence_count + 1",
        params![id, kind, canonical_key, label, metadata, seen_at],
    )?;
    transaction
        .query_row(
            "SELECT id FROM graph_nodes WHERE canonical_key = ?1",
            [canonical_key],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn upsert_edge(
    transaction: &Transaction<'_>,
    src: &str,
    dst: &str,
    kind: &str,
    seen_at: &str,
    session_id: &str,
    evidence_kind: &str,
    evidence_id: &str,
    scope_state: &str,
) -> anyhow::Result<()> {
    transaction.execute(
        "INSERT INTO graph_edges(src_id, dst_id, kind, first_seen, last_seen, evidence_count)
         VALUES (?1, ?2, ?3, ?4, ?4, 1)
         ON CONFLICT(src_id, dst_id, kind) DO UPDATE SET
             first_seen = CASE
                 WHEN julianday(excluded.first_seen) < julianday(graph_edges.first_seen)
                 THEN excluded.first_seen ELSE graph_edges.first_seen END,
             last_seen = CASE
                 WHEN julianday(excluded.last_seen) > julianday(graph_edges.last_seen)
                 THEN excluded.last_seen ELSE graph_edges.last_seen END,
             evidence_count = graph_edges.evidence_count + 1",
        params![src, dst, kind, seen_at],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO graph_edge_evidence(
            src_id, dst_id, edge_kind, session_id, evidence_kind, evidence_id,
            scope_state, captured_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            src,
            dst,
            kind,
            session_id,
            evidence_kind,
            evidence_id,
            scope_state,
            seen_at
        ],
    )?;
    Ok(())
}

fn attach_evidence(
    transaction: &Transaction<'_>,
    node_id: &str,
    session_id: &str,
    evidence_kind: &str,
    evidence_id: &str,
    scope_state: &str,
    captured_at: &str,
) -> anyhow::Result<()> {
    transaction.execute(
        "INSERT OR IGNORE INTO graph_node_evidence(
            node_id, session_id, evidence_kind, evidence_id, scope_state, captured_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            node_id,
            session_id,
            evidence_kind,
            evidence_id,
            scope_state,
            captured_at
        ],
    )?;
    Ok(())
}

fn origin_label(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

fn normalized_path(path: &str) -> String {
    if path.is_empty() {
        "/".into()
    } else {
        path.into()
    }
}
