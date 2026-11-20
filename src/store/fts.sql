CREATE VIRTUAL TABLE IF NOT EXISTS traffic_fts USING fts5(
    observation_id UNINDEXED,
    url,
    headers,
    body_preview,
    tokenize = 'trigram'
);

CREATE VIRTUAL TABLE IF NOT EXISTS graph_fts USING fts5(
    node_id UNINDEXED,
    label,
    metadata,
    tokenize = 'trigram'
);

CREATE VIRTUAL TABLE IF NOT EXISTS websocket_fts USING fts5(
    event_id UNINDEXED,
    connection_id UNINDEXED,
    payload,
    tokenize = 'trigram'
);

