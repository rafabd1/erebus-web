use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use base64::Engine;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use reqwest::Client;
use reqwest::redirect::Policy;
use serde_json::json;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use url::Url;
use uuid::Uuid;

use crate::model::{
    BodyEncoding, BodyState, CapturedBody, CapturedRequest, CapturedResponse, HeaderField,
    HttpCaptureEvent, HttpIngestBatch, IngestStatus, ReplayRequest, ReplayResult,
};
use crate::store::{MAX_BODY_BYTES, ReplayRecord, ReplaySourceRequest, Store, invalid};

const REPLAY_SEQUENCE: u64 = 1;
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MIN_TIMEOUT_MS: u64 = 100;
const MAX_TIMEOUT_MS: u64 = 120_000;

pub enum ReplayPreparation {
    Existing(ReplayResult),
    MissingSource,
    Ready(Box<PreparedReplay>),
}

pub struct PreparedReplay {
    replay_id: String,
    producer_id: String,
    input_hash: String,
    source_observation_id: String,
    destination_session_id: String,
    method: Method,
    url: Url,
    headers: HeaderMap,
    captured_headers: Vec<HeaderField>,
    body: Option<Vec<u8>>,
    follow_redirects: bool,
    timeout_ms: u64,
    client: Client,
}

pub struct CompletedReplay {
    replay_id: String,
    producer_id: String,
    input_hash: String,
    destination_session_id: String,
    event: HttpCaptureEvent,
}

pub fn prepare(store: &Store, input: ReplayRequest) -> anyhow::Result<ReplayPreparation> {
    validate_identifier("observationId", &input.observation_id)?;
    validate_identifier("sessionId", &input.session_id)?;
    let replay_id = match input.replay_id.as_deref() {
        Some(value) => {
            validate_identifier("replayId", value)?;
            if value.len() > 128 {
                return Err(invalid("replayId cannot exceed 128 bytes"));
            }
            value.to_owned()
        }
        None => Uuid::now_v7().to_string(),
    };
    let producer_id = format!("replay:{}", blake3::hash(replay_id.as_bytes()).to_hex());
    let input_hash = replay_input_hash(&input)?;
    if let Some(record) = store.replay_record(&producer_id, REPLAY_SEQUENCE)? {
        return replay_result_from_record(record, &replay_id, &input_hash)
            .map(ReplayPreparation::Existing);
    }

    store.ensure_active_session(&input.session_id)?;
    let Some(source) = store.replay_source(&input.observation_id)? else {
        return Ok(ReplayPreparation::MissingSource);
    };
    let method = parse_method(input.method.as_deref().unwrap_or(&source.method))?;
    let url = parse_url(input.url.as_deref().unwrap_or(&source.url))?;
    let source_headers = input.headers.as_ref().unwrap_or(&source.headers);
    let (captured_headers, headers) = sanitize_headers(source_headers)?;
    let body = match input.body.as_ref() {
        Some(body) => Some(decode_body_override(body)?),
        None => source_body(source)?,
    };
    let timeout_ms = input
        .timeout_ms
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .clamp(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS);
    let redirect_policy = if input.follow_redirects {
        Policy::limited(10)
    } else {
        Policy::none()
    };
    let client = Client::builder()
        .redirect(redirect_policy)
        .timeout(Duration::from_millis(timeout_ms))
        .connect_timeout(Duration::from_millis(timeout_ms.min(30_000)))
        .build()
        .context("failed to build replay HTTP client")?;

    Ok(ReplayPreparation::Ready(Box::new(PreparedReplay {
        replay_id,
        producer_id,
        input_hash,
        source_observation_id: input.observation_id,
        destination_session_id: input.session_id,
        method,
        url,
        headers,
        captured_headers,
        body,
        follow_redirects: input.follow_redirects,
        timeout_ms,
        client,
    })))
}

pub async fn execute(prepared: PreparedReplay) -> CompletedReplay {
    let started = Instant::now();
    let captured_at = OffsetDateTime::now_utc().format(&Rfc3339).ok();
    let request_body = prepared.body.as_deref().map(captured_body);
    let request = CapturedRequest {
        method: prepared.method.as_str().into(),
        url: prepared.url.to_string(),
        headers: prepared.captured_headers,
        body: request_body,
    };
    let mut builder = prepared
        .client
        .request(prepared.method, prepared.url)
        .headers(prepared.headers);
    if let Some(body) = prepared.body {
        builder = builder.body(body);
    }

    let (response, error) = match builder.send().await {
        Ok(mut response) => {
            let status = response.status().as_u16();
            let headers = capture_headers(response.headers());
            let mut body = ResponseBodyCapture::default();
            let mut stream_error = None;
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) => body.append(&chunk),
                    Ok(None) => break,
                    Err(error) => {
                        stream_error = Some(format!("response body stream failed: {error}"));
                        break;
                    }
                }
            }
            let captured = body.finish(stream_error.as_deref());
            (
                Some(CapturedResponse {
                    status,
                    headers,
                    body: Some(captured),
                }),
                stream_error,
            )
        }
        Err(error) => (None, Some(format!("replay request failed: {error}"))),
    };
    let event = HttpCaptureEvent {
        sequence: REPLAY_SEQUENCE,
        captured_at,
        duration_ms: Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
        request,
        response,
        error,
        tab_id: None,
        initiator: Some("replay".into()),
        parent_sequence: None,
        metadata: json!({
            "source": "replay",
            "replayedFrom": prepared.source_observation_id,
            "replayId": prepared.replay_id,
            "replayInputHash": prepared.input_hash,
            "followRedirects": prepared.follow_redirects,
            "timeoutMs": prepared.timeout_ms,
        }),
    };
    CompletedReplay {
        replay_id: event.metadata["replayId"]
            .as_str()
            .expect("replayId metadata is a string")
            .to_owned(),
        producer_id: prepared.producer_id,
        input_hash: event.metadata["replayInputHash"]
            .as_str()
            .expect("replayInputHash metadata is a string")
            .to_owned(),
        destination_session_id: prepared.destination_session_id,
        event,
    }
}

pub fn persist(store: &Store, completed: CompletedReplay) -> anyhow::Result<ReplayResult> {
    let status = completed
        .event
        .response
        .as_ref()
        .map(|response| response.status);
    let error = completed.event.error.clone();
    let results = store.ingest_http(HttpIngestBatch {
        producer_id: completed.producer_id.clone(),
        session_id: completed.destination_session_id,
        events: vec![completed.event],
    })?;
    let result = results
        .into_iter()
        .next()
        .context("replay ingest returned no result")?;
    match result.status {
        IngestStatus::Accepted => Ok(ReplayResult {
            replay_id: completed.replay_id,
            observation_id: result
                .observation_id
                .context("accepted replay has no observation id")?,
            status,
            error,
        }),
        IngestStatus::Duplicate => {
            let record = store
                .replay_record(&completed.producer_id, REPLAY_SEQUENCE)?
                .context("duplicate replay record disappeared")?;
            replay_result_from_record(record, &completed.replay_id, &completed.input_hash)
        }
        IngestStatus::Rejected => {
            if let Some(record) = store.replay_record(&completed.producer_id, REPLAY_SEQUENCE)? {
                return replay_result_from_record(
                    record,
                    &completed.replay_id,
                    &completed.input_hash,
                );
            }
            Err(invalid(
                result
                    .error
                    .unwrap_or_else(|| "replay capture was rejected".into()),
            ))
        }
    }
}

fn replay_result_from_record(
    record: ReplayRecord,
    replay_id: &str,
    input_hash: &str,
) -> anyhow::Result<ReplayResult> {
    let stored_hash = record
        .metadata
        .get("replayInputHash")
        .and_then(serde_json::Value::as_str);
    if stored_hash != Some(input_hash) {
        return Err(invalid("replayId was already used for a different request"));
    }
    Ok(ReplayResult {
        replay_id: replay_id.into(),
        observation_id: record.observation_id,
        status: record.status,
        error: record.error,
    })
}

fn replay_input_hash(input: &ReplayRequest) -> anyhow::Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(input)?)
        .to_hex()
        .to_string())
}

fn validate_identifier(name: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{name} is required")));
    }
    Ok(())
}

fn parse_method(value: &str) -> anyhow::Result<Method> {
    Method::from_bytes(value.trim().as_bytes())
        .map_err(|error| invalid(format!("replay method is invalid: {error}")))
}

fn parse_url(value: &str) -> anyhow::Result<Url> {
    let url =
        Url::parse(value).map_err(|error| invalid(format!("replay URL is invalid: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid("replay URL must use http or https"));
    }
    if url.host_str().is_none() {
        return Err(invalid("replay URL has no host"));
    }
    Ok(url)
}

fn source_body(source: ReplaySourceRequest) -> anyhow::Result<Option<Vec<u8>>> {
    match source.body.state.as_str() {
        "complete" => {
            let bytes = source.body.bytes.unwrap_or_default();
            if source.body.original_size != bytes.len() as u64 {
                bail!("stored complete request body has inconsistent size");
            }
            Ok(Some(bytes))
        }
        "unavailable" if source.body.original_size == 0 => Ok(None),
        state => {
            let detail = source
                .body
                .reason
                .as_deref()
                .map(|reason| format!(": {reason}"))
                .unwrap_or_default();
            Err(invalid(format!(
                "source request body is {state}{detail}; provide a complete body override"
            )))
        }
    }
}

fn decode_body_override(body: &CapturedBody) -> anyhow::Result<Vec<u8>> {
    if body.state != BodyState::Complete {
        return Err(invalid("replay body override must be complete"));
    }
    let bytes = match body.encoding {
        BodyEncoding::Utf8 => body.data.as_bytes().to_vec(),
        BodyEncoding::Base64 => base64::engine::general_purpose::STANDARD
            .decode(&body.data)
            .map_err(|_| invalid("replay body override is not valid base64"))?,
    };
    if bytes.len() > MAX_BODY_BYTES {
        return Err(invalid(format!(
            "replay body exceeds the {MAX_BODY_BYTES} byte limit"
        )));
    }
    if body
        .original_size
        .is_some_and(|size| size != bytes.len() as u64)
    {
        return Err(invalid(
            "replay body originalSize must match the complete body",
        ));
    }
    Ok(bytes)
}

fn captured_body(bytes: &[u8]) -> CapturedBody {
    CapturedBody {
        encoding: BodyEncoding::Base64,
        data: base64::engine::general_purpose::STANDARD.encode(bytes),
        state: BodyState::Complete,
        reason: None,
        original_size: Some(bytes.len() as u64),
    }
}

fn sanitize_headers(fields: &[HeaderField]) -> anyhow::Result<(Vec<HeaderField>, HeaderMap)> {
    let connection_headers = fields
        .iter()
        .filter(|field| {
            field.name.eq_ignore_ascii_case("connection")
                || field.name.eq_ignore_ascii_case("proxy-connection")
        })
        .flat_map(|field| field.value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<_>>();
    let mut captured = Vec::with_capacity(fields.len());
    let mut headers = HeaderMap::with_capacity(fields.len());
    for field in fields {
        let normalized = field.name.trim().to_ascii_lowercase();
        if is_removed_header(&normalized, &connection_headers) {
            continue;
        }
        let name = HeaderName::from_bytes(normalized.as_bytes())
            .map_err(|error| invalid(format!("replay header name is invalid: {error}")))?;
        let value = HeaderValue::from_bytes(field.value.as_bytes())
            .map_err(|error| invalid(format!("replay header value is invalid: {error}")))?;
        headers.append(name, value);
        captured.push(field.clone());
    }
    Ok((captured, headers))
}

fn is_removed_header(name: &str, connection_headers: &HashSet<String>) -> bool {
    connection_headers.contains(name)
        || matches!(
            name,
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "host"
                | "content-length"
        )
}

fn capture_headers(headers: &HeaderMap) -> Vec<HeaderField> {
    let mut captured = Vec::with_capacity(headers.len());
    for name in headers.keys() {
        for value in headers.get_all(name).iter() {
            captured.push(HeaderField {
                name: name.as_str().into(),
                value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
            });
        }
    }
    captured
}

#[derive(Default)]
struct ResponseBodyCapture {
    bytes: Vec<u8>,
    original_size: u64,
}

impl ResponseBodyCapture {
    fn append(&mut self, chunk: &[u8]) {
        self.original_size = self.original_size.saturating_add(chunk.len() as u64);
        let remaining = MAX_BODY_BYTES.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    fn finish(self, stream_error: Option<&str>) -> CapturedBody {
        let limited = self.original_size > self.bytes.len() as u64;
        let state = if limited || stream_error.is_some() {
            BodyState::Truncated
        } else {
            BodyState::Complete
        };
        let reason = stream_error
            .map(str::to_owned)
            .or_else(|| limited.then(|| "replay capture limit reached".into()));
        CapturedBody {
            encoding: BodyEncoding::Base64,
            data: base64::engine::general_purpose::STANDARD.encode(self.bytes),
            state,
            reason,
            original_size: Some(self.original_size),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use super::*;
    use crate::model::{CreateSession, HttpCaptureEvent, HttpIngestBatch, TrafficQuery};

    #[tokio::test]
    async fn replays_binary_requests_with_overrides_without_following_redirects() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let source_session = store
            .create_session(CreateSession {
                label: "source".into(),
                source: "test".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let destination_session = store
            .create_session(CreateSession {
                label: "destination".into(),
                source: "replay".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let (origin, mut requests, server) = spawn_origin(2, b"redirect body".to_vec()).await;
        let original_body = vec![0, 255, 1];
        let source = HttpCaptureEvent {
            sequence: 1,
            captured_at: None,
            duration_ms: Some(1),
            request: CapturedRequest {
                method: "POST".into(),
                url: format!("{origin}/original"),
                headers: vec![
                    header("Authorization", "Bearer source-token"),
                    header("X-Duplicate", "first"),
                    header("X-Duplicate", "second"),
                    header("Connection", "X-Hop"),
                    header("X-Hop", "remove-me"),
                    header("Host", "stale.invalid"),
                    header("Content-Length", "999"),
                ],
                body: Some(captured_body(&original_body)),
            },
            response: None,
            error: None,
            tab_id: None,
            initiator: None,
            parent_sequence: None,
            metadata: json!({}),
        };
        let source_id = store
            .ingest_http(HttpIngestBatch {
                producer_id: "seed".into(),
                session_id: source_session.id,
                events: vec![source],
            })
            .unwrap()
            .remove(0)
            .observation_id
            .unwrap();

        let first_input = ReplayRequest {
            observation_id: source_id.clone(),
            session_id: destination_session.id.clone(),
            replay_id: Some("first-replay".into()),
            method: Some("PUT".into()),
            url: Some(format!("{origin}/first")),
            headers: None,
            body: None,
            follow_redirects: false,
            timeout_ms: Some(5_000),
        };
        let first = run_replay(&store, first_input.clone()).await;
        assert_eq!(first.status, Some(302));
        let raw = requests.recv().await.unwrap();
        let (head, body) = split_request(&raw);
        assert!(head.starts_with("PUT /first HTTP/1.1\r\n"));
        assert_eq!(body, original_body);
        assert_eq!(header_count(&head, "x-duplicate"), 2);
        assert!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer source-token\r\n")
        );
        assert!(!head.to_ascii_lowercase().contains("x-hop:"));
        assert!(!head.contains("stale.invalid"));
        assert!(head.to_ascii_lowercase().contains("content-length: 3\r\n"));

        let override_body = vec![9, 0, 8, 7];
        let second_input = ReplayRequest {
            observation_id: source_id.clone(),
            session_id: destination_session.id.clone(),
            replay_id: Some("second-replay".into()),
            method: Some("PATCH".into()),
            url: Some(format!("{origin}/second")),
            headers: Some(vec![
                header("Authorization", "Bearer override-token"),
                header("X-Duplicate", "alpha"),
                header("X-Duplicate", "beta"),
                header("Connection", "X-Remove"),
                header("X-Remove", "remove-me"),
                header("Host", "stale.invalid"),
                header("Content-Length", "888"),
            ]),
            body: Some(captured_body(&override_body)),
            follow_redirects: false,
            timeout_ms: Some(5_000),
        };
        let second = run_replay(&store, second_input).await;
        assert_eq!(second.status, Some(302));
        let raw = requests.recv().await.unwrap();
        let (head, body) = split_request(&raw);
        assert!(head.starts_with("PATCH /second HTTP/1.1\r\n"));
        assert_eq!(body, override_body);
        assert_eq!(header_count(&head, "x-duplicate"), 2);
        assert!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer override-token\r\n")
        );
        assert!(!head.to_ascii_lowercase().contains("x-remove:"));
        assert!(head.to_ascii_lowercase().contains("content-length: 4\r\n"));

        server.await.unwrap();
        let detail = store.exchange(&first.observation_id).unwrap().unwrap();
        assert_eq!(detail.summary.status, Some(302));
        assert_eq!(detail.metadata["source"], "replay");
        assert_eq!(detail.metadata["replayedFrom"], source_id);
        assert_eq!(
            detail
                .request_headers
                .iter()
                .filter(|header| header.name.eq_ignore_ascii_case("x-duplicate"))
                .count(),
            2
        );
        assert_eq!(
            store
                .traffic(TrafficQuery {
                    session_id: Some(destination_session.id),
                    ..TrafficQuery::default()
                })
                .unwrap()
                .items
                .len(),
            2
        );

        match prepare(&store, first_input).unwrap() {
            ReplayPreparation::Existing(existing) => {
                assert_eq!(existing.observation_id, first.observation_id)
            }
            _ => panic!("repeated replayId should return the stored observation"),
        }
    }

    #[tokio::test]
    async fn drains_large_response_while_storing_a_bounded_capture() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path()).unwrap();
        let source_session = store
            .create_session(CreateSession {
                label: "source".into(),
                source: "test".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let destination_session = store
            .create_session(CreateSession {
                label: "destination".into(),
                source: "replay".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let response_body = vec![b'x'; MAX_BODY_BYTES + 8_192];
        let response_size = response_body.len() as u64;
        let (origin, _requests, server) = spawn_origin(1, response_body).await;
        let source_id = store
            .ingest_http(HttpIngestBatch {
                producer_id: "large-seed".into(),
                session_id: source_session.id,
                events: vec![HttpCaptureEvent {
                    sequence: 1,
                    captured_at: None,
                    duration_ms: None,
                    request: CapturedRequest {
                        method: "GET".into(),
                        url: format!("{origin}/large"),
                        headers: vec![],
                        body: Some(captured_body(&[])),
                    },
                    response: None,
                    error: None,
                    tab_id: None,
                    initiator: None,
                    parent_sequence: None,
                    metadata: json!({}),
                }],
            })
            .unwrap()
            .remove(0)
            .observation_id
            .unwrap();
        let replay = run_replay(
            &store,
            ReplayRequest {
                observation_id: source_id,
                session_id: destination_session.id,
                replay_id: Some("large-response".into()),
                method: None,
                url: None,
                headers: None,
                body: None,
                follow_redirects: false,
                timeout_ms: Some(30_000),
            },
        )
        .await;
        server.await.unwrap();
        assert_eq!(replay.status, Some(302));
        let detail = store.exchange(&replay.observation_id).unwrap().unwrap();
        let body = detail.response_body.unwrap();
        assert_eq!(body.state, "truncated");
        assert_eq!(body.captured_size, MAX_BODY_BYTES as u64);
        assert_eq!(body.original_size, response_size);
    }

    async fn run_replay(store: &Store, input: ReplayRequest) -> ReplayResult {
        let prepared = match prepare(store, input).unwrap() {
            ReplayPreparation::Ready(prepared) => *prepared,
            _ => panic!("new replay should be ready"),
        };
        persist(store, execute(prepared).await).unwrap()
    }

    fn header(name: &str, value: &str) -> HeaderField {
        HeaderField {
            name: name.into(),
            value: value.into(),
        }
    }

    async fn spawn_origin(
        expected_requests: usize,
        response_body: Vec<u8>,
    ) -> (String, mpsc::Receiver<Vec<u8>>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel(expected_requests);
        let body = Arc::new(response_body);
        let server = tokio::spawn(async move {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                sender.send(request).await.unwrap();
                let headers = format!(
                    "HTTP/1.1 302 Found\r\nLocation: /followed\r\nX-Duplicate: one\r\nX-Duplicate: two\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(headers.as_bytes()).await.unwrap();
                for chunk in body.chunks(64 * 1024) {
                    stream.write_all(chunk).await.unwrap();
                }
                stream.shutdown().await.unwrap();
            }
        });
        (format!("http://{address}"), receiver, server)
    }

    async fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let header_end = loop {
            let mut chunk = [0_u8; 8 * 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "request closed before headers completed");
            request.extend_from_slice(&chunk[..read]);
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let mut chunk = [0_u8; 8 * 1024];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "request closed before body completed");
            request.extend_from_slice(&chunk[..read]);
        }
        request.truncate(header_end + content_length);
        request
    }

    fn split_request(request: &[u8]) -> (String, Vec<u8>) {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .unwrap()
            + 4;
        (
            String::from_utf8(request[..header_end].to_vec()).unwrap(),
            request[header_end..].to_vec(),
        )
    }

    fn header_count(headers: &str, name: &str) -> usize {
        headers
            .lines()
            .filter_map(|line| line.split_once(':').map(|(name, _)| name))
            .filter(|candidate| candidate.eq_ignore_ascii_case(name))
            .count()
    }
}
