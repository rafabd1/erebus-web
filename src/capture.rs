use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;

use crate::model::{
    CaptureGapInput, HttpCaptureEvent, HttpIngestBatch, IngestResult, IngestStatus,
    WebSocketFrameEvent, WebSocketIngestBatch,
};
use crate::proxy::{
    ProxyCapture, ProxyConfig, ProxyHandle, UpstreamTlsConfig, atomic_write, start_proxy,
};
use crate::store::Store;

const INGEST_BATCH_ITEMS: usize = 100;
const INGEST_INTERVAL: Duration = Duration::from_millis(50);
const SHUTDOWN_DRAIN_LIMIT: Duration = Duration::from_secs(5);
const SHUTDOWN_JOIN_LIMIT: Duration = Duration::from_secs(12);
const UPSTREAM_TLS_FILE_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureProxyDescriptor {
    pub url: String,
    pub ca_endpoint: &'static str,
    pub upstream_tls_mode: String,
}

#[derive(Clone)]
pub struct CaptureManager {
    inner: Arc<CaptureManagerInner>,
}

struct CaptureManagerInner {
    store: Arc<Store>,
    proxy_data_dir: PathBuf,
    active: Mutex<HashMap<String, ManagedProxy>>,
}

struct ManagedProxy {
    descriptor: CaptureProxyDescriptor,
    upstream_tls: UpstreamTlsConfig,
    handle: ProxyHandle,
    worker: JoinHandle<()>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedUpstreamTls {
    version: u32,
    session_id: String,
    config: UpstreamTlsConfig,
}

impl CaptureManager {
    pub fn new(store: Arc<Store>, data_dir: &Path) -> Self {
        Self {
            inner: Arc::new(CaptureManagerInner {
                store,
                proxy_data_dir: data_dir.join("proxy"),
                active: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub async fn start(
        &self,
        session_id: &str,
        upstream_tls: Option<UpstreamTlsConfig>,
    ) -> anyhow::Result<CaptureProxyDescriptor> {
        let upstream_tls = upstream_tls.map(|config| config.normalized()).transpose()?;
        let mut active = self.inner.active.lock().await;
        if let Some(existing) = active.get(session_id) {
            if upstream_tls
                .as_ref()
                .is_some_and(|requested| requested != &existing.upstream_tls)
            {
                anyhow::bail!("pause the capture proxy before changing upstream TLS settings");
            }
            return Ok(existing.descriptor.clone());
        }
        let upstream_tls = match upstream_tls {
            Some(config) => config,
            None => self.upstream_tls(session_id)?,
        };
        let upstream_tls_mode = upstream_tls.mode().to_owned();
        let runtime = start_proxy(
            &self.inner.proxy_data_dir,
            ProxyConfig {
                upstream_tls: upstream_tls.clone(),
                ..ProxyConfig::default()
            },
        )
        .await?;
        let expected_ca_path = self.inner.proxy_data_dir.join("proxy-ca.pem");
        if runtime.handle.ca_certificate_path() != expected_ca_path {
            let _ = runtime.handle.shutdown().await;
            anyhow::bail!("capture proxy returned an unexpected CA path");
        }
        if let Err(error) = self.persist_upstream_tls(session_id, &upstream_tls) {
            let _ = runtime.handle.shutdown().await;
            return Err(error);
        }
        let descriptor = CaptureProxyDescriptor {
            url: format!("http://{}", runtime.handle.address()),
            ca_endpoint: "/v1/proxy/ca",
            upstream_tls_mode,
        };
        let producer_id = format!("proxy-{}", Uuid::new_v4());
        let worker = tokio::spawn(ingest_proxy_events(
            self.inner.store.clone(),
            session_id.to_owned(),
            producer_id,
            runtime.events,
        ));
        active.insert(
            session_id.to_owned(),
            ManagedProxy {
                descriptor: descriptor.clone(),
                upstream_tls,
                handle: runtime.handle,
                worker,
            },
        );
        Ok(descriptor)
    }

    pub async fn descriptor(&self, session_id: &str) -> Option<CaptureProxyDescriptor> {
        self.inner
            .active
            .lock()
            .await
            .get(session_id)
            .map(|proxy| proxy.descriptor.clone())
    }

    pub async fn stop(&self, session_id: &str) -> anyhow::Result<()> {
        let managed = self.inner.active.lock().await.remove(session_id);
        let Some(managed) = managed else {
            return Ok(());
        };
        let ManagedProxy {
            handle, mut worker, ..
        } = managed;
        let dropped_events = handle.dropped_events();
        let shutdown_result = handle.shutdown().await;
        let drain_result = match tokio::time::timeout(SHUTDOWN_JOIN_LIMIT, &mut worker).await {
            Ok(result) => result.map_err(anyhow::Error::from),
            Err(_) => {
                worker.abort();
                let _ = worker.await;
                Err(anyhow::anyhow!("capture event drain timed out"))
            }
        };
        if dropped_events > 0 {
            tracing::warn!(%session_id, dropped_events, "capture proxy dropped events");
            persist_gap(
                self.inner.store.clone(),
                session_id.to_owned(),
                dropped_events,
                "capture queue remained full while the proxy stopped".into(),
            )
            .await;
        }
        match (shutdown_result, drain_result) {
            (Err(error), _) | (_, Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    pub async fn shutdown_all(&self) {
        let session_ids = self
            .inner
            .active
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut stops = JoinSet::new();
        for session_id in session_ids {
            let manager = self.clone();
            stops.spawn(async move {
                let result = manager.stop(&session_id).await;
                (session_id, result)
            });
        }
        while let Some(result) = stops.join_next().await {
            match result {
                Ok((session_id, Err(error))) => {
                    tracing::warn!(%session_id, %error, "failed to stop capture proxy cleanly");
                }
                Err(error) => {
                    tracing::warn!(%error, "capture proxy shutdown task failed");
                }
                Ok((_, Ok(()))) => {}
            }
        }
    }

    pub async fn ca_certificate(&self) -> anyhow::Result<String> {
        let path = self.inner.proxy_data_dir.join("proxy-ca.pem");
        tokio::fs::read_to_string(&path)
            .await
            .map_err(anyhow::Error::from)
    }

    pub fn upstream_tls(&self, session_id: &str) -> anyhow::Result<UpstreamTlsConfig> {
        let path = self.upstream_tls_path(session_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(UpstreamTlsConfig::Verify);
            }
            Err(error) => return Err(error.into()),
        };
        let persisted: PersistedUpstreamTls = serde_json::from_slice(&bytes)?;
        if persisted.version != UPSTREAM_TLS_FILE_VERSION {
            anyhow::bail!("unsupported upstream TLS settings version");
        }
        if persisted.session_id != session_id {
            anyhow::bail!("upstream TLS settings belong to another capture session");
        }
        let normalized = persisted.config.normalized()?;
        normalized.validate()?;
        if normalized != persisted.config {
            self.persist_upstream_tls(session_id, &normalized)?;
        }
        Ok(normalized)
    }

    pub fn upstream_tls_mode(&self, session_id: &str) -> anyhow::Result<String> {
        Ok(self.upstream_tls(session_id)?.mode().to_owned())
    }

    pub fn forget_upstream_tls(&self, session_id: &str) -> anyhow::Result<()> {
        match fs::remove_file(self.upstream_tls_path(session_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn persist_upstream_tls(
        &self,
        session_id: &str,
        config: &UpstreamTlsConfig,
    ) -> anyhow::Result<()> {
        let config = config.normalized()?;
        let bytes = serde_json::to_vec(&PersistedUpstreamTls {
            version: UPSTREAM_TLS_FILE_VERSION,
            session_id: session_id.to_owned(),
            config,
        })?;
        atomic_write(&self.upstream_tls_path(session_id), &bytes, true)
    }

    fn upstream_tls_path(&self, session_id: &str) -> PathBuf {
        let digest = blake3::hash(session_id.as_bytes()).to_hex().to_string();
        self.inner
            .proxy_data_dir
            .join("upstream-tls")
            .join(format!("{digest}.json"))
    }
}

async fn ingest_proxy_events(
    store: Arc<Store>,
    session_id: String,
    producer_id: String,
    mut receiver: tokio::sync::mpsc::Receiver<ProxyCapture>,
) {
    let mut http = Vec::<HttpCaptureEvent>::new();
    let mut websocket = Vec::<WebSocketFrameEvent>::new();
    let mut interval = tokio::time::interval(INGEST_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if http.len() >= INGEST_BATCH_ITEMS || websocket.len() >= INGEST_BATCH_ITEMS {
            if flush_pending(&store, &session_id, &producer_id, &mut http, &mut websocket)
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            continue;
        }
        tokio::select! {
            event = receiver.recv() => {
                match event {
                    Some(ProxyCapture::Http(event)) => http.push(event),
                    Some(ProxyCapture::WebSocket(event)) => websocket.push(event),
                    Some(ProxyCapture::Gap(gap)) => {
                        let input = CaptureGapInput {
                            session_id: session_id.clone(),
                            source: "proxy".into(),
                            dropped_events: gap.dropped_events,
                            reason: gap.reason.into(),
                            observed_at: None,
                            producer_id: None,
                            sequence: None,
                        };
                        let store = store.clone();
                        match tokio::task::spawn_blocking(move || store.record_gap(input)).await {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => tracing::warn!(%error, "capture gap was not stored"),
                            Err(error) => tracing::warn!(%error, "capture gap writer task failed"),
                        }
                    }
                    None => break,
                }
            }
            _ = interval.tick() => {
                if let Err(error) = flush_pending(
                    &store,
                    &session_id,
                    &producer_id,
                    &mut http,
                    &mut websocket,
                ).await {
                    tracing::warn!(%session_id, %error, "capture event batch was not stored yet");
                }
            }
        }
    }

    let deadline = tokio::time::Instant::now() + SHUTDOWN_DRAIN_LIMIT;
    while (!http.is_empty() || !websocket.is_empty()) && tokio::time::Instant::now() < deadline {
        if flush_pending(&store, &session_id, &producer_id, &mut http, &mut websocket)
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    if !http.is_empty() || !websocket.is_empty() {
        let remaining = (http.len() + websocket.len()) as u64;
        tracing::error!(
            %session_id,
            http_events = http.len(),
            websocket_events = websocket.len(),
            "capture events remained after the shutdown drain limit"
        );
        persist_shutdown_drain_gap(store, session_id, remaining).await;
    }
}

async fn persist_shutdown_drain_gap(store: Arc<Store>, session_id: String, remaining: u64) {
    persist_gap(
        store,
        session_id,
        remaining,
        "capture events remained after the shutdown drain limit".into(),
    )
    .await;
}

async fn flush_pending(
    store: &Arc<Store>,
    session_id: &str,
    producer_id: &str,
    http: &mut Vec<HttpCaptureEvent>,
    websocket: &mut Vec<WebSocketFrameEvent>,
) -> anyhow::Result<()> {
    if !http.is_empty() {
        let events = http.clone();
        let ingest_store = store.clone();
        let batch = HttpIngestBatch {
            producer_id: producer_id.to_owned(),
            session_id: session_id.to_owned(),
            events: events.clone(),
        };
        let results =
            tokio::task::spawn_blocking(move || ingest_store.ingest_http(batch)).await??;
        let rejected_events = events
            .iter()
            .zip(&results)
            .filter(|(_, result)| matches!(result.status, IngestStatus::Rejected))
            .map(|(event, _)| event.clone())
            .collect::<Vec<_>>();
        let rejected_results = if rejected_events.is_empty() {
            Vec::new()
        } else {
            tokio::task::yield_now().await;
            let retry_store = store.clone();
            let retry = HttpIngestBatch {
                producer_id: producer_id.to_owned(),
                session_id: session_id.to_owned(),
                events: rejected_events,
            };
            tokio::task::spawn_blocking(move || retry_store.ingest_http(retry)).await??
        };
        http.clear();
        if let Some(rejection) = rejection_summary("HTTP", &rejected_results) {
            tracing::warn!(
                %session_id,
                rejected = rejection.count,
                reason = %rejection.reason,
                "proxy capture events were rejected after retry"
            );
            persist_rejection_gap(
                store.clone(),
                session_id.to_owned(),
                producer_id.to_owned(),
                rejection,
            )
            .await;
        }
    }
    if !websocket.is_empty() {
        let frames = websocket.clone();
        let ingest_store = store.clone();
        let batch = WebSocketIngestBatch {
            producer_id: producer_id.to_owned(),
            session_id: session_id.to_owned(),
            frames: frames.clone(),
        };
        let results =
            tokio::task::spawn_blocking(move || ingest_store.ingest_websocket(batch)).await??;
        let rejected_frames = frames
            .iter()
            .zip(&results)
            .filter(|(_, result)| matches!(result.status, IngestStatus::Rejected))
            .map(|(frame, _)| frame.clone())
            .collect::<Vec<_>>();
        let rejected_results = if rejected_frames.is_empty() {
            Vec::new()
        } else {
            tokio::task::yield_now().await;
            let retry_store = store.clone();
            let retry = WebSocketIngestBatch {
                producer_id: producer_id.to_owned(),
                session_id: session_id.to_owned(),
                frames: rejected_frames,
            };
            tokio::task::spawn_blocking(move || retry_store.ingest_websocket(retry)).await??
        };
        websocket.clear();
        if let Some(rejection) = rejection_summary("WebSocket", &rejected_results) {
            tracing::warn!(
                %session_id,
                rejected = rejection.count,
                reason = %rejection.reason,
                "proxy capture events were rejected after retry"
            );
            persist_rejection_gap(
                store.clone(),
                session_id.to_owned(),
                producer_id.to_owned(),
                rejection,
            )
            .await;
        }
    }
    Ok(())
}

async fn persist_gap(store: Arc<Store>, session_id: String, dropped_events: u64, reason: String) {
    let input = CaptureGapInput {
        session_id,
        source: "proxy".into(),
        dropped_events,
        reason,
        observed_at: None,
        producer_id: None,
        sequence: None,
    };
    match tokio::task::spawn_blocking(move || store.record_gap(input)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::warn!(%error, "capture gap was not stored"),
        Err(error) => tracing::warn!(%error, "capture gap writer task failed"),
    }
}

#[derive(Debug)]
struct RejectionSummary {
    count: usize,
    first_sequence: Option<u64>,
    reason: String,
}

fn rejection_summary(kind: &str, results: &[IngestResult]) -> Option<RejectionSummary> {
    let rejected = results
        .iter()
        .filter(|result| matches!(result.status, IngestStatus::Rejected))
        .collect::<Vec<_>>();
    if rejected.is_empty() {
        return None;
    }
    let details = rejected
        .iter()
        .take(4)
        .map(|result| {
            let error = result
                .error
                .as_deref()
                .unwrap_or("traffic store did not provide a reason")
                .replace(['\r', '\n'], " ");
            let bounded = error.chars().take(300).collect::<String>();
            format!("sequence {}: {bounded}", result.sequence)
        })
        .collect::<Vec<_>>()
        .join("; ");
    Some(RejectionSummary {
        count: rejected.len(),
        first_sequence: rejected.first().map(|result| result.sequence),
        reason: format!("proxy {kind} events were rejected after one retry: {details}"),
    })
}

async fn persist_rejection_gap(
    store: Arc<Store>,
    session_id: String,
    producer_id: String,
    rejection: RejectionSummary,
) {
    let input = CaptureGapInput {
        session_id,
        source: "proxy".into(),
        dropped_events: rejection.count as u64,
        reason: rejection.reason,
        observed_at: Some(
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .expect("RFC 3339 formatting is supported"),
        ),
        producer_id: Some(producer_id),
        sequence: rejection.first_sequence,
    };
    match tokio::task::spawn_blocking(move || store.record_gap(input)).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => tracing::warn!(%error, "capture rejection gap was not stored"),
        Err(error) => tracing::warn!(%error, "capture rejection gap writer task failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CaptureGapQuery, CapturedRequest, CreateSession, TrafficQuery};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test]
    async fn proxy_session_forwards_and_persists_into_its_capture_session() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path()).unwrap());
        let session = store
            .create_session(CreateSession {
                label: "proxy".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let manager = CaptureManager::new(store.clone(), temp.path());
        let proxy = manager
            .start(&session.id, Some(UpstreamTlsConfig::Verify))
            .await
            .unwrap();
        let proxy_address = proxy.url.strip_prefix("http://").unwrap();

        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (mut stream, _) = origin.accept().await.unwrap();
            let mut request = [0_u8; 2_048];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(proxy_address).await.unwrap();
        client
            .write_all(
                format!(
                    "GET http://{origin_address}/account HTTP/1.1\r\nHost: {origin_address}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).contains("200 OK"));
        origin_task.await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = store
                    .traffic(TrafficQuery {
                        session_id: Some(session.id.clone()),
                        ..TrafficQuery::default()
                    })
                    .unwrap();
                if page.items.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        manager.stop(&session.id).await.unwrap();
    }

    #[tokio::test]
    async fn custom_upstream_ca_survives_pause_and_manager_restart() {
        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_certificate = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::new(ca_params, ca_key);
        let server_key = rcgen::KeyPair::generate().unwrap();
        let mut server_params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params.signed_by(&server_key, &issuer).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![server_certificate.der().clone()],
                rustls::pki_types::PrivateKeyDer::Pkcs8(
                    rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
                ),
            )
            .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = origin.accept().await.unwrap();
                let mut stream = acceptor.accept(stream).await.unwrap();
                let mut request = [0_u8; 2_048];
                let read = stream.read(&mut request).await.unwrap();
                assert!(String::from_utf8_lossy(&request[..read]).contains("GET /persisted"));
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .unwrap();
            }
        });

        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path()).unwrap());
        let session = store
            .create_session(CreateSession {
                label: "custom CA".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let target = format!("https://localhost:{}/persisted", origin_address.port());
        let manager = CaptureManager::new(store.clone(), temp.path());
        let first = manager
            .start(
                &session.id,
                Some(UpstreamTlsConfig::CustomCa {
                    ca_pem: ca_certificate.pem(),
                }),
            )
            .await
            .unwrap();
        assert_eq!(first.upstream_tls_mode, "customCa");
        assert_https_through_proxy(
            &first.url,
            &manager.ca_certificate().await.unwrap(),
            &target,
        )
        .await;
        manager.stop(&session.id).await.unwrap();

        let restarted = CaptureManager::new(store, temp.path());
        let second = restarted.start(&session.id, None).await.unwrap();
        assert_eq!(second.upstream_tls_mode, "customCa");
        assert_https_through_proxy(
            &second.url,
            &restarted.ca_certificate().await.unwrap(),
            &target,
        )
        .await;
        restarted.stop(&session.id).await.unwrap();
        origin_task.await.unwrap();
    }

    async fn assert_https_through_proxy(proxy_url: &str, capture_ca: &str, target: &str) {
        let client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(proxy_url).unwrap())
            .add_root_certificate(reqwest::Certificate::from_pem(capture_ca.as_bytes()).unwrap())
            .build()
            .unwrap();
        let response = client.get(target).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "ok");
    }

    #[tokio::test]
    async fn shutdown_drain_loss_is_persisted_as_a_gap() {
        assert!(SHUTDOWN_JOIN_LIMIT > SHUTDOWN_DRAIN_LIMIT);
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path()).unwrap());
        let session = store
            .create_session(CreateSession {
                label: "drain".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();

        persist_shutdown_drain_gap(store.clone(), session.id.clone(), 5).await;

        let gaps = store
            .capture_gaps(CaptureGapQuery {
                session_id: Some(session.id),
                limit: None,
            })
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].dropped_events, 5);
        assert_eq!(
            gaps[0].reason,
            "capture events remained after the shutdown drain limit"
        );
    }

    #[tokio::test]
    async fn rejected_proxy_events_are_retried_and_keep_the_store_reason() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path()).unwrap());
        let session = store
            .create_session(CreateSession {
                label: "rejection".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let mut http = vec![HttpCaptureEvent {
            sequence: 5,
            captured_at: None,
            duration_ms: None,
            request: CapturedRequest {
                method: "POST".into(),
                url: "not a URL".into(),
                headers: Vec::new(),
                body: None,
            },
            response: None,
            error: None,
            tab_id: None,
            initiator: None,
            parent_sequence: None,
            metadata: serde_json::json!({}),
        }];
        let mut websocket = Vec::new();

        flush_pending(
            &store,
            &session.id,
            "proxy-rejection-test",
            &mut http,
            &mut websocket,
        )
        .await
        .unwrap();

        assert!(http.is_empty());
        let gaps = store
            .capture_gaps(CaptureGapQuery {
                session_id: Some(session.id),
                limit: None,
            })
            .unwrap();
        assert_eq!(gaps.len(), 1);
        assert!(gaps[0].id.starts_with("gap-"));
        assert!(gaps[0].reason.contains("sequence 5"));
        assert!(gaps[0].reason.contains("request URL is invalid"));
        assert!(gaps[0].reason.contains("after one retry"));
    }

    #[tokio::test]
    async fn shutdown_all_stops_every_active_proxy() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(temp.path()).unwrap());
        let first = store
            .create_session(CreateSession {
                label: "first".into(),
                source: "proxy".into(),
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
        let manager = CaptureManager::new(store, temp.path());
        manager
            .start(&first.id, Some(UpstreamTlsConfig::Verify))
            .await
            .unwrap();
        manager
            .start(&second.id, Some(UpstreamTlsConfig::Verify))
            .await
            .unwrap();

        manager.shutdown_all().await;

        assert!(manager.descriptor(&first.id).await.is_none());
        assert!(manager.descriptor(&second.id).await.is_none());
    }
}
