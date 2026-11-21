use std::convert::Infallible;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::{Context as _, bail};
use base64::Engine as _;
use bytes::{Buf, Bytes};
use fastwebsockets::{OpCode, Role, WebSocketError, after_handshake_split};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Empty, Full};
use http_mitm_proxy::default_client::Upgraded;
use http_mitm_proxy::hyper::body::{Body, Frame, Incoming, SizeHint};
use http_mitm_proxy::hyper::client;
use http_mitm_proxy::hyper::header::{
    self, CONNECTION, SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_PROTOCOL, UPGRADE,
};
use http_mitm_proxy::hyper::server;
use http_mitm_proxy::hyper::service::{HttpService as _, service_fn};
use http_mitm_proxy::hyper::{Request, Response, StatusCode, Uri, Version};
use http_mitm_proxy::moka::sync::Cache;
use http_mitm_proxy::{DefaultClient, MitmProxy, RemoteAddr};
use hyper_util::rt::{TokioExecutor, TokioIo};
use pin_project_lite::pin_project;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, DnValue, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::model::{
    BodyEncoding, BodyState, CapturedBody, CapturedRequest, CapturedResponse, HeaderField,
    HttpCaptureEvent, WebSocketDirection, WebSocketEventKind, WebSocketFrameEvent,
};

type DynError = Box<dyn std::error::Error + Send + Sync>;
type ProxyBody = BoxBody<Bytes, DynError>;

const CA_FILE_VERSION: u32 = 1;
const DEFAULT_EVENT_CAPACITY: usize = 2_048;
const DEFAULT_BODY_LIMIT: usize = 4 * 1024 * 1024;
const DEFAULT_CERT_CACHE: u64 = 512;

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub bind: SocketAddr,
    pub event_capacity: usize,
    pub max_body_bytes: usize,
    pub certificate_cache_entries: u64,
    pub upstream_tls: UpstreamTlsConfig,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            event_capacity: DEFAULT_EVENT_CAPACITY,
            max_body_bytes: DEFAULT_BODY_LIMIT,
            certificate_cache_entries: DEFAULT_CERT_CACHE,
            upstream_tls: UpstreamTlsConfig::Verify,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "mode")]
pub enum UpstreamTlsConfig {
    #[default]
    #[serde(rename = "verify")]
    Verify,
    #[serde(rename = "customCa")]
    CustomCa {
        #[serde(rename = "caPem")]
        ca_pem: String,
    },
    #[serde(rename = "insecure")]
    Insecure,
}

impl UpstreamTlsConfig {
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::CustomCa { .. } => "customCa",
            Self::Insecure => "insecure",
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Verify => Ok(()),
            Self::CustomCa { .. } | Self::Insecure => CustomTlsClient::new(self).map(drop),
        }
    }

    pub fn normalized(&self) -> anyhow::Result<Self> {
        match self {
            Self::CustomCa { ca_pem } => Ok(Self::CustomCa {
                ca_pem: normalize_upstream_ca_pem(ca_pem)?,
            }),
            Self::Verify | Self::Insecure => Ok(self.clone()),
        }
    }
}

fn parse_upstream_ca_certificates(ca_pem: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    if ca_pem.trim().is_empty() {
        bail!("the upstream CA PEM cannot be empty");
    }
    let mut reader = BufReader::new(ca_pem.as_bytes());
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context("the upstream CA PEM is invalid")?;
    if certificates.is_empty() {
        bail!("the upstream CA PEM does not contain a certificate");
    }
    Ok(certificates)
}

fn normalize_upstream_ca_pem(ca_pem: &str) -> anyhow::Result<String> {
    let certificates = parse_upstream_ca_certificates(ca_pem)?;
    let mut normalized = String::new();
    for certificate in certificates {
        normalized.push_str("-----BEGIN CERTIFICATE-----\n");
        let encoded = base64::engine::general_purpose::STANDARD.encode(certificate.as_ref());
        for line in encoded.as_bytes().chunks(64) {
            normalized.push_str(std::str::from_utf8(line).expect("base64 output is ASCII"));
            normalized.push('\n');
        }
        normalized.push_str("-----END CERTIFICATE-----\n");
    }
    Ok(normalized)
}

#[derive(Clone, Debug)]
pub enum ProxyCapture {
    Http(HttpCaptureEvent),
    WebSocket(WebSocketFrameEvent),
    Gap(ProxyGap),
}

#[derive(Clone, Debug)]
pub struct ProxyGap {
    pub dropped_events: u64,
    pub reason: &'static str,
}

pub struct ProxyRuntime {
    pub handle: ProxyHandle,
    pub events: mpsc::Receiver<ProxyCapture>,
}

pub struct ProxyHandle {
    address: SocketAddr,
    ca_certificate_path: PathBuf,
    dropped_events: Arc<AtomicU64>,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl ProxyHandle {
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn ca_certificate_path(&self) -> &Path {
        &self.ca_certificate_path
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(true);
        }
        if let Some(task) = self.task.take() {
            task.await.context("proxy task failed")?;
        }
        Ok(())
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(true);
        }
    }
}

#[derive(Clone)]
enum UpstreamClient {
    Default(DefaultClient),
    Custom(CustomTlsClient),
}

impl UpstreamClient {
    fn new(config: &UpstreamTlsConfig) -> anyhow::Result<Self> {
        match config {
            UpstreamTlsConfig::Verify => Ok(Self::Default(DefaultClient::new().with_upgrades())),
            UpstreamTlsConfig::CustomCa { .. } | UpstreamTlsConfig::Insecure => {
                Ok(Self::Custom(CustomTlsClient::new(config)?))
            }
        }
    }

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> anyhow::Result<(
        Response<Incoming>,
        Option<JoinHandle<Result<Upgraded, http_mitm_proxy::hyper::Error>>>,
    )>
    where
        B: Body<Data = Bytes> + Send + Sync + 'static,
        B::Data: Send + Buf,
        B::Error: Into<DynError>,
    {
        match self {
            Self::Default(client) => client
                .send_request(request)
                .await
                .map_err(anyhow::Error::from),
            Self::Custom(client) => client.send_request(request).await,
        }
    }
}

#[derive(Clone)]
struct CustomTlsClient {
    no_alpn: tokio_rustls::TlsConnector,
    with_h2: tokio_rustls::TlsConnector,
}

impl CustomTlsClient {
    fn new(config: &UpstreamTlsConfig) -> anyhow::Result<Self> {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let UpstreamTlsConfig::CustomCa { ca_pem } = config {
            for certificate in parse_upstream_ca_certificates(ca_pem)? {
                roots
                    .add(certificate)
                    .context("the upstream CA certificate is invalid")?;
            }
        }

        let mut base = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        if matches!(config, UpstreamTlsConfig::Insecure) {
            base.dangerous()
                .set_certificate_verifier(Arc::new(AcceptInvalidServerCertificate::new()));
        }
        let mut h2 = base.clone();
        h2.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            no_alpn: tokio_rustls::TlsConnector::from(Arc::new(base)),
            with_h2: tokio_rustls::TlsConnector::from(Arc::new(h2)),
        })
    }

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> anyhow::Result<(
        Response<Incoming>,
        Option<JoinHandle<Result<Upgraded, http_mitm_proxy::hyper::Error>>>,
    )>
    where
        B: Body<Data = Bytes> + Send + Sync + 'static,
        B::Data: Send + Buf,
        B::Error: Into<DynError>,
    {
        let target_uri = request.uri().clone();
        let (host, port, is_tls) = upstream_host_port(&target_uri)?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("failed to connect to upstream {host}:{port}"))?;
        let _ = tcp.set_nodelay(true);
        let mut sender = if is_tls {
            let connector = if request.version() == Version::HTTP_2 {
                &self.with_h2
            } else {
                &self.no_alpn
            };
            let server_name = ServerName::try_from(host.clone())
                .map_err(|_| anyhow::anyhow!("invalid upstream TLS server name {host}"))?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .with_context(|| format!("upstream TLS handshake failed for {host}:{port}"))?;
            if tls.get_ref().1.alpn_protocol() == Some(b"h2") {
                let (sender, connection) = client::conn::http2::Builder::new(TokioExecutor::new())
                    .handshake(TokioIo::new(tls))
                    .await
                    .with_context(|| {
                        format!("upstream HTTP/2 handshake failed for {target_uri}")
                    })?;
                tokio::spawn(connection);
                CustomSender::Http2(sender)
            } else {
                let (sender, connection) = client::conn::http1::Builder::new()
                    .preserve_header_case(true)
                    .title_case_headers(true)
                    .handshake(TokioIo::new(tls))
                    .await
                    .with_context(|| {
                        format!("upstream HTTP/1 handshake failed for {target_uri}")
                    })?;
                tokio::spawn(connection.with_upgrades());
                CustomSender::Http1(sender)
            }
        } else {
            let (sender, connection) = client::conn::http1::Builder::new()
                .preserve_header_case(true)
                .title_case_headers(true)
                .handshake(TokioIo::new(tcp))
                .await
                .with_context(|| format!("upstream HTTP/1 handshake failed for {target_uri}"))?;
            tokio::spawn(connection.with_upgrades());
            CustomSender::Http1(sender)
        };

        let (request_parts, request_body) = request.into_parts();
        let boxed_request = Request::from_parts(
            request_parts.clone(),
            request_body.map_err(Into::into).boxed(),
        );
        let response = sender
            .send_request(boxed_request)
            .await
            .with_context(|| format!("upstream request failed for {target_uri}"))?;
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Ok((response, None));
        }

        let (response_parts, response_body) = response.into_parts();
        let client_request = Request::from_parts(request_parts, Empty::<Bytes>::new());
        let server_response = Response::from_parts(response_parts.clone(), Empty::<Bytes>::new());
        let upgrade = tokio::spawn(async move {
            let client = http_mitm_proxy::hyper::upgrade::on(client_request).await?;
            let server = http_mitm_proxy::hyper::upgrade::on(server_response).await?;
            Ok(Upgraded {
                client: TokioIo::new(client),
                server: TokioIo::new(server),
            })
        });
        Ok((
            Response::from_parts(response_parts, response_body),
            Some(upgrade),
        ))
    }
}

type UpstreamBody = BoxBody<Bytes, DynError>;

enum CustomSender {
    Http1(client::conn::http1::SendRequest<UpstreamBody>),
    Http2(client::conn::http2::SendRequest<UpstreamBody>),
}

impl CustomSender {
    async fn send_request(
        &mut self,
        mut request: Request<UpstreamBody>,
    ) -> Result<Response<Incoming>, http_mitm_proxy::hyper::Error> {
        match self {
            Self::Http1(sender) => {
                if request.version() == Version::HTTP_2
                    && let Some(authority) = request.uri().authority().cloned()
                    && let Ok(host) = authority.as_str().parse::<header::HeaderValue>()
                {
                    request.headers_mut().insert(header::HOST, host);
                }
                remove_upstream_authority(&mut request);
                sender.send_request(request).await
            }
            Self::Http2(sender) => {
                if request.version() != Version::HTTP_2 {
                    request.headers_mut().remove(header::HOST);
                }
                sender.send_request(request).await
            }
        }
    }
}

fn upstream_host_port(uri: &Uri) -> anyhow::Result<(String, u16, bool)> {
    let host = uri
        .host()
        .ok_or_else(|| anyhow::anyhow!("upstream URL has no host: {uri}"))?
        .to_owned();
    let is_tls = uri.scheme_str() == Some("https");
    Ok((
        host,
        uri.port_u16().unwrap_or(if is_tls { 443 } else { 80 }),
        is_tls,
    ))
}

fn remove_upstream_authority<B>(request: &mut Request<B>) {
    let mut parts = request.uri().clone().into_parts();
    parts.scheme = None;
    parts.authority = None;
    if let Ok(uri) = Uri::from_parts(parts) {
        *request.uri_mut() = uri;
    }
}

#[derive(Debug)]
struct AcceptInvalidServerCertificate {
    schemes: Vec<SignatureScheme>,
}

impl AcceptInvalidServerCertificate {
    fn new() -> Self {
        Self {
            schemes: rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
        }
    }
}

impl ServerCertVerifier for AcceptInvalidServerCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _certificate: &CertificateDer<'_>,
        _signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _certificate: &CertificateDer<'_>,
        _signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

pub async fn start_proxy(data_dir: &Path, config: ProxyConfig) -> anyhow::Result<ProxyRuntime> {
    if !config.bind.ip().is_loopback() {
        bail!("the capture proxy may only bind to a loopback address");
    }
    if config.event_capacity == 0 {
        bail!("the capture event queue cannot be empty");
    }
    if config.max_body_bytes == 0 {
        bail!("the capture body limit cannot be zero");
    }
    if config.certificate_cache_entries == 0 {
        bail!("the certificate cache cannot be empty");
    }

    fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;
    let ca = load_or_create_ca(data_dir)?;
    let listener = TcpListener::bind(config.bind)
        .await
        .with_context(|| format!("failed to bind capture proxy at {}", config.bind))?;
    let address = listener.local_addr()?;
    let (event_tx, events) = mpsc::channel(config.event_capacity);
    let sink = CaptureSink::new(event_tx);
    let dropped_events = sink.dropped.clone();
    let proxy = Arc::new(MitmProxy::new(
        Some(ca.issuer),
        Some(Cache::new(config.certificate_cache_entries)),
    ));
    let client = UpstreamClient::new(&config.upstream_tls)?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let max_body_bytes = config.max_body_bytes;
    let task = tokio::spawn(run_listener(
        listener,
        proxy,
        client,
        sink,
        max_body_bytes,
        shutdown_rx,
    ));

    Ok(ProxyRuntime {
        handle: ProxyHandle {
            address,
            ca_certificate_path: ca.certificate_path,
            dropped_events,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        },
        events,
    })
}

async fn run_listener(
    listener: TcpListener,
    proxy: Arc<MitmProxy<Issuer<'static, KeyPair>>>,
    client: UpstreamClient,
    sink: CaptureSink,
    max_body_bytes: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    let relay_shutdown = CancellationToken::new();
    let relays = TaskTracker::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, remote_addr)) => {
                        let proxy = proxy.clone();
                        let client = client.clone();
                        let sink = sink.clone();
                        let relays = relays.clone();
                        let relay_shutdown = relay_shutdown.clone();
                        connections.spawn(async move {
                            let capture_service = service_fn(move |request| {
                                capture_request(
                                    request,
                                    client.clone(),
                                    sink.clone(),
                                    max_body_bytes,
                                    relays.clone(),
                                    relay_shutdown.clone(),
                                )
                            });
                            let connection = server::conn::http1::Builder::new()
                                .preserve_header_case(true)
                                .title_case_headers(true)
                                .serve_connection(
                                    TokioIo::new(stream),
                                    service_fn(move |mut request| {
                                        request.extensions_mut().insert(RemoteAddr(remote_addr));
                                        MitmProxy::wrap_service(
                                            proxy.clone(),
                                            capture_service.clone(),
                                        )
                                        .call(request)
                                    }),
                                )
                                .with_upgrades();
                            if let Err(error) = connection.await {
                                tracing::debug!(%remote_addr, %error, "capture proxy connection ended");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(%error, "capture proxy accept failed");
                    }
                }
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::debug!(%error, "capture proxy connection task failed");
                }
            }
        }
    }
    relay_shutdown.cancel();
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    relays.close();
    relays.wait().await;
}

async fn capture_request(
    mut request: Request<Incoming>,
    client: UpstreamClient,
    sink: CaptureSink,
    max_body_bytes: usize,
    relays: TaskTracker,
    relay_shutdown: CancellationToken,
) -> Result<Response<ProxyBody>, Infallible> {
    let remote_addr = request
        .extensions()
        .get::<RemoteAddr>()
        .map(|remote| remote.0.to_string());
    let request_version = format!("{:?}", request.version());
    let is_websocket = is_websocket_upgrade(&request);
    let headers = capture_headers(request.headers());
    let compression_removed = is_websocket
        && request
            .headers_mut()
            .remove(SEC_WEBSOCKET_EXTENSIONS)
            .is_some();
    let request_capture = CapturedRequest {
        method: request.method().as_str().into(),
        url: request.uri().to_string(),
        headers,
        body: None,
    };
    let mut metadata = Map::new();
    metadata.insert("source".into(), Value::String("proxy".into()));
    metadata.insert("clientHttpVersion".into(), Value::String(request_version));
    if let Some(remote_addr) = remote_addr {
        metadata.insert("clientAddress".into(), Value::String(remote_addr));
    }
    if compression_removed {
        metadata.insert("websocketCompressionRemoved".into(), Value::Bool(true));
    }
    let exchange = ExchangeCapture::new(
        sink.clone(),
        request_capture,
        Value::Object(metadata),
        max_body_bytes,
    );
    let (parts, body) = request.into_parts();
    let request = Request::from_parts(parts, TeeBody::new(body, exchange.tap(BodySide::Request)));
    let request_url = request.uri().to_string();
    let websocket_url = is_websocket.then(|| websocket_url(&request_url));

    match client.send_request(request).await {
        Ok((mut response, upgrade)) => {
            let response_headers = capture_headers(response.headers());
            let response_compression_removed = is_websocket
                && response
                    .headers_mut()
                    .remove(SEC_WEBSOCKET_EXTENSIONS)
                    .is_some();
            let subprotocol = response
                .headers()
                .get(SEC_WEBSOCKET_PROTOCOL)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            exchange.set_response(
                CapturedResponse {
                    status: response.status().as_u16(),
                    headers: response_headers,
                    body: None,
                },
                format!("{:?}", response.version()),
                response_compression_removed,
            );
            if let Some(upgrade) = upgrade {
                let connection_id = Uuid::now_v7().to_string();
                sink.emit(ProxyCapture::WebSocket(WebSocketFrameEvent {
                    sequence: sink.next_sequence(),
                    connection_id: connection_id.clone(),
                    captured_at: None,
                    kind: WebSocketEventKind::Open,
                    direction: None,
                    opcode: None,
                    payload: None,
                    url: websocket_url.clone(),
                    subprotocol: subprotocol.clone(),
                    close_code: None,
                    close_reason: None,
                    error: None,
                }));
                relays.spawn(relay_websocket(
                    upgrade,
                    sink.clone(),
                    connection_id,
                    websocket_url.unwrap_or(request_url),
                    max_body_bytes,
                    relay_shutdown.child_token(),
                ));
            }
            let (parts, body) = response.into_parts();
            let body = TeeBody::new(body, exchange.tap(BodySide::Response))
                .map_err(|error| -> DynError { Box::new(error) })
                .boxed();
            Ok(Response::from_parts(parts, body))
        }
        Err(error) => {
            exchange.set_upstream_error(error.to_string());
            let body = Full::new(Bytes::from_static(b"upstream request failed"))
                .map_err(|never| -> DynError { match never {} })
                .boxed();
            Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(body)
                .expect("static proxy error response is valid"))
        }
    }
}

fn websocket_url(value: &str) -> String {
    let Ok(mut url) = url::Url::parse(value) else {
        return value.into();
    };
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => return value.into(),
    };
    if url.set_scheme(scheme).is_err() {
        value.into()
    } else {
        url.into()
    }
}

fn is_websocket_upgrade(request: &Request<Incoming>) -> bool {
    request
        .headers()
        .get(UPGRADE)
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
        && request.headers().get_all(CONNECTION).iter().any(|value| {
            value
                .as_bytes()
                .split(|byte| *byte == b',')
                .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
        })
}

async fn relay_websocket(
    upgrade: JoinHandle<Result<Upgraded, http_mitm_proxy::hyper::Error>>,
    sink: CaptureSink,
    connection_id: String,
    url: String,
    max_body_bytes: usize,
    shutdown: CancellationToken,
) {
    let upgraded = tokio::select! {
        _ = shutdown.cancelled() => return,
        upgraded = upgrade => upgraded,
    };
    let Upgraded { client, server } = match upgraded {
        Ok(Ok(upgraded)) => upgraded,
        Ok(Err(error)) => {
            emit_websocket_error(&sink, &connection_id, &url, &error.to_string());
            return;
        }
        Err(error) => {
            emit_websocket_error(&sink, &connection_id, &url, &error.to_string());
            return;
        }
    };

    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let (mut client_read, client_write) =
        after_handshake_split(client_read, client_write, Role::Server);
    let (mut server_read, server_write) =
        after_handshake_split(server_read, server_write, Role::Client);
    client_read.set_auto_close(false);
    client_read.set_auto_pong(false);
    server_read.set_auto_close(false);
    server_read.set_auto_pong(false);

    let mut client_to_server = Box::pin(forward_websocket_frames(
        client_read,
        server_write,
        sink.clone(),
        connection_id.clone(),
        url.clone(),
        WebSocketDirection::ClientToServer,
        max_body_bytes,
    ));
    let mut server_to_client = Box::pin(forward_websocket_frames(
        server_read,
        client_write,
        sink.clone(),
        connection_id.clone(),
        url.clone(),
        WebSocketDirection::ServerToClient,
        max_body_bytes,
    ));
    let result = tokio::select! {
        _ = shutdown.cancelled() => return,
        client_result = &mut client_to_server => {
            match client_result {
                Ok(()) => tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    &mut server_to_client,
                )
                .await
                .ok(),
                Err(error) => Some(Err(error)),
            }
        }
        server_result = &mut server_to_client => {
            match server_result {
                Ok(()) => tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    &mut client_to_server,
                )
                .await
                .ok(),
                Err(error) => Some(Err(error)),
            }
        }
    };
    if let Some(Err(error)) = result {
        emit_websocket_error(&sink, &connection_id, &url, &error.to_string());
    } else if result.is_none() {
        emit_websocket_error(
            &sink,
            &connection_id,
            &url,
            "websocket close handshake timed out",
        );
    }
}

async fn forward_websocket_frames<R, W>(
    mut reader: fastwebsockets::WebSocketRead<R>,
    mut writer: fastwebsockets::WebSocketWrite<W>,
    sink: CaptureSink,
    connection_id: String,
    url: String,
    direction: WebSocketDirection,
    max_body_bytes: usize,
) -> Result<(), WebSocketError>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    loop {
        let mut no_obligation = |_frame| async { Ok::<(), std::io::Error>(()) };
        let frame = reader.read_frame(&mut no_obligation).await?;
        let opcode = frame.opcode;
        let payload = frame.payload.to_vec();
        let (close_code, close_reason) = if opcode == OpCode::Close {
            parse_close_payload(&payload)
        } else {
            (None, None)
        };
        sink.emit(ProxyCapture::WebSocket(WebSocketFrameEvent {
            sequence: sink.next_sequence(),
            connection_id: connection_id.clone(),
            captured_at: None,
            kind: if opcode == OpCode::Close {
                WebSocketEventKind::Close
            } else {
                WebSocketEventKind::Frame
            },
            direction: Some(direction),
            opcode: Some(opcode as u8),
            payload: Some(captured_bytes(&payload, max_body_bytes)),
            url: Some(url.clone()),
            subprotocol: None,
            close_code,
            close_reason,
            error: None,
        }));
        writer.write_frame(frame).await?;
        if opcode == OpCode::Close {
            break;
        }
    }
    Ok(())
}

fn emit_websocket_error(sink: &CaptureSink, connection_id: &str, url: &str, error: &str) {
    sink.emit(ProxyCapture::WebSocket(WebSocketFrameEvent {
        sequence: sink.next_sequence(),
        connection_id: connection_id.into(),
        captured_at: None,
        kind: WebSocketEventKind::Error,
        direction: None,
        opcode: None,
        payload: None,
        url: Some(url.into()),
        subprotocol: None,
        close_code: None,
        close_reason: None,
        error: Some(error.into()),
    }));
}

fn parse_close_payload(payload: &[u8]) -> (Option<u16>, Option<String>) {
    if payload.len() < 2 {
        return (None, None);
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    let reason = std::str::from_utf8(&payload[2..]).ok().map(str::to_owned);
    (Some(code), reason)
}

fn captured_bytes(bytes: &[u8], max_body_bytes: usize) -> CapturedBody {
    let captured = &bytes[..bytes.len().min(max_body_bytes)];
    CapturedBody {
        encoding: BodyEncoding::Base64,
        data: base64::engine::general_purpose::STANDARD.encode(captured),
        state: if captured.len() == bytes.len() {
            BodyState::Complete
        } else {
            BodyState::Truncated
        },
        reason: (captured.len() != bytes.len()).then(|| "proxy body limit reached".into()),
        original_size: Some(bytes.len() as u64),
    }
}

#[derive(Clone)]
struct CaptureSink {
    sender: mpsc::Sender<ProxyCapture>,
    sequence: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
}

impl CaptureSink {
    fn new(sender: mpsc::Sender<ProxyCapture>) -> Self {
        Self {
            sender,
            sequence: Arc::new(AtomicU64::new(1)),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::Relaxed)
    }

    fn emit(&self, event: ProxyCapture) {
        let dropped = self.dropped.swap(0, Ordering::AcqRel);
        if dropped > 0
            && self
                .sender
                .try_send(ProxyCapture::Gap(ProxyGap {
                    dropped_events: dropped,
                    reason: "capture queue full",
                }))
                .is_err()
        {
            self.dropped.fetch_add(dropped + 1, Ordering::Relaxed);
            return;
        }
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy)]
enum BodySide {
    Request,
    Response,
}

struct BodyAccumulator {
    bytes: Vec<u8>,
    original_size: u64,
    state: BodyState,
    reason: Option<String>,
}

impl BodyAccumulator {
    fn new(max_body_bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(max_body_bytes.min(64 * 1024)),
            original_size: 0,
            state: BodyState::Complete,
            reason: None,
        }
    }

    fn append(&mut self, chunk: &[u8], max_body_bytes: usize) {
        self.original_size = self.original_size.saturating_add(chunk.len() as u64);
        let remaining = max_body_bytes.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if chunk.len() > remaining {
            self.state = BodyState::Truncated;
            self.reason = Some("proxy body limit reached".into());
        }
    }

    fn abort(&mut self, reason: String) {
        if self.state != BodyState::Truncated {
            self.state = if self.original_size == 0 {
                BodyState::Unavailable
            } else {
                BodyState::Truncated
            };
            self.reason = Some(reason);
        }
    }

    fn captured(&self) -> CapturedBody {
        CapturedBody {
            encoding: BodyEncoding::Base64,
            data: base64::engine::general_purpose::STANDARD.encode(&self.bytes),
            state: self.state,
            reason: self.reason.clone(),
            original_size: Some(self.original_size),
        }
    }
}

struct ExchangeState {
    request: CapturedRequest,
    response: Option<CapturedResponse>,
    request_body: BodyAccumulator,
    response_body: BodyAccumulator,
    request_done: bool,
    response_done: bool,
    error: Option<String>,
    metadata: Value,
    emitted: bool,
}

struct ExchangeCapture {
    sink: CaptureSink,
    started: Instant,
    max_body_bytes: usize,
    state: Mutex<ExchangeState>,
}

impl ExchangeCapture {
    fn new(
        sink: CaptureSink,
        request: CapturedRequest,
        metadata: Value,
        max_body_bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            sink,
            started: Instant::now(),
            max_body_bytes,
            state: Mutex::new(ExchangeState {
                request,
                response: None,
                request_body: BodyAccumulator::new(max_body_bytes),
                response_body: BodyAccumulator::new(max_body_bytes),
                request_done: false,
                response_done: false,
                error: None,
                metadata,
                emitted: false,
            }),
        })
    }

    fn tap(self: &Arc<Self>, side: BodySide) -> BodyTap {
        BodyTap {
            exchange: self.clone(),
            side,
        }
    }

    fn append(&self, side: BodySide, bytes: &[u8]) {
        let mut state = self.state.lock().expect("exchange capture lock poisoned");
        match side {
            BodySide::Request => state.request_body.append(bytes, self.max_body_bytes),
            BodySide::Response => state.response_body.append(bytes, self.max_body_bytes),
        }
    }

    fn trailers(&self, side: BodySide, headers: &http_mitm_proxy::hyper::HeaderMap) {
        let mut state = self.state.lock().expect("exchange capture lock poisoned");
        if let Value::Object(metadata) = &mut state.metadata {
            let name = match side {
                BodySide::Request => "requestTrailers",
                BodySide::Response => "responseTrailers",
            };
            if let Ok(value) = serde_json::to_value(capture_headers(headers)) {
                metadata.insert(name.into(), value);
            }
        }
    }

    fn finish(&self, side: BodySide) {
        let event = {
            let mut state = self.state.lock().expect("exchange capture lock poisoned");
            match side {
                BodySide::Request => state.request_done = true,
                BodySide::Response => state.response_done = true,
            }
            self.take_event(&mut state)
        };
        if let Some(event) = event {
            self.sink.emit(ProxyCapture::Http(event));
        }
    }

    fn abort(&self, side: BodySide, reason: String) {
        let event = {
            let mut state = self.state.lock().expect("exchange capture lock poisoned");
            match side {
                BodySide::Request => {
                    state.request_body.abort(reason);
                    state.request_done = true;
                }
                BodySide::Response => {
                    state.response_body.abort(reason);
                    state.response_done = true;
                }
            }
            self.take_event(&mut state)
        };
        if let Some(event) = event {
            self.sink.emit(ProxyCapture::Http(event));
        }
    }

    fn set_response(
        &self,
        response: CapturedResponse,
        version: String,
        websocket_compression_removed: bool,
    ) {
        let mut state = self.state.lock().expect("exchange capture lock poisoned");
        state.response = Some(response);
        if let Value::Object(metadata) = &mut state.metadata {
            metadata.insert("upstreamHttpVersion".into(), Value::String(version));
            if websocket_compression_removed {
                metadata.insert(
                    "upstreamWebsocketCompressionRemoved".into(),
                    Value::Bool(true),
                );
            }
        }
    }

    fn set_upstream_error(&self, error: String) {
        let event = {
            let mut state = self.state.lock().expect("exchange capture lock poisoned");
            state.error = Some(error);
            state.response_done = true;
            self.take_event(&mut state)
        };
        if let Some(event) = event {
            self.sink.emit(ProxyCapture::Http(event));
        }
    }

    fn take_event(&self, state: &mut ExchangeState) -> Option<HttpCaptureEvent> {
        if state.emitted || !state.request_done || !state.response_done {
            return None;
        }
        state.emitted = true;
        let mut request = state.request.clone();
        request.body = Some(state.request_body.captured());
        let mut response = state.response.clone();
        if let Some(response) = &mut response {
            response.body = Some(state.response_body.captured());
        }
        Some(HttpCaptureEvent {
            sequence: self.sink.next_sequence(),
            captured_at: None,
            duration_ms: Some(self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
            request,
            response,
            error: state.error.clone(),
            tab_id: None,
            initiator: Some("proxy".into()),
            parent_sequence: None,
            metadata: state.metadata.clone(),
        })
    }
}

#[derive(Clone)]
struct BodyTap {
    exchange: Arc<ExchangeCapture>,
    side: BodySide,
}

impl BodyTap {
    fn append(&self, bytes: &[u8]) {
        self.exchange.append(self.side, bytes);
    }

    fn trailers(&self, headers: &http_mitm_proxy::hyper::HeaderMap) {
        self.exchange.trailers(self.side, headers);
    }

    fn finish(&self) {
        self.exchange.finish(self.side);
    }

    fn abort(&self, reason: String) {
        self.exchange.abort(self.side, reason);
    }
}

pin_project! {
    struct TeeBody<B> {
        #[pin]
        inner: B,
        tap: BodyTap,
        finished: bool,
    }

    impl<B> PinnedDrop for TeeBody<B> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if !*this.finished {
                this.tap.abort("body stream dropped before completion".into());
                *this.finished = true;
            }
        }
    }
}

impl<B> TeeBody<B>
where
    B: Body<Data = Bytes>,
{
    fn new(inner: B, tap: BodyTap) -> Self {
        let finished = inner.is_end_stream();
        if finished {
            tap.finish();
        }
        Self {
            inner,
            tap,
            finished,
        }
    }
}

impl<B> Body for TeeBody<B>
where
    B: Body<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.tap.append(data);
                }
                if let Some(trailers) = frame.trailers_ref() {
                    this.tap.trailers(trailers);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                if !*this.finished {
                    this.tap.abort(format!("body stream failed: {error}"));
                    *this.finished = true;
                }
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                if !*this.finished {
                    this.tap.finish();
                    *this.finished = true;
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn capture_headers(headers: &http_mitm_proxy::hyper::HeaderMap) -> Vec<HeaderField> {
    headers
        .iter()
        .map(|(name, value)| HeaderField {
            name: name.as_str().into(),
            value: String::from_utf8_lossy(value.as_bytes()).into_owned(),
        })
        .collect()
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedCa {
    version: u32,
    certificate_pem: String,
    private_key_pem: String,
}

struct CaMaterial {
    issuer: Issuer<'static, KeyPair>,
    certificate_path: PathBuf,
}

fn load_or_create_ca(data_dir: &Path) -> anyhow::Result<CaMaterial> {
    let material_path = data_dir.join("proxy-ca.json");
    let certificate_path = data_dir.join("proxy-ca.pem");
    let persisted = match fs::read(&material_path) {
        Ok(bytes) => serde_json::from_slice::<PersistedCa>(&bytes)
            .context("failed to parse persisted proxy CA")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let generated = generate_ca()?;
            let bytes = serde_json::to_vec(&generated)?;
            atomic_write(&material_path, &bytes, true)?;
            generated
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read {}", material_path.display()));
        }
    };
    if persisted.version != CA_FILE_VERSION {
        bail!("unsupported proxy CA file version {}", persisted.version);
    }
    match fs::read_to_string(&certificate_path) {
        Ok(current) if current == persisted.certificate_pem => {}
        Ok(_) | Err(_) => atomic_write(
            &certificate_path,
            persisted.certificate_pem.as_bytes(),
            false,
        )?,
    }
    let key = KeyPair::from_pem(&persisted.private_key_pem)
        .context("persisted proxy CA key is invalid")?;
    let issuer = Issuer::from_ca_cert_pem(&persisted.certificate_pem, key)
        .context("persisted proxy CA certificate is invalid")?;
    Ok(CaMaterial {
        issuer,
        certificate_path,
    })
}

fn generate_ca() -> anyhow::Result<PersistedCa> {
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(
        DnType::CommonName,
        DnValue::Utf8String("Erebus Web Capture CA".into()),
    );
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let key = KeyPair::generate().context("failed to generate proxy CA key")?;
    let certificate = params
        .self_signed(&key)
        .context("failed to sign proxy CA certificate")?;
    Ok(PersistedCa {
        version: CA_FILE_VERSION,
        certificate_pem: certificate.pem(),
        private_key_pem: key.serialize_pem(),
    })
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8], secret: bool) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{} has no parent directory", path.display()))?;
    fs::create_dir_all(parent)?;
    let temp_path = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("file"),
        Uuid::new_v4()
    ));
    let result = (|| -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        #[cfg(unix)]
        if secret {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        let _ = secret;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result.with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[test]
    fn custom_ca_normalization_drops_private_key_material() {
        let mut params = CertificateParams::new(Vec::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        let bundle = format!("{}\n{}", certificate.pem(), key.serialize_pem());

        let normalized = UpstreamTlsConfig::CustomCa { ca_pem: bundle }
            .normalized()
            .unwrap();
        let UpstreamTlsConfig::CustomCa { ca_pem } = normalized else {
            panic!("custom CA normalization changed the mode");
        };
        assert!(ca_pem.contains("BEGIN CERTIFICATE"));
        assert!(!ca_pem.contains("PRIVATE KEY"));
        assert_eq!(ca_pem.matches("BEGIN CERTIFICATE").count(), 1);
    }

    #[test]
    fn ca_is_persisted_and_reloaded() {
        let temp = tempfile::tempdir().unwrap();
        let first = load_or_create_ca(temp.path()).unwrap();
        let certificate = fs::read(first.certificate_path).unwrap();
        let second = load_or_create_ca(temp.path()).unwrap();
        assert_eq!(certificate, fs::read(second.certificate_path).unwrap());
        assert!(temp.path().join("proxy-ca.json").is_file());
    }

    #[test]
    fn capture_limit_keeps_original_size() {
        let body = captured_bytes(b"abcdef", 3);
        assert_eq!(body.state, BodyState::Truncated);
        assert_eq!(body.original_size, Some(6));
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(body.data)
                .unwrap(),
            b"abc"
        );
    }

    #[tokio::test]
    async fn queue_overflow_is_reported_as_a_gap() {
        let (sender, mut receiver) = mpsc::channel(1);
        let sink = CaptureSink::new(sender);
        sink.emit(ProxyCapture::Gap(ProxyGap {
            dropped_events: 0,
            reason: "test",
        }));
        sink.emit(ProxyCapture::Gap(ProxyGap {
            dropped_events: 0,
            reason: "overflow",
        }));
        let _ = receiver.recv().await;
        sink.emit(ProxyCapture::Gap(ProxyGap {
            dropped_events: 0,
            reason: "next",
        }));
        let Some(ProxyCapture::Gap(gap)) = receiver.recv().await else {
            panic!("expected a gap marker");
        };
        assert_eq!(gap.dropped_events, 1);
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn close_payload_is_decoded_without_panicking_on_invalid_utf8() {
        assert_eq!(
            parse_close_payload(&[0x03, 0xe8, b'b', b'y', b'e']),
            (Some(1000), Some("bye".into()))
        );
        assert_eq!(parse_close_payload(&[0xff]), (None, None));
    }

    #[test]
    fn upstream_tls_defaults_to_certificate_verification() {
        assert_eq!(
            ProxyConfig::default().upstream_tls,
            UpstreamTlsConfig::Verify
        );
    }

    #[tokio::test]
    async fn proxy_trusts_an_explicit_upstream_ca_for_a_real_https_origin() {
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca_certificate = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
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
        let tls_acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (stream, _) = origin.accept().await.unwrap();
            let mut stream = tls_acceptor.accept(stream).await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).contains("GET /private-ca"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\naccepted",
                )
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(
            temp.path(),
            ProxyConfig {
                upstream_tls: UpstreamTlsConfig::CustomCa {
                    ca_pem: ca_certificate.pem(),
                },
                ..ProxyConfig::default()
            },
        )
        .await
        .unwrap();
        let proxy_url = format!("http://{}", handle.address());
        let capture_ca = fs::read(handle.ca_certificate_path()).unwrap();
        let client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(proxy_url).unwrap())
            .add_root_certificate(reqwest::Certificate::from_pem(&capture_ca).unwrap())
            .build()
            .unwrap();
        let response = client
            .get(format!(
                "https://localhost:{}/private-ca",
                origin_address.port()
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "accepted");

        let capture = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        let ProxyCapture::Http(exchange) = capture else {
            panic!("expected an HTTPS capture");
        };
        assert_eq!(exchange.response.unwrap().status, 200);

        origin_task.await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn proxy_forwards_and_captures_an_http_exchange() {
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = socket.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..read]).contains("GET /probe?q=1"));
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(temp.path(), ProxyConfig::default())
            .await
            .unwrap();
        let mut client = tokio::net::TcpStream::connect(handle.address())
            .await
            .unwrap();
        client
            .write_all(
                format!(
                    "GET http://{origin_address}/probe?q=1 HTTP/1.1\r\nHost: {origin_address}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).ends_with("hello"));

        let capture = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        let ProxyCapture::Http(exchange) = capture else {
            panic!("expected an HTTP capture");
        };
        assert_eq!(exchange.request.method, "GET");
        assert_eq!(exchange.response.as_ref().unwrap().status, 200);
        let response_body = exchange.response.unwrap().body.unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(response_body.data)
                .unwrap(),
            b"hello"
        );

        origin_task.await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn capture_truncation_does_not_truncate_forwarded_response() {
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\n0123456789abcdef",
                )
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(
            temp.path(),
            ProxyConfig {
                max_body_bytes: 4,
                ..ProxyConfig::default()
            },
        )
        .await
        .unwrap();
        let mut client = tokio::net::TcpStream::connect(handle.address())
            .await
            .unwrap();
        client
            .write_all(
                format!(
                    "GET http://{origin_address}/large HTTP/1.1\r\nHost: {origin_address}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(String::from_utf8_lossy(&response).ends_with("0123456789abcdef"));

        let ProxyCapture::Http(exchange) =
            tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected an HTTP capture");
        };
        let body = exchange.response.unwrap().body.unwrap();
        assert_eq!(body.state, BodyState::Truncated);
        assert_eq!(body.original_size, Some(16));
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(body.data)
                .unwrap(),
            b"0123"
        );

        origin_task.await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn proxy_relays_and_captures_websocket_frames() {
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (stream, _) = origin.accept().await.unwrap();
            let service = service_fn(|mut request: Request<Incoming>| async move {
                let (response, upgrade) = fastwebsockets::upgrade::upgrade(&mut request)?;
                tokio::spawn(async move {
                    let mut websocket = upgrade.await.unwrap();
                    loop {
                        let frame = websocket.read_frame().await.unwrap();
                        let opcode = frame.opcode;
                        if matches!(opcode, OpCode::Text | OpCode::Binary) {
                            websocket.write_frame(frame).await.unwrap();
                        }
                        if opcode == OpCode::Close {
                            break;
                        }
                    }
                });
                Ok::<_, WebSocketError>(response)
            });
            server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(temp.path(), ProxyConfig::default())
            .await
            .unwrap();
        let mut client = tokio::net::TcpStream::connect(handle.address())
            .await
            .unwrap();
        let key = fastwebsockets::handshake::generate_key();
        client
            .write_all(
                format!(
                    "GET http://{origin_address}/socket HTTP/1.1\r\nHost: {origin_address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut handshake = Vec::new();
        while !handshake.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            client.read_exact(&mut byte).await.unwrap();
            handshake.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&handshake).starts_with("HTTP/1.1 101"));

        let mut websocket = fastwebsockets::WebSocket::after_handshake(client, Role::Client);
        websocket
            .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
                b"hello websocket".to_vec(),
            )))
            .await
            .unwrap();
        let echoed = websocket.read_frame().await.unwrap();
        assert_eq!(&*echoed.payload, b"hello websocket");
        websocket
            .write_frame(fastwebsockets::Frame::close(1000, b"done"))
            .await
            .unwrap();

        let mut client_frame = false;
        let mut server_frame = false;
        let expected_url = format!("ws://{origin_address}/socket");
        for _ in 0..8 {
            let capture = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap();
            if let ProxyCapture::WebSocket(frame) = capture
                && frame.kind.as_str() == "frame"
                && frame.opcode == Some(OpCode::Text as u8)
            {
                assert_eq!(frame.url.as_deref(), Some(expected_url.as_str()));
                match frame.direction {
                    Some(WebSocketDirection::ClientToServer) => client_frame = true,
                    Some(WebSocketDirection::ServerToClient) => server_frame = true,
                    None => {}
                }
            }
            if client_frame && server_frame {
                break;
            }
        }
        assert!(client_frame && server_frame);

        origin_task.await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn custom_upstream_ca_preserves_wss_upgrades() {
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca_certificate = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);
        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
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
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (stream, _) = origin.accept().await.unwrap();
            let stream = acceptor.accept(stream).await.unwrap();
            let service = service_fn(|mut request: Request<Incoming>| async move {
                let (response, upgrade) = fastwebsockets::upgrade::upgrade(&mut request)?;
                tokio::spawn(async move {
                    let mut websocket = upgrade.await.unwrap();
                    loop {
                        let frame = websocket.read_frame().await.unwrap();
                        let opcode = frame.opcode;
                        if matches!(opcode, OpCode::Text | OpCode::Binary) {
                            websocket.write_frame(frame).await.unwrap();
                        }
                        if opcode == OpCode::Close {
                            break;
                        }
                    }
                });
                Ok::<_, WebSocketError>(response)
            });
            server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(
            temp.path(),
            ProxyConfig {
                upstream_tls: UpstreamTlsConfig::CustomCa {
                    ca_pem: ca_certificate.pem(),
                },
                ..ProxyConfig::default()
            },
        )
        .await
        .unwrap();
        let mut tunnel = TcpStream::connect(handle.address()).await.unwrap();
        tunnel
            .write_all(
                format!(
                    "CONNECT localhost:{} HTTP/1.1\r\nHost: localhost:{}\r\n\r\n",
                    origin_address.port(),
                    origin_address.port()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut connect_response = Vec::new();
        while !connect_response.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            tunnel.read_exact(&mut byte).await.unwrap();
            connect_response.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&connect_response).contains("200"));

        let mut roots = RootCertStore::empty();
        let capture_ca = fs::read(handle.ca_certificate_path()).unwrap();
        let mut reader = BufReader::new(capture_ca.as_slice());
        for certificate in rustls_pemfile::certs(&mut reader) {
            roots.add(certificate.unwrap()).unwrap();
        }
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let mut tunnel = connector
            .connect(ServerName::try_from("localhost").unwrap(), tunnel)
            .await
            .unwrap();
        let key = fastwebsockets::handshake::generate_key();
        tunnel
            .write_all(
                format!(
                    "GET /socket HTTP/1.1\r\nHost: localhost:{}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n",
                    origin_address.port()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut handshake = Vec::new();
        while !handshake.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            tunnel.read_exact(&mut byte).await.unwrap();
            handshake.push(byte[0]);
        }
        assert!(String::from_utf8_lossy(&handshake).starts_with("HTTP/1.1 101"));

        let mut websocket = fastwebsockets::WebSocket::after_handshake(tunnel, Role::Client);
        websocket
            .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
                b"wss custom CA".to_vec(),
            )))
            .await
            .unwrap();
        assert_eq!(
            &*websocket.read_frame().await.unwrap().payload,
            b"wss custom CA"
        );
        websocket
            .write_frame(fastwebsockets::Frame::close(1000, b"done"))
            .await
            .unwrap();

        let mut client_frame = false;
        let mut server_frame = false;
        for _ in 0..8 {
            let capture = tokio::time::timeout(std::time::Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap();
            if let ProxyCapture::WebSocket(frame) = capture
                && frame.kind.as_str() == "frame"
                && frame.opcode == Some(OpCode::Text as u8)
            {
                match frame.direction {
                    Some(WebSocketDirection::ClientToServer) => client_frame = true,
                    Some(WebSocketDirection::ServerToClient) => server_frame = true,
                    None => {}
                }
            }
            if client_frame && server_frame {
                break;
            }
        }
        assert!(client_frame && server_frame);
        origin_task.await.unwrap();
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_an_open_websocket_relay() {
        let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let origin_address = origin.local_addr().unwrap();
        let origin_task = tokio::spawn(async move {
            let (stream, _) = origin.accept().await.unwrap();
            let service = service_fn(|mut request: Request<Incoming>| async move {
                let (response, upgrade) = fastwebsockets::upgrade::upgrade(&mut request)?;
                tokio::spawn(async move {
                    let mut websocket = upgrade.await.unwrap();
                    while let Ok(frame) = websocket.read_frame().await {
                        if matches!(frame.opcode, OpCode::Text | OpCode::Binary) {
                            websocket.write_frame(frame).await.unwrap();
                        }
                    }
                });
                Ok::<_, WebSocketError>(response)
            });
            server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await
                .unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let ProxyRuntime { handle, mut events } = start_proxy(temp.path(), ProxyConfig::default())
            .await
            .unwrap();
        let mut client = tokio::net::TcpStream::connect(handle.address())
            .await
            .unwrap();
        let key = fastwebsockets::handshake::generate_key();
        client
            .write_all(
                format!(
                    "GET http://{origin_address}/open HTTP/1.1\r\nHost: {origin_address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut handshake = Vec::new();
        while !handshake.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            client.read_exact(&mut byte).await.unwrap();
            handshake.push(byte[0]);
        }
        let mut websocket = fastwebsockets::WebSocket::after_handshake(client, Role::Client);
        websocket
            .write_frame(fastwebsockets::Frame::text(fastwebsockets::Payload::Owned(
                b"alive".to_vec(),
            )))
            .await
            .unwrap();
        assert_eq!(&*websocket.read_frame().await.unwrap().payload, b"alive");

        tokio::time::timeout(std::time::Duration::from_secs(2), handle.shutdown())
            .await
            .expect("proxy shutdown timed out")
            .unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), websocket.read_frame(),)
                .await
                .expect("client socket stayed open")
                .is_err()
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while events.recv().await.is_some() {}
        })
        .await
        .expect("capture sender survived shutdown");
        origin_task.await.unwrap();
    }
}
