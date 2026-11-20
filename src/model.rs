use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderField {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedBody {
    #[serde(default = "body_encoding")]
    pub encoding: BodyEncoding,
    #[serde(default)]
    pub data: String,
    #[serde(default = "body_complete")]
    pub state: BodyState,
    pub reason: Option<String>,
    pub original_size: Option<u64>,
}

fn body_encoding() -> BodyEncoding {
    BodyEncoding::Utf8
}

fn body_complete() -> BodyState {
    BodyState::Complete
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BodyEncoding {
    #[default]
    Utf8,
    Base64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyState {
    #[default]
    Complete,
    Truncated,
    Unavailable,
    Skipped,
    Evicted,
}

impl BodyState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Truncated => "truncated",
            Self::Unavailable => "unavailable",
            Self::Skipped => "skipped",
            Self::Evicted => "evicted",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedRequest {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<HeaderField>,
    pub body: Option<CapturedBody>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturedResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: Vec<HeaderField>,
    pub body: Option<CapturedBody>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpCaptureEvent {
    pub sequence: u64,
    pub captured_at: Option<String>,
    pub duration_ms: Option<u64>,
    pub request: CapturedRequest,
    pub response: Option<CapturedResponse>,
    pub error: Option<String>,
    pub tab_id: Option<String>,
    pub initiator: Option<String>,
    pub parent_sequence: Option<u64>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpIngestBatch {
    pub producer_id: String,
    pub session_id: String,
    pub events: Vec<HttpCaptureEvent>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayRequest {
    pub observation_id: String,
    pub session_id: String,
    pub replay_id: Option<String>,
    pub method: Option<String>,
    pub url: Option<String>,
    pub headers: Option<Vec<HeaderField>>,
    pub body: Option<CapturedBody>,
    #[serde(default)]
    pub follow_redirects: bool,
    pub timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayResult {
    pub replay_id: String,
    pub observation_id: String,
    pub status: Option<u16>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketFrameEvent {
    pub sequence: u64,
    pub connection_id: String,
    pub captured_at: Option<String>,
    #[serde(default)]
    pub kind: WebSocketEventKind,
    pub direction: Option<WebSocketDirection>,
    pub opcode: Option<u8>,
    pub payload: Option<CapturedBody>,
    pub url: Option<String>,
    pub subprotocol: Option<String>,
    pub close_code: Option<u16>,
    pub close_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WebSocketEventKind {
    Open,
    #[default]
    Frame,
    Close,
    Error,
}

impl WebSocketEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Frame => "frame",
            Self::Close => "close",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketDirection {
    ClientToServer,
    ServerToClient,
}

impl WebSocketDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClientToServer => "client_to_server",
            Self::ServerToClient => "server_to_client",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketIngestBatch {
    pub producer_id: String,
    pub session_id: String,
    pub frames: Vec<WebSocketFrameEvent>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestResult {
    pub sequence: u64,
    pub status: IngestStatus,
    pub observation_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IngestStatus {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSession {
    pub label: String,
    #[serde(default = "session_source")]
    pub source: String,
    pub scope_profile_id: Option<String>,
}

fn session_source() -> String {
    "mixed".into()
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureSession {
    pub id: String,
    pub label: String,
    pub source: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub scope_profile_id: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDeleteResult {
    pub id: String,
    pub observations: u64,
    pub websocket_events: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairingToken {
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeRuleInput {
    pub action: ScopeAction,
    pub matcher: ScopeMatcher,
    pub pattern: String,
    #[serde(default = "rule_enabled")]
    pub enabled: bool,
}

fn rule_enabled() -> bool {
    true
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ScopeAction {
    Include,
    Exclude,
}

impl ScopeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Include => "include",
            Self::Exclude => "exclude",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScopeMatcher {
    Host,
    HostWildcard,
    UrlWildcard,
    Regex,
    Cidr,
}

impl ScopeMatcher {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::HostWildcard => "host_wildcard",
            Self::UrlWildcard => "url_wildcard",
            Self::Regex => "regex",
            Self::Cidr => "cidr",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveScopeProfile {
    pub name: String,
    #[serde(default)]
    pub rules: Vec<ScopeRuleInput>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeRule {
    pub id: String,
    pub action: ScopeAction,
    pub matcher: ScopeMatcher,
    pub pattern: String,
    pub enabled: bool,
    pub order_index: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeProfileView {
    pub id: String,
    pub name: String,
    pub version: u64,
    pub created_at: String,
    pub updated_at: String,
    pub rules: Vec<ScopeRule>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeDecision {
    pub state: ScopeState,
    pub rule_id: Option<String>,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScopeState {
    InScope,
    OutOfScope,
    Unknown,
}

impl ScopeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InScope => "in_scope",
            Self::OutOfScope => "out_of_scope",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GraphQuery {
    pub session_id: Option<String>,
    pub kind: Option<String>,
    pub text: Option<String>,
    pub limit: Option<u32>,
    #[serde(default = "default_in_scope")]
    pub in_scope_only: bool,
}

fn default_in_scope() -> bool {
    true
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphNode {
    pub id: String,
    pub kind: String,
    pub canonical_key: String,
    pub label: String,
    pub metadata: Value,
    pub first_seen: String,
    pub last_seen: String,
    pub evidence_count: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphEdge {
    pub source_id: String,
    pub target_id: String,
    pub kind: String,
    pub first_seen: String,
    pub last_seen: String,
    pub evidence_count: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphPage {
    pub nodes: Vec<GraphNode>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphNeighborhood {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TrafficQuery {
    pub session_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub host: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status: Option<u16>,
    pub scope: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficSummary {
    pub id: String,
    pub captured_at: String,
    pub method: String,
    pub url: String,
    pub host: String,
    pub path: String,
    pub status: Option<u16>,
    pub duration_ms: Option<u64>,
    pub scope: String,
    pub request_body_size: u64,
    pub response_body_size: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficPage {
    pub items: Vec<TrafficSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketQuery {
    pub session_id: Option<String>,
    pub connection_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub url: Option<String>,
    pub kind: Option<String>,
    pub direction: Option<String>,
    pub opcode: Option<u8>,
    pub scope: Option<String>,
    pub text: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketEventSummary {
    pub id: String,
    pub connection_id: String,
    pub captured_at: String,
    pub kind: String,
    pub direction: Option<String>,
    pub opcode: Option<u8>,
    pub url: Option<String>,
    pub subprotocol: Option<String>,
    pub connection_state: String,
    pub close_code: Option<u16>,
    pub close_reason: Option<String>,
    pub scope: String,
    pub payload_size: u64,
    pub payload_original_size: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketPage {
    pub items: Vec<WebSocketEventSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketEventDetail {
    pub summary: WebSocketEventSummary,
    pub payload: Option<BodyDetail>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureGapInput {
    pub session_id: String,
    pub source: String,
    pub dropped_events: u64,
    pub reason: String,
    pub observed_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CaptureGapQuery {
    pub session_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureGap {
    pub id: String,
    pub session_id: String,
    pub source: String,
    pub dropped_events: u64,
    pub reason: String,
    pub observed_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeDetail {
    pub summary: TrafficSummary,
    pub request_headers: Vec<HeaderField>,
    pub response_headers: Vec<HeaderField>,
    pub request_body: Option<BodyDetail>,
    pub response_body: Option<BodyDetail>,
    pub error: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BodyDetail {
    pub state: String,
    pub reason: Option<String>,
    pub captured_size: u64,
    pub original_size: u64,
    pub mime: Option<String>,
    pub text_preview: Option<String>,
    pub hash: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredBody {
    pub hash: String,
    pub mime: Option<String>,
    pub captured_size: u64,
    pub stored_size: u64,
    pub codec: String,
    pub data_base64: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationInput {
    pub target_type: String,
    pub target_id: String,
    pub body: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationQuery {
    pub target_type: Option<String>,
    pub target_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAnnotation {
    pub body: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Annotation {
    pub id: String,
    pub target_type: String,
    pub target_id: String,
    pub body: String,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Health {
    pub status: &'static str,
    pub protocol_version: u32,
    pub schema_version: u32,
}
