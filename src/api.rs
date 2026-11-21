use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use rand::RngCore as _;
use serde::Serialize;
use tokio::sync::{Mutex, Semaphore};

use crate::capture::{CaptureManager, CaptureProxyDescriptor};
use crate::config::Config;
use crate::error::AppError;
use crate::model::*;
use crate::replay::{self, ReplayPreparation};
use crate::store::Store;

const MAX_API_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_BLOCKING_TASKS: usize = 8;
const MAX_REVOKED_PAIRINGS: usize = 1_024;

#[derive(Clone)]
pub struct AppState {
    config: Config,
    store: Arc<Store>,
    captures: CaptureManager,
    pairings: PairingTokens,
    blocking_slots: Arc<Semaphore>,
}

impl AppState {
    pub fn new(config: Config, store: Arc<Store>) -> Self {
        let captures = CaptureManager::new(store.clone(), &config.data_dir);
        Self {
            config,
            store,
            captures,
            pairings: PairingTokens::default(),
            blocking_slots: Arc::new(Semaphore::new(MAX_BLOCKING_TASKS)),
        }
    }

    pub fn captures(&self) -> CaptureManager {
        self.captures.clone()
    }

    async fn run_blocking<T, F>(&self, work: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    {
        let permit = self
            .blocking_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| AppError::Busy)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(AppError::Join)?
        .map_err(|error| {
            if crate::store::is_input_error(&error) {
                AppError::InvalidInput(error.to_string())
            } else {
                AppError::Storage(error)
            }
        })
    }
}

#[derive(Clone, Default)]
struct PairingTokens {
    state: Arc<Mutex<PairingTokenState>>,
}

#[derive(Default)]
struct PairingTokenState {
    active: HashMap<String, String>,
    revoked: VecDeque<String>,
}

enum PairingResolution {
    Active(String),
    Revoked,
    Unknown,
}

impl PairingTokens {
    async fn issue(&self, session_id: &str) -> String {
        let mut random = [0_u8; 32];
        rand::rng().fill_bytes(&mut random);
        let token = blake3::hash(&random).to_hex().to_string();
        let mut state = self.state.lock().await;
        state
            .active
            .retain(|_, paired_session| paired_session != session_id);
        state.revoked.retain(|revoked| revoked != &token);
        state.active.insert(token.clone(), session_id.to_owned());
        token
    }

    async fn resolve(&self, token: &str) -> PairingResolution {
        let state = self.state.lock().await;
        if let Some(session_id) = state.active.get(token) {
            PairingResolution::Active(session_id.clone())
        } else if state.revoked.iter().any(|revoked| revoked == token) {
            PairingResolution::Revoked
        } else {
            PairingResolution::Unknown
        }
    }

    async fn revoke_session(&self, session_id: &str) {
        let mut state = self.state.lock().await;
        let revoked = state
            .active
            .iter()
            .filter_map(|(token, paired_session)| {
                (paired_session == session_id).then_some(token.clone())
            })
            .collect::<Vec<_>>();
        state
            .active
            .retain(|_, paired_session| paired_session != session_id);
        for token in revoked {
            state.revoked.push_back(token);
        }
        while state.revoked.len() > MAX_REVOKED_PAIRINGS {
            state.revoked.pop_front();
        }
    }
}

#[derive(Clone, Debug)]
enum RuntimeAuthorization {
    Master,
    Pairing(String),
}

impl RuntimeAuthorization {
    fn require_session(&self, session_id: &str) -> Result<(), AppError> {
        match self {
            Self::Master => Ok(()),
            Self::Pairing(paired_session) if paired_session == session_id => Ok(()),
            Self::Pairing(_) => Err(AppError::Forbidden),
        }
    }
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/sessions", get(list_sessions).post(create_session))
        .route("/sessions/{id}", get(get_session).delete(delete_session))
        .route("/sessions/{id}/end", post(end_session))
        .route("/sessions/{id}/proxy/start", post(start_session_proxy))
        .route("/sessions/{id}/proxy/stop", post(stop_session_proxy))
        .route("/sessions/{id}/pairing", post(create_pairing))
        .route("/proxy/ca", get(proxy_ca))
        .route("/scope", get(list_scope).post(save_scope))
        .route(
            "/scope/{id}",
            get(get_scope).put(update_scope).delete(delete_scope),
        )
        .route("/ingest/http", post(ingest_http))
        .route("/ingest/ws", post(ingest_websocket))
        .route("/replay", post(replay_request))
        .route("/traffic", get(list_traffic))
        .route("/traffic/{id}", get(get_exchange))
        .route("/bodies/{hash}", get(get_body))
        .route("/websockets", get(list_websocket_events))
        .route("/websockets/{id}", get(get_websocket_event))
        .route("/gaps", get(list_capture_gaps).post(record_capture_gap))
        .route("/map", get(list_graph))
        .route("/map/{id}", get(get_graph_neighborhood))
        .route(
            "/annotations",
            get(list_annotations).post(create_annotation),
        )
        .route(
            "/annotations/{id}",
            get(get_annotation)
                .patch(update_annotation)
                .delete(delete_annotation),
        )
        .layer(DefaultBodyLimit::max(MAX_API_BODY_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), authorize));

    Router::new()
        .route("/v1/health", get(health))
        .nest("/v1", protected)
        .with_state(state)
}

async fn authorize(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(AppError::Unauthorized)?;
    let authorization = if constant_time_eq(token.as_bytes(), state.config.token().as_bytes()) {
        RuntimeAuthorization::Master
    } else if pairing_capture_path(request.method(), request.uri().path()) {
        match state.pairings.resolve(token).await {
            PairingResolution::Active(session_id) => RuntimeAuthorization::Pairing(session_id),
            PairingResolution::Revoked => return Err(AppError::Gone),
            PairingResolution::Unknown => return Err(AppError::Unauthorized),
        }
    } else {
        return Err(AppError::Unauthorized);
    };
    request.extensions_mut().insert(authorization);
    Ok(next.run(request).await)
}

fn pairing_capture_path(method: &Method, path: &str) -> bool {
    if *method != Method::POST {
        return false;
    }
    matches!(
        path.strip_prefix("/v1").unwrap_or(path),
        "/ingest/http" | "/ingest/ws" | "/gaps"
    )
}

async fn health(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        protocol_version: 1,
        schema_version: state.store.schema_version(),
    })
}

#[derive(serde::Deserialize, Default)]
struct CreateSessionQuery {
    #[serde(default)]
    proxy: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CaptureSessionView {
    #[serde(flatten)]
    session: CaptureSession,
    proxy: Option<CaptureProxyDescriptor>,
}

async fn create_session(
    State(state): State<AppState>,
    Query(query): Query<CreateSessionQuery>,
    Json(input): Json<CreateSession>,
) -> Result<Json<CaptureSessionView>, AppError> {
    let store = state.store.clone();
    let session = state
        .run_blocking(move || store.create_session(input))
        .await?;
    let proxy = if query.proxy {
        match state.captures.start(&session.id).await {
            Ok(proxy) => Some(proxy),
            Err(error) => {
                let session_id = session.id.clone();
                let store = state.store.clone();
                let _ = state
                    .run_blocking(move || store.end_session(&session_id))
                    .await;
                return Err(AppError::Storage(error));
            }
        }
    } else {
        None
    };
    Ok(Json(CaptureSessionView { session, proxy }))
}

async fn list_sessions(
    State(state): State<AppState>,
) -> Result<Json<Vec<CaptureSessionView>>, AppError> {
    let store = state.store.clone();
    let sessions = state.run_blocking(move || store.list_sessions()).await?;
    let mut views = Vec::with_capacity(sessions.len());
    for session in sessions {
        let proxy = state.captures.descriptor(&session.id).await;
        views.push(CaptureSessionView { session, proxy });
    }
    Ok(Json(views))
}

async fn get_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CaptureSessionView>, AppError> {
    let store = state.store.clone();
    let lookup_id = id.clone();
    let session = state
        .run_blocking(move || store.session(&lookup_id))
        .await?
        .ok_or(AppError::NotFound)?;
    let proxy = state.captures.descriptor(&id).await;
    Ok(Json(CaptureSessionView { session, proxy }))
}

async fn start_session_proxy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CaptureSessionView>, AppError> {
    let store = state.store.clone();
    let lookup_id = id.clone();
    let session = state
        .run_blocking(move || store.session(&lookup_id))
        .await?
        .ok_or(AppError::NotFound)?;
    if session.ended_at.is_some() {
        return Err(AppError::InvalidInput(
            "ended capture sessions cannot restart their proxy".into(),
        ));
    }
    let proxy = state.captures.start(&id).await.map_err(AppError::Storage)?;
    Ok(Json(CaptureSessionView {
        session,
        proxy: Some(proxy),
    }))
}

async fn stop_session_proxy(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CaptureSessionView>, AppError> {
    let store = state.store.clone();
    let lookup_id = id.clone();
    let session = state
        .run_blocking(move || store.session(&lookup_id))
        .await?
        .ok_or(AppError::NotFound)?;
    if session.ended_at.is_some() {
        return Err(AppError::InvalidInput(
            "ended capture sessions do not have an active proxy".into(),
        ));
    }
    state.captures.stop(&id).await.map_err(AppError::Storage)?;
    Ok(Json(CaptureSessionView {
        session,
        proxy: None,
    }))
}

async fn create_pairing(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<PairingToken>, AppError> {
    let store = state.store.clone();
    let lookup_id = id.clone();
    let session = state
        .run_blocking(move || store.session(&lookup_id))
        .await?
        .ok_or(AppError::NotFound)?;
    if session.ended_at.is_some() {
        return Err(AppError::InvalidInput(
            "ended capture sessions cannot be paired".into(),
        ));
    }
    Ok(Json(PairingToken {
        token: state.pairings.issue(&id).await,
    }))
}

async fn end_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CaptureSessionView>, AppError> {
    state.captures.stop(&id).await.map_err(AppError::Storage)?;
    let store = state.store.clone();
    let session_id = id.clone();
    let session = state
        .run_blocking(move || store.end_session(&session_id))
        .await?
        .ok_or(AppError::NotFound)?;
    state.pairings.revoke_session(&id).await;
    Ok(Json(CaptureSessionView {
        session,
        proxy: None,
    }))
}

async fn delete_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionDeleteResult>, AppError> {
    if state.captures.descriptor(&id).await.is_some() {
        return Err(AppError::InvalidInput(
            "stop and end the capture session before deleting it".into(),
        ));
    }
    let store = state.store.clone();
    let session_id = id.clone();
    let deleted = state
        .run_blocking(move || store.delete_session(&session_id))
        .await?
        .ok_or(AppError::NotFound)?;
    state.pairings.revoke_session(&id).await;
    Ok(Json(deleted))
}

async fn proxy_ca(State(state): State<AppState>) -> Result<Response, AppError> {
    let pem = state
        .captures
        .ca_certificate()
        .await
        .map_err(AppError::Storage)?;
    Ok((
        [
            (CONTENT_TYPE, "application/x-pem-file"),
            (
                CONTENT_DISPOSITION,
                "attachment; filename=erebus-web-capture-ca.pem",
            ),
        ],
        pem,
    )
        .into_response())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScopeSaved {
    id: String,
}

async fn save_scope(
    State(state): State<AppState>,
    Json(input): Json<SaveScopeProfile>,
) -> Result<Json<ScopeSaved>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.save_scope_profile(input))
        .await
        .map(|id| Json(ScopeSaved { id }))
}

async fn list_scope(
    State(state): State<AppState>,
) -> Result<Json<Vec<ScopeProfileView>>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.scope_profiles())
        .await
        .map(Json)
}

async fn get_scope(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ScopeProfileView>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.scope_profile(&id))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn update_scope(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<SaveScopeProfile>,
) -> Result<Json<ScopeProfileView>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.update_scope_profile(&id, input))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn delete_scope(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let store = state.store.clone();
    if state
        .run_blocking(move || store.delete_scope_profile(&id))
        .await?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

async fn ingest_http(
    State(state): State<AppState>,
    Extension(authorization): Extension<RuntimeAuthorization>,
    Json(batch): Json<HttpIngestBatch>,
) -> Result<Json<Vec<IngestResult>>, AppError> {
    authorization.require_session(&batch.session_id)?;
    let store = state.store.clone();
    state
        .run_blocking(move || store.ingest_http(batch))
        .await
        .map(Json)
}

async fn ingest_websocket(
    State(state): State<AppState>,
    Extension(authorization): Extension<RuntimeAuthorization>,
    Json(batch): Json<WebSocketIngestBatch>,
) -> Result<Json<Vec<IngestResult>>, AppError> {
    authorization.require_session(&batch.session_id)?;
    let store = state.store.clone();
    state
        .run_blocking(move || store.ingest_websocket(batch))
        .await
        .map(Json)
}

async fn replay_request(
    State(state): State<AppState>,
    Json(input): Json<ReplayRequest>,
) -> Result<Json<ReplayResult>, AppError> {
    let store = state.store.clone();
    let preparation = state
        .run_blocking(move || replay::prepare(&store, input))
        .await?;
    let prepared = match preparation {
        ReplayPreparation::Existing(result) => return Ok(Json(result)),
        ReplayPreparation::MissingSource => return Err(AppError::NotFound),
        ReplayPreparation::Ready(prepared) => *prepared,
    };
    let completed = replay::execute(prepared).await;
    let store = state.store.clone();
    state
        .run_blocking(move || replay::persist(&store, completed))
        .await
        .map(Json)
}

async fn list_traffic(
    State(state): State<AppState>,
    Query(query): Query<TrafficQuery>,
) -> Result<Json<TrafficPage>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.traffic(query))
        .await
        .map(Json)
}

async fn get_exchange(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ExchangeDetail>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.exchange(&id))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn get_body(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<StoredBody>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.body(&hash))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn list_websocket_events(
    State(state): State<AppState>,
    Query(query): Query<WebSocketQuery>,
) -> Result<Json<WebSocketPage>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.websocket_events(query))
        .await
        .map(Json)
}

async fn get_websocket_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<WebSocketEventDetail>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.websocket_event(&id))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn record_capture_gap(
    State(state): State<AppState>,
    Extension(authorization): Extension<RuntimeAuthorization>,
    Json(input): Json<CaptureGapInput>,
) -> Result<Json<CaptureGap>, AppError> {
    authorization.require_session(&input.session_id)?;
    let store = state.store.clone();
    state
        .run_blocking(move || store.record_gap(input))
        .await
        .map(Json)
}

async fn list_capture_gaps(
    State(state): State<AppState>,
    Query(query): Query<CaptureGapQuery>,
) -> Result<Json<Vec<CaptureGap>>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.capture_gaps(query))
        .await
        .map(Json)
}

async fn list_graph(
    State(state): State<AppState>,
    Query(query): Query<GraphQuery>,
) -> Result<Json<GraphPage>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.graph(query))
        .await
        .map(Json)
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct NeighborhoodQuery {
    session_id: Option<String>,
    scope: Option<ScopeState>,
    #[serde(default = "default_true")]
    in_scope_only: bool,
}

fn default_true() -> bool {
    true
}

async fn get_graph_neighborhood(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<NeighborhoodQuery>,
) -> Result<Json<GraphNeighborhood>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || {
            store.graph_neighborhood(
                &id,
                query.session_id.as_deref(),
                query.scope,
                query.in_scope_only,
            )
        })
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn create_annotation(
    State(state): State<AppState>,
    Json(input): Json<AnnotationInput>,
) -> Result<Json<Annotation>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.annotate(input))
        .await
        .map(Json)
}

async fn list_annotations(
    State(state): State<AppState>,
    Query(query): Query<AnnotationQuery>,
) -> Result<Json<Vec<Annotation>>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.annotations(query))
        .await
        .map(Json)
}

async fn get_annotation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Annotation>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.annotation(&id))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn update_annotation(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(input): Json<UpdateAnnotation>,
) -> Result<Json<Annotation>, AppError> {
    let store = state.store.clone();
    state
        .run_blocking(move || store.update_annotation(&id, input))
        .await?
        .map(Json)
        .ok_or(AppError::NotFound)
}

async fn delete_annotation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    let store = state.store.clone();
    if state
        .run_blocking(move || store.delete_annotation(&id))
        .await?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use super::{AppState, PairingResolution, PairingTokens, constant_time_eq, router};
    use crate::config::Config;
    use crate::model::CreateSession;
    use crate::store::Store;

    #[test]
    fn token_comparison_checks_length_and_bytes() {
        assert!(constant_time_eq(b"same", b"same"));
        assert!(!constant_time_eq(b"same", b"diff"));
        assert!(!constant_time_eq(b"same", b"short"));
    }

    #[tokio::test]
    async fn pairing_replacement_stays_rebindable_and_revocation_is_terminal() {
        let pairings = PairingTokens::default();
        let first = pairings.issue("session").await;
        let second = pairings.issue("session").await;
        assert!(matches!(
            pairings.resolve(&first).await,
            PairingResolution::Unknown
        ));
        assert!(matches!(
            pairings.resolve(&second).await,
            PairingResolution::Active(session) if session == "session"
        ));

        pairings.revoke_session("session").await;
        assert!(matches!(
            pairings.resolve(&second).await,
            PairingResolution::Revoked
        ));
    }

    async fn call(
        app: &axum::Router,
        method: &str,
        path: &str,
        token: &str,
        body: Option<Value>,
    ) -> axum::response::Response {
        let mut builder = Request::builder()
            .method(method)
            .uri(path)
            .header("authorization", format!("Bearer {token}"));
        let body = match body {
            Some(body) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&body).unwrap())
            }
            None => Body::empty(),
        };
        app.clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn pairing_token_is_bound_to_one_active_session_and_ingest_only() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: temp.path().to_path_buf(),
            bind: "127.0.0.1:0".parse().unwrap(),
            token: Some("master-token".into()),
        }
        .prepare()
        .unwrap();
        let store = Arc::new(Store::open(&config.data_dir).unwrap());
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
        let app = router(AppState::new(config, store));

        let response = call(
            &app,
            "POST",
            &format!("/v1/sessions/{}/pairing", first.id),
            "master-token",
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 8 * 1024).await.unwrap();
        let pairing: Value = serde_json::from_slice(&body).unwrap();
        let pairing = pairing["token"].as_str().unwrap();

        let read = call(&app, "GET", "/v1/traffic", pairing, None).await;
        assert_eq!(read.status(), StatusCode::UNAUTHORIZED);
        let replay = call(
            &app,
            "POST",
            "/v1/replay",
            pairing,
            Some(json!({
                "observationId": "missing",
                "sessionId": first.id,
            })),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
        let delete = call(
            &app,
            "DELETE",
            &format!("/v1/sessions/{}", first.id),
            pairing,
            None,
        )
        .await;
        assert_eq!(delete.status(), StatusCode::UNAUTHORIZED);

        let wrong_session = call(
            &app,
            "POST",
            "/v1/ingest/http",
            pairing,
            Some(json!({
                "producerId": "extension-test",
                "sessionId": second.id,
                "events": [{
                    "sequence": 1,
                    "request": { "method": "GET", "url": "http://example.test/" },
                    "response": { "status": 200 },
                }],
            })),
        )
        .await;
        assert_eq!(wrong_session.status(), StatusCode::FORBIDDEN);

        let accepted = call(
            &app,
            "POST",
            "/v1/ingest/http",
            pairing,
            Some(json!({
                "producerId": "extension-test",
                "sessionId": first.id,
                "events": [{
                    "sequence": 1,
                    "request": { "method": "GET", "url": "http://example.test/" },
                    "response": { "status": 200 },
                }],
            })),
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::OK);

        let gap = json!({
            "sessionId": first.id,
            "source": "extension",
            "droppedEvents": 2,
            "reason": "debugger detached",
            "observedAt": "2026-11-20T12:00:00Z",
            "producerId": "extension-test",
            "sequence": 2,
        });
        let first_gap = call(&app, "POST", "/v1/gaps", pairing, Some(gap.clone())).await;
        assert_eq!(first_gap.status(), StatusCode::OK);
        let first_gap: serde_json::Value =
            serde_json::from_slice(&to_bytes(first_gap.into_body(), 16 * 1024).await.unwrap())
                .unwrap();
        let retry_gap = call(&app, "POST", "/v1/gaps", pairing, Some(gap)).await;
        assert_eq!(retry_gap.status(), StatusCode::OK);
        let retry_gap: serde_json::Value =
            serde_json::from_slice(&to_bytes(retry_gap.into_body(), 16 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(retry_gap["id"], first_gap["id"]);

        let ended = call(
            &app,
            "POST",
            &format!("/v1/sessions/{}/end", first.id),
            "master-token",
            None,
        )
        .await;
        assert_eq!(ended.status(), StatusCode::OK);
        let revoked = call(
            &app,
            "POST",
            "/v1/gaps",
            pairing,
            Some(json!({
                "sessionId": first.id,
                "source": "extension",
                "droppedEvents": 1,
                "reason": "restart",
            })),
        )
        .await;
        assert_eq!(revoked.status(), StatusCode::GONE);

        let unknown = call(
            &app,
            "POST",
            "/v1/gaps",
            "never-issued",
            Some(json!({
                "sessionId": first.id,
                "source": "extension",
                "droppedEvents": 1,
                "reason": "unknown token",
            })),
        )
        .await;
        assert_eq!(unknown.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn proxy_lifecycle_is_idempotent_and_session_bound() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: temp.path().to_path_buf(),
            bind: "127.0.0.1:0".parse().unwrap(),
            token: Some("master-token".into()),
        }
        .prepare()
        .unwrap();
        let store = Arc::new(Store::open(&config.data_dir).unwrap());
        let session = store
            .create_session(CreateSession {
                label: "proxy".into(),
                source: "proxy".into(),
                scope_profile_id: None,
            })
            .unwrap();
        let app = router(AppState::new(config, store));

        let start_path = format!("/v1/sessions/{}/proxy/start", session.id);
        let first = call(&app, "POST", &start_path, "master-token", None).await;
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), 16 * 1024).await.unwrap();
        let first_body: Value = serde_json::from_slice(&first_body).unwrap();
        let second = call(&app, "POST", &start_path, "master-token", None).await;
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_bytes(second.into_body(), 16 * 1024).await.unwrap();
        let second_body: Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(first_body["proxy"]["url"], second_body["proxy"]["url"]);

        let delete = call(
            &app,
            "DELETE",
            &format!("/v1/sessions/{}", session.id),
            "master-token",
            None,
        )
        .await;
        assert_eq!(delete.status(), StatusCode::BAD_REQUEST);

        let stop_path = format!("/v1/sessions/{}/proxy/stop", session.id);
        assert_eq!(
            call(&app, "POST", &stop_path, "master-token", None)
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "POST", &stop_path, "master-token", None)
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            call(
                &app,
                "POST",
                &format!("/v1/sessions/{}/end", session.id),
                "master-token",
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "POST", &start_path, "master-token", None)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn graph_scope_query_accepts_exact_states_and_rejects_unknown_values() {
        let temp = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: temp.path().to_path_buf(),
            bind: "127.0.0.1:0".parse().unwrap(),
            token: Some("master-token".into()),
        }
        .prepare()
        .unwrap();
        let store = Arc::new(Store::open(&config.data_dir).unwrap());
        let app = router(AppState::new(config, store));

        assert_eq!(
            call(
                &app,
                "GET",
                "/v1/map?scope=out_of_scope&inScopeOnly=true",
                "master-token",
                None,
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "GET", "/v1/map?scope=invalid", "master-token", None,)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            call(
                &app,
                "GET",
                "/v1/map/missing?scope=invalid",
                "master-token",
                None,
            )
            .await
            .status(),
            StatusCode::BAD_REQUEST
        );
    }
}
