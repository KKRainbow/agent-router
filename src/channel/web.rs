use std::{
    convert::Infallible,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path as AxumPath, State},
    http::{Request, StatusCode, Uri, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use include_dir::{Dir, include_dir};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};
use uuid::Uuid;

use crate::{
    ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome,
    config::{ChannelEventMode, WebAssetSource, WebConfig},
    router::{RouterChannelEvent, RouterChannelEventKind, RouterOutputSink, RouterService},
    session::{MessageRole, SessionState, TranscriptMessage},
};

const WEB_USER_ID: &str = "local";
const WEB_SOURCE: &str = "web";
static WEB_DIST: Dir<'static> = include_dir!("$OUT_DIR/web-dist-embed");

#[derive(Debug, Clone)]
pub struct WebChannel {
    cfg: WebConfig,
}

impl WebChannel {
    pub fn new(cfg: WebConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        let bind = self.cfg.bind;
        let asset_source = self.cfg.asset_source.clone();
        let app = build_app(self.cfg, router);
        let listener = tokio::net::TcpListener::bind(bind).await?;
        match asset_source {
            WebAssetSource::Embedded => {
                tracing::info!(bind = %bind, "serving web channel with embedded assets");
            }
            WebAssetSource::StaticDir(static_dir) => {
                tracing::info!(
                    bind = %bind,
                    static_dir = %static_dir.display(),
                    "serving web channel with static assets"
                );
            }
        }
        axum::serve(listener, app).await?;
        Ok(())
    }
}

#[derive(Clone)]
struct WebState {
    router: Arc<dyn RouterService>,
    asset_source: WebAssetSource,
    channel_events: ChannelEventMode,
}

pub(crate) fn build_app(cfg: WebConfig, router: Arc<dyn RouterService>) -> Router {
    let state = Arc::new(WebState {
        router,
        asset_source: cfg.asset_source,
        channel_events: cfg.channel_events,
    });

    let mut api = Router::new()
        .route("/bootstrap", get(bootstrap))
        .route("/sessions/{session_id}/transcript", get(transcript))
        .route("/sessions/{session_id}/messages", post(post_message))
        .route("/sessions/{session_id}/stop", post(post_stop))
        .with_state(state.clone());

    if let Some(token) = cfg.auth_token {
        api = api.route_layer(middleware::from_fn_with_state(token, require_auth));
    }

    Router::new()
        .nest("/api/web", api)
        .fallback(get(web_static_asset))
        .with_state(state)
}

async fn web_static_asset(State(state): State<Arc<WebState>>, uri: Uri) -> Response {
    match &state.asset_source {
        WebAssetSource::Embedded => embedded_static_asset(uri.path()),
        WebAssetSource::StaticDir(static_dir) => runtime_static_asset(static_dir, uri.path()).await,
    }
}

fn embedded_static_asset(path: &str) -> Response {
    let asset_path = static_asset_path(path);
    if let Some(file) = WEB_DIST.get_file(asset_path.url_path.as_str()) {
        return embedded_file_response(file, asset_path.url_path.as_str());
    }

    if let Some(file) = WEB_DIST.get_file("index.html") {
        return embedded_file_response(file, "index.html");
    }

    (StatusCode::NOT_FOUND, "embedded web UI is empty").into_response()
}

async fn runtime_static_asset(static_dir: &Path, path: &str) -> Response {
    let asset_path = static_asset_path(path);
    let file_path = static_dir.join(&asset_path.relative_path);

    match runtime_file_response(&file_path, asset_path.url_path.as_str()).await {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    let index_path = static_dir.join("index.html");
    match runtime_file_response(&index_path, "index.html").await {
        Ok(Some(response)) => response,
        Ok(None) => (StatusCode::NOT_FOUND, "static web UI index.html not found").into_response(),
        Err(response) => response,
    }
}

async fn runtime_file_response(
    path: &Path,
    asset_path: &str,
) -> Result<Option<Response>, Response> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Ok(None),
        Err(err) if is_missing_static_asset(&err) => return Ok(None),
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "failed to inspect web static asset");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to inspect static web asset",
            )
                .into_response());
        }
    }

    match tokio::fs::read(path).await {
        Ok(contents) => Ok(Some(
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, asset_content_type(asset_path))
                .body(Body::from(contents))
                .expect("valid runtime asset response"),
        )),
        Err(err) if is_missing_static_asset(&err) => Ok(None),
        Err(err) => {
            tracing::warn!(path = %path.display(), error = %err, "failed to read web static asset");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to read static web asset",
            )
                .into_response())
        }
    }
}

fn is_missing_static_asset(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
    )
}

#[derive(Debug, PartialEq, Eq)]
struct StaticAssetPath {
    relative_path: PathBuf,
    url_path: String,
}

fn static_asset_path(path: &str) -> StaticAssetPath {
    let Some(segments) = safe_static_asset_segments(path) else {
        return index_asset_path();
    };

    let mut relative_path = PathBuf::new();
    for segment in &segments {
        relative_path.push(segment);
    }
    StaticAssetPath {
        url_path: segments.join("/"),
        relative_path,
    }
}

fn safe_static_asset_segments(path: &str) -> Option<Vec<String>> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return Some(vec!["index.html".to_string()]);
    }

    let mut segments = Vec::new();
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        if !is_safe_static_asset_segment(segment) {
            return None;
        }
        segments.push(segment.to_string());
    }

    if segments.is_empty() {
        Some(vec!["index.html".to_string()])
    } else {
        Some(segments)
    }
}

fn is_safe_static_asset_segment(segment: &str) -> bool {
    if segment == "." || segment == ".." || segment.contains(['\\', ':']) {
        return false;
    }
    matches!(
        Path::new(segment).components().next(),
        Some(Component::Normal(_))
    ) && Path::new(segment).components().count() == 1
}

fn index_asset_path() -> StaticAssetPath {
    StaticAssetPath {
        relative_path: PathBuf::from("index.html"),
        url_path: "index.html".to_string(),
    }
}

fn embedded_file_response(file: &include_dir::File<'_>, path: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset_content_type(path))
        .body(Body::from(Bytes::copy_from_slice(file.contents())))
        .expect("valid embedded asset response")
}

fn asset_content_type(path: &str) -> &'static str {
    match std::path::Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
    {
        "css" => "text/css; charset=utf-8",
        "html" => "text/html; charset=utf-8",
        "ico" => "image/x-icon",
        "jpeg" | "jpg" => "image/jpeg",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "txt" => "text/plain; charset=utf-8",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    }
}

async fn require_auth(
    State(expected_token): State<String>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let authorized = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected_token);

    if authorized {
        next.run(req).await
    } else {
        WebError::unauthorized("missing or invalid bearer token").into_response()
    }
}

#[derive(Serialize)]
struct BootstrapResponse {
    user_id: &'static str,
    channel_events: &'static str,
}

async fn bootstrap(State(state): State<Arc<WebState>>) -> Json<BootstrapResponse> {
    Json(BootstrapResponse {
        user_id: WEB_USER_ID,
        channel_events: channel_event_mode_name(state.channel_events),
    })
}

#[derive(Serialize)]
struct TranscriptResponse {
    messages: Vec<WebTranscriptMessage>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct WebTranscriptMessage {
    id: String,
    role: &'static str,
    content: Vec<WebTextContent>,
    created_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    executor: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct WebTextContent {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
}

async fn transcript(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<WebState>>,
) -> Result<Json<TranscriptResponse>, WebError> {
    let session_key = web_session_key(&session_id)?;
    let session = state.router.session_state(&session_key).await?;
    let messages = session
        .map(|state| transcript_messages(&state))
        .unwrap_or_default();
    Ok(Json(TranscriptResponse { messages }))
}

#[derive(Debug, Deserialize)]
struct MessageRequest {
    text: String,
    #[serde(default)]
    client_message_id: Option<String>,
}

async fn post_message(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<WebState>>,
    Json(request): Json<MessageRequest>,
) -> Result<Response, WebError> {
    let session_key = web_session_key(&session_id)?;
    if request.text.trim().is_empty() {
        return Err(WebError::bad_request("message text is required"));
    }
    let _client_message_id = request.client_message_id;

    let (tx, rx) = mpsc::unbounded_channel();
    send_stream_event(
        &tx,
        &WebStreamEvent::Accepted {
            turn_id: Uuid::new_v4().to_string(),
        },
    );

    let router = state.router.clone();
    let channel_events = state.channel_events;
    // Browser disconnect is not the cancellation contract for web turns. The
    // router turn continues so a reload can later hydrate the committed
    // transcript; explicit cancellation goes through /stop for the same session.
    tokio::spawn(async move {
        let mut output = NdjsonOutputSink {
            tx: tx.clone(),
            channel_events,
        };
        if let Err(err) = route_web_text(router, session_key, request.text, &mut output).await {
            send_stream_event(
                &tx,
                &WebStreamEvent::Error {
                    message: err.to_string(),
                },
            );
        }
        send_stream_event(&tx, &WebStreamEvent::Done);
    });

    Ok(ndjson_response(rx))
}

async fn post_stop(
    AxumPath(session_id): AxumPath<String>,
    State(state): State<Arc<WebState>>,
) -> Result<Json<StopResponse>, WebError> {
    let session_key = web_session_key(&session_id)?;
    let mut output = CollectingOutputSink::default();
    route_web_text(
        state.router.clone(),
        session_key,
        "/stop".to_string(),
        &mut output,
    )
    .await?;
    Ok(Json(StopResponse {
        stopped: output.final_reply.as_deref() == Some("Stopped the active turn."),
        text: output.final_reply.unwrap_or_default(),
    }))
}

#[derive(Serialize)]
struct StopResponse {
    stopped: bool,
    text: String,
}

async fn route_web_text(
    router: Arc<dyn RouterService>,
    session_key: String,
    text: String,
    output: &mut dyn RouterOutputSink,
) -> anyhow::Result<()> {
    let outcome = router
        .begin_channel_input(ChannelInput {
            session_key,
            text,
            user_id: Some(WEB_USER_ID.to_string()),
            source: WEB_SOURCE.to_string(),
            intent: ChannelInputIntent::Route,
            context_policy: ChannelContextPolicy::disabled(WEB_SOURCE),
        })
        .await?;
    let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
        return Ok(());
    };
    router.finish_channel_input(ticket, None, output).await
}

fn transcript_messages(state: &SessionState) -> Vec<WebTranscriptMessage> {
    state
        .transcript
        .iter()
        .enumerate()
        .map(|(idx, message)| transcript_message(idx, message))
        .collect()
}

fn transcript_message(idx: usize, message: &TranscriptMessage) -> WebTranscriptMessage {
    WebTranscriptMessage {
        id: format!("committed-{idx}"),
        role: match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
            MessageRole::System => "system",
        },
        content: vec![WebTextContent {
            kind: "text",
            text: message.content.clone(),
        }],
        created_at_ms: message.timestamp_ms,
        executor: message.executor.clone(),
    }
}

fn ndjson_response(rx: mpsc::UnboundedReceiver<Bytes>) -> Response {
    let stream = UnboundedReceiverStream::new(rx).map(Ok::<Bytes, Infallible>);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from_stream(stream))
        .expect("valid NDJSON response")
}

struct NdjsonOutputSink {
    tx: mpsc::UnboundedSender<Bytes>,
    channel_events: ChannelEventMode,
}

#[async_trait]
impl RouterOutputSink for NdjsonOutputSink {
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        if self.channel_events == ChannelEventMode::Off {
            return;
        }
        send_stream_event(
            &self.tx,
            &WebStreamEvent::Activity {
                kind: event.kind.into(),
                executor: event.executor,
                title: event.title,
                text: event.text,
            },
        );
    }

    fn send_reply_break(&mut self) {
        send_stream_event(&self.tx, &WebStreamEvent::ReplyBreak);
    }

    fn send_reply_chunk(&mut self, chunk: String) {
        send_stream_event(&self.tx, &WebStreamEvent::ReplyDelta { text: chunk });
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        if send_stream_event(&self.tx, &WebStreamEvent::FinalReply { text }) {
            Ok(())
        } else {
            anyhow::bail!("web response stream closed")
        }
    }
}

#[derive(Default)]
struct CollectingOutputSink {
    final_reply: Option<String>,
}

#[async_trait]
impl RouterOutputSink for CollectingOutputSink {
    fn send_channel_event(&mut self, _event: RouterChannelEvent) {}

    fn send_reply_chunk(&mut self, _chunk: String) {}

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        self.final_reply = Some(text);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WebStreamEvent {
    Accepted {
        turn_id: String,
    },
    Activity {
        kind: WebActivityKind,
        executor: String,
        title: String,
        text: String,
    },
    ReplyDelta {
        text: String,
    },
    ReplyBreak,
    FinalReply {
        text: String,
    },
    Error {
        message: String,
    },
    Done,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WebActivityKind {
    AgentProgress,
    ReasoningSummary,
    ToolCall,
}

impl From<RouterChannelEventKind> for WebActivityKind {
    fn from(kind: RouterChannelEventKind) -> Self {
        match kind {
            RouterChannelEventKind::AgentProgress => Self::AgentProgress,
            RouterChannelEventKind::ReasoningSummary => Self::ReasoningSummary,
            RouterChannelEventKind::ToolCall => Self::ToolCall,
        }
    }
}

fn send_stream_event(tx: &mpsc::UnboundedSender<Bytes>, event: &WebStreamEvent) -> bool {
    match serde_json::to_vec(event) {
        Ok(mut line) => {
            line.push(b'\n');
            tx.send(Bytes::from(line)).is_ok()
        }
        Err(err) => {
            tracing::warn!(error = %err, "failed to serialize web stream event");
            false
        }
    }
}

fn web_session_key(session_id: &str) -> Result<String, WebError> {
    if is_valid_session_id(session_id) {
        Ok(format!("web:user:{WEB_USER_ID}:{session_id}"))
    } else {
        Err(WebError::bad_request("invalid session id"))
    }
}

pub(crate) fn is_valid_session_id(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id.len() <= 64
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn channel_event_mode_name(mode: ChannelEventMode) -> &'static str {
    match mode {
        ChannelEventMode::Off => "off",
        ChannelEventMode::Compact => "compact",
        ChannelEventMode::Verbose => "verbose",
    }
}

#[derive(Debug)]
struct WebError {
    status: StatusCode,
    message: String,
}

impl WebError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for WebError {
    fn from(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        (
            self.status,
            Json(ErrorResponse {
                message: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, net::SocketAddr, path::PathBuf};

    use axum::{
        body::to_bytes,
        http::{Method, Request},
    };
    use tokio::sync::Mutex;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        ChannelRouteTicket, RouterOutputEvent,
        session::{SessionState, TranscriptMessage},
    };

    #[test]
    fn validates_session_ids() {
        assert!(is_valid_session_id("abc_123-XYZ"));
        assert!(is_valid_session_id(&"a".repeat(64)));
        assert!(!is_valid_session_id(""));
        assert!(!is_valid_session_id(&"a".repeat(65)));
        assert!(!is_valid_session_id("../secret"));
        assert!(!is_valid_session_id("space here"));
    }

    #[test]
    fn serializes_stream_events() {
        let line = serde_json::to_string(&WebStreamEvent::Activity {
            kind: WebActivityKind::ToolCall,
            executor: "kimi".to_string(),
            title: "Bash".to_string(),
            text: "cargo test".to_string(),
        })
        .unwrap();

        assert_eq!(
            line,
            r#"{"type":"activity","kind":"tool_call","executor":"kimi","title":"Bash","text":"cargo test"}"#
        );
    }

    #[test]
    fn static_asset_paths_are_relative_and_safe() {
        assert_eq!(
            static_asset_path("/assets/app.js"),
            StaticAssetPath {
                relative_path: ["assets", "app.js"].iter().collect::<PathBuf>(),
                url_path: "assets/app.js".to_string(),
            }
        );
        assert_eq!(static_asset_path("/"), index_asset_path());
        assert_eq!(static_asset_path("/../secret.txt"), index_asset_path());
        assert_eq!(
            static_asset_path("/C:/Users/secret.txt"),
            index_asset_path()
        );
        assert_eq!(static_asset_path("/assets\\secret.txt"), index_asset_path());
    }

    #[tokio::test]
    async fn serves_embedded_index_from_binary() {
        let app = build_app(web_config(None), Arc::new(RecordingRouter::default()));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<html"));
    }

    #[tokio::test]
    async fn serves_embedded_index_for_frontend_routes() {
        let app = build_app(web_config(None), Arc::new(RecordingRouter::default()));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/sessions/local-session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<html"));
    }

    #[tokio::test]
    async fn static_dir_serves_runtime_assets_instead_of_embedded_assets() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("index.html"),
            "<!doctype html><html><body>runtime static dir</body></html>",
        )
        .unwrap();
        fs::create_dir(dir.path().join("assets")).unwrap();
        fs::write(
            dir.path().join("assets").join("app.js"),
            "console.log('ok');",
        )
        .unwrap();

        let mut cfg = web_config(None);
        cfg.asset_source = WebAssetSource::StaticDir(dir.path().to_path_buf());
        let app = build_app(cfg, Arc::new(RecordingRouter::default()));

        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("runtime static dir")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/assets/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/javascript; charset=utf-8"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            String::from_utf8(body.to_vec()).unwrap(),
            "console.log('ok');"
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/sessions/dev-session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("runtime static dir")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/assets")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("runtime static dir")
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/assets/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("runtime static dir")
        );
    }

    #[tokio::test]
    async fn auth_rejects_missing_or_invalid_bearer_token() {
        let app = build_app(
            web_config(Some("secret")),
            Arc::new(RecordingRouter::default()),
        );

        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/web/bootstrap")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let invalid = app
            .oneshot(
                Request::builder()
                    .uri("/api/web/bootstrap")
                    .header(header::AUTHORIZATION, "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn message_stream_routes_agent_status_and_emits_ndjson() {
        let router = Arc::new(RecordingRouter::with_events(vec![
            RouterOutputEvent::ReplyChunk("hel".to_string()),
            RouterOutputEvent::ReplyChunk("lo".to_string()),
            RouterOutputEvent::ReplyBreak,
            RouterOutputEvent::Channel(RouterChannelEvent {
                kind: RouterChannelEventKind::AgentProgress,
                executor: "kimi".to_string(),
                title: "Progress".to_string(),
                text: "working".to_string(),
            }),
            RouterOutputEvent::FinalReply("hello".to_string()),
        ]));
        let app = build_app(web_config(None), router.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/web/sessions/session_1/messages")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"text":"/agent status"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        let lines = body.lines().collect::<Vec<_>>();
        assert!(lines[0].starts_with(r#"{"type":"accepted","turn_id":"#));
        assert_eq!(lines[1], r#"{"type":"reply_delta","text":"hel"}"#);
        assert_eq!(lines[2], r#"{"type":"reply_delta","text":"lo"}"#);
        assert_eq!(lines[3], r#"{"type":"reply_break"}"#);
        assert_eq!(
            lines[4],
            r#"{"type":"activity","kind":"agent_progress","executor":"kimi","title":"Progress","text":"working"}"#
        );
        assert_eq!(lines[5], r#"{"type":"final_reply","text":"hello"}"#);
        assert_eq!(lines[6], r#"{"type":"done"}"#);

        let began = router.began.lock().await;
        assert_eq!(began.len(), 1);
        assert_eq!(began[0].session_key, "web:user:local:session_1");
        assert_eq!(began[0].text, "/agent status");
        assert_eq!(began[0].user_id.as_deref(), Some(WEB_USER_ID));
        assert_eq!(began[0].source, WEB_SOURCE);
        assert_eq!(
            began[0].context_policy,
            ChannelContextPolicy::disabled(WEB_SOURCE)
        );
    }

    #[tokio::test]
    async fn stop_routes_stop_command_for_same_web_session() {
        let router = Arc::new(RecordingRouter::with_events(vec![
            RouterOutputEvent::FinalReply("Stopped the active turn.".to_string()),
        ]));
        let app = build_app(web_config(None), router.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/web/sessions/session-2/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            String::from_utf8(body.to_vec()).unwrap(),
            r#"{"stopped":true,"text":"Stopped the active turn."}"#
        );

        let began = router.began.lock().await;
        assert_eq!(began.len(), 1);
        assert_eq!(began[0].session_key, "web:user:local:session-2");
        assert_eq!(began[0].text, "/stop");
    }

    #[tokio::test]
    async fn stop_reports_when_no_active_turn_was_stopped() {
        let router = Arc::new(RecordingRouter::with_events(vec![
            RouterOutputEvent::FinalReply("No active turn for this session.".to_string()),
        ]));
        let app = build_app(web_config(None), router);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/web/sessions/session-2/stop")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            String::from_utf8(body.to_vec()).unwrap(),
            r#"{"stopped":false,"text":"No active turn for this session."}"#
        );
    }

    #[tokio::test]
    async fn dropped_response_stream_does_not_cancel_routing() {
        let router = Arc::new(RecordingRouter::with_events(vec![
            RouterOutputEvent::ReplyChunk("partial".to_string()),
            RouterOutputEvent::FinalReply("done".to_string()),
        ]));
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let mut output = NdjsonOutputSink {
            tx,
            channel_events: ChannelEventMode::Compact,
        };

        let err = route_web_text(
            router.clone(),
            "web:user:local:s1".to_string(),
            "run".to_string(),
            &mut output,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("web response stream closed"));
        assert_eq!(*router.finished.lock().await, 1);
    }

    #[tokio::test]
    async fn transcript_returns_committed_session_messages() {
        let mut state = SessionState::new("web:user:local:s1", "kimi");
        state.transcript.push(TranscriptMessage::user("hi"));
        state
            .transcript
            .push(TranscriptMessage::assistant("hello", "kimi", None));
        let router = Arc::new(RecordingRouter::with_session_state(state));
        let app = build_app(web_config(None), router);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/web/sessions/s1/transcript")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"][0]["text"], "hi");
        assert_eq!(body["messages"][1]["role"], "assistant");
        assert_eq!(body["messages"][1]["content"][0]["text"], "hello");
        assert_eq!(body["messages"][1]["executor"], "kimi");
    }

    fn web_config(auth_token: Option<&str>) -> WebConfig {
        WebConfig {
            enabled: true,
            bind: "127.0.0.1:8787".parse::<SocketAddr>().unwrap(),
            asset_source: WebAssetSource::Embedded,
            channel_events: ChannelEventMode::Compact,
            auth_token: auth_token.map(str::to_string),
        }
    }

    #[derive(Default)]
    struct RecordingRouter {
        began: Mutex<Vec<ChannelInput>>,
        finished: Mutex<usize>,
        events: Vec<RouterOutputEvent>,
        states: Mutex<HashMap<String, SessionState>>,
    }

    impl RecordingRouter {
        fn with_events(events: Vec<RouterOutputEvent>) -> Self {
            Self {
                events,
                ..Self::default()
            }
        }

        fn with_session_state(state: SessionState) -> Self {
            let mut states = HashMap::new();
            states.insert(state.session_key.clone(), state);
            Self {
                states: Mutex::new(states),
                ..Self::default()
            }
        }
    }

    #[async_trait]
    impl RouterService for RecordingRouter {
        async fn begin_channel_input(
            &self,
            input: ChannelInput,
        ) -> anyhow::Result<ChannelIntakeOutcome> {
            self.began.lock().await.push(input.clone());
            Ok(ChannelIntakeOutcome::Route {
                ticket: ChannelRouteTicket::for_test(input),
                context_allowed: false,
            })
        }

        async fn finish_channel_input(
            &self,
            _ticket: ChannelRouteTicket,
            _context: Option<crate::session::ContextSyncRequest>,
            output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            *self.finished.lock().await += 1;
            for event in self.events.clone() {
                match event {
                    RouterOutputEvent::Channel(event) => output.send_channel_event(event),
                    RouterOutputEvent::ReplyBreak => output.send_reply_break(),
                    RouterOutputEvent::ReplyChunk(chunk) => output.send_reply_chunk(chunk),
                    RouterOutputEvent::FinalReply(text) => output.send_final_reply(text).await?,
                }
            }
            Ok(())
        }

        async fn session_state(&self, session_key: &str) -> anyhow::Result<Option<SessionState>> {
            Ok(self.states.lock().await.get(session_key).cloned())
        }

        async fn handle(
            &self,
            _input: crate::RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn observe(&self, _input: crate::RouterInput) -> anyhow::Result<()> {
            Ok(())
        }
    }
}
