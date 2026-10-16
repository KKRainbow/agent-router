use super::*;
use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use serde_json::Value;
use tokio::sync::Mutex;
use tower::ServiceExt;

use crate::{
    agent_api::{API_TOKEN_ENV, AgentApi, build_app},
    executor::{
        ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptOutcome, ExecutorPromptRequest, PreparedExecutor, TurnCancellation,
        test_support::FakeExecutorBackend,
    },
    router::{AgentRouter, RouterChannelEvent, RouterInput, RouterOutputSink},
    session::store::{InMemorySessionStore, SessionStore},
};

#[derive(Default)]
struct RecordingDelivery {
    sent: Mutex<Vec<(MessageTarget, String)>>,
    fail: bool,
    disabled: Option<MessageChannel>,
}

#[async_trait]
impl NotificationDelivery for RecordingDelivery {
    fn supports(&self, channel: MessageChannel) -> bool {
        self.disabled != Some(channel)
    }

    async fn deliver(&self, target: &MessageTarget, text: &str) -> anyhow::Result<()> {
        self.sent.lock().await.push((target.clone(), text.into()));
        anyhow::ensure!(!self.fail, "platform refused delivery");
        Ok(())
    }
}

fn target(channel: MessageChannel, id: &str, thread: Option<&str>) -> MessageTarget {
    MessageTarget {
        channel,
        target: id.into(),
        thread: thread.map(str::to_owned),
    }
}

fn configured_targets() -> BTreeMap<String, MessageTarget> {
    BTreeMap::from([
        (
            "reports".into(),
            target(MessageChannel::Slack, "C_REPORTS", None),
        ),
        (
            "private-topic".into(),
            target(MessageChannel::Telegram, "-100", Some("42")),
        ),
        (
            "qq-group".into(),
            target(MessageChannel::Qq, "group:GROUP", None),
        ),
        (
            "web-chat".into(),
            target(MessageChannel::Web, "user:local:other", None),
        ),
    ])
}

fn app(
    api: Arc<AgentApi>,
    delivery: Arc<RecordingDelivery>,
    store: Arc<InMemorySessionStore>,
) -> Router {
    let router = Arc::new(AgentRouter::new(
        "kimi",
        store,
        Arc::new(FakeExecutorBackend::default()),
    ));
    build_app(api, routes(router, delivery, configured_targets()))
}

fn credential(api: &AgentApi, execution: &str, origin: &str) -> String {
    api.environment(execution, origin, "kimi", Some("U1".into()))[API_TOKEN_ENV].clone()
}

async fn request(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .method(method)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn discovery_and_sending_share_auth_without_scheduling() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "schedule:task", "slack:channel:C_ORIGIN:123.456");
    let delivery = Arc::new(RecordingDelivery {
        disabled: Some(MessageChannel::Qq),
        ..Default::default()
    });
    let store = Arc::new(InMemorySessionStore::default());
    let app = app(api.clone(), delivery.clone(), store.clone());
    for (method, path) in [("GET", "/api/channels"), ("POST", "/api/messages")] {
        assert_eq!(
            request(&app, "invalid", method, path, json!({"text":"hello"}))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }
    let (status, body) = request(&app, &token, "GET", "/api/channels", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["current"],
        json!({"channel":"slack", "target":"C_ORIGIN", "thread":"123.456"})
    );
    assert_eq!(body["targets"]["reports"]["target"], "C_REPORTS");
    assert!(body["targets"].get("qq-group").is_none());

    let (status, body) = request(
        &app,
        &token,
        "POST",
        "/api/messages",
        json!({"text":"progress"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["recorded"], true);
    assert_eq!(body["delivery"], "accepted");
    let state = store
        .load("slack:channel:C_ORIGIN:123.456")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.transcript.len(), 1);
    assert_eq!(state.transcript[0].content, "progress");
    assert!(store.load("schedule:task").await.unwrap().is_none());
    assert_eq!(
        delivery.sent.lock().await[0].0,
        target(MessageChannel::Slack, "C_ORIGIN", Some("123.456"))
    );

    // Replacing a process identity also revokes access to outbound messages.
    let replacement = api.environment(
        "schedule:task",
        "slack:channel:C_ORIGIN:123.456",
        "kimi",
        Some("U2".into()),
    );
    assert_ne!(replacement[API_TOKEN_ENV], token);
    assert_eq!(
        request(
            &app,
            &token,
            "POST",
            "/api/messages",
            json!({"text":"stale"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn explicit_targets_enforce_configured_channel_and_thread_grants() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "first", "slack:channel:C_ORIGIN:123.456");
    let second = credential(&api, "second", "slack:channel:C_OTHER:789.000");
    let delivery = Arc::new(RecordingDelivery::default());
    let store = Arc::new(InMemorySessionStore::default());
    let app = app(api, delivery.clone(), store.clone());
    for body in [
        json!({"channel":"slack", "target":"C_REPORTS", "text":"report"}),
        json!({"channel":"slack", "target":"C_REPORTS", "thread":"555.000", "text":"thread report"}),
        json!({"channel":"telegram", "target":"-100", "thread":"42", "text":"topic report"}),
        json!({"channel":"qq", "target":"group:GROUP", "text":"qq report"}),
    ] {
        let (status, response) = request(&app, &token, "POST", "/api/messages", body).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["recorded"], false);
    }
    // Explicit cross-channel sends do not fabricate sessions or notify the source.
    assert!(
        store
            .load("slack:channel:C_ORIGIN:123.456")
            .await
            .unwrap()
            .is_none()
    );
    for body in [
        json!({"channel":"slack", "target":"C_OTHER", "thread":"789.000", "text":"not mine"}),
        json!({"channel":"slack", "target":"C_ORIGIN", "text":"parent channel is not my thread"}),
        json!({"channel":"slack", "target":"C_ORIGIN", "thread":"999.000", "text":"sibling"}),
        json!({"channel":"telegram", "target":"-100", "text":"outside topic"}),
        json!({"channel":"telegram", "target":"-100", "thread":"43", "text":"wrong topic"}),
        json!({"channel":"qq", "target":"c2c:GROUP", "text":"wrong target kind"}),
        json!({"channel":"web", "target":"user:local:private", "text":"unknown conversation"}),
    ] {
        assert_eq!(
            request(&app, &token, "POST", "/api/messages", body).await.0,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(delivery.sent.lock().await.len(), 4);
    assert_eq!(
        request(
            &app,
            &second,
            "POST",
            "/api/messages",
            json!({"text":"mine"})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(delivery.sent.lock().await[4].0.target, "C_OTHER");
}

#[tokio::test]
async fn validation_and_disabled_channels_have_no_side_effects() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "session", "slack:channel:C_ORIGIN:123.456");
    let delivery = Arc::new(RecordingDelivery {
        disabled: Some(MessageChannel::Slack),
        ..Default::default()
    });
    let store = Arc::new(InMemorySessionStore::default());
    let app = app(api, delivery.clone(), store.clone());
    for body in [
        json!({"text":" \n\t"}),
        json!({"text":"中".repeat(11_000)}),
        json!({"channel":"slack", "text":"missing target"}),
        json!({"target":"C_REPORTS", "text":"missing channel"}),
        json!({"thread":"123", "text":"missing target"}),
        json!({"channel":"slack", "target":"", "text":"empty target"}),
        json!({"channel":"slack", "target":"C_REPORTS", "thread":"", "text":"empty thread"}),
        json!({"channel":"qq", "target":"group:GROUP", "thread":"123", "text":"no threads"}),
        json!({"channel":"qq", "target":"group:../GROUP", "text":"invalid ID"}),
    ] {
        assert_eq!(
            request(&app, &token, "POST", "/api/messages", body).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        request(
            &app,
            &token,
            "POST",
            "/api/messages",
            json!({"text":"hello", "session_key":"other"})
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, body) = request(
        &app,
        &token,
        "POST",
        "/api/messages",
        json!({"text":"disabled"}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["recorded"], false);
    assert_eq!(body["delivery"], "not_attempted");
    assert!(delivery.sent.lock().await.is_empty());
    assert!(
        store
            .load("slack:channel:C_ORIGIN:123.456")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delivery_failure_reports_saved_history_without_retrying() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "session", "slack:channel:C_ORIGIN:123.456");
    let delivery = Arc::new(RecordingDelivery {
        fail: true,
        ..Default::default()
    });
    let store = Arc::new(InMemorySessionStore::default());
    let app = app(api, delivery.clone(), store.clone());
    let (status, body) = request(
        &app,
        &token,
        "POST",
        "/api/messages",
        json!({"text":"saved"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body["recorded"], true);
    assert_eq!(body["delivery"], "unknown");
    assert_eq!(
        store
            .load("slack:channel:C_ORIGIN:123.456")
            .await
            .unwrap()
            .unwrap()
            .transcript
            .len(),
        1
    );
    assert_eq!(delivery.sent.lock().await.len(), 1);
}

struct FailingStore;

#[async_trait]
impl SessionStore for FailingStore {
    async fn load(&self, _: &str) -> anyhow::Result<Option<crate::session::SessionState>> {
        anyhow::bail!("storage unavailable")
    }

    async fn load_or_create(
        &self,
        _: &str,
        _: &str,
    ) -> anyhow::Result<crate::session::SessionState> {
        anyhow::bail!("storage unavailable")
    }

    async fn save(&self, _: crate::session::SessionState) -> anyhow::Result<()> {
        anyhow::bail!("storage unavailable")
    }
}

#[tokio::test]
async fn persistence_failure_prevents_channel_delivery() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "session", "slack:channel:C_ORIGIN:123.456");
    let delivery = Arc::new(RecordingDelivery::default());
    let router = Arc::new(AgentRouter::new(
        "kimi",
        Arc::new(FailingStore),
        Arc::new(FakeExecutorBackend::default()),
    ));
    let app = build_app(api, routes(router, delivery.clone(), configured_targets()));
    let (status, body) = request(
        &app,
        &token,
        "POST",
        "/api/messages",
        json!({"text":"hello"}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["recorded"], false);
    assert_eq!(body["delivery"], "not_attempted");
    assert!(delivery.sent.lock().await.is_empty());
}

#[tokio::test]
async fn local_targets_persist_and_publish_to_the_selected_conversation() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let token = credential(&api, "session", "tui:user:local:my work");
    let delivery = Arc::new(RecordingDelivery::default());
    let store = Arc::new(InMemorySessionStore::default());
    let router = Arc::new(AgentRouter::new(
        "kimi",
        store.clone(),
        Arc::new(FakeExecutorBackend::default()),
    ));
    let mut notifications = router.subscribe_notifications().unwrap();
    let app = build_app(api, routes(router, delivery, configured_targets()));
    for (body, expected_session) in [
        (json!({"text":"local"}), "tui:user:local:my work"),
        (
            json!({"channel":"web", "target":"user:local:other", "text":"local"}),
            "web:user:local:other",
        ),
    ] {
        let (status, response) = request(&app, &token, "POST", "/api/messages", body).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["recorded"], true);
        assert_eq!(
            notifications.try_recv().unwrap().session_key,
            expected_session
        );
        assert_eq!(
            store
                .load(expected_session)
                .await
                .unwrap()
                .unwrap()
                .transcript[0]
                .content,
            "local"
        );
    }
}

struct MessagingExecutor {
    backend: FakeExecutorBackend,
    app: Mutex<Option<(Router, String)>>,
}

#[async_trait]
impl ExecutorBackend for MessagingExecutor {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.backend.get(name)
    }
    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.backend.list()
    }

    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        self.backend.prepare(request, cancel).await
    }

    async fn prompt(
        &self,
        input: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        let (app, token) = self.app.lock().await.as_ref().unwrap().clone();
        let (status, body) = request(
            &app,
            &token,
            "POST",
            "/api/messages",
            json!({"text":"in progress"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        self.backend.prompt(input, events, cancel).await
    }
}

#[derive(Default)]
struct Output;

#[async_trait]
impl RouterOutputSink for Output {
    fn send_channel_event(&mut self, _: RouterChannelEvent) {}
    async fn send_final_reply(&mut self, _: String) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn an_executor_can_send_during_its_turn_without_losing_transcript_updates() {
    let api = AgentApi::new(vec!["kimi".into()]);
    let origin = "tui:user:local:origin";
    let token = credential(&api, origin, origin);
    let backend = Arc::new(MessagingExecutor {
        backend: FakeExecutorBackend::default(),
        app: Mutex::new(None),
    });
    let store = Arc::new(InMemorySessionStore::default());
    let router = Arc::new(
        AgentRouter::new("kimi", store.clone(), backend.clone()).with_agent_api(Some(api.clone())),
    );
    let app = build_app(
        api,
        routes(
            router.clone(),
            Arc::new(RecordingDelivery::default()),
            BTreeMap::new(),
        ),
    );
    *backend.app.lock().await = Some((app, token));
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        router.handle(
            RouterInput {
                session_key: origin.into(),
                text: "do work".into(),
                user_id: Some("U1".into()),
            },
            &mut Output,
        ),
    )
    .await
    .expect("sending must not wait for the active turn")
    .unwrap();
    let transcript = store.load(origin).await.unwrap().unwrap().transcript;
    assert!(
        transcript
            .iter()
            .any(|message| message.content == "in progress")
    );
    assert!(
        transcript
            .iter()
            .any(|message| message.content == "do work")
    );
    assert!(
        transcript
            .iter()
            .any(|message| message.content.contains("fake response"))
    );
    *backend.app.lock().await = None;
}
