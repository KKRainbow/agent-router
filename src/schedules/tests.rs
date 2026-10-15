use super::*;
use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    agent_api::{API_TOKEN_ENV, AgentApi, build_app},
    executor::{
        ExecutorBackend, ExecutorDescriptor, ExecutorEventSink, ExecutorPrepareRequest,
        ExecutorPromptOutcome, ExecutorPromptRequest, PreparedExecutor, TurnCancellation,
        test_support::FakeExecutorBackend,
    },
    router::{
        AgentRouter, RouterChannelEvent, RouterInput, RouterOutputSink, RouterService,
        SessionNotification,
    },
    session::{
        AgentRoutingMode,
        store::{InMemorySessionStore, SessionStore},
    },
};

fn scheduler() -> (tempfile::TempDir, Arc<Scheduler>, Arc<AgentApi>, String) {
    let root = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::open(root.path(), vec!["kimi".into()]).unwrap();
    let api = AgentApi::new(vec!["kimi".into()]);
    let token =
        api.environment("tui:owner", "tui:owner", "kimi", Some("U1".into()))[API_TOKEN_ENV].clone();
    (root, scheduler, api, token)
}

fn task_body() -> Value {
    json!({"name":"Check CI", "prompt":"Check https://example.com/ci", "schedule":{"type":"interval","seconds":60}})
}

async fn request(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(
                    body.map(|body| body.to_string()).unwrap_or_default(),
                ))
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

async fn create(scheduler: &Arc<Scheduler>, api: &Arc<AgentApi>, token: &str) -> ScheduledTask {
    let (status, body) = request(
        &build_app(api.clone(), routes(scheduler.clone())),
        token,
        "POST",
        "/api/schedules/tasks",
        Some(task_body()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    serde_json::from_value(body).unwrap()
}

fn timestamp(value: &str) -> u64 {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .timestamp_millis() as u64
}

#[test]
fn schedules_validate_time_offsets_intervals_and_timezones() {
    let now = timestamp("2030-01-02T00:00:00Z");
    let once = Schedule::Once {
        at: "2030-01-02T09:00:00+08:00".into(),
    };
    assert_eq!(once.next_after(now).unwrap(), now + 3_600_000);
    assert!(once.next_after(now + 3_600_000).is_err());
    assert!(
        Schedule::Once {
            at: "2030-01-02T09:00:00".into()
        }
        .next_after(now)
        .is_err()
    );
    assert!(Schedule::Interval { seconds: 0 }.next_after(now).is_err());
    assert!(
        Schedule::Interval { seconds: u64::MAX }
            .next_after(now)
            .is_err()
    );
    let cron = Schedule::Cron {
        expression: "0 9 * * 1-5".into(),
        timezone: "Asia/Shanghai".into(),
    };
    assert_eq!(cron.next_after(now).unwrap(), now + 3_600_000);
    assert!(
        Schedule::Cron {
            expression: "* * * * * *".into(),
            timezone: "UTC".into()
        }
        .next_after(now)
        .is_err()
    );
    assert!(
        Schedule::Cron {
            expression: "* * * * *".into(),
            timezone: "Mars".into()
        }
        .next_after(now)
        .is_err()
    );
}

#[test]
fn cron_follows_local_time_across_daylight_saving() {
    let cron = Schedule::Cron {
        expression: "0 9 * * *".into(),
        timezone: "America/New_York".into(),
    };
    let before = timestamp("2030-03-09T14:00:00Z");
    assert_eq!(
        cron.next_after(before).unwrap(),
        timestamp("2030-03-10T13:00:00Z")
    );
}

#[tokio::test]
async fn api_scopes_tasks_and_revokes_changed_process_identity() {
    let (_root, scheduler, api, token) = scheduler();
    let app = build_app(api.clone(), routes(scheduler.clone()));
    assert_eq!(
        request(&app, "wrong", "GET", "/api/schedules/tasks", None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let task = create(&scheduler, &api, &token).await;
    assert_eq!(task.requester_user_id.as_deref(), Some("U1"));
    let other = api.environment("other", "other", "kimi", None)[API_TOKEN_ENV].clone();
    assert_eq!(
        request(&app, &other, "GET", "/api/schedules/tasks", None)
            .await
            .1,
        json!([])
    );
    let path = format!("/api/schedules/tasks/{}", task.id);
    for method in ["GET", "PATCH", "DELETE"] {
        assert_eq!(
            request(&app, &other, method, &path, Some(json!({})))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(
        request(&app, &other, "POST", &format!("{path}/run"), None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        api.environment("tui:owner", "tui:owner", "kimi", Some("U1".into()))[API_TOKEN_ENV],
        token
    );
    let replaced =
        api.environment("tui:owner", "tui:owner", "kimi", Some("U2".into()))[API_TOKEN_ENV].clone();
    assert_ne!(replaced, token);
    assert_eq!(
        request(&app, &token, "GET", "/api/schedules/tasks", None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        request(&app, &replaced, "GET", &path, None).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn api_rejects_owner_injection_and_invalid_schedule() {
    let (_root, scheduler, api, token) = scheduler();
    let app = build_app(api.clone(), routes(scheduler));
    for (field, value) in [
        ("owner_session_key", json!("other")),
        ("executor", json!("unknown")),
        ("schedule", json!({"type":"interval","seconds":0})),
    ] {
        let mut body = task_body();
        body[field] = value;
        assert!(
            request(&app, &token, "POST", "/api/schedules/tasks", Some(body))
                .await
                .0
                .is_client_error()
        );
    }
}

#[tokio::test]
async fn pause_manual_run_self_update_and_delete_are_persistent() {
    let (root, scheduler, api, token) = scheduler();
    let app = build_app(api.clone(), routes(scheduler.clone()));
    let task = create(&scheduler, &api, &token).await;
    let path = format!("/api/schedules/tasks/{}", task.id);
    let (_, paused) = request(&app, &token, "PATCH", &path, Some(json!({"enabled":false}))).await;
    assert_eq!(paused["enabled"], false);
    assert!(paused["next_run_at_ms"].is_null());
    assert_eq!(
        request(&app, &token, "POST", &format!("{path}/run"), None)
            .await
            .0,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        request(&app, &token, "POST", &format!("{path}/run"), None)
            .await
            .0,
        StatusCode::CONFLICT
    );
    let jobs = scheduler.claim_ready(now_ms(), 4).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(
        scheduler
            .claim_ready(now_ms() + 1_000_000, 4)
            .await
            .unwrap()
            .is_empty()
    );
    // The background process has the same owner scope and can manage itself.
    let background = api.environment(
        &task.session_key(),
        &task.owner_session_key,
        "kimi",
        task.requester_user_id.clone(),
    )[API_TOKEN_ENV]
        .clone();
    assert_eq!(
        request(
            &app,
            &background,
            "PATCH",
            &path,
            Some(json!({"name":"Completed check"}))
        )
        .await
        .0,
        StatusCode::OK
    );
    scheduler
        .finish_run(&task.id, &jobs[0].1, RunStatus::Succeeded, "done", None)
        .await
        .unwrap();
    drop(app);
    drop(scheduler);
    let reopened = Scheduler::open(root.path(), vec!["kimi".into()]).unwrap();
    let saved = reopened.state.lock().await.tasks[&task.id].clone();
    assert_eq!(saved.name, "Completed check");
    assert!(!saved.enabled);
    assert_eq!(saved.runs[0].status, RunStatus::Succeeded);
    assert_eq!(saved.runs[0].output, "done");
    let token = api.environment("tui:owner", "tui:owner", "kimi", None)[API_TOKEN_ENV].clone();
    let app = build_app(api.clone(), routes(reopened));
    assert_eq!(
        request(&app, &token, "DELETE", &path, None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        request(&app, &token, "GET", &path, None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn claim_limits_concurrency_and_skips_missed_recurrences() {
    let (_root, scheduler, api, token) = scheduler();
    for _ in 0..3 {
        create(&scheduler, &api, &token).await;
    }
    let due = now_ms() + 600_000;
    let jobs = scheduler.claim_ready(due, 2).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|(task, _)| task.next_run_at_ms == Some(due + 60_000))
    );
    assert_eq!(scheduler.claim_ready(due, 2).await.unwrap().len(), 1);
    assert!(
        scheduler
            .claim_ready(due + 600_000, 2)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn restart_interrupts_running_jobs_without_replay_and_runs_overdue_once() {
    let (root, scheduler, api, token) = scheduler();
    let running = create(&scheduler, &api, &token).await;
    scheduler.claim_ready(now_ms() + 60_001, 1).await.unwrap();
    let scope = api.authenticate(&token).unwrap();
    let once = scheduler
        .create(
            &scope,
            CreateTask {
                name: "once".into(),
                prompt: "once".into(),
                executor: None,
                schedule: Schedule::Once {
                    at: "2099-01-01T00:00:00Z".into(),
                },
            },
        )
        .await
        .unwrap();
    {
        let mut state = scheduler.state.lock().await;
        state.tasks.get_mut(&running.id).unwrap().next_run_at_ms = Some(1);
        state.tasks.get_mut(&once.id).unwrap().next_run_at_ms = Some(1);
        scheduler.persist(&state).unwrap();
    }
    assert!(Scheduler::open(root.path(), vec!["kimi".into()]).is_err());
    drop(scheduler);
    let reopened = Scheduler::open(root.path(), vec!["kimi".into()]).unwrap();
    let jobs = reopened.claim_ready(now_ms(), 4).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].0.id, once.id);
    assert!(!jobs[0].0.enabled);
    let state = reopened.state.lock().await;
    assert_eq!(
        state.tasks[&running.id].runs[0].status,
        RunStatus::Interrupted
    );
    assert!(state.tasks[&running.id].next_run_at_ms.unwrap() > now_ms());
}

#[tokio::test]
async fn failed_persistence_does_not_publish_mutation() {
    let (_root, scheduler, api, token) = scheduler();
    let task = create(&scheduler, &api, &token).await;
    std::fs::remove_file(&scheduler.path).unwrap();
    std::fs::create_dir(&scheduler.path).unwrap();
    let app = build_app(api.clone(), routes(scheduler.clone()));
    let path = format!("/api/schedules/tasks/{}", task.id);
    assert_eq!(
        request(&app, &token, "DELETE", &path, None).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert!(scheduler.state.lock().await.tasks.contains_key(&task.id));
}

struct FailedDelivery;
#[async_trait]
impl ScheduleDelivery for FailedDelivery {
    async fn deliver(&self, _: &str, _: &str) -> anyhow::Result<()> {
        anyhow::bail!("platform refused delivery")
    }
}

#[tokio::test]
async fn scheduled_execution_uses_its_own_session_and_saves_result_before_delivery() {
    let (root, scheduler, api, token) = scheduler();
    let task = create(&scheduler, &api, &token).await;
    let store = Arc::new(InMemorySessionStore::default());
    let backend = Arc::new(FakeExecutorBackend::default());
    let router = Arc::new(
        AgentRouter::new("kimi", store.clone(), backend.clone())
            .with_workspace_root(Some(root.path().into()))
            .with_agent_api(Some(api.clone())),
    );
    let mut notifications = router.subscribe_notifications().unwrap();
    let mut origin = store
        .load_or_create(&task.owner_session_key, "kimi")
        .await
        .unwrap();
    origin
        .transcript
        .push(crate::session::TranscriptMessage::user(
            "private original context",
        ));
    store.save(origin).await.unwrap();
    let (claimed, run) = scheduler
        .claim_ready(now_ms() + 60_001, 1)
        .await
        .unwrap()
        .remove(0);
    scheduler
        .execute(router, Arc::new(FailedDelivery), claimed, run, 10)
        .await
        .unwrap();
    let prompts = backend.prompts.lock().await;
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0].session_key, task.session_key());
    assert_eq!(prompts[0].user_id.as_deref(), Some("U1"));
    assert!(!prompts[0].prompt.contains("private original context"));
    let prepared = backend.prepared.lock().await;
    assert_eq!(prepared[0].environment["AGENT_ROUTER_TASK_ID"], task.id);
    assert_eq!(
        api.authenticate(&prepared[0].environment[API_TOKEN_ENV])
            .unwrap()
            .owner_session_key,
        task.owner_session_key
    );
    let background = store
        .load_or_create(&task.session_key(), "kimi")
        .await
        .unwrap();
    assert_eq!(background.routing_mode, AgentRoutingMode::Manual);
    let original = store
        .load_or_create(&task.owner_session_key, "kimi")
        .await
        .unwrap();
    assert_eq!(original.transcript.len(), 2);
    let notification = notifications.recv().await.unwrap();
    assert!(
        notification.text.contains("fake response"),
        "{}",
        notification.text
    );
    let state = scheduler.state.lock().await;
    let run = &state.tasks[&task.id].runs[0];
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(run.output, "fake response");
    assert!(
        run.delivery_error
            .as_deref()
            .unwrap()
            .contains("platform refused")
    );
}

#[derive(Default)]
struct HangingRouter {
    stopped: std::sync::atomic::AtomicBool,
    drained: std::sync::atomic::AtomicBool,
    stop_requested: Notify,
    cancelled: Notify,
    release_cleanup: Notify,
}
#[async_trait]
impl RouterService for HangingRouter {
    async fn run_scheduled(
        &self,
        _: &ScheduledTask,
        _: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        self.cancelled.notified().await;
        self.release_cleanup.notified().await;
        self.drained
            .store(true, std::sync::atomic::Ordering::SeqCst);
        anyhow::bail!("cancelled")
    }
    async fn handle(&self, input: RouterInput, _: &mut dyn RouterOutputSink) -> anyhow::Result<()> {
        assert!(input.session_key.starts_with("schedule:"));
        assert_eq!(input.text, "/stop");
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.cancelled.notify_one();
        self.stop_requested.notify_one();
        Ok(())
    }
    async fn observe(&self, _: RouterInput) -> anyhow::Result<()> {
        Ok(())
    }
    async fn append_notification(&self, _: SessionNotification) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn execution_timeout_drains_cancellation_before_releasing_the_task() {
    let (_root, scheduler, api, token) = scheduler();
    create(&scheduler, &api, &token).await;
    let (task, run) = scheduler
        .claim_ready(now_ms() + 60_001, 1)
        .await
        .unwrap()
        .remove(0);
    let router = Arc::new(HangingRouter::default());
    let execution = tokio::spawn({
        let scheduler = scheduler.clone();
        let router = router.clone();
        let task = task.clone();
        async move {
            scheduler
                .execute(router, Arc::new(FailedDelivery), task, run, 0)
                .await
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        router.stop_requested.notified(),
    )
    .await
    .unwrap();
    assert!(router.stopped.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        scheduler.state.lock().await.tasks[&task.id].runs[0].status,
        RunStatus::Running
    );
    assert!(
        scheduler
            .claim_ready(now_ms() + 600_000, 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!execution.is_finished());
    router.release_cleanup.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(router.drained.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        scheduler.state.lock().await.tasks[&task.id].runs[0].status,
        RunStatus::Failed
    );
}

#[derive(Default)]
struct ConcurrentExecutor {
    fake: FakeExecutorBackend,
    started: Notify,
    release: Notify,
}

#[derive(Default)]
struct DelayedSessionStore {
    inner: InMemorySessionStore,
    saving: Notify,
    release: Notify,
}

#[async_trait]
impl SessionStore for DelayedSessionStore {
    async fn load(&self, key: &str) -> anyhow::Result<Option<crate::session::SessionState>> {
        self.inner.load(key).await
    }
    async fn load_or_create(
        &self,
        key: &str,
        executor: &str,
    ) -> anyhow::Result<crate::session::SessionState> {
        self.inner.load_or_create(key, executor).await
    }
    async fn save(&self, state: crate::session::SessionState) -> anyhow::Result<()> {
        if state.session_key.starts_with("schedule:") {
            self.saving.notify_one();
            self.release.notified().await;
        }
        self.inner.save(state).await
    }
}

#[tokio::test]
async fn stop_during_scheduled_session_setup_prevents_executor_start() {
    let (_root, scheduler, api, token) = scheduler();
    let task = create(&scheduler, &api, &token).await;
    let store = Arc::new(DelayedSessionStore::default());
    let executor = Arc::new(FakeExecutorBackend::default());
    let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor.clone()));
    let execution = tokio::spawn({
        let router = router.clone();
        let task = task.clone();
        async move { router.run_scheduled(&task, &mut SilentOutput).await }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), store.saving.notified())
        .await
        .unwrap();
    router
        .handle(
            RouterInput {
                session_key: task.session_key(),
                text: "/stop".into(),
                user_id: None,
            },
            &mut SilentOutput,
        )
        .await
        .unwrap();
    store.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(executor.prepared.lock().await.is_empty());
}

#[async_trait]
impl ExecutorBackend for ConcurrentExecutor {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.fake.get(name)
    }
    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.fake.list()
    }
    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        self.fake.prepare(request, cancel).await
    }
    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        if request.turn.session_key.starts_with("tui:") {
            self.started.notify_one();
            tokio::select! {
                _ = self.release.notified() => {},
                _ = cancel.cancelled() => panic!("scheduled execution interrupted the original conversation"),
            }
        }
        self.fake.prompt(request, events, cancel).await
    }
}

struct SilentOutput;
#[async_trait]
impl RouterOutputSink for SilentOutput {
    fn send_channel_event(&mut self, _: RouterChannelEvent) {}
    async fn send_final_reply(&mut self, _: String) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn background_result_and_active_conversation_both_survive() {
    let (root, scheduler, api, token) = scheduler();
    let task = create(&scheduler, &api, &token).await;
    let store = Arc::new(InMemorySessionStore::default());
    let executor = Arc::new(ConcurrentExecutor::default());
    let router = Arc::new(
        AgentRouter::new("kimi", store.clone(), executor.clone())
            .with_workspace_root(Some(root.path().into()))
            .with_agent_api(Some(api.clone())),
    );
    let origin_router = router.clone();
    let active = tokio::spawn(async move {
        origin_router
            .handle(
                RouterInput {
                    session_key: "tui:owner".into(),
                    text: "original message".into(),
                    user_id: Some("U1".into()),
                },
                &mut SilentOutput,
            )
            .await
            .unwrap();
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        executor.started.notified(),
    )
    .await
    .unwrap();
    let (claimed, run) = scheduler
        .claim_ready(now_ms() + 60_001, 1)
        .await
        .unwrap()
        .remove(0);
    scheduler
        .execute(router, Arc::new(FailedDelivery), claimed, run, 2)
        .await
        .unwrap();
    assert!(!active.is_finished());
    executor.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), active)
        .await
        .unwrap()
        .unwrap();
    let origin = store
        .load_or_create(&task.owner_session_key, "kimi")
        .await
        .unwrap();
    assert_eq!(origin.transcript.len(), 3);
    assert!(origin.transcript[0].content.contains("Scheduled task"));
    assert_eq!(origin.transcript[1].content, "original message");
    assert_eq!(executor.fake.prompts.lock().await.len(), 2);
}
