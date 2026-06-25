use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin},
    sync::{Mutex, broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Duration, sleep},
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
        SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorChannelEvent, ExecutorDescriptor, ExecutorEventSink,
        ExecutorInterruptRequest, ExecutorPrepareRequest, ExecutorPromptOutcome,
        ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate, PreparedExecutor,
        TurnCancellation, summarize_json_rpc_error,
    },
    machine::{MachinePrepareRequest, MachineRegistry, MachineWorkspaceRecord, StdioCommand},
};

type SessionKey = (String, String);
type SharedCodexSession = Arc<Mutex<CodexAppServerSession>>;
type SessionMap = HashMap<SessionKey, SharedCodexSession>;
type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedJsonRpcNextId = Arc<AtomicU64>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
type SharedCodexEventStreams = Arc<StdMutex<CodexEventStreams>>;
type SharedActiveCodexTurns = Arc<Mutex<HashMap<SessionKey, ActiveCodexTurn>>>;
const MAX_READY_NOTIFICATIONS_BEFORE_REQUEST: usize = 1024;
const CODEX_INTERRUPT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct CodexEventStreams {
    next_generation: u64,
    active: Option<CodexActiveEventStream>,
    completed_turn_ids: HashSet<String>,
}

#[derive(Debug)]
struct CodexActiveEventStream {
    generation: u64,
    thread_id: String,
    turn_id: Option<String>,
    notifications: mpsc::UnboundedSender<Value>,
    server_requests: mpsc::UnboundedSender<Value>,
}

struct CodexTurnStreams {
    generation: u64,
    notifications: mpsc::UnboundedReceiver<Value>,
    server_requests: mpsc::UnboundedReceiver<Value>,
    _guard: CodexTurnStreamGuard,
}

struct CodexTurnStreamGuard {
    streams: SharedCodexEventStreams,
    generation: u64,
}

impl CodexEventStreams {
    fn open(
        &mut self,
        thread_id: String,
        notifications: mpsc::UnboundedSender<Value>,
        server_requests: mpsc::UnboundedSender<Value>,
    ) -> u64 {
        self.next_generation = self.next_generation.saturating_add(1);
        let generation = self.next_generation;
        self.active = Some(CodexActiveEventStream {
            generation,
            thread_id,
            turn_id: None,
            notifications,
            server_requests,
        });
        generation
    }

    fn set_turn_id(&mut self, generation: u64, turn_id: String) {
        let Some(active) = self.active.as_mut() else {
            return;
        };
        if active.generation == generation {
            active.turn_id = Some(turn_id);
        }
    }

    fn finish(&mut self, generation: u64, turn_id: Option<&str>) {
        if let Some(turn_id) = turn_id.filter(|turn_id| !turn_id.is_empty()) {
            self.completed_turn_ids.insert(turn_id.to_string());
        }
        self.close_active(generation);
    }

    fn close_active(&mut self, generation: u64) {
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.generation == generation)
        {
            self.active = None;
        }
    }

    fn notification_sender_for(&self, message: &Value) -> Option<mpsc::UnboundedSender<Value>> {
        self.active
            .as_ref()
            .filter(|active| active.accepts(message, &self.completed_turn_ids))
            .map(|active| active.notifications.clone())
    }

    fn server_request_sender_for(&self, message: &Value) -> Option<mpsc::UnboundedSender<Value>> {
        self.active
            .as_ref()
            .filter(|active| active.accepts(message, &self.completed_turn_ids))
            .map(|active| active.server_requests.clone())
    }
}

impl CodexActiveEventStream {
    fn accepts(&self, message: &Value, completed_turn_ids: &HashSet<String>) -> bool {
        if let Some(thread_id) = codex_message_thread_id(message)
            && thread_id != self.thread_id
        {
            return false;
        }
        let Some(message_turn_id) = codex_message_turn_id(message) else {
            return !codex_message_requires_turn_id(message)
                && self.turn_id.is_none()
                && completed_turn_ids.is_empty();
        };
        if completed_turn_ids.contains(message_turn_id) {
            return false;
        }
        match self.turn_id.as_deref() {
            Some(active_turn_id) => message_turn_id == active_turn_id,
            None => true,
        }
    }
}

impl Drop for CodexTurnStreamGuard {
    fn drop(&mut self) {
        let mut streams = self.streams.lock().unwrap();
        streams.close_active(self.generation);
    }
}

struct CodexServerRequest {
    message: Value,
    pending_file_change: Option<String>,
}

struct CodexTurnScope {
    cancelled: ApprovalCancellation,
}

impl CodexTurnScope {
    fn new() -> Self {
        Self {
            cancelled: ApprovalCancellation::new(),
        }
    }

    fn subscribe(&self) -> ApprovalCancellation {
        self.cancelled.clone()
    }

    fn cancel(&self) {
        self.cancelled.cancel();
    }
}

impl Drop for CodexTurnScope {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug, Clone)]
struct ActiveCodexTurn {
    thread_id: String,
    turn_id: Option<String>,
    stream_generation: u64,
    request_id: Option<u64>,
    generation: u64,
    client: CodexJsonRpcClientHandle,
    turn_cancelled: ApprovalCancellation,
    interrupt_state: CodexInterruptState,
}

#[derive(Debug, Clone)]
enum CodexInterruptState {
    NotRequested,
    PendingTurnId,
    Sent(CodexInterruptAck),
}

#[derive(Debug, Clone)]
struct CodexTurnRegistration {
    active_turns: SharedActiveCodexTurns,
    key: SessionKey,
    generation: u64,
}

#[derive(Debug, Clone)]
struct CodexInterruptTarget {
    thread_id: String,
    turn_id: String,
    generation: u64,
    request_id: Option<u64>,
    client: CodexJsonRpcClientHandle,
    ack: CodexInterruptAck,
}

#[derive(Debug)]
enum CodexInterruptAction {
    Send(CodexInterruptTarget),
    AwaitTurnId,
    AlreadySent(CodexInterruptAck),
}

#[derive(Debug, Clone)]
struct CodexInterruptAck {
    changed: watch::Sender<Option<Result<(), String>>>,
}

impl CodexInterruptAck {
    fn new() -> Self {
        let (changed, _) = watch::channel(None);
        Self { changed }
    }

    async fn complete(&self, result: anyhow::Result<()>) {
        if self.changed.borrow().is_none() {
            self.changed
                .send_replace(Some(result.map_err(|err| err.to_string())));
        }
    }

    async fn wait(&self) -> anyhow::Result<()> {
        let mut changed = self.changed.subscribe();
        loop {
            if let Some(result) = changed.borrow().clone() {
                return result.map_err(anyhow::Error::msg);
            }
            if changed.changed().await.is_err() {
                anyhow::bail!("codex app-server turn/interrupt ack channel closed");
            }
        }
    }
}

impl CodexInterruptTarget {
    async fn issue(&self) -> anyhow::Result<()> {
        let result = self.issue_inner().await;
        self.ack
            .complete(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|err| anyhow::anyhow!("{err}")),
            )
            .await;
        result
    }

    async fn issue_in_background(self) -> anyhow::Result<()> {
        let request = match self
            .client
            .request_started("turn/interrupt", self.request_params())
            .await
        {
            Ok(request) => request,
            Err(err) => {
                self.client
                    .close("codex app-server turn/interrupt failed")
                    .await;
                self.ack.complete(Err(anyhow::anyhow!("{err}"))).await;
                return Err(err);
            }
        };
        tokio::spawn(async move {
            let result = self.await_started_request(request).await;
            self.ack
                .complete(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|err| anyhow::anyhow!("{err}")),
                )
                .await;
        });
        Ok(())
    }

    fn request_params(&self) -> Value {
        json!({
            "threadId": self.thread_id,
            "turnId": self.turn_id,
        })
    }

    async fn issue_inner(&self) -> anyhow::Result<()> {
        let request = self
            .client
            .request_started("turn/interrupt", self.request_params())
            .await
            .inspect_err(|_| {
                tracing::debug!(
                    target: "agent_router::codex_app_server",
                    generation = self.generation,
                    request_id = ?self.request_id,
                    turn_id = %self.turn_id,
                    "Codex turn/interrupt request could not be written"
                );
            });
        match request {
            Ok(request) => self.await_started_request(request).await,
            Err(err) => {
                self.client
                    .close("codex app-server turn/interrupt failed")
                    .await;
                Err(err)
            }
        }
    }

    async fn await_started_request(&self, request: PendingJsonRpcRequest) -> anyhow::Result<()> {
        let id = request.id;
        let method = request.method;
        let timeout_fut = sleep(CODEX_INTERRUPT_TIMEOUT);
        tokio::pin!(timeout_fut);
        let result = tokio::select! {
            response = request.response => {
                match response {
                    Ok(Ok(response)) => json_rpc_result(&method, response).map(|_| ()),
                    Ok(Err(err)) => Err(err),
                    Err(_) => Err(anyhow::anyhow!("codex app-server response channel closed")),
                }
            }
            _ = &mut timeout_fut => {
                cancel_pending_request(&self.client.state, id).await;
                Err(anyhow::anyhow!(
                    "codex app-server `{method}` timed out after {}s",
                    CODEX_INTERRUPT_TIMEOUT.as_secs()
                ))
            }
        };
        if result.is_err() {
            self.client
                .close("codex app-server turn/interrupt failed")
                .await;
        }
        result
    }
}

#[derive(Debug, Clone, Copy)]
struct CodexRuntimeLimits {
    rpc_timeout: Duration,
}

impl Default for CodexRuntimeLimits {
    fn default() -> Self {
        Self {
            rpc_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
pub struct CodexAppServerManager {
    executors: BTreeMap<String, ExecutorConfig>,
    machines: MachineRegistry,
    approvals: SharedApprovalBroker,
    limits: CodexRuntimeLimits,
    sessions: Mutex<SessionMap>,
    active_turns: SharedActiveCodexTurns,
}

impl CodexAppServerManager {
    pub fn new(executors: BTreeMap<String, ExecutorConfig>) -> Self {
        Self::with_approvals(executors, Arc::new(ApprovalBroker::default()))
    }

    pub fn with_approvals(
        executors: BTreeMap<String, ExecutorConfig>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self::with_machines(executors, MachineRegistry::local_default(), approvals)
    }

    pub fn with_machines(
        executors: BTreeMap<String, ExecutorConfig>,
        machines: MachineRegistry,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self::with_limits(
            executors,
            machines,
            approvals,
            CodexRuntimeLimits::default(),
        )
    }

    fn with_limits(
        executors: BTreeMap<String, ExecutorConfig>,
        machines: MachineRegistry,
        approvals: SharedApprovalBroker,
        limits: CodexRuntimeLimits,
    ) -> Self {
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::AppServer)
            .collect();
        Self {
            executors,
            machines,
            approvals,
            limits,
            sessions: Mutex::new(HashMap::new()),
            active_turns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_or_create_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        router_workspace: Option<&Path>,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<(SharedCodexSession, bool)> {
        let key = (session_key.to_string(), executor.to_string());
        let prepared_command = self
            .machines
            .prepare_executor_command(MachinePrepareRequest {
                machine_id: &cfg.machine,
                session_key,
                router_workspace,
                executor_cwd: cfg.cwd.as_deref(),
                command: &cfg.command,
                args: &cfg.args,
                env: &cfg.env,
                cancel: Some(cancel),
            })
            .await?;
        let existing = self.sessions.lock().await.get(&key).cloned();
        if let Some(existing) = existing {
            let matches = existing.lock().await.matches(cfg, &prepared_command.stdio);
            if matches {
                return Ok((existing, false));
            }
        }
        let session = Arc::new(Mutex::new(
            CodexAppServerSession::start(
                cfg.clone(),
                prepared_command.stdio,
                prepared_command.workspace,
                session_key.to_string(),
                executor.to_string(),
                self.approvals.clone(),
                self.limits,
            )
            .await?,
        ));
        self.sessions.lock().await.insert(key, session.clone());
        Ok((session, true))
    }

    async fn existing_session(
        &self,
        session_key: &str,
        executor: &str,
    ) -> anyhow::Result<SharedCodexSession> {
        self.sessions
            .lock()
            .await
            .get(&(session_key.to_string(), executor.to_string()))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared Codex app-server session for `{session_key}`"
                )
            })
    }
}

#[async_trait]
impl ExecutorBackend for CodexAppServerManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: "app_server".to_string(),
            machine_id: cfg.machine.clone(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: "app_server".to_string(),
                machine_id: cfg.machine.clone(),
            })
            .collect()
    }

    async fn prepare(
        &self,
        request: ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        if cancel.is_cancelled().await {
            anyhow::bail!("Codex app-server prepare cancelled");
        }
        let cfg = self.executors.get(&request.turn.executor).ok_or_else(|| {
            anyhow::anyhow!("executor `{}` is not configured", request.turn.executor)
        })?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            "preparing Codex app-server executor session"
        );
        let (session, _) = self
            .get_or_create_session(
                &request.turn.session_key,
                &request.turn.executor,
                cfg,
                request.cwd.as_deref(),
                &cancel,
            )
            .await?;
        if cancel.is_cancelled().await {
            anyhow::bail!("Codex app-server prepare cancelled");
        }
        let mut session = session.lock().await;
        let (thread_id, started_new_session) = session
            .ensure_thread(request.previous_session_id.as_deref(), cancel)
            .await?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            thread_id = %thread_id,
            started_new_session,
            "prepared Codex app-server executor session"
        );
        Ok(PreparedExecutor {
            external_session_id: Some(thread_id),
            started_new_session,
            machine_id: Some(session.machine_id().to_string()),
            cwd: Some(session.cwd.clone()),
            machine_workspace: session.workspace.clone(),
        })
    }

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        let session = match self
            .existing_session(&request.turn.session_key, &request.turn.executor)
            .await
        {
            Ok(session) => session,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let mut session = session.lock().await;
        let session_key = request.turn.session_key.clone();
        let executor = request.turn.executor.clone();
        let active_key = (session_key.clone(), executor.clone());
        let generation = request.turn.generation;
        let prompt_len = request.prompt.len();
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            generation,
            prompt_len,
            "starting Codex app-server turn"
        );
        let active_turn = CodexTurnRegistration {
            active_turns: self.active_turns.clone(),
            key: active_key,
            generation,
        };
        let result = session
            .run_turn(
                &request.prompt,
                request.user_id,
                events,
                cancel,
                active_turn,
            )
            .await;
        match &result {
            ExecutorPromptOutcome::Completed(response) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                final_text_len = response.final_text.len(),
                "completed Codex app-server turn"
            ),
            ExecutorPromptOutcome::Cancelled => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                "cancelled Codex app-server turn"
            ),
            ExecutorPromptOutcome::Failed(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                generation,
                "failed Codex app-server turn"
            ),
        }
        result
    }

    async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
        let key = (
            request.turn.session_key.clone(),
            request.turn.executor.clone(),
        );
        let target = {
            let mut active_turns = self.active_turns.lock().await;
            let Some(active_turn) = active_turns.get_mut(&key) else {
                return Ok(());
            };
            if active_turn.generation != request.turn.generation {
                tracing::debug!(
                    target: "agent_router::codex_app_server",
                    executor = %request.turn.executor,
                    session_key = %request.turn.session_key,
                    interrupted_generation = request.turn.generation,
                    active_generation = active_turn.generation,
                    active_request_id = ?active_turn.request_id,
                    reason = ?request.reason,
                    "ignoring stale Codex interrupt for newer active turn"
                );
                return Ok(());
            }
            active_turn.turn_cancelled.cancel();
            match request_codex_interrupt_action(active_turn) {
                CodexInterruptAction::Send(target) => Some(target),
                CodexInterruptAction::AwaitTurnId => {
                    tracing::debug!(
                        target: "agent_router::codex_app_server",
                        executor = %request.turn.executor,
                        session_key = %request.turn.session_key,
                        generation = request.turn.generation,
                        active_request_id = ?active_turn.request_id,
                        reason = ?request.reason,
                        "recorded pending Codex interrupt before turn/start acknowledged"
                    );
                    None
                }
                CodexInterruptAction::AlreadySent(_) => None,
            }
        };
        if let Some(target) = target {
            tracing::debug!(
                target: "agent_router::codex_app_server",
                executor = %request.turn.executor,
                session_key = %request.turn.session_key,
                generation = target.generation,
                request_id = ?target.request_id,
                turn_id = %target.turn_id,
                reason = ?request.reason,
                "requesting Codex active turn interruption"
            );
            target.issue_in_background().await?;
        }
        Ok(())
    }
}

async fn set_active_codex_turn(registration: &CodexTurnRegistration, turn: ActiveCodexTurn) {
    registration
        .active_turns
        .lock()
        .await
        .insert(registration.key.clone(), turn);
}

async fn set_active_codex_turn_request_id(
    registration: &CodexTurnRegistration,
    stream_generation: u64,
    request_id: u64,
) {
    let mut active_turns = registration.active_turns.lock().await;
    let Some(active_turn) = active_turns.get_mut(&registration.key) else {
        return;
    };
    if active_turn.generation == registration.generation
        && active_turn.stream_generation == stream_generation
    {
        active_turn.request_id = Some(request_id);
    }
}

async fn set_active_codex_turn_id(
    registration: &CodexTurnRegistration,
    stream_generation: u64,
    turn_id: String,
) -> Option<CodexInterruptTarget> {
    let mut active_turns = registration.active_turns.lock().await;
    let active_turn = active_turns.get_mut(&registration.key)?;
    if active_turn.generation != registration.generation
        || active_turn.stream_generation != stream_generation
    {
        return None;
    }
    active_turn.turn_id = Some(turn_id.clone());
    if matches!(
        active_turn.interrupt_state,
        CodexInterruptState::PendingTurnId
    ) {
        let ack = CodexInterruptAck::new();
        active_turn.interrupt_state = CodexInterruptState::Sent(ack.clone());
        return Some(codex_interrupt_target(active_turn, turn_id, ack));
    }
    None
}

async fn request_active_codex_turn_interrupt(
    registration: &CodexTurnRegistration,
    stream_generation: u64,
) -> Option<CodexInterruptAction> {
    let mut active_turns = registration.active_turns.lock().await;
    let active_turn = active_turns.get_mut(&registration.key)?;
    if active_turn.generation != registration.generation
        || active_turn.stream_generation != stream_generation
    {
        return None;
    }
    active_turn.turn_cancelled.cancel();
    Some(request_codex_interrupt_action(active_turn))
}

fn request_codex_interrupt_action(active_turn: &mut ActiveCodexTurn) -> CodexInterruptAction {
    match &active_turn.interrupt_state {
        CodexInterruptState::NotRequested => {
            if let Some(turn_id) = active_turn.turn_id.clone() {
                let ack = CodexInterruptAck::new();
                active_turn.interrupt_state = CodexInterruptState::Sent(ack.clone());
                CodexInterruptAction::Send(codex_interrupt_target(active_turn, turn_id, ack))
            } else {
                active_turn.interrupt_state = CodexInterruptState::PendingTurnId;
                CodexInterruptAction::AwaitTurnId
            }
        }
        CodexInterruptState::PendingTurnId => CodexInterruptAction::AwaitTurnId,
        CodexInterruptState::Sent(ack) => CodexInterruptAction::AlreadySent(ack.clone()),
    }
}

fn codex_interrupt_target(
    active_turn: &ActiveCodexTurn,
    turn_id: String,
    ack: CodexInterruptAck,
) -> CodexInterruptTarget {
    CodexInterruptTarget {
        thread_id: active_turn.thread_id.clone(),
        turn_id,
        generation: active_turn.generation,
        request_id: active_turn.request_id,
        client: active_turn.client.clone(),
        ack,
    }
}

async fn active_codex_turn_interrupt_ack(
    registration: &CodexTurnRegistration,
    stream_generation: u64,
) -> Option<CodexInterruptAck> {
    let active_turns = registration.active_turns.lock().await;
    let active_turn = active_turns.get(&registration.key)?;
    if active_turn.generation != registration.generation
        || active_turn.stream_generation != stream_generation
    {
        return None;
    }
    match &active_turn.interrupt_state {
        CodexInterruptState::Sent(ack) => Some(ack.clone()),
        CodexInterruptState::NotRequested | CodexInterruptState::PendingTurnId => None,
    }
}

async fn wait_codex_interrupt_ack(ack: CodexInterruptAck, context: &'static str) {
    if let Err(err) = ack.wait().await {
        tracing::debug!(
            target: "agent_router::codex_app_server",
            error = %err,
            context,
            "Codex turn/interrupt did not complete cleanly before turn exit"
        );
    }
}

async fn clear_active_codex_turn(registration: &CodexTurnRegistration, stream_generation: u64) {
    let mut active_turns = registration.active_turns.lock().await;
    if active_turns
        .get(&registration.key)
        .is_some_and(|active_turn| {
            active_turn.generation == registration.generation
                && active_turn.stream_generation == stream_generation
        })
    {
        active_turns.remove(&registration.key);
    }
}

#[derive(Debug)]
struct CodexAppServerSession {
    cfg: ExecutorConfig,
    stdio: StdioCommand,
    cwd: String,
    workspace: Option<MachineWorkspaceRecord>,
    client: CodexJsonRpcClient,
    session_key: String,
    executor: String,
    approvals: SharedApprovalBroker,
    pending_file_changes: HashMap<String, String>,
    limits: CodexRuntimeLimits,
    thread_id: Option<String>,
    initialized: bool,
}

impl CodexAppServerSession {
    async fn start(
        cfg: ExecutorConfig,
        stdio: StdioCommand,
        workspace: Option<MachineWorkspaceRecord>,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
        limits: CodexRuntimeLimits,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            cwd = %stdio.executor_cwd,
            "starting Codex app-server process"
        );
        let client =
            CodexJsonRpcClient::spawn(&stdio, session_key.clone(), executor.clone()).await?;
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            "started Codex app-server process"
        );
        Ok(Self {
            cfg,
            cwd: stdio.executor_cwd.clone(),
            stdio,
            workspace,
            client,
            session_key,
            executor,
            approvals,
            pending_file_changes: HashMap::new(),
            limits,
            thread_id: None,
            initialized: false,
        })
    }

    fn machine_id(&self) -> &str {
        &self.cfg.machine
    }

    fn matches(&self, cfg: &ExecutorConfig, stdio: &StdioCommand) -> bool {
        self.cfg.command == cfg.command
            && self.cfg.args == cfg.args
            && self.cfg.env == cfg.env
            && self.cfg.machine == cfg.machine
            && &self.stdio == stdio
            && self.client.is_alive()
    }

    async fn initialize(&mut self, cancel: TurnCancellation) -> anyhow::Result<()> {
        if self.initialized {
            return Ok(());
        }
        let initialized = self
            .client
            .lifecycle_request_until_cancelled(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "agent-router",
                        "title": "Agent Router",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {},
                }),
                self.limits.rpc_timeout,
                cancel.clone(),
            )
            .await;
        let initialized = match initialized {
            Ok(initialized) => initialized,
            Err(err) => {
                self.client
                    .close("codex app-server initialize failed")
                    .await;
                return Err(err);
            }
        };
        self.client.notify("initialized", json!({})).await?;
        self.initialized = true;
        if initialized.cancelled || cancel.is_cancelled().await {
            anyhow::bail!("codex app-server initialize cancelled");
        }
        Ok(())
    }

    async fn ensure_thread(
        &mut self,
        previous_session_id: Option<&str>,
        cancel: TurnCancellation,
    ) -> anyhow::Result<(String, bool)> {
        self.initialize(cancel.clone()).await?;
        if let Some(thread_id) = &self.thread_id {
            return Ok((
                thread_id.clone(),
                previous_session_id != Some(thread_id.as_str()),
            ));
        }
        let result = self
            .client
            .lifecycle_request_until_cancelled(
                "thread/start",
                json!({ "cwd": self.cwd }),
                self.limits.rpc_timeout,
                cancel.clone(),
            )
            .await;
        let result = match result {
            Ok(result) => result,
            Err(err) => {
                self.client
                    .close("codex app-server thread/start failed")
                    .await;
                return Err(err);
            }
        };
        let thread_id = thread_id_from_result(&result.result)
            .ok_or_else(|| anyhow::anyhow!("codex thread/start did not return thread id"))?;
        self.thread_id = Some(thread_id.clone());
        if result.cancelled || cancel.is_cancelled().await {
            anyhow::bail!("codex app-server thread/start cancelled");
        }
        let started_new_session = previous_session_id != Some(thread_id.as_str());
        Ok((thread_id, started_new_session))
    }

    async fn run_turn(
        &mut self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
        active_turn: CodexTurnRegistration,
    ) -> ExecutorPromptOutcome {
        let thread_id = match self.thread_id.clone() {
            Some(thread_id) => thread_id,
            None => {
                return ExecutorPromptOutcome::Failed(anyhow::anyhow!(
                    "codex app-server thread has not been created"
                ));
            }
        };
        if cancel.is_cancelled().await {
            return ExecutorPromptOutcome::Cancelled;
        }
        let CodexTurnStreams {
            generation: turn_generation,
            mut notifications,
            mut server_requests,
            _guard: _turn_stream_guard,
        } = self.client.open_turn_streams(thread_id.clone());
        let turn_scope = CodexTurnScope::new();
        set_active_codex_turn(
            &active_turn,
            ActiveCodexTurn {
                thread_id: thread_id.clone(),
                turn_id: None,
                stream_generation: turn_generation,
                request_id: None,
                generation: active_turn.generation,
                client: self.client.handle(),
                turn_cancelled: turn_scope.subscribe(),
                interrupt_state: CodexInterruptState::NotRequested,
            },
        )
        .await;
        let mut closed = self.client.subscribe_closed();
        let (server_response_tx, mut server_responses) = mpsc::channel(64);
        let (server_request_tx, server_request_rx) = mpsc::unbounded_channel();
        let server_request_handler = self.spawn_server_request_worker(
            server_request_rx,
            user_id.clone(),
            server_response_tx.clone(),
            turn_scope.subscribe(),
        );
        let mut turn_start = match self
            .client
            .request_started(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": prompt}],
                }),
            )
            .await
        {
            Ok(turn_start) => {
                set_active_codex_turn_request_id(&active_turn, turn_generation, turn_start.id)
                    .await;
                turn_start
            }
            Err(err) => {
                clear_active_codex_turn(&active_turn, turn_generation).await;
                drop(server_request_tx);
                drop(server_responses);
                turn_scope.cancel();
                let _ = server_request_handler.await;
                return ExecutorPromptOutcome::Failed(err);
            }
        };

        let mut active_turn_id = None;
        let mut cancelled = false;
        let mut cancellation_requested = false;
        let mut backend_interrupted = false;
        let result: anyhow::Result<ExecutorResponse> = async {
            let mut final_text = String::new();
            let mut turn_start_acknowledged = false;
            let mut turn_completed = false;
            let turn_start_timeout = sleep(self.limits.rpc_timeout);
            tokio::pin!(turn_start_timeout);
            loop {
                tokio::select! {
                response = &mut turn_start.response, if !turn_start_acknowledged => {
                    turn_start_acknowledged = true;
                    let result = json_rpc_result(&turn_start.method, response??)?;
                    if let Some(turn_id) = turn_id_from_turn_start_result(&result) {
                        self.client
                            .set_turn_stream_turn_id(turn_generation, turn_id.clone());
                        let pending_interrupt =
                            set_active_codex_turn_id(&active_turn, turn_generation, turn_id.clone())
                                .await;
                        active_turn_id = Some(turn_id);
                        if let Some(target) = pending_interrupt {
                            if let Err(err) = target.issue().await {
                                tracing::debug!(
                                    target: "agent_router::codex_app_server",
                                    error = %err,
                                    generation = target.generation,
                                    request_id = ?target.request_id,
                                    turn_id = %target.turn_id,
                                    "Codex turn/interrupt request failed after pending interrupt"
                                );
                            }
                            cancelled = true;
                            turn_scope.cancel();
                            break;
                        }
                    }
                    if turn_completed {
                        break;
                    }
                }
                request = server_requests.recv() => {
                    match request {
                        Some(request) => {
                            self.drain_ready_notifications(
                                &mut notifications,
                                &mut final_text,
                                events,
                                &mut turn_completed,
                                &mut backend_interrupted,
                            ).await?;
                            if turn_completed {
                                if turn_start_acknowledged {
                                    break;
                                }
                                continue;
                            }
                            server_request_tx
                                .send(CodexServerRequest {
                                    pending_file_change: self.pending_file_change_summary(
                                        request.get("params").unwrap_or(&Value::Null),
                                    ),
                                    message: request,
                                })
                                .map_err(|_| {
                                    anyhow::anyhow!("codex app-server request worker closed")
                                })?;
                        }
                        None => {
                            anyhow::bail!("codex app-server request stream closed")
                        }
                    }
                }
                response = server_responses.recv() => {
                    if let Some(response) = response {
                        self.drain_ready_notifications(
                            &mut notifications,
                            &mut final_text,
                            events,
                            &mut turn_completed,
                            &mut backend_interrupted,
                        ).await?;
                        if turn_completed {
                            if turn_start_acknowledged {
                                break;
                            }
                            continue;
                        }
                        if !turn_start_acknowledged && turn_start_timeout.as_ref().is_elapsed() {
                            self.client.cancel_pending(turn_start.id).await;
                            self.client.close("codex app-server turn/start timed out").await;
                            anyhow::bail!(
                                "codex app-server `turn/start` timed out after {}s",
                                self.limits.rpc_timeout.as_secs()
                            );
                        }
                        write_json(&self.client.stdin, response).await?;
                    }
                }
                notification = notifications.recv() => {
                    match notification {
                        Some(notification) => {
                            self.handle_notification(
                                notification,
                                &mut final_text,
                                events,
                                &mut turn_completed,
                                &mut backend_interrupted,
                            ).await?;
                            if turn_completed && turn_start_acknowledged {
                                break;
                            }
                        }
                        None => {
                            anyhow::bail!("codex app-server notification stream closed")
                        }
                    }
                }
                reason = closed.recv() => {
                    let reason = reason.unwrap_or_else(|_| "codex app-server closed".to_string());
                    anyhow::bail!("{reason}");
                }
                _ = &mut turn_start_timeout, if !turn_start_acknowledged => {
                    self.client.cancel_pending(turn_start.id).await;
                    self.client.close("codex app-server turn/start timed out").await;
                    anyhow::bail!(
                        "codex app-server `turn/start` timed out after {}s",
                        self.limits.rpc_timeout.as_secs()
                    );
                }
                _ = cancel.cancelled(), if !cancellation_requested => {
                    cancellation_requested = true;
                    cancelled = true;
                    turn_scope.cancel();
                    match request_active_codex_turn_interrupt(&active_turn, turn_generation).await {
                        Some(CodexInterruptAction::Send(target)) => {
                            if let Err(err) = target.issue().await {
                                tracing::debug!(
                                    target: "agent_router::codex_app_server",
                                    error = %err,
                                    generation = target.generation,
                                    request_id = ?target.request_id,
                                    turn_id = %target.turn_id,
                                    "Codex turn/interrupt request failed after local cancellation"
                                );
                            }
                            break;
                        }
                        Some(CodexInterruptAction::AlreadySent(ack)) => {
                            wait_codex_interrupt_ack(ack, "local cancellation").await;
                            break;
                        }
                        Some(CodexInterruptAction::AwaitTurnId) | None => {}
                    }
                }
                }
            }

            if !cancelled && cancel.is_cancelled().await {
                cancelled = true;
                if let Some(CodexInterruptAction::Send(target)) =
                    request_active_codex_turn_interrupt(&active_turn, turn_generation).await
                    && let Err(err) = target.issue().await
                {
                    tracing::debug!(
                        target: "agent_router::codex_app_server",
                        error = %err,
                        generation = target.generation,
                        request_id = ?target.request_id,
                        turn_id = %target.turn_id,
                        "Codex turn/interrupt request failed after observed cancellation"
                    );
                }
                if let Some(CodexInterruptAction::AlreadySent(ack)) =
                    request_active_codex_turn_interrupt(&active_turn, turn_generation).await
                {
                    wait_codex_interrupt_ack(ack, "observed cancellation").await;
                }
            }
            self.client
                .finish_turn_streams(turn_generation, active_turn_id.as_deref());
            Ok(ExecutorResponse { final_text })
        }
        .await;

        if result.is_err() {
            self.client
                .finish_turn_streams(turn_generation, active_turn_id.as_deref());
        }
        drop(server_request_tx);
        drop(server_responses);
        turn_scope.cancel();
        let _ = server_request_handler.await;
        if !cancelled && cancel.is_cancelled().await {
            cancelled = true;
            if let Some(CodexInterruptAction::Send(target)) =
                request_active_codex_turn_interrupt(&active_turn, turn_generation).await
                && let Err(err) = target.issue().await
            {
                tracing::debug!(
                    target: "agent_router::codex_app_server",
                    error = %err,
                    generation = target.generation,
                    request_id = ?target.request_id,
                    turn_id = %target.turn_id,
                    "Codex turn/interrupt request failed while finishing cancellation"
                );
            }
            if let Some(CodexInterruptAction::AlreadySent(ack)) =
                request_active_codex_turn_interrupt(&active_turn, turn_generation).await
            {
                wait_codex_interrupt_ack(ack, "finishing cancellation").await;
            }
        }
        if let Some(ack) = active_codex_turn_interrupt_ack(&active_turn, turn_generation).await {
            wait_codex_interrupt_ack(ack, "turn exit").await;
        }
        clear_active_codex_turn(&active_turn, turn_generation).await;
        if cancelled || backend_interrupted {
            ExecutorPromptOutcome::Cancelled
        } else {
            match result {
                Ok(response) => ExecutorPromptOutcome::Completed(response),
                Err(err) => ExecutorPromptOutcome::Failed(err),
            }
        }
    }

    async fn drain_ready_notifications(
        &mut self,
        notifications: &mut mpsc::UnboundedReceiver<Value>,
        final_text: &mut String,
        events: &mut dyn ExecutorEventSink,
        turn_completed: &mut bool,
        backend_interrupted: &mut bool,
    ) -> anyhow::Result<()> {
        for _ in 0..MAX_READY_NOTIFICATIONS_BEFORE_REQUEST {
            match notifications.try_recv() {
                Ok(notification) => {
                    self.handle_notification(
                        notification,
                        final_text,
                        events,
                        turn_completed,
                        backend_interrupted,
                    )
                    .await?;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    anyhow::bail!("codex app-server notification stream closed")
                }
            }
        }
        Ok(())
    }

    async fn handle_notification(
        &mut self,
        notification: Value,
        final_text: &mut String,
        events: &mut dyn ExecutorEventSink,
        turn_completed: &mut bool,
        backend_interrupted: &mut bool,
    ) -> anyhow::Result<()> {
        self.track_pending_file_change(&notification);
        let collected = collect_codex_notification(notification, final_text)?;
        for update in collected.updates {
            events.send(update).await?;
        }
        match collected.outcome {
            CodexNotificationOutcome::Pending => {}
            CodexNotificationOutcome::TurnCompleted { interrupted } => {
                *turn_completed = true;
                *backend_interrupted |= interrupted;
            }
        }
        Ok(())
    }

    fn spawn_server_request_worker(
        &self,
        mut requests: mpsc::UnboundedReceiver<CodexServerRequest>,
        user_id: Option<String>,
        responses: mpsc::Sender<Value>,
        turn_cancelled: ApprovalCancellation,
    ) -> JoinHandle<()> {
        let approvals = self.approvals.clone();
        let session_key = self.session_key.clone();
        let executor = self.executor.clone();

        tokio::spawn(async move {
            loop {
                let Some(request) = requests.recv().await else {
                    break;
                };
                match Self::handle_server_request(
                    approvals.clone(),
                    session_key.clone(),
                    executor.clone(),
                    user_id.clone(),
                    request.message,
                    request.pending_file_change,
                    turn_cancelled.clone(),
                )
                .await
                {
                    Ok(Some(response)) => {
                        if responses.send(response).await.is_err() {
                            tracing::debug!("dropping Codex app-server response for closed turn");
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to handle Codex app-server request");
                    }
                }
            }
        })
    }

    async fn handle_server_request(
        approvals: SharedApprovalBroker,
        session_key: String,
        executor: String,
        user_id: Option<String>,
        message: Value,
        pending_file_change: Option<String>,
        turn_cancelled: ApprovalCancellation,
    ) -> anyhow::Result<Option<Value>> {
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let params = message.get("params").unwrap_or(&Value::Null);
        match method {
            "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
                let request = codex_approval_request(
                    &session_key,
                    &executor,
                    user_id,
                    method,
                    params,
                    pending_file_change,
                );
                let Some(selection) = approvals
                    .request_until_cancelled(request, turn_cancelled)
                    .await
                else {
                    return Ok(Some(codex_result_response(
                        id,
                        json!({ "decision": "decline" }),
                    )));
                };
                let decision = match selection {
                    ApprovalSelection::Selected(option_id) if option_id == "accept" => "accept",
                    _ => "decline",
                };
                Ok(Some(codex_result_response(
                    id,
                    json!({ "decision": decision }),
                )))
            }
            "item/permissions/requestApproval" => Ok(Some(codex_result_response(
                id,
                json!({ "decision": "decline" }),
            ))),
            "mcpServer/elicitation/request" => Ok(Some(codex_result_response(
                id,
                json!({ "action": "decline" }),
            ))),
            _ => Ok(Some(codex_error_response(
                id,
                -32601,
                format!("agent-router does not support codex client method `{method}`"),
            ))),
        }
    }

    fn track_pending_file_change(&mut self, notification: &Value) {
        let method = notification
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("");
        if method != "item/started" && method != "item/completed" {
            return;
        }
        let Some(item) = notification
            .get("params")
            .and_then(|params| params.get("item"))
        else {
            return;
        };
        if item.get("type").and_then(Value::as_str) != Some("fileChange") {
            return;
        }
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
        if item_id.is_empty() {
            return;
        }
        if method == "item/completed" {
            self.pending_file_changes.remove(item_id);
            return;
        }
        let summary = summarize_file_change(item);
        self.pending_file_changes
            .insert(item_id.to_string(), summary);
    }

    fn pending_file_change_summary(&self, params: &Value) -> Option<String> {
        let item_id = params.get("itemId").and_then(Value::as_str)?;
        self.pending_file_changes.get(item_id).cloned()
    }
}

fn codex_approval_request(
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
    method: &str,
    params: &Value,
    pending_file_change: Option<String>,
) -> ApprovalRequest {
    let (title, body) = match method {
        "item/commandExecution/requestApproval" => {
            let command = params
                .get("command")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("");
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("");
            let mut lines = Vec::new();
            if !cwd.is_empty() {
                lines.push(format!("cwd: {cwd}"));
            }
            if !reason.is_empty() {
                lines.push(format!("reason: {reason}"));
            }
            lines.push(format!("$ {command}"));
            ("Codex command approval".to_string(), lines.join("\n"))
        }
        "item/fileChange/requestApproval" => {
            let reason = params.get("reason").and_then(Value::as_str).unwrap_or("");
            let grant_root = params
                .get("grantRoot")
                .and_then(Value::as_str)
                .unwrap_or("");
            let item_id = params.get("itemId").and_then(Value::as_str).unwrap_or("");
            let mut lines = Vec::new();
            if !reason.is_empty() {
                lines.push(format!("reason: {reason}"));
            }
            if !grant_root.is_empty() {
                lines.push(format!("grant root: {grant_root}"));
            }
            if !item_id.is_empty() {
                lines.push(format!("item: {item_id}"));
            }
            if let Some(summary) = pending_file_change.filter(|summary| !summary.is_empty()) {
                lines.push(format!("changes: {summary}"));
            }
            ("Codex file change approval".to_string(), lines.join("\n"))
        }
        _ => ("Codex approval".to_string(), String::new()),
    };
    ApprovalRequest {
        session_key: session_key.to_string(),
        executor: executor.to_string(),
        requester_user_id,
        title,
        body: truncate_text(body, 2_000),
        options: vec![
            ApprovalOption {
                id: "accept".to_string(),
                kind: "allow_once".to_string(),
                name: "Approve".to_string(),
                auto_approvable: true,
            },
            ApprovalOption {
                id: "decline".to_string(),
                kind: "reject_once".to_string(),
                name: "Deny".to_string(),
                auto_approvable: false,
            },
        ],
    }
}

#[derive(Debug)]
struct CodexJsonRpcClient {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: SharedJsonRpcNextId,
    event_streams: SharedCodexEventStreams,
    closed: broadcast::Sender<String>,
    child: Arc<Mutex<Child>>,
    session_key: String,
    executor: String,
}

#[derive(Debug, Clone)]
struct CodexJsonRpcClientHandle {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: SharedJsonRpcNextId,
    closed: broadcast::Sender<String>,
    child: Arc<Mutex<Child>>,
    session_key: String,
    executor: String,
}

#[derive(Debug)]
struct PendingJsonRpcRequest {
    id: u64,
    method: String,
    response: oneshot::Receiver<anyhow::Result<Value>>,
}

#[derive(Debug)]
struct CodexLifecycleResponse {
    result: Value,
    cancelled: bool,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    closed_reason: Option<String>,
    pending: HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>,
}

impl CodexJsonRpcClientHandle {
    async fn request_started(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        start_json_rpc_request(&self.stdin, &self.state, &self.next_id, method, params).await
    }

    async fn close(&self, reason: &str) {
        close_codex_app_server(
            &self.state,
            &self.closed,
            &self.child,
            &self.executor,
            &self.session_key,
            reason,
        )
        .await;
    }
}

impl CodexJsonRpcClient {
    async fn spawn(
        stdio: &StdioCommand,
        session_key: String,
        executor: String,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            target: "agent_router::codex_app_server",
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            arg_count = stdio.args.len(),
            cwd = %stdio.executor_cwd,
            "spawning Codex app-server process"
        );
        let mut child = stdio.spawn().map_err(|err| {
            anyhow::anyhow!(
                "could not start codex app-server command `{}`: {err}",
                stdio.program
            )
        })?;
        let pid = child.id();
        tracing::info!(
            target: "agent_router::codex_app_server",
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            pid = ?pid,
            "spawned Codex app-server process"
        );
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app-server process did not expose stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("codex app-server process did not expose stdout"))?;
        let stderr = child.stderr.take();

        let stdin = Arc::new(Mutex::new(stdin));
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let next_id = Arc::new(AtomicU64::new(1));
        let event_streams = Arc::new(StdMutex::new(CodexEventStreams::default()));
        let (closed, _) = broadcast::channel(8);
        let child = Arc::new(Mutex::new(child));

        tokio::spawn(read_codex_stdout(
            BufReader::new(stdout),
            state.clone(),
            event_streams.clone(),
            closed.clone(),
            session_key.clone(),
            executor.clone(),
            stdio.strict_json_stdout,
        ));
        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(
                        target: "agent_router::codex_app_server",
                        bytes = line.len(),
                        "codex app-server emitted stderr"
                    );
                }
            });
        }

        Ok(Self {
            stdin,
            state,
            next_id,
            event_streams,
            closed,
            child,
            session_key,
            executor,
        })
    }

    fn open_turn_streams(&self, thread_id: String) -> CodexTurnStreams {
        let (notifications_tx, notifications) = mpsc::unbounded_channel();
        let (server_requests_tx, server_requests) = mpsc::unbounded_channel();
        let generation = {
            let mut streams = self.event_streams.lock().unwrap();
            streams.open(thread_id, notifications_tx, server_requests_tx)
        };
        CodexTurnStreams {
            generation,
            notifications,
            server_requests,
            _guard: CodexTurnStreamGuard {
                streams: self.event_streams.clone(),
                generation,
            },
        }
    }

    fn set_turn_stream_turn_id(&self, generation: u64, turn_id: String) {
        self.event_streams
            .lock()
            .unwrap()
            .set_turn_id(generation, turn_id);
    }

    fn finish_turn_streams(&self, generation: u64, turn_id: Option<&str>) {
        self.event_streams
            .lock()
            .unwrap()
            .finish(generation, turn_id);
    }

    fn handle(&self) -> CodexJsonRpcClientHandle {
        CodexJsonRpcClientHandle {
            stdin: self.stdin.clone(),
            state: self.state.clone(),
            next_id: self.next_id.clone(),
            closed: self.closed.clone(),
            child: self.child.clone(),
            session_key: self.session_key.clone(),
            executor: self.executor.clone(),
        }
    }

    fn subscribe_closed(&self) -> broadcast::Receiver<String> {
        self.closed.subscribe()
    }

    fn is_alive(&self) -> bool {
        if self
            .state
            .try_lock()
            .map(|state| state.closed)
            .unwrap_or(true)
        {
            return false;
        }
        self.child
            .try_lock()
            .map(|mut child| {
                child
                    .try_wait()
                    .map(|status| status.is_none())
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    }

    async fn lifecycle_request_until_cancelled(
        &self,
        method: &str,
        params: Value,
        timeout_duration: Duration,
        cancel: TurnCancellation,
    ) -> anyhow::Result<CodexLifecycleResponse> {
        let mut request = self.request_started(method, params).await?;
        let id = request.id;
        let method = request.method;
        let timeout_fut = sleep(timeout_duration);
        tokio::pin!(timeout_fut);
        let mut cancelled = false;
        loop {
            tokio::select! {
                response = &mut request.response => {
                    let response = match response {
                        Ok(Ok(response)) => response,
                        Ok(Err(err)) => return Err(err),
                        Err(_) => anyhow::bail!("codex app-server response channel closed"),
                    };
                    return Ok(CodexLifecycleResponse {
                        result: json_rpc_result(&method, response)?,
                        cancelled,
                    });
                }
                _ = &mut timeout_fut => {
                    self.cancel_pending(id).await;
                    anyhow::bail!(
                        "codex app-server `{method}` timed out after {}s",
                        timeout_duration.as_secs()
                    );
                }
                _ = cancel.cancelled(), if !cancelled => {
                    cancelled = true;
                }
            }
        }
    }

    async fn request_started(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        start_json_rpc_request(&self.stdin, &self.state, &self.next_id, method, params).await
    }

    async fn cancel_pending(&self, id: u64) {
        cancel_pending_request(&self.state, id).await;
    }

    async fn close(&self, reason: &str) {
        close_codex_app_server(
            &self.state,
            &self.closed,
            &self.child,
            &self.executor,
            &self.session_key,
            reason,
        )
        .await;
    }

    async fn notify(&self, method: &str, params: Value) -> anyhow::Result<()> {
        write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            }),
        )
        .await
    }
}

async fn close_codex_app_server(
    state: &SharedJsonRpcState,
    closed: &broadcast::Sender<String>,
    child: &Arc<Mutex<Child>>,
    executor: &str,
    session_key: &str,
    reason: &str,
) {
    fail_all_pending(state, reason).await;
    let _ = closed.send(reason.to_string());
    let mut child = child.lock().await;
    let pid = child.id();
    tracing::warn!(
        target: "agent_router::codex_app_server",
        executor = %executor,
        session_key = %session_key,
        pid = ?pid,
        reason,
        "closing Codex app-server process"
    );
    if let Err(err) = child.start_kill() {
        tracing::warn!(
            target: "agent_router::codex_app_server",
            executor = %executor,
            session_key = %session_key,
            pid = ?pid,
            reason,
            error = %err,
            "failed to signal Codex app-server process"
        );
    }
}

async fn start_json_rpc_request(
    stdin: &SharedStdin,
    state: &SharedJsonRpcState,
    next_id: &SharedJsonRpcNextId,
    method: &str,
    params: Value,
) -> anyhow::Result<PendingJsonRpcRequest> {
    let id = next_id.fetch_add(1, Ordering::Relaxed);
    let (tx, rx) = oneshot::channel();
    {
        let mut state = state.lock().await;
        ensure_stdout_open(&state)?;
        state.pending.insert(id, tx);
    }
    if let Err(err) = write_json(
        stdin,
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }),
    )
    .await
    {
        cancel_pending_request(state, id).await;
        return Err(err);
    }
    Ok(PendingJsonRpcRequest {
        id,
        method: method.to_string(),
        response: rx,
    })
}

async fn cancel_pending_request(state: &SharedJsonRpcState, id: u64) {
    state.lock().await.pending.remove(&id);
}

fn json_rpc_result(method: &str, response: Value) -> anyhow::Result<Value> {
    if let Some(error) = response.get("error") {
        anyhow::bail!(
            "codex app-server `{method}` failed: {}",
            summarize_json_rpc_error(error)
        );
    }
    Ok(response.get("result").cloned().unwrap_or(Value::Null))
}

fn codex_result_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn codex_error_response(id: Value, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        },
    })
}

async fn read_codex_stdout<R>(
    reader: BufReader<R>,
    state: SharedJsonRpcState,
    event_streams: SharedCodexEventStreams,
    closed: broadcast::Sender<String>,
    session_key: String,
    executor: String,
    strict_json_stdout: bool,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            if strict_json_stdout {
                let reason = "codex app-server emitted non-JSON stdout before protocol handshake"
                    .to_string();
                tracing::warn!(
                    target: "agent_router::codex_app_server",
                    executor = %executor,
                    session_key = %session_key,
                    bytes = line.len(),
                    "closing Codex app-server client after non-JSON stdout"
                );
                fail_all_pending(&state, &reason).await;
                let _ = closed.send(reason);
                return;
            }
            tracing::debug!(
                target: "agent_router::codex_app_server",
                bytes = line.len(),
                "ignoring non-json codex stdout"
            );
            continue;
        };
        if strict_json_stdout && !is_json_rpc_like(&message) {
            let reason =
                "codex app-server emitted non-protocol JSON stdout before protocol handshake"
                    .to_string();
            tracing::warn!(
                target: "agent_router::codex_app_server",
                executor = %executor,
                session_key = %session_key,
                bytes = line.len(),
                "closing Codex app-server client after non-protocol JSON stdout"
            );
            fail_all_pending(&state, &reason).await;
            let _ = closed.send(reason);
            return;
        }
        dispatch_codex_message(message, &state, &event_streams).await;
    }
    tracing::warn!(
        target: "agent_router::codex_app_server",
        executor = %executor,
        session_key = %session_key,
        "Codex app-server stdout closed"
    );
    fail_all_pending(&state, "codex app-server closed stdout").await;
    let _ = closed.send("codex app-server closed stdout".to_string());
}

async fn dispatch_codex_message(
    message: Value,
    state: &SharedJsonRpcState,
    event_streams: &SharedCodexEventStreams,
) {
    let has_method = message.get("method").is_some();
    let has_id = message.get("id").is_some();
    if has_id && !has_method {
        let sender = match message.get("id").and_then(Value::as_u64) {
            Some(id) => state.lock().await.pending.remove(&id),
            None => None,
        };
        if let Some(tx) = sender {
            let _ = tx.send(Ok(message));
        }
        return;
    }
    if has_id && has_method {
        let sender = {
            event_streams
                .lock()
                .unwrap()
                .server_request_sender_for(&message)
        };
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
        return;
    }
    if has_method {
        let sender = {
            event_streams
                .lock()
                .unwrap()
                .notification_sender_for(&message)
        };
        if let Some(sender) = sender {
            let _ = sender.send(message);
        }
    }
}

async fn fail_all_pending(state: &SharedJsonRpcState, message: &str) {
    let drained = {
        let mut guard = state.lock().await;
        guard.closed = true;
        guard.closed_reason = Some(message.to_string());
        guard.pending.drain().collect::<Vec<_>>()
    };
    for (_, tx) in drained {
        let _ = tx.send(Err(anyhow::anyhow!("{message}")));
    }
}

fn ensure_stdout_open(state: &JsonRpcState) -> anyhow::Result<()> {
    if state.closed {
        anyhow::bail!(
            "{}",
            state
                .closed_reason
                .as_deref()
                .unwrap_or("codex app-server stdout is closed")
        );
    }
    Ok(())
}

fn is_json_rpc_like(message: &Value) -> bool {
    let Some(map) = message.as_object() else {
        return false;
    };
    if map.contains_key("id") && (map.contains_key("result") || map.contains_key("error")) {
        return true;
    }
    if map.contains_key("id") {
        return map.get("method").and_then(Value::as_str).is_some();
    }
    map.get("method")
        .and_then(Value::as_str)
        .is_some_and(is_codex_notification_method)
}

fn is_codex_notification_method(method: &str) -> bool {
    matches!(method, "item/completed" | "turn/completed")
}

async fn write_json(stdin: &SharedStdin, value: Value) -> anyhow::Result<()> {
    let mut guard = stdin.lock().await;
    let line = serde_json::to_string(&value)?;
    guard.write_all(line.as_bytes()).await?;
    guard.write_all(b"\n").await?;
    guard.flush().await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexNotificationOutcome {
    Pending,
    TurnCompleted { interrupted: bool },
}

#[derive(Debug)]
struct CollectedCodexNotification {
    outcome: CodexNotificationOutcome,
    updates: Vec<ExecutorUpdate>,
}

fn collect_codex_notification(
    notification: Value,
    final_text: &mut String,
) -> anyhow::Result<CollectedCodexNotification> {
    let method = notification
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("");
    if method == "turn/completed" {
        let interrupted = validate_turn_completed(&notification)?;
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::TurnCompleted { interrupted },
            updates: Vec::new(),
        });
    }
    if method != "item/completed" {
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::Pending,
            updates: Vec::new(),
        });
    }
    let Some(item) = notification
        .get("params")
        .and_then(|params| params.get("item"))
    else {
        return Ok(CollectedCodexNotification {
            outcome: CodexNotificationOutcome::Pending,
            updates: Vec::new(),
        });
    };
    let item_type = item.get("type").and_then(Value::as_str).unwrap_or("");
    let mut updates = Vec::new();
    match item_type {
        "agentMessage" => {
            let text = item
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let phase = item.get("phase").and_then(Value::as_str).unwrap_or("");
            let mut update = ExecutorUpdate::new(
                "agent_message_chunk",
                String::new(),
                text.clone(),
                String::new(),
            );
            if phase == "commentary" {
                update = update.with_channel_event(ExecutorChannelEvent::agent_progress(text));
            } else {
                *final_text = text;
            }
            updates.push(update);
        }
        "reasoning" => {
            let summary = extract_text(item.get("summary"));
            let text = summary
                .clone()
                .or_else(|| extract_text(item.get("content")))
                .unwrap_or_default();
            let mut update =
                ExecutorUpdate::new("agent_thought_chunk", "Reasoning", text, String::new());
            if let Some(summary) = summary.filter(|summary| !summary.trim().is_empty()) {
                let summary = truncate_text(summary, 1_000);
                update = update
                    .with_channel_event(ExecutorChannelEvent::reasoning_summary(summary.clone()));
                update = update.with_transcript_summary(format!("Reasoning: {summary}"));
            }
            updates.push(update);
        }
        "commandExecution" => {
            let summary = command_execution_summary(item);
            let channel_summary = command_execution_channel_summary(item);
            updates.push(
                ExecutorUpdate::new(
                    "tool_call",
                    "Bash",
                    summary.clone(),
                    item.get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                )
                .with_transcript_summary(format!("Bash: {channel_summary}"))
                .with_channel_event(ExecutorChannelEvent::tool_call("Bash", channel_summary)),
            );
        }
        "fileChange" => {
            updates.push(ExecutorUpdate::new(
                "diff",
                "File change",
                file_change_summary(item),
                item.get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        "mcpToolCall" | "dynamicToolCall" => {
            let summary = tool_call_item_summary(item);
            updates.push(
                ExecutorUpdate::new(
                    "tool_call",
                    item_type.to_string(),
                    summary.clone(),
                    item.get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                )
                .with_transcript_summary(format!("{item_type}: {summary}"))
                .with_channel_event(ExecutorChannelEvent::tool_call(item_type, summary)),
            );
        }
        _ => {}
    }
    Ok(CollectedCodexNotification {
        outcome: CodexNotificationOutcome::Pending,
        updates,
    })
}

fn validate_turn_completed(notification: &Value) -> anyhow::Result<bool> {
    let turn = notification
        .get("params")
        .and_then(|params| params.get("turn"))
        .unwrap_or(&Value::Null);
    let status = turn.get("status").and_then(Value::as_str).unwrap_or("");
    if status.is_empty() || status == "completed" {
        return Ok(false);
    }
    if matches!(status, "interrupted" | "cancelled" | "canceled") {
        return Ok(true);
    }
    let error = turn
        .get("error")
        .map(summarize_json_rpc_error)
        .unwrap_or_else(|| "no error details".to_string());
    anyhow::bail!("codex app-server turn ended with status `{status}`: {error}")
}

fn turn_id_from_turn_start_result(result: &Value) -> Option<String> {
    result
        .get("turn")
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
        .or_else(|| result.get("turnId").and_then(Value::as_str))
        .map(str::to_string)
}

fn codex_message_thread_id(message: &Value) -> Option<&str> {
    let params = message.get("params")?;
    params
        .get("threadId")
        .and_then(Value::as_str)
        .or_else(|| params.get("thread_id").and_then(Value::as_str))
        .or_else(|| {
            params
                .get("item")
                .and_then(|item| item.get("threadId"))
                .and_then(Value::as_str)
        })
}

fn codex_message_turn_id(message: &Value) -> Option<&str> {
    let params = message.get("params")?;
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| params.get("turn_id").and_then(Value::as_str))
        .or_else(|| {
            params
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            params
                .get("item")
                .and_then(|item| item.get("turnId"))
                .and_then(Value::as_str)
        })
}

fn codex_message_requires_turn_id(message: &Value) -> bool {
    matches!(
        message.get("method").and_then(Value::as_str).unwrap_or(""),
        "item/started"
            | "item/completed"
            | "turn/completed"
            | "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "mcpServer/elicitation/request"
    )
}

fn command_execution_summary(item: &Value) -> String {
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let output = item
        .get("aggregatedOutput")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let exit_code = item.get("exitCode").and_then(Value::as_i64);
    let mut lines = Vec::new();
    if !command.is_empty() {
        lines.push(format!("$ {command}"));
    }
    if let Some(exit_code) = exit_code {
        lines.push(format!("exit: {exit_code}"));
    }
    if !output.is_empty() {
        lines.push(output.to_string());
    }
    truncate_text(lines.join("\n"), 2_000)
}

fn command_execution_channel_summary(item: &Value) -> String {
    let command = item
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let exit_code = item.get("exitCode").and_then(Value::as_i64);
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut lines = Vec::new();
    if !command.trim().is_empty() {
        lines.push(format!("$ {}", truncate_text(one_line(command), 500)));
    }
    if let Some(exit_code) = exit_code {
        lines.push(format!("exit: {exit_code}"));
    }
    if !status.is_empty() {
        lines.push(format!("status: {status}"));
    }
    if lines.is_empty() {
        "command execution completed".to_string()
    } else {
        truncate_text(lines.join("\n"), 1_000)
    }
}

fn tool_call_item_summary(item: &Value) -> String {
    let name = item
        .get("name")
        .or_else(|| item.get("toolName"))
        .or_else(|| item.get("tool_name"))
        .or_else(|| item.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("tool call");
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if status.is_empty() {
        name.to_string()
    } else {
        format!("{name}\nstatus: {status}")
    }
}

fn file_change_summary(item: &Value) -> String {
    summarize_file_change(item)
}

fn summarize_file_change(item: &Value) -> String {
    let changes = item
        .get("changes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
    if changes.is_empty() {
        let summary = if status.is_empty() {
            "1 change pending".to_string()
        } else {
            format!("status={status}; 1 change pending")
        };
        return truncate_text(summary, 2_000);
    }
    let mut kinds = BTreeMap::<String, usize>::new();
    let paths = changes
        .iter()
        .filter_map(|change| {
            let kind = change
                .get("kind")
                .and_then(|kind| kind.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("update");
            *kinds.entry(kind.to_string()).or_default() += 1;
            change.get("path").and_then(Value::as_str)
        })
        .take(8)
        .collect::<Vec<_>>();
    let counts = kinds
        .into_iter()
        .map(|(kind, count)| format!("{count} {kind}"))
        .collect::<Vec<_>>()
        .join(", ");
    let paths = paths.join(", ");
    let mut parts = Vec::new();
    if !status.is_empty() {
        parts.push(format!("status={status}"));
    }
    if !counts.is_empty() {
        parts.push(counts);
    }
    if !paths.is_empty() {
        parts.push(paths);
    }
    truncate_text(parts.join("; "), 2_000)
}

fn extract_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .and_then(|value| extract_text(Some(value))),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| extract_text(Some(item)))
                .collect::<Vec<_>>()
                .join("");
            (!text.is_empty()).then_some(text)
        }
        other => Some(other.to_string()),
    }
}

fn thread_id_from_result(result: &Value) -> Option<String> {
    result
        .get("thread")
        .and_then(|thread| {
            thread
                .get("id")
                .or_else(|| thread.get("sessionId"))
                .and_then(Value::as_str)
        })
        .or_else(|| result.get("sessionId").and_then(Value::as_str))
        .or_else(|| result.get("threadId").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}

fn truncate_text(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, sync::Arc, time::Duration};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tokio::time::timeout;

    use crate::approval::ApprovalBroker;
    use crate::executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorInterruptRequest,
        ExecutorPrepareRequest, ExecutorPromptRequest, ExecutorTurnRef, InterruptReason,
        test_support::CollectingExecutorEventSink,
    };

    use super::*;

    fn turn_ref(session_key: &str, executor: &str, generation: u64) -> ExecutorTurnRef {
        ExecutorTurnRef {
            session_key: session_key.to_string(),
            executor: executor.to_string(),
            generation,
        }
    }

    fn executor_config(script: &Path, cwd: &Path) -> BTreeMap<String, ExecutorConfig> {
        let mut executors = BTreeMap::new();
        executors.insert(
            "codex".to_string(),
            ExecutorConfig {
                name: "codex".to_string(),
                protocol: ExecutorProtocol::AppServer,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(cwd.to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        executors
    }

    fn make_executable(path: &Path) {
        #[cfg(unix)]
        {
            let mut permissions = fs::metadata(path).unwrap().permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).unwrap();
        }
    }

    #[test]
    fn strict_stdout_accepts_only_json_rpc_shapes() {
        assert!(is_json_rpc_like(&json!({"id": 1, "result": {}})));
        assert!(is_json_rpc_like(&json!({"id": 1, "error": {"code": -1}})));
        assert!(is_json_rpc_like(
            &json!({"method": "turn/completed", "params": {}})
        ));
        assert!(is_json_rpc_like(
            &json!({"id": 1, "method": "client/request"})
        ));
        assert!(!is_json_rpc_like(&json!({"id": 1, "message": "startup"})));
        assert!(!is_json_rpc_like(&json!({"method": "startup"})));
        assert!(!is_json_rpc_like(&json!({"hello": "world"})));
        assert!(!is_json_rpc_like(&json!("banner")));
    }

    #[test]
    fn codex_reasoning_channel_event_requires_summary() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "content": "raw thinking"}}
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
        assert!(collected.updates[0].summary(700).is_none());

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "summary": [{"text": "safe summary"}]}}
            }),
            &mut final_text,
        )
        .unwrap();

        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ReasoningSummary);
        assert_eq!(event.text, "safe summary");
        assert_eq!(
            collected.updates[0].summary(700).as_deref(),
            Some("Reasoning: safe summary")
        );
    }

    #[test]
    fn codex_commentary_agent_message_emits_progress_event() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "phase": "commentary",
                        "text": "I will inspect the config first."
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        assert!(final_text.is_empty());
        assert_eq!(collected.updates.len(), 1);
        assert_eq!(collected.updates[0].kind, "agent_message_chunk");
        assert_eq!(
            collected.updates[0].text,
            "I will inspect the config first."
        );
        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(event.text, "I will inspect the config first.");
    }

    #[test]
    fn codex_final_agent_message_does_not_emit_progress_event() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "phase": "final_answer",
                        "text": "done"
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(final_text, "done");
        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
    }

    #[test]
    fn codex_legacy_agent_message_remains_final_reply_only() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "legacy reply"}}
            }),
            &mut final_text,
        )
        .unwrap();

        assert_eq!(final_text, "legacy reply");
        assert_eq!(collected.updates.len(), 1);
        assert!(collected.updates[0].channel_event.is_none());
    }

    #[test]
    fn codex_command_channel_event_includes_command_without_aggregated_output() {
        let mut final_text = String::new();

        let collected = collect_codex_notification(
            json!({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "commandExecution",
                        "command": "printenv SECRET_TOKEN",
                        "aggregatedOutput": "SECRET_TOKEN=super-secret",
                        "exitCode": 0,
                        "status": "completed"
                    }
                }
            }),
            &mut final_text,
        )
        .unwrap();

        let event = collected.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert!(event.text.contains("$ printenv SECRET_TOKEN"));
        assert!(event.text.contains("exit: 0"));
        assert!(!event.text.contains("super-secret"));
        let summary = collected.updates[0].summary(700).unwrap();
        assert!(summary.contains("$ printenv SECRET_TOKEN"));
        assert!(summary.contains("exit: 0"));
        assert!(!summary.contains("super-secret"));
    }

    fn write_fake_codex_script(path: &Path, behavior: &str) {
        fs::write(
            path,
            format!(
                r#"#!/usr/bin/env python3
import json
import sys
import time

THREAD_ID = "thread-1"
TURN_ID = "turn-1"
TURN_SCOPED_METHODS = {{
    "item/started",
    "item/completed",
    "turn/completed",
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "mcpServer/elicitation/request",
}}

def attach_turn_scope(payload):
    method = payload.get("method")
    params = payload.get("params")
    if method not in TURN_SCOPED_METHODS or not isinstance(params, dict):
        return payload
    params.setdefault("threadId", THREAD_ID)
    if method == "turn/completed":
        turn = params.setdefault("turn", {{}})
        if isinstance(turn, dict):
            turn.setdefault("id", TURN_ID)
    else:
        params.setdefault("turnId", TURN_ID)
    return payload

def send(payload):
    payload = attach_turn_scope(payload)
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

turn_request_id = None

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"userAgent": "fake"}}}})
    elif method == "initialized":
        pass
{behavior}
    elif method == "thread/start":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"thread": {{"id": THREAD_ID}}}}}})
"#
            ),
        )
        .unwrap();
        make_executable(path);
    }

    #[tokio::test]
    async fn codex_prepare_prefers_session_cwd_over_executor_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let executor_cwd = tmp.path().join("executor-cwd");
        let session_cwd = tmp.path().join("session-cwd");
        fs::create_dir_all(&executor_cwd).unwrap();
        fs::create_dir_all(&session_cwd).unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(&script, "");
        let manager = CodexAppServerManager::new(executor_config(&script, &executor_cwd));

        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: Some(session_cwd.clone()),
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        assert_eq!(
            session.lock().await.cwd,
            session_cwd.canonicalize().unwrap()
        );
    }

    #[tokio::test]
    async fn codex_manager_prompts_fake_app_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": "codex reply"}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        })
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(prepared.external_session_id.as_deref(), Some("thread-1"));
        assert!(prepared.started_new_session);
        assert_eq!(response.final_text, "codex reply");
    }

    #[tokio::test]
    async fn codex_prompt_cancellation_sends_turn_interrupt_without_closing_app_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cancellable_codex.py");
        let prompt_marker = tmp.path().join("turn_started");
        let interrupt_marker = tmp.path().join("turn_interrupted");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let interrupt_marker_literal =
            serde_json::to_string(&interrupt_marker.display().to_string())
                .expect("interrupt marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        turn_index = globals().get("turn_index", 0) + 1
        globals()["turn_index"] = turn_index
        active_turn_id = "turn-" + str(turn_index)
        globals()["active_turn_id"] = active_turn_id
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": active_turn_id}}}}}})
        if turn_index > 1:
            send({{
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {{"turnId": active_turn_id, "item": {{"type": "agentMessage", "text": "after interrupt"}}}},
            }})
            send({{
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {{"turn": {{"id": active_turn_id, "status": "completed"}}}},
            }})
    elif method == "turn/interrupt":
        interrupted_turn_id = msg.get("params", {{}}).get("turnId")
        with open({interrupt_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": interrupted_turn_id, "status": "interrupted"}}}},
        }})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());
        assert!(cancel.cancel(InterruptReason::UserStop).await);
        assert!(matches!(
            prompt_task.await.unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        assert!(interrupt_marker.exists());

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        assert!(session.lock().await.client.is_alive());

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 2),
                    prompt: "next".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.final_text, "after interrupt");
    }

    #[tokio::test]
    async fn codex_interrupt_sends_turn_interrupt_while_prompt_lock_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("interruptible_codex.py");
        let prompt_marker = tmp.path().join("turn_started");
        let interrupt_marker = tmp.path().join("turn_interrupted");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let interrupt_marker_literal =
            serde_json::to_string(&interrupt_marker.display().to_string())
                .expect("interrupt marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
    elif method == "turn/interrupt":
        with open({interrupt_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "interrupted"}}}},
        }})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());

        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();
        for _ in 0..50 {
            if interrupt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(interrupt_marker.exists());

        assert!(matches!(
            timeout(Duration::from_secs(5), prompt_task)
                .await
                .expect("prompt task timed out")
                .unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        assert!(session.lock().await.client.is_alive());
    }

    #[tokio::test]
    async fn codex_router_cancel_and_interrupt_send_one_turn_interrupt() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("single_interrupt_codex.py");
        let prompt_marker = tmp.path().join("turn_started");
        let interrupt_count = tmp.path().join("interrupt_count");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let interrupt_count_literal = serde_json::to_string(&interrupt_count.display().to_string())
            .expect("interrupt count path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
    elif method == "turn/interrupt":
        import os
        count = 0
        if os.path.exists({interrupt_count_literal}):
            with open({interrupt_count_literal}) as f:
                count = int(f.read() or "0")
        with open({interrupt_count_literal}, "w") as f:
            f.write(str(count + 1))
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "interrupted"}}}},
        }})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());

        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::ReplacedByNewMessage,
            })
            .await
            .unwrap();

        assert!(matches!(
            timeout(Duration::from_secs(5), prompt_task)
                .await
                .expect("prompt task timed out")
                .unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));

        let mut count = String::new();
        for _ in 0..50 {
            count = fs::read_to_string(&interrupt_count).unwrap_or_default();
            if count == "1" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(count, "1");
    }

    #[tokio::test]
    async fn codex_prompt_waits_for_direct_interrupt_ack_before_releasing_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("delayed_interrupt_ack_codex.py");
        let prompt_marker = tmp.path().join("turn_started");
        let interrupt_marker = tmp.path().join("interrupt_seen");
        let gate = tmp.path().join("release_interrupt");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let interrupt_marker_literal =
            serde_json::to_string(&interrupt_marker.display().to_string())
                .expect("interrupt marker path serializes");
        let gate_literal =
            serde_json::to_string(&gate.display().to_string()).expect("gate path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
    elif method == "turn/interrupt":
        with open({interrupt_marker_literal}, "w") as f:
            f.write("seen")
            f.flush()
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "interrupted"}}}},
        }})
        while not __import__("os").path.exists({gate_literal}):
            time.sleep(0.01)
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let mut prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());

        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();
        for _ in 0..50 {
            if interrupt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(interrupt_marker.exists());

        assert!(cancel.cancel(InterruptReason::UserStop).await);
        assert!(
            timeout(Duration::from_millis(100), &mut prompt_task)
                .await
                .is_err(),
            "prompt released the session before turn/interrupt was acknowledged"
        );

        fs::write(&gate, "go").unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(5), prompt_task)
                .await
                .expect("prompt task timed out")
                .unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
    }

    #[tokio::test]
    async fn codex_direct_interrupt_clears_pending_approval_scope() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("interrupt_approval_codex.py");
        let approval_response_marker = tmp.path().join("approval_response");
        let approval_response_marker_literal =
            serde_json::to_string(&approval_response_marker.display().to_string())
                .expect("approval response marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
        send({{
            "jsonrpc": "2.0",
            "id": 900,
            "method": "item/commandExecution/requestApproval",
            "params": {{"command": "danger", "cwd": "/tmp", "reason": "test"}},
        }})
    elif method == "turn/interrupt":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "interrupted"}}}},
        }})
    elif request_id == 900:
        with open({approval_response_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
"#
            ),
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });

        let approval_prompt = timeout(Duration::from_secs(5), prompts.recv())
            .await
            .expect("approval prompt timed out")
            .unwrap();
        assert!(approvals.has_pending_for_session("session-1").await);

        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        assert!(matches!(
            timeout(Duration::from_secs(5), prompt_task)
                .await
                .expect("prompt task timed out")
                .unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        assert!(!approvals.has_pending_for_session("session-1").await);
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", approval_prompt.id)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let approval_response = fs::read_to_string(&approval_response_marker).unwrap();
        assert!(approval_response.contains(r#""decision": "decline""#));
    }

    #[tokio::test]
    async fn codex_interrupt_error_closes_app_server_as_unhealthy() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("interrupt_error_codex.py");
        let prompt_marker = tmp.path().join("turn_started");
        let interrupt_marker = tmp.path().join("turn_interrupted");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let interrupt_marker_literal =
            serde_json::to_string(&interrupt_marker.display().to_string())
                .expect("interrupt marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
    elif method == "turn/interrupt":
        with open({interrupt_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
        send({{
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {{"code": -32000, "message": "interrupt failed"}},
        }})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "wait".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });

        for _ in 0..50 {
            if prompt_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(prompt_marker.exists());

        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        let err = timeout(Duration::from_secs(5), prompt_task)
            .await
            .expect("prompt task timed out")
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("turn/interrupt failed"));
        assert!(interrupt_marker.exists());

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        for _ in 0..50 {
            if !session.lock().await.client.is_alive() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!session.lock().await.client.is_alive());
    }

    #[tokio::test]
    async fn codex_pre_turn_id_interrupt_declines_early_approval_before_interrupting() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("early_approval_interrupt_codex.py");
        let approval_response_marker = tmp.path().join("approval_response.json");
        let interrupt_marker = tmp.path().join("turn_interrupted.json");
        let approval_response_marker_literal =
            serde_json::to_string(&approval_response_marker.display().to_string())
                .expect("approval response marker path serializes");
        let interrupt_marker_literal =
            serde_json::to_string(&interrupt_marker.display().to_string())
                .expect("interrupt marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({{
            "jsonrpc": "2.0",
            "id": 900,
            "method": "item/commandExecution/requestApproval",
            "params": {{"command": "danger", "cwd": "/tmp", "reason": "test"}},
        }})
    elif request_id == 900:
        with open({approval_response_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
        if msg.get("result", {{}}).get("decision") == "decline":
            send({{"jsonrpc": "2.0", "id": turn_request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
    elif method == "turn/interrupt":
        with open({interrupt_marker_literal}, "w") as f:
            f.write(json.dumps(msg))
            f.flush()
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "interrupted"}}}},
        }})
"#
            ),
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "needs approval before ack".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        let approval_prompt = timeout(Duration::from_secs(5), prompts.recv())
            .await
            .expect("approval prompt timed out")
            .unwrap();
        assert!(approvals.has_pending_for_session("session-1").await);

        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref("session-1", "codex", 1),
                reason: InterruptReason::ReplacedByNewMessage,
            })
            .await
            .unwrap();

        assert!(matches!(
            timeout(Duration::from_secs(5), prompt_task)
                .await
                .expect("prompt task timed out")
                .unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        assert!(!approvals.has_pending_for_session("session-1").await);
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", approval_prompt.id)
        );

        let approval_response = fs::read_to_string(&approval_response_marker).unwrap();
        assert!(approval_response.contains(r#""decision": "decline""#));
        assert!(interrupt_marker.exists());
        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        assert!(session.lock().await.client.is_alive());
    }

    #[tokio::test]
    async fn codex_cancelled_prepare_after_publication_keeps_session_reusable() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("delayed_thread_start_codex.py");
        let thread_start_marker = tmp.path().join("thread_start_seen");
        let gate = tmp.path().join("release_thread_start");
        let thread_start_marker_literal =
            serde_json::to_string(&thread_start_marker.display().to_string())
                .expect("marker path serializes");
        let gate_literal =
            serde_json::to_string(&gate.display().to_string()).expect("gate path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "thread/start":
        thread_start_count = globals().get("thread_start_count", 0) + 1
        globals()["thread_start_count"] = thread_start_count
        if thread_start_count == 1:
            with open({thread_start_marker_literal}, "w") as f:
                f.write("seen")
                f.flush()
            while not __import__("os").path.exists({gate_literal}):
                time.sleep(0.01)
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"thread": {{"id": THREAD_ID}}}}}})
"#
            ),
        );
        let manager = Arc::new(CodexAppServerManager::new(executor_config(
            &script,
            tmp.path(),
        )));

        let cancel = TurnCancellation::new();
        let prepare_cancel = cancel.clone();
        let prepare_manager = manager.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        cwd: None,
                        previous_session_id: None,
                    },
                    prepare_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if thread_start_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(thread_start_marker.exists());
        let published = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();

        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        fs::write(&gate, "go").unwrap();
        let prepare_err = prepare_task.await.unwrap().unwrap_err();
        assert!(prepare_err.to_string().contains("thread/start cancelled"));

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 2),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        let reused = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&published, &reused));
        assert_eq!(prepared.external_session_id.as_deref(), Some("thread-1"));
        assert!(prepared.started_new_session);
        assert!(reused.lock().await.client.is_alive());
    }

    #[tokio::test]
    async fn codex_command_approval_waits_for_text_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "pwd", "cwd": "/tmp", "reason": "test"},
        })
    elif request_id == 900:
        if msg.get("result", {}).get("decision") == "accept":
            send({"jsonrpc": "2.0", "id": turn_request_id, "result": {"turn": {"id": "turn-1"}}})
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "approved"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
        else:
            send({"jsonrpc": "2.0", "id": turn_request_id, "error": {"code": -32000, "message": "not approved"}})
"#,
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "run pwd".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("$ pwd"));
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "approved");
    }

    #[tokio::test]
    async fn codex_pending_approval_is_cancelled_when_turn_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("completed_with_pending_approval.py");
        let stale_response_marker = tmp.path().join("stale_response.json");
        let marker_literal = serde_json::to_string(&stale_response_marker.display().to_string())
            .expect("marker path serializes");
        write_fake_codex_script(
            &script,
            &format!(
                r#"    elif method == "turn/start":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"turn": {{"id": "turn-1"}}}}}})
        send({{
            "jsonrpc": "2.0",
            "id": 903,
            "method": "item/commandExecution/requestApproval",
            "params": {{"command": "sleep 10", "cwd": "/tmp", "reason": "test"}},
        }})
        time.sleep(0.05)
        send({{
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {{"item": {{"type": "agentMessage", "text": "done"}}}},
        }})
        send({{
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {{"turn": {{"id": "turn-1", "status": "completed"}}}},
        }})
    elif request_id == 903:
        with open({marker_literal}, "w") as f:
            f.write(json.dumps(msg))
"#
            ),
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "finish with pending approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });

        let approval_prompt = timeout(Duration::from_secs(5), prompts.recv())
            .await
            .expect("approval prompt timed out")
            .unwrap();
        let response = timeout(Duration::from_secs(5), prompt_task)
            .await
            .expect("prompt task timed out")
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "done");

        assert!(!approvals.has_pending_for_session("session-1").await);
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", approval_prompt.id)
        );

        sleep(Duration::from_millis(50)).await;
        assert!(!stale_response_marker.exists());
    }

    #[tokio::test]
    async fn codex_failed_turn_status_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("failed_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "turn": {
                    "id": "turn-1",
                    "status": "failed",
                    "error": {"code": 123, "message": "secret error detail"},
                },
            },
        })
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "fail".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("status `failed`"));
        assert!(message.contains("code=123"));
        assert!(!message.contains("secret error detail"));
    }

    #[tokio::test]
    async fn codex_file_change_approval_includes_cached_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("file_change_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        for i in range(12):
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "reasoning", "summary": ["noise " + str(i)]}},
            })
        send({
            "jsonrpc": "2.0",
            "method": "item/started",
            "params": {
                "item": {
                    "id": "patch-1",
                    "type": "fileChange",
                    "changes": [
                        {"kind": {"type": "modify"}, "path": "src/lib.rs"},
                        {"kind": {"type": "add"}, "path": "src/new.rs"},
                    ],
                },
            },
        })
        send({
            "jsonrpc": "2.0",
            "id": 901,
            "method": "item/fileChange/requestApproval",
            "params": {"itemId": "patch-1", "grantRoot": "/repo", "reason": "apply patch"},
        })
    elif request_id == 901:
        if msg.get("result", {}).get("decision") == "accept":
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "patched"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
"#,
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_approvals(
            executor_config(&script, tmp.path()),
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "patch".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("src/lib.rs"));
        assert!(approval_prompt.body.contains("src/new.rs"));
        assert!(approval_prompt.body.contains("1 add"));
        assert!(approval_prompt.body.contains("1 modify"));
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "patched");
    }

    #[tokio::test]
    async fn codex_mcp_elicitation_declines_with_action_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("elicitation_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        send({
            "jsonrpc": "2.0",
            "id": 902,
            "method": "mcpServer/elicitation/request",
            "params": {"serverName": "external"},
        })
    elif request_id == 902:
        result = msg.get("result", {})
        if result == {"action": "decline"}:
            send({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {"item": {"type": "agentMessage", "text": "declined"}},
            })
            send({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1", "status": "completed"}},
            })
        else:
            send({"jsonrpc": "2.0", "id": turn_request_id, "error": {"code": -32000, "message": "wrong shape"}})
"#,
        );
        let manager = CodexAppServerManager::new(executor_config(&script, tmp.path()));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "elicit".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.final_text, "declined");
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("stuck_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        pass
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(50),
            },
        );
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_is_not_blocked_by_early_server_request() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("early_request_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "sleep 10", "cwd": "/tmp", "reason": "test"},
        })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            Arc::new(ApprovalBroker::new(Duration::from_secs(5))),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(100),
            },
        );
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = tokio::time::timeout(
            Duration::from_millis(500),
            manager.prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            ),
        )
        .await
        .expect("turn/start timeout waited behind approval")
        .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_turn_start_timeout_survives_many_early_server_requests() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("many_early_requests_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        for index in range(80):
            send({
                "jsonrpc": "2.0",
                "id": 1000 + index,
                "method": "item/commandExecution/requestApproval",
                "params": {"command": "sleep 10", "cwd": "/tmp", "reason": "test"},
            })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            Arc::new(ApprovalBroker::new(Duration::from_secs(5))),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(100),
            },
        );
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let err = timeout(
            Duration::from_millis(500),
            manager.prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "hang".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            ),
        )
        .await
        .expect("turn/start timeout waited behind saturated request queue")
        .unwrap_err();

        assert!(err.to_string().contains("turn/start"));
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn codex_many_early_approval_requests_are_ordered_and_answered() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("many_ordered_requests_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        turn_request_id = request_id
        response_ids = []
        for index in range(80):
            send({
                "jsonrpc": "2.0",
                "id": 1000 + index,
                "method": "item/commandExecution/requestApproval",
                "params": {"command": "cmd " + str(index), "cwd": "/tmp", "reason": "test"},
            })
    elif 1000 <= request_id < 1080:
        response_ids.append(request_id)
        if len(response_ids) == 80:
            if response_ids == list(range(1000, 1080)):
                send({"jsonrpc": "2.0", "id": turn_request_id, "result": {"turn": {"id": "turn-1"}}})
                send({
                    "jsonrpc": "2.0",
                    "method": "item/completed",
                    "params": {"item": {"type": "agentMessage", "text": "answered"}},
                })
                send({
                    "jsonrpc": "2.0",
                    "method": "turn/completed",
                    "params": {"turn": {"id": "turn-1", "status": "completed"}},
                })
            else:
                send({"jsonrpc": "2.0", "id": turn_request_id, "error": {"code": -32000, "message": "out of order"}})
"#,
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            approvals.clone(),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_secs(5),
            },
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let prompt_manager = manager.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "codex", 1),
                        prompt: "many approvals".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });

        for index in 0..80 {
            let prompt = timeout(Duration::from_secs(2), prompts.recv())
                .await
                .expect("approval prompt timed out")
                .unwrap();
            assert!(prompt.body.contains(&format!("$ cmd {index}")));
            approvals
                .resolve_command("session-1", &format!("/deny {}", prompt.id), Some("U1"))
                .await
                .unwrap();
        }

        let response = timeout(Duration::from_secs(2), prompt_task)
            .await
            .expect("prompt task timed out")
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "answered");
    }

    #[tokio::test]
    async fn codex_turn_allows_silent_work_after_start() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("silent_codex.py");
        write_fake_codex_script(
            &script,
            r#"    elif method == "turn/start":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn-1"}}})
        time.sleep(0.15)
        send({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": "silent done"}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn-1", "status": "completed"}},
        })
"#,
        );
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_millis(50),
            },
        );
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    prompt: "work silently".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.final_text, "silent done");
    }

    #[tokio::test]
    async fn codex_drain_ready_notifications_collects_ready_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_codex.py");
        write_fake_codex_script(&script, "");
        let manager = CodexAppServerManager::with_limits(
            executor_config(&script, tmp.path()),
            crate::machine::MachineRegistry::local_default(),
            Arc::new(ApprovalBroker::default()),
            CodexRuntimeLimits {
                rpc_timeout: Duration::from_secs(1),
            },
        );
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "codex", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let session = manager
            .existing_session("session-1", "codex")
            .await
            .unwrap();
        let mut session = session.lock().await;
        let (tx, mut notifications) = mpsc::unbounded_channel();
        let mut events = CollectingExecutorEventSink::default();
        let mut final_text = String::new();
        let mut turn_completed = false;
        let mut backend_interrupted = false;

        tx.send(json!({
            "method": "item/completed",
            "params": {"item": {"type": "reasoning", "summary": [{"text": "still working"}]}}
        }))
        .unwrap();

        session
            .drain_ready_notifications(
                &mut notifications,
                &mut final_text,
                &mut events,
                &mut turn_completed,
                &mut backend_interrupted,
            )
            .await
            .unwrap();

        assert!(!turn_completed);
        assert!(!backend_interrupted);
        assert_eq!(events.updates.len(), 1);
    }

    #[tokio::test]
    async fn codex_turn_streams_drop_completed_turn_events_after_next_turn_opens() {
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let event_streams = Arc::new(StdMutex::new(CodexEventStreams::default()));

        {
            let (old_notifications, _old_notification_rx) = mpsc::unbounded_channel();
            let (old_requests, _old_request_rx) = mpsc::unbounded_channel();
            let generation = event_streams.lock().unwrap().open(
                "thread-1".to_string(),
                old_notifications,
                old_requests,
            );
            event_streams
                .lock()
                .unwrap()
                .set_turn_id(generation, "turn-1".to_string());
            event_streams
                .lock()
                .unwrap()
                .finish(generation, Some("turn-1"));
        }
        for index in 2..=20 {
            let (notifications, _notification_rx) = mpsc::unbounded_channel();
            let (requests, _request_rx) = mpsc::unbounded_channel();
            let generation =
                event_streams
                    .lock()
                    .unwrap()
                    .open("thread-1".to_string(), notifications, requests);
            let turn_id = format!("turn-{index}");
            event_streams
                .lock()
                .unwrap()
                .set_turn_id(generation, turn_id.clone());
            event_streams
                .lock()
                .unwrap()
                .finish(generation, Some(&turn_id));
        }

        let (new_notifications_tx, mut new_notifications) = mpsc::unbounded_channel();
        let (new_requests_tx, mut new_requests) = mpsc::unbounded_channel();
        let new_generation = event_streams.lock().unwrap().open(
            "thread-1".to_string(),
            new_notifications_tx,
            new_requests_tx,
        );

        dispatch_codex_message(
            json!({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"type": "agentMessage", "text": "stale"},
                },
            }),
            &state,
            &event_streams,
        )
        .await;
        dispatch_codex_message(
            json!({
                "jsonrpc": "2.0",
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1", "status": "completed"},
                },
            }),
            &state,
            &event_streams,
        )
        .await;
        dispatch_codex_message(
            json!({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "item": {"type": "agentMessage", "text": "unscoped stale"},
                },
            }),
            &state,
            &event_streams,
        )
        .await;
        assert!(new_notifications.try_recv().is_err());

        for method in [
            "mcpServer/elicitation/request",
            "item/permissions/requestApproval",
        ] {
            dispatch_codex_message(
                json!({
                    "jsonrpc": "2.0",
                    "id": 100,
                    "method": method,
                    "params": {
                        "threadId": "thread-1",
                    },
                }),
                &state,
                &event_streams,
            )
            .await;
        }
        assert!(new_requests.try_recv().is_err());

        event_streams
            .lock()
            .unwrap()
            .set_turn_id(new_generation, "turn-21".to_string());

        dispatch_codex_message(
            json!({
                "jsonrpc": "2.0",
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-21",
                    "item": {"type": "agentMessage", "text": "fresh"},
                },
            }),
            &state,
            &event_streams,
        )
        .await;
        let notification = new_notifications.recv().await.unwrap();
        assert_eq!(
            notification
                .get("params")
                .and_then(|params| params.get("turnId"))
                .and_then(Value::as_str),
            Some("turn-21")
        );
    }
}
