use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin},
    sync::{Mutex, Notify, broadcast, oneshot},
    time::{Duration, Instant, sleep},
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
        SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorChannelEvent, ExecutorChannelEventKind, ExecutorDescriptor,
        ExecutorEventSink, ExecutorInterruptRequest, ExecutorPrepareRequest, ExecutorPromptOutcome,
        ExecutorPromptRequest, ExecutorResponse, ExecutorUpdate, PreparedExecutor,
        TurnCancellation, summarize_json_rpc_error,
    },
    machine::{MachinePrepareRequest, MachineRegistry, MachineWorkspaceRecord, StdioCommand},
    text::append_reply_message_break,
};

type SharedJsonRpcState = Arc<Mutex<JsonRpcState>>;
type SharedAcpToolCalls = Arc<Mutex<HashMap<String, AcpToolCallState>>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
type SessionKey = (String, String);
type SharedAcpSession = Arc<Mutex<AcpSession>>;
type SessionMap = HashMap<SessionKey, SharedAcpSession>;
type SharedAcpCancelBarrier = Arc<AcpCancelBarrier>;
type SharedActiveAcpPrompts = Arc<Mutex<HashMap<SessionKey, ActiveAcpPrompt>>>;

const ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Default)]
struct AcpToolCallState {
    title: String,
    text: String,
    status: String,
    activity_emitted: bool,
}

#[derive(Debug, Default)]
struct AcpCancelBarrier {
    changed: Notify,
}

impl AcpCancelBarrier {
    fn notify_changed(&self) {
        self.changed.notify_waiters();
    }
}

#[derive(Debug, Clone)]
struct ActiveAcpPrompt {
    session_id: String,
    request_id: u64,
    generation: u64,
    client: JsonRpcClientHandle,
}

impl ActiveAcpPrompt {
    async fn notify_cancel(&self) -> anyhow::Result<()> {
        self.client
            .notify("session/cancel", json!({ "sessionId": self.session_id }))
            .await
    }
}

#[derive(Debug, Clone)]
struct AcpPromptTurnRegistration {
    active_prompts: SharedActiveAcpPrompts,
    key: SessionKey,
    generation: u64,
}

#[derive(Debug, Clone)]
struct JsonRpcClientHandle {
    stdin: SharedStdin,
}

impl JsonRpcClientHandle {
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

#[derive(Debug)]
pub struct AcpExecutorManager {
    executors: BTreeMap<String, ExecutorConfig>,
    session_manager: AcpBackendSessionManager,
    active_prompts: SharedActiveAcpPrompts,
}

impl AcpExecutorManager {
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
        let executors = executors
            .into_iter()
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::Acp)
            .collect();
        Self {
            executors,
            session_manager: AcpBackendSessionManager::new(approvals, machines),
            active_prompts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
struct AcpBackendSessionManager {
    approvals: SharedApprovalBroker,
    machines: MachineRegistry,
    sessions: Mutex<SessionMap>,
}

impl AcpBackendSessionManager {
    fn new(approvals: SharedApprovalBroker, machines: MachineRegistry) -> Self {
        Self {
            approvals,
            machines,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    async fn get_or_create_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        router_workspace: Option<&Path>,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<Arc<Mutex<AcpSession>>> {
        let key = (session_key.to_string(), executor.to_string());
        loop {
            if cancel.is_cancelled().await {
                anyhow::bail!("ACP prepare cancelled");
            }
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
            if let Some(existing) = existing.as_ref() {
                let matches = existing.lock().await.matches(cfg, &prepared_command.stdio);
                if matches {
                    return Ok(existing.clone());
                }
                if cancel.is_cancelled().await {
                    anyhow::bail!("ACP prepare cancelled");
                }
                if !self
                    .remove_unhealthy_session_if_same(
                        &key,
                        existing,
                        "ACP session no longer matches config",
                        cancel,
                    )
                    .await
                {
                    continue;
                }
            }

            let session = Arc::new(Mutex::new(
                AcpSession::start(
                    cfg.clone(),
                    prepared_command.stdio,
                    prepared_command.workspace,
                    session_key.to_string(),
                    executor.to_string(),
                    self.approvals.clone(),
                )
                .await?,
            ));
            if cancel.is_cancelled().await {
                session
                    .lock()
                    .await
                    .client
                    .close("ACP prepare cancelled before publication")
                    .await;
                anyhow::bail!("ACP prepare cancelled");
            }
            let mut sessions = self.sessions.lock().await;
            if sessions.contains_key(&key) {
                drop(sessions);
                session
                    .lock()
                    .await
                    .client
                    .close("ACP session publication lost race")
                    .await;
                continue;
            }
            sessions.insert(key.clone(), session.clone());
            return Ok(session);
        }
    }

    async fn remove_unhealthy_session_if_same(
        &self,
        key: &SessionKey,
        session: &Arc<Mutex<AcpSession>>,
        reason: &str,
        cancel: &TurnCancellation,
    ) -> bool {
        let removed = {
            let mut sessions = self.sessions.lock().await;
            if !cancel.is_cancelled_now()
                && sessions
                    .get(key)
                    .is_some_and(|existing| Arc::ptr_eq(existing, session))
            {
                sessions.remove(key);
                true
            } else {
                false
            }
        };
        if removed {
            let mut session = session.lock().await;
            session.client.close(reason).await;
            session.session_id = None;
        }
        removed
    }

    async fn existing_session(
        &self,
        session_key: &str,
        executor: &str,
    ) -> anyhow::Result<Arc<Mutex<AcpSession>>> {
        self.sessions
            .lock()
            .await
            .get(&(session_key.to_string(), executor.to_string()))
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared ACP session for `{session_key}`"
                )
            })
    }
}

#[async_trait]
impl ExecutorBackend for AcpExecutorManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: "acp".to_string(),
            machine_id: cfg.machine.clone(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: "acp".to_string(),
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
            anyhow::bail!("ACP prepare cancelled");
        }
        let cfg = self.executors.get(&request.turn.executor).ok_or_else(|| {
            anyhow::anyhow!("executor `{}` is not configured", request.turn.executor)
        })?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            previous_session_id = ?request.previous_session_id,
            "preparing ACP executor session"
        );
        let session = self
            .session_manager
            .get_or_create_session(
                &request.turn.session_key,
                &request.turn.executor,
                cfg,
                request.cwd.as_deref(),
                &cancel,
            )
            .await?;
        if cancel.is_cancelled().await {
            anyhow::bail!("ACP prepare cancelled");
        }
        let mut session = session.lock().await;
        let (external_session_id, started_new_session) = session
            .ensure_session(request.previous_session_id.as_deref(), cancel)
            .await?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            external_session_id = %external_session_id,
            started_new_session,
            "prepared ACP executor session"
        );
        Ok(PreparedExecutor {
            external_session_id: Some(external_session_id),
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
        let session_key = request.turn.session_key.clone();
        let executor = request.turn.executor.clone();
        let active_key = (session_key.clone(), executor.clone());
        let session = match self
            .session_manager
            .existing_session(&session_key, &executor)
            .await
        {
            Ok(session) => session,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let mut session = session.lock().await;
        let generation = request.turn.generation;
        let prompt_len = request.prompt.len();
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            generation,
            prompt_len,
            "starting ACP executor turn"
        );
        let active_prompt_turn = AcpPromptTurnRegistration {
            active_prompts: self.active_prompts.clone(),
            key: active_key,
            generation,
        };
        let result = session
            .prompt(
                &request.prompt,
                request.user_id,
                events,
                cancel,
                active_prompt_turn,
            )
            .await;
        match &result {
            ExecutorPromptOutcome::Completed(result) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                final_text_len = result.final_text.len(),
                "completed ACP executor turn"
            ),
            ExecutorPromptOutcome::Cancelled => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                "cancelled ACP executor turn"
            ),
            ExecutorPromptOutcome::Failed(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                generation,
                "failed ACP executor turn"
            ),
        }
        result
    }

    async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
        let key = (
            request.turn.session_key.clone(),
            request.turn.executor.clone(),
        );
        let active_prompt = self.active_prompts.lock().await.get(&key).cloned();
        if let Some(active_prompt) = active_prompt {
            if active_prompt.generation != request.turn.generation {
                tracing::debug!(
                    target: "agent_router::acp",
                    executor = %request.turn.executor,
                    session_key = %request.turn.session_key,
                    interrupted_generation = request.turn.generation,
                    active_generation = active_prompt.generation,
                    active_request_id = active_prompt.request_id,
                    reason = ?request.reason,
                    "ignoring stale ACP interrupt for newer active prompt"
                );
                return Ok(());
            }
            tracing::debug!(
                target: "agent_router::acp",
                executor = %request.turn.executor,
                session_key = %request.turn.session_key,
                generation = request.turn.generation,
                request_id = active_prompt.request_id,
                reason = ?request.reason,
                "notifying ACP active prompt cancellation"
            );
            active_prompt.notify_cancel().await?;
            return Ok(());
        }
        Ok(())
    }
}

async fn set_active_acp_prompt(
    active_prompts: &SharedActiveAcpPrompts,
    key: SessionKey,
    prompt: ActiveAcpPrompt,
) {
    active_prompts.lock().await.insert(key, prompt);
}

async fn clear_active_acp_prompt(
    active_prompts: &SharedActiveAcpPrompts,
    key: &SessionKey,
    request_id: u64,
) {
    let mut prompts = active_prompts.lock().await;
    if prompts
        .get(key)
        .is_some_and(|prompt| prompt.request_id == request_id)
    {
        prompts.remove(key);
    }
}

#[derive(Debug)]
struct AcpSession {
    cfg: ExecutorConfig,
    stdio: StdioCommand,
    cwd: String,
    workspace: Option<MachineWorkspaceRecord>,
    client: JsonRpcClient,
    session_id: Option<String>,
    initialized: bool,
}

impl AcpSession {
    async fn start(
        cfg: ExecutorConfig,
        stdio: StdioCommand,
        workspace: Option<MachineWorkspaceRecord>,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            cwd = %stdio.executor_cwd,
            "starting ACP executor process"
        );
        let client =
            JsonRpcClient::spawn(&stdio, session_key.clone(), executor.clone(), approvals).await?;
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            "started ACP executor process"
        );
        Ok(Self {
            cfg,
            cwd: stdio.executor_cwd.clone(),
            stdio,
            workspace,
            client,
            session_id: None,
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
        let response = self
            .client
            .lifecycle_request_until_cancelled(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": {
                        "name": "agent-router",
                        "title": "Agent Router",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }),
                cancel,
            )
            .await?;
        self.initialized = true;
        if response.cancelled {
            anyhow::bail!("ACP initialize cancelled");
        }
        Ok(())
    }

    async fn ensure_session(
        &mut self,
        preferred_session_id: Option<&str>,
        cancel: TurnCancellation,
    ) -> anyhow::Result<(String, bool)> {
        self.initialize(cancel.clone()).await?;
        if let Some(session_id) = &self.session_id {
            return Ok((
                session_id.clone(),
                preferred_session_id != Some(session_id.as_str()),
            ));
        }

        if let Some(preferred) = preferred_session_id.filter(|value| !value.is_empty()) {
            for method in ["session/load", "session/resume"] {
                let response = self
                    .client
                    .lifecycle_request_until_cancelled(
                        method,
                        json!({
                            "cwd": self.cwd,
                            "sessionId": preferred,
                            "mcpServers": [],
                        }),
                        cancel.clone(),
                    )
                    .await;
                if let Ok(response) = response {
                    let session_id = session_id_from_result(&response.result)
                        .unwrap_or_else(|| preferred.to_string());
                    self.session_id = Some(session_id.clone());
                    if response.cancelled {
                        anyhow::bail!("ACP session resume cancelled");
                    }
                    return Ok((session_id, false));
                }
                if cancel.is_cancelled().await {
                    anyhow::bail!("ACP session resume cancelled");
                }
            }
        }

        let response = self
            .client
            .lifecycle_request_until_cancelled(
                "session/new",
                json!({
                    "cwd": self.cwd,
                    "mcpServers": [],
                }),
                cancel,
            )
            .await?;
        let session_id = session_id_from_result(&response.result)
            .ok_or_else(|| anyhow::anyhow!("ACP session/new did not return sessionId"))?;
        self.session_id = Some(session_id.clone());
        if response.cancelled {
            anyhow::bail!("ACP session/new cancelled");
        }
        Ok((session_id, true))
    }

    async fn prompt(
        &mut self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
        active_prompt_turn: AcpPromptTurnRegistration,
    ) -> ExecutorPromptOutcome {
        self.client.set_active_user(user_id).await;
        self.client.set_active_turn_cancel(cancel.clone()).await;
        let result = self
            .prompt_with_active_user(prompt, events, cancel, active_prompt_turn)
            .await;
        self.client.clear_active_turn_cancel().await;
        self.client.clear_active_user().await;
        result
    }

    async fn prompt_with_active_user(
        &mut self,
        prompt: &str,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
        active_prompt_turn: AcpPromptTurnRegistration,
    ) -> ExecutorPromptOutcome {
        let session_id = match self.session_id.clone() {
            Some(session_id) => session_id,
            None => {
                return ExecutorPromptOutcome::Failed(anyhow::anyhow!(
                    "ACP session has not been created"
                ));
            }
        };
        if cancel.is_cancelled().await {
            return ExecutorPromptOutcome::Cancelled;
        }
        tokio::select! {
            result = self.client.wait_for_cancel_barrier() => {
                if let Err(err) = result {
                    return ExecutorPromptOutcome::Failed(err);
                }
            }
            _ = cancel.cancelled() => return ExecutorPromptOutcome::Cancelled,
        }
        if cancel.is_cancelled().await {
            return ExecutorPromptOutcome::Cancelled;
        }
        let mut updates_rx = self.client.subscribe();
        let request = match self
            .client
            .prompt_request_started(
                &session_id,
                prompt,
                &active_prompt_turn.active_prompts,
                active_prompt_turn.key.clone(),
                active_prompt_turn.generation,
            )
            .await
        {
            Ok(request) => request,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let response_fut = request.response;
        tokio::pin!(response_fut);

        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();
        let result = loop {
            tokio::select! {
                result = &mut response_fut => {
                    let response = match result {
                        Ok(Ok(response)) => response,
                        Ok(Err(err)) => {
                            let outcome = self
                                .failed_prompt_or_cancelled(&cancel, &session_id, request.id, err)
                                .await;
                            clear_active_acp_prompt(
                                &active_prompt_turn.active_prompts,
                                &active_prompt_turn.key,
                                request.id,
                            )
                            .await;
                            return outcome;
                        }
                        Err(_) => {
                            let outcome = self
                                .failed_prompt_or_cancelled(
                                    &cancel,
                                    &session_id,
                                    request.id,
                                    anyhow::anyhow!("ACP response channel closed"),
                                )
                                .await;
                            clear_active_acp_prompt(
                                &active_prompt_turn.active_prompts,
                                &active_prompt_turn.key,
                                request.id,
                            )
                            .await;
                            return outcome;
                        }
                    };
                    if let Some(error) = response.get("error") {
                        let outcome = self
                            .failed_prompt_or_cancelled(
                                &cancel,
                                &session_id,
                                request.id,
                                anyhow::anyhow!(
                                    "ACP `{}` failed: {}",
                                    request.method,
                                    summarize_json_rpc_error(error)
                                ),
                            )
                            .await;
                        clear_active_acp_prompt(
                            &active_prompt_turn.active_prompts,
                            &active_prompt_turn.key,
                            request.id,
                        )
                        .await;
                        return outcome;
                    }
                    break response.get("result").cloned().unwrap_or(Value::Null);
                }
                _ = cancel.cancelled() => {
                    self.cancel_prompt_session(&session_id, request.id).await;
                    clear_active_acp_prompt(
                        &active_prompt_turn.active_prompts,
                        &active_prompt_turn.key,
                        request.id,
                    )
                    .await;
                    return ExecutorPromptOutcome::Cancelled;
                }
                received = updates_rx.recv() => {
                    match received {
                        Ok(update) => {
                            if let Err(err) = collect_update(
                                update,
                                events,
                                &mut text_parts,
                                &mut pending_message_chunks,
                            ).await {
                                let outcome = self
                                    .failed_prompt_or_cancelled(
                                        &cancel,
                                        &session_id,
                                        request.id,
                                        err,
                                    )
                                    .await;
                                clear_active_acp_prompt(
                                    &active_prompt_turn.active_prompts,
                                    &active_prompt_turn.key,
                                    request.id,
                                )
                                .await;
                                return outcome;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => continue,
                    }
                }
            }
        };
        if cancel.is_cancelled().await {
            clear_active_acp_prompt(
                &active_prompt_turn.active_prompts,
                &active_prompt_turn.key,
                request.id,
            )
            .await;
            return ExecutorPromptOutcome::Cancelled;
        }
        while let Ok(update) = updates_rx.try_recv() {
            if let Err(err) =
                collect_update(update, events, &mut text_parts, &mut pending_message_chunks).await
            {
                let outcome = self
                    .failed_prompt_or_cancelled(&cancel, &session_id, request.id, err)
                    .await;
                clear_active_acp_prompt(
                    &active_prompt_turn.active_prompts,
                    &active_prompt_turn.key,
                    request.id,
                )
                .await;
                return outcome;
            }
        }
        if acp_result_cancelled(&result) {
            clear_active_acp_prompt(
                &active_prompt_turn.active_prompts,
                &active_prompt_turn.key,
                request.id,
            )
            .await;
            return ExecutorPromptOutcome::Cancelled;
        }
        if let Err(err) = pending_message_chunks
            .flush_as_final(events, &mut text_parts)
            .await
        {
            let outcome = self
                .failed_prompt_or_cancelled(&cancel, &session_id, request.id, err)
                .await;
            clear_active_acp_prompt(
                &active_prompt_turn.active_prompts,
                &active_prompt_turn.key,
                request.id,
            )
            .await;
            return outcome;
        }
        let final_text = if text_parts.is_empty() {
            extract_text_result(&result)
        } else {
            text_parts.join()
        };
        clear_active_acp_prompt(
            &active_prompt_turn.active_prompts,
            &active_prompt_turn.key,
            request.id,
        )
        .await;
        ExecutorPromptOutcome::Completed(ExecutorResponse { final_text })
    }

    async fn failed_prompt_or_cancelled(
        &mut self,
        cancel: &TurnCancellation,
        session_id: &str,
        request_id: u64,
        err: anyhow::Error,
    ) -> ExecutorPromptOutcome {
        if cancel.is_cancelled().await {
            self.cancel_prompt_session(session_id, request_id).await;
            ExecutorPromptOutcome::Cancelled
        } else {
            ExecutorPromptOutcome::Failed(err)
        }
    }

    async fn cancel_prompt_session(&mut self, session_id: &str, request_id: u64) {
        if !self
            .client
            .begin_cancel_barrier_if_pending(request_id)
            .await
        {
            tracing::debug!(
                target: "agent_router::acp",
                request_id,
                "skipping ACP session/cancel because prompt response is no longer pending"
            );
            return;
        }
        let notify_result = self
            .client
            .notify("session/cancel", json!({ "sessionId": session_id }))
            .await;
        if let Err(err) = notify_result {
            tracing::debug!(
                target: "agent_router::acp",
                error = %err,
                "ignored failed ACP session/cancel notification after local cancellation"
            );
        }
    }
}

async fn collect_update(
    update: ExecutorUpdate,
    events: &mut dyn ExecutorEventSink,
    text_parts: &mut ReplyTextParts,
    pending_message_chunks: &mut PendingAcpMessageChunks,
) -> anyhow::Result<()> {
    if PendingAcpMessageChunks::should_buffer(&update) {
        pending_message_chunks.push(update);
        return Ok(());
    }
    if pending_message_chunks.should_flush_as_progress_before(&update) {
        pending_message_chunks.flush_as_progress(events).await?;
    } else if pending_message_chunks.should_flush_as_final_before(&update) {
        pending_message_chunks
            .flush_as_final(events, text_parts)
            .await?;
    }
    collect_ready_update(update, events, text_parts).await
}

async fn collect_ready_update(
    update: ExecutorUpdate,
    events: &mut dyn ExecutorEventSink,
    text_parts: &mut ReplyTextParts,
) -> anyhow::Result<()> {
    if update.kind == "agent_message_chunk" {
        text_parts.push_update(&update);
    }
    events.send(update).await
}

#[derive(Debug, Default)]
struct PendingAcpMessageChunks {
    updates: Vec<ExecutorUpdate>,
}

impl PendingAcpMessageChunks {
    fn should_buffer(update: &ExecutorUpdate) -> bool {
        update.kind == "agent_message_chunk"
            && update.channel_event.is_none()
            && update.reply_message_id.is_none()
    }

    fn push(&mut self, update: ExecutorUpdate) {
        self.updates.push(update);
    }

    fn should_flush_as_progress_before(&self, update: &ExecutorUpdate) -> bool {
        !self.updates.is_empty() && update_starts_activity(update)
    }

    fn should_flush_as_final_before(&self, update: &ExecutorUpdate) -> bool {
        !self.updates.is_empty() && update.kind == "agent_message_chunk"
    }

    async fn flush_as_progress(
        &mut self,
        events: &mut dyn ExecutorEventSink,
    ) -> anyhow::Result<()> {
        let text = self.take_text();
        if text.trim().is_empty() {
            return Ok(());
        }
        events
            .send(
                ExecutorUpdate::new("agent_progress", "", text.clone(), "")
                    .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
            )
            .await
    }

    async fn flush_as_final(
        &mut self,
        events: &mut dyn ExecutorEventSink,
        text_parts: &mut ReplyTextParts,
    ) -> anyhow::Result<()> {
        let updates = std::mem::take(&mut self.updates);
        for update in updates {
            collect_ready_update(update, events, text_parts).await?;
        }
        Ok(())
    }

    fn take_text(&mut self) -> String {
        let updates = std::mem::take(&mut self.updates);
        updates
            .into_iter()
            .map(|update| update.text)
            .collect::<Vec<_>>()
            .join("")
    }
}

fn update_starts_activity(update: &ExecutorUpdate) -> bool {
    if matches!(
        update.channel_event.as_ref().map(|event| event.kind),
        Some(ExecutorChannelEventKind::AgentProgress | ExecutorChannelEventKind::ToolCall)
    ) {
        return true;
    }
    matches!(
        update.kind.as_str(),
        "agent_progress" | "plan" | "tool_call"
    ) || update.kind.starts_with("tool_")
}

#[derive(Debug, Default)]
struct ReplyTextParts {
    parts: Vec<String>,
    last_message_id: Option<String>,
    started: bool,
}

impl ReplyTextParts {
    fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    fn join(&self) -> String {
        self.parts.join("")
    }

    fn push_update(&mut self, update: &ExecutorUpdate) {
        if self.should_break_before(update) {
            let mut text = self.join();
            append_reply_message_break(&mut text);
            self.parts.clear();
            self.parts.push(text);
        }
        self.parts.push(update.text.clone());
        self.observe(update);
    }

    fn should_break_before(&self, update: &ExecutorUpdate) -> bool {
        self.started
            && matches!(
                (&self.last_message_id, &update.reply_message_id),
                (Some(last), Some(next)) if last != next
            )
    }

    fn observe(&mut self, update: &ExecutorUpdate) {
        self.started = true;
        if let Some(id) = &update.reply_message_id {
            self.last_message_id = Some(id.clone());
        }
    }
}

#[derive(Debug)]
struct JsonRpcClient {
    stdin: SharedStdin,
    state: SharedJsonRpcState,
    next_id: AtomicU64,
    updates: broadcast::Sender<ExecutorUpdate>,
    cancel_barrier: SharedAcpCancelBarrier,
    child: Arc<Mutex<Child>>,
    active_user_id: Arc<Mutex<Option<String>>>,
    active_turn_cancel: Arc<Mutex<Option<TurnCancellation>>>,
}

#[derive(Debug, Default)]
struct JsonRpcState {
    closed: bool,
    closed_reason: Option<String>,
    pending: HashMap<u64, oneshot::Sender<anyhow::Result<Value>>>,
    cancel_barrier_response_id: Option<u64>,
}

#[derive(Debug)]
struct PendingJsonRpcRequest {
    id: u64,
    method: String,
    response: oneshot::Receiver<anyhow::Result<Value>>,
}

#[derive(Debug)]
struct AcpLifecycleResponse {
    result: Value,
    cancelled: bool,
}

#[derive(Debug, Clone)]
struct JsonRpcServerContext {
    state: SharedJsonRpcState,
    tool_calls: SharedAcpToolCalls,
    stdin: SharedStdin,
    updates: broadcast::Sender<ExecutorUpdate>,
    cancel_barrier: SharedAcpCancelBarrier,
    approvals: SharedApprovalBroker,
    session_key: String,
    executor: String,
    active_user_id: Arc<Mutex<Option<String>>>,
    active_turn_cancel: Arc<Mutex<Option<TurnCancellation>>>,
}

impl JsonRpcClient {
    async fn spawn(
        stdio: &StdioCommand,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
    ) -> anyhow::Result<Self> {
        tracing::info!(
            target: "agent_router::acp",
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            arg_count = stdio.args.len(),
            cwd = %stdio.executor_cwd,
            "spawning ACP process"
        );
        let mut child = stdio.spawn().map_err(|err| {
            anyhow::anyhow!("could not start ACP command `{}`: {err}", stdio.program)
        })?;
        let pid = child.id();
        tracing::info!(
            target: "agent_router::acp",
            executor = %executor,
            session_key = %session_key,
            command = %stdio.program,
            pid = ?pid,
            "spawned ACP process"
        );
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("ACP process did not expose stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("ACP process did not expose stdout"))?;
        let stderr = child.stderr.take();

        let stdin = Arc::new(Mutex::new(stdin));
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let tool_calls = Arc::new(Mutex::new(HashMap::new()));
        let (updates, _) = broadcast::channel(256);
        let cancel_barrier = Arc::new(AcpCancelBarrier::default());
        let child = Arc::new(Mutex::new(child));
        let active_user_id = Arc::new(Mutex::new(None));
        let active_turn_cancel = Arc::new(Mutex::new(None));
        let server_context = JsonRpcServerContext {
            state: state.clone(),
            tool_calls,
            stdin: stdin.clone(),
            updates: updates.clone(),
            cancel_barrier: cancel_barrier.clone(),
            approvals,
            session_key,
            executor,
            active_user_id: active_user_id.clone(),
            active_turn_cancel: active_turn_cancel.clone(),
        };

        tokio::spawn(read_stdout(
            BufReader::new(stdout),
            server_context,
            stdio.strict_json_stdout,
        ));
        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "agent_router::acp", stderr = %line);
                }
            });
        }

        Ok(Self {
            stdin,
            state,
            next_id: AtomicU64::new(1),
            updates,
            cancel_barrier,
            child,
            active_user_id,
            active_turn_cancel,
        })
    }

    fn handle(&self) -> JsonRpcClientHandle {
        JsonRpcClientHandle {
            stdin: self.stdin.clone(),
        }
    }

    async fn set_active_user(&self, user_id: Option<String>) {
        *self.active_user_id.lock().await = user_id;
    }

    async fn clear_active_user(&self) {
        *self.active_user_id.lock().await = None;
    }

    async fn set_active_turn_cancel(&self, cancel: TurnCancellation) {
        *self.active_turn_cancel.lock().await = Some(cancel);
    }

    async fn clear_active_turn_cancel(&self) {
        *self.active_turn_cancel.lock().await = None;
    }

    fn subscribe(&self) -> broadcast::Receiver<ExecutorUpdate> {
        self.updates.subscribe()
    }

    async fn begin_cancel_barrier_if_pending(&self, id: u64) -> bool {
        begin_cancel_barrier_if_pending(&self.state, &self.cancel_barrier, id).await
    }

    async fn wait_for_cancel_barrier(&self) -> anyhow::Result<()> {
        wait_for_cancel_barrier(&self.state, &self.cancel_barrier, || async {
            self.close("ACP cancelled prompt response did not settle")
                .await;
        })
        .await
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

    async fn request_started(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            ensure_stdout_open(&state)?;
            state.pending.insert(id, tx);
        }
        if let Err(err) = write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }),
        )
        .await
        {
            self.state.lock().await.pending.remove(&id);
            return Err(err);
        }
        Ok(PendingJsonRpcRequest {
            id,
            method: method.to_string(),
            response: rx,
        })
    }

    async fn prompt_request_started(
        &self,
        session_id: &str,
        prompt: &str,
        active_prompts: &SharedActiveAcpPrompts,
        active_key: SessionKey,
        generation: u64,
    ) -> anyhow::Result<PendingJsonRpcRequest> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            ensure_stdout_open(&state)?;
            state.pending.insert(id, tx);
        }
        set_active_acp_prompt(
            active_prompts,
            active_key.clone(),
            ActiveAcpPrompt {
                session_id: session_id.to_string(),
                request_id: id,
                generation,
                client: self.handle(),
            },
        )
        .await;

        if let Err(err) = write_json(
            &self.stdin,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": prompt}],
                },
            }),
        )
        .await
        {
            self.state.lock().await.pending.remove(&id);
            clear_active_acp_prompt(active_prompts, &active_key, id).await;
            return Err(err);
        }
        Ok(PendingJsonRpcRequest {
            id,
            method: "session/prompt".to_string(),
            response: rx,
        })
    }

    async fn lifecycle_request_until_cancelled(
        &self,
        method: &str,
        params: Value,
        cancel: TurnCancellation,
    ) -> anyhow::Result<AcpLifecycleResponse> {
        self.set_active_turn_cancel(cancel.clone()).await;
        let result = self
            .lifecycle_request_until_cancelled_inner(method, params, cancel)
            .await;
        self.clear_active_turn_cancel().await;
        result
    }

    async fn lifecycle_request_until_cancelled_inner(
        &self,
        method: &str,
        params: Value,
        cancel: TurnCancellation,
    ) -> anyhow::Result<AcpLifecycleResponse> {
        let request = self.request_started(method, params).await?;
        let PendingJsonRpcRequest {
            method, response, ..
        } = request;
        let mut response = response;
        let mut cancelled = false;
        let settle_timeout = sleep(ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT);
        tokio::pin!(settle_timeout);
        loop {
            tokio::select! {
                received = &mut response => {
                    let response = received
                        .map_err(|_| anyhow::anyhow!("ACP response channel closed"))??;
                    if let Some(error) = response.get("error") {
                        anyhow::bail!(
                            "ACP `{}` failed: {}",
                            method,
                            summarize_json_rpc_error(error)
                        );
                    }
                    return Ok(AcpLifecycleResponse {
                        result: response.get("result").cloned().unwrap_or(Value::Null),
                        cancelled,
                    });
                }
                _ = cancel.cancelled(), if !cancelled => {
                    cancelled = true;
                    settle_timeout
                        .as_mut()
                        .reset(Instant::now() + ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT);
                }
                _ = &mut settle_timeout, if cancelled => {
                    self.close("ACP lifecycle request did not settle after cancellation").await;
                    anyhow::bail!(
                        "ACP `{method}` did not settle within {}s after cancellation",
                        ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT.as_secs()
                    );
                }
            }
        }
    }

    async fn notify(&self, method: &str, params: Value) -> anyhow::Result<()> {
        self.handle().notify(method, params).await
    }

    async fn close(&self, reason: &str) {
        fail_all_pending(&self.state, &self.cancel_barrier, reason).await;
        let mut child = self.child.lock().await;
        let pid = child.id();
        tracing::warn!(
            target: "agent_router::acp",
            pid = ?pid,
            reason,
            "closing ACP process"
        );
        if let Err(err) = child.start_kill() {
            tracing::debug!(
                target: "agent_router::acp",
                error = %err,
                reason,
                "failed to kill ACP process"
            );
        }
    }
}

async fn read_stdout<R>(
    reader: BufReader<R>,
    context: JsonRpcServerContext,
    strict_json_stdout: bool,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            if strict_json_stdout {
                let reason =
                    "ACP process emitted non-JSON stdout before protocol handshake".to_string();
                tracing::warn!(
                    target: "agent_router::acp",
                    executor = %context.executor,
                    session_key = %context.session_key,
                    raw_stdout = %line,
                    "closing ACP client after non-JSON stdout"
                );
                fail_all_pending(&context.state, &context.cancel_barrier, &reason).await;
                return;
            }
            tracing::debug!(target: "agent_router::acp", raw_stdout = %line, "ignoring non-json ACP stdout");
            continue;
        };
        if strict_json_stdout && !is_json_rpc_like(&message) {
            let reason = "ACP process emitted non-protocol JSON stdout before protocol handshake"
                .to_string();
            tracing::warn!(
                target: "agent_router::acp",
                executor = %context.executor,
                session_key = %context.session_key,
                raw_stdout = %line,
                "closing ACP client after non-protocol JSON stdout"
            );
            fail_all_pending(&context.state, &context.cancel_barrier, &reason).await;
            return;
        }
        dispatch_message(message, &context).await;
    }
    tracing::warn!(
        target: "agent_router::acp",
        executor = %context.executor,
        session_key = %context.session_key,
        "ACP process stdout closed"
    );
    fail_all_pending(
        &context.state,
        &context.cancel_barrier,
        "ACP process closed stdout",
    )
    .await;
}

async fn fail_all_pending(
    state: &SharedJsonRpcState,
    cancel_barrier: &SharedAcpCancelBarrier,
    message: &str,
) {
    let (drained, cleared_barrier) = {
        let mut guard = state.lock().await;
        guard.closed = true;
        guard.closed_reason = Some(message.to_string());
        let cleared_barrier = guard.cancel_barrier_response_id.take().is_some();
        (guard.pending.drain().collect::<Vec<_>>(), cleared_barrier)
    };
    if cleared_barrier {
        cancel_barrier.notify_changed();
    }
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
                .unwrap_or("ACP client stdout is closed")
        );
    }
    Ok(())
}

fn is_json_rpc_like(message: &Value) -> bool {
    let Some(map) = message.as_object() else {
        return false;
    };
    map.get("method")
        .and_then(Value::as_str)
        .is_some_and(is_acp_protocol_method)
        || (map.contains_key("id") && (map.contains_key("result") || map.contains_key("error")))
}

fn is_acp_protocol_method(method: &str) -> bool {
    matches!(method, "session/update" | "session/request_permission")
}

async fn dispatch_message(message: Value, context: &JsonRpcServerContext) {
    if message.get("method").is_none() {
        let response_id = message.get("id").and_then(Value::as_u64);
        let (sender, cleared_barrier) = match response_id {
            Some(id) => {
                response_sender_and_clear_barrier(&context.state, &context.cancel_barrier, id).await
            }
            None => (None, false),
        };
        if cleared_barrier {
            tracing::debug!(
                target: "agent_router::acp",
                executor = %context.executor,
                session_key = %context.session_key,
                response_id,
                "cleared ACP cancelled prompt barrier"
            );
        }
        if let Some(tx) = sender {
            let _ = tx.send(Ok(message));
        }
        return;
    }

    if message.get("id").is_some() {
        if let Some(request_id) = active_cancel_barrier_id(&context.state).await {
            respond_to_cancel_barrier_server_request(context, &message, request_id).await;
            return;
        }
        respond_to_server_request(context, &message).await;
        return;
    }

    if message.get("method").and_then(Value::as_str) == Some("session/update") {
        if let Some(request_id) = active_cancel_barrier_id(&context.state).await {
            tracing::debug!(
                target: "agent_router::acp",
                executor = %context.executor,
                session_key = %context.session_key,
                suppressed_until_response_id = request_id,
                "dropping ACP session/update while cancelled prompt response is pending"
            );
            return;
        }
        let update = {
            let mut tool_calls = context.tool_calls.lock().await;
            project_acp_update_with_state(&message, &mut tool_calls)
        };
        if let Some(update) = update {
            let _ = context.updates.send(update);
        }
    }
}

async fn begin_cancel_barrier_if_pending(
    state: &SharedJsonRpcState,
    cancel_barrier: &SharedAcpCancelBarrier,
    response_id: u64,
) -> bool {
    let mut guard = state.lock().await;
    if guard.pending.remove(&response_id).is_none() {
        return false;
    }
    guard.cancel_barrier_response_id = Some(response_id);
    cancel_barrier.notify_changed();
    true
}

async fn response_sender_and_clear_barrier(
    state: &SharedJsonRpcState,
    cancel_barrier: &SharedAcpCancelBarrier,
    response_id: u64,
) -> (Option<oneshot::Sender<anyhow::Result<Value>>>, bool) {
    let (sender, cleared_barrier) = {
        let mut guard = state.lock().await;
        let cleared_barrier = if guard.cancel_barrier_response_id == Some(response_id) {
            guard.cancel_barrier_response_id = None;
            true
        } else {
            false
        };
        (guard.pending.remove(&response_id), cleared_barrier)
    };
    if cleared_barrier {
        cancel_barrier.notify_changed();
    }
    (sender, cleared_barrier)
}

async fn active_cancel_barrier_id(state: &SharedJsonRpcState) -> Option<u64> {
    state.lock().await.cancel_barrier_response_id
}

async fn wait_for_cancel_barrier<F, Fut>(
    state: &SharedJsonRpcState,
    cancel_barrier: &SharedAcpCancelBarrier,
    on_timeout: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = ()>,
{
    let timeout = sleep(ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT);
    tokio::pin!(timeout);
    let mut on_timeout = Some(on_timeout);
    loop {
        let changed = cancel_barrier.changed.notified();
        if state.lock().await.cancel_barrier_response_id.is_none() {
            return Ok(());
        }
        tokio::select! {
            _ = changed => {}
            _ = &mut timeout => {
                if active_cancel_barrier_id(state).await.is_none() {
                    return Ok(());
                }
                if let Some(on_timeout) = on_timeout.take() {
                    on_timeout().await;
                }
                anyhow::bail!(
                    "ACP cancelled prompt response did not settle within {}s",
                    ACP_CANCELLED_LIFECYCLE_SETTLE_TIMEOUT.as_secs()
                );
            }
        }
    }
}

async fn respond_to_cancel_barrier_server_request(
    context: &JsonRpcServerContext,
    message: &Value,
    request_id: u64,
) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    tracing::debug!(
        target: "agent_router::acp",
        executor = %context.executor,
        session_key = %context.session_key,
        suppressed_until_response_id = request_id,
        method,
        "responding to ACP server request from cancelled prompt barrier"
    );
    let payload = if method == "session/request_permission" {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": permission_result(ApprovalSelection::Cancelled),
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("agent-router ignored `{method}` from a cancelled ACP prompt")
            }
        })
    };
    let _ = write_json(&context.stdin, payload).await;
}

async fn respond_to_server_request(context: &JsonRpcServerContext, message: &Value) {
    let id = message.get("id").cloned().unwrap_or(Value::Null);
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let payload = if method == "session/request_permission" {
        let requester_user_id = context.active_user_id.lock().await.clone();
        let request = approval_request_from_permission_message(
            message,
            &context.session_key,
            &context.executor,
            requester_user_id,
        );
        let turn_cancel = context.active_turn_cancel.lock().await.clone();
        let selection = request_permission_until_turn_cancelled(
            context.approvals.clone(),
            request,
            turn_cancel,
        )
        .await;
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": permission_result(selection),
        })
    } else {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32601,
                "message": format!("agent-router MVP does not support ACP client method `{method}`")
            }
        })
    };
    let _ = write_json(&context.stdin, payload).await;
}

async fn request_permission_until_turn_cancelled(
    approvals: SharedApprovalBroker,
    request: ApprovalRequest,
    turn_cancel: Option<TurnCancellation>,
) -> ApprovalSelection {
    let Some(turn_cancel) = turn_cancel else {
        return approvals.request(request).await;
    };
    if turn_cancel.is_cancelled().await {
        return ApprovalSelection::Cancelled;
    }
    let approval_cancel = ApprovalCancellation::new();
    let approval_cancel_for_turn = approval_cancel.clone();
    let watcher = tokio::spawn(async move {
        let _ = turn_cancel.cancelled().await;
        approval_cancel_for_turn.cancel();
    });
    let selection = approvals
        .request_until_cancelled(request, approval_cancel)
        .await
        .unwrap_or(ApprovalSelection::Cancelled);
    watcher.abort();
    selection
}

fn approval_request_from_permission_message(
    message: &Value,
    session_key: &str,
    executor: &str,
    requester_user_id: Option<String>,
) -> ApprovalRequest {
    let params = message.get("params").unwrap_or(&Value::Null);
    let tool_call = params
        .get("toolCall")
        .or_else(|| params.get("tool_call"))
        .unwrap_or(params);
    let title = tool_call
        .get("title")
        .or_else(|| tool_call.get("name"))
        .or_else(|| tool_call.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("Tool permission")
        .to_string();
    ApprovalRequest {
        session_key: session_key.to_string(),
        executor: executor.to_string(),
        requester_user_id,
        title,
        body: permission_body(params, tool_call),
        options: permission_options(params),
    }
}

fn permission_body(params: &Value, tool_call: &Value) -> String {
    let content = extract_text(tool_call.get("content"))
        .or_else(|| extract_text(tool_call.get("rawInput")))
        .or_else(|| extract_text(tool_call.get("raw_input")))
        .or_else(|| extract_text(params.get("toolCall")))
        .or_else(|| extract_text(params.get("tool_call")))
        .unwrap_or_default();
    truncate_text(content, 2_000)
}

fn permission_options(params: &Value) -> Vec<ApprovalOption> {
    params
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let id = item
                        .get("optionId")
                        .or_else(|| item.get("option_id"))
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str)?;
                    let kind = item.get("kind").and_then(Value::as_str).unwrap_or_default();
                    let name = item
                        .get("name")
                        .or_else(|| item.get("label"))
                        .and_then(Value::as_str)
                        .unwrap_or(id);
                    Some(ApprovalOption {
                        id: id.to_string(),
                        kind: kind.to_string(),
                        name: name.to_string(),
                        auto_approvable: true,
                    })
                })
                .collect::<Vec<_>>()
        })
        .filter(|options| !options.is_empty())
        .unwrap_or_else(|| {
            vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: false,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ]
        })
}

fn permission_result(selection: ApprovalSelection) -> Value {
    match selection {
        ApprovalSelection::Selected(option_id) => json!({
            "outcome": {
                "outcome": "selected",
                "optionId": option_id,
            }
        }),
        ApprovalSelection::Cancelled => json!({
            "outcome": {
                "outcome": "cancelled",
            }
        }),
    }
}

fn truncate_text(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

async fn write_json(stdin: &SharedStdin, value: Value) -> anyhow::Result<()> {
    let mut guard = stdin.lock().await;
    let line = serde_json::to_string(&value)?;
    guard.write_all(line.as_bytes()).await?;
    guard.write_all(b"\n").await?;
    guard.flush().await?;
    Ok(())
}

#[cfg(test)]
fn project_acp_update(message: &Value) -> Option<ExecutorUpdate> {
    let mut tool_calls = HashMap::new();
    project_acp_update_with_state(message, &mut tool_calls)
}

fn project_acp_update_with_state(
    message: &Value,
    tool_calls: &mut HashMap<String, AcpToolCallState>,
) -> Option<ExecutorUpdate> {
    let params = message.get("params")?;
    let raw_update = params
        .get("update")
        .or_else(|| params.get("sessionUpdate"))
        .unwrap_or(params);
    let update = acp_logical_update(raw_update);
    let reply_message_id = acp_reply_message_id(raw_update, update);
    let tool_call_id = tool_call_id(update);
    let kind = update
        .get("sessionUpdate")
        .or_else(|| update.get("kind"))
        .or_else(|| update.get("type"))
        .or_else(|| update.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("update");
    let lower_kind = kind.to_ascii_lowercase();
    let is_tool_update = lower_kind.contains("tool");
    let tool = if is_tool_update {
        update
            .get("toolCall")
            .or_else(|| update.get("tool_call"))
            .unwrap_or(update)
    } else {
        update
    };
    let text = if is_tool_update {
        extract_tool_raw_input(update.get("rawInput"))
            .or_else(|| extract_tool_raw_input(update.get("raw_input")))
            .or_else(|| extract_tool_raw_input(tool.get("rawInput")))
            .or_else(|| extract_tool_raw_input(tool.get("raw_input")))
    } else {
        acp_update_text(update)
    };
    let title = update
        .get("title")
        .or_else(|| update.get("name"))
        .or_else(|| tool.get("title"))
        .or_else(|| tool.get("name"))
        .or_else(|| tool.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let status = update
        .get("status")
        .or_else(|| tool.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let message_phase = acp_message_phase(update);
    let is_agent_message = is_acp_agent_message_kind(kind, update);
    let normalized_kind = if is_agent_message && message_phase == Some("commentary") {
        "agent_progress".to_string()
    } else if is_agent_message {
        "agent_message_chunk".to_string()
    } else if matches!(kind, "agent_thought_chunk" | "agent_thought") {
        "agent_thought_chunk".to_string()
    } else if is_tool_update {
        format!("tool_{kind}")
    } else if lower_kind.contains("plan") {
        "plan".to_string()
    } else if lower_kind.contains("diff") || lower_kind.contains("file") {
        "diff".to_string()
    } else if lower_kind.contains("error") {
        "error".to_string()
    } else {
        kind.to_string()
    };
    let tool_activity_input =
        is_tool_update && text.as_ref().is_some_and(|text| !text.trim().is_empty());
    let text = text.unwrap_or_default();
    let (title, text, status, emit_tool_activity) = if is_tool_update {
        merge_acp_tool_update(
            tool_calls,
            tool_call_id,
            title,
            text,
            status,
            tool_activity_input,
        )
    } else {
        (title, text, status, false)
    };
    let plan_summary = if normalized_kind == "plan" {
        acp_plan_summary(text.as_str(), update.get("entries"))
    } else {
        None
    };
    let is_agent_progress = normalized_kind == "agent_progress";
    let is_final_agent_message = !is_agent_progress && normalized_kind == "agent_message_chunk";
    let mut update =
        ExecutorUpdate::new(normalized_kind, title.clone(), text.clone(), status.clone());
    if is_final_agent_message && let Some(id) = reply_message_id {
        update = update.with_reply_message_id(id);
    }
    if is_agent_progress && !text.trim().is_empty() {
        update = update.with_channel_event(ExecutorChannelEvent::agent_progress(text.clone()));
    } else if let Some(summary) = plan_summary {
        update = update.with_channel_event(ExecutorChannelEvent::agent_progress(summary));
    } else if emit_tool_activity {
        update = update.with_channel_event(ExecutorChannelEvent::tool_call(
            acp_tool_title(&title),
            acp_tool_channel_summary(&title, &text, &status),
        ));
    }
    Some(update)
}

fn acp_logical_update(update: &Value) -> &Value {
    let wrapper_type = update.get("type").and_then(Value::as_str);
    if matches!(wrapper_type, Some("event_msg" | "response_item"))
        && let Some(payload) = update.get("payload").filter(|value| value.is_object())
    {
        return payload;
    }
    update
}

fn acp_reply_message_id(raw_update: &Value, update: &Value) -> Option<String> {
    raw_update
        .get("payload")
        .and_then(reply_message_id_field)
        .or_else(|| reply_message_id_field(update))
        .or_else(|| reply_message_id_field(raw_update))
}

fn reply_message_id_field(value: &Value) -> Option<String> {
    ["messageId", "message_id", "itemId", "item_id", "id"]
        .into_iter()
        .find_map(|field| {
            value
                .get(field)
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn is_acp_agent_message_kind(kind: &str, update: &Value) -> bool {
    let lower_kind = kind.to_ascii_lowercase();
    matches!(
        lower_kind.as_str(),
        "agent_message_chunk" | "agent_message" | "agent_message_delta"
    ) || matches!(lower_kind.as_str(), "agentmessage" | "agentmessagechunk")
        || (lower_kind == "message"
            && update.get("role").and_then(Value::as_str) == Some("assistant"))
}

fn acp_update_text(update: &Value) -> Option<String> {
    extract_text(update.get("content"))
        .or_else(|| extract_text(update.get("text")))
        .or_else(|| extract_text(update.get("message")))
}

fn acp_message_phase(update: &Value) -> Option<&str> {
    fn direct_phase(value: &Value) -> Option<&str> {
        value
            .get("phase")
            .or_else(|| value.get("messagePhase"))
            .or_else(|| value.get("message_phase"))
            .and_then(Value::as_str)
            .filter(|phase| matches!(*phase, "commentary" | "final_answer"))
    }

    direct_phase(update)
        .or_else(|| update.get("content").and_then(direct_phase))
        .or_else(|| {
            update
                .get("_meta")
                .and_then(|meta| meta.get("codex"))
                .and_then(direct_phase)
        })
}

fn acp_plan_summary(text: &str, entries: Option<&Value>) -> Option<String> {
    let text = text.trim();
    if !text.is_empty() {
        return Some(truncate_text(text.to_string(), 1_000));
    }
    let entries = entries?.as_array()?;
    let mut lines = Vec::new();
    for entry in entries.iter().take(6) {
        let content = entry
            .get("content")
            .or_else(|| entry.get("text"))
            .or_else(|| entry.get("step"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if content.is_empty() {
            continue;
        }
        let status = entry
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if status.is_empty() {
            lines.push(format!("- {content}"));
        } else {
            lines.push(format!("- [{status}] {content}"));
        }
    }
    let remaining = entries.len().saturating_sub(6);
    if remaining > 0 {
        lines.push(format!("- {remaining} more"));
    }
    (!lines.is_empty()).then(|| truncate_text(lines.join("\n"), 1_000))
}

fn tool_call_id(update: &Value) -> Option<&str> {
    update
        .get("toolCallId")
        .or_else(|| update.get("tool_call_id"))
        .or_else(|| {
            update
                .get("toolCall")
                .and_then(|tool| tool.get("toolCallId"))
        })
        .or_else(|| {
            update
                .get("tool_call")
                .and_then(|tool| tool.get("tool_call_id"))
        })
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
}

fn merge_acp_tool_update(
    tool_calls: &mut HashMap<String, AcpToolCallState>,
    tool_call_id: Option<&str>,
    title: String,
    text: String,
    status: String,
    tool_activity_input: bool,
) -> (String, String, String, bool) {
    let emit_without_state = tool_activity_input && !text.trim().is_empty();
    let Some(tool_call_id) = tool_call_id else {
        return (title, text, status, emit_without_state);
    };

    let state = tool_calls.entry(tool_call_id.to_string()).or_default();
    let emit_tool_activity = emit_without_state && !state.activity_emitted;
    if !title.trim().is_empty() {
        state.title = title;
    }
    if !text.trim().is_empty() {
        state.text = text;
    }
    if !status.trim().is_empty() {
        state.status = status;
    }
    if emit_tool_activity {
        state.activity_emitted = true;
    }

    let merged = (
        state.title.clone(),
        state.text.clone(),
        state.status.clone(),
    );
    if is_terminal_tool_status(&merged.2) {
        tool_calls.remove(tool_call_id);
    }
    (merged.0, merged.1, merged.2, emit_tool_activity)
}

fn is_terminal_tool_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "completed" | "failed" | "cancelled" | "canceled"
    )
}

fn acp_tool_title(title: &str) -> String {
    let title = title.trim();
    if title.is_empty() {
        "External agent tool".to_string()
    } else {
        title.to_string()
    }
}

fn acp_tool_channel_summary(title: &str, text: &str, status: &str) -> String {
    let mut lines = Vec::new();
    let text = text.trim();
    if !text.is_empty() {
        lines.push(text.to_string());
    }
    let status = status.trim();
    if !status.is_empty() {
        lines.push(format!("status: {status}"));
    }
    if lines.is_empty() {
        lines.push(acp_tool_title(title));
    }
    truncate_text(lines.join("\n"), 1_000)
}

fn extract_tool_raw_input(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => nonempty_one_line(text),
        Value::Object(map) => {
            for key in ["command", "cmd", "script"] {
                if let Some(command) = map.get(key).and_then(Value::as_str) {
                    return nonempty_one_line(command).map(|command| format!("$ {command}"));
                }
            }
            for key in ["path", "file", "query", "pattern"] {
                if let Some(value) = map.get(key).and_then(Value::as_str)
                    && let Some(value) = nonempty_one_line(value)
                {
                    return Some(value);
                }
            }
            None
        }
        _ => extract_text(value),
    }
}

fn nonempty_one_line(text: &str) -> Option<String> {
    let line = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!line.is_empty()).then_some(line)
}

fn extract_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("message"))
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

fn session_id_from_result(result: &Value) -> Option<String> {
    result
        .get("sessionId")
        .or_else(|| result.get("session_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn extract_text_result(result: &Value) -> String {
    ["text", "message", "content"]
        .iter()
        .find_map(|key| extract_text(result.get(*key)))
        .unwrap_or_default()
}

fn acp_result_cancelled(result: &Value) -> bool {
    ["stopReason", "stop_reason", "reason"]
        .iter()
        .filter_map(|key| result.get(*key).and_then(Value::as_str))
        .any(|reason| {
            let reason = reason.to_ascii_lowercase();
            reason == "cancelled" || reason == "canceled"
        })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use crate::approval::ApprovalBroker;
    use crate::executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorInterruptRequest,
        ExecutorPrepareRequest, ExecutorPromptRequest, ExecutorTurnRef, InterruptReason,
        test_support::CollectingExecutorEventSink,
    };

    use super::*;

    fn stdio_command(program: &str, args: &[&str]) -> StdioCommand {
        StdioCommand {
            program: program.to_string(),
            args: args.iter().map(|arg| arg.to_string()).collect(),
            current_dir: None,
            env: BTreeMap::new(),
            env_remove: Vec::new(),
            executor_cwd: "/tmp".to_string(),
            strict_json_stdout: true,
        }
    }

    fn turn_ref(session_key: &str, executor: &str, generation: u64) -> ExecutorTurnRef {
        ExecutorTurnRef {
            session_key: session_key.to_string(),
            executor: executor.to_string(),
            generation,
        }
    }

    #[tokio::test]
    async fn acp_cancel_barrier_does_not_begin_after_response_is_dispatched() {
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let barrier = Arc::new(AcpCancelBarrier::default());
        let (tx, _rx) = tokio::sync::oneshot::channel::<anyhow::Result<Value>>();
        state.lock().await.pending.insert(7, tx);

        let (sender, cleared_barrier) =
            response_sender_and_clear_barrier(&state, &barrier, 7).await;
        assert!(sender.is_some());
        assert!(!cleared_barrier);

        assert!(!begin_cancel_barrier_if_pending(&state, &barrier, 7).await);
        assert_eq!(active_cancel_barrier_id(&state).await, None);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                wait_for_cancel_barrier(&state, &barrier, || async {})
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn acp_cancel_barrier_clears_when_cancelled_prompt_response_arrives() {
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let barrier = Arc::new(AcpCancelBarrier::default());
        let (tx, _rx) = tokio::sync::oneshot::channel::<anyhow::Result<Value>>();
        state.lock().await.pending.insert(7, tx);

        assert!(begin_cancel_barrier_if_pending(&state, &barrier, 7).await);
        assert_eq!(active_cancel_barrier_id(&state).await, Some(7));

        let (sender, cleared_barrier) =
            response_sender_and_clear_barrier(&state, &barrier, 7).await;
        assert!(sender.is_none());
        assert!(cleared_barrier);
        assert_eq!(active_cancel_barrier_id(&state).await, None);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                wait_for_cancel_barrier(&state, &barrier, || async {})
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn acp_cancel_barrier_timeout_clears_pending_barrier() {
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let barrier = Arc::new(AcpCancelBarrier::default());
        let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<Value>>();
        state.lock().await.pending.insert(7, tx);

        assert!(begin_cancel_barrier_if_pending(&state, &barrier, 7).await);
        assert_eq!(active_cancel_barrier_id(&state).await, Some(7));

        let result = wait_for_cancel_barrier(&state, &barrier, {
            let state = state.clone();
            let barrier = barrier.clone();
            || async move {
                fail_all_pending(&state, &barrier, "test cancelled prompt timeout").await;
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(active_cancel_barrier_id(&state).await, None);
        if let Ok(response) = rx.await {
            assert!(response.is_err());
        }
    }

    #[tokio::test]
    async fn acp_cancel_barrier_timeout_rechecks_cleared_barrier_before_recovery() {
        let state = Arc::new(Mutex::new(JsonRpcState::default()));
        let barrier = Arc::new(AcpCancelBarrier::default());
        let (tx, _rx) = tokio::sync::oneshot::channel::<anyhow::Result<Value>>();
        state.lock().await.pending.insert(7, tx);

        assert!(begin_cancel_barrier_if_pending(&state, &barrier, 7).await);
        assert_eq!(active_cancel_barrier_id(&state).await, Some(7));

        let timeout_hook_called = Arc::new(AtomicBool::new(false));
        let mut wait_task = tokio::spawn({
            let state = state.clone();
            let barrier = barrier.clone();
            let timeout_hook_called = timeout_hook_called.clone();
            async move {
                wait_for_cancel_barrier(&state, &barrier, || {
                    let timeout_hook_called = timeout_hook_called.clone();
                    async move {
                        timeout_hook_called.store(true, Ordering::SeqCst);
                    }
                })
                .await
            }
        });

        tokio::select! {
            result = &mut wait_task => {
                panic!("barrier wait finished before the test cleared it: {result:?}");
            }
            _ = sleep(Duration::from_millis(50)) => {}
        }

        state.lock().await.cancel_barrier_response_id = None;

        let result = wait_task.await.unwrap();
        assert!(result.is_ok());
        assert!(!timeout_hook_called.load(Ordering::SeqCst));
    }

    #[test]
    fn synthetic_acp_permission_options_are_not_auto_approvable() {
        let message = serde_json::json!({
            "params": {
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                }
            }
        });

        let request = approval_request_from_permission_message(
            &message,
            "session-1",
            "kimi",
            Some("U1".to_string()),
        );

        assert!(
            request
                .options
                .iter()
                .any(|option| option.id == "allow_once")
        );
        assert_eq!(request.allow_once_option_id(), None);
    }

    #[test]
    fn acp_tool_update_projects_channel_event() {
        let update = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call",
                    "title": "Bash",
                    "rawInput": {"command": "cargo test"},
                    "status": "completed"
                }
            }
        }))
        .unwrap();

        assert_eq!(update.kind, "tool_tool_call");
        assert_eq!(update.title, "Bash");
        assert_eq!(update.text, "$ cargo test");
        assert_eq!(update.status, "completed");
        let event = update.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert_eq!(event.title, "Bash");
        assert_eq!(event.text, "$ cargo test\nstatus: completed");

        let nested = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCall": {
                        "name": "read_file",
                        "rawInput": {"path": "src/main.rs"},
                        "status": "failed"
                    }
                }
            }
        }))
        .unwrap();
        let event = nested.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert_eq!(event.title, "read_file");
        assert_eq!(event.text, "src/main.rs\nstatus: failed");
    }

    #[test]
    fn acp_tool_content_only_updates_do_not_project_channel_events() {
        let mut tool_calls = HashMap::new();
        let started = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-1",
                        "title": "Bash",
                        "rawInput": {"command": "sleep 3"},
                        "status": "pending"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert!(started.channel_event.is_some());

        let output = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "content": {"text": "5 times sleep 3 completed"}
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert_eq!(output.title, "Bash");
        assert_eq!(output.text, "$ sleep 3");
        assert_eq!(output.status, "pending");
        assert!(output.channel_event.is_none());

        let titled_output = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "title": "Bash",
                        "status": "running",
                        "content": {"text": "still sleeping"}
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert_eq!(titled_output.title, "Bash");
        assert_eq!(titled_output.text, "$ sleep 3");
        assert_eq!(titled_output.status, "running");
        assert!(titled_output.channel_event.is_none());

        let repeated_input = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "rawInput": {"command": "sleep 3"},
                        "status": "running"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert_eq!(repeated_input.title, "Bash");
        assert_eq!(repeated_input.text, "$ sleep 3");
        assert_eq!(repeated_input.status, "running");
        assert!(repeated_input.channel_event.is_none());

        let json_args = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "content": {
                            "text": "{\"command\":\"sleep 3 && sleep 3\",\"timeout\":60}"
                        }
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert!(json_args.channel_event.is_none());
    }

    #[test]
    fn acp_tool_update_merges_partial_updates_by_id() {
        let mut tool_calls = HashMap::new();
        let started = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-1",
                        "title": "Run tests",
                        "rawInput": {"command": "cargo test -q"},
                        "status": "pending"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        let event = started.channel_event.unwrap();
        assert_eq!(event.title, "Run tests");
        assert_eq!(event.text, "$ cargo test -q\nstatus: pending");

        let failed = project_acp_update_with_state(
            &json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "call-1",
                        "status": "failed"
                    }
                }
            }),
            &mut tool_calls,
        )
        .unwrap();
        assert_eq!(failed.title, "Run tests");
        assert_eq!(failed.text, "$ cargo test -q");
        assert_eq!(failed.status, "failed");
        assert!(failed.channel_event.is_none());
        assert!(tool_calls.is_empty());
    }

    #[test]
    fn acp_legacy_agent_message_remains_final_reply_only() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"text": "final answer"}
                }
            }
        }))
        .unwrap();
        assert_eq!(agent_message.kind, "agent_message_chunk");
        assert!(agent_message.channel_event.is_none());
    }

    #[test]
    fn acp_unphased_agent_message_remains_final_reply_only_by_default() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "final answer"}
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_message_chunk");
        assert_eq!(agent_message.text, "final answer");
        assert!(agent_message.channel_event.is_none());
    }

    #[test]
    fn acp_unphased_agent_message_does_not_project_progress_event() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "I am checking the instruction shape."}
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_message_chunk");
        assert_eq!(agent_message.text, "I am checking the instruction shape.");
        assert!(agent_message.channel_event.is_none());
    }

    #[test]
    fn acp_commentary_agent_message_projects_progress_event() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "phase": "commentary",
                    "content": {"text": "I will inspect the config first."}
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_progress");
        assert_eq!(agent_message.text, "I will inspect the config first.");
        let event = agent_message.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(event.text, "I will inspect the config first.");
    }

    #[test]
    fn acp_codex_event_message_projects_progress_event() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "commentary",
                        "message": "I will inspect the config first."
                    }
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_progress");
        assert_eq!(agent_message.text, "I will inspect the config first.");
        let event = agent_message.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(event.text, "I will inspect the config first.");
    }

    #[test]
    fn acp_codex_response_message_projects_progress_event() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "assistant",
                        "phase": "commentary",
                        "content": [{"type": "output_text", "text": "I will inspect the config first."}]
                    }
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_progress");
        assert_eq!(agent_message.text, "I will inspect the config first.");
        let event = agent_message.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(event.text, "I will inspect the config first.");
    }

    #[test]
    fn acp_final_answer_phase_remains_final_reply_only() {
        let agent_message = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "phase": "final_answer",
                    "content": {"text": "final answer"}
                }
            }
        }))
        .unwrap();

        assert_eq!(agent_message.kind, "agent_message_chunk");
        assert_eq!(agent_message.text, "final answer");
        assert!(agent_message.channel_event.is_none());
    }

    #[test]
    fn acp_plan_update_projects_progress_event() {
        let plan = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "plan",
                    "entries": [
                        {"status": "in_progress", "content": "Inspect ACP events"},
                        {"status": "pending", "content": "Patch router projection"}
                    ]
                }
            }
        }))
        .unwrap();

        assert_eq!(plan.kind, "plan");
        let event = plan.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(
            event.text,
            "- [in_progress] Inspect ACP events\n- [pending] Patch router projection"
        );
    }

    #[tokio::test]
    async fn acp_commentary_agent_message_does_not_enter_final_text() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();
        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "phase": "commentary",
                        "content": {"text": "I will inspect the config first."}
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();
        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "phase": "final_answer",
                        "content": {"text": "done"}
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();
        pending_message_chunks
            .flush_as_final(&mut events, &mut text_parts)
            .await
            .unwrap();

        assert_eq!(text_parts.parts, ["done"]);
        assert_eq!(events.updates.len(), 2);
        assert!(events.updates[0].channel_event.is_some());
    }

    #[tokio::test]
    async fn acp_unphased_chunks_flush_to_final_text_at_turn_end() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();

        for chunk in ["I", " am", " checking"] {
            collect_update(
                project_acp_update(&json!({
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": chunk}
                        }
                    }
                }))
                .unwrap(),
                &mut events,
                &mut text_parts,
                &mut pending_message_chunks,
            )
            .await
            .unwrap();
        }

        assert!(text_parts.parts.is_empty());
        assert!(events.updates.is_empty());
        pending_message_chunks
            .flush_as_final(&mut events, &mut text_parts)
            .await
            .unwrap();

        assert_eq!(text_parts.parts, ["I", " am", " checking"]);
        assert_eq!(events.updates.len(), 3);
        assert!(
            events
                .updates
                .iter()
                .all(|update| update.channel_event.is_none())
        );
    }

    #[tokio::test]
    async fn acp_unphased_chunks_before_tool_flush_as_progress() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();

        for chunk in ["I will", " inspect", " the config."] {
            collect_update(
                project_acp_update(&json!({
                    "method": "session/update",
                    "params": {
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": chunk}
                        }
                    }
                }))
                .unwrap(),
                &mut events,
                &mut text_parts,
                &mut pending_message_chunks,
            )
            .await
            .unwrap();
        }

        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-1",
                        "title": "Bash",
                        "rawInput": {"command": "cargo test"},
                        "status": "pending"
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();

        assert!(text_parts.parts.is_empty());
        assert_eq!(events.updates.len(), 2);
        let progress = events.updates[0].channel_event.as_ref().unwrap();
        assert_eq!(progress.kind, ExecutorChannelEventKind::AgentProgress);
        assert_eq!(progress.text, "I will inspect the config.");
        let tool = events.updates[1].channel_event.as_ref().unwrap();
        assert_eq!(tool.kind, ExecutorChannelEventKind::ToolCall);
    }

    #[tokio::test]
    async fn acp_distinct_reply_message_ids_separate_final_text() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();

        for (id, text) in [("msg-1", "first"), ("msg-2", "second")] {
            collect_update(
                project_acp_update(&json!({
                    "method": "session/update",
                    "params": {
                        "update": {
                            "id": id,
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": text}
                        }
                    }
                }))
                .unwrap(),
                &mut events,
                &mut text_parts,
                &mut pending_message_chunks,
            )
            .await
            .unwrap();
        }

        assert_eq!(text_parts.join(), "first\n\nsecond");
        assert_eq!(events.updates[0].reply_message_id.as_deref(), Some("msg-1"));
        assert_eq!(events.updates[1].reply_message_id.as_deref(), Some("msg-2"));
    }

    #[tokio::test]
    async fn acp_pending_unphased_chunks_flush_before_identified_final_chunk() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();

        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "hello "}
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();
        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "id": "msg-1",
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "world"}
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();

        assert_eq!(text_parts.join(), "hello world");
        assert_eq!(events.updates.len(), 2);
        assert_eq!(events.updates[0].reply_message_id, None);
        assert_eq!(events.updates[1].reply_message_id.as_deref(), Some("msg-1"));
    }

    #[tokio::test]
    async fn acp_payload_message_id_beats_wrapper_event_id() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();

        for (event_id, text) in [("event-1", "first"), ("event-2", " chunk")] {
            collect_update(
                project_acp_update(&json!({
                    "method": "session/update",
                    "params": {
                        "update": {
                            "id": event_id,
                            "type": "response_item",
                            "payload": {
                                "messageId": "msg-1",
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": text}]
                            }
                        }
                    }
                }))
                .unwrap(),
                &mut events,
                &mut text_parts,
                &mut pending_message_chunks,
            )
            .await
            .unwrap();
        }

        assert_eq!(text_parts.join(), "first chunk");
        assert_eq!(events.updates[0].reply_message_id.as_deref(), Some("msg-1"));
        assert_eq!(events.updates[1].reply_message_id.as_deref(), Some("msg-1"));
    }

    #[tokio::test]
    async fn acp_codex_event_commentary_does_not_enter_final_text() {
        let mut events = CollectingExecutorEventSink::default();
        let mut text_parts = ReplyTextParts::default();
        let mut pending_message_chunks = PendingAcpMessageChunks::default();
        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "type": "event_msg",
                        "payload": {
                            "type": "agent_message",
                            "phase": "commentary",
                            "message": "I will inspect the config first."
                        }
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();
        collect_update(
            project_acp_update(&json!({
                "method": "session/update",
                "params": {
                    "update": {
                        "type": "event_msg",
                        "payload": {
                            "type": "agent_message",
                            "phase": "final_answer",
                            "message": "done"
                        }
                    }
                }
            }))
            .unwrap(),
            &mut events,
            &mut text_parts,
            &mut pending_message_chunks,
        )
        .await
        .unwrap();
        pending_message_chunks
            .flush_as_final(&mut events, &mut text_parts)
            .await
            .unwrap();

        assert_eq!(text_parts.parts, ["done"]);
        assert_eq!(events.updates.len(), 2);
        assert!(events.updates[0].channel_event.is_some());
        assert!(events.updates[1].channel_event.is_none());
    }

    #[test]
    fn acp_thought_update_stays_internal_without_explicit_projection() {
        let thought = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_thought_chunk",
                    "content": {"text": "raw thought"}
                }
            }
        }))
        .unwrap();
        assert_eq!(thought.kind, "agent_thought_chunk");
        assert!(thought.channel_event.is_none());
    }

    #[test]
    fn acp_commentary_phase_does_not_override_non_message_updates() {
        let thought = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "agent_thought_chunk",
                    "phase": "commentary",
                    "content": {"text": "raw thought must stay internal"}
                }
            }
        }))
        .unwrap();
        assert_eq!(thought.kind, "agent_thought_chunk");
        assert!(thought.channel_event.is_none());

        let tool = project_acp_update(&json!({
            "method": "session/update",
            "params": {
                "update": {
                    "sessionUpdate": "tool_call",
                    "title": "Bash",
                    "rawInput": {"command": "cargo test"},
                    "status": "completed",
                    "_meta": {"codex": {"phase": "commentary"}}
                }
            }
        }))
        .unwrap();
        assert_eq!(tool.kind, "tool_tool_call");
        let event = tool.channel_event.unwrap();
        assert_eq!(event.kind, ExecutorChannelEventKind::ToolCall);
        assert_eq!(event.text, "$ cargo test\nstatus: completed");
    }

    #[test]
    fn strict_stdout_accepts_only_json_rpc_shapes() {
        assert!(is_json_rpc_like(&json!({"id": 1, "result": {}})));
        assert!(is_json_rpc_like(&json!({"id": 1, "error": {"code": -1}})));
        assert!(is_json_rpc_like(
            &json!({"method": "session/update", "params": {}})
        ));
        assert!(!is_json_rpc_like(&json!({"id": 1, "message": "startup"})));
        assert!(!is_json_rpc_like(&json!({"method": "startup"})));
        assert!(!is_json_rpc_like(&json!({"hello": "world"})));
        assert!(!is_json_rpc_like(&json!("banner")));
    }

    #[tokio::test]
    async fn acp_prepare_prefers_session_cwd_over_executor_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let executor_cwd = tmp.path().join("executor-cwd");
        let session_cwd = tmp.path().join("session-cwd");
        fs::create_dir_all(&executor_cwd).unwrap();
        fs::create_dir_all(&session_cwd).unwrap();
        let script = tmp.path().join("fake_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys
import time

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(executor_cwd),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);

        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: Some(session_cwd.clone()),
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let session = manager
            .session_manager
            .existing_session("session-1", "kimi")
            .await
            .unwrap();
        assert_eq!(
            session.lock().await.cwd,
            session_cwd.canonicalize().unwrap()
        );
    }

    #[tokio::test]
    async fn acp_cancelled_prepare_after_publication_keeps_session_reusable() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("prepare_cancel_acp.py");
        let initialize_marker = tmp.path().join("initialize_started");
        let initialize_gate = tmp.path().join("allow_initialize");
        let initialize_marker_literal =
            serde_json::to_string(&initialize_marker.display().to_string())
                .expect("initialize marker path serializes");
        let initialize_gate_literal = serde_json::to_string(&initialize_gate.display().to_string())
            .expect("initialize gate path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import os
import sys
import time

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        with open({initialize_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        while not os.path.exists({initialize_gate_literal}):
            time.sleep(0.01)
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        let cancel = TurnCancellation::new();
        let prepare_manager = manager.clone();
        let prepare_cancel = cancel.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        cwd: None,
                        previous_session_id: None,
                    },
                    prepare_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if initialize_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(initialize_marker.exists());
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        fs::write(&initialize_gate, "go").unwrap();

        let err = tokio::time::timeout(Duration::from_secs(2), prepare_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("ACP initialize cancelled"));

        let session_before = manager
            .session_manager
            .existing_session("session-1", "kimi")
            .await
            .unwrap();
        {
            let session = session_before.lock().await;
            assert!(session.client.is_alive());
            assert!(session.initialized);
            assert_eq!(session.session_id, None);
        }

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        let session_after = manager
            .session_manager
            .existing_session("session-1", "kimi")
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&session_before, &session_after));
        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("fake-session")
        );
        assert!(prepared.started_new_session);
    }

    #[tokio::test]
    async fn acp_cancelled_lifecycle_permission_request_does_not_create_pending_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("prepare_permission_after_cancel_acp.py");
        let initialize_marker = tmp.path().join("initialize_started");
        let permission_gate = tmp.path().join("send_permission");
        let permission_cancelled_marker = tmp.path().join("permission_cancelled");
        let initialize_marker_literal =
            serde_json::to_string(&initialize_marker.display().to_string())
                .expect("initialize marker path serializes");
        let permission_gate_literal = serde_json::to_string(&permission_gate.display().to_string())
            .expect("permission gate path serializes");
        let permission_cancelled_marker_literal =
            serde_json::to_string(&permission_cancelled_marker.display().to_string())
                .expect("permission marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import os
import sys
import time

initialize_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        initialize_id = request_id
        with open({initialize_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        while not os.path.exists({permission_gate_literal}):
            time.sleep(0.01)
        send({{
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {{
                "sessionId": "fake-session",
                "toolCall": {{
                    "title": "Run shell command",
                    "content": [{{"type": "text", "text": "$ cargo test"}}]
                }},
                "options": [
                    {{"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"}},
                    {{"optionId": "deny", "kind": "reject_once", "name": "Deny"}}
                ]
            }}
        }})
    elif request_id == 900:
        outcome = msg.get("result", {{}}).get("outcome", {{}}).get("outcome")
        if outcome == "cancelled":
            with open({permission_cancelled_marker_literal}, "w") as f:
                f.write("cancelled")
                f.flush()
        send({{"jsonrpc": "2.0", "id": initialize_id, "result": {{}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        let cancel = TurnCancellation::new();
        let prepare_manager = manager.clone();
        let prepare_cancel = cancel.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        cwd: None,
                        previous_session_id: None,
                    },
                    prepare_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if initialize_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(initialize_marker.exists());
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        fs::write(&permission_gate, "go").unwrap();

        let err = tokio::time::timeout(Duration::from_secs(2), prepare_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("ACP initialize cancelled"));
        assert!(permission_cancelled_marker.exists());
        assert!(!approvals.has_pending_for_session("session-1").await);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), prompts.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn acp_reused_cancelled_created_session_reports_started_new_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("prepare_cancel_after_new_acp.py");
        let session_new_marker = tmp.path().join("session_new_started");
        let session_new_count = tmp.path().join("session_new_count");
        let session_new_gate = tmp.path().join("allow_session_new");
        let session_new_marker_literal =
            serde_json::to_string(&session_new_marker.display().to_string())
                .expect("session/new marker path serializes");
        let session_new_count_literal =
            serde_json::to_string(&session_new_count.display().to_string())
                .expect("session/new count path serializes");
        let session_new_gate_literal =
            serde_json::to_string(&session_new_gate.display().to_string())
                .expect("session/new gate path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import os
import sys
import time

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        with open({session_new_count_literal}, "a") as f:
            f.write("new\n")
            f.flush()
        with open({session_new_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
        while not os.path.exists({session_new_gate_literal}):
            time.sleep(0.01)
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "cancelled-created-session"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        let cancel = TurnCancellation::new();
        let prepare_manager = manager.clone();
        let prepare_cancel = cancel.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        cwd: None,
                        previous_session_id: None,
                    },
                    prepare_cancel,
                )
                .await
        });

        for _ in 0..50 {
            if session_new_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(session_new_marker.exists());
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);
        fs::write(&session_new_gate, "go").unwrap();

        let err = tokio::time::timeout(Duration::from_secs(2), prepare_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("ACP session/new cancelled"));

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("cancelled-created-session")
        );
        assert!(prepared.started_new_session);
        assert_eq!(fs::read_to_string(session_new_count).unwrap(), "new\n");
    }

    #[tokio::test]
    async fn acp_unhealthy_session_removal_does_not_remove_newer_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script_1 = tmp.path().join("first_acp.py");
        let script_2 = tmp.path().join("second_acp.py");
        let script_body = r#"
import sys

for line in sys.stdin:
    if not line.strip():
        continue
"#;
        fs::write(&script_1, script_body).unwrap();
        fs::write(&script_2, script_body).unwrap();
        let manager = AcpBackendSessionManager::new(
            Arc::new(ApprovalBroker::default()),
            crate::machine::MachineRegistry::local_default(),
        );
        let cfg_1 = ExecutorConfig {
            name: "kimi".to_string(),
            protocol: ExecutorProtocol::Acp,
            machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
            command: "python3".to_string(),
            args: vec![script_1.display().to_string()],
            cwd: Some(tmp.path().to_path_buf()),
            env: BTreeMap::new(),
        };
        let cfg_2 = ExecutorConfig {
            args: vec![script_2.display().to_string()],
            ..cfg_1.clone()
        };
        let key = ("session-1".to_string(), "kimi".to_string());

        let first_cancel = TurnCancellation::new();
        let first = manager
            .get_or_create_session("session-1", "kimi", &cfg_1, None, &first_cancel)
            .await
            .unwrap();
        let second_cancel = TurnCancellation::new();
        let second = manager
            .get_or_create_session("session-1", "kimi", &cfg_2, None, &second_cancel)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));

        assert!(
            !manager
                .remove_unhealthy_session_if_same(
                    &key,
                    &first,
                    "stale cleanup",
                    &TurnCancellation::new()
                )
                .await
        );
        let current = manager.existing_session("session-1", "kimi").await.unwrap();
        assert!(Arc::ptr_eq(&current, &second));
        second.lock().await.client.close("test complete").await;
    }

    #[tokio::test]
    async fn acp_cancelled_removal_does_not_remove_matching_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("current_acp.py");
        fs::write(
            &script,
            r#"
import sys

for line in sys.stdin:
    if not line.strip():
        continue
"#,
        )
        .unwrap();
        let manager = AcpBackendSessionManager::new(
            Arc::new(ApprovalBroker::default()),
            MachineRegistry::local_default(),
        );
        let cfg = ExecutorConfig {
            name: "kimi".to_string(),
            protocol: ExecutorProtocol::Acp,
            machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
            command: "python3".to_string(),
            args: vec![script.display().to_string()],
            cwd: Some(tmp.path().to_path_buf()),
            env: BTreeMap::new(),
        };
        let key = ("session-1".to_string(), "kimi".to_string());
        let cancel = TurnCancellation::new();
        let session = manager
            .get_or_create_session("session-1", "kimi", &cfg, None, &cancel)
            .await
            .unwrap();
        assert!(cancel.cancel(InterruptReason::ReplacedByNewMessage).await);

        assert!(
            !manager
                .remove_unhealthy_session_if_same(
                    &key,
                    &session,
                    "cancelled stale removal",
                    &cancel
                )
                .await
        );

        let current = manager.existing_session("session-1", "kimi").await.unwrap();
        assert!(Arc::ptr_eq(&current, &session));
        assert!(session.lock().await.client.is_alive());
        session.lock().await.client.close("test complete").await;
    }

    #[tokio::test]
    async fn acp_cancelled_mismatched_prepare_does_not_remove_newer_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script_1 = tmp.path().join("old_acp.py");
        let script_2 = tmp.path().join("new_acp.py");
        let script_body = r#"
import sys

for line in sys.stdin:
    if not line.strip():
        continue
"#;
        fs::write(&script_1, script_body).unwrap();
        fs::write(&script_2, script_body).unwrap();
        let manager = AcpBackendSessionManager::new(
            Arc::new(ApprovalBroker::default()),
            crate::machine::MachineRegistry::local_default(),
        );
        let cfg_1 = ExecutorConfig {
            name: "kimi".to_string(),
            protocol: ExecutorProtocol::Acp,
            machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
            command: "python3".to_string(),
            args: vec![script_1.display().to_string()],
            cwd: Some(tmp.path().to_path_buf()),
            env: BTreeMap::new(),
        };
        let cfg_2 = ExecutorConfig {
            args: vec![script_2.display().to_string()],
            ..cfg_1.clone()
        };
        let publish_cancel = TurnCancellation::new();
        let newer = manager
            .get_or_create_session("session-1", "kimi", &cfg_2, None, &publish_cancel)
            .await
            .unwrap();
        let stale_cancel = TurnCancellation::new();
        assert!(
            stale_cancel
                .cancel(InterruptReason::ReplacedByNewMessage)
                .await
        );

        let err = manager
            .get_or_create_session("session-1", "kimi", &cfg_1, None, &stale_cancel)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("ACP prepare cancelled"));
        let current = manager.existing_session("session-1", "kimi").await.unwrap();
        assert!(Arc::ptr_eq(&current, &newer));
        newer.lock().await.client.close("test complete").await;
    }

    #[tokio::test]
    async fn acp_manager_prompts_fake_stdio_server() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("fake_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        prompt = "".join(item.get("text", "") for item in msg.get("params", {}).get("prompt", []))
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "plan", "title": "Plan", "content": {"text": "working"}}},
        })
        send({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "reply:" + prompt}}},
        })
        send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "end_turn"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
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
                    turn: turn_ref("session-1", "kimi", 1),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("fake-session")
        );
        assert_eq!(response.final_text, "reply:hello");
        assert!(prepared.started_new_session);
        assert_eq!(events.updates[0].kind, "plan");
    }

    #[tokio::test]
    async fn acp_prompt_cancellation_sends_soft_cancel_and_reuses_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cancellable_acp.py");
        let prompt_marker = tmp.path().join("prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys

cancelled = False
prompt_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt = "".join(item.get("text", "") for item in msg.get("params", {{}}).get("prompt", []))
        if not cancelled:
            prompt_id = request_id
            with open({prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
        else:
            send({{"jsonrpc": "2.0", "id": request_id, "result": {{"stopReason": "end_turn", "content": [{{"type": "text", "text": "reply:" + prompt}}]}}}})
    elif method == "session/cancel":
        cancelled = True
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
        if prompt_id is not None:
            send({{"jsonrpc": "2.0", "id": prompt_id, "result": {{"stopReason": "cancelled"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "kimi", 1),
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
        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());
        let session = manager
            .session_manager
            .existing_session("session-1", "kimi")
            .await
            .unwrap();
        {
            let session = session.lock().await;
            assert!(session.client.is_alive());
            assert_eq!(session.session_id.as_deref(), Some("fake-session"));
        }

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    prompt: "after cancel".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        assert_eq!(response.final_text, "reply:after cancel");
    }

    #[tokio::test]
    async fn acp_cancelled_prompt_late_updates_do_not_pollute_replacement_prompt() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("late_update_acp.py");
        let prompt_marker = tmp.path().join("first_prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let old_response_marker = tmp.path().join("old_response_sent");
        let second_prompt_marker = tmp.path().join("second_prompt_started");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        let old_response_marker_literal =
            serde_json::to_string(&old_response_marker.display().to_string())
                .expect("old response marker path serializes");
        let second_prompt_marker_literal =
            serde_json::to_string(&second_prompt_marker.display().to_string())
                .expect("second prompt marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys
import time

first_prompt_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

def prompt_text(msg):
    return "".join(item.get("text", "") for item in msg.get("params", {{}}).get("prompt", []))

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt = prompt_text(msg)
        if prompt == "first":
            first_prompt_id = request_id
            with open({prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
        else:
            with open({second_prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{"update": {{"sessionUpdate": "agent_message_chunk", "content": {{"text": "new reply"}}}}}},
            }})
            send({{"jsonrpc": "2.0", "id": request_id, "result": {{"stopReason": "end_turn"}}}})
    elif method == "session/cancel":
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
        send({{
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {{"update": {{"sessionUpdate": "agent_message_chunk", "content": {{"text": "old late"}}}}}},
        }})
        time.sleep(0.1)
        if first_prompt_id is not None:
            send({{"jsonrpc": "2.0", "id": first_prompt_id, "result": {{"stopReason": "cancelled"}}}})
            with open({old_response_marker_literal}, "w") as f:
                f.write("sent")
                f.flush()
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let first_prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        prompt: "first".to_string(),
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
        let first_outcome = tokio::time::timeout(Duration::from_secs(2), first_prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first_outcome, ExecutorPromptOutcome::Cancelled));
        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());
        for _ in 0..50 {
            if old_response_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(old_response_marker.exists());

        let mut events = CollectingExecutorEventSink::default();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            manager.prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    prompt: "second".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(second_prompt_marker.exists());
        assert_eq!(response.final_text, "new reply");
        assert!(
            events
                .updates
                .iter()
                .all(|update| update.text != "old late")
        );
    }

    #[tokio::test]
    async fn acp_late_permission_request_after_cancel_does_not_create_pending_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("late_permission_acp.py");
        let prompt_marker = tmp.path().join("first_prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let permission_gate = tmp.path().join("send_late_permission");
        let permission_cancelled_marker = tmp.path().join("permission_cancelled");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        let permission_gate_literal = serde_json::to_string(&permission_gate.display().to_string())
            .expect("permission gate path serializes");
        let permission_cancelled_marker_literal =
            serde_json::to_string(&permission_cancelled_marker.display().to_string())
                .expect("permission marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import os
import sys
import time

prompt_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

def prompt_text(msg):
    return "".join(item.get("text", "") for item in msg.get("params", {{}}).get("prompt", []))

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt = prompt_text(msg)
        if prompt == "first":
            prompt_id = request_id
            with open({prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
        else:
            send({{
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {{"update": {{"sessionUpdate": "agent_message_chunk", "content": {{"text": "new reply"}}}}}},
            }})
            send({{"jsonrpc": "2.0", "id": request_id, "result": {{"stopReason": "end_turn"}}}})
    elif method == "session/cancel":
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
        while not os.path.exists({permission_gate_literal}):
            time.sleep(0.01)
        send({{
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {{
                "sessionId": "fake-session",
                "toolCall": {{
                    "title": "Run shell command",
                    "content": [{{"type": "text", "text": "$ cargo test"}}]
                }},
                "options": [
                    {{"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"}},
                    {{"optionId": "deny", "kind": "reject_once", "name": "Deny"}}
                ]
            }}
        }})
    elif request_id == 900:
        outcome = msg.get("result", {{}}).get("outcome", {{}}).get("outcome")
        if outcome == "cancelled":
            with open({permission_cancelled_marker_literal}, "w") as f:
                f.write("cancelled")
                f.flush()
        if prompt_id is not None:
            send({{"jsonrpc": "2.0", "id": prompt_id, "result": {{"stopReason": "cancelled"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let first_prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        prompt: "first".to_string(),
                        user_id: Some("U1".to_string()),
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
        let first_outcome = tokio::time::timeout(Duration::from_secs(2), first_prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first_outcome, ExecutorPromptOutcome::Cancelled));
        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());

        fs::write(&permission_gate, "go").unwrap();
        for _ in 0..100 {
            if permission_cancelled_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(permission_cancelled_marker.exists());
        assert!(!approvals.has_pending_for_session("session-1").await);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), prompts.recv())
                .await
                .is_err()
        );

        let mut events = CollectingExecutorEventSink::default();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            manager.prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    prompt: "second".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(response.final_text, "new reply");
    }

    #[tokio::test]
    async fn acp_cancel_barrier_timeout_closes_unsettled_session() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("unsettled_cancel_acp.py");
        let prompt_marker = tmp.path().join("first_prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let second_prompt_marker = tmp.path().join("second_prompt_started");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        let second_prompt_marker_literal =
            serde_json::to_string(&second_prompt_marker.display().to_string())
                .expect("second marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

def prompt_text(msg):
    return "".join(item.get("text", "") for item in msg.get("params", {{}}).get("prompt", []))

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt = prompt_text(msg)
        if prompt == "first":
            with open({prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
        else:
            with open({second_prompt_marker_literal}, "w") as f:
                f.write("started")
                f.flush()
            send({{"jsonrpc": "2.0", "id": request_id, "result": {{"stopReason": "end_turn"}}}})
    elif method == "session/cancel":
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let first_prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "kimi", 1),
                        prompt: "first".to_string(),
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
        let first_outcome = tokio::time::timeout(Duration::from_secs(2), first_prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first_outcome, ExecutorPromptOutcome::Cancelled));
        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());

        let mut events = CollectingExecutorEventSink::default();
        let outcome = tokio::time::timeout(
            Duration::from_secs(4),
            manager.prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    prompt: "second".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            ),
        )
        .await
        .unwrap();
        let err = match outcome {
            ExecutorPromptOutcome::Failed(err) => err,
            other => panic!("expected failed prompt after barrier timeout, got {other:?}"),
        };
        assert!(
            err.to_string()
                .contains("ACP cancelled prompt response did not settle")
        );
        assert!(!second_prompt_marker.exists());
        let stale_session = manager
            .session_manager
            .existing_session("session-1", "kimi")
            .await
            .unwrap();
        assert!(!stale_session.lock().await.client.is_alive());

        let prepared = manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 3),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some("fake-session")
        );
    }

    #[tokio::test]
    async fn acp_interrupt_sends_cancel_while_prompt_is_running() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("interrupt_acp.py");
        let prompt_marker = tmp.path().join("prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys

prompt_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt_id = request_id
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
    elif method == "session/cancel":
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
        if prompt_id is not None:
            send({{"jsonrpc": "2.0", "id": prompt_id, "result": {{"stopReason": "cancelled"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
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
                        turn: turn_ref("session-1", "kimi", 1),
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
                turn: turn_ref("session-1", "kimi", 1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());
        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
    }

    #[tokio::test]
    async fn acp_stale_generation_interrupt_does_not_cancel_newer_prompt() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("stale_interrupt_acp.py");
        let prompt_marker = tmp.path().join("prompt_started");
        let cancel_marker = tmp.path().join("cancel_received");
        let prompt_marker_literal = serde_json::to_string(&prompt_marker.display().to_string())
            .expect("prompt marker path serializes");
        let cancel_marker_literal = serde_json::to_string(&cancel_marker.display().to_string())
            .expect("cancel marker path serializes");
        fs::write(
            &script,
            format!(
                r#"
import json
import sys

prompt_id = None

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{}}}})
    elif method == "session/new":
        send({{"jsonrpc": "2.0", "id": request_id, "result": {{"sessionId": "fake-session"}}}})
    elif method == "session/prompt":
        prompt_id = request_id
        with open({prompt_marker_literal}, "w") as f:
            f.write("started")
            f.flush()
    elif method == "session/cancel":
        with open({cancel_marker_literal}, "w") as f:
            f.write("cancelled")
            f.flush()
        if prompt_id is not None:
            send({{"jsonrpc": "2.0", "id": prompt_id, "result": {{"stopReason": "cancelled"}}}})
"#
            ),
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = Arc::new(AcpExecutorManager::new(executors));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 2),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let cancel = TurnCancellation::new();
        let prompt_manager = manager.clone();
        let prompt_cancel = cancel.clone();
        let prompt_task = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn_ref("session-1", "kimi", 2),
                        prompt: "newer".to_string(),
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
                turn: turn_ref("session-1", "kimi", 1),
                reason: InterruptReason::ReplacedByNewMessage,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!cancel_marker.exists());

        assert!(cancel.cancel(InterruptReason::UserStop).await);
        for _ in 0..50 {
            if cancel_marker.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(cancel_marker.exists());
        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt_task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
    }

    #[tokio::test]
    async fn acp_cancelled_stop_reason_returns_cancelled_outcome() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("cancelled_result_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"stopReason": "cancelled"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    cwd: None,
                    previous_session_id: None,
                },
                TurnCancellation::new(),
            )
            .await
            .unwrap();

        let mut events = CollectingExecutorEventSink::default();
        let outcome = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn_ref("session-1", "kimi", 1),
                    prompt: "cancel".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await;
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
    }

    #[tokio::test]
    async fn acp_prompt_cancellation_removes_pending_permission() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_wait_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {
                "sessionId": "fake-session",
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                },
                "options": [
                    {"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"},
                    {"optionId": "deny", "kind": "reject_once", "name": "Deny"}
                ]
            }
        })
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
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
                        turn: turn_ref("session-1", "kimi", 1),
                        prompt: "run tests".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approvals.has_pending_for_session("session-1").await);
        assert!(cancel.cancel(InterruptReason::UserStop).await);
        assert!(matches!(
            prompt_task.await.unwrap(),
            ExecutorPromptOutcome::Cancelled
        ));
        for _ in 0..50 {
            if !approvals.has_pending_for_session("session-1").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
    }

    #[tokio::test]
    async fn acp_permission_request_waits_for_text_approval() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("approval_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        send({
            "jsonrpc": "2.0",
            "id": 900,
            "method": "session/request_permission",
            "params": {
                "sessionId": "fake-session",
                "toolCall": {
                    "title": "Run shell command",
                    "content": [{"type": "text", "text": "$ cargo test"}]
                },
                "options": [
                    {"optionId": "allow_once", "kind": "allow_once", "name": "Allow once"},
                    {"optionId": "deny", "kind": "reject_once", "name": "Deny"}
                ]
            }
        })
    elif request_id == 900:
        outcome = msg.get("result", {}).get("outcome", {})
        if outcome.get("outcome") == "selected" and outcome.get("optionId") == "allow_once":
            send({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "approved"}}},
            })
            send({"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}})
        else:
            send({"jsonrpc": "2.0", "id": 3, "error": {"code": -32000, "message": "not approved"}})
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let manager = Arc::new(AcpExecutorManager::with_approvals(
            executors,
            approvals.clone(),
        ));
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
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
                        turn: turn_ref("session-1", "kimi", 1),
                        prompt: "run tests".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::new(),
                )
                .await
        });
        let approval_prompt = prompts.recv().await.unwrap();
        assert!(approval_prompt.body.contains("cargo test"));
        let reply = approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval_prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert!(reply.text.contains("Approved"));

        let response = prompt_task.await.unwrap().unwrap();
        assert_eq!(response.final_text, "approved");
    }

    #[tokio::test]
    async fn acp_prompt_returns_error_when_child_exits_without_response() {
        let tmp = tempfile::tempdir().unwrap();
        let script = tmp.path().join("exiting_acp.py");
        fs::write(
            &script,
            r#"
import json
import sys

def send(payload):
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()

for line in sys.stdin:
    if not line.strip():
        continue
    msg = json.loads(line)
    method = msg.get("method")
    request_id = msg.get("id")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method == "session/new":
        send({"jsonrpc": "2.0", "id": request_id, "result": {"sessionId": "fake-session"}})
    elif method == "session/prompt":
        sys.exit(0)
"#,
        )
        .unwrap();

        let mut executors = BTreeMap::new();
        executors.insert(
            "kimi".to_string(),
            ExecutorConfig {
                name: "kimi".to_string(),
                protocol: ExecutorProtocol::Acp,
                machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                command: "python3".to_string(),
                args: vec![script.display().to_string()],
                cwd: Some(tmp.path().to_path_buf()),
                env: BTreeMap::new(),
            },
        );
        let manager = AcpExecutorManager::new(executors);
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn_ref("session-1", "kimi", 1),
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
                    turn: turn_ref("session-1", "kimi", 1),
                    prompt: "hello".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut events,
                TurnCancellation::new(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed stdout"));
    }
}
