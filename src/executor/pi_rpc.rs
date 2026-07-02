use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout},
    sync::{Mutex, MutexGuard, mpsc},
};

use crate::{
    approval::{
        ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest, ApprovalSelection,
        SharedApprovalBroker,
    },
    config::{ExecutorConfig, ExecutorProtocol},
    executor::{
        ExecutorBackend, ExecutorChannelEvent, ExecutorDescriptor, ExecutorEventSink,
        ExecutorInterruptRequest, ExecutorPromptOutcome, ExecutorPromptRequest, ExecutorResponse,
        ExecutorSlashCommand, ExecutorSlashCommandOutcome, ExecutorSlashCommandRequest,
        ExecutorSlashCommandSupport, ExecutorTurnRef, ExecutorUpdate, PreparedExecutor,
        TurnCancellation,
    },
    machine::{MachinePrepareRequest, MachineRegistry, MachineWorkspaceRecord, StdioCommand},
    text::truncate_chars,
};

type SessionKey = (String, String);
type SharedPiSession = Arc<Mutex<PiRpcSession>>;
type SharedStdin = Arc<Mutex<ChildStdin>>;
type SharedChild = Arc<Mutex<Child>>;
type SharedLifecycle = Arc<Mutex<()>>;
type PendingExtensionApprovals = Arc<Mutex<HashMap<String, ApprovalCancellation>>>;
type SharedActivePiPrompts = Arc<Mutex<HashMap<SessionKey, PiActivePrompt>>>;
type ExtensionResponseSender = mpsc::UnboundedSender<Value>;
type StdoutMessageReceiver = mpsc::UnboundedReceiver<anyhow::Result<Value>>;

const PI_RPC_PROTOCOL: &str = "pi_rpc";
const DEFAULT_APPROVAL_FLAG: &str = "--approve";

#[derive(Debug)]
pub struct PiRpcExecutorManager {
    executors: BTreeMap<String, ExecutorConfig>,
    machines: MachineRegistry,
    approvals: SharedApprovalBroker,
    sessions: PiSessionRegistry,
    active_prompts: SharedActivePiPrompts,
}

#[derive(Debug)]
struct PiSessionRegistry {
    sessions: Mutex<HashMap<SessionKey, SharedPiSession>>,
}

#[derive(Clone, Debug)]
struct PublishedPiSession {
    session: SharedPiSession,
}

#[derive(Clone, Debug)]
struct StartedPiSession {
    key: SessionKey,
    session: SharedPiSession,
}

#[derive(Clone, Debug)]
enum PiSessionAcquisition {
    Published(PublishedPiSession),
    Started(StartedPiSession),
}

impl PiSessionRegistry {
    fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    async fn get(&self, session_key: &str, executor: &str) -> anyhow::Result<PublishedPiSession> {
        let key = (session_key.to_string(), executor.to_string());
        self.sessions
            .lock()
            .await
            .get(&key)
            .cloned()
            .map(|session| PublishedPiSession { session })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "executor `{executor}` has not prepared Pi RPC session for `{session_key}`"
                )
            })
    }

    async fn lookup(&self, key: &SessionKey) -> Option<PublishedPiSession> {
        self.sessions
            .lock()
            .await
            .get(key)
            .cloned()
            .map(|session| PublishedPiSession { session })
    }

    async fn publish_started(
        &self,
        started: &StartedPiSession,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<Option<PublishedPiSession>> {
        let mut sessions = self.sessions.lock().await;
        if cancel.is_cancelled_now() {
            anyhow::bail!("Pi RPC prepare cancelled");
        }
        if sessions.contains_key(&started.key) {
            return Ok(None);
        }
        sessions.insert(started.key.clone(), started.session.clone());
        Ok(Some(PublishedPiSession {
            session: started.session.clone(),
        }))
    }

    async fn remove_if_same(
        &self,
        key: &SessionKey,
        session: &SharedPiSession,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<bool> {
        let removed = {
            let mut sessions = self.sessions.lock().await;
            if cancel.is_cancelled_now() {
                anyhow::bail!("Pi RPC prepare cancelled");
            }
            if sessions
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
            session.lock().await.close().await;
        }
        Ok(removed)
    }

    async fn remove(&self, key: &SessionKey) -> Option<SharedPiSession> {
        self.sessions.lock().await.remove(key)
    }
}

impl PublishedPiSession {
    fn shared(&self) -> SharedPiSession {
        self.session.clone()
    }

    async fn lock(&self) -> MutexGuard<'_, PiRpcSession> {
        self.session.lock().await
    }
}

impl StartedPiSession {
    async fn close(&self) {
        self.session.lock().await.close().await;
    }

    async fn lock(&self) -> MutexGuard<'_, PiRpcSession> {
        self.session.lock().await
    }
}

impl PiRpcExecutorManager {
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
            .filter(|(_, cfg)| cfg.protocol == ExecutorProtocol::PiRpc)
            .collect();
        Self {
            executors,
            machines,
            approvals,
            sessions: PiSessionRegistry::new(),
            active_prompts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn acquire_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        router_workspace: Option<&Path>,
        previous_session_id: Option<String>,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<PiSessionAcquisition> {
        let key = (session_key.to_string(), executor.to_string());
        loop {
            if cancel.is_cancelled().await {
                anyhow::bail!("Pi RPC prepare cancelled");
            }
            let identity_args = pi_rpc_args(&cfg.args, None)?;
            let identity_command = self
                .machines
                .prepare_executor_command(MachinePrepareRequest {
                    machine_id: &cfg.machine,
                    session_key,
                    router_workspace,
                    executor_cwd: cfg.cwd.as_deref(),
                    command: &cfg.command,
                    args: &identity_args,
                    env: &cfg.env,
                    cancel: Some(cancel),
                })
                .await?;

            if let Some(existing) = self.sessions.lookup(&key).await {
                let mut guard = existing.lock().await;
                let matches = guard.is_alive().await && guard.matches(&identity_command.stdio);
                drop(guard);
                if matches {
                    return Ok(PiSessionAcquisition::Published(existing));
                }
                if cancel.is_cancelled().await {
                    anyhow::bail!("Pi RPC prepare cancelled");
                }
                if !self
                    .sessions
                    .remove_if_same(&key, &existing.session, cancel)
                    .await?
                {
                    continue;
                }
            }

            let start_args = pi_rpc_args(&cfg.args, previous_session_id.as_deref())?;
            let start_command = self
                .machines
                .prepare_executor_command(MachinePrepareRequest {
                    machine_id: &cfg.machine,
                    session_key,
                    router_workspace,
                    executor_cwd: cfg.cwd.as_deref(),
                    command: &cfg.command,
                    args: &start_args,
                    env: &cfg.env,
                    cancel: Some(cancel),
                })
                .await?;
            let session = Arc::new(Mutex::new(
                PiRpcSession::start(
                    cfg.clone(),
                    start_command.stdio,
                    identity_command.stdio,
                    start_command.workspace,
                    session_key.to_string(),
                    executor.to_string(),
                    self.approvals.clone(),
                )
                .await?,
            ));
            if cancel.is_cancelled().await {
                session.lock().await.close().await;
                anyhow::bail!("Pi RPC prepare cancelled");
            }
            return Ok(PiSessionAcquisition::Started(StartedPiSession {
                key: key.clone(),
                session,
            }));
        }
    }

    async fn get_or_publish_session(
        &self,
        session_key: &str,
        executor: &str,
        cfg: &ExecutorConfig,
        router_workspace: Option<&Path>,
        previous_session_id: Option<String>,
        cancel: &TurnCancellation,
    ) -> anyhow::Result<PublishedPiSession> {
        loop {
            match self
                .acquire_session(
                    session_key,
                    executor,
                    cfg,
                    router_workspace,
                    previous_session_id.clone(),
                    cancel,
                )
                .await?
            {
                PiSessionAcquisition::Published(session) => return Ok(session),
                PiSessionAcquisition::Started(session) => {
                    if cancel.is_cancelled().await {
                        session.close().await;
                        anyhow::bail!("Pi RPC prepare cancelled");
                    }
                    match self.sessions.publish_started(&session, cancel).await {
                        Ok(Some(published)) => return Ok(published),
                        Ok(None) => {
                            session.close().await;
                            continue;
                        }
                        Err(err) => {
                            session.close().await;
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    async fn discard_session(&self, session_key: &str, executor: &str) -> bool {
        let key = (session_key.to_string(), executor.to_string());
        let active_prompt = self.active_prompts.lock().await.remove(&key);
        let session = self.sessions.remove(&key).await;
        if let Some(active_prompt) = active_prompt {
            active_prompt.force_close().await;
            return session.is_some();
        }
        let Some(session) = session else {
            return false;
        };
        session.lock().await.close().await;
        true
    }
}

#[async_trait]
impl ExecutorBackend for PiRpcExecutorManager {
    fn get(&self, name: &str) -> Option<ExecutorDescriptor> {
        self.executors.get(name).map(|cfg| ExecutorDescriptor {
            name: cfg.name.clone(),
            protocol: PI_RPC_PROTOCOL.to_string(),
            machine_id: cfg.machine.clone(),
        })
    }

    fn list(&self) -> Vec<ExecutorDescriptor> {
        self.executors
            .values()
            .map(|cfg| ExecutorDescriptor {
                name: cfg.name.clone(),
                protocol: PI_RPC_PROTOCOL.to_string(),
                machine_id: cfg.machine.clone(),
            })
            .collect()
    }

    async fn prepare(
        &self,
        request: crate::executor::ExecutorPrepareRequest,
        cancel: TurnCancellation,
    ) -> anyhow::Result<PreparedExecutor> {
        if cancel.is_cancelled().await {
            anyhow::bail!("Pi RPC prepare cancelled");
        }
        let cfg = self.executors.get(&request.turn.executor).ok_or_else(|| {
            anyhow::anyhow!("executor `{}` is not configured", request.turn.executor)
        })?;
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            previous_session_id = ?request.previous_session_id,
            "preparing Pi RPC executor session"
        );
        let previous_session_id = request.previous_session_id.clone();
        let prepared = loop {
            let acquisition = self
                .acquire_session(
                    &request.turn.session_key,
                    &request.turn.executor,
                    cfg,
                    request.cwd.as_deref(),
                    previous_session_id.clone(),
                    &cancel,
                )
                .await?;
            match acquisition {
                PiSessionAcquisition::Published(session) => {
                    if cancel.is_cancelled().await {
                        anyhow::bail!("Pi RPC prepare cancelled");
                    }
                    let mut session = session.lock().await;
                    let state = match session.get_state(&cancel).await {
                        Ok(state) => state,
                        Err(err) => {
                            if session.invalidated.load(Ordering::Acquire) {
                                session.close().await;
                            }
                            return Err(err);
                        }
                    };
                    break session.prepared_from_state(&state, previous_session_id.as_ref());
                }
                PiSessionAcquisition::Started(session) => {
                    if cancel.is_cancelled().await {
                        session.close().await;
                        anyhow::bail!("Pi RPC prepare cancelled");
                    }
                    let prepared = {
                        let mut session_guard = session.lock().await;
                        let state = match session_guard.get_state(&cancel).await {
                            Ok(state) => state,
                            Err(err) => {
                                session_guard.close().await;
                                return Err(err);
                            }
                        };
                        session_guard.prepared_from_state(&state, previous_session_id.as_ref())
                    };
                    match self.sessions.publish_started(&session, &cancel).await {
                        Ok(Some(_)) => break prepared,
                        Ok(None) => {
                            session.close().await;
                            continue;
                        }
                        Err(err) => {
                            session.close().await;
                            return Err(err);
                        }
                    }
                }
            }
        };
        tracing::info!(
            executor = %request.turn.executor,
            session_key = %request.turn.session_key,
            generation = request.turn.generation,
            external_session_id = ?prepared.external_session_id,
            started_new_session = prepared.started_new_session,
            "prepared Pi RPC executor session"
        );
        Ok(prepared)
    }

    async fn prompt(
        &self,
        request: ExecutorPromptRequest,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
    ) -> ExecutorPromptOutcome {
        let session_key = request.turn.session_key.clone();
        let executor = request.turn.executor.clone();
        let generation = request.turn.generation;
        let published_session = match self.sessions.get(&session_key, &executor).await {
            Ok(session) => session,
            Err(err) => return ExecutorPromptOutcome::Failed(err),
        };
        let shared_session = published_session.shared();
        let mut session = shared_session.lock().await;
        let active_key = (session_key.clone(), executor.clone());
        let prompt_guard = PiPromptGuard::registered(
            self.active_prompts.clone(),
            active_key.clone(),
            session.active_prompt(generation),
            Some(shared_session.clone()),
        )
        .await;
        tracing::info!(
            executor = %executor,
            session_key = %session_key,
            generation,
            prompt_len = request.prompt.len(),
            "starting Pi RPC executor turn"
        );
        let result = session
            .run_prompt(
                &request.prompt,
                request.user_id,
                events,
                cancel,
                prompt_guard.active_prompt(),
            )
            .await;
        prompt_guard.finish().await;
        match &result {
            ExecutorPromptOutcome::Completed(response) => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                final_text_len = response.final_text.len(),
                "completed Pi RPC executor turn"
            ),
            ExecutorPromptOutcome::Cancelled => tracing::info!(
                executor = %executor,
                session_key = %session_key,
                generation,
                "cancelled Pi RPC executor turn"
            ),
            ExecutorPromptOutcome::Failed(err) => tracing::warn!(
                error = %err,
                executor = %executor,
                session_key = %session_key,
                generation,
                "failed Pi RPC executor turn"
            ),
        }
        result
    }

    fn slash_command_support(
        &self,
        executor: &str,
        _command: &ExecutorSlashCommand,
    ) -> ExecutorSlashCommandSupport {
        if self.executors.contains_key(executor) {
            ExecutorSlashCommandSupport::IdleOnly
        } else {
            ExecutorSlashCommandSupport::Unsupported
        }
    }

    async fn slash_command(
        &self,
        request: ExecutorSlashCommandRequest,
    ) -> ExecutorSlashCommandOutcome {
        let Some(cfg) = self.executors.get(&request.executor) else {
            return ExecutorSlashCommandOutcome::Unsupported;
        };
        let cancel = request.cancel.clone();
        let published_session = match self
            .get_or_publish_session(
                &request.session_key,
                &request.executor,
                cfg,
                request.cwd.as_deref(),
                request.previous_session_id.clone(),
                &cancel,
            )
            .await
        {
            Ok(session) => session,
            Err(err) => return ExecutorSlashCommandOutcome::Failed(err),
        };
        let shared_session = published_session.shared();
        let mut session = shared_session.lock().await;
        let mut events = DiscardingEventSink;
        let prompt_guard = if let Some(turn) = request.turn.as_ref() {
            PiPromptGuard::registered(
                self.active_prompts.clone(),
                (turn.session_key.clone(), turn.executor.clone()),
                session.active_prompt(turn.generation),
                Some(shared_session.clone()),
            )
            .await
        } else {
            PiPromptGuard::unregistered(session.active_prompt(0), Some(shared_session.clone()))
        };
        match session
            .run_prompt(
                &request.command.raw,
                request.user_id,
                &mut events,
                cancel.clone(),
                prompt_guard.active_prompt(),
            )
            .await
        {
            outcome => {
                prompt_guard.finish().await;
                match outcome {
                    ExecutorPromptOutcome::Completed(response) => {
                        let prepared = match session.get_state(&cancel).await {
                            Ok(state) => session
                                .prepared_from_state(&state, request.previous_session_id.as_ref()),
                            Err(err) => {
                                if session.invalidated.load(Ordering::Acquire) {
                                    session.close().await;
                                }
                                return ExecutorSlashCommandOutcome::Failed(err);
                            }
                        };
                        ExecutorSlashCommandOutcome::CompletedWithSession { response, prepared }
                    }
                    ExecutorPromptOutcome::Cancelled => ExecutorSlashCommandOutcome::Failed(
                        anyhow::anyhow!("Pi RPC slash command cancelled"),
                    ),
                    ExecutorPromptOutcome::Failed(err) => ExecutorSlashCommandOutcome::Failed(err),
                }
            }
        }
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
                    target: "agent_router::pi_rpc",
                    executor = %request.turn.executor,
                    session_key = %request.turn.session_key,
                    interrupted_generation = request.turn.generation,
                    active_generation = active_prompt.generation,
                    reason = ?request.reason,
                    "ignoring stale Pi RPC interrupt for newer active prompt"
                );
                return Ok(());
            }
            if let Err(err) = active_prompt.abort().await {
                active_prompt.force_close().await;
                return Err(err);
            }
        }
        Ok(())
    }

    async fn discard_session(&self, turn: ExecutorTurnRef, reason: &str) -> anyhow::Result<()> {
        if self
            .discard_session(&turn.session_key, &turn.executor)
            .await
        {
            tracing::debug!(
                target: "agent_router::pi_rpc",
                session_key = %turn.session_key,
                executor = %turn.executor,
                reason,
                "discarded Pi RPC executor session"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PiActivePrompt {
    generation: u64,
    stdin: SharedStdin,
    child: SharedChild,
    pending_approvals: PendingExtensionApprovals,
    abort_sent: Arc<AtomicBool>,
    invalidated: Arc<AtomicBool>,
    lifecycle: SharedLifecycle,
}

impl PiActivePrompt {
    async fn abort(&self) -> anyhow::Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.invalidated.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.abort_sent.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        cancel_pending_extension_approvals(&self.stdin, &self.pending_approvals, false).await?;
        if self.invalidated.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Err(err) = write_json(&self.stdin, json!({"type": "abort"})).await {
            self.invalidated.store(true, Ordering::Release);
            return Err(err);
        }
        Ok(())
    }

    async fn write_extension_response(&self, response: Value) -> anyhow::Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.invalidated.load(Ordering::Acquire) || self.abort_sent.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Err(err) = write_json(&self.stdin, response).await {
            self.invalidated.store(true, Ordering::Release);
            return Err(err);
        }
        Ok(())
    }

    async fn force_close(&self) {
        self.invalidated.store(true, Ordering::Release);
        self.abort_sent.store(true, Ordering::Release);
        let _lifecycle = self.lifecycle.lock().await;
        let _ =
            cancel_pending_extension_approvals(&self.stdin, &self.pending_approvals, false).await;
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    fn invalidate_session(&self) {
        self.invalidated.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct PiPromptGuard {
    active_prompt: PiActivePrompt,
    active_prompts: Option<SharedActivePiPrompts>,
    active_key: Option<SessionKey>,
    cleanup_session: Option<SharedPiSession>,
    generation: u64,
    armed: bool,
}

impl PiPromptGuard {
    async fn registered(
        active_prompts: SharedActivePiPrompts,
        active_key: SessionKey,
        active_prompt: PiActivePrompt,
        cleanup_session: Option<SharedPiSession>,
    ) -> Self {
        set_active_pi_prompt(&active_prompts, active_key.clone(), active_prompt.clone()).await;
        Self {
            generation: active_prompt.generation,
            active_prompt,
            active_prompts: Some(active_prompts),
            active_key: Some(active_key),
            cleanup_session,
            armed: true,
        }
    }

    fn unregistered(
        active_prompt: PiActivePrompt,
        cleanup_session: Option<SharedPiSession>,
    ) -> Self {
        Self {
            generation: active_prompt.generation,
            active_prompt,
            active_prompts: None,
            active_key: None,
            cleanup_session,
            armed: true,
        }
    }

    fn active_prompt(&self) -> PiActivePrompt {
        self.active_prompt.clone()
    }

    async fn finish(mut self) {
        if let (Some(active_prompts), Some(active_key)) =
            (self.active_prompts.as_ref(), self.active_key.as_ref())
        {
            clear_active_pi_prompt(active_prompts, active_key, self.generation).await;
        }
        self.armed = false;
    }
}

impl Drop for PiPromptGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let active_prompt = self.active_prompt.clone();
        let active_prompts = self.active_prompts.clone();
        let active_key = self.active_key.clone();
        let cleanup_session = self.cleanup_session.clone();
        let generation = self.generation;
        active_prompt.invalidate_session();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let (Some(active_prompts), Some(active_key)) = (active_prompts, active_key) {
                    clear_active_pi_prompt(&active_prompts, &active_key, generation).await;
                }
                if let Some(session) = cleanup_session {
                    session.lock().await.close().await;
                } else {
                    let _ = cancel_pending_extension_approvals(
                        &active_prompt.stdin,
                        &active_prompt.pending_approvals,
                        false,
                    )
                    .await;
                }
            });
        }
    }
}

#[derive(Debug)]
struct PiRpcSession {
    cfg: ExecutorConfig,
    identity_stdio: StdioCommand,
    cwd: String,
    workspace: Option<MachineWorkspaceRecord>,
    stdin: SharedStdin,
    stdout: StdoutMessageReceiver,
    child: SharedChild,
    next_request_id: AtomicU64,
    session_key: String,
    executor: String,
    approvals: SharedApprovalBroker,
    pending_approvals: PendingExtensionApprovals,
    invalidated: Arc<AtomicBool>,
    lifecycle: SharedLifecycle,
    closed: bool,
}

impl PiRpcSession {
    async fn start(
        cfg: ExecutorConfig,
        stdio: StdioCommand,
        identity_stdio: StdioCommand,
        workspace: Option<MachineWorkspaceRecord>,
        session_key: String,
        executor: String,
        approvals: SharedApprovalBroker,
    ) -> anyhow::Result<Self> {
        let mut child = stdio
            .spawn()
            .with_context(|| format!("failed to spawn Pi RPC executor: {}", cfg.command))?;
        let stdin = Arc::new(Mutex::new(
            child.stdin.take().context("missing Pi RPC child stdin")?,
        ));
        let stdout = child.stdout.take().context("missing Pi RPC child stdout")?;
        let stderr = child.stderr.take().context("missing Pi RPC child stderr")?;
        let stderr_text = Arc::new(Mutex::new(String::new()));
        let lifecycle = Arc::new(Mutex::new(()));
        let invalidated = Arc::new(AtomicBool::new(false));
        let (stdout_tx, stdout_rx) = mpsc::unbounded_channel();
        let child = Arc::new(Mutex::new(child));
        tokio::spawn(collect_stderr(stderr, stderr_text.clone()));
        tokio::spawn(collect_stdout(
            stdout,
            stderr_text.clone(),
            invalidated.clone(),
            stdout_tx,
        ));
        Ok(Self {
            cfg,
            cwd: stdio.executor_cwd.clone(),
            identity_stdio,
            workspace,
            stdin,
            stdout: stdout_rx,
            child,
            next_request_id: AtomicU64::new(1),
            session_key,
            executor,
            approvals,
            pending_approvals: Arc::new(Mutex::new(HashMap::new())),
            invalidated,
            lifecycle,
            closed: false,
        })
    }

    fn matches(&self, identity_stdio: &StdioCommand) -> bool {
        &self.identity_stdio == identity_stdio
    }

    async fn is_alive(&mut self) -> bool {
        if self.closed {
            return false;
        }
        if self.invalidated.load(Ordering::Acquire) {
            return false;
        }
        matches!(self.child.lock().await.try_wait(), Ok(None))
    }

    fn machine_id(&self) -> &str {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.machine_id.as_str())
            .unwrap_or(self.cfg.machine.as_str())
    }

    fn active_prompt(&self, generation: u64) -> PiActivePrompt {
        PiActivePrompt {
            generation,
            stdin: self.stdin.clone(),
            child: self.child.clone(),
            pending_approvals: self.pending_approvals.clone(),
            abort_sent: Arc::new(AtomicBool::new(false)),
            invalidated: self.invalidated.clone(),
            lifecycle: self.lifecycle.clone(),
        }
    }

    fn prepared_from_state(
        &self,
        state: &Value,
        previous_session_id: Option<&String>,
    ) -> PreparedExecutor {
        let external_session_id = pi_state_session_id(state);
        PreparedExecutor {
            started_new_session: previous_session_id.is_none()
                || external_session_id.as_ref() != previous_session_id,
            external_session_id,
            machine_id: Some(self.machine_id().to_string()),
            cwd: Some(self.cwd.clone()),
            machine_workspace: self.workspace.clone(),
        }
    }

    async fn close(&mut self) {
        if self.closed {
            return;
        }
        let _lifecycle = self.lifecycle.lock().await;
        self.invalidated.store(true, Ordering::Release);
        let _ =
            cancel_pending_extension_approvals(&self.stdin, &self.pending_approvals, false).await;
        let mut child = self.child.lock().await;
        let _ = child.start_kill();
        let _ = child.wait().await;
        self.closed = true;
    }

    async fn get_state(&mut self, cancel: &TurnCancellation) -> anyhow::Result<Value> {
        if cancel.is_cancelled().await {
            anyhow::bail!("Pi RPC prepare cancelled");
        }
        let request_id = match self
            .send_command(json!({
                "type": "get_state",
            }))
            .await
        {
            Ok(request_id) => request_id,
            Err(err) => {
                if self.invalidated.load(Ordering::Acquire) {
                    self.close().await;
                }
                return Err(err);
            }
        };
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    anyhow::bail!("Pi RPC prepare cancelled");
                }
                message = self.read_message() => {
                    let message = message?;
                    if let Some(response) = command_response(&message, &request_id, "get_state") {
                        return response_success_data(response);
                    }
                    if is_extension_ui_request(&message) {
                        self.handle_extension_ui_request(&message, None, None, false, None)
                            .await?;
                    }
                }
            }
        }
    }

    async fn run_prompt(
        &mut self,
        prompt: &str,
        user_id: Option<String>,
        events: &mut dyn ExecutorEventSink,
        cancel: TurnCancellation,
        active_prompt: PiActivePrompt,
    ) -> ExecutorPromptOutcome {
        if cancel.is_cancelled().await {
            self.close().await;
            return ExecutorPromptOutcome::Cancelled;
        }
        let request_id = match self
            .send_command(json!({
                "type": "prompt",
                "message": prompt,
            }))
            .await
        {
            Ok(request_id) => request_id,
            Err(err) => {
                if self.invalidated.load(Ordering::Acquire) {
                    self.close().await;
                }
                return ExecutorPromptOutcome::Failed(err);
            }
        };
        let mut prompt_acknowledged = false;
        let mut agent_ended = false;
        let mut cancelled = false;
        let mut final_text = String::new();
        let mut fallback_final_text = None;
        let (extension_response_tx, mut extension_response_rx) = mpsc::unbounded_channel();
        let result: anyhow::Result<()> = async {
            loop {
                if prompt_acknowledged && agent_ended {
                    break;
                }
                tokio::select! {
                    message = self.read_message() => {
                        let message = message?;
                        if !cancelled && cancel.is_cancelled().await {
                            cancelled = true;
                            let abort_result = active_prompt.abort().await;
                            self.close().await;
                            abort_result?;
                            break;
                        }
                        if let Some(response) = command_response(&message, &request_id, "prompt") {
                            if response.get("success").and_then(Value::as_bool) == Some(true) {
                                prompt_acknowledged = true;
                                continue;
                            }
                            let err = response
                                .get("error")
                                .and_then(Value::as_str)
                                .unwrap_or("Pi RPC prompt command failed");
                            anyhow::bail!("{err}");
                        }
                        if is_response_command(&message, "abort") {
                            continue;
                        }
                        if self
                            .handle_runtime_message(
                                &message,
                                events,
                                user_id.clone(),
                                &mut final_text,
                                &mut fallback_final_text,
                                cancelled,
                                Some(extension_response_tx.clone()),
                            )
                            .await?
                        {
                            agent_ended = true;
                        }
                    }
                    response = extension_response_rx.recv() => {
                        if let Some(response) = response {
                            if !cancelled && cancel.is_cancelled().await {
                                cancelled = true;
                                let abort_result = active_prompt.abort().await;
                                self.close().await;
                                abort_result?;
                                break;
                            }
                            if !cancelled {
                                active_prompt.write_extension_response(response).await?;
                            }
                        }
                    }
                    _ = cancel.cancelled(), if !cancelled => {
                        cancelled = true;
                        let abort_result = active_prompt.abort().await;
                        self.close().await;
                        abort_result?;
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;

        if cancelled || cancel.is_cancelled().await {
            let _ = cancel_pending_extension_approvals(&self.stdin, &self.pending_approvals, false)
                .await;
            self.close().await;
            return ExecutorPromptOutcome::Cancelled;
        }
        match result {
            Ok(()) => {
                if final_text.is_empty()
                    && let Some(fallback) = fallback_final_text
                {
                    final_text = fallback;
                }
                ExecutorPromptOutcome::Completed(ExecutorResponse { final_text })
            }
            Err(err) => {
                let _ =
                    cancel_pending_extension_approvals(&self.stdin, &self.pending_approvals, false)
                        .await;
                if self.invalidated.load(Ordering::Acquire) {
                    self.close().await;
                }
                ExecutorPromptOutcome::Failed(err)
            }
        }
    }

    async fn handle_runtime_message(
        &mut self,
        message: &Value,
        events: &mut dyn ExecutorEventSink,
        user_id: Option<String>,
        final_text: &mut String,
        fallback_final_text: &mut Option<String>,
        cancelled: bool,
        response_tx: Option<ExtensionResponseSender>,
    ) -> anyhow::Result<bool> {
        let message_type = message.get("type").and_then(Value::as_str).unwrap_or("");
        match message_type {
            "message_update" => {
                if let Some(delta) = pi_text_delta(message) {
                    final_text.push_str(&delta);
                    events
                        .send(ExecutorUpdate::new("agent_message_chunk", "", delta, ""))
                        .await?;
                } else if let Some(delta) = pi_thinking_delta(message) {
                    if let Some(update) = project_pi_thinking_progress(delta) {
                        events.send(update).await?;
                    }
                }
            }
            "thinking_delta" => {
                if let Some(delta) = extract_textish(message.get("delta"))
                    .or_else(|| extract_textish(message.get("thinking")))
                {
                    if let Some(update) = project_pi_thinking_progress(delta) {
                        events.send(update).await?;
                    }
                }
            }
            "tool_execution_start" | "tool_start" => {
                events
                    .send(project_pi_tool_event(message, "running"))
                    .await?;
            }
            "tool_execution_update" | "tool_update" => {
                events
                    .send(project_pi_tool_event(message, "running"))
                    .await?;
            }
            "tool_execution_end" | "tool_end" => {
                let status = if message.get("isError").and_then(Value::as_bool) == Some(true)
                    || message.get("is_error").and_then(Value::as_bool) == Some(true)
                {
                    "failed"
                } else {
                    "completed"
                };
                events.send(project_pi_tool_event(message, status)).await?;
            }
            "compaction_start" => {
                let reason = message
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("manual");
                let text = format!("Pi compacting context ({reason})");
                events
                    .send(
                        ExecutorUpdate::new("agent_progress", "Compaction", &text, "running")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                    )
                    .await?;
            }
            "compaction_end" => {
                let text = if message.get("aborted").and_then(Value::as_bool) == Some(true) {
                    "Pi context compaction cancelled".to_string()
                } else if let Some(error) = message.get("errorMessage").and_then(Value::as_str) {
                    format!("Pi context compaction failed: {error}")
                } else {
                    "Pi context compaction finished".to_string()
                };
                events
                    .send(
                        ExecutorUpdate::new("agent_progress", "Compaction", &text, "completed")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                    )
                    .await?;
            }
            "auto_retry_start" => {
                let attempt = message.get("attempt").and_then(Value::as_u64).unwrap_or(0);
                let max_attempts = message
                    .get("maxAttempts")
                    .or_else(|| message.get("max_attempts"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let text = format!("Pi retrying agent turn ({attempt}/{max_attempts})");
                events
                    .send(
                        ExecutorUpdate::new("agent_progress", "Retry", &text, "running")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                    )
                    .await?;
            }
            "auto_retry_end" => {
                if message.get("success").and_then(Value::as_bool) == Some(false) {
                    let text = message
                        .get("finalError")
                        .or_else(|| message.get("final_error"))
                        .and_then(Value::as_str)
                        .map(|err| format!("Pi retry failed: {err}"))
                        .unwrap_or_else(|| "Pi retry failed".to_string());
                    events
                        .send(
                            ExecutorUpdate::new("agent_progress", "Retry", &text, "failed")
                                .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                        )
                        .await?;
                }
            }
            "extension_error" => {
                let text = extension_error_text(message);
                events
                    .send(
                        ExecutorUpdate::new("agent_progress", "Extension", &text, "failed")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                    )
                    .await?;
            }
            "extension_ui_request" => {
                self.handle_extension_ui_request(
                    message,
                    Some(events),
                    user_id,
                    cancelled,
                    response_tx,
                )
                .await?;
            }
            "agent_end" => {
                if agent_end_will_retry(message) {
                    final_text.clear();
                    *fallback_final_text = None;
                    return Ok(false);
                }
                *fallback_final_text = extract_agent_end_final_text(message);
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }

    async fn handle_extension_ui_request(
        &mut self,
        message: &Value,
        mut events: Option<&mut dyn ExecutorEventSink>,
        user_id: Option<String>,
        cancelled: bool,
        response_tx: Option<ExtensionResponseSender>,
    ) -> anyhow::Result<()> {
        let id = message
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        if cancelled && matches!(method, "confirm" | "select" | "input" | "editor") {
            if !id.is_empty() {
                self.write_json_or_invalidate(
                    json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
                )
                .await?;
            }
            return Ok(());
        }
        match method {
            "confirm" => {
                if id.is_empty() {
                    return Ok(());
                }
                let Some(response_tx) = response_tx.clone() else {
                    self.write_json_or_invalidate(
                        json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
                    )
                    .await?;
                    return Ok(());
                };
                let cancel = ApprovalCancellation::new();
                self.pending_approvals
                    .lock()
                    .await
                    .insert(id.clone(), cancel.clone());
                let request = ApprovalRequest {
                    session_key: self.session_key.clone(),
                    executor: self.executor.clone(),
                    requester_user_id: user_id,
                    title: message
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or("Pi confirmation")
                        .to_string(),
                    body: message
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    options: vec![
                        ApprovalOption {
                            id: "allow_once".to_string(),
                            kind: "allow_once".to_string(),
                            name: "Confirm".to_string(),
                            auto_approvable: true,
                        },
                        ApprovalOption {
                            id: "deny".to_string(),
                            kind: "reject_once".to_string(),
                            name: "Cancel".to_string(),
                            auto_approvable: false,
                        },
                    ],
                };
                spawn_confirm_approval_response(
                    self.approvals.clone(),
                    response_tx,
                    self.pending_approvals.clone(),
                    id,
                    cancel,
                    request,
                );
            }
            "select" => {
                if id.is_empty() {
                    return Ok(());
                }
                let options = select_options(message.get("options"));
                if options.is_empty() {
                    self.write_json_or_invalidate(
                        json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
                    )
                    .await?;
                    return Ok(());
                }
                let Some(response_tx) = response_tx.clone() else {
                    self.write_json_or_invalidate(
                        json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
                    )
                    .await?;
                    return Ok(());
                };
                let cancel = ApprovalCancellation::new();
                self.pending_approvals
                    .lock()
                    .await
                    .insert(id.clone(), cancel.clone());
                let mut approval_options = Vec::new();
                let mut option_values = HashMap::new();
                for (index, option) in options.iter().enumerate() {
                    let option_id = format!("option_{}", index + 1);
                    option_values.insert(option_id.clone(), option.value.clone());
                    approval_options.push(ApprovalOption {
                        id: option_id,
                        kind: "select".to_string(),
                        name: option.label.clone(),
                        auto_approvable: false,
                    });
                }
                approval_options.push(ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Cancel".to_string(),
                    auto_approvable: false,
                });
                let body = options
                    .iter()
                    .map(|option| format!("- {} ({})", option.label, option.value))
                    .collect::<Vec<_>>()
                    .join("\n");
                let request = ApprovalRequest {
                    session_key: self.session_key.clone(),
                    executor: self.executor.clone(),
                    requester_user_id: user_id,
                    title: message
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or("Pi selection")
                        .to_string(),
                    body,
                    options: approval_options,
                };
                spawn_select_approval_response(
                    self.approvals.clone(),
                    response_tx,
                    self.pending_approvals.clone(),
                    id,
                    cancel,
                    request,
                    option_values,
                );
            }
            "input" | "editor" => {
                if !id.is_empty() {
                    self.write_json_or_invalidate(
                        json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
                    )
                    .await?;
                }
            }
            "notify" | "setStatus" | "setWidget" | "setTitle" | "set_editor_text" => {
                if let Some(events) = events.as_mut()
                    && let Some(text) = extension_ui_progress_text(message)
                {
                    events
                        .send(
                            ExecutorUpdate::new("agent_progress", "Pi", &text, "")
                                .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
                        )
                        .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn send_command(&mut self, mut command: Value) -> anyhow::Result<String> {
        let id = format!(
            "agent-router-{}",
            self.next_request_id.fetch_add(1, Ordering::Relaxed)
        );
        if let Some(object) = command.as_object_mut() {
            object.insert("id".to_string(), Value::String(id.clone()));
        }
        if let Err(err) = write_json(&self.stdin, command).await {
            self.invalidate_after_protocol_error();
            return Err(err);
        }
        Ok(id)
    }

    async fn write_json_or_invalidate(&mut self, value: Value) -> anyhow::Result<()> {
        if let Err(err) = write_json(&self.stdin, value).await {
            self.invalidate_after_protocol_error();
            return Err(err);
        }
        Ok(())
    }

    async fn read_message(&mut self) -> anyhow::Result<Value> {
        match self.stdout.recv().await {
            Some(message) => message,
            None => {
                self.invalidate_after_protocol_error();
                anyhow::bail!("Pi RPC stdout reader stopped");
            }
        }
    }

    fn invalidate_after_protocol_error(&self) {
        self.invalidated.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectOption {
    value: String,
    label: String,
}

struct DiscardingEventSink;

#[async_trait]
impl ExecutorEventSink for DiscardingEventSink {
    async fn send(&mut self, _update: ExecutorUpdate) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn set_active_pi_prompt(
    active_prompts: &SharedActivePiPrompts,
    key: SessionKey,
    active_prompt: PiActivePrompt,
) {
    active_prompts.lock().await.insert(key, active_prompt);
}

async fn clear_active_pi_prompt(
    active_prompts: &SharedActivePiPrompts,
    key: &SessionKey,
    generation: u64,
) {
    let mut active_prompts = active_prompts.lock().await;
    if active_prompts
        .get(key)
        .is_some_and(|active| active.generation == generation)
    {
        active_prompts.remove(key);
    }
}

async fn collect_stderr(
    mut stderr: tokio::process::ChildStderr,
    stderr_text: Arc<Mutex<String>>,
) -> anyhow::Result<()> {
    let mut buffer = [0; 1024];
    loop {
        let bytes = stderr.read(&mut buffer).await?;
        if bytes == 0 {
            return Ok(());
        }
        let chunk = String::from_utf8_lossy(&buffer[..bytes]);
        stderr_text.lock().await.push_str(&chunk);
    }
}

async fn collect_stdout(
    stdout: ChildStdout,
    stderr_text: Arc<Mutex<String>>,
    invalidated: Arc<AtomicBool>,
    tx: mpsc::UnboundedSender<anyhow::Result<Value>>,
) {
    let mut stdout = BufReader::new(stdout);
    loop {
        let mut line = String::new();
        let bytes = match stdout.read_line(&mut line).await {
            Ok(bytes) => bytes,
            Err(err) => {
                invalidated.store(true, Ordering::Release);
                let _ = tx.send(Err(err.into()));
                return;
            }
        };
        if bytes == 0 {
            invalidated.store(true, Ordering::Release);
            let stderr = stderr_text.lock().await.trim().to_string();
            let err = if stderr.is_empty() {
                anyhow::anyhow!("Pi RPC process closed stdout")
            } else {
                anyhow::anyhow!("Pi RPC process closed stdout: {stderr}")
            };
            let _ = tx.send(Err(err));
            return;
        }
        match serde_json::from_str::<Value>(line.trim_end()) {
            Ok(message) => {
                if tx.send(Ok(message)).is_err() {
                    return;
                }
            }
            Err(err) => {
                invalidated.store(true, Ordering::Release);
                let _ = tx.send(Err(err).with_context(|| {
                    format!("Pi RPC stdout emitted non-JSON line: {}", line.trim_end())
                }));
                return;
            }
        }
    }
}

async fn write_json(stdin: &SharedStdin, value: Value) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(&value)?;
    line.push(b'\n');
    let mut stdin = stdin.lock().await;
    stdin.write_all(&line).await?;
    stdin.flush().await?;
    Ok(())
}

async fn cancel_pending_extension_approvals(
    stdin: &SharedStdin,
    pending_approvals: &PendingExtensionApprovals,
    respond: bool,
) -> anyhow::Result<()> {
    let pending = {
        let mut guard = pending_approvals.lock().await;
        guard.drain().collect::<Vec<_>>()
    };
    for (id, cancel) in pending {
        cancel.cancel();
        if respond {
            write_json(
                stdin,
                json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
            )
            .await?;
        }
    }
    Ok(())
}

fn spawn_confirm_approval_response(
    approvals: SharedApprovalBroker,
    response_tx: ExtensionResponseSender,
    pending_approvals: PendingExtensionApprovals,
    id: String,
    cancel: ApprovalCancellation,
    request: ApprovalRequest,
) {
    tokio::spawn(async move {
        let selection = approvals.request_until_cancelled(request, cancel).await;
        if pending_approvals.lock().await.remove(&id).is_some() {
            let response = match selection {
                Some(ApprovalSelection::Selected(option)) if option != "deny" => {
                    json!({"type": "extension_ui_response", "id": id, "confirmed": true})
                }
                Some(_) => json!({"type": "extension_ui_response", "id": id, "confirmed": false}),
                None => json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
            };
            let _ = response_tx.send(response);
        }
    });
}

fn spawn_select_approval_response(
    approvals: SharedApprovalBroker,
    response_tx: ExtensionResponseSender,
    pending_approvals: PendingExtensionApprovals,
    id: String,
    cancel: ApprovalCancellation,
    request: ApprovalRequest,
    option_values: HashMap<String, String>,
) {
    tokio::spawn(async move {
        let selection = approvals.request_until_cancelled(request, cancel).await;
        if pending_approvals.lock().await.remove(&id).is_some() {
            let response = match selection {
                Some(ApprovalSelection::Selected(option_id)) => {
                    if let Some(value) = option_values.get(&option_id) {
                        json!({"type": "extension_ui_response", "id": id, "value": value})
                    } else {
                        json!({"type": "extension_ui_response", "id": id, "cancelled": true})
                    }
                }
                _ => json!({"type": "extension_ui_response", "id": id, "cancelled": true}),
            };
            let _ = response_tx.send(response);
        }
    });
}

fn pi_rpc_args(
    base_args: &[String],
    previous_session_id: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    reject_lifecycle_args(base_args)?;
    let mut args = base_args.to_vec();
    args.extend(["--mode".to_string(), "rpc".to_string()]);
    if !has_trust_flag(base_args) {
        args.push(DEFAULT_APPROVAL_FLAG.to_string());
    }
    if let Some(id) = previous_session_id.filter(|id| !id.trim().is_empty()) {
        args.push("--session".to_string());
        args.push(id.to_string());
    }
    Ok(args)
}

fn reject_lifecycle_args(args: &[String]) -> anyhow::Result<()> {
    const FORBIDDEN: &[&str] = &[
        "--mode",
        "--print",
        "-p",
        "--continue",
        "-c",
        "--resume",
        "-r",
        "--session",
        "--session-id",
        "--fork",
        "--no-session",
    ];
    for arg in args {
        for flag in FORBIDDEN {
            if arg == flag || arg.starts_with(&format!("{flag}=")) {
                anyhow::bail!(
                    "Pi RPC executor args must not include lifecycle flag `{flag}`; agent-router manages Pi RPC session lifecycle"
                );
            }
        }
    }
    Ok(())
}

fn has_trust_flag(args: &[String]) -> bool {
    args.iter()
        .any(|arg| matches!(arg.as_str(), "--approve" | "-a" | "--no-approve" | "-na"))
}

fn command_response<'a>(message: &'a Value, request_id: &str, command: &str) -> Option<&'a Value> {
    if message.get("type").and_then(Value::as_str) != Some("response") {
        return None;
    }
    if message.get("id").and_then(Value::as_str) != Some(request_id) {
        return None;
    }
    if message.get("command").and_then(Value::as_str) != Some(command) {
        return None;
    }
    Some(message)
}

fn is_response_command(message: &Value, command: &str) -> bool {
    message.get("type").and_then(Value::as_str) == Some("response")
        && message.get("command").and_then(Value::as_str) == Some(command)
}

fn response_success_data(response: &Value) -> anyhow::Result<Value> {
    if response.get("success").and_then(Value::as_bool) == Some(true) {
        return Ok(response.get("data").cloned().unwrap_or(Value::Null));
    }
    let err = response
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("Pi RPC command failed");
    anyhow::bail!("{err}");
}

fn pi_state_session_id(state: &Value) -> Option<String> {
    state
        .get("sessionFile")
        .or_else(|| state.get("session_file"))
        .or_else(|| state.get("sessionId"))
        .or_else(|| state.get("session_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn is_extension_ui_request(message: &Value) -> bool {
    message.get("type").and_then(Value::as_str) == Some("extension_ui_request")
}

fn pi_text_delta(message: &Value) -> Option<String> {
    assistant_event(message, "text_delta")
        .and_then(|event| extract_textish(event.get("delta")))
        .or_else(|| extract_textish(message.get("text_delta")))
}

fn pi_thinking_delta(message: &Value) -> Option<String> {
    assistant_event(message, "thinking_delta")
        .and_then(|event| extract_textish(event.get("delta")))
        .or_else(|| extract_textish(message.get("thinking_delta")))
}

fn project_pi_thinking_progress(delta: String) -> Option<ExecutorUpdate> {
    let text = one_line(&delta);
    if text.is_empty() {
        return None;
    }
    let text = truncate_chars(&text, 240);
    Some(
        ExecutorUpdate::new("agent_thought_chunk", "Progress", text.clone(), "")
            .with_channel_event(ExecutorChannelEvent::agent_progress(text)),
    )
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn assistant_event<'a>(message: &'a Value, event_type: &str) -> Option<&'a Value> {
    let event = message
        .get("assistantMessageEvent")
        .or_else(|| message.get("assistant_message_event"))?;
    (event.get("type").and_then(Value::as_str) == Some(event_type)).then_some(event)
}

fn project_pi_tool_event(message: &Value, status: &str) -> ExecutorUpdate {
    let title = message
        .get("toolName")
        .or_else(|| message.get("tool_name"))
        .and_then(Value::as_str)
        .unwrap_or("Tool")
        .to_string();
    let text = tool_event_text(message, status);
    ExecutorUpdate::new("tool_call", title.clone(), text.clone(), status)
        .with_transcript_summary(format!("{title}: status: {status}"))
        .with_channel_event(ExecutorChannelEvent::tool_call(title, text))
}

fn tool_event_text(message: &Value, status: &str) -> String {
    let mut lines = Vec::new();
    if let Some(args) = message.get("args") {
        lines.push(format!("args: {}", compact_json(args)));
    }
    if let Some(partial) = message
        .get("partialResult")
        .or_else(|| message.get("partial_result"))
    {
        lines.push(format!(
            "partial: {}",
            extract_textish(Some(partial)).unwrap_or_else(|| compact_json(partial))
        ));
    }
    if let Some(result) = message.get("result") {
        lines.push(format!(
            "result: {}",
            extract_textish(Some(result)).unwrap_or_else(|| compact_json(result))
        ));
    }
    lines.push(format!("status: {status}"));
    lines.join("\n")
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_string())
}

fn extract_textish(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let text = items
                .iter()
                .filter_map(|item| extract_textish(Some(item)))
                .collect::<Vec<_>>()
                .join("");
            (!text.is_empty()).then_some(text)
        }
        Value::Object(object) => [
            "text",
            "delta",
            "message",
            "statusText",
            "status_text",
            "content",
            "value",
        ]
        .iter()
        .find_map(|key| {
            object
                .get(*key)
                .and_then(|value| extract_textish(Some(value)))
        }),
        _ => None,
    }
}

fn extract_agent_end_final_text(message: &Value) -> Option<String> {
    let messages = message.get("messages")?.as_array()?;
    messages
        .iter()
        .rev()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .and_then(|message| extract_textish(message.get("content")))
}

fn agent_end_will_retry(message: &Value) -> bool {
    message
        .get("willRetry")
        .or_else(|| message.get("will_retry"))
        .and_then(Value::as_bool)
        == Some(true)
}

fn extension_error_text(message: &Value) -> String {
    let extension = message
        .get("extensionPath")
        .or_else(|| message.get("extension_path"))
        .and_then(Value::as_str)
        .unwrap_or("extension");
    let event = message
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or("event");
    let error = message
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    format!("Pi extension `{extension}` failed during {event}: {error}")
}

fn extension_ui_progress_text(message: &Value) -> Option<String> {
    match message.get("method").and_then(Value::as_str)? {
        "notify" => extract_textish(message.get("message")),
        "setStatus" => extract_textish(message.get("statusText"))
            .or_else(|| extract_textish(message.get("status_text")))
            .or_else(|| {
                message
                    .get("statusKey")
                    .or_else(|| message.get("status_key"))
                    .and_then(Value::as_str)
                    .map(|key| format!("Pi status updated: {key}"))
            }),
        "setWidget" => extract_textish(message.get("widgetLines"))
            .or_else(|| extract_textish(message.get("widget_lines")))
            .or_else(|| {
                message
                    .get("widgetKey")
                    .or_else(|| message.get("widget_key"))
                    .and_then(Value::as_str)
                    .map(|key| format!("Pi widget updated: {key}"))
            }),
        "setTitle" => extract_textish(message.get("title")),
        "set_editor_text" => Some("Pi updated editor text".to_string()),
        _ => None,
    }
}

fn select_options(value: Option<&Value>) -> Vec<SelectOption> {
    let Some(Value::Array(items)) = value else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| match item {
            Value::String(value) => Some(SelectOption {
                value: value.clone(),
                label: value.clone(),
            }),
            Value::Object(object) => {
                let value = object
                    .get("value")
                    .or_else(|| object.get("id"))
                    .and_then(Value::as_str)?;
                let label = object
                    .get("label")
                    .or_else(|| object.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or(value);
                Some(SelectOption {
                    value: value.to_string(),
                    label: label.to_string(),
                })
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::Path, sync::Arc, time::Duration};

    use tempfile::TempDir;
    use tokio::time::sleep;

    use super::*;
    use crate::{
        executor::{
            ExecutorChannelEventKind, ExecutorPrepareRequest, InterruptReason,
            test_support::CollectingExecutorEventSink,
        },
        machine::LOCAL_MACHINE_ID,
    };

    const FAKE_PI: &str = r#"
import json
import os
import sys
import time

scenario = os.environ.get("PI_FAKE_SCENARIO", "stream")
log_path = os.environ.get("PI_FAKE_LOG")
session_file = os.environ.get("PI_FAKE_SESSION_FILE", "/tmp/pi-session.jsonl")
get_state_count = 0

def log(obj):
    if not log_path:
        return
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True) + "\n")

def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()

def response(cmd, command, success=True, data=None, error=None):
    obj = {"type": "response", "command": command, "success": success}
    if cmd.get("id") is not None:
        obj["id"] = cmd.get("id")
    if data is not None:
        obj["data"] = data
    if error is not None:
        obj["error"] = error
    send(obj)

def agent_end(text="", will_retry=False):
    send({
        "type": "agent_end",
        "willRetry": will_retry,
        "messages": [
            {"role": "assistant", "content": [{"type": "text", "text": text}]}
        ],
    })

def text_delta(text):
    send({
        "type": "message_update",
        "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
        "assistantMessageEvent": {"type": "text_delta", "delta": text},
    })

def thinking_delta(text):
    send({
        "type": "message_update",
        "message": {"role": "assistant", "content": [{"type": "thinking", "thinking": text}]},
        "assistantMessageEvent": {"type": "thinking_delta", "delta": text},
    })

def standalone_thinking_delta(text):
    send({"type": "thinking_delta", "delta": text})

def partial_text_delta(text):
    line = json.dumps({
        "type": "message_update",
        "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
        "assistantMessageEvent": {"type": "text_delta", "delta": text},
    }, separators=(",", ":"))
    cut = max(1, len(line) // 2)
    sys.stdout.write(line[:cut])
    sys.stdout.flush()
    return line[cut:] + "\n"

def read_json_line():
    line = sys.stdin.readline()
    if not line:
        return None
    try:
        obj = json.loads(line)
    except Exception as exc:
        log({"stdin_parse_error": str(exc), "line": line})
        return {}
    log({"stdin": obj})
    return obj

def close_stdin_fd():
    try:
        os.close(0)
    except OSError:
        pass

def wait_for_extension_response_or_abort():
    while True:
        msg = read_json_line()
        if msg is None:
            return None
        if msg.get("type") == "extension_ui_response":
            return msg
        if msg.get("type") == "abort":
            response(msg, "abort")
            agent_end("")
            return {"cancelled": True}

log({"argv": sys.argv[1:]})

while True:
    cmd = read_json_line()
    if cmd is None:
        break
    typ = cmd.get("type")
    if typ == "get_state":
        get_state_count += 1
        if scenario == "drop_first_get_state" and get_state_count == 1:
            log({"event": "drop_first_get_state"})
            continue
        if scenario == "drop_second_get_state" and get_state_count == 2:
            log({"event": "drop_second_get_state"})
            continue
        if scenario == "hang_get_state" and "--session" not in sys.argv:
            log({"event": "hang_get_state"})
            time.sleep(60)
            break
        response(cmd, "get_state", data={
            "sessionFile": session_file,
            "sessionId": "fake-session-id",
            "isStreaming": False,
            "isCompacting": False,
            "messageCount": 0,
            "pendingMessageCount": 0,
        })
        if scenario == "exit_after_get_state" and "--session" not in sys.argv:
            log({"event": "exit_after_get_state"})
            sys.exit(0)
    elif typ == "prompt":
        if scenario == "prompt_fail":
            response(cmd, "prompt", success=False, error="preflight failed")
        elif scenario == "exit_on_prompt":
            sys.exit(0)
        elif scenario == "stderr_exit_on_prompt":
            sys.stderr.write("fatal pi error\n")
            sys.stderr.flush()
            time.sleep(0.05)
            sys.exit(2)
        elif scenario == "non_json_prompt" and "--session" not in sys.argv:
            sys.stdout.write("not json\n")
            sys.stdout.flush()
        elif scenario == "cancel":
            response(cmd, "prompt")
            while True:
                msg = read_json_line()
                if msg is None:
                    break
                if msg.get("type") == "abort":
                    response(msg, "abort")
                    agent_end("")
                    break
        elif scenario == "abort_without_agent_end_then_stream" and "--session" not in sys.argv:
            response(cmd, "prompt")
            while True:
                msg = read_json_line()
                if msg is None:
                    break
                if msg.get("type") == "abort":
                    response(msg, "abort")
                    time.sleep(60)
                    break
        elif scenario == "late_approval_after_abort":
            response(cmd, "prompt")
            while True:
                msg = read_json_line()
                if msg is None:
                    break
                if msg.get("type") == "abort":
                    response(msg, "abort")
                    send({
                        "type": "extension_ui_request",
                        "id": "confirm-late",
                        "method": "confirm",
                        "title": "Late approval",
                        "message": "This should be cancelled without publishing.",
                    })
                    resp = wait_for_extension_response_or_abort() or {"cancelled": True}
                    text_delta("cancelled" if resp.get("cancelled") else "unexpected")
                    agent_end("")
                    break
        elif scenario == "close_stdin_after_approval_request":
            response(cmd, "prompt")
            send({
                "type": "extension_ui_request",
                "id": "confirm-closed-stdin",
                "method": "confirm",
                "title": "Run risky action",
                "message": "Allow Pi extension action?",
            })
            log({"event": "closed_stdin_after_approval_request"})
            close_stdin_fd()
            time.sleep(60)
        elif scenario == "close_stdin_after_prompt_ack":
            response(cmd, "prompt")
            log({"event": "closed_stdin_after_prompt_ack"})
            close_stdin_fd()
            time.sleep(60)
        elif scenario == "partial_stdout_during_approval":
            response(cmd, "prompt")
            send({
                "type": "extension_ui_request",
                "id": "confirm-partial-stdout",
                "method": "confirm",
                "title": "Run risky action",
                "message": "Allow Pi extension action?",
            })
            suffix = partial_text_delta("approved")
            resp = wait_for_extension_response_or_abort() or {"cancelled": True}
            if resp.get("confirmed"):
                sys.stdout.write(suffix)
                sys.stdout.flush()
                agent_end("approved")
            else:
                sys.stdout.write(suffix.replace("approved", "cancelled"))
                sys.stdout.flush()
                agent_end("cancelled")
        elif scenario == "will_retry":
            response(cmd, "prompt")
            text_delta("old")
            agent_end("old", True)
            send({"type": "auto_retry_start", "attempt": 1, "maxAttempts": 2, "delayMs": 1, "errorMessage": "try again"})
            send({"type": "auto_retry_end", "success": True, "attempt": 1})
            text_delta("new")
            agent_end("new")
        elif scenario == "approval_confirm":
            response(cmd, "prompt")
            send({
                "type": "extension_ui_request",
                "id": "confirm-1",
                "method": "confirm",
                "title": "Run risky action",
                "message": "Allow Pi extension action?",
            })
            resp = wait_for_extension_response_or_abort() or {"cancelled": True}
            if resp.get("cancelled"):
                text_delta("cancelled")
                agent_end("cancelled")
            elif resp.get("confirmed"):
                text_delta("approved")
                agent_end("approved")
            else:
                text_delta("denied")
                agent_end("denied")
        elif scenario == "approval_select":
            response(cmd, "prompt")
            send({
                "type": "extension_ui_request",
                "id": "select-1",
                "method": "select",
                "title": "Pick target",
                "options": [
                    {"label": "First", "value": "first"},
                    {"label": "Second", "value": "second"},
                ],
            })
            resp = wait_for_extension_response_or_abort() or {"cancelled": True}
            if resp.get("cancelled"):
                text_delta("cancelled")
                agent_end("cancelled")
            else:
                text_delta(resp.get("value", "missing"))
                agent_end(resp.get("value", "missing"))
        elif scenario == "approval_select_special":
            response(cmd, "prompt")
            send({
                "type": "extension_ui_request",
                "id": "select-1",
                "method": "select",
                "title": "Pick target",
                "options": [
                    {"label": "Space Value", "value": "value with spaces"},
                    {"label": "Deny Value", "value": "deny me"},
                ],
            })
            resp = wait_for_extension_response_or_abort() or {"cancelled": True}
            if resp.get("cancelled"):
                text_delta("cancelled")
                agent_end("cancelled")
            else:
                text_delta(resp.get("value", "missing"))
                agent_end(resp.get("value", "missing"))
        else:
            response(cmd, "prompt")
            text_delta("hello ")
            thinking_delta("thinking")
            standalone_thinking_delta("standalone thinking")
            send({"type": "tool_execution_start", "toolCallId": "tool-1", "toolName": "bash", "args": {"command": "echo hi"}})
            send({"type": "tool_execution_update", "toolCallId": "tool-1", "toolName": "bash", "args": {"command": "echo hi"}, "partialResult": {"content": [{"type": "text", "text": "hi"}]}})
            send({"type": "tool_execution_end", "toolCallId": "tool-1", "toolName": "bash", "result": {"content": [{"type": "text", "text": "hi"}]}, "isError": False})
            text_delta("world")
            agent_end("hello world")
    elif typ == "abort":
        response(cmd, "abort")
        agent_end("")
"#;

    struct FakePi {
        _tmp: TempDir,
        script: std::path::PathBuf,
        log: std::path::PathBuf,
        session_file: String,
    }

    impl FakePi {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let script = tmp.path().join("fake_pi.py");
            let log = tmp.path().join("fake_pi.log");
            fs::write(&script, FAKE_PI).unwrap();
            let session_file = tmp.path().join("session.jsonl").display().to_string();
            Self {
                _tmp: tmp,
                script,
                log,
                session_file,
            }
        }

        fn manager(&self, scenario: &str) -> (PiRpcExecutorManager, Arc<ApprovalBroker>) {
            let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
            let mut env = BTreeMap::new();
            env.insert("PI_FAKE_SCENARIO".to_string(), scenario.to_string());
            env.insert("PI_FAKE_LOG".to_string(), self.log.display().to_string());
            env.insert(
                "PI_FAKE_SESSION_FILE".to_string(),
                self.session_file.clone(),
            );
            let executors = BTreeMap::from([(
                "pi".to_string(),
                ExecutorConfig {
                    name: "pi".to_string(),
                    protocol: ExecutorProtocol::PiRpc,
                    machine: LOCAL_MACHINE_ID.to_string(),
                    command: "python3".to_string(),
                    args: vec![self.script.display().to_string()],
                    cwd: None,
                    env,
                },
            )]);
            (
                PiRpcExecutorManager::with_machines(
                    executors,
                    MachineRegistry::local_default(),
                    approvals.clone(),
                ),
                approvals,
            )
        }

        fn log_entries(&self) -> Vec<Value> {
            read_jsonl(&self.log)
        }
    }

    fn turn(generation: u64) -> ExecutorTurnRef {
        ExecutorTurnRef {
            session_key: "session-1".to_string(),
            executor: "pi".to_string(),
            generation,
        }
    }

    async fn prepare(
        manager: &PiRpcExecutorManager,
        previous_session_id: Option<String>,
    ) -> PreparedExecutor {
        manager
            .prepare(
                ExecutorPrepareRequest {
                    turn: turn(1),
                    cwd: None,
                    previous_session_id,
                },
                TurnCancellation::default(),
            )
            .await
            .unwrap()
    }

    fn read_jsonl(path: &Path) -> Vec<Value> {
        let Ok(text) = fs::read_to_string(path) else {
            return Vec::new();
        };
        text.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect()
    }

    async fn wait_for_log<F>(path: &Path, predicate: F)
    where
        F: Fn(&[Value]) -> bool,
    {
        for _ in 0..100 {
            let entries = read_jsonl(path);
            if predicate(&entries) {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for fake Pi log condition");
    }

    fn stdin_types(entries: &[Value]) -> Vec<String> {
        entries
            .iter()
            .filter_map(|entry| {
                entry
                    .get("stdin")
                    .and_then(|stdin| stdin.get("type"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect()
    }

    fn has_event(entries: &[Value], event: &str) -> bool {
        entries
            .iter()
            .any(|entry| entry.get("event").and_then(Value::as_str) == Some(event))
    }

    #[test]
    fn pi_rpc_args_append_lifecycle_defaults() {
        assert_eq!(
            pi_rpc_args(&[], None).unwrap(),
            ["--mode", "rpc", "--approve"]
        );
        assert_eq!(
            pi_rpc_args(&[], Some("session-file")).unwrap(),
            ["--mode", "rpc", "--approve", "--session", "session-file"]
        );
    }

    #[test]
    fn pi_rpc_args_respect_explicit_trust_flags() {
        assert_eq!(
            pi_rpc_args(&["--no-approve".to_string()], None).unwrap(),
            ["--no-approve", "--mode", "rpc"]
        );
        assert_eq!(
            pi_rpc_args(&["-a".to_string()], None).unwrap(),
            ["-a", "--mode", "rpc"]
        );
    }

    #[test]
    fn pi_rpc_args_reject_lifecycle_conflicts() {
        for flag in [
            "--mode",
            "--mode=rpc",
            "--print",
            "-p",
            "--continue",
            "-c",
            "--resume",
            "-r",
            "--session",
            "--session-id",
            "--fork",
            "--no-session",
        ] {
            let err = pi_rpc_args(&[flag.to_string()], None).unwrap_err();
            assert!(
                err.to_string()
                    .contains("agent-router manages Pi RPC session lifecycle")
            );
        }
    }

    #[tokio::test]
    async fn prepare_captures_session_file_and_resumes_with_session_arg() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("stream");

        let first = prepare(&manager, None).await;
        assert_eq!(
            first.external_session_id.as_deref(),
            Some(fake.session_file.as_str())
        );
        assert!(first.started_new_session);

        assert!(manager.discard_session("session-1", "pi").await);
        let second = prepare(&manager, first.external_session_id.clone()).await;
        assert_eq!(
            second.external_session_id.as_deref(),
            Some(fake.session_file.as_str())
        );
        assert!(!second.started_new_session);

        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert!(argv_entries.len() >= 2);
        let second_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(second_argv.iter().any(|arg| arg == "--session"));
        assert!(second_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn prepare_cancellation_keeps_published_session_for_replacement() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("drop_second_get_state");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;

        let cancel = TurnCancellation::default();
        let prepare_cancel = cancel.clone();
        let prepare_manager = manager.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_manager
                .prepare(
                    ExecutorPrepareRequest {
                        turn: turn(2),
                        cwd: None,
                        previous_session_id: first.external_session_id.clone(),
                    },
                    prepare_cancel,
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            has_event(entries, "drop_second_get_state")
        })
        .await;
        cancel.cancel(InterruptReason::ReplacedByNewMessage).await;

        let err = tokio::time::timeout(Duration::from_secs(2), prepare_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(err.to_string().contains("Pi RPC prepare cancelled"));

        let prepared = tokio::time::timeout(
            Duration::from_secs(2),
            manager.prepare(
                ExecutorPrepareRequest {
                    turn: turn(3),
                    cwd: None,
                    previous_session_id: Some(fake.session_file.clone()),
                },
                TurnCancellation::default(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            prepared.external_session_id.as_deref(),
            Some(fake.session_file.as_str())
        );
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert_eq!(argv_entries.len(), 1);
        assert!(manager.discard_session("session-1", "pi").await);
    }

    #[tokio::test]
    async fn prompt_streams_text_thinking_tool_and_final_response() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("stream");
        prepare(&manager, None).await;

        let mut events = CollectingExecutorEventSink::default();
        let outcome = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await;

        let response = outcome.unwrap();
        assert_eq!(response.final_text, "hello world");
        let chunks = events
            .updates
            .iter()
            .filter(|update| update.kind == "agent_message_chunk")
            .map(|update| update.text.as_str())
            .collect::<Vec<_>>();
        assert_eq!(chunks, ["hello ", "world"]);
        let thinking_events = events
            .updates
            .iter()
            .filter(|update| update.kind == "agent_thought_chunk")
            .collect::<Vec<_>>();
        assert_eq!(thinking_events.len(), 2);
        assert_eq!(
            thinking_events
                .iter()
                .map(|update| {
                    update
                        .channel_event
                        .as_ref()
                        .map(|event| (event.kind, event.text.as_str()))
                })
                .collect::<Vec<_>>(),
            [
                Some((ExecutorChannelEventKind::AgentProgress, "thinking")),
                Some((
                    ExecutorChannelEventKind::AgentProgress,
                    "standalone thinking"
                )),
            ]
        );
        assert!(
            events
                .updates
                .iter()
                .any(|update| { update.kind == "tool_call" && update.channel_event.is_some() })
        );
    }

    #[tokio::test]
    async fn agent_end_with_will_retry_does_not_finish_turn() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("will_retry");
        prepare(&manager, None).await;

        let mut events = CollectingExecutorEventSink::default();
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "retry".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap();

        assert_eq!(response.final_text, "new");
        assert!(
            events
                .updates
                .iter()
                .any(|update| update.kind == "agent_progress" && update.title == "Retry")
        );
    }

    #[tokio::test]
    async fn prompt_command_failure_returns_failed_outcome() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("prompt_fail");
        prepare(&manager, None).await;
        let mut events = CollectingExecutorEventSink::default();

        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("preflight failed"));
    }

    #[tokio::test]
    async fn prompt_process_exit_returns_failed_outcome() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("exit_on_prompt");
        prepare(&manager, None).await;
        let mut events = CollectingExecutorEventSink::default();

        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("closed stdout"));
    }

    #[tokio::test]
    async fn prompt_process_exit_includes_stderr_in_failure() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("stderr_exit_on_prompt");
        prepare(&manager, None).await;
        let mut events = CollectingExecutorEventSink::default();

        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("fatal pi error"));
    }

    #[tokio::test]
    async fn prompt_non_json_stdout_returns_failed_outcome() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("non_json_prompt");
        let first = prepare(&manager, None).await;
        let mut events = CollectingExecutorEventSink::default();

        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("non-JSON"));

        prepare(&manager, first.external_session_id).await;
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(2),
                    prompt: "after non-json".to_string(),
                    user_id: None,
                },
                &mut CollectingExecutorEventSink::default(),
                TurnCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(response.final_text, "hello world");
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        let restarted_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(restarted_argv.iter().any(|arg| arg == "--session"));
        assert!(restarted_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn prompt_write_failure_closes_session_before_reuse() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("exit_after_get_state");
        let first = prepare(&manager, None).await;
        wait_for_log(&fake.log, |entries| {
            has_event(entries, "exit_after_get_state")
        })
        .await;
        let mut events = CollectingExecutorEventSink::default();

        let err = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(1),
                    prompt: "hello".to_string(),
                    user_id: None,
                },
                &mut events,
                TurnCancellation::default(),
            )
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());

        prepare(&manager, first.external_session_id).await;
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert!(argv_entries.len() >= 2);
        let restarted_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(restarted_argv.iter().any(|arg| arg == "--session"));
        assert!(restarted_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn extension_response_write_failure_closes_session_before_reuse() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("close_stdin_after_approval_request");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        wait_for_log(&fake.log, |entries| {
            has_event(entries, "closed_stdin_after_approval_request")
        })
        .await;
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let err = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(!err.to_string().is_empty());
        assert!(!approvals.has_pending(&approval.id).await);

        prepare(&manager, first.external_session_id).await;
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert!(argv_entries.len() >= 2);
        let restarted_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(restarted_argv.iter().any(|arg| arg == "--session"));
        assert!(restarted_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn interrupt_abort_write_failure_closes_session_before_reuse() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("close_stdin_after_prompt_ack");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "hello".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            has_event(entries, "closed_stdin_after_prompt_ack")
        })
        .await;
        let err = manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn(1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap_err();
        assert!(!err.to_string().is_empty());

        let prompt_err = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(!prompt_err.to_string().is_empty());

        prepare(&manager, first.external_session_id).await;
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert!(argv_entries.len() >= 2);
        let restarted_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(restarted_argv.iter().any(|arg| arg == "--session"));
        assert!(restarted_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn cancellation_sends_abort_and_returns_cancelled() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("cancel");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let cancel = TurnCancellation::default();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "hello".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            stdin_types(entries).iter().any(|ty| ty == "prompt")
        })
        .await;
        cancel.cancel(InterruptReason::UserStop).await;
        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn(1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
    }

    #[tokio::test]
    async fn confirm_approval_responds_after_approve_command() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_confirm");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "approved");
        let entries = fake.log_entries();
        assert!(entries.iter().any(|entry| {
            entry
                .get("stdin")
                .is_some_and(|stdin| stdin.get("confirmed").and_then(Value::as_bool) == Some(true))
        }));
    }

    #[tokio::test]
    async fn approval_response_does_not_drop_partial_stdout_line() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("partial_stdout_during_approval");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            let response = prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await;
            (response, events)
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {}", approval.id),
                Some("U1"),
            )
            .await
            .unwrap();

        let (response, events) = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.unwrap().final_text, "approved");
        assert!(
            events.updates.iter().any(|update| {
                update.kind == "agent_message_chunk" && update.text == "approved"
            })
        );
    }

    #[tokio::test]
    async fn select_approval_responds_cancelled_after_deny_command() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_select");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs selection".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        approvals
            .resolve_command("session-1", &format!("/deny {}", approval.id), Some("U1"))
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "cancelled");
        let entries = fake.log_entries();
        assert!(entries.iter().any(|entry| {
            entry
                .get("stdin")
                .is_some_and(|stdin| stdin.get("cancelled").and_then(Value::as_bool) == Some(true))
        }));
    }

    #[tokio::test]
    async fn select_approval_can_choose_explicit_option() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_select");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs selection".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        let second_option_id = approval
            .options
            .iter()
            .find(|option| option.name == "Second")
            .map(|option| option.id.clone())
            .unwrap();
        assert_ne!(second_option_id, "second");
        assert!(
            approval
                .render_text()
                .contains(&format!("/approve {} {}", approval.id, second_option_id))
        );
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {} {}", approval.id, second_option_id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "second");
        let entries = fake.log_entries();
        assert!(entries.iter().any(|entry| {
            entry.get("stdin").is_some_and(|stdin| {
                stdin.get("type").and_then(Value::as_str) == Some("extension_ui_response")
                    && stdin.get("value").and_then(Value::as_str) == Some("second")
            })
        }));
    }

    #[tokio::test]
    async fn select_approval_maps_opaque_option_id_to_pi_value() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_select_special");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs special selection".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        let deny_value_option_id = approval
            .options
            .iter()
            .find(|option| option.name == "Deny Value")
            .map(|option| option.id.clone())
            .unwrap();
        assert_ne!(deny_value_option_id, "deny me");
        approvals
            .resolve_command(
                "session-1",
                &format!("/approve {} {}", approval.id, deny_value_option_id),
                Some("U1"),
            )
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.final_text, "deny me");
        let entries = fake.log_entries();
        assert!(entries.iter().any(|entry| {
            entry.get("stdin").is_some_and(|stdin| {
                stdin.get("type").and_then(Value::as_str) == Some("extension_ui_response")
                    && stdin.get("value").and_then(Value::as_str) == Some("deny me")
            })
        }));
    }

    #[tokio::test]
    async fn interrupt_cancels_pending_extension_approval() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_confirm");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let cancel = TurnCancellation::default();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(approvals.has_pending(&approval.id).await);
        cancel.cancel(InterruptReason::UserStop).await;
        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn(1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
        assert!(!approvals.has_pending(&approval.id).await);
    }

    #[tokio::test]
    async fn discard_session_cancels_pending_extension_approval() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_confirm");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(approvals.has_pending(&approval.id).await);
        let discarded = tokio::time::timeout(
            Duration::from_secs(2),
            manager.discard_session("session-1", "pi"),
        )
        .await
        .unwrap();
        assert!(discarded);

        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            outcome,
            ExecutorPromptOutcome::Completed(_) | ExecutorPromptOutcome::Failed(_)
        ));
        assert!(!approvals.has_pending(&approval.id).await);
    }

    #[tokio::test]
    async fn discard_session_cleans_up_approval_after_prompt_future_is_dropped() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_confirm");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(approvals.has_pending(&approval.id).await);
        prompt.abort();
        let _ = prompt.await;
        assert!(manager.discard_session("session-1", "pi").await);

        for _ in 0..100 {
            if !approvals.has_pending(&approval.id).await {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(!approvals.has_pending(&approval.id).await);
    }

    #[tokio::test]
    async fn dropped_prompt_future_cleans_up_pending_extension_approval() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("approval_confirm");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;
        let mut prompts = approvals.subscribe();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "needs approval".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        let approval = tokio::time::timeout(Duration::from_secs(2), prompts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(approvals.has_pending(&approval.id).await);
        prompt.abort();
        let _ = prompt.await;

        let restarted = prepare(&manager, first.external_session_id.clone()).await;
        assert_eq!(
            restarted.external_session_id.as_deref(),
            Some(fake.session_file.as_str())
        );

        for _ in 0..100 {
            if !approvals.has_pending(&approval.id).await {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(!approvals.has_pending(&approval.id).await);

        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        assert!(argv_entries.len() >= 2);
        let restarted_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(restarted_argv.iter().any(|arg| arg == "--session"));
        assert!(restarted_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn cancellation_cancels_late_extension_request_after_abort() {
        let fake = FakePi::new();
        let (manager, approvals) = fake.manager("late_approval_after_abort");
        let manager = Arc::new(manager);
        prepare(&manager, None).await;
        let cancel = TurnCancellation::default();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "hello".to_string(),
                        user_id: Some("U1".to_string()),
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            stdin_types(entries).iter().any(|ty| ty == "prompt")
        })
        .await;
        cancel.cancel(InterruptReason::UserStop).await;
        manager
            .interrupt(ExecutorInterruptRequest {
                turn: turn(1),
                reason: InterruptReason::UserStop,
            })
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));
        assert!(!approvals.has_pending_for_session("session-1").await);
    }

    #[tokio::test]
    async fn cancellation_closes_pi_when_abort_has_no_agent_end() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("abort_without_agent_end_then_stream");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;
        let cancel = TurnCancellation::default();
        let prompt_cancel = cancel.clone();
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "hello".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    prompt_cancel,
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            stdin_types(entries).iter().any(|ty| ty == "prompt")
        })
        .await;
        cancel.cancel(InterruptReason::UserStop).await;

        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Cancelled));

        prepare(&manager, first.external_session_id).await;
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(2),
                    prompt: "after cancel".to_string(),
                    user_id: None,
                },
                &mut CollectingExecutorEventSink::default(),
                TurnCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(response.final_text, "hello world");

        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        let second_argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(second_argv.iter().any(|arg| arg == "--session"));
        assert!(second_argv.iter().any(|arg| arg == &fake.session_file));
    }

    #[tokio::test]
    async fn discard_closes_pi_when_abort_would_not_end_turn() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("abort_without_agent_end_then_stream");
        let manager = Arc::new(manager);
        let first = prepare(&manager, None).await;
        let prompt_manager = manager.clone();
        let prompt = tokio::spawn(async move {
            let mut events = CollectingExecutorEventSink::default();
            prompt_manager
                .prompt(
                    ExecutorPromptRequest {
                        turn: turn(1),
                        prompt: "hello".to_string(),
                        user_id: None,
                    },
                    &mut events,
                    TurnCancellation::default(),
                )
                .await
        });

        wait_for_log(&fake.log, |entries| {
            stdin_types(entries).iter().any(|ty| ty == "prompt")
        })
        .await;
        let discarded = tokio::time::timeout(
            Duration::from_secs(2),
            manager.discard_session("session-1", "pi"),
        )
        .await
        .unwrap();
        assert!(discarded);
        let outcome = tokio::time::timeout(Duration::from_secs(2), prompt)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ExecutorPromptOutcome::Failed(_)));

        prepare(&manager, first.external_session_id).await;
        let response = manager
            .prompt(
                ExecutorPromptRequest {
                    turn: turn(2),
                    prompt: "after discard".to_string(),
                    user_id: None,
                },
                &mut CollectingExecutorEventSink::default(),
                TurnCancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(response.final_text, "hello world");
    }

    #[tokio::test]
    async fn slash_command_uses_pi_prompt_command_path() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("stream");

        let outcome = manager
            .slash_command(ExecutorSlashCommandRequest {
                session_key: "session-1".to_string(),
                executor: "pi".to_string(),
                cwd: None,
                turn: None,
                cancel: TurnCancellation::default(),
                previous_session_id: None,
                command: ExecutorSlashCommand {
                    raw: "/status --json".to_string(),
                    name: "status".to_string(),
                    args: "--json".to_string(),
                },
                user_id: None,
            })
            .await;

        let response = match outcome {
            ExecutorSlashCommandOutcome::CompletedWithSession { response, prepared } => {
                assert_eq!(
                    prepared.external_session_id.as_deref(),
                    Some(fake.session_file.as_str())
                );
                response
            }
            other => panic!("unexpected slash command outcome: {other:?}"),
        };
        assert_eq!(response.final_text, "hello world");
        let entries = fake.log_entries();
        assert!(entries.iter().any(|entry| {
            entry.get("stdin").is_some_and(|stdin| {
                stdin.get("type").and_then(Value::as_str) == Some("prompt")
                    && stdin.get("message").and_then(Value::as_str) == Some("/status --json")
            })
        }));
    }

    #[tokio::test]
    async fn slash_command_restores_previous_pi_session_id() {
        let fake = FakePi::new();
        let (manager, _) = fake.manager("stream");

        let outcome = manager
            .slash_command(ExecutorSlashCommandRequest {
                session_key: "session-1".to_string(),
                executor: "pi".to_string(),
                cwd: None,
                turn: None,
                cancel: TurnCancellation::default(),
                previous_session_id: Some(fake.session_file.clone()),
                command: ExecutorSlashCommand {
                    raw: "/status".to_string(),
                    name: "status".to_string(),
                    args: String::new(),
                },
                user_id: None,
            })
            .await;

        assert!(matches!(
            outcome,
            ExecutorSlashCommandOutcome::CompletedWithSession { .. }
        ));
        let argv_entries = fake
            .log_entries()
            .into_iter()
            .filter_map(|entry| entry.get("argv").cloned())
            .collect::<Vec<_>>();
        let argv = argv_entries.last().unwrap().as_array().unwrap();
        assert!(argv.iter().any(|arg| arg == "--session"));
        assert!(argv.iter().any(|arg| arg == &fake.session_file));
    }
}
