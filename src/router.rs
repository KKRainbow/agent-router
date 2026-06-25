mod turns;

use std::{
    collections::HashMap,
    future::Future,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    approval::{
        ApprovalBroker, ApprovalPolicy, ApprovalRequest, ApprovalSelection, SharedApprovalBroker,
    },
    executor::{
        ExecutorBackend, ExecutorChannelEventKind, ExecutorEventSink, ExecutorInterruptRequest,
        ExecutorPrepareRequest, ExecutorPromptOutcome, ExecutorPromptRequest, ExecutorUpdate,
    },
    session::{
        ApprovalMode, ContextArtifactRecord, ContextSyncRequest, ExecutorBinding, ExecutorHealth,
        SessionState, TranscriptMessage,
        context::{ContextSyncPlan, prepare_context_sync, read_context_artifacts_from_manifest},
        projection::{
            ProjectionInput, build_context_projection, merge_seen_context,
            projected_assistant_content, visible_message_fingerprints,
        },
        store::SessionStore,
    },
    text::truncate_chars,
};

use self::turns::{InterruptedTurn, TurnGuard, TurnRegistry};
pub use self::turns::{TurnBeginMode, TurnReservation};

#[cfg(test)]
use crate::executor::{InterruptReason, TurnCancellation};
#[cfg(test)]
use crate::session::context::write_context_sync;

#[derive(Debug, Clone)]
pub struct RouterInput {
    pub session_key: String,
    pub text: String,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterOutputEvent {
    Channel(RouterChannelEvent),
    FinalReply(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterChannelEvent {
    pub kind: RouterChannelEventKind,
    pub executor: String,
    pub title: String,
    pub text: String,
}

impl RouterChannelEvent {
    pub fn render_text(&self) -> String {
        let heading = match self.kind {
            RouterChannelEventKind::AgentProgress => "Progress".to_string(),
            RouterChannelEventKind::ReasoningSummary => "Reasoning summary".to_string(),
            RouterChannelEventKind::ToolCall if self.title.trim().is_empty() => {
                "Tool call".to_string()
            }
            RouterChannelEventKind::ToolCall => format!("Tool call: {}", self.title.trim()),
        };
        format!("[{}] {heading}\n{}", self.executor, self.text)
    }
}

pub(crate) fn render_compact_channel_events(events: &[RouterChannelEvent]) -> Option<String> {
    render_compact_channel_events_inner(events, true)
}

pub(crate) fn render_live_compact_channel_events(events: &[RouterChannelEvent]) -> Option<String> {
    render_compact_channel_events_inner(events, false)
}

fn render_compact_channel_events_inner(
    events: &[RouterChannelEvent],
    suppress_single_successful_tool: bool,
) -> Option<String> {
    let first = events.first()?;
    let mut latest_progress = None;
    let mut latest_reasoning = None;
    let mut tool_total = 0usize;
    let mut command_counts: Vec<(String, usize)> = Vec::new();
    let mut tool_counts: Vec<(String, usize)> = Vec::new();
    let mut attention = Vec::new();

    for event in events {
        match event.kind {
            RouterChannelEventKind::AgentProgress => {
                latest_progress = Some(truncate_chars(one_line(&event.text).as_str(), 240));
            }
            RouterChannelEventKind::ReasoningSummary => {
                latest_reasoning = Some(truncate_chars(one_line(&event.text).as_str(), 240));
            }
            RouterChannelEventKind::ToolCall => {
                tool_total += 1;
                let item = compact_tool_item(event);
                match &item {
                    CompactToolItem::Command(command) => {
                        push_compact_count(&mut command_counts, command.clone());
                    }
                    CompactToolItem::Tool(label) => {
                        push_compact_count(&mut tool_counts, label.clone());
                    }
                }
                if let Some(status) = compact_attention_status(event) {
                    attention.push(format!("{}: {status}", item.attention_label()));
                }
            }
        }
    }

    if suppress_single_successful_tool
        && latest_progress.is_none()
        && latest_reasoning.is_none()
        && attention.is_empty()
        && tool_total <= 1
    {
        return None;
    }

    let mut lines = vec![format!("[{}] Activity", first.executor)];
    if let Some(reasoning) = latest_reasoning
        && !reasoning.is_empty()
    {
        lines.push(format!("Reasoning: {reasoning}"));
    }
    append_compact_count_lines(&mut lines, "Commands", &command_counts, true);
    append_compact_count_lines(&mut lines, "Tools", &tool_counts, false);
    if !attention.is_empty() {
        lines.push("Attention:".to_string());
        for item in attention.iter().take(6) {
            lines.push(format!("- {item}"));
        }
        let remaining = attention.len().saturating_sub(6);
        if remaining > 0 {
            lines.push(format!("- {remaining} more"));
        }
    }
    if let Some(progress) = latest_progress
        && !progress.is_empty()
    {
        lines.push(format!("Progress: {progress}"));
    }
    Some(lines.join("\n"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CompactToolItem {
    Command(String),
    Tool(String),
}

impl CompactToolItem {
    fn attention_label(&self) -> String {
        match self {
            CompactToolItem::Command(command) => inline_code(command),
            CompactToolItem::Tool(label) => label.clone(),
        }
    }
}

fn push_compact_count(counts: &mut Vec<(String, usize)>, label: String) {
    if let Some(index) = counts.iter().position(|(existing, _)| existing == &label) {
        let (_, count) = counts.remove(index);
        counts.insert(0, (label, count + 1));
    } else {
        counts.insert(0, (label, 1));
    }
}

fn append_compact_count_lines(
    lines: &mut Vec<String>,
    heading: &str,
    counts: &[(String, usize)],
    format_as_code: bool,
) {
    if counts.is_empty() {
        return;
    }

    lines.push(format!("{heading}:"));
    for (label, count) in counts.iter().take(6) {
        let label = if format_as_code {
            inline_code(label)
        } else {
            label.clone()
        };
        let suffix = if *count > 1 {
            format!(" x{count}")
        } else {
            String::new()
        };
        lines.push(format!("- {label}{suffix}"));
    }
    let remaining = counts
        .iter()
        .skip(6)
        .map(|(_, count)| *count)
        .sum::<usize>();
    if remaining > 0 {
        lines.push(format!("- {remaining} more"));
    }
}

fn compact_tool_item(event: &RouterChannelEvent) -> CompactToolItem {
    if let Some(command) = compact_command_preview(event) {
        return CompactToolItem::Command(command);
    }
    if let Some(label) = compact_text_label(event) {
        return CompactToolItem::Tool(label);
    }
    let title = event.title.trim();
    if !title.is_empty() && !is_unhelpful_tool_title(title) {
        return CompactToolItem::Tool(title.to_string());
    }
    CompactToolItem::Tool("tool".to_string())
}

fn compact_command_preview(event: &RouterChannelEvent) -> Option<String> {
    event
        .text
        .lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix("$ "))
        .map(one_line)
        .filter(|command| !command.is_empty())
        .map(|command| truncate_chars(command.as_str(), 160))
}

fn compact_text_label(event: &RouterChannelEvent) -> Option<String> {
    event
        .text
        .lines()
        .map(str::trim)
        .find(|line| {
            let lower = line.to_ascii_lowercase();
            !line.is_empty()
                && !line.starts_with("$ ")
                && !lower.starts_with("status:")
                && !lower.starts_with("exit:")
                && !is_unhelpful_tool_title(line)
        })
        .map(one_line)
        .filter(|label| !label.is_empty())
        .map(|label| truncate_chars(label.as_str(), 120))
}

fn is_unhelpful_tool_title(title: &str) -> bool {
    matches!(
        title.to_ascii_lowercase().as_str(),
        "base" | "mcptoolcall" | "dynamictoolcall" | "tool_call" | "tool call"
    )
}

fn inline_code(text: &str) -> String {
    format!("`{}`", text.replace('`', "'"))
}

fn compact_attention_status(event: &RouterChannelEvent) -> Option<String> {
    for line in event.text.lines().map(str::trim) {
        let lower = line.to_ascii_lowercase();
        if let Some(status) = lower.strip_prefix("status:") {
            let status = status.trim();
            if status.contains("fail")
                || status.contains("error")
                || status.contains("cancel")
                || status.contains("denied")
            {
                return Some(line.to_string());
            }
        }
        if let Some(exit) = lower.strip_prefix("exit:") {
            let code = exit.trim();
            if code.parse::<i64>().is_ok_and(|code| code != 0) {
                return Some(line.to_string());
            }
        }
    }
    None
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterChannelEventKind {
    AgentProgress,
    ReasoningSummary,
    ToolCall,
}

#[async_trait]
pub trait RouterOutputSink: Send {
    fn send_channel_event(&mut self, event: RouterChannelEvent);
    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()>;
}

#[async_trait]
pub trait RouterService: Send + Sync + 'static {
    async fn reserve_turn(
        &self,
        _session_key: &str,
        _mode: TurnBeginMode,
    ) -> anyhow::Result<Option<TurnReservation>> {
        Ok(None)
    }

    async fn context_artifacts(
        &self,
        _session_key: &str,
        _source: &str,
    ) -> anyhow::Result<Vec<ContextArtifactRecord>> {
        Ok(Vec::new())
    }

    async fn sync_context(&self, _request: ContextSyncRequest) -> anyhow::Result<()> {
        Ok(())
    }

    async fn handle_with_context(
        &self,
        input: RouterInput,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(context) = context {
            self.sync_context(context).await?;
        }
        self.handle(input, output).await
    }

    async fn handle_reserved(
        &self,
        input: RouterInput,
        reservation: TurnReservation,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let _ = reservation;
        self.handle_with_context(input, context, output).await
    }

    async fn handle(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()>;

    async fn observe(&self, input: RouterInput) -> anyhow::Result<()>;
}

pub struct SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    default_executor: String,
    default_mode: ApprovalMode,
    store: Arc<S>,
}

impl<S> SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    pub fn new(
        default_executor: impl Into<String>,
        default_mode: ApprovalMode,
        store: Arc<S>,
    ) -> Self {
        Self {
            default_executor: default_executor.into(),
            default_mode,
            store,
        }
    }
}

#[async_trait]
impl<S> ApprovalPolicy for SessionApprovalPolicy<S>
where
    S: SessionStore,
{
    async fn auto_selection(&self, request: &ApprovalRequest) -> Option<ApprovalSelection> {
        let state = self
            .store
            .load_or_create(&request.session_key, &self.default_executor)
            .await;
        let effective_mode = state.approval_mode_override.unwrap_or(self.default_mode);
        if effective_mode != ApprovalMode::Yolo {
            return None;
        }
        let option_id = request.allow_once_option_id()?;
        tracing::info!(
            session_key = %request.session_key,
            executor = %request.executor,
            option_id = %option_id,
            "auto-approving request in YOLO mode"
        );
        Some(ApprovalSelection::Selected(option_id))
    }
}

pub struct AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    default_executor: String,
    default_approval_mode: ApprovalMode,
    store: Arc<S>,
    executor: Arc<E>,
    approvals: SharedApprovalBroker,
    workspace_root: Option<PathBuf>,
    session_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    turns: Arc<TurnRegistry>,
}

struct PreparedContextSync {
    state: SessionState,
    plan: ContextSyncPlan,
    session_key: String,
    source: String,
    unresolved_count: usize,
    record_count: usize,
    session_cwd: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContextCommitCheckpoint {
    AfterInstall,
    AfterStateSave,
}

impl PreparedContextSync {
    async fn commit<S>(mut self, store: &S) -> anyhow::Result<()>
    where
        S: SessionStore + ?Sized,
    {
        let records = self.plan.commit()?;
        self.state.context_artifacts = records;
        store.save(self.state).await;
        tracing::info!(
            session_key = %self.session_key,
            source = %self.source,
            context_records = self.record_count,
            unresolved_count = self.unresolved_count,
            cwd = %self.session_cwd.display(),
            "synced session context artifacts"
        );
        Ok(())
    }

    async fn commit_if_current<S>(self, turn: &TurnGuard, store: &S) -> anyhow::Result<bool>
    where
        S: SessionStore + ?Sized,
    {
        self.commit_if_current_with_hook(turn, store, |_| async {})
            .await
    }

    async fn commit_if_current_with_hook<S, F, Fut>(
        mut self,
        turn: &TurnGuard,
        store: &S,
        mut hook: F,
    ) -> anyhow::Result<bool>
    where
        S: SessionStore + ?Sized,
        F: FnMut(ContextCommitCheckpoint) -> Fut,
        Fut: Future<Output = ()>,
    {
        let cancel = turn.cancellation();
        if cancel.is_cancelled().await || !turn.is_current().await {
            return Ok(false);
        }
        let installed = self.plan.install()?;
        hook(ContextCommitCheckpoint::AfterInstall).await;
        if cancel.is_cancelled().await || !turn.is_current().await {
            drop(installed);
            return Ok(false);
        }
        let old_state = self.state.clone();
        let records = installed.records().to_vec();
        self.state.context_artifacts = records;
        store.save(self.state).await;
        hook(ContextCommitCheckpoint::AfterStateSave).await;
        if cancel.is_cancelled().await || !turn.is_current().await {
            drop(installed);
            store.save(old_state).await;
            return Ok(false);
        }
        installed.finish();
        tracing::info!(
            session_key = %self.session_key,
            source = %self.source,
            context_records = self.record_count,
            unresolved_count = self.unresolved_count,
            cwd = %self.session_cwd.display(),
            generation = turn.generation(),
            "synced session context artifacts"
        );
        Ok(true)
    }
}

impl<S, E> AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    pub fn new(default_executor: impl Into<String>, store: Arc<S>, executor: Arc<E>) -> Self {
        Self::with_approvals(
            default_executor,
            store,
            executor,
            Arc::new(ApprovalBroker::default()),
        )
    }

    pub fn with_approvals(
        default_executor: impl Into<String>,
        store: Arc<S>,
        executor: Arc<E>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self::with_approval_mode(
            default_executor,
            ApprovalMode::Normal,
            store,
            executor,
            approvals,
        )
    }

    pub fn with_approval_mode(
        default_executor: impl Into<String>,
        default_approval_mode: ApprovalMode,
        store: Arc<S>,
        executor: Arc<E>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self {
            default_executor: default_executor.into(),
            default_approval_mode,
            store,
            executor,
            approvals,
            workspace_root: None,
            session_locks: Mutex::new(HashMap::new()),
            turns: TurnRegistry::new(),
        }
    }

    pub fn with_workspace_root(mut self, workspace_root: Option<PathBuf>) -> Self {
        self.workspace_root = workspace_root;
        self
    }

    async fn session_lock(&self, session_key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn reserve_replacement_turn(&self, session_key: &str) -> TurnReservation {
        let reserved = self.turns.reserve_replacement(session_key).await;
        if let Some(interrupted) = reserved.interrupted {
            self.interrupt_turn(interrupted).await;
        }
        reserved.reservation
    }

    async fn interrupt_turn(&self, turn: InterruptedTurn) {
        let Some(turn_ref) = turn.executor_turn_ref() else {
            return;
        };
        if let Err(err) = self
            .executor
            .interrupt(ExecutorInterruptRequest {
                turn: turn_ref,
                reason: turn.reason,
            })
            .await
        {
            tracing::debug!(
                error = %err,
                session_key = %turn.session_key,
                generation = turn.generation,
                "executor interrupt request failed"
            );
        }
    }

    async fn handle_input(
        &self,
        input: RouterInput,
        reserved_turn: Option<TurnReservation>,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let text = input.text.trim();
        let command = text.split_whitespace().next().unwrap_or("");
        if command == "/stop" {
            return self.handle_stop_command(&input.session_key, output).await;
        }

        if command == "/agent" {
            let lock = self.session_lock(&input.session_key).await;
            let _guard = lock.lock().await;
            return self
                .handle_agent_command(&input.session_key, text, output)
                .await;
        }
        if command == "/yolo" {
            let lock = self.session_lock(&input.session_key).await;
            let _guard = lock.lock().await;
            return self
                .handle_yolo_command(&input.session_key, text, output)
                .await;
        }
        self.route_to_active_executor(input, reserved_turn, context, output)
            .await
    }

    async fn observe_locked(&self, input: RouterInput) -> anyhow::Result<()> {
        let text = input.text.trim();
        if text.is_empty() {
            return Ok(());
        }
        let text_len = text.len();
        let Some(mut state) = self.store.load(&input.session_key).await else {
            tracing::debug!(
                session_key = %input.session_key,
                text_len,
                "ignored observed message for unknown session"
            );
            return Ok(());
        };
        state.transcript.push(TranscriptMessage::user(input.text));
        self.store.save(state).await;
        tracing::info!(
            session_key = %input.session_key,
            text_len,
            "recorded observed message"
        );
        Ok(())
    }

    async fn prepare_context_sync_locked(
        &self,
        request: ContextSyncRequest,
    ) -> anyhow::Result<Option<PreparedContextSync>> {
        let mut state = self
            .store
            .load_or_create(&request.session_key, &self.default_executor)
            .await;
        let Some(session_cwd) = self.ensure_session_cwd(&mut state)? else {
            tracing::warn!(
                session_key = %request.session_key,
                source = %request.source,
                "skipping context sync because no workspace root is configured"
            );
            return Ok(None);
        };
        let session_key = request.session_key.clone();
        let source = request.source.clone();
        let unresolved_count = request.unresolved.len();
        let (recovered_context, recovery_failed) =
            recover_context_artifacts_from_manifest(&session_cwd, &session_key, &source);
        let recovered_context_count = recovered_context.len();
        let (mut existing_context, used_recovered_context) =
            merge_recovered_context_artifacts(&state.context_artifacts, &source, recovered_context);
        if recovery_failed {
            existing_context.retain(|record| record.source != source);
        }
        if used_recovered_context {
            tracing::info!(
                session_key = %session_key,
                source = %source,
                recovered_context_records = recovered_context_count,
                cwd = %session_cwd.display(),
                "using recovered session context artifacts from manifest"
            );
        }
        let plan = prepare_context_sync(&session_cwd, request, &existing_context)?;
        let record_count = plan.record_count();
        Ok(Some(PreparedContextSync {
            state,
            plan,
            session_key,
            source,
            unresolved_count,
            record_count,
            session_cwd,
        }))
    }

    async fn sync_context_locked(&self, request: ContextSyncRequest) -> anyhow::Result<()> {
        if let Some(prepared) = self.prepare_context_sync_locked(request).await? {
            prepared.commit(self.store.as_ref()).await?;
        }
        Ok(())
    }

    async fn handle_agent_command(
        &self,
        session_key: &str,
        text: &str,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let args = text.trim_start_matches("/agent").trim();
        let mut state = self
            .store
            .load_or_create(session_key, &self.default_executor)
            .await;
        if args.is_empty() || args == "status" {
            return output.send_final_reply(self.render_status(&state)).await;
        }
        if args.split_whitespace().count() != 1 {
            return output
                .send_final_reply("Usage: /agent [status|done|<executor>]".to_string())
                .await;
        }

        let target = args;
        if target == "done" {
            state.active_executor = state.default_executor.clone();
            self.store.save(state.clone()).await;
            return output
                .send_final_reply(format!(
                    "Agent handoff ended. Active executor: {}",
                    state.active_executor
                ))
                .await;
        }

        if self.executor.get(target).is_none() {
            return output
                .send_final_reply(format!("Executor `{target}` is not configured."))
                .await;
        }
        state.active_executor = target.to_string();
        self.store.save(state).await;
        output
            .send_final_reply(format!("Active executor: {target}"))
            .await
    }

    async fn handle_yolo_command(
        &self,
        session_key: &str,
        text: &str,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let args = text.trim_start_matches("/yolo").trim();
        let mut state = self
            .store
            .load_or_create(session_key, &self.default_executor)
            .await;
        if args.is_empty() || args == "status" {
            return output
                .send_final_reply(self.render_yolo_status(&state))
                .await;
        }
        if args.split_whitespace().count() != 1 {
            return output
                .send_final_reply("Usage: /yolo [status|on|off|inherit]".to_string())
                .await;
        }

        match args {
            "on" => {
                state.approval_mode_override = Some(ApprovalMode::Yolo);
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode enabled for this session.",
                        &state,
                    ))
                    .await
            }
            "off" => {
                state.approval_mode_override = Some(ApprovalMode::Normal);
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode disabled for this session.",
                        &state,
                    ))
                    .await
            }
            "inherit" => {
                state.approval_mode_override = None;
                self.store.save(state.clone()).await;
                output
                    .send_final_reply(self.render_yolo_status_with_prefix(
                        "YOLO mode now inherits the global default.",
                        &state,
                    ))
                    .await
            }
            _ => {
                output
                    .send_final_reply("Usage: /yolo [status|on|off|inherit]".to_string())
                    .await
            }
        }
    }

    async fn handle_stop_command(
        &self,
        session_key: &str,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(active) = self.turns.stop(session_key).await {
            self.interrupt_turn(active).await;
            output
                .send_final_reply("Stopped the active turn.".to_string())
                .await
        } else {
            output
                .send_final_reply("No active turn for this session.".to_string())
                .await
        }
    }

    async fn route_to_active_executor(
        &self,
        input: RouterInput,
        reserved_turn: Option<TurnReservation>,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        let session_key = input.session_key.clone();
        let (turn, replaced, state, executor_name, descriptor, binding, session_cwd) = {
            let lock = self.session_lock(&session_key).await;
            let _guard = lock.lock().await;
            let mut context = context;
            if reserved_turn.is_none()
                && let Some(context) = context.take()
            {
                self.sync_context_locked(context).await?;
            }
            let mut state = self
                .store
                .load(&session_key)
                .await
                .unwrap_or_else(|| SessionState::new(&session_key, &self.default_executor));
            let executor_name = state.active_executor.clone();
            let descriptor = self.executor.get(&executor_name).ok_or_else(|| {
                anyhow::anyhow!("active executor `{executor_name}` is not configured")
            })?;
            let session_cwd = self.ensure_session_cwd(&mut state)?;
            let (turn, replaced) = if let Some(reservation) = reserved_turn {
                match reservation.adopt(executor_name.clone()).await {
                    Some(turn) => (turn, None),
                    None => {
                        tracing::debug!(
                            session_key = %session_key,
                            generation = reservation.generation(),
                            "discarded stale preempted router turn before prompt"
                        );
                        return Ok(());
                    }
                }
            } else {
                let begun = self.turns.begin(&session_key, executor_name.clone()).await;
                (begun.guard, begun.interrupted)
            };
            if session_cwd.is_some() {
                self.store.save(state.clone()).await;
            }
            if let Some(context) = context {
                let prepared = match self.prepare_context_sync_locked(context).await {
                    Ok(prepared) => prepared,
                    Err(err) => {
                        let _ = turn.finish_if_current().await;
                        return Err(err);
                    }
                };
                if let Some(prepared) = prepared {
                    let committed =
                        match prepared.commit_if_current(&turn, self.store.as_ref()).await {
                            Ok(committed) => committed,
                            Err(err) => {
                                let _ = turn.finish_if_current().await;
                                return Err(err);
                            }
                        };
                    if !committed {
                        tracing::debug!(
                            session_key = %session_key,
                            generation = turn.generation(),
                            "discarded stale reserved router turn before context commit"
                        );
                        return Ok(());
                    }
                    state = self
                        .store
                        .load_or_create(&session_key, &self.default_executor)
                        .await;
                }
            }
            let binding = state.binding_for(&executor_name);
            (
                turn,
                replaced,
                state,
                executor_name,
                descriptor,
                binding,
                session_cwd,
            )
        };

        if let Some(replaced) = replaced {
            self.interrupt_turn(replaced).await;
        }

        debug_assert_eq!(turn.session_key(), session_key);
        debug_assert_eq!(turn.executor(), executor_name);
        let generation = turn.generation();
        let cancel = turn.cancellation();
        let executor_turn = turn.executor_turn_ref();
        tracing::info!(
            session_key = %session_key,
            executor = %executor_name,
            generation,
            cwd = session_cwd
                .as_deref()
                .map(|cwd| cwd.display().to_string())
                .unwrap_or_else(|| "executor-default".to_string()),
            text_len = input.text.len(),
            "routing turn to active executor"
        );

        let prepared = self
            .executor
            .prepare(
                ExecutorPrepareRequest {
                    turn: executor_turn.clone(),
                    cwd: session_cwd.clone(),
                    previous_session_id: binding.external_session_id.clone(),
                },
                cancel.clone(),
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                if cancel.is_cancelled().await {
                    let _ = turn.finish_if_current().await;
                    tracing::debug!(
                        session_key = %session_key,
                        executor = %executor_name,
                        generation,
                        error = %err,
                        "discarded stale or cancelled prepare failure"
                    );
                    return Ok(());
                }
                let committed_failure = {
                    let lock = self.session_lock(&session_key).await;
                    let _guard = lock.lock().await;
                    turn.commit_if_current(|| async {
                        let mut latest = self
                            .store
                            .load_or_create(&session_key, &self.default_executor)
                            .await;
                        let latest_binding = latest.binding_for(&executor_name);
                        latest.executor_bindings.insert(
                            executor_name.clone(),
                            ExecutorBinding {
                                protocol: descriptor.protocol.clone(),
                                machine_id: Some(descriptor.machine_id.clone()),
                                health: ExecutorHealth::Unhealthy,
                                ..binding_with_executor_cwd(
                                    latest_binding,
                                    session_cwd
                                        .as_ref()
                                        .map(|cwd| cwd.display().to_string())
                                        .as_deref(),
                                )
                            },
                        );
                        self.store.save(latest).await;
                    })
                    .await
                    .is_some()
                };
                if !committed_failure {
                    tracing::debug!(
                        session_key = %session_key,
                        executor = %executor_name,
                        generation,
                        error = %err,
                        "discarded stale prepare failure"
                    );
                    return Ok(());
                }
                return Err(err);
            }
        };
        if cancel.is_cancelled().await {
            let _ = turn.finish_if_current().await;
            tracing::info!(
                session_key = %session_key,
                executor = %executor_name,
                generation,
                "router turn cancelled before prompt"
            );
            return Ok(());
        }
        let prepared_machine_id = prepared
            .machine_id
            .clone()
            .unwrap_or_else(|| descriptor.machine_id.clone());
        let prepared_cwd = prepared
            .cwd
            .clone()
            .or_else(|| session_cwd.as_ref().map(|cwd| cwd.display().to_string()));
        let prepared_machine_workspace = prepared.machine_workspace.clone();

        let projection = build_context_projection(ProjectionInput {
            transcript: &state.transcript,
            context_artifacts: &state.context_artifacts,
            seen_context: &binding.seen_context,
            current_message: &input.text,
            started_new_session: prepared.started_new_session,
            max_messages: 40,
        });

        let (response, updates) = {
            let mut executor_events =
                RouterExecutorEventSink::new(turn.clone(), &executor_name, output);
            let response = self
                .executor
                .prompt(
                    ExecutorPromptRequest {
                        turn: executor_turn,
                        prompt: projection.prompt,
                        user_id: input.user_id.clone(),
                    },
                    &mut executor_events,
                    cancel.clone(),
                )
                .await;
            (response, executor_events.into_updates())
        };

        match response {
            ExecutorPromptOutcome::Completed(response) => {
                let activity_summaries = updates
                    .iter()
                    .filter_map(|update| update.summary(700))
                    .collect::<Vec<_>>();
                let assistant_content = projected_assistant_content(
                    &executor_name,
                    &response.final_text,
                    &activity_summaries,
                );
                let user_entry = TranscriptMessage::user(input.text);
                let assistant_entry = TranscriptMessage::assistant(
                    assistant_content,
                    executor_name.clone(),
                    prepared.external_session_id.clone(),
                );
                let new_fingerprints =
                    visible_message_fingerprints(&[user_entry.clone(), assistant_entry.clone()])
                        .into_iter()
                        .map(|(_, fingerprint)| fingerprint)
                        .collect::<Vec<_>>();

                let committed = {
                    let lock = self.session_lock(&session_key).await;
                    let _guard = lock.lock().await;
                    turn.commit_if_current(|| async {
                        let mut latest = self
                            .store
                            .load_or_create(&session_key, &self.default_executor)
                            .await;
                        let latest_binding = latest.binding_for(&executor_name);
                        latest.transcript.push(user_entry);
                        latest.transcript.push(assistant_entry);
                        if let Some(machine_workspace) = prepared_machine_workspace.clone() {
                            latest
                                .machine_workspaces
                                .insert(machine_workspace.machine_id.clone(), machine_workspace);
                        }
                        latest.executor_bindings.insert(
                            executor_name.clone(),
                            update_binding_after_success(
                                latest_binding,
                                prepared.external_session_id,
                                descriptor.protocol,
                                prepared_machine_id,
                                prepared_cwd.as_deref(),
                                projection.acknowledged_fingerprints,
                                new_fingerprints,
                            ),
                        );
                        self.store.save(latest).await;
                    })
                    .await
                    .is_some()
                };
                if !committed {
                    tracing::debug!(
                        session_key = %session_key,
                        executor = %executor_name,
                        generation,
                        "discarded stale successful router turn"
                    );
                    return Ok(());
                }
                tracing::info!(
                    session_key = %session_key,
                    executor = %executor_name,
                    generation,
                    final_text_len = response.final_text.len(),
                    "committed successful router turn"
                );
                output.send_final_reply(response.final_text).await
            }
            ExecutorPromptOutcome::Cancelled => {
                let current = turn.finish_if_current().await;
                tracing::info!(
                    session_key = %session_key,
                    executor = %executor_name,
                    generation,
                    current,
                    "router turn cancelled"
                );
                Ok(())
            }
            ExecutorPromptOutcome::Failed(err) => {
                let committed_failure = {
                    let lock = self.session_lock(&session_key).await;
                    let _guard = lock.lock().await;
                    turn.commit_if_current(|| async {
                        let mut latest = self
                            .store
                            .load_or_create(&session_key, &self.default_executor)
                            .await;
                        let latest_binding = latest.binding_for(&executor_name);
                        if let Some(machine_workspace) = prepared_machine_workspace.clone() {
                            latest
                                .machine_workspaces
                                .insert(machine_workspace.machine_id.clone(), machine_workspace);
                        }
                        latest.executor_bindings.insert(
                            executor_name.clone(),
                            update_binding_after_prompt_failure(
                                latest_binding,
                                prepared.external_session_id,
                                descriptor.protocol,
                                prepared_machine_id,
                                prepared_cwd.as_deref(),
                            ),
                        );
                        self.store.save(latest).await;
                    })
                    .await
                    .is_some()
                };
                if !committed_failure {
                    tracing::debug!(
                        session_key = %session_key,
                        executor = %executor_name,
                        generation,
                        error = %err,
                        "discarded stale router turn failure"
                    );
                    return Ok(());
                }
                tracing::warn!(
                    error = %err,
                    session_key = %session_key,
                    executor = %executor_name,
                    generation,
                    "router turn failed"
                );
                Err(err)
            }
        }
    }

    fn render_status(&self, state: &SessionState) -> String {
        let mut lines = vec![
            format!("Default executor: {}", state.default_executor),
            format!("Active executor: {}", state.active_executor),
        ];
        if let Some(cwd) = &state.cwd {
            lines.push(format!("Session cwd: {}", cwd.display()));
        }
        lines.push("Executors:".to_string());
        for descriptor in self.executor.list() {
            let binding = state.executor_bindings.get(&descriptor.name);
            let suffix = binding
                .and_then(|binding| binding.external_session_id.as_ref())
                .map(|session_id| format!(", session {session_id}"))
                .unwrap_or_default();
            lines.push(format!(
                "- {}: {}{}",
                descriptor.name, descriptor.protocol, suffix
            ));
        }
        lines.join("\n")
    }

    fn ensure_session_cwd(&self, state: &mut SessionState) -> anyhow::Result<Option<PathBuf>> {
        let Some(root) = &self.workspace_root else {
            return Ok(None);
        };
        let cwd = match &state.cwd {
            Some(cwd) => cwd.clone(),
            None => {
                let cwd = root.join(session_workspace_dir_name(&state.session_key));
                state.cwd = Some(cwd.clone());
                cwd
            }
        };
        ensure_dir_path_without_symlinks(&cwd)?;
        state.cwd = Some(cwd.clone());
        Ok(Some(cwd))
    }

    fn render_yolo_status_with_prefix(&self, prefix: &str, state: &SessionState) -> String {
        format!("{prefix}\n{}", self.render_yolo_status(state))
    }

    fn render_yolo_status(&self, state: &SessionState) -> String {
        let override_label = state
            .approval_mode_override
            .map(ApprovalMode::as_str)
            .unwrap_or("inherit");
        let effective_mode = state
            .approval_mode_override
            .unwrap_or(self.default_approval_mode);
        [
            format!(
                "Global default approval mode: {}",
                self.default_approval_mode.as_str()
            ),
            format!("Session override: {override_label}"),
            format!("Effective approval mode: {}", effective_mode.as_str()),
        ]
        .join("\n")
    }
}

fn merge_recovered_context_artifacts(
    state_context: &[ContextArtifactRecord],
    source: &str,
    recovered_context: Vec<ContextArtifactRecord>,
) -> (Vec<ContextArtifactRecord>, bool) {
    if recovered_context.is_empty() {
        return (state_context.to_vec(), false);
    }
    let has_state_source = state_context.iter().any(|record| record.source == source);
    let use_recovered = !has_state_source
        || match (
            source_manifest_updated_at(state_context, source),
            source_manifest_updated_at(&recovered_context, source),
        ) {
            (Some(state_updated_at), Some(recovered_updated_at)) => {
                recovered_updated_at > state_updated_at
            }
            (None, Some(_)) => true,
            _ => false,
        };
    if !use_recovered {
        return (state_context.to_vec(), false);
    }

    let mut records = state_context
        .iter()
        .filter(|record| record.source != source)
        .cloned()
        .collect::<Vec<_>>();
    records.extend(recovered_context);
    sort_context_artifact_records(&mut records);
    (records, true)
}

fn source_manifest_updated_at(records: &[ContextArtifactRecord], source: &str) -> Option<u64> {
    let canonical_manifest = Path::new(source).join("manifest.json");
    records
        .iter()
        .find(|record| {
            record.source == source
                && record.kind == "manifest"
                && record
                    .paths
                    .first()
                    .is_some_and(|path| Path::new(path) == canonical_manifest.as_path())
        })
        .map(|record| record.updated_at_ms)
}

fn sort_context_artifact_records(records: &mut [ContextArtifactRecord]) {
    records.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn recover_context_artifacts_from_manifest(
    cwd: &Path,
    session_key: &str,
    source: &str,
) -> (Vec<ContextArtifactRecord>, bool) {
    match read_context_artifacts_from_manifest(cwd, source, session_key, Path::new(source)) {
        Ok(records) => (records, false),
        Err(err) => {
            tracing::warn!(
                session_key,
                source,
                cwd = %cwd.display(),
                error = %err,
                "ignored invalid recovered session context manifest"
            );
            (Vec::new(), true)
        }
    }
}

#[async_trait]
impl<S, E> RouterService for AgentRouter<S, E>
where
    S: SessionStore,
    E: ExecutorBackend,
{
    async fn reserve_turn(
        &self,
        session_key: &str,
        mode: TurnBeginMode,
    ) -> anyhow::Result<Option<TurnReservation>> {
        match mode {
            TurnBeginMode::ReplaceActive => {
                Ok(Some(self.reserve_replacement_turn(session_key).await))
            }
            TurnBeginMode::NoPreempt => Ok(None),
        }
    }

    async fn context_artifacts(
        &self,
        session_key: &str,
        source: &str,
    ) -> anyhow::Result<Vec<ContextArtifactRecord>> {
        let lock = self.session_lock(session_key).await;
        let _guard = lock.lock().await;
        let (state_context, state_cwd) = self
            .store
            .load(session_key)
            .await
            .map(|state| (state.context_artifacts, state.cwd))
            .unwrap_or_default();
        let recovery_cwd = state_cwd.or_else(|| {
            self.workspace_root
                .as_ref()
                .map(|root| root.join(session_workspace_dir_name(session_key)))
        });
        let records = if let Some(cwd) = recovery_cwd {
            let (recovered, recovery_failed) =
                recover_context_artifacts_from_manifest(&cwd, session_key, source);
            let (mut records, _) =
                merge_recovered_context_artifacts(&state_context, source, recovered);
            if recovery_failed {
                records.retain(|record| record.source != source);
            }
            records
        } else {
            state_context
        };
        Ok(records
            .into_iter()
            .filter(|record| record.source == source)
            .collect())
    }

    async fn sync_context(&self, request: ContextSyncRequest) -> anyhow::Result<()> {
        let lock = self.session_lock(&request.session_key).await;
        let _guard = lock.lock().await;
        self.sync_context_locked(request).await
    }

    async fn handle_with_context(
        &self,
        input: RouterInput,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(reply) = self
            .approvals
            .resolve_command(&input.session_key, &input.text, input.user_id.as_deref())
            .await
        {
            return output.send_final_reply(reply.text).await;
        }
        self.handle_input(input, None, context, output).await
    }

    async fn handle_reserved(
        &self,
        input: RouterInput,
        reservation: TurnReservation,
        context: Option<ContextSyncRequest>,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(reply) = self
            .approvals
            .resolve_command(&input.session_key, &input.text, input.user_id.as_deref())
            .await
        {
            let _ = reservation.discard_if_current().await;
            return output.send_final_reply(reply.text).await;
        }
        match self
            .handle_input(input, Some(reservation.clone()), context, output)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = reservation.discard_if_current().await;
                Err(err)
            }
        }
    }

    async fn handle(
        &self,
        input: RouterInput,
        output: &mut dyn RouterOutputSink,
    ) -> anyhow::Result<()> {
        if let Some(reply) = self
            .approvals
            .resolve_command(&input.session_key, &input.text, input.user_id.as_deref())
            .await
        {
            return output.send_final_reply(reply.text).await;
        }
        self.handle_input(input, None, None, output).await
    }

    async fn observe(&self, input: RouterInput) -> anyhow::Result<()> {
        let lock = self.session_lock(&input.session_key).await;
        let _guard = lock.lock().await;
        self.observe_locked(input).await
    }
}

struct RouterExecutorEventSink<'a> {
    turn: TurnGuard,
    executor: &'a str,
    output: &'a mut dyn RouterOutputSink,
    updates: Vec<ExecutorUpdate>,
}

impl<'a> RouterExecutorEventSink<'a> {
    fn new(turn: TurnGuard, executor: &'a str, output: &'a mut dyn RouterOutputSink) -> Self {
        Self {
            turn,
            executor,
            output,
            updates: Vec::new(),
        }
    }

    fn into_updates(self) -> Vec<ExecutorUpdate> {
        self.updates
    }
}

#[async_trait]
impl ExecutorEventSink for RouterExecutorEventSink<'_> {
    async fn send(&mut self, update: ExecutorUpdate) -> anyhow::Result<()> {
        if let Some(event) = channel_event_from_executor_update(self.executor, &update)
            && self.turn.is_current().await
        {
            self.output.send_channel_event(event);
        }
        self.updates.push(update);
        Ok(())
    }
}

fn channel_event_from_executor_update(
    executor: &str,
    update: &ExecutorUpdate,
) -> Option<RouterChannelEvent> {
    let event = update.channel_event.as_ref()?;
    if event.text.trim().is_empty() {
        return None;
    }
    Some(RouterChannelEvent {
        kind: router_channel_event_kind(event.kind),
        executor: executor.to_string(),
        title: event.title.clone(),
        text: truncate_chars(event.text.trim(), 1_500),
    })
}

fn router_channel_event_kind(kind: ExecutorChannelEventKind) -> RouterChannelEventKind {
    match kind {
        ExecutorChannelEventKind::AgentProgress => RouterChannelEventKind::AgentProgress,
        ExecutorChannelEventKind::ReasoningSummary => RouterChannelEventKind::ReasoningSummary,
        ExecutorChannelEventKind::ToolCall => RouterChannelEventKind::ToolCall,
    }
}

fn update_binding_after_success(
    mut binding: ExecutorBinding,
    external_session_id: Option<String>,
    protocol: String,
    machine_id: String,
    cwd: Option<&str>,
    handoff_fingerprints: Vec<String>,
    new_message_fingerprints: Vec<String>,
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.machine_id = Some(machine_id);
    binding.external_session_id = external_session_id;
    binding.health = ExecutorHealth::Healthy;
    binding = binding_with_executor_cwd(binding, cwd);
    binding.seen_context = merge_seen_context(
        &binding.seen_context,
        &[handoff_fingerprints, new_message_fingerprints].concat(),
    );
    binding
}

fn update_binding_after_prompt_failure(
    mut binding: ExecutorBinding,
    prepared_session_id: Option<String>,
    protocol: String,
    machine_id: String,
    cwd: Option<&str>,
) -> ExecutorBinding {
    binding.protocol = protocol;
    binding.machine_id = Some(machine_id);
    binding.health = ExecutorHealth::Unhealthy;
    binding = binding_with_executor_cwd(binding, cwd);
    if prepared_session_id != binding.external_session_id {
        binding.external_session_id = prepared_session_id;
        binding.seen_context.clear();
    }
    binding
}

fn binding_with_executor_cwd(mut binding: ExecutorBinding, cwd: Option<&str>) -> ExecutorBinding {
    if let Some(cwd) = cwd {
        binding.cwd = Some(cwd.to_string());
    }
    binding
}

fn session_workspace_dir_name(session_key: &str) -> String {
    let raw_prefix = session_key
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let mut prefix = raw_prefix
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if prefix.is_empty() {
        prefix = "session".to_string();
    }
    if prefix.len() > 48 {
        prefix.truncate(48);
        prefix = prefix.trim_end_matches('-').to_string();
        if prefix.is_empty() {
            prefix = "session".to_string();
        }
    }
    let digest = Sha256::digest(session_key.as_bytes());
    let hash = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}-{hash}")
}

fn ensure_dir_path_without_symlinks(path: &Path) -> anyhow::Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir => {
                anyhow::bail!(
                    "session cwd must not contain parent components: {}",
                    path.display()
                );
            }
            Component::Normal(segment) => {
                current.push(segment);
                match std::fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        anyhow::bail!("session cwd component is a symlink: {}", current.display());
                    }
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        anyhow::bail!(
                            "session cwd component is not a directory: {}",
                            current.display()
                        );
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        if let Err(err) = std::fs::create_dir(&current)
                            && err.kind() != std::io::ErrorKind::AlreadyExists
                        {
                            return Err(anyhow::anyhow!(
                                "create session cwd directory {}: {}",
                                current.display(),
                                err
                            ));
                        }
                        let metadata = std::fs::symlink_metadata(&current).map_err(|err| {
                            anyhow::anyhow!(
                                "stat session cwd directory {}: {}",
                                current.display(),
                                err
                            )
                        })?;
                        anyhow::ensure!(
                            metadata.is_dir() && !metadata.file_type().is_symlink(),
                            "session cwd component is invalid after create: {}",
                            current.display()
                        );
                    }
                    Err(err) => {
                        return Err(anyhow::anyhow!(
                            "stat session cwd component {}: {}",
                            current.display(),
                            err
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        approval::{
            ApprovalBroker, ApprovalOption, ApprovalPolicy, ApprovalRequest, ApprovalSelection,
        },
        executor::{ExecutorChannelEvent, test_support::FakeExecutorBackend},
        session::{
            TranscriptMessage,
            context::{
                ContextArtifactInput, ContextArtifactRemovalInput, ContextFileContent,
                ContextFileInput, ContextSyncIssueInput,
            },
            projection::message_fingerprint,
            store::InMemorySessionStore,
        },
    };
    use serde_json::json;
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Duration,
    };

    #[derive(Debug, Default)]
    struct CollectingRouterOutputSink {
        events: Vec<RouterOutputEvent>,
    }

    #[async_trait::async_trait]
    impl RouterOutputSink for CollectingRouterOutputSink {
        fn send_channel_event(&mut self, event: RouterChannelEvent) {
            self.events.push(RouterOutputEvent::Channel(event));
        }

        async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
            self.events.push(RouterOutputEvent::FinalReply(text));
            Ok(())
        }
    }

    impl CollectingRouterOutputSink {
        fn final_reply(&self) -> &str {
            match self.events.last().expect("router emitted no events") {
                RouterOutputEvent::FinalReply(text) => text,
                RouterOutputEvent::Channel(_) => panic!("last router event was not final reply"),
            }
        }
    }

    fn slack_thread_context_request(session_key: &str, content: &str) -> ContextSyncRequest {
        ContextSyncRequest {
            session_key: session_key.to_string(),
            source: "slack".to_string(),
            base_path: PathBuf::from("slack"),
            artifacts: vec![ContextArtifactInput {
                id: "thread".to_string(),
                kind: "slack_current_thread".to_string(),
                title: "Thread".to_string(),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.md"),
                    content: ContextFileContent::Text(content.to_string()),
                }],
                metadata: Default::default(),
            }],
            remove_artifacts: Vec::new(),
            unresolved: Vec::new(),
        }
    }

    fn slack_thread_and_extra_context_request(
        session_key: &str,
        content: &str,
    ) -> ContextSyncRequest {
        let mut request = slack_thread_context_request(session_key, content);
        request.artifacts.push(ContextArtifactInput {
            id: "old-extra".to_string(),
            kind: "slack_current_thread".to_string(),
            title: "Old Extra".to_string(),
            source_locator: None,
            files: vec![ContextFileInput {
                relative_path: PathBuf::from("slack/old-extra.md"),
                content: ContextFileContent::Text("old extra context".to_string()),
            }],
            metadata: Default::default(),
        });
        request
    }

    fn slack_thread_replacement_context_request(
        session_key: &str,
        content: &str,
    ) -> ContextSyncRequest {
        let mut request = slack_thread_context_request(session_key, content);
        request
            .remove_artifacts
            .push(ContextArtifactRemovalInput::ExceptKind {
                kind: "slack_current_thread".to_string(),
                retain_ids: BTreeSet::from(["thread".to_string()]),
            });
        request
    }

    fn assert_context_record_restored(saved: &SessionState, id: &str, path: &str) {
        let record = saved
            .context_artifacts
            .iter()
            .find(|record| {
                record.source == "slack" && record.kind == "slack_current_thread" && record.id == id
            })
            .unwrap_or_else(|| panic!("missing restored context record {id}"));
        assert_eq!(record.source, "slack");
        assert_eq!(record.kind, "slack_current_thread");
        assert_eq!(record.paths, vec![path.to_string()]);
    }

    #[tokio::test]
    async fn agent_status_shows_default_and_active_executor() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store, executor);

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/agent status".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert!(output.final_reply().contains("Default executor: kimi"));
        assert!(output.final_reply().contains("Active executor: kimi"));
    }

    #[tokio::test]
    async fn yolo_commands_update_current_session_override() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::with_approval_mode(
            "kimi",
            ApprovalMode::Normal,
            store.clone(),
            executor,
            Arc::new(ApprovalBroker::default()),
        );

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo on".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: yolo"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, Some(ApprovalMode::Yolo));

        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo inherit".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: inherit"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, None);

        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo off".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();
        assert!(output.final_reply().contains("Session override: normal"));
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, Some(ApprovalMode::Normal));
    }

    #[tokio::test]
    async fn concatenated_yolo_command_name_is_not_a_router_command() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yoloon".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "fake response");
        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        assert_eq!(saved.approval_mode_override, None);
        assert!(
            executor.prompts.lock().await[0]
                .prompt
                .contains("Current user message:\n/yoloon")
        );
    }

    #[tokio::test]
    async fn executor_prepare_and_prompt_share_turn_identity() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store, executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "hello".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let prepared = executor.prepared.lock().await;
        let prompts = executor.prompts.lock().await;
        assert_eq!(prepared.len(), 1);
        assert_eq!(prompts.len(), 1);
        assert_eq!(prepared[0].turn.session_key, "slack:dm:D1:111.000");
        assert_eq!(prepared[0].turn.executor, "kimi");
        assert_eq!(prepared[0].turn.generation, prompts[0].generation);
        assert_eq!(prompts[0].session_key, "slack:dm:D1:111.000");
        assert_eq!(prompts[0].executor, "kimi");
    }

    #[tokio::test]
    async fn yolo_command_enables_broker_auto_approval_for_session() {
        let store = Arc::new(InMemorySessionStore::default());
        let approvals = Arc::new(ApprovalBroker::with_policy(
            Duration::from_secs(5),
            Arc::new(SessionApprovalPolicy::new(
                "kimi",
                ApprovalMode::Normal,
                store.clone(),
            )),
        ));
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::with_approval_mode(
            "kimi",
            ApprovalMode::Normal,
            store,
            executor,
            approvals.clone(),
        );
        let mut prompts = approvals.subscribe();
        let mut notices = approvals.subscribe_auto_selections();

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:dm:D1:111.000".to_string(),
                    text: "/yolo on".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let selection = approvals
            .request(ApprovalRequest {
                session_key: "slack:dm:D1:111.000".to_string(),
                executor: "kimi".to_string(),
                requester_user_id: Some("U1".to_string()),
                title: "Run command".to_string(),
                body: "$ cargo test".to_string(),
                options: vec![ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: true,
                }],
            })
            .await;

        assert_eq!(
            selection,
            ApprovalSelection::Selected("allow_once".to_string())
        );
        assert!(prompts.try_recv().is_err());
        let notice = notices.try_recv().unwrap();
        assert_eq!(notice.session_key, "slack:dm:D1:111.000");
    }

    #[tokio::test]
    async fn session_approval_policy_isolates_slack_dm_sessions() {
        let store = Arc::new(InMemorySessionStore::default());
        let policy = SessionApprovalPolicy::new("kimi", ApprovalMode::Normal, store.clone());
        let mut yolo_session = SessionState::new("slack:dm:D1:111.000", "kimi");
        yolo_session.approval_mode_override = Some(ApprovalMode::Yolo);
        store.save(yolo_session).await;

        let yolo_request = ApprovalRequest {
            session_key: "slack:dm:D1:111.000".to_string(),
            executor: "kimi".to_string(),
            requester_user_id: Some("U1".to_string()),
            title: "Run command".to_string(),
            body: "$ cargo test".to_string(),
            options: vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: true,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ],
        };
        let normal_request = ApprovalRequest {
            session_key: "slack:dm:D1:222.000".to_string(),
            ..yolo_request.clone()
        };

        assert_eq!(
            policy.auto_selection(&yolo_request).await,
            Some(ApprovalSelection::Selected("allow_once".to_string()))
        );
        assert_eq!(policy.auto_selection(&normal_request).await, None);
    }

    #[tokio::test]
    async fn session_approval_policy_normal_override_disables_global_yolo() {
        let store = Arc::new(InMemorySessionStore::default());
        let policy = SessionApprovalPolicy::new("kimi", ApprovalMode::Yolo, store.clone());
        let mut state = SessionState::new("slack:channel:C1:111.000", "kimi");
        state.approval_mode_override = Some(ApprovalMode::Normal);
        store.save(state).await;

        let request = ApprovalRequest {
            session_key: "slack:channel:C1:111.000".to_string(),
            executor: "kimi".to_string(),
            requester_user_id: Some("U1".to_string()),
            title: "Run command".to_string(),
            body: "$ cargo test".to_string(),
            options: vec![ApprovalOption {
                id: "allow_once".to_string(),
                kind: "allow_once".to_string(),
                name: "Allow once".to_string(),
                auto_approvable: true,
            }],
        };

        assert_eq!(policy.auto_selection(&request).await, None);

        let inherited_request = ApprovalRequest {
            session_key: "slack:channel:C1:222.000".to_string(),
            ..request
        };
        assert_eq!(
            policy.auto_selection(&inherited_request).await,
            Some(ApprovalSelection::Selected("allow_once".to_string()))
        );
    }

    #[tokio::test]
    async fn approval_command_resolves_pending_request() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let router =
            AgentRouter::with_approvals("kimi", store, executor.clone(), approvals.clone());
        let request_broker = approvals.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(ApprovalRequest {
                    session_key: "slack:C1:T1".to_string(),
                    executor: "kimi".to_string(),
                    requester_user_id: Some("U1".to_string()),
                    title: "Run command".to_string(),
                    body: "$ cargo test".to_string(),
                    options: vec![
                        ApprovalOption {
                            id: "allow_once".to_string(),
                            kind: "allow_once".to_string(),
                            name: "Allow once".to_string(),
                            auto_approvable: true,
                        },
                        ApprovalOption {
                            id: "deny".to_string(),
                            kind: "reject_once".to_string(),
                            name: "Deny".to_string(),
                            auto_approvable: false,
                        },
                    ],
                })
                .await
        });

        let prompt = prompts.recv().await.unwrap();
        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: format!("/approve {}", prompt.id),
                    user_id: Some("U1".to_string()),
                },
                &mut output,
            )
            .await
            .unwrap();

        assert!(output.final_reply().contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
        assert!(executor.prompts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn normal_message_routes_with_projected_context() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        state.transcript.push(TranscriptMessage::user("prior"));
        store.save(state).await;
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "next".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "fake response");
        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("user: prior"));
        assert!(prompts[0].prompt.contains("Current user message:\nnext"));
        drop(prompts);
        let prepared = executor.prepared.lock().await;
        assert_eq!(prepared[0].previous_session_id, None);
        drop(prepared);
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 3);
        assert!(
            saved.executor_bindings["kimi"]
                .external_session_id
                .is_some()
        );
        assert_eq!(saved.executor_bindings["kimi"].protocol, "fake");
    }

    #[tokio::test]
    async fn observe_records_existing_session_context_without_executor_turn() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let mut state = SessionState::new("slack:channel:C1:111.000", "kimi");
        state.transcript.push(TranscriptMessage::user("root"));
        store.save(state).await;
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        router
            .observe(RouterInput {
                session_key: "slack:channel:C1:111.000".to_string(),
                text: "middle context".to_string(),
                user_id: Some("U2".to_string()),
            })
            .await
            .unwrap();

        assert!(executor.prepared.lock().await.is_empty());
        assert!(executor.prompts.lock().await.is_empty());
        let saved = store.load("slack:channel:C1:111.000").await.unwrap();
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[1].content, "middle context");

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:channel:C1:111.000".to_string(),
                    text: "next".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "fake response");
        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("user: root"));
        assert!(prompts[0].prompt.contains("user: middle context"));
        assert!(prompts[0].prompt.contains("Current user message:\nnext"));
    }

    #[tokio::test]
    async fn observe_does_not_create_unknown_session() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        router
            .observe(RouterInput {
                session_key: "slack:channel:C1:111.000".to_string(),
                text: "orphan context".to_string(),
                user_id: Some("U2".to_string()),
            })
            .await
            .unwrap();

        assert!(store.load("slack:channel:C1:111.000").await.is_none());
        assert!(executor.prepared.lock().await.is_empty());
        assert!(executor.prompts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn workspace_root_assigns_stable_distinct_session_cwds() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone())
            .with_workspace_root(Some(workspace_root.clone()));
        let mut output = CollectingRouterOutputSink::default();

        for (session_key, text) in [
            ("slack:dm:D1:111.000", "first"),
            ("slack:dm:D1:222.000", "second"),
            ("slack:dm:D1:111.000", "third"),
        ] {
            router
                .handle(
                    RouterInput {
                        session_key: session_key.to_string(),
                        text: text.to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
        }

        let prepared = executor.prepared.lock().await;
        let first_cwd = prepared[0].cwd.as_ref().unwrap();
        let second_cwd = prepared[1].cwd.as_ref().unwrap();
        let third_cwd = prepared[2].cwd.as_ref().unwrap();

        assert!(first_cwd.starts_with(workspace_root.canonicalize().unwrap()));
        assert!(first_cwd.is_dir());
        assert!(second_cwd.is_dir());
        assert_ne!(first_cwd, second_cwd);
        assert_eq!(first_cwd, third_cwd);
        assert!(
            !first_cwd
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(':')
        );

        let saved = store.load_or_create("slack:dm:D1:111.000", "kimi").await;
        let first_cwd_text = first_cwd.display().to_string();
        assert_eq!(
            saved.cwd.as_ref().unwrap().canonicalize().unwrap(),
            first_cwd.clone()
        );
        assert_eq!(
            saved.executor_bindings["kimi"].cwd.as_deref(),
            Some(first_cwd_text.as_str())
        );
    }

    #[tokio::test]
    async fn stores_prepared_machine_workspace_and_executor_visible_cwd() {
        #[derive(Debug)]
        struct MachineAwareBackend;

        #[async_trait::async_trait]
        impl ExecutorBackend for MachineAwareBackend {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                    machine_id: "remote-dev".to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                _request: ExecutorPrepareRequest,
                _cancel: TurnCancellation,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("remote-session".to_string()),
                    started_new_session: true,
                    machine_id: Some("remote-dev".to_string()),
                    cwd: Some("/remote/work/session-a".to_string()),
                    machine_workspace: Some(crate::machine::MachineWorkspaceRecord {
                        machine_id: "remote-dev".to_string(),
                        cwd: "/remote/work/session-a".to_string(),
                        materialization:
                            crate::machine::MachineWorkspaceMaterialization::Materialized,
                        artifact_fingerprint: Some("fingerprint".to_string()),
                    }),
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                _events: &mut dyn ExecutorEventSink,
                _cancel: TurnCancellation,
            ) -> ExecutorPromptOutcome {
                ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                    final_text: "done".to_string(),
                })
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let router = AgentRouter::new("kimi", store.clone(), Arc::new(MachineAwareBackend));
        let mut output = CollectingRouterOutputSink::default();

        router
            .handle(
                RouterInput {
                    session_key: "session-a".to_string(),
                    text: "hello".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let saved = store.load_or_create("session-a", "kimi").await;
        let binding = &saved.executor_bindings["kimi"];
        assert_eq!(binding.machine_id.as_deref(), Some("remote-dev"));
        assert_eq!(binding.cwd.as_deref(), Some("/remote/work/session-a"));
        assert_eq!(
            binding.external_session_id.as_deref(),
            Some("remote-session")
        );
        let workspace = &saved.machine_workspaces["remote-dev"];
        assert_eq!(workspace.cwd, "/remote/work/session-a");
        assert_eq!(
            workspace.materialization,
            crate::machine::MachineWorkspaceMaterialization::Materialized
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn workspace_root_rejects_symlink_session_cwd() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let session_key = "slack:dm:D1:111.000";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        symlink(&outside, &cwd).unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router =
            AgentRouter::new("kimi", store, executor).with_workspace_root(Some(workspace_root));
        let mut output = CollectingRouterOutputSink::default();

        let err = router
            .handle(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "hello".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("symlink"));
        assert!(!router.turns.has_current(session_key).await);
        assert!(!outside.join("slack").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cwd_validation_failure_does_not_replace_existing_active_turn() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let session_key = "slack:dm:D1:111.000";
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, mut cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(
            AgentRouter::new("kimi", store, executor)
                .with_workspace_root(Some(workspace_root.clone())),
        );

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: session_key.to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        std::fs::remove_dir_all(&cwd).unwrap();
        symlink(&outside, &cwd).unwrap();
        let mut output = CollectingRouterOutputSink::default();
        let err = router
            .handle(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("symlink"));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut cancelled_rx)
                .await
                .is_err()
        );
        assert!(router.turns.has_current(session_key).await);

        let mut stop_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "/stop".to_string(),
                    user_id: None,
                },
                &mut stop_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();
        assert_eq!(stop_output.final_reply(), "Stopped the active turn.");
        assert!(first_output.events.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reserved_cwd_validation_failure_clears_placeholder_turn() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let session_key = "slack:dm:D1:111.000";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        symlink(&outside, &cwd).unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router =
            AgentRouter::new("kimi", store, executor).with_workspace_root(Some(workspace_root));
        let reservation = router
            .reserve_turn(session_key, TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        let mut output = CollectingRouterOutputSink::default();

        let err = router
            .handle_reserved(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "hello".to_string(),
                    user_id: None,
                },
                reservation,
                None,
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("symlink"));
        assert!(!router.turns.has_current(session_key).await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reserved_cwd_validation_failure_does_not_leave_replacement_placeholder() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let session_key = "slack:dm:D1:111.000";
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(
            AgentRouter::new("kimi", store, executor)
                .with_workspace_root(Some(workspace_root.clone())),
        );

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: session_key.to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        std::fs::remove_dir_all(&cwd).unwrap();
        symlink(&outside, &cwd).unwrap();
        let reservation = router
            .reserve_turn(session_key, TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let mut output = CollectingRouterOutputSink::default();

        let err = router
            .handle_reserved(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                reservation,
                None,
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("symlink"));
        assert!(!router.turns.has_current(session_key).await);
        assert!(first.await.unwrap().events.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_context_rejects_symlink_session_cwd() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&workspace_root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let session_key = "slack:channel:C1:111.000";
        symlink(
            &outside,
            workspace_root.join(session_workspace_dir_name(session_key)),
        )
        .unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router =
            AgentRouter::new("kimi", store, executor).with_workspace_root(Some(workspace_root));

        let err = router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:thread:C1:111.000".to_string(),
                    kind: "slack_current_thread".to_string(),
                    title: "Current Slack thread".to_string(),
                    source_locator: Some("slack://C1/111.000".to_string()),
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/current-thread.md"),
                        content: ContextFileContent::Text("thread".to_string()),
                    }],
                    metadata: BTreeMap::new(),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("symlink"));
        assert!(!outside.join("slack").exists());
    }

    #[test]
    fn session_workspace_dir_name_is_safe_and_stable() {
        let name = session_workspace_dir_name("slack:dm:D1:111.000");

        assert!(name.starts_with("slack-dm-d1-111-000-"));
        assert!(
            name.chars()
                .all(|ch| { ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' })
        );
        assert_eq!(name, session_workspace_dir_name("slack:dm:D1:111.000"));
        assert_ne!(name, session_workspace_dir_name("slack:dm:D1:222.000"));
    }

    #[tokio::test]
    async fn synced_context_artifacts_are_projected_and_marked_seen() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone())
            .with_workspace_root(Some(workspace_root));

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle_with_context(
                RouterInput {
                    session_key: "slack:channel:C1:111.000".to_string(),
                    text: "use the thread".to_string(),
                    user_id: Some("U1".to_string()),
                },
                Some(ContextSyncRequest {
                    session_key: "slack:channel:C1:111.000".to_string(),
                    source: "slack".to_string(),
                    base_path: PathBuf::from("slack"),
                    artifacts: vec![ContextArtifactInput {
                        id: "slack:thread:C1:111.000".to_string(),
                        kind: "slack_current_thread".to_string(),
                        title: "Current Slack thread".to_string(),
                        source_locator: Some("slack://C1/111.000".to_string()),
                        files: vec![ContextFileInput {
                            relative_path: PathBuf::from("slack/current-thread.md"),
                            content: ContextFileContent::Text("thread history".to_string()),
                        }],
                        metadata: Default::default(),
                    }],
                    remove_artifacts: Vec::new(),
                    unresolved: Vec::new(),
                }),
                &mut output,
            )
            .await
            .unwrap();

        let saved = store.load("slack:channel:C1:111.000").await.unwrap();
        let cwd = saved.cwd.clone().unwrap();
        assert!(cwd.join("slack/current-thread.md").is_file());
        assert!(cwd.join("slack/manifest.json").is_file());
        assert_eq!(saved.context_artifacts.len(), 2);

        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("Synced session context files"));
        assert!(prompts[0].prompt.contains("slack/manifest.json"));
        assert!(prompts[0].prompt.contains("slack/current-thread.md"));
        drop(prompts);

        let saved = store.load("slack:channel:C1:111.000").await.unwrap();
        let seen = &saved.executor_bindings["kimi"].seen_context;
        assert!(seen.iter().any(|item| item.starts_with("artifact:")));

        router
            .handle(
                RouterInput {
                    session_key: "slack:channel:C1:111.000".to_string(),
                    text: "next".to_string(),
                    user_id: Some("U1".to_string()),
                },
                &mut output,
            )
            .await
            .unwrap();
        let prompts = executor.prompts.lock().await;
        assert!(!prompts[1].prompt.contains("Synced session context files"));
    }

    #[tokio::test]
    async fn post_restart_context_sync_preserves_recovered_manifest_state() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:channel:C1:111.000";
        let executor = Arc::new(FakeExecutorBackend::default());
        let first_store = Arc::new(InMemorySessionStore::default());
        let first_router = AgentRouter::new("kimi", first_store, executor.clone())
            .with_workspace_root(Some(workspace_root.clone()));

        first_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:file:F1".to_string(),
                    kind: "slack_file".to_string(),
                    title: "Slack file F1".to_string(),
                    source_locator: None,
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                        content: ContextFileContent::Text("{}".to_string()),
                    }],
                    metadata: BTreeMap::from([("file_id".to_string(), json!("F1"))]),
                }],
                remove_artifacts: Vec::new(),
                unresolved: vec![ContextSyncIssueInput {
                    kind: "file".to_string(),
                    reference: "F2".to_string(),
                    reason: "transient".to_string(),
                }],
            })
            .await
            .unwrap();

        let restarted_store = Arc::new(InMemorySessionStore::default());
        let mut restarted_state = SessionState::new(session_key, "kimi");
        restarted_state
            .context_artifacts
            .push(ContextArtifactRecord {
                id: "other:artifact".to_string(),
                source: "other".to_string(),
                kind: "other".to_string(),
                title: "Other context".to_string(),
                source_locator: None,
                paths: vec!["other/context.md".to_string()],
                fingerprint: "artifact:other".to_string(),
                updated_at_ms: 1,
                metadata: BTreeMap::new(),
            });
        restarted_store.save(restarted_state).await;
        let restarted_router = AgentRouter::new("kimi", restarted_store.clone(), executor)
            .with_workspace_root(Some(workspace_root));
        restarted_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:thread:C1:111.000".to_string(),
                    kind: "slack_current_thread".to_string(),
                    title: "Current Slack thread".to_string(),
                    source_locator: Some("slack://C1/111.000".to_string()),
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/current-thread.md"),
                        content: ContextFileContent::Text("thread".to_string()),
                    }],
                    metadata: BTreeMap::new(),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            })
            .await
            .unwrap();
        let saved = restarted_store.load(session_key).await.unwrap();
        let artifacts = saved.context_artifacts;

        assert!(artifacts.iter().any(|record| record.kind == "slack_file"));
        assert!(
            artifacts
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        assert!(artifacts.iter().any(|record| record.source == "other"));
        let manifest = artifacts
            .iter()
            .find(|record| record.kind == "manifest")
            .unwrap();
        assert_eq!(manifest.metadata["unresolved_count"].as_u64(), Some(1));
        assert_eq!(
            manifest.metadata["unresolved"][0]["reference"].as_str(),
            Some("F2")
        );
    }

    #[tokio::test]
    async fn post_restart_context_sync_prefers_newer_manifest_over_stale_state() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:channel:C1:111.000";
        let executor = Arc::new(FakeExecutorBackend::default());
        let first_store = Arc::new(InMemorySessionStore::default());
        let first_router = AgentRouter::new("kimi", first_store, executor.clone())
            .with_workspace_root(Some(workspace_root.clone()));

        first_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:file:F1".to_string(),
                    kind: "slack_file".to_string(),
                    title: "Slack file F1".to_string(),
                    source_locator: None,
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                        content: ContextFileContent::Text("{}".to_string()),
                    }],
                    metadata: BTreeMap::from([("file_id".to_string(), json!("F1"))]),
                }],
                remove_artifacts: Vec::new(),
                unresolved: vec![ContextSyncIssueInput {
                    kind: "file".to_string(),
                    reference: "F2".to_string(),
                    reason: "transient".to_string(),
                }],
            })
            .await
            .unwrap();

        let restarted_store = Arc::new(InMemorySessionStore::default());
        let mut stale_state = SessionState::new(session_key, "kimi");
        stale_state.context_artifacts = vec![
            ContextArtifactRecord {
                id: "slack:manifest".to_string(),
                source: "slack".to_string(),
                kind: "manifest".to_string(),
                title: "Stale manifest".to_string(),
                source_locator: None,
                paths: vec!["slack/manifest.json".to_string()],
                fingerprint: "artifact:stale-manifest".to_string(),
                updated_at_ms: 0,
                metadata: BTreeMap::new(),
            },
            ContextArtifactRecord {
                id: "slack:file:F0".to_string(),
                source: "slack".to_string(),
                kind: "slack_file".to_string(),
                title: "Stale Slack file".to_string(),
                source_locator: None,
                paths: vec!["slack/files/F0/metadata.json".to_string()],
                fingerprint: "artifact:stale-file".to_string(),
                updated_at_ms: 0,
                metadata: BTreeMap::from([("file_id".to_string(), json!("F0"))]),
            },
        ];
        restarted_store.save(stale_state).await;

        let restarted_router = AgentRouter::new("kimi", restarted_store.clone(), executor)
            .with_workspace_root(Some(workspace_root));
        restarted_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:thread:C1:111.000".to_string(),
                    kind: "slack_current_thread".to_string(),
                    title: "Current Slack thread".to_string(),
                    source_locator: Some("slack://C1/111.000".to_string()),
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/current-thread.md"),
                        content: ContextFileContent::Text("thread".to_string()),
                    }],
                    metadata: BTreeMap::new(),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            })
            .await
            .unwrap();

        let artifacts = restarted_store
            .load(session_key)
            .await
            .unwrap()
            .context_artifacts;
        assert!(artifacts.iter().any(|record| record.id == "slack:file:F1"));
        assert!(!artifacts.iter().any(|record| record.id == "slack:file:F0"));
        assert!(
            artifacts
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        let manifest = artifacts
            .iter()
            .find(|record| record.kind == "manifest")
            .unwrap();
        assert_eq!(manifest.metadata["unresolved_count"].as_u64(), Some(1));
        assert_eq!(
            manifest.metadata["unresolved"][0]["reference"].as_str(),
            Some("F2")
        );
    }

    #[tokio::test]
    async fn post_restart_context_sync_prefers_recovered_manifest_over_invalid_state_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:channel:C1:111.000";
        let executor = Arc::new(FakeExecutorBackend::default());
        let first_store = Arc::new(InMemorySessionStore::default());
        let first_router = AgentRouter::new("kimi", first_store, executor.clone())
            .with_workspace_root(Some(workspace_root.clone()));

        first_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:file:F1".to_string(),
                    kind: "slack_file".to_string(),
                    title: "Slack file F1".to_string(),
                    source_locator: None,
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                        content: ContextFileContent::Text("{}".to_string()),
                    }],
                    metadata: BTreeMap::from([("file_id".to_string(), json!("F1"))]),
                }],
                remove_artifacts: Vec::new(),
                unresolved: vec![ContextSyncIssueInput {
                    kind: "file".to_string(),
                    reference: "F2".to_string(),
                    reason: "transient".to_string(),
                }],
            })
            .await
            .unwrap();

        let restarted_store = Arc::new(InMemorySessionStore::default());
        let mut stale_state = SessionState::new(session_key, "kimi");
        stale_state.context_artifacts = vec![
            ContextArtifactRecord {
                id: "slack:manifest".to_string(),
                source: "slack".to_string(),
                kind: "manifest".to_string(),
                title: "Invalid manifest pointer".to_string(),
                source_locator: None,
                paths: vec!["other/manifest.json".to_string()],
                fingerprint: "artifact:invalid-manifest".to_string(),
                updated_at_ms: u64::MAX,
                metadata: BTreeMap::new(),
            },
            ContextArtifactRecord {
                id: "slack:file:F0".to_string(),
                source: "slack".to_string(),
                kind: "slack_file".to_string(),
                title: "Stale Slack file".to_string(),
                source_locator: None,
                paths: vec!["slack/files/F0/metadata.json".to_string()],
                fingerprint: "artifact:stale-file".to_string(),
                updated_at_ms: u64::MAX,
                metadata: BTreeMap::from([("file_id".to_string(), json!("F0"))]),
            },
        ];
        restarted_store.save(stale_state).await;

        let restarted_router = AgentRouter::new("kimi", restarted_store.clone(), executor)
            .with_workspace_root(Some(workspace_root));
        restarted_router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:thread:C1:111.000".to_string(),
                    kind: "slack_current_thread".to_string(),
                    title: "Current Slack thread".to_string(),
                    source_locator: Some("slack://C1/111.000".to_string()),
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/current-thread.md"),
                        content: ContextFileContent::Text("thread".to_string()),
                    }],
                    metadata: BTreeMap::new(),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            })
            .await
            .unwrap();

        let artifacts = restarted_store
            .load(session_key)
            .await
            .unwrap()
            .context_artifacts;
        assert!(artifacts.iter().any(|record| record.id == "slack:file:F1"));
        assert!(!artifacts.iter().any(|record| record.id == "slack:file:F0"));
        assert!(
            artifacts
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        let manifest = artifacts
            .iter()
            .find(|record| record.kind == "manifest")
            .unwrap();
        assert_eq!(manifest.metadata["unresolved_count"].as_u64(), Some(1));
        assert_eq!(
            manifest.metadata["unresolved"][0]["reference"].as_str(),
            Some("F2")
        );
    }

    #[tokio::test]
    async fn sync_context_ignores_invalid_recovered_manifest_and_overwrites_it() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:channel:C1:111.000";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        std::fs::create_dir_all(cwd.join("slack")).unwrap();
        std::fs::write(cwd.join("slack/manifest.json"), "{not-json").unwrap();

        let store = Arc::new(InMemorySessionStore::default());
        let mut state = SessionState::new(session_key, "kimi");
        state.context_artifacts.push(ContextArtifactRecord {
            id: "slack:manifest".to_string(),
            source: "slack".to_string(),
            kind: "manifest".to_string(),
            title: "Stale manifest".to_string(),
            source_locator: None,
            paths: vec!["slack/manifest.json".to_string()],
            fingerprint: "artifact:stale-manifest".to_string(),
            updated_at_ms: 1,
            metadata: BTreeMap::new(),
        });
        store.save(state).await;
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor)
            .with_workspace_root(Some(workspace_root));

        router
            .sync_context(ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:thread:C1:111.000".to_string(),
                    kind: "slack_current_thread".to_string(),
                    title: "Current Slack thread".to_string(),
                    source_locator: Some("slack://C1/111.000".to_string()),
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/current-thread.md"),
                        content: ContextFileContent::Text("thread".to_string()),
                    }],
                    metadata: BTreeMap::new(),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            })
            .await
            .unwrap();

        let artifacts = store.load(session_key).await.unwrap().context_artifacts;
        assert!(
            artifacts
                .iter()
                .any(|record| record.kind == "slack_current_thread")
        );
        let manifest = std::fs::read_to_string(cwd.join("slack/manifest.json")).unwrap();
        assert!(serde_json::from_str::<serde_json::Value>(&manifest).is_ok());
        assert!(!manifest.contains("{not-json"));
    }

    #[tokio::test]
    async fn context_artifacts_recover_from_persisted_session_cwd() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let persisted_cwd = tmp.path().join("persisted-cwd");
        let session_key = "slack:channel:C1:111.000";
        let records = write_context_sync(
            &persisted_cwd,
            ContextSyncRequest {
                session_key: session_key.to_string(),
                source: "slack".to_string(),
                base_path: PathBuf::from("slack"),
                artifacts: vec![ContextArtifactInput {
                    id: "slack:file:F1".to_string(),
                    kind: "slack_file".to_string(),
                    title: "Slack file F1".to_string(),
                    source_locator: None,
                    files: vec![ContextFileInput {
                        relative_path: PathBuf::from("slack/files/F1/metadata.json"),
                        content: ContextFileContent::Text("{}".to_string()),
                    }],
                    metadata: BTreeMap::from([("file_id".to_string(), json!("F1"))]),
                }],
                remove_artifacts: Vec::new(),
                unresolved: Vec::new(),
            },
            &[],
        )
        .unwrap();
        assert!(records.iter().any(|record| record.id == "slack:file:F1"));

        let store = Arc::new(InMemorySessionStore::default());
        let mut state = SessionState::new(session_key, "kimi");
        state.cwd = Some(persisted_cwd);
        store.save(state).await;
        let executor = Arc::new(FakeExecutorBackend::default());
        let router =
            AgentRouter::new("kimi", store, executor).with_workspace_root(Some(workspace_root));

        let recovered = router
            .context_artifacts(session_key, "slack")
            .await
            .unwrap();

        assert!(recovered.iter().any(|record| record.id == "slack:file:F1"));
    }

    #[test]
    fn channel_events_include_progress_reasoning_and_tool_updates_only() {
        let updates = [
            ExecutorUpdate::new("plan", "Plan", "working", ""),
            ExecutorUpdate::new(
                "agent_message_chunk",
                "",
                "I will inspect the config first.",
                "",
            )
            .with_channel_event(ExecutorChannelEvent::agent_progress(
                "I will inspect the config first.",
            )),
            ExecutorUpdate::new(
                "agent_thought_chunk",
                "Reasoning",
                "raw thinking should not leak",
                "",
            ),
            ExecutorUpdate::new("tool_call", "Bash", "$ cargo test\nok", "completed")
                .with_channel_event(ExecutorChannelEvent::tool_call("Bash", "$ cargo test")),
            ExecutorUpdate::new("agent_thought_chunk", "Reasoning", "summary", "")
                .with_channel_event(ExecutorChannelEvent::reasoning_summary(
                    "I should inspect the config.",
                )),
        ];
        let events = updates
            .iter()
            .filter_map(|update| channel_event_from_executor_update("codex", update))
            .collect::<Vec<_>>();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind, RouterChannelEventKind::AgentProgress);
        assert_eq!(
            events[0].render_text(),
            "[codex] Progress\nI will inspect the config first."
        );
        assert_eq!(events[1].kind, RouterChannelEventKind::ToolCall);
        assert_eq!(
            events[1].render_text(),
            "[codex] Tool call: Bash\n$ cargo test"
        );
        assert_eq!(events[2].kind, RouterChannelEventKind::ReasoningSummary);
        assert_eq!(
            events[2].render_text(),
            "[codex] Reasoning summary\nI should inspect the config."
        );
    }

    #[test]
    fn compact_channel_events_suppress_single_successful_tool() {
        let events = vec![RouterChannelEvent {
            kind: RouterChannelEventKind::ToolCall,
            executor: "codex".to_string(),
            title: "Bash".to_string(),
            text: "exit: 0\nstatus: completed".to_string(),
        }];

        assert_eq!(render_compact_channel_events(&events), None);
    }

    #[test]
    fn live_compact_channel_events_show_single_successful_tool() {
        let events = vec![RouterChannelEvent {
            kind: RouterChannelEventKind::ToolCall,
            executor: "codex".to_string(),
            title: "Bash".to_string(),
            text: "$ sleep 3\nexit: 0\nstatus: completed".to_string(),
        }];

        assert_eq!(
            render_live_compact_channel_events(&events).as_deref(),
            Some("[codex] Activity\nCommands:\n- `sleep 3`")
        );
    }

    #[test]
    fn compact_channel_events_include_latest_progress() {
        let events = vec![
            RouterChannelEvent {
                kind: RouterChannelEventKind::AgentProgress,
                executor: "codex".to_string(),
                title: "Progress".to_string(),
                text: "I will inspect the config first.".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::AgentProgress,
                executor: "codex".to_string(),
                title: "Progress".to_string(),
                text: "Now I will add the focused test.".to_string(),
            },
        ];

        assert_eq!(
            render_compact_channel_events(&events).as_deref(),
            Some("[codex] Activity\nProgress: Now I will add the focused test.")
        );
    }

    #[test]
    fn compact_channel_events_group_activity() {
        let events = vec![
            RouterChannelEvent {
                kind: RouterChannelEventKind::AgentProgress,
                executor: "codex".to_string(),
                title: "Progress".to_string(),
                text: "I will inspect the failing test first.".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ReasoningSummary,
                executor: "codex".to_string(),
                title: "Reasoning".to_string(),
                text: "Need to inspect the failing test first.".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "Base".to_string(),
                text: "$ sleep 3\nexit: 0\nstatus: completed".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "Base".to_string(),
                text: "$ sleep 3\nexit: 0\nstatus: completed".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "Base".to_string(),
                text: "$ sleep 3\nexit: 0\nstatus: completed".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "Bash".to_string(),
                text: "$ cargo test -q\nexit: 0\nstatus: completed".to_string(),
            },
            RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "dynamicToolCall".to_string(),
                text: "read_file\nstatus: completed".to_string(),
            },
        ];

        assert_eq!(
            render_compact_channel_events(&events).as_deref(),
            Some(
                "[codex] Activity\nReasoning: Need to inspect the failing test first.\nCommands:\n- `cargo test -q`\n- `sleep 3` x3\nTools:\n- read_file\nProgress: I will inspect the failing test first."
            )
        );
    }

    #[test]
    fn compact_channel_events_show_recent_commands_before_more() {
        let events = (1..=8)
            .map(|index| RouterChannelEvent {
                kind: RouterChannelEventKind::ToolCall,
                executor: "codex".to_string(),
                title: "Base".to_string(),
                text: format!("$ cmd {index}\nexit: 0\nstatus: completed"),
            })
            .collect::<Vec<_>>();

        assert_eq!(
            render_live_compact_channel_events(&events).as_deref(),
            Some(
                "[codex] Activity\nCommands:\n- `cmd 8`\n- `cmd 7`\n- `cmd 6`\n- `cmd 5`\n- `cmd 4`\n- `cmd 3`\n- 2 more"
            )
        );
    }

    #[test]
    fn compact_channel_events_keep_failed_tool_attention() {
        let events = vec![RouterChannelEvent {
            kind: RouterChannelEventKind::ToolCall,
            executor: "codex".to_string(),
            title: "Bash".to_string(),
            text: "$ sleep 3\nexit: 2\nstatus: failed".to_string(),
        }];

        assert_eq!(
            render_compact_channel_events(&events).as_deref(),
            Some("[codex] Activity\nCommands:\n- `sleep 3`\nAttention:\n- `sleep 3`: exit: 2")
        );
    }

    #[tokio::test]
    async fn executor_channel_events_are_emitted_before_final_reply() {
        #[derive(Debug)]
        struct StreamingExecutorBackend {
            release: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        }

        #[async_trait::async_trait]
        impl ExecutorBackend for StreamingExecutorBackend {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                    machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                _request: ExecutorPrepareRequest,
                _cancel: TurnCancellation,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("stream-session".to_string()),
                    started_new_session: true,
                    machine_id: None,
                    cwd: None,
                    machine_workspace: None,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                events: &mut dyn ExecutorEventSink,
                _cancel: TurnCancellation,
            ) -> ExecutorPromptOutcome {
                if let Err(err) = events
                    .send(
                        ExecutorUpdate::new("tool_call", "Bash", "$ cargo test", "completed")
                            .with_transcript_summary("Bash: status: completed")
                            .with_channel_event(ExecutorChannelEvent::tool_call(
                                "Bash",
                                "status: completed",
                            )),
                    )
                    .await
                {
                    return ExecutorPromptOutcome::Failed(err);
                }
                let release = match self
                    .release
                    .lock()
                    .await
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("release gate already consumed"))
                {
                    Ok(release) => release,
                    Err(err) => return ExecutorPromptOutcome::Failed(err),
                };
                if let Err(err) = release.await {
                    return ExecutorPromptOutcome::Failed(err.into());
                }
                ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                    final_text: "done".to_string(),
                })
            }
        }

        struct ChannelRouterOutputSink {
            tx: tokio::sync::mpsc::UnboundedSender<RouterOutputEvent>,
        }

        #[async_trait::async_trait]
        impl RouterOutputSink for ChannelRouterOutputSink {
            fn send_channel_event(&mut self, event: RouterChannelEvent) {
                let _ = self.tx.send(RouterOutputEvent::Channel(event));
            }

            async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
                self.tx
                    .send(RouterOutputEvent::FinalReply(text))
                    .map_err(|_| anyhow::anyhow!("router output receiver dropped"))
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(StreamingExecutorBackend {
            release: tokio::sync::Mutex::new(Some(release_rx)),
        });
        let router = AgentRouter::new("kimi", store, executor);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(async move {
            let mut output = ChannelRouterOutputSink { tx };
            router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "run tests".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
        });

        let first = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(first, RouterOutputEvent::Channel(_)));
        assert!(rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(second, RouterOutputEvent::FinalReply(text) if text == "done"));
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn best_effort_channel_event_delivery_does_not_fail_successful_turn() {
        #[derive(Debug)]
        struct ToolEventExecutorBackend;

        #[async_trait::async_trait]
        impl ExecutorBackend for ToolEventExecutorBackend {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                    machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                _request: ExecutorPrepareRequest,
                _cancel: TurnCancellation,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("tool-session".to_string()),
                    started_new_session: true,
                    machine_id: None,
                    cwd: None,
                    machine_workspace: None,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                events: &mut dyn ExecutorEventSink,
                _cancel: TurnCancellation,
            ) -> ExecutorPromptOutcome {
                if let Err(err) = events
                    .send(
                        ExecutorUpdate::new("tool_call", "Bash", "$ cargo test", "completed")
                            .with_transcript_summary("Bash: status: completed")
                            .with_channel_event(ExecutorChannelEvent::tool_call(
                                "Bash",
                                "status: completed",
                            )),
                    )
                    .await
                {
                    return ExecutorPromptOutcome::Failed(err);
                }
                ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                    final_text: "done".to_string(),
                })
            }
        }

        #[derive(Default)]
        struct BestEffortChannelEventSink {
            channel_event_count: usize,
            final_replies: Vec<String>,
        }

        #[async_trait::async_trait]
        impl RouterOutputSink for BestEffortChannelEventSink {
            fn send_channel_event(&mut self, _event: RouterChannelEvent) {
                self.channel_event_count += 1;
            }

            async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
                self.final_replies.push(text);
                Ok(())
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(ToolEventExecutorBackend);
        let router = AgentRouter::new("kimi", store.clone(), executor);
        let mut output = BestEffortChannelEventSink::default();

        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "run tests".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.channel_event_count, 1);
        assert_eq!(output.final_replies, ["done".to_string()]);
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(
            saved.executor_bindings["kimi"].health,
            ExecutorHealth::Healthy
        );
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PrepareCancellationPoint {
        BeforePublication,
        AfterPublication,
    }

    struct PrepareCancellationExecutorBackend {
        point: PrepareCancellationPoint,
        prepare_started: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        prepare_cancelled: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        prepare_count: tokio::sync::Mutex<usize>,
        published_sessions: tokio::sync::Mutex<Vec<String>>,
        prompts: tokio::sync::Mutex<Vec<String>>,
    }

    impl PrepareCancellationExecutorBackend {
        fn new(
            point: PrepareCancellationPoint,
            prepare_started: tokio::sync::oneshot::Sender<()>,
            prepare_cancelled: tokio::sync::oneshot::Sender<()>,
        ) -> Self {
            Self {
                point,
                prepare_started: tokio::sync::Mutex::new(Some(prepare_started)),
                prepare_cancelled: tokio::sync::Mutex::new(Some(prepare_cancelled)),
                prepare_count: tokio::sync::Mutex::new(0),
                published_sessions: tokio::sync::Mutex::new(Vec::new()),
                prompts: tokio::sync::Mutex::new(Vec::new()),
            }
        }

        async fn publish_session(&self, id: &str) {
            let mut sessions = self.published_sessions.lock().await;
            if !sessions.iter().any(|session| session == id) {
                sessions.push(id.to_string());
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for PrepareCancellationExecutorBackend {
        fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
            (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
                machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
            })
        }

        fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
            self.get("kimi").into_iter().collect()
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            cancel: TurnCancellation,
        ) -> anyhow::Result<crate::executor::PreparedExecutor> {
            let prepare_index = {
                let mut count = self.prepare_count.lock().await;
                *count += 1;
                *count
            };

            if prepare_index == 1 {
                if self.point == PrepareCancellationPoint::AfterPublication {
                    self.publish_session("shared-session").await;
                }
                if let Some(started) = self.prepare_started.lock().await.take() {
                    let _ = started.send(());
                }
                let _ = cancel.cancelled().await;
                if let Some(cancelled) = self.prepare_cancelled.lock().await.take() {
                    let _ = cancelled.send(());
                }
                anyhow::bail!("prepare cancelled");
            }

            let external_session_id = match self.point {
                PrepareCancellationPoint::BeforePublication => {
                    let id = format!("published-session-{prepare_index}");
                    self.publish_session(&id).await;
                    id
                }
                PrepareCancellationPoint::AfterPublication => {
                    self.publish_session("shared-session").await;
                    "shared-session".to_string()
                }
            };

            let started_new_session =
                request.previous_session_id.as_deref() != Some(external_session_id.as_str());
            Ok(crate::executor::PreparedExecutor {
                external_session_id: Some(external_session_id),
                started_new_session,
                machine_id: None,
                cwd: None,
                machine_workspace: None,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            _events: &mut dyn ExecutorEventSink,
            _cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            let prompt_index = {
                let mut prompts = self.prompts.lock().await;
                prompts.push(request.prompt);
                prompts.len()
            };
            ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                final_text: format!("response {prompt_index}"),
            })
        }
    }

    async fn run_prepare_cancellation_test(
        point: PrepareCancellationPoint,
        expected_published_sessions: &[&str],
        expected_external_session_id: &str,
    ) {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(PrepareCancellationExecutorBackend::new(
            point,
            started_tx,
            cancelled_tx,
        ));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor.clone()));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut second_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                &mut second_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert!(first_output.events.is_empty());
        assert_eq!(second_output.final_reply(), "response 1");
        assert_eq!(
            executor.published_sessions.lock().await.clone(),
            expected_published_sessions
                .iter()
                .map(|session| (*session).to_string())
                .collect::<Vec<_>>()
        );
        let prompts = executor.prompts.lock().await;
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].contains("Current user message:\nsecond"));
        drop(prompts);

        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[0].content, "second");
        let binding = saved.executor_bindings.get("kimi").unwrap();
        assert_eq!(binding.health, ExecutorHealth::Healthy);
        assert_eq!(
            binding.external_session_id.as_deref(),
            Some(expected_external_session_id)
        );
        assert!(!router.turns.has_current("slack:C1:T1").await);
    }

    #[tokio::test]
    async fn prepare_cancelled_before_session_publication_does_not_mark_binding_unhealthy() {
        run_prepare_cancellation_test(
            PrepareCancellationPoint::BeforePublication,
            &["published-session-2"],
            "published-session-2",
        )
        .await;
    }

    #[tokio::test]
    async fn prepare_cancelled_after_session_publication_keeps_published_session_reusable() {
        run_prepare_cancellation_test(
            PrepareCancellationPoint::AfterPublication,
            &["shared-session"],
            "shared-session",
        )
        .await;
    }

    struct PromptCancellationExecutorBackend {
        emit_before_cancel: bool,
        emit_after_cancel: bool,
        ready_to_cancel: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        prompt_cancelled: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        prompts: tokio::sync::Mutex<Vec<String>>,
    }

    impl PromptCancellationExecutorBackend {
        fn new(
            emit_before_cancel: bool,
            emit_after_cancel: bool,
            ready_to_cancel: tokio::sync::oneshot::Sender<()>,
            prompt_cancelled: tokio::sync::oneshot::Sender<()>,
        ) -> Self {
            Self {
                emit_before_cancel,
                emit_after_cancel,
                ready_to_cancel: tokio::sync::Mutex::new(Some(ready_to_cancel)),
                prompt_cancelled: tokio::sync::Mutex::new(Some(prompt_cancelled)),
                prompts: tokio::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for PromptCancellationExecutorBackend {
        fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
            (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
                machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
            })
        }

        fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
            self.get("kimi").into_iter().collect()
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            _cancel: TurnCancellation,
        ) -> anyhow::Result<crate::executor::PreparedExecutor> {
            let previous_session_id = request.previous_session_id;
            let external_session_id = previous_session_id
                .clone()
                .unwrap_or_else(|| "prompt-session".to_string());
            let started_new_session =
                previous_session_id.as_deref() != Some(external_session_id.as_str());
            Ok(crate::executor::PreparedExecutor {
                external_session_id: Some(external_session_id),
                started_new_session,
                machine_id: None,
                cwd: None,
                machine_workspace: None,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            events: &mut dyn ExecutorEventSink,
            cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            self.prompts.lock().await.push(request.prompt);
            if self.emit_before_cancel
                && let Err(err) = events
                    .send(
                        ExecutorUpdate::new("progress", "Progress", "before cancel", "")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(
                                "before cancel",
                            )),
                    )
                    .await
            {
                return ExecutorPromptOutcome::Failed(err);
            }
            if let Some(ready) = self.ready_to_cancel.lock().await.take() {
                let _ = ready.send(());
            }
            let _ = cancel.cancelled().await;
            if self.emit_after_cancel
                && let Err(err) = events
                    .send(
                        ExecutorUpdate::new("progress", "Progress", "after cancel", "")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(
                                "after cancel",
                            )),
                    )
                    .await
            {
                return ExecutorPromptOutcome::Failed(err);
            }
            if let Some(cancelled) = self.prompt_cancelled.lock().await.take() {
                let _ = cancelled.send(());
            }
            ExecutorPromptOutcome::Cancelled
        }
    }

    fn session_with_healthy_binding(session_key: &str) -> SessionState {
        let mut state = SessionState::new(session_key, "kimi");
        state.transcript.push(TranscriptMessage::user("prior"));
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "fake".to_string(),
                machine_id: Some(crate::machine::LOCAL_MACHINE_ID.to_string()),
                external_session_id: Some("existing-session".to_string()),
                health: ExecutorHealth::Healthy,
                ..ExecutorBinding::default()
            },
        );
        state
    }

    async fn run_prompt_cancellation_test(
        emit_before_cancel: bool,
        emit_after_cancel: bool,
        expected_turn_events: Vec<RouterOutputEvent>,
    ) {
        let session_key = "slack:C1:T1";
        let store = Arc::new(InMemorySessionStore::default());
        store.save(session_with_healthy_binding(session_key)).await;
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(PromptCancellationExecutorBackend::new(
            emit_before_cancel,
            emit_after_cancel,
            ready_tx,
            cancelled_tx,
        ));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor));

        let turn_router = router.clone();
        let turn = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            turn_router
                .handle(
                    RouterInput {
                        session_key: session_key.to_string(),
                        text: "cancel me".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), ready_rx)
            .await
            .unwrap()
            .unwrap();

        let mut stop_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "/stop".to_string(),
                    user_id: None,
                },
                &mut stop_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let turn_output = turn.await.unwrap();

        assert_eq!(turn_output.events, expected_turn_events);
        assert_eq!(stop_output.final_reply(), "Stopped the active turn.");
        let saved = store.load_or_create(session_key, "kimi").await;
        assert_eq!(saved.transcript.len(), 1);
        assert_eq!(saved.transcript[0].content, "prior");
        let binding = saved.executor_bindings.get("kimi").unwrap();
        assert_eq!(binding.health, ExecutorHealth::Healthy);
        assert_eq!(
            binding.external_session_id.as_deref(),
            Some("existing-session")
        );
        assert!(!router.turns.has_current(session_key).await);
    }

    #[tokio::test]
    async fn prompt_cancelled_before_first_backend_event_does_not_commit_turn() {
        run_prompt_cancellation_test(false, false, Vec::new()).await;
    }

    #[tokio::test]
    async fn prompt_cancelled_after_backend_events_does_not_commit_turn() {
        run_prompt_cancellation_test(
            true,
            true,
            vec![RouterOutputEvent::Channel(RouterChannelEvent {
                kind: RouterChannelEventKind::AgentProgress,
                executor: "kimi".to_string(),
                title: "Progress".to_string(),
                text: "before cancel".to_string(),
            })],
        )
        .await;
    }

    struct InterruptibleExecutorBackend {
        prompts: tokio::sync::Mutex<Vec<String>>,
        interrupts: tokio::sync::Mutex<Vec<ExecutorInterruptRequest>>,
        first_started: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        first_cancelled: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl InterruptibleExecutorBackend {
        fn new(
            first_started: tokio::sync::oneshot::Sender<()>,
            first_cancelled: tokio::sync::oneshot::Sender<()>,
        ) -> Self {
            Self {
                prompts: tokio::sync::Mutex::new(Vec::new()),
                interrupts: tokio::sync::Mutex::new(Vec::new()),
                first_started: tokio::sync::Mutex::new(Some(first_started)),
                first_cancelled: tokio::sync::Mutex::new(Some(first_cancelled)),
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for InterruptibleExecutorBackend {
        fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
            (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
                machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
            })
        }

        fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
            self.get("kimi").into_iter().collect()
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            cancel: TurnCancellation,
        ) -> anyhow::Result<crate::executor::PreparedExecutor> {
            if cancel.is_cancelled().await {
                anyhow::bail!("prepare cancelled");
            }
            Ok(crate::executor::PreparedExecutor {
                external_session_id: Some(format!("{}-session", request.turn.executor)),
                started_new_session: request.previous_session_id.is_none(),
                machine_id: None,
                cwd: None,
                machine_workspace: None,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            _events: &mut dyn ExecutorEventSink,
            cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            let prompt_index = {
                let mut prompts = self.prompts.lock().await;
                prompts.push(request.prompt);
                prompts.len()
            };
            if prompt_index == 1 {
                if let Some(started) = self.first_started.lock().await.take() {
                    let _ = started.send(());
                }
                let _ = cancel.cancelled().await;
                if let Some(cancelled) = self.first_cancelled.lock().await.take() {
                    let _ = cancelled.send(());
                }
                return ExecutorPromptOutcome::Cancelled;
            }
            ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                final_text: format!("response {prompt_index}"),
            })
        }

        async fn interrupt(&self, request: ExecutorInterruptRequest) -> anyhow::Result<()> {
            self.interrupts.lock().await.push(request);
            Ok(())
        }
    }

    #[tokio::test]
    async fn new_message_interrupts_active_turn_and_commits_replacement_only() {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor.clone()));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut second_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                &mut second_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert!(first_output.events.is_empty());
        assert_eq!(second_output.final_reply(), "response 2");
        let interrupts = executor.interrupts.lock().await;
        assert_eq!(interrupts.len(), 1);
        assert_eq!(interrupts[0].turn.session_key, "slack:C1:T1");
        assert_eq!(interrupts[0].turn.executor, "kimi");
        assert_eq!(interrupts[0].turn.generation, 1);
        assert_eq!(interrupts[0].reason, InterruptReason::ReplacedByNewMessage);
        drop(interrupts);

        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[0].content, "second");
        assert!(saved.transcript[1].content.contains("response 2"));
        assert!(
            !saved
                .transcript
                .iter()
                .any(|message| message.content == "first")
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum StaleFirstOutcome {
        Completed,
        Failed,
    }

    struct StaleOutcomeExecutorBackend {
        outcome: StaleFirstOutcome,
        prompts: tokio::sync::Mutex<Vec<String>>,
        first_started: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        first_finished: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl StaleOutcomeExecutorBackend {
        fn new(
            outcome: StaleFirstOutcome,
            first_started: tokio::sync::oneshot::Sender<()>,
            first_finished: tokio::sync::oneshot::Sender<()>,
        ) -> Self {
            Self {
                outcome,
                prompts: tokio::sync::Mutex::new(Vec::new()),
                first_started: tokio::sync::Mutex::new(Some(first_started)),
                first_finished: tokio::sync::Mutex::new(Some(first_finished)),
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for StaleOutcomeExecutorBackend {
        fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
            (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
                machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
            })
        }

        fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
            self.get("kimi").into_iter().collect()
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            _cancel: TurnCancellation,
        ) -> anyhow::Result<crate::executor::PreparedExecutor> {
            Ok(crate::executor::PreparedExecutor {
                external_session_id: Some(format!("{}-session", request.turn.executor)),
                started_new_session: request.previous_session_id.is_none(),
                machine_id: None,
                cwd: None,
                machine_workspace: None,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            _events: &mut dyn ExecutorEventSink,
            cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            let prompt_index = {
                let mut prompts = self.prompts.lock().await;
                prompts.push(request.prompt);
                prompts.len()
            };
            if prompt_index == 1 {
                if let Some(started) = self.first_started.lock().await.take() {
                    let _ = started.send(());
                }
                let _ = cancel.cancelled().await;
                if let Some(finished) = self.first_finished.lock().await.take() {
                    let _ = finished.send(());
                }
                return match self.outcome {
                    StaleFirstOutcome::Completed => {
                        ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                            final_text: "stale response".to_string(),
                        })
                    }
                    StaleFirstOutcome::Failed => {
                        ExecutorPromptOutcome::Failed(anyhow::anyhow!("stale failure"))
                    }
                };
            }
            ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                final_text: "fresh response".to_string(),
            })
        }
    }

    async fn run_stale_first_outcome_test(outcome: StaleFirstOutcome) -> SessionState {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(StaleOutcomeExecutorBackend::new(
            outcome,
            started_tx,
            finished_tx,
        ));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .ok();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut second_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                &mut second_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert!(first_output.events.is_empty());
        assert_eq!(second_output.final_reply(), "fresh response");
        store.load_or_create("slack:C1:T1", "kimi").await
    }

    #[tokio::test]
    async fn stale_successful_turn_cannot_commit_after_replacement() {
        let saved = run_stale_first_outcome_test(StaleFirstOutcome::Completed).await;

        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[0].content, "second");
        assert!(saved.transcript[1].content.contains("fresh response"));
        assert!(
            !saved
                .transcript
                .iter()
                .any(|message| message.content.contains("stale response"))
        );
        assert_eq!(
            saved.executor_bindings["kimi"].health,
            ExecutorHealth::Healthy
        );
    }

    #[tokio::test]
    async fn stale_failed_turn_cannot_mark_binding_unhealthy_after_replacement() {
        let saved = run_stale_first_outcome_test(StaleFirstOutcome::Failed).await;

        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[0].content, "second");
        assert_eq!(
            saved.executor_bindings["kimi"].health,
            ExecutorHealth::Healthy
        );
    }

    struct EventAfterCancelExecutorBackend {
        prompts: tokio::sync::Mutex<Vec<String>>,
        first_started: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        first_cancelled: tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl EventAfterCancelExecutorBackend {
        fn new(
            first_started: tokio::sync::oneshot::Sender<()>,
            first_cancelled: tokio::sync::oneshot::Sender<()>,
        ) -> Self {
            Self {
                prompts: tokio::sync::Mutex::new(Vec::new()),
                first_started: tokio::sync::Mutex::new(Some(first_started)),
                first_cancelled: tokio::sync::Mutex::new(Some(first_cancelled)),
            }
        }
    }

    #[async_trait::async_trait]
    impl ExecutorBackend for EventAfterCancelExecutorBackend {
        fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
            (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                name: "kimi".to_string(),
                protocol: "fake".to_string(),
                machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
            })
        }

        fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
            self.get("kimi").into_iter().collect()
        }

        async fn prepare(
            &self,
            request: ExecutorPrepareRequest,
            _cancel: TurnCancellation,
        ) -> anyhow::Result<crate::executor::PreparedExecutor> {
            Ok(crate::executor::PreparedExecutor {
                external_session_id: Some(format!("{}-session", request.turn.executor)),
                started_new_session: request.previous_session_id.is_none(),
                machine_id: None,
                cwd: None,
                machine_workspace: None,
            })
        }

        async fn prompt(
            &self,
            request: ExecutorPromptRequest,
            events: &mut dyn ExecutorEventSink,
            cancel: TurnCancellation,
        ) -> ExecutorPromptOutcome {
            let prompt_index = {
                let mut prompts = self.prompts.lock().await;
                prompts.push(request.prompt);
                prompts.len()
            };
            if prompt_index == 1 {
                if let Some(started) = self.first_started.lock().await.take() {
                    let _ = started.send(());
                }
                let _ = cancel.cancelled().await;
                let _ = events
                    .send(
                        ExecutorUpdate::new("progress", "Progress", "stale progress", "")
                            .with_channel_event(ExecutorChannelEvent::agent_progress(
                                "stale progress",
                            )),
                    )
                    .await;
                if let Some(cancelled) = self.first_cancelled.lock().await.take() {
                    let _ = cancelled.send(());
                }
                return ExecutorPromptOutcome::Cancelled;
            }
            ExecutorPromptOutcome::Completed(crate::executor::ExecutorResponse {
                final_text: "response 2".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn stale_turn_channel_events_are_suppressed_after_replacement() {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(EventAfterCancelExecutorBackend::new(
            started_tx,
            cancelled_tx,
        ));
        let router = Arc::new(AgentRouter::new("kimi", store, executor));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut second_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                &mut second_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert!(first_output.events.is_empty());
        assert_eq!(second_output.final_reply(), "response 2");
    }

    #[tokio::test]
    async fn unresolved_approval_command_does_not_interrupt_active_turn() {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, mut cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor.clone()));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut approval_output = CollectingRouterOutputSink::default();
        router
            .handle_with_context(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/approve 1".to_string(),
                    user_id: Some("U1".to_string()),
                },
                None,
                &mut approval_output,
            )
            .await
            .unwrap();

        assert_eq!(approval_output.final_reply(), "Approval 1 is not pending.");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut cancelled_rx)
                .await
                .is_err()
        );
        assert!(executor.interrupts.lock().await.is_empty());
        assert!(router.turns.has_current("slack:C1:T1").await);

        let mut stop_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/stop".to_string(),
                    user_id: None,
                },
                &mut stop_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), &mut cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert_eq!(stop_output.final_reply(), "Stopped the active turn.");
        assert!(first_output.events.is_empty());
    }

    #[tokio::test]
    async fn stop_cancels_active_turn_without_committing_transcript() {
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(AgentRouter::new("kimi", store.clone(), executor.clone()));

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let mut stop_output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/stop".to_string(),
                    user_id: None,
                },
                &mut stop_output,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();
        let first_output = first.await.unwrap();

        assert!(first_output.events.is_empty());
        assert_eq!(stop_output.final_reply(), "Stopped the active turn.");
        let interrupts = executor.interrupts.lock().await;
        assert_eq!(interrupts.len(), 1);
        assert_eq!(interrupts[0].reason, InterruptReason::UserStop);
        drop(interrupts);

        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert!(saved.transcript.is_empty());
    }

    #[tokio::test]
    async fn stop_command_skips_context_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor)
            .with_workspace_root(Some(tmp.path().join("workspaces")));

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle_with_context(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "/stop".to_string(),
                    user_id: None,
                },
                Some(ContextSyncRequest {
                    session_key: "slack:C1:T1".to_string(),
                    source: "slack".to_string(),
                    base_path: PathBuf::from("slack"),
                    artifacts: vec![ContextArtifactInput {
                        id: "thread".to_string(),
                        kind: "slack_current_thread".to_string(),
                        title: "Thread".to_string(),
                        source_locator: None,
                        files: vec![ContextFileInput {
                            relative_path: PathBuf::from("slack/current-thread.md"),
                            content: ContextFileContent::Text("should not sync".to_string()),
                        }],
                        metadata: Default::default(),
                    }],
                    remove_artifacts: Vec::new(),
                    unresolved: Vec::new(),
                }),
                &mut output,
            )
            .await
            .unwrap();

        assert_eq!(output.final_reply(), "No active turn for this session.");
        assert!(store.load("slack:C1:T1").await.is_none());
    }

    #[tokio::test]
    async fn preempted_context_error_clears_placeholder_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:C1:T1";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        std::fs::create_dir_all(cwd.join("slack/current-thread.md")).unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router =
            AgentRouter::new("kimi", store, executor).with_workspace_root(Some(workspace_root));
        let reservation = router
            .reserve_turn(session_key, TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        let mut output = CollectingRouterOutputSink::default();

        let err = router
            .handle_reserved(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                reservation,
                Some(ContextSyncRequest {
                    session_key: session_key.to_string(),
                    source: "slack".to_string(),
                    base_path: PathBuf::from("slack"),
                    artifacts: vec![ContextArtifactInput {
                        id: "thread".to_string(),
                        kind: "slack_current_thread".to_string(),
                        title: "Thread".to_string(),
                        source_locator: None,
                        files: vec![ContextFileInput {
                            relative_path: PathBuf::from("slack/current-thread.md"),
                            content: ContextFileContent::Text("thread context".to_string()),
                        }],
                        metadata: Default::default(),
                    }],
                    remove_artifacts: Vec::new(),
                    unresolved: Vec::new(),
                }),
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("context path is not a file"));
        assert!(!router.turns.has_current(session_key).await);
    }

    #[tokio::test]
    async fn preempted_context_turn_prevents_interrupted_turn_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
        let executor = Arc::new(InterruptibleExecutorBackend::new(started_tx, cancelled_tx));
        let router = Arc::new(
            AgentRouter::new("kimi", store.clone(), executor)
                .with_workspace_root(Some(tmp.path().join("workspaces"))),
        );

        let first_router = router.clone();
        let first = tokio::spawn(async move {
            let mut output = CollectingRouterOutputSink::default();
            first_router
                .handle(
                    RouterInput {
                        session_key: "slack:C1:T1".to_string(),
                        text: "first".to_string(),
                        user_id: None,
                    },
                    &mut output,
                )
                .await
                .unwrap();
            output
        });
        tokio::time::timeout(Duration::from_secs(1), started_rx)
            .await
            .unwrap()
            .unwrap();

        let reservation = router
            .reserve_turn("slack:C1:T1", TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .unwrap()
            .unwrap();

        let mut second_output = CollectingRouterOutputSink::default();
        router
            .handle_reserved(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "second".to_string(),
                    user_id: None,
                },
                reservation,
                Some(ContextSyncRequest {
                    session_key: "slack:C1:T1".to_string(),
                    source: "slack".to_string(),
                    base_path: PathBuf::from("slack"),
                    artifacts: vec![ContextArtifactInput {
                        id: "thread".to_string(),
                        kind: "slack_current_thread".to_string(),
                        title: "Thread".to_string(),
                        source_locator: None,
                        files: vec![ContextFileInput {
                            relative_path: PathBuf::from("slack/current-thread.md"),
                            content: ContextFileContent::Text("thread context".to_string()),
                        }],
                        metadata: Default::default(),
                    }],
                    remove_artifacts: Vec::new(),
                    unresolved: Vec::new(),
                }),
                &mut second_output,
            )
            .await
            .unwrap();

        assert!(first.await.unwrap().events.is_empty());
        assert_eq!(second_output.final_reply(), "response 2");
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        assert_eq!(saved.transcript.len(), 2);
        assert_eq!(saved.transcript[0].content, "second");
    }

    #[tokio::test]
    async fn stale_preempted_message_does_not_interrupt_newer_generation() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone());

        let stale_reservation = router
            .reserve_turn("slack:C1:T1", TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        let current_reservation = router
            .reserve_turn("slack:C1:T1", TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();

        let mut stale_output = CollectingRouterOutputSink::default();
        router
            .handle_reserved(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "older delayed".to_string(),
                    user_id: None,
                },
                stale_reservation,
                None,
                &mut stale_output,
            )
            .await
            .unwrap();
        assert!(stale_output.events.is_empty());
        assert!(executor.prompts.lock().await.is_empty());

        let mut current_output = CollectingRouterOutputSink::default();
        router
            .handle_reserved(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "newer".to_string(),
                    user_id: None,
                },
                current_reservation,
                None,
                &mut current_output,
            )
            .await
            .unwrap();

        assert_eq!(current_output.final_reply(), "fake response");
        let prompts = executor.prompts.lock().await;
        assert_eq!(prompts.len(), 1);
        assert!(prompts[0].prompt.contains("Current user message:\nnewer"));
    }

    #[tokio::test]
    async fn stale_reserved_context_does_not_commit_or_create_session_state() {
        let tmp = tempfile::tempdir().unwrap();
        let session_key = "slack:C1:T1";
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor.clone())
            .with_workspace_root(Some(tmp.path().join("workspaces")));

        let stale_reservation = router
            .reserve_turn(session_key, TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();
        let current_reservation = router
            .reserve_turn(session_key, TurnBeginMode::ReplaceActive)
            .await
            .unwrap()
            .unwrap();

        let mut stale_output = CollectingRouterOutputSink::default();
        router
            .handle_reserved(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "older delayed".to_string(),
                    user_id: None,
                },
                stale_reservation,
                Some(ContextSyncRequest {
                    session_key: session_key.to_string(),
                    source: "slack".to_string(),
                    base_path: PathBuf::from("slack"),
                    artifacts: vec![ContextArtifactInput {
                        id: "thread".to_string(),
                        kind: "slack_current_thread".to_string(),
                        title: "Thread".to_string(),
                        source_locator: None,
                        files: vec![ContextFileInput {
                            relative_path: PathBuf::from("slack/current-thread.md"),
                            content: ContextFileContent::Text("stale context".to_string()),
                        }],
                        metadata: Default::default(),
                    }],
                    remove_artifacts: Vec::new(),
                    unresolved: Vec::new(),
                }),
                &mut stale_output,
            )
            .await
            .unwrap();

        assert!(stale_output.events.is_empty());
        assert!(store.load(session_key).await.is_none());
        assert!(executor.prompts.lock().await.is_empty());

        let mut current_output = CollectingRouterOutputSink::default();
        router
            .handle_reserved(
                RouterInput {
                    session_key: session_key.to_string(),
                    text: "newer".to_string(),
                    user_id: None,
                },
                current_reservation,
                None,
                &mut current_output,
            )
            .await
            .unwrap();

        assert_eq!(current_output.final_reply(), "fake response");
        let saved = store.load(session_key).await.unwrap();
        assert!(saved.context_artifacts.is_empty());
        assert_eq!(saved.transcript[0].content, "newer");
    }

    #[tokio::test]
    async fn context_commit_rolls_back_files_when_turn_stale_after_install() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:C1:T1";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        let old_records = write_context_sync(
            &cwd,
            slack_thread_and_extra_context_request(session_key, "old context"),
            &[],
        )
        .unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor)
            .with_workspace_root(Some(workspace_root));
        let mut state = SessionState::new(session_key, "kimi");
        state.cwd = Some(cwd.clone());
        state.context_artifacts = old_records;
        store.save(state.clone()).await;
        let turn = router
            .turns
            .begin(session_key, "kimi".to_string())
            .await
            .guard;
        let prepared = router
            .prepare_context_sync_locked(slack_thread_replacement_context_request(
                session_key,
                "new context",
            ))
            .await
            .unwrap()
            .unwrap();
        let stale_turn = turn.clone();

        let committed = prepared
            .commit_if_current_with_hook(&turn, store.as_ref(), move |checkpoint| {
                let stale_turn = stale_turn.clone();
                async move {
                    if checkpoint == ContextCommitCheckpoint::AfterInstall {
                        let _ = stale_turn.finish_if_current().await;
                    }
                }
            })
            .await
            .unwrap();

        assert!(!committed);
        assert_eq!(
            std::fs::read_to_string(cwd.join("slack/current-thread.md")).unwrap(),
            "old context"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("slack/old-extra.md")).unwrap(),
            "old extra context"
        );
        let saved = store.load(session_key).await.unwrap();
        assert_context_record_restored(&saved, "thread", "slack/current-thread.md");
        assert_context_record_restored(&saved, "old-extra", "slack/old-extra.md");
    }

    #[tokio::test]
    async fn context_commit_restores_state_when_turn_stale_after_save() {
        let tmp = tempfile::tempdir().unwrap();
        let workspace_root = tmp.path().join("workspaces");
        let session_key = "slack:C1:T1";
        let cwd = workspace_root.join(session_workspace_dir_name(session_key));
        let old_records = write_context_sync(
            &cwd,
            slack_thread_and_extra_context_request(session_key, "old context"),
            &[],
        )
        .unwrap();
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let router = AgentRouter::new("kimi", store.clone(), executor)
            .with_workspace_root(Some(workspace_root));
        let mut state = SessionState::new(session_key, "kimi");
        state.cwd = Some(cwd.clone());
        state.context_artifacts = old_records;
        store.save(state.clone()).await;
        let turn = router
            .turns
            .begin(session_key, "kimi".to_string())
            .await
            .guard;
        let prepared = router
            .prepare_context_sync_locked(slack_thread_replacement_context_request(
                session_key,
                "new context",
            ))
            .await
            .unwrap()
            .unwrap();
        let stale_turn = turn.clone();

        let committed = prepared
            .commit_if_current_with_hook(&turn, store.as_ref(), move |checkpoint| {
                let stale_turn = stale_turn.clone();
                async move {
                    if checkpoint == ContextCommitCheckpoint::AfterStateSave {
                        let _ = stale_turn.finish_if_current().await;
                    }
                }
            })
            .await
            .unwrap();

        assert!(!committed);
        assert_eq!(
            std::fs::read_to_string(cwd.join("slack/current-thread.md")).unwrap(),
            "old context"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("slack/old-extra.md")).unwrap(),
            "old extra context"
        );
        let saved = store.load(session_key).await.unwrap();
        assert_context_record_restored(&saved, "thread", "slack/current-thread.md");
        assert_context_record_restored(&saved, "old-extra", "slack/old-extra.md");
    }

    #[tokio::test]
    async fn seen_context_is_not_replayed_to_resumed_executor() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        let new = TranscriptMessage::user("new");
        state.transcript = vec![old.clone(), new];
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("ext-1".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store, executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "continue".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let prompts = executor.prompts.lock().await;
        assert!(!prompts[0].prompt.contains("user: old"));
        assert!(prompts[0].prompt.contains("user: new"));
        drop(prompts);
        let prepared = executor.prepared.lock().await;
        assert_eq!(prepared[0].previous_session_id.as_deref(), Some("ext-1"));
    }

    #[tokio::test]
    async fn fresh_executor_session_gets_full_context_even_with_previous_binding() {
        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FakeExecutorBackend {
            force_started_new_session: true,
            ..FakeExecutorBackend::default()
        });
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        state.transcript.push(old.clone());
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("stale-session".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store, executor.clone());

        let mut output = CollectingRouterOutputSink::default();
        router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "recover".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap();

        let prompts = executor.prompts.lock().await;
        assert!(prompts[0].prompt.contains("Recent router transcript"));
        assert!(prompts[0].prompt.contains("user: old"));
    }

    #[tokio::test]
    async fn prompt_failure_after_replacement_session_clears_stale_cursor() {
        #[derive(Debug, Default)]
        struct FailingAfterPrepare {
            prepared: tokio::sync::Mutex<Vec<ExecutorPrepareRequest>>,
        }

        #[async_trait::async_trait]
        impl ExecutorBackend for FailingAfterPrepare {
            fn get(&self, name: &str) -> Option<crate::executor::ExecutorDescriptor> {
                (name == "kimi").then(|| crate::executor::ExecutorDescriptor {
                    name: "kimi".to_string(),
                    protocol: "fake".to_string(),
                    machine_id: crate::machine::LOCAL_MACHINE_ID.to_string(),
                })
            }

            fn list(&self) -> Vec<crate::executor::ExecutorDescriptor> {
                self.get("kimi").into_iter().collect()
            }

            async fn prepare(
                &self,
                request: ExecutorPrepareRequest,
                _cancel: TurnCancellation,
            ) -> anyhow::Result<crate::executor::PreparedExecutor> {
                self.prepared.lock().await.push(request);
                Ok(crate::executor::PreparedExecutor {
                    external_session_id: Some("replacement-session".to_string()),
                    started_new_session: true,
                    machine_id: None,
                    cwd: None,
                    machine_workspace: None,
                })
            }

            async fn prompt(
                &self,
                _request: ExecutorPromptRequest,
                _events: &mut dyn ExecutorEventSink,
                _cancel: TurnCancellation,
            ) -> ExecutorPromptOutcome {
                ExecutorPromptOutcome::Failed(anyhow::anyhow!("prompt failed"))
            }
        }

        let store = Arc::new(InMemorySessionStore::default());
        let executor = Arc::new(FailingAfterPrepare::default());
        let mut state = SessionState::new("slack:C1:T1", "kimi");
        let old = TranscriptMessage::user("old");
        state.transcript.push(old.clone());
        state.executor_bindings.insert(
            "kimi".to_string(),
            ExecutorBinding {
                protocol: "acp".to_string(),
                external_session_id: Some("stale-session".to_string()),
                seen_context: vec![message_fingerprint(&old)],
                ..ExecutorBinding::default()
            },
        );
        store.save(state).await;
        let router = AgentRouter::new("kimi", store.clone(), executor);

        let mut output = CollectingRouterOutputSink::default();
        let err = router
            .handle(
                RouterInput {
                    session_key: "slack:C1:T1".to_string(),
                    text: "recover".to_string(),
                    user_id: None,
                },
                &mut output,
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("prompt failed"));
        let saved = store.load_or_create("slack:C1:T1", "kimi").await;
        let binding = saved.executor_bindings.get("kimi").unwrap();
        assert_eq!(
            binding.external_session_id.as_deref(),
            Some("replacement-session")
        );
        assert!(binding.seen_context.is_empty());
        assert_eq!(binding.health, ExecutorHealth::Unhealthy);
    }
}
