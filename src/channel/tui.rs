use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome,
    approval::{SharedApprovalBroker, is_approval_command},
    config::{ChannelEventMode, TuiConfig},
    router::{
        ChannelRouteTicket, RouterChannelEvent, RouterOutputSink, RouterService,
        is_slash_command_input, render_live_compact_channel_events,
    },
    text::append_reply_message_break,
};

const TUI_SOURCE: &str = "tui";
const TUI_USER_ID: &str = "local";

#[derive(Debug, Clone)]
pub struct TerminalChatChannel {
    cfg: TuiConfig,
    default_executor: String,
    available_executors: Vec<String>,
    external_channels: Option<Vec<String>>,
    log_path: Option<String>,
    approvals: SharedApprovalBroker,
}

impl TerminalChatChannel {
    pub fn new(
        cfg: TuiConfig,
        default_executor: impl Into<String>,
        available_executors: Vec<String>,
        external_channels: Option<Vec<String>>,
        log_path: Option<String>,
        approvals: SharedApprovalBroker,
    ) -> Self {
        Self {
            cfg,
            default_executor: default_executor.into(),
            available_executors,
            external_channels,
            log_path,
            approvals,
        }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        let writer = SharedTerminalWriter::new(io::stdout());
        self.print_header(&writer)?;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let reader = tokio::spawn(read_terminal_input(tx));
        let session_key = tui_session_key(&self.cfg.session_id);
        let approval_notifier = spawn_terminal_approval_notifier(
            self.approvals.clone(),
            session_key.clone(),
            writer.clone(),
        );
        let mut turns = JoinSet::new();
        let mut active_prompt_turns = 0usize;
        let mut input_closed = false;
        let mut abort_turns = false;
        let mut notifications = router.subscribe_notifications();

        writer.write_str("> ")?;
        loop {
            if input_closed && turns.is_empty() {
                break;
            }
            tokio::select! {
                notification = async { notifications.as_mut().unwrap().recv().await }, if notifications.is_some() => {
                    match notification {
                        Ok(notification) if notification.session_key == session_key => {
                            writer.write_str(&format!("\n{}\n", notification.text))?;
                            write_ready_prompt(&writer, active_prompt_turns)?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => notifications = None,
                        _ => {}
                    }
                }
                event = rx.recv(), if !input_closed => {
                    match event {
                        Some(TerminalInput::Prompt(text)) => {
                            if active_prompt_turns > 0 && !can_route_during_active_prompt(&text) {
                                writer.write_str(
                                    "\nbusy> input was not sent; only /stop, /approve, and /deny are accepted while this turn runs.\n",
                                )?;
                                continue;
                            }

                            let turn_kind = TerminalTurnKind::from_text(&text);
                            let status_message = turn_status_message(&text);
                            let status = TerminalStatus::start(writer.clone(), status_message);
                            let ticket = match begin_tui_text(router.clone(), session_key.clone(), text).await {
                                Ok(Some(ticket)) => ticket,
                                Ok(None) => {
                                    drop(status);
                                    write_ready_prompt(&writer, active_prompt_turns)?;
                                    continue;
                                }
                                Err(err) => {
                                    drop(status);
                                    writer.write_str(&format!("\nerror> {err}\n"))?;
                                    write_ready_prompt(&writer, active_prompt_turns)?;
                                    continue;
                                }
                            };
                            let writer = writer.clone();
                            let channel_events = self.cfg.channel_events;
                            let router = router.clone();
                            if turn_kind.is_prompt() {
                                active_prompt_turns += 1;
                            }
                            turns.spawn(async move {
                                let mut output = TerminalOutputSink::new_with_status(
                                    writer.clone(),
                                    channel_events,
                                    status,
                                );
                                if let Err(err) = finish_tui_text(router, ticket, &mut output).await {
                                    let _ = writer.write_str(&format!("\nerror> {err}\n"));
                                }
                                turn_kind
                            });
                        }
                        Some(TerminalInput::EndOfInput) | None => {
                            input_closed = true;
                        }
                        Some(TerminalInput::Exit) => {
                            abort_turns = true;
                            break;
                        }
                        Some(TerminalInput::ReadError(err)) => {
                            anyhow::bail!("failed to read terminal input: {err}");
                        }
                    }
                }
                result = turns.join_next(), if !turns.is_empty() => {
                    match result {
                        Some(Ok(turn_kind)) => {
                            if turn_kind.is_prompt() {
                                active_prompt_turns = active_prompt_turns.saturating_sub(1);
                            }
                            write_ready_prompt(&writer, active_prompt_turns)?;
                        }
                        Some(Err(err)) if err.is_cancelled() => {}
                        Some(Err(err)) => {
                            writer.write_str(&format!("\nerror> terminal turn task failed: {err}\n"))?;
                            write_ready_prompt(&writer, active_prompt_turns)?;
                        }
                        None => {}
                    }
                }
            }
        }

        if abort_turns {
            turns.abort_all();
        }
        while turns.join_next().await.is_some() {}
        reader.abort();
        approval_notifier.abort();
        writer.write_str("\n")?;
        Ok(())
    }

    fn print_header<W>(&self, writer: &SharedTerminalWriter<W>) -> anyhow::Result<()>
    where
        W: Write + Send + 'static,
    {
        writer.write_str("Agent Router\n")?;
        writer.write_str(&format!("default: {}\n", self.default_executor))?;
        if !self.available_executors.is_empty() {
            writer.write_str(&format!(
                "agents: {}\n",
                self.available_executors.join(", ")
            ))?;
            writer.write_str("switch: /agent <name>\n")?;
        }
        if let Some(channels) = &self.external_channels {
            let channels = if channels.is_empty() {
                "none".to_string()
            } else {
                channels.join(", ")
            };
            writer.write_str(&format!("channels: {channels}\n"))?;
        }
        writer.write_str(&format!("session: {}\n", self.cfg.session_id))?;
        if let Some(log_path) = &self.log_path {
            writer.write_str(&format!("log: {log_path}\n"))?;
        }
        writer.write_str("\n")?;
        Ok(())
    }
}

pub(crate) fn tui_session_key(session_id: &str) -> String {
    format!("tui:user:local:{}", session_id.trim())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalTurnKind {
    Prompt,
    Control,
}

impl TerminalTurnKind {
    fn from_text(text: &str) -> Self {
        if is_prompt_turn_input(text) {
            Self::Prompt
        } else {
            Self::Control
        }
    }

    fn is_prompt(self) -> bool {
        matches!(self, Self::Prompt)
    }
}

fn write_ready_prompt<W>(
    writer: &SharedTerminalWriter<W>,
    active_prompt_turns: usize,
) -> anyhow::Result<()>
where
    W: Write + Send + 'static,
{
    if active_prompt_turns == 0 {
        writer.write_str("\n> ")?;
    }
    Ok(())
}

fn turn_status_message(text: &str) -> String {
    let command = text.trim().split_whitespace().next().unwrap_or("");
    if is_approval_command(text) {
        return "resolving approval".to_string();
    }
    if command == "/stop" {
        return "stopping active turn".to_string();
    }
    if is_slash_command_input(text) {
        return "handling slash command".to_string();
    }
    "waiting for output".to_string()
}

fn is_prompt_turn_input(text: &str) -> bool {
    let text = text.trim();
    let command = text.split_whitespace().next().unwrap_or("");
    !is_approval_command(text)
        && !matches!(command, "/stop" | "/agent" | "/yolo")
        && !is_slash_command_input(text)
}

fn can_route_during_active_prompt(text: &str) -> bool {
    let command = text.trim().split_whitespace().next().unwrap_or("");
    command == "/stop" || is_approval_command(text)
}

#[cfg(test)]
pub(crate) async fn route_tui_text(
    router: Arc<dyn RouterService>,
    session_key: String,
    text: String,
    output: &mut dyn RouterOutputSink,
) -> anyhow::Result<()> {
    let Some(ticket) = begin_tui_text(router.clone(), session_key, text).await? else {
        return Ok(());
    };
    finish_tui_text(router, ticket, output).await
}

async fn begin_tui_text(
    router: Arc<dyn RouterService>,
    session_key: String,
    text: String,
) -> anyhow::Result<Option<ChannelRouteTicket>> {
    let outcome = router
        .begin_channel_input(ChannelInput {
            session_key,
            text,
            user_id: Some(TUI_USER_ID.to_string()),
            source: TUI_SOURCE.to_string(),
            intent: ChannelInputIntent::Route,
            context_policy: ChannelContextPolicy::disabled(TUI_SOURCE),
        })
        .await?;
    let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
        return Ok(None);
    };
    Ok(Some(ticket))
}

async fn finish_tui_text(
    router: Arc<dyn RouterService>,
    ticket: ChannelRouteTicket,
    output: &mut dyn RouterOutputSink,
) -> anyhow::Result<()> {
    router.finish_channel_input(ticket, None, output).await
}

async fn read_terminal_input(tx: mpsc::UnboundedSender<TerminalInput>) {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                if let Some(input) = parse_terminal_input(&line) {
                    let should_exit = input == TerminalInput::Exit;
                    if tx.send(input).is_err() || should_exit {
                        break;
                    }
                }
            }
            Ok(None) => {
                let _ = tx.send(TerminalInput::EndOfInput);
                break;
            }
            Err(err) => {
                let _ = tx.send(TerminalInput::ReadError(err.to_string()));
                break;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TerminalInput {
    Prompt(String),
    Exit,
    EndOfInput,
    ReadError(String),
}

fn parse_terminal_input(line: &str) -> Option<TerminalInput> {
    let text = line.trim_end_matches(['\r', '\n']);
    if text.trim().is_empty() {
        return None;
    }
    match text.trim() {
        "/exit" | "/quit" => Some(TerminalInput::Exit),
        _ => Some(TerminalInput::Prompt(text.to_string())),
    }
}

#[derive(Debug)]
pub(crate) struct TerminalOutputSink<W>
where
    W: Write + Send + 'static,
{
    writer: SharedTerminalWriter<W>,
    channel_events: ChannelEventMode,
    status: Option<TerminalStatus<W>>,
    activity_events: Vec<RouterChannelEvent>,
    last_activity: Option<String>,
    streamed_text: String,
    reply_started: bool,
}

impl<W> TerminalOutputSink<W>
where
    W: Write + Send + 'static,
{
    #[cfg(test)]
    pub(crate) fn new(writer: SharedTerminalWriter<W>, channel_events: ChannelEventMode) -> Self {
        Self {
            writer,
            channel_events,
            status: None,
            activity_events: Vec::new(),
            last_activity: None,
            streamed_text: String::new(),
            reply_started: false,
        }
    }

    fn new_with_status(
        writer: SharedTerminalWriter<W>,
        channel_events: ChannelEventMode,
        status: TerminalStatus<W>,
    ) -> Self {
        Self {
            writer,
            channel_events,
            status: Some(status),
            activity_events: Vec::new(),
            last_activity: None,
            streamed_text: String::new(),
            reply_started: false,
        }
    }

    fn stop_status(&mut self) {
        if let Some(mut status) = self.status.take() {
            status.stop();
        }
    }

    fn write_reply_prefix(&mut self) -> anyhow::Result<()> {
        if !self.reply_started {
            self.stop_status();
            self.writer.write_str("\nassistant> ")?;
            self.reply_started = true;
        }
        Ok(())
    }

    fn write_activity(&self, text: &str) -> anyhow::Result<()> {
        self.writer.write_str(&format!("\nactivity>\n{text}\n"))
    }

    fn write_or_log(&self, text: &str) {
        if let Err(err) = self.writer.write_str(text) {
            tracing::warn!(error = %err, "failed to write terminal output");
        }
    }
}

#[async_trait]
impl<W> RouterOutputSink for TerminalOutputSink<W>
where
    W: Write + Send + 'static,
{
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        match self.channel_events {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => {
                self.activity_events.push(event);
                let Some(summary) = render_live_compact_channel_events(&self.activity_events)
                else {
                    return;
                };
                if self.last_activity.as_deref() == Some(summary.as_str()) {
                    return;
                }
                self.stop_status();
                self.last_activity = Some(summary.clone());
                if let Err(err) = self.write_activity(&summary) {
                    tracing::warn!(error = %err, "failed to write terminal activity summary");
                }
            }
            ChannelEventMode::Verbose => {
                self.stop_status();
                if let Err(err) = self.write_activity(&event.render_text()) {
                    tracing::warn!(error = %err, "failed to write terminal activity event");
                }
            }
        }
    }

    fn send_reply_chunk(&mut self, chunk: String) {
        if chunk.is_empty() {
            return;
        }
        if let Err(err) = self.write_reply_prefix() {
            tracing::warn!(error = %err, "failed to write terminal reply prefix");
            return;
        }
        self.streamed_text.push_str(&chunk);
        self.write_or_log(&chunk);
    }

    fn send_reply_break(&mut self) {
        let previous_len = self.streamed_text.len();
        append_reply_message_break(&mut self.streamed_text);
        let added = self
            .streamed_text
            .get(previous_len..)
            .unwrap_or("")
            .to_string();
        if !added.is_empty() {
            self.write_or_log(&added);
        }
    }

    async fn discard_reply_stream(&mut self) {
        self.stop_status();
        self.streamed_text.clear();
        self.reply_started = false;
        self.write_or_log("\n");
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        self.write_reply_prefix()?;
        if self.streamed_text.is_empty() {
            self.writer.write_str(&text)?;
        } else if text == self.streamed_text {
            // The streamed chunks already painted the final answer.
        } else if let Some(rest) = text.strip_prefix(&self.streamed_text) {
            self.writer.write_str(rest)?;
            self.streamed_text.push_str(rest);
        } else {
            self.writer.write_str("\n")?;
            self.writer.write_str(&text)?;
            self.streamed_text = text;
        }
        self.writer.write_str("\n")?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct SharedTerminalWriter<W>
where
    W: Write + Send + 'static,
{
    inner: Arc<Mutex<W>>,
}

impl<W> Clone for SharedTerminalWriter<W>
where
    W: Write + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<W> SharedTerminalWriter<W>
where
    W: Write + Send + 'static,
{
    pub(crate) fn new(writer: W) -> Self {
        Self {
            inner: Arc::new(Mutex::new(writer)),
        }
    }

    fn write_str(&self, text: &str) -> anyhow::Result<()> {
        let mut writer = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("terminal writer lock poisoned"))?;
        writer.write_all(text.as_bytes())?;
        writer.flush()?;
        Ok(())
    }
}

#[derive(Debug)]
struct TerminalStatus<W>
where
    W: Write + Send + 'static,
{
    _writer: SharedTerminalWriter<W>,
}

impl<W> TerminalStatus<W>
where
    W: Write + Send + 'static,
{
    fn start(writer: SharedTerminalWriter<W>, message: String) -> Self {
        if let Err(err) = writer.write_str(&format!("working> {message}\n")) {
            tracing::warn!(error = %err, "failed to write terminal status");
        }
        Self { _writer: writer }
    }

    fn stop(&mut self) {}
}

impl<W> Drop for TerminalStatus<W>
where
    W: Write + Send + 'static,
{
    fn drop(&mut self) {
        self.stop();
    }
}

fn spawn_terminal_approval_notifier<W>(
    approvals: SharedApprovalBroker,
    session_key: String,
    writer: SharedTerminalWriter<W>,
) -> tokio::task::JoinHandle<()>
where
    W: Write + Send + 'static,
{
    let mut prompts = approvals.subscribe();
    tokio::spawn(async move {
        loop {
            let prompt = match prompts.recv().await {
                Ok(prompt) => prompt,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };
            if prompt.session_key != session_key {
                continue;
            }
            if !approvals.has_pending(&prompt.id).await {
                continue;
            }
            let text = prompt.render_text();
            let result = tokio::select! {
                biased;
                _ = prompt.cancelled() => continue,
                result = async { writer.write_str(&format!("\napproval>\n{text}\napproval-input> ")) } => result,
            };
            if let Err(err) = result {
                tracing::warn!(error = %err, "failed to write terminal approval prompt");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, io::Cursor, time::Duration};

    use crate::{
        RouterInput,
        approval::{
            ApprovalBroker, ApprovalCancellation, ApprovalOption, ApprovalRequest,
            ApprovalResolverPolicy, ApprovalScope,
        },
        router::{ChannelRouteTicket, RouterChannelEventKind, TurnBeginMode, TurnReservation},
    };
    use tokio::sync::Mutex as TokioMutex;

    use super::*;

    #[test]
    fn builds_stable_tui_session_key() {
        assert_eq!(tui_session_key(" local "), "tui:user:local:local");
    }

    #[test]
    fn parses_local_exit_without_routing() {
        assert_eq!(parse_terminal_input("/exit"), Some(TerminalInput::Exit));
        assert_eq!(parse_terminal_input("/quit"), Some(TerminalInput::Exit));
        assert_eq!(
            parse_terminal_input("/stop"),
            Some(TerminalInput::Prompt("/stop".to_string()))
        );
        assert_eq!(parse_terminal_input("   "), None);
    }

    #[test]
    fn header_prints_available_agents_and_switch_hint() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let channel = TerminalChatChannel::new(
            TuiConfig {
                enabled: true,
                session_id: "local".to_string(),
                channel_events: ChannelEventMode::Compact,
            },
            "codex",
            vec!["codex".to_string(), "claude".to_string()],
            Some(vec!["slack".to_string(), "web".to_string()]),
            Some("/tmp/agent-router.log".to_string()),
            Arc::new(ApprovalBroker::default()),
        );

        channel.print_header(&writer).unwrap();

        let text = writer_text(&handle);
        assert!(text.contains("default: codex\n"));
        assert!(text.contains("agents: codex, claude\n"));
        assert!(text.contains("switch: /agent <name>\n"));
        assert!(text.contains("channels: slack, web\n"));
    }

    #[test]
    fn header_prints_no_external_channels_when_requested() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let channel = TerminalChatChannel::new(
            TuiConfig {
                enabled: true,
                session_id: "local".to_string(),
                channel_events: ChannelEventMode::Compact,
            },
            "codex",
            vec!["codex".to_string()],
            Some(Vec::new()),
            None,
            Arc::new(ApprovalBroker::default()),
        );

        channel.print_header(&writer).unwrap();

        assert!(writer_text(&handle).contains("channels: none\n"));
    }

    #[test]
    fn turn_status_message_describes_local_state() {
        assert_eq!(turn_status_message("/stop"), "stopping active turn");
        assert_eq!(turn_status_message("/approve 1"), "resolving approval");
        assert_eq!(
            turn_status_message("/agent status"),
            "handling slash command"
        );
        assert_eq!(turn_status_message("new prompt"), "waiting for output");
    }

    #[test]
    fn prompt_turn_tracking_matches_router_intake_shape() {
        assert!(is_prompt_turn_input("normal prompt"));
        assert!(!is_prompt_turn_input("/stop"));
        assert!(!is_prompt_turn_input("/agent codex"));
        assert!(!is_prompt_turn_input("/yolo on"));
        assert!(!is_prompt_turn_input("/approve 1"));
        assert!(!is_prompt_turn_input("//agent-owned-command"));
    }

    #[test]
    fn running_prompt_policy_only_allows_controls() {
        assert!(!can_route_during_active_prompt("normal prompt"));
        assert!(can_route_during_active_prompt("/stop"));
        assert!(can_route_during_active_prompt("/approve 1"));
        assert!(can_route_during_active_prompt("/deny 1"));
        assert!(!can_route_during_active_prompt("/agent status"));
        assert!(!can_route_during_active_prompt("/yolo on"));
        assert!(!can_route_during_active_prompt("//agent-owned-command"));
    }

    #[tokio::test]
    async fn terminal_approval_notifier_prints_pending_prompt_for_session() {
        let approvals = Arc::new(ApprovalBroker::new(Duration::from_secs(30)));
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let notifier = spawn_terminal_approval_notifier(
            approvals.clone(),
            "tui:user:local:s1".to_string(),
            writer,
        );
        let cancel = ApprovalCancellation::new();
        let request_task = tokio::spawn({
            let approvals = approvals.clone();
            let cancel = cancel.clone();
            async move {
                approvals
                    .request_resolution_until_cancelled(
                        approval_request("tui:user:local:s1"),
                        cancel,
                    )
                    .await
            }
        });

        wait_for_writer_contains(&handle, "Approval required: Run command").await;

        cancel.cancel();
        let _ = request_task.await.unwrap();
        notifier.abort();
        let text = writer_text(&handle);
        assert!(text.contains("Approve: /approve"));
        assert!(text.contains("Deny: /deny"));
    }

    #[tokio::test]
    async fn route_tui_text_sends_expected_channel_input() {
        let router = Arc::new(RecordingBeginRouter::default());
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        route_tui_text(
            router.clone(),
            "tui:user:local:s1".to_string(),
            "hello".to_string(),
            &mut output,
        )
        .await
        .unwrap();

        let inputs = router.inputs.lock().await;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].session_key, "tui:user:local:s1");
        assert_eq!(inputs[0].text, "hello");
        assert_eq!(inputs[0].user_id.as_deref(), Some("local"));
        assert_eq!(inputs[0].source, "tui");
        assert_eq!(inputs[0].intent, ChannelInputIntent::Route);
        assert_eq!(
            inputs[0].context_policy,
            ChannelContextPolicy::disabled("tui")
        );
        assert_eq!(*router.finished.lock().await, 1);
    }

    #[tokio::test]
    async fn normal_prompt_uses_router_replacement_path() {
        let router = Arc::new(ReservingRouter::default());
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        route_tui_text(
            router.clone(),
            "tui:user:local:s1".to_string(),
            "run long task".to_string(),
            &mut output,
        )
        .await
        .unwrap();

        assert_eq!(
            *router.reservations.lock().await,
            vec![(
                "tui:user:local:s1".to_string(),
                TurnBeginMode::ReplaceActive
            )]
        );
    }

    #[tokio::test]
    async fn router_stop_command_is_forwarded_without_replacement_reservation() {
        let router = Arc::new(ReservingRouter::default());
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        route_tui_text(
            router.clone(),
            "tui:user:local:s1".to_string(),
            "/stop".to_string(),
            &mut output,
        )
        .await
        .unwrap();

        assert!(router.reservations.lock().await.is_empty());
        assert_eq!(
            router
                .handled
                .lock()
                .await
                .front()
                .map(|input| input.text.as_str()),
            Some("/stop")
        );
    }

    #[tokio::test]
    async fn terminal_output_does_not_duplicate_streamed_final_reply() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        output.send_reply_chunk("hel".to_string());
        output.send_reply_chunk("lo".to_string());
        output.send_final_reply("hello".to_string()).await.unwrap();

        assert_eq!(writer_text(&handle), "\nassistant> hello\n");
    }

    #[tokio::test]
    async fn terminal_output_prints_final_reply_without_chunks() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        output.send_final_reply("done".to_string()).await.unwrap();

        assert_eq!(writer_text(&handle), "\nassistant> done\n");
    }

    #[tokio::test]
    async fn terminal_status_is_stable_text_before_final_reply() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let status = TerminalStatus::start(writer.clone(), "sent to kimi".to_string());
        let mut output = TerminalOutputSink::new_with_status(writer, ChannelEventMode::Off, status);

        output.send_final_reply("done".to_string()).await.unwrap();

        assert_eq!(
            writer_text(&handle),
            "working> sent to kimi\n\nassistant> done\n"
        );
    }

    #[test]
    fn terminal_output_renders_verbose_activity() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Verbose);

        output.send_channel_event(tool_event("$ cargo test\nexit: 0\nstatus: completed"));

        assert_eq!(
            writer_text(&handle),
            "\nactivity>\n[codex] Tool call: Bash\n$ cargo test\nexit: 0\nstatus: completed\n"
        );
    }

    #[test]
    fn terminal_output_honors_off_activity_mode() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Off);

        output.send_channel_event(tool_event("$ cargo test\nexit: 0\nstatus: completed"));

        assert_eq!(writer_text(&handle), "");
    }

    #[test]
    fn terminal_output_renders_compact_activity_once_per_change() {
        let writer = SharedTerminalWriter::new(Cursor::new(Vec::new()));
        let handle = writer.inner.clone();
        let mut output = TerminalOutputSink::new(writer, ChannelEventMode::Compact);

        let event = reasoning_event("checking state");
        output.send_channel_event(event.clone());
        output.send_channel_event(event);

        assert_eq!(
            writer_text(&handle),
            "\nactivity>\n[codex] Activity\nReasoning: checking state\n"
        );
    }

    fn writer_text(handle: &Arc<Mutex<Cursor<Vec<u8>>>>) -> String {
        let writer = handle.lock().unwrap();
        String::from_utf8(writer.get_ref().clone()).unwrap()
    }

    fn tool_event(text: &str) -> RouterChannelEvent {
        RouterChannelEvent {
            kind: RouterChannelEventKind::ToolCall,
            executor: "codex".to_string(),
            title: "Bash".to_string(),
            text: text.to_string(),
        }
    }

    fn reasoning_event(text: &str) -> RouterChannelEvent {
        RouterChannelEvent {
            kind: RouterChannelEventKind::ReasoningSummary,
            executor: "codex".to_string(),
            title: "Reasoning".to_string(),
            text: text.to_string(),
        }
    }

    async fn wait_for_writer_contains(handle: &Arc<Mutex<Cursor<Vec<u8>>>>, needle: &str) {
        for _ in 0..100 {
            if writer_text(handle).contains(needle) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "writer never contained {needle:?}; output: {}",
            writer_text(handle)
        );
    }

    fn approval_request(session_key: &str) -> ApprovalRequest {
        ApprovalRequest {
            session_key: session_key.to_string(),
            executor: "codex".to_string(),
            requester_user_id: Some("local".to_string()),
            resolver_policy: ApprovalResolverPolicy::Requester,
            scope: ApprovalScope::ToolPermission,
            title: "Run command".to_string(),
            body: "cargo test".to_string(),
            options: vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_once".to_string(),
                    name: "Allow once".to_string(),
                    auto_approvable: false,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ],
        }
    }

    #[derive(Default)]
    struct RecordingBeginRouter {
        inputs: TokioMutex<Vec<ChannelInput>>,
        finished: TokioMutex<usize>,
    }

    #[async_trait]
    impl RouterService for RecordingBeginRouter {
        async fn begin_channel_input(
            &self,
            input: ChannelInput,
        ) -> anyhow::Result<ChannelIntakeOutcome> {
            self.inputs.lock().await.push(input.clone());
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
            output.send_final_reply("ok".to_string()).await
        }

        async fn handle(
            &self,
            _input: RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct ReservingRouter {
        reservations: TokioMutex<Vec<(String, TurnBeginMode)>>,
        handled: TokioMutex<VecDeque<RouterInput>>,
    }

    #[async_trait]
    impl RouterService for ReservingRouter {
        async fn reserve_turn(
            &self,
            session_key: &str,
            mode: TurnBeginMode,
        ) -> anyhow::Result<Option<TurnReservation>> {
            self.reservations
                .lock()
                .await
                .push((session_key.to_string(), mode));
            Ok(None)
        }

        async fn handle(
            &self,
            input: RouterInput,
            output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            self.handled.lock().await.push_back(input);
            output.send_final_reply("ok".to_string()).await
        }

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            Ok(())
        }
    }
}
