use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    approval::{SharedApprovalBroker, is_approval_command},
    channel::EventDeduper,
    config::{ChannelEventMode, SlackConfig},
    router::{
        RouterChannelEvent, RouterInput, RouterOutputSink, RouterService,
        render_live_compact_channel_events,
    },
    session::context::{
        ContextArtifactInput, ContextFileContent, ContextFileInput, ContextSyncIssueInput,
        ContextSyncRequest, sanitize_path_segment,
    },
};

#[derive(Debug, Clone)]
pub struct SlackSocketModeChannel {
    cfg: SlackConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    seen_events: Arc<Mutex<EventDeduper>>,
}

impl SlackSocketModeChannel {
    pub fn new(cfg: SlackConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            seen_events: Arc::new(Mutex::new(EventDeduper::new(512))),
        }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        self.validate_tokens()?;
        let bot_user_id = self.auth_test().await?;
        let url = self.open_socket_url().await?;
        tracing::info!("connecting Slack Socket Mode");
        let (stream, _) = connect_async(url).await?;
        tracing::info!(bot_user_id = %bot_user_id, "connected Slack Socket Mode");
        let (mut sink, mut stream) = stream.split();
        let channel = Arc::new(self);
        channel.clone().spawn_approval_notifier();

        while let Some(frame) = stream.next().await {
            let frame = frame?;
            match frame {
                Message::Text(text) => {
                    let envelope: SlackEnvelope = serde_json::from_str(&text)?;
                    if envelope.kind == "hello" {
                        continue;
                    }
                    if let Some(envelope_id) = &envelope.envelope_id {
                        sink.send(Message::Text(
                            json!({"envelope_id": envelope_id}).to_string().into(),
                        ))
                        .await?;
                    }
                    let channel_ref = channel.clone();
                    let router_ref = router.clone();
                    let bot_user_id = bot_user_id.clone();
                    tokio::spawn(async move {
                        if let Err(err) = channel_ref
                            .handle_envelope(envelope, router_ref, &bot_user_id)
                            .await
                        {
                            tracing::warn!(error = %err, "failed to handle Slack envelope");
                        }
                    });
                }
                Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
                Message::Close(close) => {
                    tracing::warn!(?close, "Slack Socket Mode closed");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn spawn_approval_notifier(self: Arc<Self>) {
        let mut prompts = self.approvals.subscribe();
        let prompt_channel = self.clone();
        tokio::spawn(async move {
            loop {
                let prompt = match prompts.recv().await {
                    Ok(prompt) => prompt,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                let Some(target) = SlackReplyTarget::from_session_key(&prompt.session_key) else {
                    continue;
                };
                if let Err(err) = prompt_channel
                    .post_message(&target, &prompt.render_text())
                    .await
                {
                    tracing::warn!(error = %err, "failed to post Slack approval prompt");
                }
            }
        });

        let mut auto_selections = self.approvals.subscribe_auto_selections();
        tokio::spawn(async move {
            loop {
                let notice = match auto_selections.recv().await {
                    Ok(notice) => notice,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                let Some(target) = SlackReplyTarget::from_session_key(&notice.session_key) else {
                    continue;
                };
                if let Err(err) = self.post_message(&target, &notice.render_text()).await {
                    tracing::warn!(error = %err, "failed to post Slack auto-approval notice");
                }
            }
        });
    }

    async fn handle_envelope(
        &self,
        envelope: SlackEnvelope,
        router: Arc<dyn RouterService>,
        bot_user_id: &str,
    ) -> anyhow::Result<()> {
        match envelope.kind.as_str() {
            "events_api" => {
                if let Some(event) = parse_message_event(&envelope.payload, bot_user_id) {
                    self.handle_message_event(event, router, bot_user_id)
                        .await?;
                }
            }
            "slash_commands" => {
                if let Some(command) = parse_slash_command(&envelope.payload) {
                    self.handle_slash_command(command, router).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_message_event(
        &self,
        event: SlackMessageEvent,
        router: Arc<dyn RouterService>,
        bot_user_id: &str,
    ) -> anyhow::Result<()> {
        if !self.should_accept_channel(&event.channel) {
            return Ok(());
        }
        if !self
            .seen_events
            .lock()
            .await
            .insert(event.event_key.clone())
        {
            return Ok(());
        }
        let is_dm = event.channel.starts_with('D');
        let mentioned = event.text.contains(&format!("<@{bot_user_id}>"));
        let text = strip_bot_mention(&event.text, bot_user_id);
        if text.is_empty() {
            return Ok(());
        }
        let session_key = event.session_key();
        let approval_command = is_approval_command(&text);
        let approval_trigger =
            approval_command && self.approvals.has_pending_for_session(&session_key).await;
        let should_route = is_dm
            || !self.cfg.require_mention
            || mentioned
            || approval_trigger
            || self.cfg.free_response_channels.contains(&event.channel);
        if !should_route {
            if event.is_thread_reply() {
                tracing::info!(
                    channel = %event.channel,
                    user_id = %event.user,
                    session_key = %session_key,
                    text_len = text.len(),
                    "observing Slack thread message"
                );
                router
                    .observe(RouterInput {
                        session_key,
                        text,
                        user_id: Some(event.user),
                    })
                    .await?;
            }
            return Ok(());
        }
        tracing::info!(
            channel = %event.channel,
            user_id = %event.user,
            session_key = %session_key,
            text_len = text.len(),
            "routing Slack message"
        );
        if self.should_sync_context(&text) {
            let request = self
                .current_thread_context_request(&event, &session_key)
                .await;
            router.sync_context(request).await?;
        }
        let reply_target = event.reply_target();
        let mut output = SlackRouterOutputSink {
            channel: self.clone(),
            target: reply_target,
            channel_events: self.cfg.channel_events,
            compact_activity: SlackCompactActivity::default(),
        };
        router
            .handle(
                RouterInput {
                    session_key,
                    text,
                    user_id: Some(event.user),
                },
                &mut output,
            )
            .await?;
        Ok(())
    }

    async fn handle_slash_command(
        &self,
        command: SlackSlashCommand,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        if !self.should_accept_channel(&command.channel_id) {
            return Ok(());
        }
        let text = format!("{} {}", command.command, command.text)
            .trim()
            .to_string();
        let reply_target = SlackReplyTarget {
            channel: command.channel_id.clone(),
            thread_ts: None,
        };
        let session_key = format!("slack:{}:slash:{}", command.channel_id, command.user_id);
        tracing::info!(
            channel = %command.channel_id,
            user_id = %command.user_id,
            session_key = %session_key,
            text_len = text.len(),
            "routing Slack slash command"
        );
        let mut output = SlackRouterOutputSink {
            channel: self.clone(),
            target: reply_target,
            channel_events: self.cfg.channel_events,
            compact_activity: SlackCompactActivity::default(),
        };
        router
            .handle(
                RouterInput {
                    session_key,
                    text,
                    user_id: Some(command.user_id),
                },
                &mut output,
            )
            .await?;
        Ok(())
    }

    async fn open_socket_url(&self) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            url: Option<String>,
            error: Option<String>,
        }

        let resp = self
            .http
            .post("https://slack.com/api/apps.connections.open")
            .bearer_auth(&self.cfg.app_token)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if !resp.ok {
            anyhow::bail!(
                "Slack apps.connections.open failed: {}",
                resp.error.unwrap_or_else(|| "unknown_error".to_string())
            );
        }
        resp.url
            .ok_or_else(|| anyhow::anyhow!("Slack apps.connections.open response omitted url"))
    }

    async fn auth_test(&self) -> anyhow::Result<String> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            user_id: Option<String>,
            error: Option<String>,
        }

        let resp = self
            .http
            .post("https://slack.com/api/auth.test")
            .bearer_auth(&self.cfg.bot_token)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if !resp.ok {
            anyhow::bail!(
                "Slack auth.test failed: {}",
                resp.error.unwrap_or_else(|| "unknown_error".to_string())
            );
        }
        resp.user_id
            .ok_or_else(|| anyhow::anyhow!("Slack auth.test response omitted user_id"))
    }

    async fn post_message(&self, target: &SlackReplyTarget, text: &str) -> anyhow::Result<()> {
        self.post_message_with_ts(target, text).await.map(|_| ())
    }

    async fn post_message_with_ts(
        &self,
        target: &SlackReplyTarget,
        text: &str,
    ) -> anyhow::Result<Option<String>> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            error: Option<String>,
            ts: Option<String>,
        }

        tracing::info!(
            channel = %target.channel,
            thread_ts = ?target.thread_ts,
            text_len = text.len(),
            "sending Slack message"
        );
        let mut body = json!({
            "channel": target.channel,
            "text": text,
        });
        if let Some(thread_ts) = &target.thread_ts {
            body["thread_ts"] = Value::String(thread_ts.clone());
        }
        let resp = self
            .http
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.cfg.bot_token)
            .json(&body)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if !resp.ok {
            anyhow::bail!(
                "Slack chat.postMessage failed: {}",
                resp.error.unwrap_or_else(|| "unknown_error".to_string())
            );
        }
        tracing::info!(
            channel = %target.channel,
            thread_ts = ?target.thread_ts,
            text_len = text.len(),
            "sent Slack message"
        );
        Ok(resp.ts)
    }

    async fn update_message(
        &self,
        target: &SlackReplyTarget,
        ts: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            error: Option<String>,
        }

        tracing::info!(
            channel = %target.channel,
            ts = %ts,
            text_len = text.len(),
            "updating Slack message"
        );
        let body = json!({
            "channel": target.channel,
            "ts": ts,
            "text": text,
        });
        let resp = self
            .http
            .post("https://slack.com/api/chat.update")
            .bearer_auth(&self.cfg.bot_token)
            .json(&body)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if !resp.ok {
            anyhow::bail!(
                "Slack chat.update failed: {}",
                resp.error.unwrap_or_else(|| "unknown_error".to_string())
            );
        }
        tracing::info!(
            channel = %target.channel,
            ts = %ts,
            text_len = text.len(),
            "updated Slack message"
        );
        Ok(())
    }

    fn validate_tokens(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cfg.bot_token.is_empty(),
            "SLACK_BOT_TOKEN is required"
        );
        anyhow::ensure!(
            !self.cfg.app_token.is_empty(),
            "SLACK_APP_TOKEN is required"
        );
        Ok(())
    }

    fn should_accept_channel(&self, channel: &str) -> bool {
        self.cfg.allowed_channels.is_empty() || self.cfg.allowed_channels.contains(channel)
    }

    fn should_sync_context(&self, text: &str) -> bool {
        self.cfg.context_sync.enabled
            && !self.cfg.bot_token.is_empty()
            && !is_approval_command(text)
    }

    async fn current_thread_context_request(
        &self,
        event: &SlackMessageEvent,
        session_key: &str,
    ) -> ContextSyncRequest {
        let thread_ts = event.thread_root_ts();
        let mut unresolved = Vec::new();
        let messages = match self.fetch_thread_messages(&event.channel, &thread_ts).await {
            Ok(messages) => messages,
            Err(err) => {
                unresolved.push(ContextSyncIssueInput {
                    kind: "current_thread".to_string(),
                    reference: format!("{}:{}", event.channel, thread_ts),
                    reason: err.to_string(),
                });
                Vec::new()
            }
        };

        let mut downloaded_files = Vec::new();
        for file in collect_slack_file_refs(&messages) {
            match self.download_slack_file(file.clone()).await {
                Ok(downloaded) => downloaded_files.push(downloaded),
                Err(err) => unresolved.push(ContextSyncIssueInput {
                    kind: "file".to_string(),
                    reference: file.id,
                    reason: err.to_string(),
                }),
            }
        }

        build_slack_context_request(
            session_key,
            &event.channel,
            &thread_ts,
            &messages,
            &downloaded_files,
            unresolved,
        )
    }

    async fn fetch_thread_messages(
        &self,
        channel: &str,
        thread_ts: &str,
    ) -> anyhow::Result<Vec<SlackThreadMessage>> {
        let mut cursor = None;
        let mut messages = Vec::new();
        loop {
            let mut query = vec![
                ("channel", channel.to_string()),
                ("ts", thread_ts.to_string()),
                ("limit", "15".to_string()),
            ];
            if let Some(cursor) = cursor.clone() {
                query.push(("cursor", cursor));
            }
            let response = self
                .http
                .get("https://slack.com/api/conversations.replies")
                .bearer_auth(&self.cfg.bot_token)
                .query(&query)
                .send()
                .await?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                anyhow::bail!("rate_limited");
            }
            let response = response.json::<SlackThreadResponse>().await?;
            if !response.ok {
                anyhow::bail!(
                    "Slack conversations.replies failed: {}",
                    response
                        .error
                        .unwrap_or_else(|| "unknown_error".to_string())
                );
            }
            for raw in response.messages.unwrap_or_default() {
                if let Some(message) = parse_slack_thread_message(raw) {
                    messages.push(message);
                }
            }
            cursor = response
                .response_metadata
                .and_then(|metadata| metadata.next_cursor)
                .filter(|cursor| !cursor.trim().is_empty());
            if cursor.is_none() {
                break;
            }
        }
        Ok(messages)
    }

    async fn download_slack_file(&self, file: SlackFileRef) -> anyhow::Result<DownloadedSlackFile> {
        if file.size_bytes > self.cfg.context_sync.max_file_bytes {
            anyhow::bail!(
                "file exceeds max_file_bytes ({} > {})",
                file.size_bytes,
                self.cfg.context_sync.max_file_bytes
            );
        }
        let url = file
            .url_private_download
            .as_deref()
            .or(file.url_private.as_deref())
            .ok_or_else(|| anyhow::anyhow!("file has no private download URL"))?;
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.cfg.bot_token)
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("Slack file download failed: {}", response.status());
        }
        let bytes = response.bytes().await?.to_vec();
        let extracted_text = if is_text_slack_file(&file) {
            std::str::from_utf8(&bytes).ok().map(ToOwned::to_owned)
        } else {
            None
        };
        Ok(DownloadedSlackFile {
            file,
            bytes,
            extracted_text,
        })
    }
}

struct SlackRouterOutputSink {
    channel: SlackSocketModeChannel,
    target: SlackReplyTarget,
    channel_events: ChannelEventMode,
    compact_activity: SlackCompactActivity,
}

#[derive(Default)]
struct SlackCompactActivity {
    events: Vec<RouterChannelEvent>,
    updates: Option<mpsc::UnboundedSender<String>>,
}

#[async_trait::async_trait]
impl RouterOutputSink for SlackRouterOutputSink {
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        match self.channel_events {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => {
                self.compact_activity.events.push(event);
                let Some(summary) =
                    render_live_compact_channel_events(&self.compact_activity.events)
                else {
                    return;
                };
                if self.compact_activity.updates.is_none() {
                    self.compact_activity.updates = Some(spawn_slack_compact_activity_updater(
                        self.channel.clone(),
                        self.target.clone(),
                    ));
                }
                if let Some(updates) = &self.compact_activity.updates
                    && updates.send(summary).is_err()
                {
                    tracing::warn!(
                        "compact Slack activity updater stopped before receiving update"
                    );
                }
            }
            ChannelEventMode::Verbose => {
                let channel = self.channel.clone();
                let target = self.target.clone();
                tokio::spawn(async move {
                    if let Err(err) = channel.post_message(&target, &event.render_text()).await {
                        tracing::warn!(error = %err, "failed to post Slack channel event");
                    }
                });
            }
        }
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        self.channel.post_message(&self.target, &text).await
    }
}

fn spawn_slack_compact_activity_updater(
    channel: SlackSocketModeChannel,
    target: SlackReplyTarget,
) -> mpsc::UnboundedSender<String> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut message_ts = None;
        while let Some(mut summary) = rx.recv().await {
            while let Ok(next) = rx.try_recv() {
                summary = next;
            }
            if let Some(ts) = message_ts.as_deref() {
                if let Err(err) = channel.update_message(&target, ts, &summary).await {
                    tracing::warn!(error = %err, "failed to update compact Slack activity message");
                }
            } else {
                match channel.post_message_with_ts(&target, &summary).await {
                    Ok(Some(ts)) => message_ts = Some(ts),
                    Ok(None) => {
                        tracing::warn!("compact Slack activity message response omitted ts");
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to post compact Slack activity message");
                    }
                }
            }
        }
    });
    tx
}

#[derive(Debug, Deserialize)]
struct SlackEnvelope {
    #[serde(rename = "type")]
    kind: String,
    envelope_id: Option<String>,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, Clone)]
struct SlackMessageEvent {
    event_key: String,
    channel: String,
    user: String,
    text: String,
    ts: String,
    thread_ts: Option<String>,
}

impl SlackMessageEvent {
    fn thread_root_ts(&self) -> String {
        self.thread_ts.clone().unwrap_or_else(|| self.ts.clone())
    }

    fn session_key(&self) -> String {
        let thread_root = self.thread_ts.as_deref().unwrap_or(&self.ts);
        if self.channel.starts_with('D') {
            format!("slack:dm:{}:{}", self.channel, thread_root)
        } else {
            format!("slack:channel:{}:{}", self.channel, thread_root)
        }
    }

    fn reply_target(&self) -> SlackReplyTarget {
        SlackReplyTarget {
            channel: self.channel.clone(),
            thread_ts: Some(self.thread_ts.clone().unwrap_or_else(|| self.ts.clone())),
        }
    }

    fn is_thread_reply(&self) -> bool {
        self.thread_ts
            .as_deref()
            .is_some_and(|thread_ts| thread_ts != self.ts)
    }
}

#[derive(Debug, Deserialize)]
struct SlackThreadResponse {
    ok: bool,
    messages: Option<Vec<Value>>,
    error: Option<String>,
    response_metadata: Option<SlackResponseMetadata>,
}

#[derive(Debug, Deserialize)]
struct SlackResponseMetadata {
    next_cursor: Option<String>,
}

#[derive(Debug, Clone)]
struct SlackThreadMessage {
    ts: String,
    user: Option<String>,
    bot_id: Option<String>,
    text: String,
    files: Vec<SlackFileRef>,
}

#[derive(Debug, Clone)]
struct SlackFileRef {
    id: String,
    name: String,
    mimetype: Option<String>,
    size_bytes: usize,
    url_private: Option<String>,
    url_private_download: Option<String>,
}

#[derive(Debug, Clone)]
struct DownloadedSlackFile {
    file: SlackFileRef,
    bytes: Vec<u8>,
    extracted_text: Option<String>,
}

#[derive(Debug, Clone)]
struct SlackSlashCommand {
    command: String,
    text: String,
    channel_id: String,
    user_id: String,
}

#[derive(Debug, Clone)]
struct SlackReplyTarget {
    channel: String,
    thread_ts: Option<String>,
}

impl SlackReplyTarget {
    fn from_session_key(session_key: &str) -> Option<Self> {
        let parts = session_key.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["slack", "dm", channel, thread_ts] => Some(Self {
                channel: (*channel).to_string(),
                thread_ts: Some((*thread_ts).to_string()),
            }),
            ["slack", "channel", channel, thread_ts] => Some(Self {
                channel: (*channel).to_string(),
                thread_ts: Some((*thread_ts).to_string()),
            }),
            ["slack", channel, "slash", _user] => Some(Self {
                channel: (*channel).to_string(),
                thread_ts: None,
            }),
            _ => None,
        }
    }
}

fn parse_message_event(payload: &Value, bot_user_id: &str) -> Option<SlackMessageEvent> {
    let event = payload.get("event")?;
    let event_type = event.get("type").and_then(Value::as_str)?;
    if !matches!(event_type, "message" | "app_mention") {
        return None;
    }
    if event_type == "message" && event.get("subtype").is_some() {
        return None;
    }
    if event.get("bot_id").is_some() {
        return None;
    }
    let user = event.get("user").and_then(Value::as_str)?.to_string();
    if user == bot_user_id {
        return None;
    }
    let channel = event.get("channel").and_then(Value::as_str)?.to_string();
    let ts = event.get("ts").and_then(Value::as_str)?.to_string();
    let text = event
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(SlackMessageEvent {
        event_key: format!("slack-message:{channel}:{ts}:{user}"),
        channel,
        user,
        text,
        ts,
        thread_ts: event
            .get("thread_ts")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn parse_slash_command(payload: &Value) -> Option<SlackSlashCommand> {
    Some(SlackSlashCommand {
        command: payload.get("command").and_then(Value::as_str)?.to_string(),
        text: payload
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        channel_id: payload
            .get("channel_id")
            .or_else(|| payload.get("channel"))
            .and_then(Value::as_str)?
            .to_string(),
        user_id: payload.get("user_id").and_then(Value::as_str)?.to_string(),
    })
}

fn parse_slack_thread_message(raw: Value) -> Option<SlackThreadMessage> {
    let ts = raw.get("ts").and_then(Value::as_str)?.to_string();
    let text = raw
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(SlackThreadMessage {
        ts,
        user: raw
            .get("user")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        bot_id: raw
            .get("bot_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        text,
        files: parse_slack_file_refs(&raw),
    })
}

fn parse_slack_file_refs(message: &Value) -> Vec<SlackFileRef> {
    message
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|file| {
            let id = file.get("id").and_then(Value::as_str)?.to_string();
            let name = file
                .get("name")
                .or_else(|| file.get("title"))
                .and_then(Value::as_str)
                .unwrap_or(&id)
                .to_string();
            let size_bytes = file
                .get("size")
                .and_then(Value::as_u64)
                .and_then(|size| usize::try_from(size).ok())
                .unwrap_or(0);
            Some(SlackFileRef {
                id,
                name,
                mimetype: file
                    .get("mimetype")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                size_bytes,
                url_private: file
                    .get("url_private")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                url_private_download: file
                    .get("url_private_download")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            })
        })
        .collect()
}

fn collect_slack_file_refs(messages: &[SlackThreadMessage]) -> Vec<SlackFileRef> {
    let mut seen = std::collections::BTreeSet::new();
    let mut files = Vec::new();
    for file in messages.iter().flat_map(|message| message.files.iter()) {
        if seen.insert(file.id.clone()) {
            files.push(file.clone());
        }
    }
    files
}

fn build_slack_context_request(
    session_key: &str,
    channel: &str,
    thread_ts: &str,
    messages: &[SlackThreadMessage],
    downloaded_files: &[DownloadedSlackFile],
    unresolved: Vec<ContextSyncIssueInput>,
) -> ContextSyncRequest {
    let mut artifacts = Vec::new();
    if !messages.is_empty() {
        let mut metadata = BTreeMap::new();
        metadata.insert("channel".to_string(), json!(channel));
        metadata.insert("thread_ts".to_string(), json!(thread_ts));
        metadata.insert("message_count".to_string(), json!(messages.len()));
        artifacts.push(ContextArtifactInput {
            id: format!("slack:thread:{channel}:{thread_ts}"),
            kind: "slack_current_thread".to_string(),
            title: "Current Slack thread".to_string(),
            source_locator: Some(format!("slack://{channel}/{thread_ts}")),
            files: vec![
                ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.md"),
                    content: ContextFileContent::Text(render_slack_thread_markdown(
                        channel, thread_ts, messages,
                    )),
                },
                ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.jsonl"),
                    content: ContextFileContent::Text(render_slack_thread_jsonl(messages)),
                },
            ],
            metadata,
        });
    }

    for downloaded in downloaded_files {
        artifacts.push(slack_file_artifact(downloaded));
    }

    ContextSyncRequest {
        session_key: session_key.to_string(),
        source: "slack".to_string(),
        base_path: PathBuf::from("slack"),
        artifacts,
        unresolved,
    }
}

fn slack_file_artifact(downloaded: &DownloadedSlackFile) -> ContextArtifactInput {
    let safe_id = sanitize_path_segment(&downloaded.file.id);
    let safe_name = sanitize_path_segment(&downloaded.file.name);
    let base = PathBuf::from("slack").join("files").join(&safe_id);
    let mut metadata = BTreeMap::new();
    metadata.insert("file_id".to_string(), json!(downloaded.file.id));
    metadata.insert("name".to_string(), json!(downloaded.file.name));
    metadata.insert("size_bytes".to_string(), json!(downloaded.file.size_bytes));
    if let Some(mimetype) = &downloaded.file.mimetype {
        metadata.insert("mimetype".to_string(), json!(mimetype));
    }

    let mut files = vec![
        ContextFileInput {
            relative_path: base.join("metadata.json"),
            content: ContextFileContent::Text(
                serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".to_string()),
            ),
        },
        ContextFileInput {
            relative_path: base.join("original").join(&safe_name),
            content: ContextFileContent::Bytes(downloaded.bytes.clone()),
        },
    ];
    if let Some(text) = &downloaded.extracted_text {
        files.push(ContextFileInput {
            relative_path: base.join("extracted.md"),
            content: ContextFileContent::Text(text.clone()),
        });
    }

    ContextArtifactInput {
        id: format!("slack:file:{}", downloaded.file.id),
        kind: "slack_file".to_string(),
        title: format!("Slack file {}", downloaded.file.name),
        source_locator: None,
        files,
        metadata,
    }
}

fn render_slack_thread_markdown(
    channel: &str,
    thread_ts: &str,
    messages: &[SlackThreadMessage],
) -> String {
    let mut lines = vec![format!("# Slack thread {channel} {thread_ts}")];
    for message in messages {
        let author = message
            .user
            .as_deref()
            .or(message.bot_id.as_deref())
            .unwrap_or("unknown");
        lines.push(String::new());
        lines.push(format!("## {message_ts} {author}", message_ts = message.ts));
        if message.text.trim().is_empty() {
            lines.push("[no text]".to_string());
        } else {
            lines.push(message.text.clone());
        }
        if !message.files.is_empty() {
            lines.push("Files:".to_string());
            for file in &message.files {
                lines.push(format!(
                    "- {} ({}, {} bytes)",
                    file.name,
                    file.mimetype.as_deref().unwrap_or("unknown"),
                    file.size_bytes
                ));
            }
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

fn render_slack_thread_jsonl(messages: &[SlackThreadMessage]) -> String {
    let mut lines = Vec::new();
    for message in messages {
        let files = message
            .files
            .iter()
            .map(|file| {
                json!({
                    "id": file.id,
                    "name": file.name,
                    "mimetype": file.mimetype,
                    "size_bytes": file.size_bytes,
                })
            })
            .collect::<Vec<_>>();
        lines.push(
            json!({
                "ts": message.ts,
                "user": message.user,
                "bot_id": message.bot_id,
                "text": message.text,
                "files": files,
            })
            .to_string(),
        );
    }
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

fn is_text_slack_file(file: &SlackFileRef) -> bool {
    let mimetype = file.mimetype.as_deref().unwrap_or("");
    if mimetype.starts_with("text/")
        || matches!(
            mimetype,
            "application/json" | "application/xml" | "application/yaml" | "application/x-yaml"
        )
    {
        return true;
    }
    let lower_name = file.name.to_ascii_lowercase();
    [
        ".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".csv", ".rs", ".py", ".js", ".ts",
        ".tsx", ".jsx", ".go", ".java", ".c", ".cc", ".cpp", ".h", ".hpp", ".sh", ".sql",
    ]
    .iter()
    .any(|suffix| lower_name.ends_with(suffix))
}

fn strip_bot_mention(text: &str, bot_user_id: &str) -> String {
    text.replace(&format!("<@{bot_user_id}>"), "")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use crate::approval::{ApprovalBroker, ApprovalOption, ApprovalRequest, ApprovalSelection};

    use serde_json::json;

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingRouter {
        handled: Mutex<Vec<RouterInput>>,
        observed: Mutex<Vec<RouterInput>>,
    }

    #[async_trait::async_trait]
    impl RouterService for RecordingRouter {
        async fn handle(
            &self,
            input: RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            self.handled.lock().await.push(input);
            Ok(())
        }

        async fn observe(&self, input: RouterInput) -> anyhow::Result<()> {
            self.observed.lock().await.push(input);
            Ok(())
        }
    }

    fn test_slack_config(require_mention: bool) -> SlackConfig {
        SlackConfig {
            enabled: true,
            bot_token: String::new(),
            app_token: String::new(),
            require_mention,
            channel_events: ChannelEventMode::Compact,
            context_sync: crate::config::SlackContextSyncConfig {
                enabled: true,
                max_file_bytes: 10 * 1024 * 1024,
            },
            allowed_channels: Default::default(),
            free_response_channels: Default::default(),
        }
    }

    fn thread_reply(text: impl Into<String>) -> SlackMessageEvent {
        SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "C1".to_string(),
            user: "U1".to_string(),
            text: text.into(),
            ts: "222.000".to_string(),
            thread_ts: Some("111.000".to_string()),
        }
    }

    fn approval_request(session_key: &str) -> ApprovalRequest {
        ApprovalRequest {
            session_key: session_key.to_string(),
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
        }
    }

    #[test]
    fn parses_app_mention_events() {
        let payload = json!({
            "event_id": "Ev1",
            "event": {
                "type": "app_mention",
                "user": "U1",
                "channel": "C1",
                "ts": "123.456",
                "text": "<@BOT> hello"
            }
        });

        let event = parse_message_event(&payload, "BOT").unwrap();

        assert_eq!(event.event_key, "slack-message:C1:123.456:U1");
        assert_eq!(event.channel, "C1");
        assert_eq!(event.text, "<@BOT> hello");
    }

    #[test]
    fn app_mention_and_message_events_for_same_message_share_event_key() {
        let app_mention = json!({
            "event_id": "Ev-app-mention",
            "event": {
                "type": "app_mention",
                "user": "U1",
                "channel": "C1",
                "ts": "123.456",
                "text": "<@BOT> hello"
            }
        });
        let message = json!({
            "event_id": "Ev-message",
            "event": {
                "type": "message",
                "user": "U1",
                "channel": "C1",
                "ts": "123.456",
                "text": "<@BOT> hello"
            }
        });

        let app_mention = parse_message_event(&app_mention, "BOT").unwrap();
        let message = parse_message_event(&message, "BOT").unwrap();

        assert_eq!(app_mention.event_key, message.event_key);
        assert_eq!(app_mention.event_key, "slack-message:C1:123.456:U1");
    }

    #[test]
    fn slack_thread_context_request_includes_current_thread_and_files() {
        let message = parse_slack_thread_message(json!({
            "ts": "111.000",
            "user": "U1",
            "text": "root",
            "files": [{
                "id": "F1",
                "name": "../design doc.md",
                "mimetype": "text/markdown",
                "size": 5,
                "url_private_download": "https://files.example/F1"
            }]
        }))
        .unwrap();
        let downloaded = DownloadedSlackFile {
            file: message.files[0].clone(),
            bytes: b"hello".to_vec(),
            extracted_text: Some("hello".to_string()),
        };

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            "C1",
            "111.000",
            &[message],
            &[downloaded],
            Vec::new(),
        );

        assert_eq!(request.source, "slack");
        assert_eq!(request.artifacts.len(), 2);
        assert_eq!(request.artifacts[0].kind, "slack_current_thread");
        let file_paths = request.artifacts[1]
            .files
            .iter()
            .map(|file| file.relative_path.display().to_string())
            .collect::<Vec<_>>();
        assert!(file_paths.contains(&"slack/files/F1/metadata.json".to_string()));
        assert!(file_paths.contains(&"slack/files/F1/original/design-doc.md".to_string()));
        assert!(file_paths.contains(&"slack/files/F1/extracted.md".to_string()));
    }

    #[test]
    fn collect_slack_file_refs_deduplicates_by_file_id() {
        let first = parse_slack_thread_message(json!({
            "ts": "111.000",
            "text": "one",
            "files": [{"id": "F1", "name": "a.txt"}]
        }))
        .unwrap();
        let second = parse_slack_thread_message(json!({
            "ts": "222.000",
            "text": "two",
            "files": [{"id": "F1", "name": "a.txt"}]
        }))
        .unwrap();

        let files = collect_slack_file_refs(&[first, second]);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "F1");
    }

    #[tokio::test]
    async fn unmentioned_thread_reply_is_observed_without_routing() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_message_event(thread_reply("middle context"), router_service, "BOT")
            .await
            .unwrap();

        assert!(router.handled.lock().await.is_empty());
        let observed = router.observed.lock().await;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].session_key, "slack:channel:C1:111.000");
        assert_eq!(observed[0].text, "middle context");
        assert_eq!(observed[0].user_id.as_deref(), Some("U1"));
    }

    #[tokio::test]
    async fn unmentioned_top_level_channel_message_is_ignored() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "C1".to_string(),
            user: "U1".to_string(),
            text: "top level".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
        };

        channel
            .handle_message_event(event, router_service, "BOT")
            .await
            .unwrap();

        assert!(router.handled.lock().await.is_empty());
        assert!(router.observed.lock().await.is_empty());
    }

    #[tokio::test]
    async fn unmentioned_approval_text_without_pending_is_observed_not_routed() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_message_event(thread_reply("/approve 1"), router_service, "BOT")
            .await
            .unwrap();

        assert!(router.handled.lock().await.is_empty());
        let observed = router.observed.lock().await;
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].text, "/approve 1");
    }

    #[tokio::test]
    async fn unmentioned_approval_text_with_pending_routes() {
        let approvals = Arc::new(ApprovalBroker::new(std::time::Duration::from_secs(5)));
        let mut prompts = approvals.subscribe();
        let request_broker = approvals.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(approval_request("slack:channel:C1:111.000"))
                .await
        });
        let prompt = prompts.recv().await.unwrap();
        let channel = SlackSocketModeChannel::new(test_slack_config(true), approvals.clone());
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_message_event(
                thread_reply(format!("/approve {}", prompt.id)),
                router_service,
                "BOT",
            )
            .await
            .unwrap();

        assert!(router.observed.lock().await.is_empty());
        let handled = router.handled.lock().await;
        assert_eq!(handled.len(), 1);
        assert_eq!(handled[0].session_key, "slack:channel:C1:111.000");
        assert_eq!(handled[0].text, format!("/approve {}", prompt.id));
        drop(handled);

        approvals
            .resolve_command(
                "slack:channel:C1:111.000",
                &format!("/deny {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[test]
    fn parses_reply_target_from_session_key() {
        let threaded = SlackReplyTarget::from_session_key("slack:channel:C1:123.456").unwrap();
        assert_eq!(threaded.channel, "C1");
        assert_eq!(threaded.thread_ts.as_deref(), Some("123.456"));

        let dm = SlackReplyTarget::from_session_key("slack:dm:D1:123.456").unwrap();
        assert_eq!(dm.channel, "D1");
        assert_eq!(dm.thread_ts.as_deref(), Some("123.456"));
    }

    #[test]
    fn dm_top_level_messages_get_distinct_session_keys() {
        let first = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "first".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
        };
        let second = SlackMessageEvent {
            event_key: "Ev2".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "second".to_string(),
            ts: "222.000".to_string(),
            thread_ts: None,
        };

        assert_eq!(first.session_key(), "slack:dm:D1:111.000");
        assert_eq!(second.session_key(), "slack:dm:D1:222.000");
    }

    #[test]
    fn dm_thread_replies_share_root_session_key() {
        let reply = SlackMessageEvent {
            event_key: "Ev2".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "reply".to_string(),
            ts: "222.000".to_string(),
            thread_ts: Some("111.000".to_string()),
        };

        assert_eq!(reply.session_key(), "slack:dm:D1:111.000");
    }
}
