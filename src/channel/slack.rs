use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const SLACK_REPLY_DRAFT_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
const SLACK_REPLY_DRAFT_UPDATE_GROWTH: usize = 500;
const SLACK_REPLY_DRAFT_INITIAL_MIN_LEN: usize = 40;
const SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES: usize = 8_000;
const SLACK_REPLY_DRAFT_TRUNCATED_PREFIX: &str = "...\n";
const SLACK_REPLY_DRAFT_MARKER: &str = "[router-draft]";

use crate::{
    approval::SharedApprovalBroker,
    channel::{
        EventDeduper,
        context::{ChannelContextResolveRequest, ChannelContextResolver},
        output::{ChannelOutputPolicy, ChannelOutputSink, ChannelReplyPort, PostedMessage},
    },
    config::{ChannelEventMode, SlackConfig},
    router::{
        ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome, RouterService,
    },
    session::context::{
        ContextArtifactInput, ContextArtifactRecord, ContextArtifactRemovalInput,
        ContextFileContent, ContextFileInput, ContextSyncIssueInput, ContextSyncRequest,
        sanitize_path_segment,
    },
};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const SLACK_MARKDOWN_BLOCK_CHAR_LIMIT: usize = 12_000;

mod context;
use self::context::*;

#[derive(Debug, Clone)]
pub struct SlackSocketModeChannel {
    cfg: SlackConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    seen_events: Arc<Mutex<EventDeduper>>,
    context_cache: Arc<Mutex<SlackContextCache>>,
}

impl SlackSocketModeChannel {
    pub fn new(cfg: SlackConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            seen_events: Arc::new(Mutex::new(EventDeduper::new(512))),
            context_cache: Arc::new(Mutex::new(SlackContextCache::default())),
        }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        self.validate_tokens()?;
        let channel = Arc::new(self);
        let bot_user_id = channel.auth_test_until_ready().await?;
        channel.clone().spawn_approval_notifier();

        loop {
            match channel.clone().run_once(router.clone(), &bot_user_id).await {
                Ok(()) => tracing::warn!("Slack Socket Mode ended; reconnecting"),
                Err(err) if slack_socket_error_is_fatal(&err) => return Err(err),
                Err(err) => {
                    tracing::warn!(error = %err, "Slack Socket Mode disconnected; reconnecting");
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn run_once(
        self: Arc<Self>,
        router: Arc<dyn RouterService>,
        bot_user_id: &str,
    ) -> anyhow::Result<()> {
        let url = self.open_socket_url().await?;
        tracing::info!("connecting Slack Socket Mode");
        let (stream, _) = connect_async(url).await?;
        tracing::info!(bot_user_id = %bot_user_id, "connected Slack Socket Mode");
        let (mut sink, mut stream) = stream.split();

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
                    let channel_ref = self.clone();
                    let router_ref = router.clone();
                    let bot_user_id = bot_user_id.to_string();
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

    async fn auth_test_until_ready(&self) -> anyhow::Result<String> {
        loop {
            match self.auth_test().await {
                Ok(bot_user_id) => return Ok(bot_user_id),
                Err(err) if slack_socket_error_is_fatal(&err) => return Err(err),
                Err(err) => {
                    tracing::warn!(error = %err, "Slack auth.test failed; retrying");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
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
                if !prompt_channel.approvals.has_pending(&prompt.id).await {
                    continue;
                }
                let text = prompt.render_text();
                tokio::select! {
                    biased;
                    _ = prompt.cancelled() => continue,
                    result = prompt_channel.post_message(&target, &text) => {
                        if let Err(err) = result {
                            tracing::warn!(error = %err, "failed to post Slack approval prompt");
                        }
                    }
                }
            }
        });

        // YOLO auto-approvals are high-frequency bookkeeping. Keep them out of
        // chat and let the tool activity message carry the useful progress.
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
        let should_route = is_dm
            || !self.cfg.require_mention
            || mentioned
            || self.cfg.free_response_channels.contains(&event.channel);
        let intent = if should_route {
            ChannelInputIntent::Route
        } else if event.is_thread_reply() {
            tracing::info!(
                channel = %event.channel,
                user_id = %event.user,
                session_key = %session_key,
                text_len = text.len(),
                "passing unmentioned Slack thread message to router intake"
            );
            ChannelInputIntent::RouteIfPendingApprovalElseObserve
        } else {
            ChannelInputIntent::Ignore
        };
        let outcome = router
            .begin_channel_input(ChannelInput {
                session_key: session_key.clone(),
                text: text.clone(),
                user_id: Some(event.user.clone()),
                source: "slack".to_string(),
                intent,
                context_policy: ChannelContextPolicy {
                    source: "slack".to_string(),
                    enabled: self.should_sync_context(),
                },
            })
            .await?;
        let ChannelIntakeOutcome::Route {
            ticket,
            context_allowed,
        } = outcome
        else {
            return Ok(());
        };
        if should_route {
            tracing::info!(
                channel = %event.channel,
                user_id = %event.user,
                session_key = %session_key,
                text_len = text.len(),
                "routing Slack message"
            );
        } else if event.is_thread_reply() {
            tracing::info!(
                channel = %event.channel,
                user_id = %event.user,
                session_key = %session_key,
                text_len = text.len(),
                "routing Slack approval command from unmentioned thread"
            );
        }
        let context_cache_token = if context_allowed {
            if let Some(cache_sequence) = ticket.context_sequence() {
                self.remember_context_cache_sequence(&session_key, cache_sequence)
                    .await;
                Some(SlackContextCacheToken {
                    session_key: session_key.clone(),
                    cache_sequence,
                })
            } else {
                tracing::warn!(
                    session_key = %session_key,
                    "skipping Slack context sync because router did not provide a route sequence"
                );
                None
            }
        } else {
            None
        };
        let context = if context_cache_token.is_some() {
            let existing_context = router.context_artifacts(&session_key, "slack").await?;
            Some(
                SlackContextResolver::new(self, &event, bot_user_id, context_cache_token.as_ref())
                    .resolve(ChannelContextResolveRequest {
                        session_key: session_key.clone(),
                        existing_artifacts: existing_context,
                    })
                    .await?,
            )
        } else {
            None
        };
        let succeeded_file_sync_keys = context
            .as_ref()
            .map(|context| context.succeeded_cache_keys.clone())
            .unwrap_or_default();
        let failed_file_sync_keys = context
            .as_ref()
            .map(|context| context.failed_cache_keys.clone())
            .unwrap_or_default();
        let reply_target = event.reply_target();
        let mut output = ChannelOutputSink::new(
            SlackReplyPort {
                channel: self.clone(),
            },
            reply_target,
            slack_output_policy(self.cfg.channel_events),
        );
        router
            .finish_channel_input(
                ticket,
                context.map(|context| context.sync_request),
                &mut output,
            )
            .await?;
        for cache_key in succeeded_file_sync_keys {
            self.mark_file_sync_succeeded(cache_key, context_cache_token.as_ref())
                .await;
        }
        for cache_key in failed_file_sync_keys {
            self.mark_file_sync_failed(cache_key, context_cache_token.as_ref())
                .await;
        }
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
        let text = normalize_slack_slash_command_text(&command);
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
        let mut output = ChannelOutputSink::new(
            SlackReplyPort {
                channel: self.clone(),
            },
            reply_target,
            slack_output_policy(self.cfg.channel_events),
        );
        let outcome = router
            .begin_channel_input(ChannelInput {
                session_key,
                text,
                user_id: Some(command.user_id),
                source: "slack".to_string(),
                intent: ChannelInputIntent::Route,
                context_policy: ChannelContextPolicy::disabled("slack"),
            })
            .await?;
        let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
            return Ok(());
        };
        router
            .finish_channel_input(ticket, None, &mut output)
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
            return Err(SlackApiError::new(
                "apps.connections.open",
                resp.error.unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
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
            return Err(SlackApiError::new(
                "auth.test",
                resp.error.unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
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
        self.post_message_body_with_ts(target, slack_message_body(target, text))
            .await
    }

    async fn post_markdown_message_with_ts(
        &self,
        target: &SlackReplyTarget,
        text: &str,
    ) -> anyhow::Result<Option<String>> {
        let Some(body) = slack_markdown_message_body(target, text) else {
            return self.post_message_with_ts(target, text).await;
        };
        match self.post_message_body_with_ts(target, body).await {
            Ok(ts) => Ok(ts),
            Err(err) if slack_error_is_invalid_blocks(&err) => {
                tracing::warn!(
                    error = %err,
                    "Slack markdown block was rejected; retrying final reply as plain text"
                );
                self.post_message_with_ts(target, text).await
            }
            Err(err) => Err(err),
        }
    }

    async fn post_message_body_with_ts(
        &self,
        target: &SlackReplyTarget,
        body: Value,
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
            text_len = body
                .get("text")
                .and_then(|value| value.as_str())
                .map(str::len)
                .unwrap_or_default(),
            "sending Slack message"
        );
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
            return Err(SlackApiError::new(
                "chat.postMessage",
                resp.error.unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        tracing::info!(
            channel = %target.channel,
            thread_ts = ?target.thread_ts,
            text_len = body
                .get("text")
                .and_then(|value| value.as_str())
                .map(str::len)
                .unwrap_or_default(),
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

    async fn update_markdown_message(
        &self,
        target: &SlackReplyTarget,
        ts: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        let Some(body) = slack_markdown_update_body(target, ts, text) else {
            return self.update_message(target, ts, text).await;
        };
        match self.update_message_body(body).await {
            Ok(()) => Ok(()),
            Err(err) if slack_error_is_invalid_blocks(&err) => {
                tracing::warn!(
                    error = %err,
                    "Slack markdown block was rejected; retrying final update as plain text"
                );
                self.update_message(target, ts, text).await
            }
            Err(err) => Err(err),
        }
    }

    async fn update_message_body(&self, body: Value) -> anyhow::Result<()> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            error: Option<String>,
        }

        let channel = body
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let ts = body.get("ts").and_then(Value::as_str).unwrap_or_default();
        let text_len = body
            .get("text")
            .and_then(Value::as_str)
            .map(str::len)
            .unwrap_or_default();
        tracing::info!(channel, ts, text_len, "updating Slack message");
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
            return Err(SlackApiError::new(
                "chat.update",
                resp.error.unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        tracing::info!(channel, ts, text_len, "updated Slack message");
        Ok(())
    }

    async fn delete_message(&self, target: &SlackReplyTarget, ts: &str) -> anyhow::Result<()> {
        #[derive(Deserialize)]
        struct Response {
            ok: bool,
            error: Option<String>,
        }

        tracing::info!(
            channel = %target.channel,
            ts,
            "deleting Slack message"
        );
        let body = json!({
            "channel": target.channel,
            "ts": ts,
        });
        let resp = self
            .http
            .post("https://slack.com/api/chat.delete")
            .bearer_auth(&self.cfg.bot_token)
            .json(&body)
            .send()
            .await?
            .json::<Response>()
            .await?;
        if !resp.ok {
            return Err(SlackApiError::new(
                "chat.delete",
                resp.error.unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        tracing::info!(
            channel = %target.channel,
            ts,
            "deleted Slack message"
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

    fn should_accept_linked_thread_channel(
        &self,
        current_channel: &str,
        linked_channel: &str,
    ) -> bool {
        linked_channel == current_channel
            || (!self.cfg.allowed_channels.is_empty()
                && self.cfg.allowed_channels.contains(linked_channel))
    }

    fn should_sync_context(&self) -> bool {
        self.cfg.context_sync.enabled
            && !self.cfg.bot_token.is_empty()
            && (self.cfg.context_sync.current_thread
                || self.cfg.context_sync.linked_threads
                || self.cfg.context_sync.files)
    }
}

#[derive(Clone)]
struct SlackReplyPort {
    channel: SlackSocketModeChannel,
}

fn slack_output_policy(activity_mode: ChannelEventMode) -> ChannelOutputPolicy {
    let mut policy = ChannelOutputPolicy::streaming_draft(activity_mode);
    policy.draft_update_interval = SLACK_REPLY_DRAFT_UPDATE_INTERVAL;
    policy.draft_update_growth = SLACK_REPLY_DRAFT_UPDATE_GROWTH;
    policy.draft_initial_min_len = SLACK_REPLY_DRAFT_INITIAL_MIN_LEN;
    policy.draft_preview_max_bytes = SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES;
    policy.draft_truncated_prefix = SLACK_REPLY_DRAFT_TRUNCATED_PREFIX;
    policy.draft_marker = SLACK_REPLY_DRAFT_MARKER;
    policy
}

#[async_trait::async_trait]
impl ChannelReplyPort for SlackReplyPort {
    type Target = SlackReplyTarget;

    async fn post_text(&self, target: &Self::Target, text: &str) -> anyhow::Result<PostedMessage> {
        Ok(self
            .channel
            .post_message_with_ts(target, text)
            .await?
            .map_or_else(PostedMessage::without_id, PostedMessage::with_id))
    }

    async fn post_markdown(
        &self,
        target: &Self::Target,
        text: &str,
    ) -> anyhow::Result<PostedMessage> {
        let Some(id) = self
            .channel
            .post_markdown_message_with_ts(target, text)
            .await?
        else {
            anyhow::bail!("Slack final reply message response omitted ts");
        };
        Ok(PostedMessage::with_id(id))
    }

    async fn update_text(&self, target: &Self::Target, id: &str, text: &str) -> anyhow::Result<()> {
        self.channel.update_message(target, id, text).await
    }

    async fn update_markdown(
        &self,
        target: &Self::Target,
        id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        self.channel.update_markdown_message(target, id, text).await
    }

    async fn delete(&self, target: &Self::Target, id: &str) -> anyhow::Result<()> {
        self.channel.delete_message(target, id).await
    }
}

fn slack_message_body(target: &SlackReplyTarget, text: &str) -> Value {
    let mut body = json!({
        "channel": target.channel,
        "text": text,
    });
    if let Some(thread_ts) = &target.thread_ts {
        body["thread_ts"] = Value::String(thread_ts.clone());
    }
    body
}

fn slack_markdown_message_body(target: &SlackReplyTarget, text: &str) -> Option<Value> {
    if text.trim().is_empty() || text.chars().count() > SLACK_MARKDOWN_BLOCK_CHAR_LIMIT {
        return None;
    }

    let mut body = slack_message_body(target, text);
    body["blocks"] = json!([
        {
            "type": "markdown",
            "text": text,
        }
    ]);
    Some(body)
}

fn slack_markdown_update_body(target: &SlackReplyTarget, ts: &str, text: &str) -> Option<Value> {
    if text.trim().is_empty() || text.chars().count() > SLACK_MARKDOWN_BLOCK_CHAR_LIMIT {
        return None;
    }

    Some(json!({
        "channel": target.channel,
        "ts": ts,
        "text": text,
        "blocks": [
            {
                "type": "markdown",
                "text": text,
            }
        ],
    }))
}

#[cfg(test)]
fn slack_reply_draft_text(text: &str) -> String {
    let preview = slack_reply_draft_preview(text);
    format!("{preview}\n\n{SLACK_REPLY_DRAFT_MARKER}")
}

#[cfg(test)]
fn slack_reply_draft_preview(text: &str) -> String {
    if text.len() <= SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES {
        return text.to_string();
    }

    let suffix_budget = SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES
        .saturating_sub(SLACK_REPLY_DRAFT_TRUNCATED_PREFIX.len());
    let mut start = text.len().saturating_sub(suffix_budget);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    format!("{}{}", SLACK_REPLY_DRAFT_TRUNCATED_PREFIX, &text[start..])
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

#[derive(Debug, Clone)]
struct SlackMessageEvent {
    event_key: String,
    channel: String,
    user: String,
    text: String,
    ts: String,
    thread_ts: Option<String>,
    files: Vec<SlackFileRef>,
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

fn parse_message_event(payload: &Value, bot_user_id: &str) -> Option<SlackMessageEvent> {
    let event = payload.get("event")?;
    let event_type = event.get("type").and_then(Value::as_str)?;
    if !matches!(event_type, "message" | "app_mention") {
        return None;
    }
    if event_type == "message"
        && event
            .get("subtype")
            .and_then(Value::as_str)
            .is_some_and(|subtype| subtype != "file_share")
    {
        return None;
    }
    if event
        .get("bot_id")
        .and_then(Value::as_str)
        .is_some_and(|bot_id| !bot_id.trim().is_empty())
    {
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
        files: parse_slack_file_refs(event),
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

fn normalize_slack_slash_command_text(command: &SlackSlashCommand) -> String {
    let name = command.command.trim();
    let text = command.text.trim();
    if matches!(name, "/stop" | "/agent" | "/yolo" | "/approve" | "/deny") {
        return format!("{name} {text}").trim().to_string();
    }
    if text.is_empty() {
        name.to_string()
    } else {
        text.to_string()
    }
}

#[derive(Debug, Clone)]
struct SlackApiError {
    method: &'static str,
    code: String,
}

impl SlackApiError {
    fn new(method: &'static str, code: impl Into<String>) -> Self {
        Self {
            method,
            code: code.into(),
        }
    }

    fn is_access_denied(&self) -> bool {
        matches!(
            self.code.as_str(),
            "not_in_channel"
                | "channel_not_found"
                | "missing_scope"
                | "not_authed"
                | "invalid_auth"
                | "account_inactive"
                | "token_revoked"
                | "not_allowed_token_type"
                | "token_expired"
                | "team_access_not_granted"
                | "access_denied"
                | "no_permission"
                | "forbidden_team"
        )
    }

    fn is_transient(&self) -> bool {
        matches!(
            self.code.as_str(),
            "rate_limited"
                | "ratelimited"
                | "internal_error"
                | "fatal_error"
                | "service_unavailable"
                | "team_added_to_org"
        )
    }
}

impl std::fmt::Display for SlackApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Slack {} failed: {}", self.method, self.code)
    }
}

impl std::error::Error for SlackApiError {}

fn slack_error_is_access_denied(err: &anyhow::Error) -> bool {
    err.downcast_ref::<SlackApiError>()
        .is_some_and(SlackApiError::is_access_denied)
}

fn slack_error_is_invalid_blocks(err: &anyhow::Error) -> bool {
    err.downcast_ref::<SlackApiError>().is_some_and(|err| {
        matches!(err.method, "chat.postMessage" | "chat.update")
            && matches!(err.code.as_str(), "invalid_blocks")
    })
}

fn slack_error_allows_cached_context(err: &anyhow::Error) -> bool {
    if let Some(err) = err.downcast_ref::<SlackApiError>() {
        return err.is_transient();
    }
    err.downcast_ref::<reqwest::Error>()
        .is_some_and(|err| err.is_timeout() || err.is_connect())
}

fn slack_error_clears_thread_cache(err: &anyhow::Error) -> bool {
    slack_error_is_access_denied(err)
}

fn slack_socket_error_is_fatal(err: &anyhow::Error) -> bool {
    err.downcast_ref::<SlackApiError>().is_some_and(|err| {
        matches!(err.method, "apps.connections.open" | "auth.test") && !err.is_transient()
    })
}

fn cached_thread_fetch_after_error(
    err: &anyhow::Error,
    current_cache: impl FnOnce() -> Vec<SlackThreadMessage>,
) -> Option<SlackThreadFetch> {
    if !slack_error_allows_cached_context(err) {
        return None;
    }
    let cached = current_cache();
    if cached.is_empty() {
        return None;
    }
    Some(SlackThreadFetch {
        messages: cached,
        stale_reason: Some(context_error_reason(err)),
    })
}

fn strip_bot_mention(text: &str, bot_user_id: &str) -> String {
    text.replace(&format!("<@{bot_user_id}>"), "")
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use crate::approval::{ApprovalBroker, ApprovalOption, ApprovalRequest, ApprovalSelection};
    use crate::channel::context::{ChannelContextResolveRequest, ChannelContextResolver};
    use crate::router::{
        ChannelRouteTicket, RouterInput, RouterOutputSink, TurnBeginMode, TurnReservation,
    };
    use crate::session::ContextSyncRequest;

    use serde_json::json;

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingRouter {
        pending_approval: bool,
        handled: Mutex<Vec<RouterInput>>,
        observed: Mutex<Vec<RouterInput>>,
        reserved: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl RouterService for RecordingRouter {
        async fn has_pending_approval(&self, _session_key: &str) -> anyhow::Result<bool> {
            Ok(self.pending_approval)
        }

        async fn reserve_turn(
            &self,
            session_key: &str,
            _mode: TurnBeginMode,
        ) -> anyhow::Result<Option<TurnReservation>> {
            self.reserved.lock().await.push(session_key.to_string());
            Ok(None)
        }

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

    #[derive(Debug, Default)]
    struct IntakeOnlyRouter {
        began: Mutex<Vec<ChannelInput>>,
        finished: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl RouterService for IntakeOnlyRouter {
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
            _context: Option<ContextSyncRequest>,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            *self.finished.lock().await += 1;
            Ok(())
        }

        async fn reserve_turn(
            &self,
            _session_key: &str,
            _mode: TurnBeginMode,
        ) -> anyhow::Result<Option<TurnReservation>> {
            panic!("Slack adapter should not reserve turns directly")
        }

        async fn handle(
            &self,
            _input: RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            panic!("Slack adapter should not handle routed input directly")
        }

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            panic!("Slack adapter should not observe input directly")
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
                current_thread: true,
                linked_threads: true,
                files: true,
                linked_thread_depth: 1,
                max_file_bytes: 10 * 1024 * 1024,
                max_files_per_turn: 20,
                max_linked_threads_per_turn: 10,
            },
            allowed_channels: Default::default(),
            free_response_channels: Default::default(),
        }
    }

    #[test]
    fn slack_markdown_message_body_uses_markdown_block() {
        let target = SlackReplyTarget {
            channel: "C1".to_string(),
            thread_ts: Some("111.000".to_string()),
        };
        let text = "**bold**\n\n| a | b |\n| - | - |\n| 1 | 2 |";

        let body = slack_markdown_message_body(&target, text).unwrap();

        assert_eq!(body["channel"], "C1");
        assert_eq!(body["thread_ts"], "111.000");
        assert_eq!(body["text"], text);
        assert_eq!(body["blocks"][0]["type"], "markdown");
        assert_eq!(body["blocks"][0]["text"], text);
    }

    #[test]
    fn slack_plain_message_body_omits_blocks() {
        let target = SlackReplyTarget {
            channel: "C1".to_string(),
            thread_ts: None,
        };

        let body = slack_message_body(&target, "plain");

        assert_eq!(body["channel"], "C1");
        assert_eq!(body["text"], "plain");
        assert!(body.get("blocks").is_none());
        assert!(body.get("thread_ts").is_none());
    }

    #[test]
    fn slack_markdown_message_body_falls_back_for_invalid_block_text() {
        let target = SlackReplyTarget {
            channel: "C1".to_string(),
            thread_ts: None,
        };
        let long_text = "x".repeat(SLACK_MARKDOWN_BLOCK_CHAR_LIMIT + 1);

        assert!(slack_markdown_message_body(&target, "").is_none());
        assert!(slack_markdown_message_body(&target, "   ").is_none());
        assert!(slack_markdown_message_body(&target, &long_text).is_none());
    }

    fn thread_reply(text: impl Into<String>) -> SlackMessageEvent {
        SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "C1".to_string(),
            user: "U1".to_string(),
            text: text.into(),
            ts: "222.000".to_string(),
            thread_ts: Some("111.000".to_string()),
            files: Vec::new(),
        }
    }

    #[test]
    fn slack_app_entry_slash_command_uses_payload_text() {
        let command = SlackSlashCommand {
            command: "/hermes".to_string(),
            text: " //status --json ".to_string(),
            channel_id: "C1".to_string(),
            user_id: "U1".to_string(),
        };

        assert_eq!(
            normalize_slack_slash_command_text(&command),
            "//status --json"
        );
    }

    #[test]
    fn router_owned_slack_platform_slash_command_keeps_command_name() {
        let command = SlackSlashCommand {
            command: "/agent".to_string(),
            text: " status ".to_string(),
            channel_id: "C1".to_string(),
            user_id: "U1".to_string(),
        };

        assert_eq!(
            normalize_slack_slash_command_text(&command),
            "/agent status"
        );
    }

    fn file_ref(id: &str) -> SlackFileRef {
        SlackFileRef {
            id: id.to_string(),
            name: format!("{id}.txt"),
            mimetype: Some("text/plain".to_string()),
            size_bytes: 1,
            url_private: None,
            url_private_download: Some(format!("https://files.example/{id}")),
        }
    }

    #[test]
    fn parses_slack_thread_message_author_name_from_inline_profile() {
        let message = parse_slack_thread_message(json!({
            "ts": "111.000",
            "user": "U1",
            "user_profile": {
                "display_name": "Alice",
                "real_name": "Alice Smith",
                "name": "asmith"
            },
            "text": "hello"
        }))
        .unwrap();

        assert_eq!(message.author_name.as_deref(), Some("Alice"));
    }

    #[test]
    fn slack_user_display_name_falls_back_to_real_name_and_username() {
        let user_with_real_name = json!({
            "id": "U1",
            "name": "asmith",
            "profile": {
                "display_name": "",
                "real_name": "Alice Smith"
            }
        });
        let user_with_username = json!({
            "id": "U2",
            "name": "bjones",
            "profile": {
                "display_name": "",
                "real_name": ""
            }
        });

        assert_eq!(
            slack_user_display_name(&user_with_real_name).as_deref(),
            Some("Alice Smith")
        );
        assert_eq!(
            slack_user_display_name(&user_with_username).as_deref(),
            Some("bjones")
        );
    }

    #[test]
    fn applies_resolved_slack_user_author_names_without_overwriting_inline_names() {
        let mut messages = vec![
            SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U1".to_string()),
                author_name: None,
                bot_id: None,
                text: "hello".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "112.000".to_string(),
                user: Some("U1".to_string()),
                author_name: Some("Inline Alice".to_string()),
                bot_id: None,
                text: "hello again".to_string(),
                files: Vec::new(),
            },
        ];
        let names = BTreeMap::from([("U1".to_string(), "Alice Smith".to_string())]);

        apply_slack_user_author_names(&mut messages, &names);

        assert_eq!(messages[0].author_name.as_deref(), Some("Alice Smith"));
        assert_eq!(messages[1].author_name.as_deref(), Some("Inline Alice"));
    }

    #[test]
    fn reuses_inline_author_names_for_same_user_messages() {
        let mut messages = vec![
            SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U1".to_string()),
                author_name: Some("Alice Smith".to_string()),
                bot_id: None,
                text: "hello".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "112.000".to_string(),
                user: Some("U1".to_string()),
                author_name: None,
                bot_id: None,
                text: "hello again".to_string(),
                files: Vec::new(),
            },
        ];
        let names = slack_inline_user_author_names(&messages);

        apply_slack_user_author_names(&mut messages, &names);

        assert_eq!(messages[1].author_name.as_deref(), Some("Alice Smith"));
    }

    #[tokio::test]
    async fn resolved_thread_message_authors_cache_inline_names() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let mut messages = vec![SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: Some("Alice Smith".to_string()),
            bot_id: None,
            text: "hello".to_string(),
            files: Vec::new(),
        }];

        channel.resolve_thread_message_authors(&mut messages).await;

        let cache = channel.context_cache.lock().await;
        assert_eq!(
            cache.user_names.get("U1").map(String::as_str),
            Some("Alice Smith")
        );
    }

    #[test]
    fn rendered_slack_thread_includes_author_name() {
        let messages = vec![SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: Some("Alice Smith".to_string()),
            bot_id: None,
            text: "hello".to_string(),
            files: Vec::new(),
        }];

        let markdown = render_slack_thread_markdown("C1", "111.000", &messages);
        let jsonl = render_slack_thread_jsonl(&messages);
        let record: Value = serde_json::from_str(jsonl.lines().next().unwrap()).unwrap();

        assert!(markdown.contains("## 111.000 Alice Smith (U1)"));
        assert_eq!(record["author_name"].as_str(), Some("Alice Smith"));
        assert_eq!(record["user"].as_str(), Some("U1"));
    }

    #[test]
    fn linked_threads_default_to_same_channel_unless_explicitly_allowed() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );

        assert!(channel.should_accept_linked_thread_channel("C1", "C1"));
        assert!(!channel.should_accept_linked_thread_channel("C1", "C2"));

        let mut cfg = test_slack_config(true);
        cfg.allowed_channels.insert("C2".to_string());
        let channel = SlackSocketModeChannel::new(cfg, Arc::new(ApprovalBroker::default()));

        assert!(channel.should_accept_linked_thread_channel("C1", "C2"));
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
    fn slack_socket_lifecycle_treats_auth_errors_as_fatal() {
        for code in [
            "invalid_auth",
            "not_allowed_token_type",
            "token_expired",
            "team_access_not_granted",
            "access_denied",
            "no_permission",
            "forbidden_team",
        ] {
            let err = anyhow::Error::new(SlackApiError::new("apps.connections.open", code));
            assert!(slack_socket_error_is_fatal(&err), "{code}");
        }

        for code in [
            "rate_limited",
            "ratelimited",
            "internal_error",
            "service_unavailable",
        ] {
            let err = anyhow::Error::new(SlackApiError::new("apps.connections.open", code));
            assert!(!slack_socket_error_is_fatal(&err), "{code}");
        }

        let auth_invalid = anyhow::Error::new(SlackApiError::new("auth.test", "invalid_auth"));
        let auth_rate_limited = anyhow::Error::new(SlackApiError::new("auth.test", "rate_limited"));
        let socket_reset =
            anyhow::anyhow!("WebSocket protocol error: Connection reset without closing handshake");

        assert!(slack_socket_error_is_fatal(&auth_invalid));
        assert!(!slack_socket_error_is_fatal(&auth_rate_limited));
        assert!(!slack_socket_error_is_fatal(&socket_reset));
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
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: true,
                messages: &[message],
            },
            &[],
            &[downloaded],
            SlackDerivedArtifactRetention::new(BTreeSet::new(), BTreeSet::new(), true),
            Vec::new(),
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
    fn slack_context_request_prunes_unretained_linked_threads_and_files() {
        let current = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "root".to_string(),
            files: Vec::new(),
        };
        let linked = LinkedSlackThread {
            link: SlackThreadLink {
                channel: "C2".to_string(),
                thread_ts: "222.000".to_string(),
                url: "https://example.slack.com/archives/C2/p222000000".to_string(),
                source_message_ts: "111.000".to_string(),
            },
            fresh: true,
            messages: vec![SlackThreadMessage {
                ts: "222.000".to_string(),
                user: Some("U2".to_string()),
                author_name: None,
                bot_id: None,
                text: "linked".to_string(),
                files: Vec::new(),
            }],
        };
        let downloaded = DownloadedSlackFile {
            file: file_ref("F1"),
            bytes: b"one".to_vec(),
            extracted_text: Some("one".to_string()),
        };
        let mut retained_files = BTreeSet::new();
        retained_files.insert(slack_file_artifact_id("F2"));

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: true,
                messages: &[current],
            },
            &[linked],
            &[downloaded],
            SlackDerivedArtifactRetention::new(BTreeSet::new(), retained_files, true),
            Vec::new(),
            Vec::new(),
        );

        let linked_retain_ids = request
            .remove_artifacts
            .iter()
            .find_map(|removal| match removal {
                ContextArtifactRemovalInput::ExceptKind { kind, retain_ids } => {
                    (kind == "slack_linked_thread").then_some(retain_ids)
                }
                _ => None,
            })
            .unwrap();
        assert!(linked_retain_ids.contains(&slack_linked_thread_artifact_id("C2", "222.000")));
        assert!(!linked_retain_ids.contains(&slack_linked_thread_artifact_id("C3", "333.000")));

        let file_retain_ids = request
            .remove_artifacts
            .iter()
            .find_map(|removal| match removal {
                ContextArtifactRemovalInput::ExceptKind { kind, retain_ids } => {
                    (kind == "slack_file").then_some(retain_ids)
                }
                _ => None,
            })
            .unwrap();
        assert!(file_retain_ids.contains(&slack_file_artifact_id("F1")));
        assert!(file_retain_ids.contains(&slack_file_artifact_id("F2")));
        assert!(!file_retain_ids.contains(&slack_file_artifact_id("F3")));
    }

    #[test]
    fn slack_context_request_retains_unfetched_current_linked_threads() {
        let current = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "root".to_string(),
            files: Vec::new(),
        };
        let linked = LinkedSlackThread {
            link: SlackThreadLink {
                channel: "C2".to_string(),
                thread_ts: "222.000".to_string(),
                url: "https://example.slack.com/archives/C2/p222000000".to_string(),
                source_message_ts: "111.000".to_string(),
            },
            fresh: true,
            messages: vec![SlackThreadMessage {
                ts: "222.000".to_string(),
                user: Some("U2".to_string()),
                author_name: None,
                bot_id: None,
                text: "linked".to_string(),
                files: Vec::new(),
            }],
        };
        let mut retained_linked_threads = BTreeSet::new();
        retained_linked_threads.insert(slack_linked_thread_artifact_id("C4", "444.000"));

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: true,
                messages: &[current],
            },
            &[linked],
            &[],
            SlackDerivedArtifactRetention::new(retained_linked_threads, BTreeSet::new(), true),
            Vec::new(),
            Vec::new(),
        );

        let linked_retain_ids = request
            .remove_artifacts
            .iter()
            .find_map(|removal| match removal {
                ContextArtifactRemovalInput::ExceptKind { kind, retain_ids } => {
                    (kind == "slack_linked_thread").then_some(retain_ids)
                }
                _ => None,
            })
            .unwrap();
        assert!(linked_retain_ids.contains(&slack_linked_thread_artifact_id("C2", "222.000")));
        assert!(linked_retain_ids.contains(&slack_linked_thread_artifact_id("C4", "444.000")));
        assert!(!linked_retain_ids.contains(&slack_linked_thread_artifact_id("C5", "555.000")));
    }

    #[test]
    fn slack_context_request_skips_derived_pruning_without_current_thread_view() {
        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: false,
                messages: &[],
            },
            &[],
            &[],
            SlackDerivedArtifactRetention::new(BTreeSet::new(), BTreeSet::new(), false),
            Vec::new(),
            vec![ContextSyncIssueInput {
                kind: "current_thread".to_string(),
                reference: "C1:111.000".to_string(),
                reason: "Slack conversations.replies failed: rate_limited".to_string(),
            }],
        );

        assert!(!request.remove_artifacts.iter().any(|removal| matches!(
            removal,
            ContextArtifactRemovalInput::ExceptKind { kind, .. }
                if kind == "slack_linked_thread" || kind == "slack_file"
        )));
        assert_eq!(request.unresolved.len(), 1);
    }

    #[test]
    fn collect_context_file_refs_deduplicates_by_file_id() {
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

        let event = thread_reply("");
        let files = collect_context_file_refs(&event, &[first, second], &[]);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "F1");
    }

    #[test]
    fn collect_context_file_refs_includes_slack_file_permalink_from_text() {
        let event = thread_reply(
            "<@BOT> try this API https://smartx1.slack.com/files/U03LA0JQ491/F0BDLKSPEF9/clio-api.md",
        );

        let files = collect_context_file_refs(&event, &[], &[]);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "F0BDLKSPEF9");
        assert_eq!(files[0].name, "clio-api.md");
        assert!(slack_file_ref_needs_info(&files[0]));
    }

    #[test]
    fn synced_file_syncs_do_not_count_against_turn_limit() {
        let mut state = SlackFileSyncState::default();
        state.synced_files.insert("session:F1".to_string());

        let selection =
            select_file_sync_attempts("session", vec![file_ref("F1"), file_ref("F2")], &state, 1);

        assert_eq!(selection.skipped_synced_files.len(), 1);
        assert_eq!(selection.attempts.len(), 1);
        assert_eq!(selection.attempts[0].0, "session:F2");
        assert_eq!(selection.attempts[0].1.id, "F2");
    }

    #[test]
    fn unsynced_files_over_turn_limit_are_omitted_for_retry() {
        let selection = select_file_sync_attempts(
            "session",
            vec![file_ref("F1"), file_ref("F2")],
            &SlackFileSyncState::default(),
            1,
        );

        assert_eq!(selection.attempts.len(), 1);
        assert_eq!(selection.attempts[0].1.id, "F1");
        assert_eq!(selection.omitted_files.len(), 1);
        assert_eq!(selection.omitted_files[0].id, "F2");
    }

    #[test]
    fn failed_file_syncs_are_retried_after_new_files() {
        let mut state = SlackFileSyncState::default();
        state.failed_files.insert("session:F1".to_string());

        let selection =
            select_file_sync_attempts("session", vec![file_ref("F1"), file_ref("F2")], &state, 2);

        assert!(selection.skipped_synced_files.is_empty());
        assert_eq!(selection.attempts.len(), 2);
        assert_eq!(selection.attempts[0].1.id, "F2");
        assert_eq!(selection.attempts[1].1.id, "F1");
    }

    #[tokio::test]
    async fn file_sync_limit_records_omitted_files_as_unresolved() {
        let mut cfg = test_slack_config(true);
        cfg.context_sync.current_thread = false;
        cfg.context_sync.linked_threads = false;
        cfg.context_sync.max_files_per_turn = 0;
        let channel = SlackSocketModeChannel::new(cfg, Arc::new(ApprovalBroker::default()));
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "C1".to_string(),
            user: "U1".to_string(),
            text: "<@BOT> see files".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
            files: vec![file_ref("F1"), file_ref("F2")],
        };

        let build = channel
            .current_thread_context_request(&event, "session", "BOT", &[], None)
            .await;

        assert!(build.succeeded_file_sync_keys.is_empty());
        assert!(build.failed_file_sync_keys.is_empty());
        assert_eq!(build.request.unresolved.len(), 2);
        assert_eq!(build.request.unresolved[0].kind, "file");
        assert_eq!(build.request.unresolved[0].reference, "F1");
        assert_eq!(
            build.request.unresolved[0].reason,
            "file sync limit reached"
        );
        assert_eq!(build.request.unresolved[1].reference, "F2");
    }

    #[tokio::test]
    async fn slack_context_resolver_returns_generic_resolve_result() {
        let mut cfg = test_slack_config(true);
        cfg.context_sync.current_thread = false;
        cfg.context_sync.linked_threads = false;
        cfg.context_sync.max_files_per_turn = 0;
        let channel = SlackSocketModeChannel::new(cfg, Arc::new(ApprovalBroker::default()));
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "C1".to_string(),
            user: "U1".to_string(),
            text: "<@BOT> see files".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
            files: vec![file_ref("F1")],
        };
        let resolver = SlackContextResolver::new(&channel, &event, "BOT", None);

        let result = resolver
            .resolve(ChannelContextResolveRequest {
                session_key: "session".to_string(),
                existing_artifacts: Vec::new(),
            })
            .await
            .unwrap();

        assert_eq!(result.sync_request.source, "slack");
        assert!(result.succeeded_cache_keys.is_empty());
        assert!(result.failed_cache_keys.is_empty());
        assert_eq!(result.sync_request.unresolved.len(), 1);
        assert_eq!(result.sync_request.unresolved[0].reference, "F1");
    }

    #[test]
    fn file_sync_state_uses_persisted_artifacts_and_unresolved_manifest() {
        let mut file_metadata = BTreeMap::new();
        file_metadata.insert("file_id".to_string(), json!("F1"));
        let mut manifest_metadata = BTreeMap::new();
        manifest_metadata.insert(
            "unresolved".to_string(),
            json!([
                {"kind": "file", "reference": "F2"},
                {"kind": "linked_thread", "reference": "C2:222.000"},
            ]),
        );
        let records = vec![
            ContextArtifactRecord {
                id: "slack:file:F1".to_string(),
                source: "slack".to_string(),
                kind: "slack_file".to_string(),
                title: "Slack file F1".to_string(),
                source_locator: None,
                paths: vec!["slack/files/F1/metadata.json".to_string()],
                fingerprint: "artifact:file".to_string(),
                updated_at_ms: 1,
                metadata: file_metadata,
            },
            ContextArtifactRecord {
                id: "slack:manifest".to_string(),
                source: "slack".to_string(),
                kind: "manifest".to_string(),
                title: "manifest".to_string(),
                source_locator: None,
                paths: vec!["slack/manifest.json".to_string()],
                fingerprint: "artifact:manifest".to_string(),
                updated_at_ms: 1,
                metadata: manifest_metadata,
            },
        ];

        let state = file_sync_state_from_context_artifacts(
            "session",
            &records,
            &SlackContextCache::default(),
        );
        let selection = select_file_sync_attempts(
            "session",
            vec![file_ref("F1"), file_ref("F2"), file_ref("F3")],
            &state,
            2,
        );

        assert_eq!(selection.skipped_synced_files.len(), 1);
        assert_eq!(selection.skipped_synced_files[0].id, "F1");
        assert_eq!(selection.attempts.len(), 2);
        assert_eq!(selection.attempts[0].1.id, "F3");
        assert_eq!(selection.attempts[1].1.id, "F2");
    }

    #[test]
    fn file_sync_state_does_not_trust_cache_without_artifact_record() {
        let mut cache = SlackContextCache::default();
        cache.synced_files.insert("session:F1".to_string());

        let state = file_sync_state_from_context_artifacts("session", &[], &cache);
        let selection = select_file_sync_attempts("session", vec![file_ref("F1")], &state, 1);

        assert!(selection.skipped_synced_files.is_empty());
        assert_eq!(selection.attempts.len(), 1);
        assert_eq!(selection.attempts[0].1.id, "F1");
    }

    #[test]
    fn parses_slack_thread_permalinks_from_mrkdwn() {
        let message = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "看 <https://smartx1.slack.com/archives/C6KFDTA49/p1782253434835649> 这个"
                .to_string(),
            files: Vec::new(),
        };

        let links = collect_slack_thread_links(&[message], "C1", "111.000");

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].channel, "C6KFDTA49");
        assert_eq!(links[0].thread_ts, "1782253434.835649");
        assert_eq!(links[0].source_message_ts, "111.000");
    }

    #[test]
    fn slack_thread_permalinks_reject_non_slack_hosts() {
        assert!(
            parse_slack_thread_permalink(
                "https://example.com/archives/C6KFDTA49/p1782253434835649"
            )
            .is_none()
        );
        assert!(
            parse_slack_thread_permalink(
                "http://smartx1.slack.com/archives/C6KFDTA49/p1782253434835649"
            )
            .is_none()
        );
    }

    #[test]
    fn current_event_text_participates_in_link_discovery() {
        let event =
            thread_reply("see <https://smartx1.slack.com/archives/C6KFDTA49/p1782253434835649>");

        let messages = context_link_messages(&event, &[]);
        let links = collect_slack_thread_links(&messages, &event.channel, &event.thread_root_ts());

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].channel, "C6KFDTA49");
        assert_eq!(links[0].source_message_ts, event.ts);
    }

    #[test]
    fn slack_reply_permalinks_use_thread_ts_query_as_root() {
        let parsed = parse_slack_thread_permalink(
            "https://smartx1.slack.com/archives/C6KFDTA49/p1782259999000000?thread_ts=1782253434.835649&cid=C6KFDTA49",
        )
        .unwrap();

        assert_eq!(parsed.0, "C6KFDTA49");
        assert_eq!(parsed.1, "1782253434.835649");
    }

    #[test]
    fn slack_file_urls_must_use_slack_hosts() {
        assert!(parse_slack_file_url("https://files.slack.com/files-pri/T1/F1/a.txt").is_ok());
        assert!(parse_slack_file_url("https://slack-files.com/T1-F1-a/download").is_ok());
        assert!(parse_slack_file_url("http://files.slack.com/files-pri/T1/F1/a.txt").is_err());
        assert!(parse_slack_file_url("https://example.com/files/F1").is_err());
    }

    #[test]
    fn access_denied_thread_errors_do_not_reuse_cached_context() {
        for code in [
            "not_in_channel",
            "channel_not_found",
            "missing_scope",
            "not_authed",
            "invalid_auth",
            "account_inactive",
            "token_revoked",
            "not_allowed_token_type",
            "token_expired",
            "team_access_not_granted",
            "access_denied",
            "no_permission",
            "forbidden_team",
        ] {
            let denied = anyhow::Error::new(SlackApiError::new("conversations.replies", code));
            assert!(!slack_error_allows_cached_context(&denied), "{code}");
            assert!(slack_error_clears_thread_cache(&denied), "{code}");
        }

        let rate_limited =
            anyhow::Error::new(SlackApiError::new("conversations.replies", "rate_limited"));

        assert!(slack_error_allows_cached_context(&rate_limited));
        assert!(!slack_error_clears_thread_cache(&rate_limited));

        let invalid_arguments = anyhow::Error::new(SlackApiError::new(
            "conversations.replies",
            "invalid_arguments",
        ));
        assert!(!slack_error_allows_cached_context(&invalid_arguments));
        assert!(!slack_error_clears_thread_cache(&invalid_arguments));
    }

    #[test]
    fn transient_thread_errors_only_reuse_current_cache_snapshot() {
        let rate_limited =
            anyhow::Error::new(SlackApiError::new("conversations.replies", "rate_limited"));
        let current_cache = vec![SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "cached".to_string(),
            files: Vec::new(),
        }];

        let mut cache_read = false;
        let fetch = cached_thread_fetch_after_error(&rate_limited, || {
            cache_read = true;
            current_cache
        })
        .unwrap();
        assert_eq!(fetch.messages.len(), 1);
        assert!(fetch.stale_reason.is_some());
        assert!(cache_read);
        assert!(cached_thread_fetch_after_error(&rate_limited, Vec::new).is_none());

        let denied = anyhow::Error::new(SlackApiError::new(
            "conversations.replies",
            "not_in_channel",
        ));
        let mut denied_cache_read = false;
        assert!(
            cached_thread_fetch_after_error(&denied, || {
                denied_cache_read = true;
                Vec::new()
            })
            .is_none()
        );
        assert!(!denied_cache_read);

        let invalid_arguments = anyhow::Error::new(SlackApiError::new(
            "conversations.replies",
            "invalid_arguments",
        ));
        let mut invalid_cache_read = false;
        assert!(
            cached_thread_fetch_after_error(&invalid_arguments, || {
                invalid_cache_read = true;
                Vec::new()
            })
            .is_none()
        );
        assert!(!invalid_cache_read);
    }

    #[tokio::test]
    async fn transient_thread_errors_do_not_use_cache_cleared_during_fetch() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let key = slack_thread_cache_key("C1", "111.000");
        channel.context_cache.lock().await.threads.insert(
            key.clone(),
            CachedSlackThreadMessages {
                messages: Some(vec![SlackThreadMessage {
                    ts: "111.000".to_string(),
                    user: Some("U1".to_string()),
                    author_name: None,
                    bot_id: None,
                    text: "stale".to_string(),
                    files: Vec::new(),
                }]),
                cache_sequence: None,
            },
        );
        let channel_for_fetch = channel.clone();
        let key_for_fetch = key.clone();

        let result = channel
            .fetch_thread_messages_cached_with(
                "C1",
                "111.000",
                async move {
                    channel_for_fetch
                        .context_cache
                        .lock()
                        .await
                        .threads
                        .remove(&key_for_fetch);
                    Err(SlackApiError::new("conversations.replies", "rate_limited").into())
                },
                None,
            )
            .await;

        assert!(result.is_err());
        assert!(
            !channel
                .context_cache
                .lock()
                .await
                .threads
                .contains_key(&key)
        );
    }

    #[tokio::test]
    async fn stale_context_cache_sequence_does_not_update_thread_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let key = slack_thread_cache_key("C1", "111.000");
        channel.context_cache.lock().await.threads.insert(
            key.clone(),
            CachedSlackThreadMessages {
                messages: Some(vec![SlackThreadMessage {
                    ts: "111.000".to_string(),
                    user: Some("U1".to_string()),
                    author_name: None,
                    bot_id: None,
                    text: "fresh context".to_string(),
                    files: Vec::new(),
                }]),
                cache_sequence: Some(2),
            },
        );
        let stale_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 1,
        };
        let message = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "stale context".to_string(),
            files: Vec::new(),
        };

        let result = channel
            .fetch_thread_messages_cached_with(
                "C1",
                "111.000",
                async move { Ok(vec![message]) },
                Some(&stale_turn),
            )
            .await
            .unwrap();

        assert_eq!(result.messages.len(), 1);
        let cache = channel.context_cache.lock().await;
        let cached = cache.threads.get(&key).unwrap();
        assert_eq!(cached.cache_sequence, Some(2));
        assert_eq!(cached.messages.as_ref().unwrap()[0].text, "fresh context");
    }

    #[tokio::test]
    async fn stale_context_cache_sequence_does_not_clear_thread_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let key = slack_thread_cache_key("C1", "111.000");
        channel.context_cache.lock().await.threads.insert(
            key.clone(),
            CachedSlackThreadMessages {
                messages: Some(vec![SlackThreadMessage {
                    ts: "111.000".to_string(),
                    user: Some("U1".to_string()),
                    author_name: None,
                    bot_id: None,
                    text: "fresh context".to_string(),
                    files: Vec::new(),
                }]),
                cache_sequence: Some(2),
            },
        );
        let stale_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 1,
        };

        let result =
            channel
                .fetch_thread_messages_cached_with(
                    "C1",
                    "111.000",
                    async move {
                        Err(SlackApiError::new("conversations.replies", "not_in_channel").into())
                    },
                    Some(&stale_turn),
                )
                .await;

        assert!(result.is_err());
        let cache = channel.context_cache.lock().await;
        let cached = cache.threads.get(&key).unwrap();
        assert_eq!(cached.cache_sequence, Some(2));
        assert!(cached.messages.is_some());
    }

    #[tokio::test]
    async fn newer_context_cache_removal_blocks_older_successful_fetch() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let key = slack_thread_cache_key("C1", "111.000");
        let newer_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 2,
        };
        let stale_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 1,
        };

        let removal =
            channel
                .fetch_thread_messages_cached_with(
                    "C1",
                    "111.000",
                    async move {
                        Err(SlackApiError::new("conversations.replies", "not_in_channel").into())
                    },
                    Some(&newer_turn),
                )
                .await;
        assert!(removal.is_err());

        let stale_message = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "stale context".to_string(),
            files: Vec::new(),
        };
        let result = channel
            .fetch_thread_messages_cached_with(
                "C1",
                "111.000",
                async move { Ok(vec![stale_message]) },
                Some(&stale_turn),
            )
            .await
            .unwrap();

        assert_eq!(result.messages.len(), 1);
        let cache = channel.context_cache.lock().await;
        let cached = cache.threads.get(&key).unwrap();
        assert_eq!(cached.cache_sequence, Some(2));
        assert!(cached.messages.is_none());
    }

    #[tokio::test]
    async fn stale_context_cache_sequence_does_not_update_file_sync_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        channel.remember_context_cache_sequence("session", 2).await;
        let stale_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 1,
        };
        let current_turn = SlackContextCacheToken {
            session_key: "session".to_string(),
            cache_sequence: 2,
        };

        channel
            .mark_file_sync_failed("session:F1".to_string(), Some(&stale_turn))
            .await;
        assert!(
            !channel
                .context_cache
                .lock()
                .await
                .failed_files
                .contains("session:F1")
        );

        channel
            .mark_file_sync_failed("session:F1".to_string(), Some(&current_turn))
            .await;
        assert!(
            channel
                .context_cache
                .lock()
                .await
                .failed_files
                .contains("session:F1")
        );
    }

    #[tokio::test]
    async fn unrelated_session_cache_sequence_does_not_block_file_sync_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        channel
            .remember_context_cache_sequence("session-b", 2)
            .await;
        let session_a_turn = SlackContextCacheToken {
            session_key: "session-a".to_string(),
            cache_sequence: 1,
        };

        channel
            .mark_file_sync_failed("session-a:F1".to_string(), Some(&session_a_turn))
            .await;

        assert!(
            channel
                .context_cache
                .lock()
                .await
                .failed_files
                .contains("session-a:F1")
        );
    }

    #[test]
    fn access_denied_thread_removals_target_prior_artifacts() {
        let current = slack_current_thread_removal("C1", "111.000");
        let linked = slack_linked_thread_removal("C2", "222.000");
        let all_linked = slack_all_linked_threads_removal();

        match current {
            ContextArtifactRemovalInput::Exact { kind, id } => {
                assert_eq!(id, "slack:thread:C1:111.000");
                assert_eq!(kind, "slack_current_thread");
            }
            ContextArtifactRemovalInput::ExceptKind { .. }
            | ContextArtifactRemovalInput::Kind { .. } => panic!("expected exact removal"),
        }
        match linked {
            ContextArtifactRemovalInput::Exact { kind, id } => {
                assert_eq!(id, "slack:linked-thread:C2:222.000");
                assert_eq!(kind, "slack_linked_thread");
            }
            ContextArtifactRemovalInput::ExceptKind { .. }
            | ContextArtifactRemovalInput::Kind { .. } => panic!("expected exact removal"),
        }
        match all_linked {
            ContextArtifactRemovalInput::Kind { kind } => {
                assert_eq!(kind, "slack_linked_thread");
            }
            ContextArtifactRemovalInput::Exact { .. }
            | ContextArtifactRemovalInput::ExceptKind { .. } => panic!("expected kind removal"),
        }
    }

    #[test]
    fn files_info_metadata_fills_missing_private_download_url() {
        let original = SlackFileRef {
            id: "F1".to_string(),
            name: "fallback.txt".to_string(),
            mimetype: None,
            size_bytes: 0,
            url_private: None,
            url_private_download: None,
        };
        let info = parse_slack_file_ref(&json!({
            "id": "F1",
            "name": "actual.txt",
            "mimetype": "text/plain",
            "size": 4,
            "url_private_download": "https://files.slack.com/files-pri/T1/F1/actual.txt"
        }))
        .unwrap();

        let merged = merge_slack_file_info(original, info);

        assert_eq!(merged.name, "actual.txt");
        assert_eq!(merged.mimetype.as_deref(), Some("text/plain"));
        assert_eq!(merged.size_bytes, 4);
        assert_eq!(
            merged.url_private_download.as_deref(),
            Some("https://files.slack.com/files-pri/T1/F1/actual.txt")
        );
    }

    #[test]
    fn file_ref_with_private_url_does_not_require_files_info() {
        let with_download_url = SlackFileRef {
            id: "F1".to_string(),
            name: "note.txt".to_string(),
            mimetype: None,
            size_bytes: 0,
            url_private: None,
            url_private_download: Some(
                "https://files.slack.com/files-pri/T1/F1/note.txt".to_string(),
            ),
        };
        let with_private_url = SlackFileRef {
            id: "F2".to_string(),
            name: "note.txt".to_string(),
            mimetype: None,
            size_bytes: 0,
            url_private: Some("https://files.slack.com/files-pri/T1/F2/note.txt".to_string()),
            url_private_download: None,
        };
        let without_url = SlackFileRef {
            id: "F3".to_string(),
            name: "note.txt".to_string(),
            mimetype: None,
            size_bytes: 0,
            url_private: None,
            url_private_download: None,
        };

        assert!(!slack_file_ref_needs_info(&with_download_url));
        assert!(!slack_file_ref_needs_info(&with_private_url));
        assert!(slack_file_ref_needs_info(&without_url));
    }

    #[test]
    fn context_filter_keeps_other_bot_messages() {
        let messages = vec![
            SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U_OTHER_BOT".to_string()),
                author_name: None,
                bot_id: Some("B_OTHER".to_string()),
                text: "root from another bot".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "112.000".to_string(),
                user: Some("BOT".to_string()),
                author_name: None,
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Activity\nTools: 1 step: Bash".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "113.000".to_string(),
                user: Some("BOT".to_string()),
                author_name: None,
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Progress\nI will inspect the config first.".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "114.000".to_string(),
                user: Some("BOT".to_string()),
                author_name: None,
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Progress report for the release".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "115.000".to_string(),
                user: Some("BOT".to_string()),
                author_name: None,
                bot_id: Some("B_SELF".to_string()),
                text: slack_reply_draft_text("partial answer"),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "116.000".to_string(),
                user: Some("U_OTHER_BOT".to_string()),
                author_name: None,
                bot_id: Some("B_OTHER".to_string()),
                text: slack_reply_draft_text("other bot draft-looking message"),
                files: Vec::new(),
            },
        ];

        let filtered = filter_context_messages(messages, "BOT");

        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[0].text, "root from another bot");
        assert_eq!(filtered[1].text, "[codex] Progress report for the release");
        assert_eq!(
            filtered[2].text,
            slack_reply_draft_text("other bot draft-looking message")
        );
    }

    #[test]
    fn slack_reply_draft_text_marks_preview_messages() {
        assert_eq!(
            slack_reply_draft_text("partial"),
            format!("partial\n\n{SLACK_REPLY_DRAFT_MARKER}")
        );
    }

    #[test]
    fn slack_reply_draft_text_truncates_long_preview_to_latest_tail() {
        let text = format!(
            "start-{}-tail",
            "x".repeat(SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES + 200)
        );

        let draft = slack_reply_draft_text(&text);

        assert!(!draft.contains("start-"));
        assert!(draft.contains("-tail"));
        assert!(draft.starts_with(SLACK_REPLY_DRAFT_TRUNCATED_PREFIX));
        assert!(draft.ends_with(SLACK_REPLY_DRAFT_MARKER));
        assert!(
            draft.len()
                <= SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES
                    + "\n\n".len()
                    + SLACK_REPLY_DRAFT_MARKER.len()
        );
    }

    #[test]
    fn slack_reply_draft_preview_truncates_on_utf8_boundary() {
        let text = format!(
            "prefix{}tail",
            "我".repeat(SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES)
        );

        let preview = slack_reply_draft_preview(&text);

        assert!(preview.starts_with(SLACK_REPLY_DRAFT_TRUNCATED_PREFIX));
        assert!(preview.ends_with("tail"));
        assert!(preview.len() <= SLACK_REPLY_DRAFT_PREVIEW_MAX_BYTES);
    }

    #[test]
    fn context_filter_keeps_own_non_noise_replies() {
        let messages = vec![SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("BOT".to_string()),
            author_name: None,
            bot_id: Some("B_SELF".to_string()),
            text: "普通回答应该保留".to_string(),
            files: Vec::new(),
        }];

        let filtered = filter_context_messages(messages, "BOT");

        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn parses_file_share_message_events() {
        let payload = json!({
            "event": {
                "type": "message",
                "subtype": "file_share",
                "bot_id": null,
                "user": "U1",
                "channel": "C1",
                "ts": "123.456",
                "text": "<@BOT> see file",
                "files": [{
                    "id": "F1",
                    "name": "note.txt",
                    "mimetype": "text/plain",
                    "size": 4,
                    "url_private_download": "https://files.example/F1"
                }]
            }
        });

        let event = parse_message_event(&payload, "BOT").unwrap();

        assert_eq!(event.files.len(), 1);
        assert_eq!(event.files[0].id, "F1");
    }

    #[test]
    fn ignores_bot_message_events_with_real_bot_id() {
        let payload = json!({
            "event": {
                "type": "message",
                "bot_id": "B1",
                "user": "U_BOT",
                "channel": "C1",
                "ts": "123.456",
                "text": "<@BOT> loop"
            }
        });

        assert!(parse_message_event(&payload, "BOT").is_none());
    }

    #[test]
    fn slack_context_request_includes_linked_thread_artifact() {
        let current = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "see <https://example.slack.com/archives/C2/p1782253434835649>".to_string(),
            files: Vec::new(),
        };
        let linked = LinkedSlackThread {
            link: SlackThreadLink {
                channel: "C2".to_string(),
                thread_ts: "1782253434.835649".to_string(),
                url: "https://example.slack.com/archives/C2/p1782253434835649".to_string(),
                source_message_ts: "111.000".to_string(),
            },
            fresh: true,
            messages: vec![SlackThreadMessage {
                ts: "1782253434.835649".to_string(),
                user: Some("U2".to_string()),
                author_name: None,
                bot_id: None,
                text: "linked context".to_string(),
                files: Vec::new(),
            }],
        };

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: true,
                messages: &[current],
            },
            &[linked],
            &[],
            SlackDerivedArtifactRetention::new(BTreeSet::new(), BTreeSet::new(), true),
            Vec::new(),
            Vec::new(),
        );

        assert!(
            request
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "slack_linked_thread")
        );
        let linked = request
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "slack_linked_thread")
            .unwrap();
        let paths = linked
            .files
            .iter()
            .map(|file| file.relative_path.display().to_string())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"slack/linked-threads/C2-1782253434.835649.md".to_string()));
        assert!(paths.contains(&"slack/linked-threads/C2-1782253434.835649.jsonl".to_string()));
        assert!(
            paths.contains(&"slack/linked-threads/C2-1782253434.835649.metadata.json".to_string())
        );
    }

    #[test]
    fn stale_thread_artifacts_do_not_resolve_cache_issues() {
        let current = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
            author_name: None,
            bot_id: None,
            text: "cached current".to_string(),
            files: Vec::new(),
        };
        let linked = LinkedSlackThread {
            link: SlackThreadLink {
                channel: "C2".to_string(),
                thread_ts: "222.000".to_string(),
                url: "https://example.slack.com/archives/C2/p222000000".to_string(),
                source_message_ts: "111.000".to_string(),
            },
            fresh: false,
            messages: vec![SlackThreadMessage {
                ts: "222.000".to_string(),
                user: Some("U2".to_string()),
                author_name: None,
                bot_id: None,
                text: "cached linked".to_string(),
                files: Vec::new(),
            }],
        };

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            CurrentSlackThreadContext {
                channel: "C1",
                thread_ts: "111.000",
                fresh: false,
                messages: &[current],
            },
            &[linked],
            &[],
            SlackDerivedArtifactRetention::new(BTreeSet::new(), BTreeSet::new(), true),
            Vec::new(),
            Vec::new(),
        );

        let current_resolves = request.artifacts[0]
            .metadata
            .get("resolves_unresolved")
            .and_then(Value::as_array)
            .unwrap();
        assert!(current_resolves.iter().any(|entry| {
            entry["kind"].as_str() == Some("current_thread")
                && entry["reference"].as_str() == Some("C1:111.000")
        }));
        assert!(!current_resolves.iter().any(|entry| {
            entry["kind"].as_str() == Some("current_thread_cache")
                && entry["reference"].as_str() == Some("C1:111.000")
        }));

        let linked_resolves = request
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "slack_linked_thread")
            .unwrap()
            .metadata
            .get("resolves_unresolved")
            .and_then(Value::as_array)
            .unwrap();
        assert!(linked_resolves.iter().any(|entry| {
            entry["kind"].as_str() == Some("linked_thread")
                && entry["reference"].as_str() == Some("C2:222.000")
        }));
        assert!(!linked_resolves.iter().any(|entry| {
            entry["kind"].as_str() == Some("linked_thread_cache")
                && entry["reference"].as_str() == Some("C2:222.000")
        }));
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
    async fn unmentioned_thread_slash_command_is_ignored() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_message_event(thread_reply("//status"), router_service, "BOT")
            .await
            .unwrap();

        assert!(router.handled.lock().await.is_empty());
        assert!(router.observed.lock().await.is_empty());
        assert!(router.reserved.lock().await.is_empty());
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
            files: Vec::new(),
        };

        channel
            .handle_message_event(event, router_service, "BOT")
            .await
            .unwrap();

        assert!(router.handled.lock().await.is_empty());
        assert!(router.observed.lock().await.is_empty());
    }

    #[tokio::test]
    async fn unmentioned_approval_text_without_pending_is_ignored() {
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
        assert!(router.observed.lock().await.is_empty());
        assert!(router.reserved.lock().await.is_empty());
    }

    #[tokio::test]
    async fn routed_approval_text_without_pending_does_not_preempt() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "/approve 1".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
            files: Vec::new(),
        };

        channel
            .handle_message_event(event, router_service, "BOT")
            .await
            .unwrap();

        assert!(router.reserved.lock().await.is_empty());
    }

    #[tokio::test]
    async fn routed_slash_command_does_not_preempt() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "/status".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
            files: Vec::new(),
        };

        channel
            .handle_message_event(event, router_service, "BOT")
            .await
            .unwrap();

        assert!(router.reserved.lock().await.is_empty());
        let handled = router.handled.lock().await;
        assert_eq!(handled.len(), 1);
        assert_eq!(handled[0].text, "/status");
    }

    #[tokio::test]
    async fn slack_adapter_uses_channel_intake_interface_for_routed_messages() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(IntakeOnlyRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();
        let event = SlackMessageEvent {
            event_key: "Ev1".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "hello".to_string(),
            ts: "111.000".to_string(),
            thread_ts: None,
            files: Vec::new(),
        };

        channel
            .handle_message_event(event, router_service, "BOT")
            .await
            .unwrap();

        let began = router.began.lock().await;
        assert_eq!(began.len(), 1);
        assert_eq!(began[0].intent, ChannelInputIntent::Route);
        drop(began);
        assert_eq!(*router.finished.lock().await, 1);
    }

    #[tokio::test]
    async fn slack_app_entry_slash_command_routes_payload_text() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_slash_command(
                SlackSlashCommand {
                    command: "/hermes".to_string(),
                    text: "//status --json".to_string(),
                    channel_id: "C1".to_string(),
                    user_id: "U1".to_string(),
                },
                router_service,
            )
            .await
            .unwrap();

        let handled = router.handled.lock().await;
        assert_eq!(handled.len(), 1);
        assert_eq!(handled[0].session_key, "slack:C1:slash:U1");
        assert_eq!(handled[0].text, "//status --json");
        assert_eq!(handled[0].user_id.as_deref(), Some("U1"));
    }

    #[tokio::test]
    async fn router_owned_slack_platform_slash_command_routes_command_name() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let router_service: Arc<dyn RouterService> = router.clone();

        channel
            .handle_slash_command(
                SlackSlashCommand {
                    command: "/agent".to_string(),
                    text: "status".to_string(),
                    channel_id: "C1".to_string(),
                    user_id: "U1".to_string(),
                },
                router_service,
            )
            .await
            .unwrap();

        let handled = router.handled.lock().await;
        assert_eq!(handled.len(), 1);
        assert_eq!(handled[0].session_key, "slack:C1:slash:U1");
        assert_eq!(handled[0].text, "/agent status");
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
        let router = Arc::new(RecordingRouter {
            pending_approval: true,
            ..Default::default()
        });
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
            files: Vec::new(),
        };
        let second = SlackMessageEvent {
            event_key: "Ev2".to_string(),
            channel: "D1".to_string(),
            user: "U1".to_string(),
            text: "second".to_string(),
            ts: "222.000".to_string(),
            thread_ts: None,
            files: Vec::new(),
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
            files: Vec::new(),
        };

        assert_eq!(reply.session_key(), "slack:dm:D1:111.000");
    }
}
