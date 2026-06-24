use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url};
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
    context_cache: Arc<Mutex<SlackContextCache>>,
    session_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
}

impl SlackSocketModeChannel {
    pub fn new(cfg: SlackConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            seen_events: Arc::new(Mutex::new(EventDeduper::new(512))),
            context_cache: Arc::new(Mutex::new(SlackContextCache::default())),
            session_locks: Arc::new(Mutex::new(BTreeMap::new())),
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
        let session_lock = self.session_lock(&session_key).await;
        let _session_guard = session_lock.lock().await;
        tracing::info!(
            channel = %event.channel,
            user_id = %event.user,
            session_key = %session_key,
            text_len = text.len(),
            "routing Slack message"
        );
        let context = if self.should_sync_context(&text) {
            Some(
                self.current_thread_context_request(&event, &session_key, bot_user_id)
                    .await,
            )
        } else {
            None
        };
        let completed_file_sync_keys = context
            .as_ref()
            .map(|context| context.completed_file_sync_keys.clone())
            .unwrap_or_default();
        let reply_target = event.reply_target();
        let mut output = SlackRouterOutputSink {
            channel: self.clone(),
            target: reply_target,
            channel_events: self.cfg.channel_events,
            compact_activity: SlackCompactActivity::default(),
        };
        router
            .handle_with_context(
                RouterInput {
                    session_key,
                    text,
                    user_id: Some(event.user),
                },
                context.map(|context| context.request),
                &mut output,
            )
            .await?;
        for cache_key in completed_file_sync_keys {
            self.mark_file_sync_completed(cache_key).await;
        }
        Ok(())
    }

    async fn session_lock(&self, session_key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.session_locks.lock().await;
        locks
            .entry(session_key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
            && (self.cfg.context_sync.current_thread
                || self.cfg.context_sync.linked_threads
                || self.cfg.context_sync.files)
            && !is_approval_command(text)
    }

    async fn current_thread_context_request(
        &self,
        event: &SlackMessageEvent,
        session_key: &str,
        bot_user_id: &str,
    ) -> SlackContextSyncBuild {
        let thread_ts = event.thread_root_ts();
        let mut unresolved = Vec::new();
        let messages = if self.cfg.context_sync.current_thread {
            match self
                .fetch_thread_messages_cached(&event.channel, &thread_ts)
                .await
            {
                Ok(fetch) => {
                    if let Some(reason) = fetch.stale_reason {
                        unresolved.push(ContextSyncIssueInput {
                            kind: "current_thread_cache".to_string(),
                            reference: format!("{}:{}", event.channel, thread_ts),
                            reason,
                        });
                    }
                    filter_context_messages(fetch.messages, bot_user_id)
                }
                Err(err) => {
                    unresolved.push(ContextSyncIssueInput {
                        kind: "current_thread".to_string(),
                        reference: format!("{}:{}", event.channel, thread_ts),
                        reason: err.to_string(),
                    });
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            message_count = messages.len(),
            "synced Slack current thread context"
        );

        let mut linked_threads = Vec::new();
        if self.cfg.context_sync.linked_threads && self.cfg.context_sync.linked_thread_depth > 0 {
            for link in collect_slack_thread_links(&messages, &event.channel, &thread_ts)
                .into_iter()
                .take(self.cfg.context_sync.max_linked_threads_per_turn)
            {
                match self
                    .fetch_thread_messages_cached(&link.channel, &link.thread_ts)
                    .await
                {
                    Ok(fetch) => {
                        if let Some(reason) = fetch.stale_reason {
                            unresolved.push(ContextSyncIssueInput {
                                kind: "linked_thread_cache".to_string(),
                                reference: link.url.clone(),
                                reason,
                            });
                        }
                        linked_threads.push(LinkedSlackThread {
                            link,
                            messages: filter_context_messages(fetch.messages, bot_user_id),
                        });
                    }
                    Err(err) => unresolved.push(ContextSyncIssueInput {
                        kind: "linked_thread".to_string(),
                        reference: link.url,
                        reason: err.to_string(),
                    }),
                }
            }
        }
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            linked_thread_count = linked_threads.len(),
            "synced Slack linked thread context"
        );

        let mut downloaded_files = Vec::new();
        let mut completed_file_sync_keys = Vec::new();
        let mut skipped_cached_files = 0usize;
        let mut failed_files = 0usize;
        let file_refs = if self.cfg.context_sync.files {
            collect_context_file_refs(&event.files, &messages, &linked_threads)
        } else {
            Vec::new()
        };
        let (file_attempts, skipped_completed_files) = select_file_sync_attempts(
            session_key,
            file_refs,
            &self.completed_file_syncs().await,
            self.cfg.context_sync.max_files_per_turn,
        );
        skipped_cached_files += skipped_completed_files;
        for (file_cache_key, file) in file_attempts {
            match self.download_slack_file(file.clone()).await {
                Ok(downloaded) => {
                    completed_file_sync_keys.push(file_cache_key);
                    downloaded_files.push(downloaded);
                }
                Err(err) => {
                    failed_files += 1;
                    completed_file_sync_keys.push(file_cache_key);
                    unresolved.push(ContextSyncIssueInput {
                        kind: "file".to_string(),
                        reference: file.id,
                        reason: context_error_reason(&err),
                    });
                }
            }
        }
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            downloaded_file_count = downloaded_files.len(),
            skipped_cached_file_count = skipped_cached_files,
            failed_file_count = failed_files,
            "synced Slack file context"
        );

        SlackContextSyncBuild {
            request: build_slack_context_request(
                session_key,
                &event.channel,
                &thread_ts,
                &messages,
                &linked_threads,
                &downloaded_files,
                unresolved,
            ),
            completed_file_sync_keys,
        }
    }

    async fn fetch_thread_messages_cached(
        &self,
        channel: &str,
        thread_ts: &str,
    ) -> anyhow::Result<SlackThreadFetch> {
        let key = slack_thread_cache_key(channel, thread_ts);
        let cached = {
            let cache = self.context_cache.lock().await;
            cache.threads.get(&key).cloned().unwrap_or_default()
        };
        match self.fetch_thread_messages(channel, thread_ts).await {
            Ok(messages) => {
                self.context_cache
                    .lock()
                    .await
                    .threads
                    .insert(key, messages.clone());
                Ok(SlackThreadFetch {
                    messages,
                    stale_reason: None,
                })
            }
            Err(err) if !cached.is_empty() => Ok(SlackThreadFetch {
                messages: cached,
                stale_reason: Some(context_error_reason(&err)),
            }),
            Err(err) => Err(err),
        }
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
            .ok_or_else(|| anyhow::anyhow!("file has no private download URL"))
            .and_then(parse_slack_file_url)?;
        let mut response = self
            .http
            .get(url)
            .bearer_auth(&self.cfg.bot_token)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Slack file download request failed"))?;
        if !response.status().is_success() {
            anyhow::bail!("Slack file download failed: {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.cfg.context_sync.max_file_bytes as u64)
        {
            anyhow::bail!(
                "file exceeds max_file_bytes (content-length > {})",
                self.cfg.context_sync.max_file_bytes
            );
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("Slack file download read failed"))?
        {
            if bytes.len() + chunk.len() > self.cfg.context_sync.max_file_bytes {
                anyhow::bail!(
                    "file exceeds max_file_bytes (downloaded > {})",
                    self.cfg.context_sync.max_file_bytes
                );
            }
            bytes.extend_from_slice(&chunk);
        }
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

    async fn completed_file_syncs(&self) -> BTreeSet<String> {
        self.context_cache.lock().await.completed_file_syncs.clone()
    }

    async fn mark_file_sync_completed(&self, cache_key: String) {
        self.context_cache
            .lock()
            .await
            .completed_file_syncs
            .insert(cache_key);
    }
}

#[derive(Debug, Default)]
struct SlackContextCache {
    threads: BTreeMap<String, Vec<SlackThreadMessage>>,
    completed_file_syncs: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct SlackContextSyncBuild {
    request: ContextSyncRequest,
    completed_file_sync_keys: Vec<String>,
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
struct SlackThreadLink {
    channel: String,
    thread_ts: String,
    url: String,
    source_message_ts: String,
}

#[derive(Debug, Clone)]
struct LinkedSlackThread {
    link: SlackThreadLink,
    messages: Vec<SlackThreadMessage>,
}

#[derive(Debug, Clone)]
struct SlackThreadFetch {
    messages: Vec<SlackThreadMessage>,
    stale_reason: Option<String>,
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

fn collect_context_file_refs(
    event_files: &[SlackFileRef],
    current_messages: &[SlackThreadMessage],
    linked_threads: &[LinkedSlackThread],
) -> Vec<SlackFileRef> {
    let mut seen = BTreeSet::new();
    let mut files = Vec::new();
    for file in event_files
        .iter()
        .chain(
            current_messages
                .iter()
                .flat_map(|message| message.files.iter()),
        )
        .chain(
            linked_threads
                .iter()
                .flat_map(|thread| thread.messages.iter())
                .flat_map(|message| message.files.iter()),
        )
    {
        if seen.insert(file.id.clone()) {
            files.push(file.clone());
        }
    }
    files
}

fn select_file_sync_attempts(
    session_key: &str,
    file_refs: Vec<SlackFileRef>,
    completed_file_syncs: &BTreeSet<String>,
    max_files_per_turn: usize,
) -> (Vec<(String, SlackFileRef)>, usize) {
    let mut skipped_completed_files = 0usize;
    let mut attempts = Vec::new();
    for file in file_refs {
        let file_cache_key = format!("{session_key}:{}", file.id);
        if completed_file_syncs.contains(&file_cache_key) {
            skipped_completed_files += 1;
            continue;
        }
        if attempts.len() >= max_files_per_turn {
            break;
        }
        attempts.push((file_cache_key, file));
    }
    (attempts, skipped_completed_files)
}

fn collect_slack_thread_links(
    messages: &[SlackThreadMessage],
    current_channel: &str,
    current_thread_ts: &str,
) -> Vec<SlackThreadLink> {
    let mut seen = BTreeSet::new();
    let mut links = Vec::new();
    for message in messages {
        for url in extract_slack_urls(&message.text) {
            if let Some((channel, thread_ts)) = parse_slack_thread_permalink(&url) {
                if channel == current_channel && thread_ts == current_thread_ts {
                    continue;
                }
                let key = format!("{channel}:{thread_ts}");
                if !seen.insert(key) {
                    continue;
                }
                links.push(SlackThreadLink {
                    channel,
                    thread_ts,
                    url,
                    source_message_ts: message.ts.clone(),
                });
            }
        }
    }
    links
}

fn extract_slack_urls(text: &str) -> Vec<String> {
    text.split(|ch: char| ch.is_whitespace() || matches!(ch, '<' | '>' | '|'))
        .filter(|part| part.contains("/archives/"))
        .map(|part| {
            part.trim_matches(|ch| matches!(ch, ',' | ')' | ']'))
                .to_string()
        })
        .filter(|part| part.starts_with("http://") || part.starts_with("https://"))
        .collect()
}

fn parse_slack_thread_permalink(url: &str) -> Option<(String, String)> {
    let url = Url::parse(url).ok()?;
    let segments = url.path_segments()?.collect::<Vec<_>>();
    let archive_index = segments.iter().position(|segment| *segment == "archives")?;
    let channel = segments.get(archive_index + 1)?.to_string();
    let path_ts = segments.get(archive_index + 2)?;
    if channel.is_empty() {
        return None;
    }
    let thread_ts = url
        .query_pairs()
        .find_map(|(key, value)| {
            (key == "thread_ts")
                .then(|| parse_slack_timestamp(&value))
                .flatten()
        })
        .or_else(|| parse_slack_timestamp(path_ts))?;
    Some((channel, thread_ts))
}

fn parse_slack_timestamp(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if let Some((seconds, micros)) = raw.split_once('.') {
        if !seconds.is_empty()
            && seconds.chars().all(|ch| ch.is_ascii_digit())
            && micros.len() == 6
            && micros.chars().all(|ch| ch.is_ascii_digit())
        {
            return Some(raw.to_string());
        }
        return None;
    }
    let timestamp = raw
        .strip_prefix('p')
        .unwrap_or(raw)
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if timestamp.len() <= 6 {
        return None;
    }
    let (seconds, micros) = timestamp.split_at(timestamp.len() - 6);
    Some(format!("{seconds}.{micros}"))
}

fn parse_slack_file_url(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("Slack file URL is invalid"))?;
    anyhow::ensure!(url.scheme() == "https", "Slack file URL must use https");
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Slack file URL has no host"))?;
    anyhow::ensure!(
        is_allowed_slack_file_host(host),
        "Slack file URL host is not allowed"
    );
    Ok(url)
}

fn is_allowed_slack_file_host(host: &str) -> bool {
    host == "slack.com"
        || host.ends_with(".slack.com")
        || host == "slack-files.com"
        || host.ends_with(".slack-files.com")
}

fn filter_context_messages(
    messages: Vec<SlackThreadMessage>,
    bot_user_id: &str,
) -> Vec<SlackThreadMessage> {
    messages
        .into_iter()
        .filter(|message| {
            !(message.user.as_deref() == Some(bot_user_id)
                && is_router_noise_message(&message.text))
        })
        .collect()
}

fn is_router_noise_message(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.starts_with("Approval required:")
        || trimmed.starts_with("Auto-approved in YOLO mode:")
        || trimmed.starts_with("Approved ")
        || trimmed.starts_with("Denied ")
    {
        return true;
    }
    let Some(rest) = trimmed.strip_prefix('[') else {
        return false;
    };
    let Some((_, rest)) = rest.split_once("] ") else {
        return false;
    };
    rest.starts_with("Activity")
        || rest.starts_with("Tool call")
        || rest.starts_with("Reasoning summary")
}

fn slack_thread_cache_key(channel: &str, thread_ts: &str) -> String {
    format!("{channel}:{thread_ts}")
}

fn context_error_reason(err: &anyhow::Error) -> String {
    let raw = err.to_string();
    if raw.contains("url_private") || raw.contains("slack-files.com") || raw.contains("http") {
        "Slack context sync request failed".to_string()
    } else {
        raw
    }
}

fn build_slack_context_request(
    session_key: &str,
    channel: &str,
    thread_ts: &str,
    messages: &[SlackThreadMessage],
    linked_threads: &[LinkedSlackThread],
    downloaded_files: &[DownloadedSlackFile],
    unresolved: Vec<ContextSyncIssueInput>,
) -> ContextSyncRequest {
    let mut artifacts = Vec::new();
    if !messages.is_empty() {
        let mut metadata = BTreeMap::new();
        let thread_ref = format!("{channel}:{thread_ts}");
        metadata.insert("channel".to_string(), json!(channel));
        metadata.insert("thread_ts".to_string(), json!(thread_ts));
        metadata.insert("message_count".to_string(), json!(messages.len()));
        metadata.insert(
            "resolves_unresolved".to_string(),
            json!([
                {"kind": "current_thread", "reference": &thread_ref},
                {"kind": "current_thread_cache", "reference": &thread_ref},
            ]),
        );
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

    for linked_thread in linked_threads {
        if linked_thread.messages.is_empty() {
            continue;
        }
        artifacts.push(linked_thread_artifact(linked_thread));
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

fn linked_thread_artifact(linked_thread: &LinkedSlackThread) -> ContextArtifactInput {
    let channel = &linked_thread.link.channel;
    let thread_ts = &linked_thread.link.thread_ts;
    let safe_channel = sanitize_path_segment(channel);
    let safe_ts = sanitize_path_segment(thread_ts);
    let base_name = format!("{safe_channel}-{safe_ts}");
    let base_dir = PathBuf::from("slack").join("linked-threads");
    let mut metadata = BTreeMap::new();
    metadata.insert("channel".to_string(), json!(channel));
    metadata.insert("thread_ts".to_string(), json!(thread_ts));
    metadata.insert(
        "resolves_unresolved".to_string(),
        json!([
            {"kind": "linked_thread", "reference": &linked_thread.link.url},
            {"kind": "linked_thread_cache", "reference": &linked_thread.link.url},
        ]),
    );
    metadata.insert(
        "source_message_ts".to_string(),
        json!(linked_thread.link.source_message_ts),
    );
    metadata.insert("url".to_string(), json!(linked_thread.link.url));
    metadata.insert(
        "message_count".to_string(),
        json!(linked_thread.messages.len()),
    );
    ContextArtifactInput {
        id: format!("slack:linked-thread:{channel}:{thread_ts}"),
        kind: "slack_linked_thread".to_string(),
        title: format!("Linked Slack thread {channel} {thread_ts}"),
        source_locator: Some(format!("slack://{channel}/{thread_ts}")),
        files: vec![
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.md")),
                content: ContextFileContent::Text(render_slack_thread_markdown(
                    channel,
                    thread_ts,
                    &linked_thread.messages,
                )),
            },
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.jsonl")),
                content: ContextFileContent::Text(render_slack_thread_jsonl(
                    &linked_thread.messages,
                )),
            },
            ContextFileInput {
                relative_path: base_dir.join(format!("{base_name}.metadata.json")),
                content: ContextFileContent::Text(
                    serde_json::to_string_pretty(&metadata).unwrap_or_else(|_| "{}".to_string()),
                ),
            },
        ],
        metadata,
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
    metadata.insert(
        "resolves_unresolved".to_string(),
        json!([{"kind": "file", "reference": &downloaded.file.id}]),
    );
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
            &[],
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

        let files = collect_context_file_refs(&[], &[first, second], &[]);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "F1");
    }

    #[test]
    fn completed_file_syncs_do_not_count_against_turn_limit() {
        let mut completed = BTreeSet::new();
        completed.insert("session:F1".to_string());

        let (attempts, skipped) = select_file_sync_attempts(
            "session",
            vec![file_ref("F1"), file_ref("F2")],
            &completed,
            1,
        );

        assert_eq!(skipped, 1);
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].0, "session:F2");
        assert_eq!(attempts[0].1.id, "F2");
    }

    #[test]
    fn parses_slack_thread_permalinks_from_mrkdwn() {
        let message = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
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

    #[tokio::test]
    async fn slack_session_locks_are_scoped_by_session_key() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );

        let first = channel.session_lock("slack:channel:C1:111.000").await;
        let same = channel.session_lock("slack:channel:C1:111.000").await;
        let other = channel.session_lock("slack:channel:C1:222.000").await;

        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn context_filter_keeps_other_bot_messages() {
        let messages = vec![
            SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U_OTHER_BOT".to_string()),
                bot_id: Some("B_OTHER".to_string()),
                text: "root from another bot".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "112.000".to_string(),
                user: Some("BOT".to_string()),
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Activity\nTools: 1 step: Bash".to_string(),
                files: Vec::new(),
            },
        ];

        let filtered = filter_context_messages(messages, "BOT");

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].text, "root from another bot");
    }

    #[test]
    fn context_filter_keeps_own_non_noise_replies() {
        let messages = vec![SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("BOT".to_string()),
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
            messages: vec![SlackThreadMessage {
                ts: "1782253434.835649".to_string(),
                user: Some("U2".to_string()),
                bot_id: None,
                text: "linked context".to_string(),
                files: Vec::new(),
            }],
        };

        let request = build_slack_context_request(
            "slack:channel:C1:111.000",
            "C1",
            "111.000",
            &[current],
            &[linked],
            &[],
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
