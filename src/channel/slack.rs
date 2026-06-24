use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};
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
        ContextArtifactInput, ContextArtifactRecord, ContextArtifactRemovalInput,
        ContextFileContent, ContextFileInput, ContextSyncIssueInput, ContextSyncRequest,
        sanitize_path_segment,
    },
};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct SlackSocketModeChannel {
    cfg: SlackConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    seen_events: Arc<Mutex<EventDeduper>>,
    context_cache: Arc<Mutex<SlackContextCache>>,
    context_generations: Arc<Mutex<BTreeMap<String, u64>>>,
    next_context_generation: Arc<AtomicU64>,
}

impl SlackSocketModeChannel {
    pub fn new(cfg: SlackConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            seen_events: Arc::new(Mutex::new(EventDeduper::new(512))),
            context_cache: Arc::new(Mutex::new(SlackContextCache::default())),
            context_generations: Arc::new(Mutex::new(BTreeMap::new())),
            next_context_generation: Arc::new(AtomicU64::new(1)),
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
        if approval_trigger {
            tracing::info!(
                channel = %event.channel,
                user_id = %event.user,
                session_key = %session_key,
                "routing Slack approval command"
            );
            let reply_target = event.reply_target();
            let mut output =
                SlackRouterOutputSink::new(self.clone(), reply_target, self.cfg.channel_events);
            router
                .handle_with_context(
                    RouterInput {
                        session_key,
                        text,
                        user_id: Some(event.user),
                    },
                    None,
                    &mut output,
                )
                .await?;
            return Ok(());
        }
        let command = text.split_whitespace().next().unwrap_or("");
        let mut context_generation = None;
        let preempt_generation =
            if approval_command || matches!(command, "/stop" | "/agent" | "/yolo") {
                None
            } else {
                let generation = self.next_context_generation.fetch_add(1, Ordering::Relaxed);
                self.remember_context_generation(&session_key, generation)
                    .await;
                context_generation = Some(generation);
                Some(router.preempt(&session_key).await?)
            };
        let context_turn = context_generation.map(|generation| SlackContextTurn {
            session_key: session_key.clone(),
            generation,
        });
        tracing::info!(
            channel = %event.channel,
            user_id = %event.user,
            session_key = %session_key,
            text_len = text.len(),
            "routing Slack message"
        );
        let context = if self.should_sync_context(&text) {
            let existing_context = router.context_artifacts(&session_key, "slack").await?;
            Some(
                self.current_thread_context_request(
                    &event,
                    &session_key,
                    bot_user_id,
                    &existing_context,
                    context_turn.as_ref(),
                )
                .await,
            )
        } else {
            None
        };
        let succeeded_file_sync_keys = context
            .as_ref()
            .map(|context| context.succeeded_file_sync_keys.clone())
            .unwrap_or_default();
        let failed_file_sync_keys = context
            .as_ref()
            .map(|context| context.failed_file_sync_keys.clone())
            .unwrap_or_default();
        let reply_target = event.reply_target();
        let mut output =
            SlackRouterOutputSink::new(self.clone(), reply_target, self.cfg.channel_events);
        if let Some(preempt_generation) = preempt_generation {
            router
                .handle_preempted_with_context(
                    RouterInput {
                        session_key,
                        text,
                        user_id: Some(event.user),
                    },
                    preempt_generation,
                    context.map(|context| context.request),
                    &mut output,
                )
                .await?;
        } else {
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
        }
        for cache_key in succeeded_file_sync_keys {
            self.mark_file_sync_succeeded(cache_key).await;
        }
        for cache_key in failed_file_sync_keys {
            self.mark_file_sync_failed(cache_key).await;
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
        let mut output =
            SlackRouterOutputSink::new(self.clone(), reply_target, self.cfg.channel_events);
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

    fn should_accept_linked_thread_channel(
        &self,
        current_channel: &str,
        linked_channel: &str,
    ) -> bool {
        linked_channel == current_channel
            || (!self.cfg.allowed_channels.is_empty()
                && self.cfg.allowed_channels.contains(linked_channel))
    }

    fn should_sync_context(&self, text: &str) -> bool {
        let command = text.split_whitespace().next().unwrap_or("");
        self.cfg.context_sync.enabled
            && !self.cfg.bot_token.is_empty()
            && (self.cfg.context_sync.current_thread
                || self.cfg.context_sync.linked_threads
                || self.cfg.context_sync.files)
            && !is_approval_command(text)
            && !matches!(command, "/stop" | "/agent" | "/yolo")
    }

    async fn remember_context_generation(&self, session_key: &str, generation: u64) {
        self.context_generations
            .lock()
            .await
            .insert(session_key.to_string(), generation);
    }

    async fn context_generation_is_current(&self, turn: Option<&SlackContextTurn>) -> bool {
        let Some(turn) = turn else {
            return true;
        };
        self.context_generations
            .lock()
            .await
            .get(&turn.session_key)
            .is_some_and(|generation| *generation == turn.generation)
    }

    async fn current_thread_context_request(
        &self,
        event: &SlackMessageEvent,
        session_key: &str,
        bot_user_id: &str,
        existing_context: &[ContextArtifactRecord],
        context_turn: Option<&SlackContextTurn>,
    ) -> SlackContextSyncBuild {
        let thread_ts = event.thread_root_ts();
        let mut unresolved = Vec::new();
        let mut remove_artifacts = Vec::new();
        let mut current_thread_fresh = false;
        let mut prune_derived_artifacts = false;
        let messages = if self.cfg.context_sync.current_thread {
            tracing::info!(
                channel = %event.channel,
                thread_ts = %thread_ts,
                "fetching Slack current thread context"
            );
            match self
                .fetch_thread_messages_cached(&event.channel, &thread_ts, context_turn)
                .await
            {
                Ok(fetch) => {
                    prune_derived_artifacts = true;
                    let fresh = fetch.stale_reason.is_none();
                    if let Some(reason) = fetch.stale_reason {
                        tracing::info!(
                            channel = %event.channel,
                            thread_ts = %thread_ts,
                            reason = %reason,
                            "using cached Slack current thread context"
                        );
                        unresolved.push(ContextSyncIssueInput {
                            kind: "current_thread_cache".to_string(),
                            reference: format!("{}:{}", event.channel, thread_ts),
                            reason,
                        });
                    } else {
                        current_thread_fresh = true;
                    }
                    let messages = filter_context_messages(fetch.messages, bot_user_id);
                    tracing::info!(
                        channel = %event.channel,
                        thread_ts = %thread_ts,
                        message_count = messages.len(),
                        fresh,
                        "fetched Slack current thread context"
                    );
                    messages
                }
                Err(err) => {
                    if slack_error_is_access_denied(&err) {
                        prune_derived_artifacts = true;
                        remove_artifacts
                            .push(slack_current_thread_removal(&event.channel, &thread_ts));
                        remove_artifacts.push(slack_all_linked_threads_removal());
                    }
                    tracing::info!(
                        channel = %event.channel,
                        thread_ts = %thread_ts,
                        reason = %context_error_reason(&err),
                        "failed to fetch Slack current thread context"
                    );
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
        let mut retained_linked_thread_artifact_ids = BTreeSet::new();
        if self.cfg.context_sync.linked_threads && self.cfg.context_sync.linked_thread_depth > 0 {
            let link_messages = context_link_messages(event, &messages);
            let mut linked_thread_fetches = 0usize;
            for link in collect_slack_thread_links(&link_messages, &event.channel, &thread_ts) {
                let linked_artifact_id =
                    slack_linked_thread_artifact_id(&link.channel, &link.thread_ts);
                if !self.should_accept_linked_thread_channel(&event.channel, &link.channel) {
                    tracing::info!(
                        linked_channel = %link.channel,
                        linked_thread_ts = %link.thread_ts,
                        source_message_ts = %link.source_message_ts,
                        "skipping Slack linked thread outside allowed channels"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: "linked_thread".to_string(),
                        reference: slack_thread_reference(&link.channel, &link.thread_ts),
                        reason: "linked thread channel is not allowed".to_string(),
                    });
                    continue;
                }
                if linked_thread_fetches >= self.cfg.context_sync.max_linked_threads_per_turn {
                    tracing::info!(
                        linked_channel = %link.channel,
                        linked_thread_ts = %link.thread_ts,
                        source_message_ts = %link.source_message_ts,
                        "skipping Slack linked thread due fetch limit"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: "linked_thread".to_string(),
                        reference: slack_thread_reference(&link.channel, &link.thread_ts),
                        reason: "linked thread fetch limit reached".to_string(),
                    });
                    retained_linked_thread_artifact_ids.insert(linked_artifact_id);
                    continue;
                }
                linked_thread_fetches += 1;
                tracing::info!(
                    linked_channel = %link.channel,
                    linked_thread_ts = %link.thread_ts,
                    source_message_ts = %link.source_message_ts,
                    "fetching Slack linked thread context"
                );
                match self
                    .fetch_thread_messages_cached(&link.channel, &link.thread_ts, context_turn)
                    .await
                {
                    Ok(fetch) => {
                        let fresh = fetch.stale_reason.is_none();
                        if let Some(reason) = fetch.stale_reason {
                            tracing::info!(
                                linked_channel = %link.channel,
                                linked_thread_ts = %link.thread_ts,
                                reason = %reason,
                                "using cached Slack linked thread context"
                            );
                            unresolved.push(ContextSyncIssueInput {
                                kind: "linked_thread_cache".to_string(),
                                reference: slack_thread_reference(&link.channel, &link.thread_ts),
                                reason,
                            });
                        }
                        linked_threads.push(LinkedSlackThread {
                            link,
                            fresh,
                            messages: filter_context_messages(fetch.messages, bot_user_id),
                        });
                        if let Some(linked_thread) = linked_threads.last() {
                            tracing::info!(
                                linked_channel = %linked_thread.link.channel,
                                linked_thread_ts = %linked_thread.link.thread_ts,
                                message_count = linked_thread.messages.len(),
                                fresh = linked_thread.fresh,
                                "fetched Slack linked thread context"
                            );
                        }
                    }
                    Err(err) => {
                        if slack_error_is_access_denied(&err) {
                            remove_artifacts
                                .push(slack_linked_thread_removal(&link.channel, &link.thread_ts));
                        } else {
                            retained_linked_thread_artifact_ids.insert(linked_artifact_id);
                        }
                        tracing::info!(
                            linked_channel = %link.channel,
                            linked_thread_ts = %link.thread_ts,
                            reason = %context_error_reason(&err),
                            "failed to fetch Slack linked thread context"
                        );
                        unresolved.push(ContextSyncIssueInput {
                            kind: "linked_thread".to_string(),
                            reference: slack_thread_reference(&link.channel, &link.thread_ts),
                            reason: err.to_string(),
                        });
                    }
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
        let mut succeeded_file_sync_keys = Vec::new();
        let mut failed_file_sync_keys = Vec::new();
        let mut skipped_cached_files = 0usize;
        let mut failed_files = 0usize;
        let file_refs = if self.cfg.context_sync.files {
            collect_context_file_refs(&event.files, &messages, &linked_threads)
        } else {
            Vec::new()
        };
        let file_selection = select_file_sync_attempts(
            session_key,
            file_refs,
            &self.file_sync_state(session_key, existing_context).await,
            self.cfg.context_sync.max_files_per_turn,
        );
        let mut retained_file_artifact_ids = BTreeSet::new();
        skipped_cached_files += file_selection.skipped_synced_files.len();
        for file in &file_selection.skipped_synced_files {
            retained_file_artifact_ids.insert(slack_file_artifact_id(&file.id));
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                "skipping already synced Slack file context"
            );
        }
        let omitted_file_count = file_selection.omitted_files.len();
        for file in &file_selection.omitted_files {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                max_files_per_turn = self.cfg.context_sync.max_files_per_turn,
                "skipping Slack file context due sync limit"
            );
            unresolved.push(ContextSyncIssueInput {
                kind: "file".to_string(),
                reference: file.id.clone(),
                reason: "file sync limit reached".to_string(),
            });
        }
        for (file_cache_key, file) in file_selection.attempts {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                size_bytes = file.size_bytes,
                "downloading Slack file context"
            );
            match self.download_slack_file(file.clone()).await {
                Ok(downloaded) => {
                    tracing::info!(
                        file_id = %downloaded.file.id,
                        file_name = %downloaded.file.name,
                        downloaded_bytes = downloaded.bytes.len(),
                        extracted_text = downloaded.extracted_text.is_some(),
                        "downloaded Slack file context"
                    );
                    retained_file_artifact_ids.insert(slack_file_artifact_id(&downloaded.file.id));
                    succeeded_file_sync_keys.push(file_cache_key);
                    downloaded_files.push(downloaded);
                }
                Err(err) => {
                    failed_files += 1;
                    let reason = context_error_reason(&err);
                    tracing::info!(
                        file_id = %file.id,
                        file_name = %file.name,
                        reason = %reason,
                        "failed to download Slack file context"
                    );
                    failed_file_sync_keys.push(file_cache_key);
                    unresolved.push(ContextSyncIssueInput {
                        kind: "file".to_string(),
                        reference: file.id,
                        reason,
                    });
                }
            }
        }
        tracing::info!(
            channel = %event.channel,
            thread_ts = %thread_ts,
            downloaded_file_count = downloaded_files.len(),
            skipped_cached_file_count = skipped_cached_files,
            omitted_file_count,
            failed_file_count = failed_files,
            "synced Slack file context"
        );

        SlackContextSyncBuild {
            request: build_slack_context_request(
                session_key,
                CurrentSlackThreadContext {
                    channel: &event.channel,
                    thread_ts: &thread_ts,
                    fresh: current_thread_fresh,
                    messages: &messages,
                },
                &linked_threads,
                &downloaded_files,
                SlackDerivedArtifactRetention::new(
                    retained_linked_thread_artifact_ids,
                    retained_file_artifact_ids,
                    prune_derived_artifacts,
                ),
                remove_artifacts,
                unresolved,
            ),
            succeeded_file_sync_keys,
            failed_file_sync_keys,
        }
    }

    async fn fetch_thread_messages_cached(
        &self,
        channel: &str,
        thread_ts: &str,
        context_turn: Option<&SlackContextTurn>,
    ) -> anyhow::Result<SlackThreadFetch> {
        self.fetch_thread_messages_cached_with(
            channel,
            thread_ts,
            self.fetch_thread_messages(channel, thread_ts),
            context_turn,
        )
        .await
    }

    async fn fetch_thread_messages_cached_with(
        &self,
        channel: &str,
        thread_ts: &str,
        fetch: impl std::future::Future<Output = anyhow::Result<Vec<SlackThreadMessage>>>,
        context_turn: Option<&SlackContextTurn>,
    ) -> anyhow::Result<SlackThreadFetch> {
        let key = slack_thread_cache_key(channel, thread_ts);
        match fetch.await {
            Ok(messages) => {
                if self.context_generation_is_current(context_turn).await {
                    self.context_cache
                        .lock()
                        .await
                        .threads
                        .insert(key, messages.clone());
                }
                Ok(SlackThreadFetch {
                    messages,
                    stale_reason: None,
                })
            }
            Err(err) if slack_error_allows_cached_context(&err) => {
                let fetch = {
                    let cache = self.context_cache.lock().await;
                    cached_thread_fetch_after_error(&err, || {
                        cache.threads.get(&key).cloned().unwrap_or_default()
                    })
                };
                if let Some(fetch) = fetch {
                    Ok(fetch)
                } else {
                    Err(err)
                }
            }
            Err(err) if slack_error_clears_thread_cache(&err) => {
                if self.context_generation_is_current(context_turn).await {
                    self.context_cache.lock().await.threads.remove(&key);
                }
                Err(err)
            }
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
                return Err(SlackApiError::new("conversations.replies", "rate_limited").into());
            }
            let response = response.json::<SlackThreadResponse>().await?;
            if !response.ok {
                return Err(SlackApiError::new(
                    "conversations.replies",
                    response
                        .error
                        .unwrap_or_else(|| "unknown_error".to_string()),
                )
                .into());
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
        let file = if slack_file_ref_needs_info(&file) {
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                "fetching Slack file metadata"
            );
            let file = self.fetch_slack_file_info(file).await?;
            tracing::info!(
                file_id = %file.id,
                file_name = %file.name,
                size_bytes = file.size_bytes,
                has_private_download_url = file.url_private_download.is_some(),
                "fetched Slack file metadata"
            );
            file
        } else {
            file
        };
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

    async fn fetch_slack_file_info(&self, file: SlackFileRef) -> anyhow::Result<SlackFileRef> {
        let response = self
            .http
            .get("https://slack.com/api/files.info")
            .bearer_auth(&self.cfg.bot_token)
            .query(&[("file", file.id.as_str())])
            .send()
            .await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(SlackApiError::new("files.info", "rate_limited").into());
        }
        let response = response.json::<SlackFileInfoResponse>().await?;
        if !response.ok {
            return Err(SlackApiError::new(
                "files.info",
                response
                    .error
                    .unwrap_or_else(|| "unknown_error".to_string()),
            )
            .into());
        }
        let Some(file_info) = response.file else {
            anyhow::bail!("Slack files.info response omitted file");
        };
        let Some(info) = parse_slack_file_ref(&file_info) else {
            anyhow::bail!("Slack files.info response file is invalid");
        };
        Ok(merge_slack_file_info(file, info))
    }

    async fn file_sync_state(
        &self,
        session_key: &str,
        existing_context: &[ContextArtifactRecord],
    ) -> SlackFileSyncState {
        let cache = self.context_cache.lock().await;
        file_sync_state_from_context_artifacts(session_key, existing_context, &cache)
    }

    async fn mark_file_sync_succeeded(&self, cache_key: String) {
        let mut cache = self.context_cache.lock().await;
        cache.failed_files.remove(&cache_key);
        cache.synced_files.insert(cache_key);
    }

    async fn mark_file_sync_failed(&self, cache_key: String) {
        let mut cache = self.context_cache.lock().await;
        if !cache.synced_files.contains(&cache_key) {
            cache.failed_files.insert(cache_key);
        }
    }
}

#[derive(Debug, Default)]
struct SlackContextCache {
    threads: BTreeMap<String, Vec<SlackThreadMessage>>,
    synced_files: BTreeSet<String>,
    failed_files: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct SlackContextTurn {
    session_key: String,
    generation: u64,
}

#[derive(Debug, Clone, Default)]
struct SlackFileSyncState {
    synced_files: BTreeSet<String>,
    failed_files: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct SlackContextSyncBuild {
    request: ContextSyncRequest,
    succeeded_file_sync_keys: Vec<String>,
    failed_file_sync_keys: Vec<String>,
}

fn file_sync_state_from_context_artifacts(
    session_key: &str,
    records: &[ContextArtifactRecord],
    cache: &SlackContextCache,
) -> SlackFileSyncState {
    let mut state = SlackFileSyncState {
        synced_files: BTreeSet::new(),
        failed_files: cache.failed_files.clone(),
    };
    for record in records {
        if record.source != "slack" {
            continue;
        }
        if record.kind == "slack_file" {
            if let Some(file_id) = context_record_file_id(record) {
                let key = format!("{session_key}:{file_id}");
                state.failed_files.remove(&key);
                state.synced_files.insert(key);
            }
        } else if record.kind == "manifest" {
            for file_id in context_record_unresolved_file_ids(record) {
                let key = format!("{session_key}:{file_id}");
                if !state.synced_files.contains(&key) {
                    state.failed_files.insert(key);
                }
            }
        }
    }
    state
}

fn context_record_file_id(record: &ContextArtifactRecord) -> Option<String> {
    record
        .metadata
        .get("file_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| record.id.strip_prefix("slack:file:").map(ToOwned::to_owned))
}

fn context_record_unresolved_file_ids(record: &ContextArtifactRecord) -> Vec<String> {
    record
        .metadata
        .get("unresolved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|issue| {
            (issue.get("kind").and_then(Value::as_str) == Some("file"))
                .then(|| issue.get("reference").and_then(Value::as_str))
                .flatten()
                .map(ToOwned::to_owned)
        })
        .collect()
}

struct SlackRouterOutputSink {
    channel: SlackSocketModeChannel,
    target: SlackReplyTarget,
    channel_events: ChannelEventMode,
    verbose_activity: SlackVerboseActivity,
    compact_activity: SlackCompactActivity,
}

impl SlackRouterOutputSink {
    fn new(
        channel: SlackSocketModeChannel,
        target: SlackReplyTarget,
        channel_events: ChannelEventMode,
    ) -> Self {
        Self {
            channel,
            target,
            channel_events,
            verbose_activity: SlackVerboseActivity::default(),
            compact_activity: SlackCompactActivity::default(),
        }
    }

    async fn flush_channel_events(&mut self) {
        match self.channel_events {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => self.compact_activity.flush().await,
            ChannelEventMode::Verbose => self.verbose_activity.flush().await,
        }
    }
}

#[derive(Default)]
struct SlackVerboseActivity {
    poster: Option<SlackVerboseActivityPoster>,
}

struct SlackVerboseActivityPoster {
    events: mpsc::UnboundedSender<RouterChannelEvent>,
    handle: JoinHandle<()>,
}

#[derive(Default)]
struct SlackCompactActivity {
    events: Vec<RouterChannelEvent>,
    updater: Option<SlackCompactActivityUpdater>,
}

struct SlackCompactActivityUpdater {
    updates: mpsc::UnboundedSender<String>,
    handle: JoinHandle<()>,
}

impl SlackVerboseActivity {
    fn send(
        &mut self,
        channel: SlackSocketModeChannel,
        target: SlackReplyTarget,
        event: RouterChannelEvent,
    ) {
        if self.poster.is_none() {
            self.poster = Some(spawn_slack_verbose_activity_poster(channel, target));
        }
        if let Some(poster) = &self.poster
            && poster.events.send(event).is_err()
        {
            tracing::warn!("verbose Slack activity poster stopped before receiving event");
        }
    }

    async fn flush(&mut self) {
        if let Some(poster) = self.poster.take() {
            drop(poster.events);
            await_slack_activity_worker(poster.handle, "verbose").await;
        }
    }
}

impl SlackCompactActivity {
    fn send(
        &mut self,
        channel: SlackSocketModeChannel,
        target: SlackReplyTarget,
        event: RouterChannelEvent,
    ) {
        self.events.push(event);
        let Some(summary) = render_live_compact_channel_events(&self.events) else {
            return;
        };
        if self.updater.is_none() {
            self.updater = Some(spawn_slack_compact_activity_updater(channel, target));
        }
        if let Some(updater) = &self.updater
            && updater.updates.send(summary).is_err()
        {
            tracing::warn!("compact Slack activity updater stopped before receiving update");
        }
    }

    async fn flush(&mut self) {
        if let Some(updater) = self.updater.take() {
            drop(updater.updates);
            await_slack_activity_worker(updater.handle, "compact").await;
        }
    }
}

#[async_trait::async_trait]
impl RouterOutputSink for SlackRouterOutputSink {
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        match self.channel_events {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => {
                self.compact_activity
                    .send(self.channel.clone(), self.target.clone(), event);
            }
            ChannelEventMode::Verbose => {
                self.verbose_activity
                    .send(self.channel.clone(), self.target.clone(), event);
            }
        }
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        self.flush_channel_events().await;
        self.channel.post_message(&self.target, &text).await
    }
}

fn spawn_slack_verbose_activity_poster(
    channel: SlackSocketModeChannel,
    target: SlackReplyTarget,
) -> SlackVerboseActivityPoster {
    let (tx, mut rx) = mpsc::unbounded_channel::<RouterChannelEvent>();
    let handle = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(err) = channel.post_message(&target, &event.render_text()).await {
                tracing::warn!(error = %err, "failed to post Slack channel event");
            }
        }
    });
    SlackVerboseActivityPoster { events: tx, handle }
}

fn spawn_slack_compact_activity_updater(
    channel: SlackSocketModeChannel,
    target: SlackReplyTarget,
) -> SlackCompactActivityUpdater {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let handle = tokio::spawn(async move {
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
    SlackCompactActivityUpdater {
        updates: tx,
        handle,
    }
}

async fn await_slack_activity_worker(handle: JoinHandle<()>, mode: &'static str) {
    if let Err(err) = handle.await {
        tracing::warn!(error = %err, mode, "Slack activity worker failed");
    }
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
struct SlackFileInfoResponse {
    ok: bool,
    file: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SlackResponseMetadata {
    next_cursor: Option<String>,
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
    fresh: bool,
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
        .filter_map(parse_slack_file_ref)
        .collect()
}

fn parse_slack_file_ref(file: &Value) -> Option<SlackFileRef> {
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
}

fn merge_slack_file_info(original: SlackFileRef, info: SlackFileRef) -> SlackFileRef {
    let SlackFileRef {
        id,
        name,
        mimetype,
        size_bytes,
        url_private,
        url_private_download,
    } = original;
    let info_name_is_fallback = info.name == info.id;
    SlackFileRef {
        id: id.clone(),
        name: if info_name_is_fallback && name != id {
            name
        } else {
            info.name
        },
        mimetype: info.mimetype.or(mimetype),
        size_bytes: if info.size_bytes == 0 {
            size_bytes
        } else {
            info.size_bytes
        },
        url_private: info.url_private.or(url_private),
        url_private_download: info.url_private_download.or(url_private_download),
    }
}

fn slack_file_ref_needs_info(file: &SlackFileRef) -> bool {
    file.url_private_download.is_none() && file.url_private.is_none()
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

fn context_link_messages(
    event: &SlackMessageEvent,
    current_messages: &[SlackThreadMessage],
) -> Vec<SlackThreadMessage> {
    let mut messages = current_messages.to_vec();
    if !messages.iter().any(|message| message.ts == event.ts) {
        messages.push(SlackThreadMessage {
            ts: event.ts.clone(),
            user: Some(event.user.clone()),
            bot_id: None,
            text: event.text.clone(),
            files: event.files.clone(),
        });
    }
    messages
}

fn select_file_sync_attempts(
    session_key: &str,
    file_refs: Vec<SlackFileRef>,
    state: &SlackFileSyncState,
    max_files_per_turn: usize,
) -> SlackFileSyncSelection {
    let mut skipped_synced_files = Vec::new();
    let mut fresh_attempts = Vec::new();
    let mut retry_attempts = Vec::new();
    for file in file_refs {
        let file_cache_key = format!("{session_key}:{}", file.id);
        if state.synced_files.contains(&file_cache_key) {
            skipped_synced_files.push(file);
            continue;
        }
        if state.failed_files.contains(&file_cache_key) {
            retry_attempts.push((file_cache_key, file));
        } else {
            fresh_attempts.push((file_cache_key, file));
        }
    }
    let mut attempts = Vec::new();
    let mut omitted_files = Vec::new();
    for (file_cache_key, file) in fresh_attempts.into_iter().chain(retry_attempts) {
        if attempts.len() < max_files_per_turn {
            attempts.push((file_cache_key, file));
        } else {
            omitted_files.push(file);
        }
    }
    SlackFileSyncSelection {
        attempts,
        skipped_synced_files,
        omitted_files,
    }
}

struct SlackFileSyncSelection {
    attempts: Vec<(String, SlackFileRef)>,
    skipped_synced_files: Vec<SlackFileRef>,
    omitted_files: Vec<SlackFileRef>,
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
    if url.scheme() != "https" || !is_allowed_slack_thread_host(url.host_str()?) {
        return None;
    }
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

fn is_allowed_slack_thread_host(host: &str) -> bool {
    host == "slack.com" || host.ends_with(".slack.com")
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
    let title = rest.lines().next().unwrap_or("").trim();
    title == "Activity"
        || title == "Progress"
        || title == "Reasoning summary"
        || title == "Tool call"
        || title.starts_with("Tool call: ")
}

fn slack_thread_cache_key(channel: &str, thread_ts: &str) -> String {
    format!("{channel}:{thread_ts}")
}

fn slack_thread_reference(channel: &str, thread_ts: &str) -> String {
    format!("{channel}:{thread_ts}")
}

fn slack_current_thread_removal(channel: &str, thread_ts: &str) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Exact {
        id: format!("slack:thread:{channel}:{thread_ts}"),
        kind: "slack_current_thread".to_string(),
    }
}

fn slack_linked_thread_artifact_id(channel: &str, thread_ts: &str) -> String {
    format!("slack:linked-thread:{channel}:{thread_ts}")
}

fn slack_linked_thread_removal(channel: &str, thread_ts: &str) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Exact {
        id: slack_linked_thread_artifact_id(channel, thread_ts),
        kind: "slack_linked_thread".to_string(),
    }
}

fn slack_all_linked_threads_removal() -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::Kind {
        kind: "slack_linked_thread".to_string(),
    }
}

fn slack_retained_linked_threads_removal(
    retain_ids: BTreeSet<String>,
) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::ExceptKind {
        kind: "slack_linked_thread".to_string(),
        retain_ids,
    }
}

fn slack_file_artifact_id(file_id: &str) -> String {
    format!("slack:file:{file_id}")
}

fn slack_retained_files_removal(retain_ids: BTreeSet<String>) -> ContextArtifactRemovalInput {
    ContextArtifactRemovalInput::ExceptKind {
        kind: "slack_file".to_string(),
        retain_ids,
    }
}

fn context_error_reason(err: &anyhow::Error) -> String {
    let raw = err.to_string();
    if raw.contains("url_private") || raw.contains("slack-files.com") || raw.contains("http") {
        "Slack context sync request failed".to_string()
    } else {
        raw
    }
}

struct CurrentSlackThreadContext<'a> {
    channel: &'a str,
    thread_ts: &'a str,
    fresh: bool,
    messages: &'a [SlackThreadMessage],
}

struct SlackDerivedArtifactRetention {
    linked_thread_ids: BTreeSet<String>,
    file_ids: BTreeSet<String>,
    prune: bool,
}

impl SlackDerivedArtifactRetention {
    fn new(linked_thread_ids: BTreeSet<String>, file_ids: BTreeSet<String>, prune: bool) -> Self {
        Self {
            linked_thread_ids,
            file_ids,
            prune,
        }
    }
}

fn build_slack_context_request(
    session_key: &str,
    current_thread: CurrentSlackThreadContext<'_>,
    linked_threads: &[LinkedSlackThread],
    downloaded_files: &[DownloadedSlackFile],
    mut retention: SlackDerivedArtifactRetention,
    remove_artifacts: Vec<ContextArtifactRemovalInput>,
    unresolved: Vec<ContextSyncIssueInput>,
) -> ContextSyncRequest {
    let mut artifacts = Vec::new();
    let mut remove_artifacts = remove_artifacts;
    if !current_thread.messages.is_empty() {
        let mut metadata = BTreeMap::new();
        let channel = current_thread.channel;
        let thread_ts = current_thread.thread_ts;
        let thread_ref = format!("{channel}:{thread_ts}");
        metadata.insert("channel".to_string(), json!(channel));
        metadata.insert("thread_ts".to_string(), json!(thread_ts));
        metadata.insert(
            "message_count".to_string(),
            json!(current_thread.messages.len()),
        );
        let mut resolves_unresolved =
            vec![json!({"kind": "current_thread", "reference": &thread_ref})];
        if current_thread.fresh {
            resolves_unresolved
                .push(json!({"kind": "current_thread_cache", "reference": &thread_ref}));
        }
        metadata.insert(
            "resolves_unresolved".to_string(),
            json!(resolves_unresolved),
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
                        channel,
                        thread_ts,
                        current_thread.messages,
                    )),
                },
                ContextFileInput {
                    relative_path: PathBuf::from("slack/current-thread.jsonl"),
                    content: ContextFileContent::Text(render_slack_thread_jsonl(
                        current_thread.messages,
                    )),
                },
            ],
            metadata,
        });
    }

    for linked_thread in linked_threads {
        if linked_thread.messages.is_empty() {
            continue;
        }
        retention
            .linked_thread_ids
            .insert(slack_linked_thread_artifact_id(
                &linked_thread.link.channel,
                &linked_thread.link.thread_ts,
            ));
        artifacts.push(linked_thread_artifact(linked_thread));
    }
    if retention.prune {
        remove_artifacts.push(slack_retained_linked_threads_removal(
            retention.linked_thread_ids,
        ));
    }

    for downloaded in downloaded_files {
        retention
            .file_ids
            .insert(slack_file_artifact_id(&downloaded.file.id));
        artifacts.push(slack_file_artifact(downloaded));
    }
    if retention.prune {
        remove_artifacts.push(slack_retained_files_removal(retention.file_ids));
    }

    ContextSyncRequest {
        session_key: session_key.to_string(),
        source: "slack".to_string(),
        base_path: PathBuf::from("slack"),
        artifacts,
        remove_artifacts,
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
    let thread_ref = slack_thread_reference(channel, thread_ts);
    let mut resolves_unresolved = vec![json!({"kind": "linked_thread", "reference": &thread_ref})];
    if linked_thread.fresh {
        resolves_unresolved.push(json!({"kind": "linked_thread_cache", "reference": &thread_ref}));
    }
    metadata.insert(
        "resolves_unresolved".to_string(),
        json!(resolves_unresolved),
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
        id: slack_linked_thread_artifact_id(channel, thread_ts),
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
        id: slack_file_artifact_id(&downloaded.file.id),
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
        preempted: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl RouterService for RecordingRouter {
        async fn preempt(&self, session_key: &str) -> anyhow::Result<u64> {
            let mut preempted = self.preempted.lock().await;
            preempted.push(session_key.to_string());
            Ok(preempted.len() as u64)
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

        let files = collect_context_file_refs(&[], &[first, second], &[]);

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].id, "F1");
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
            vec![SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U1".to_string()),
                bot_id: None,
                text: "stale".to_string(),
                files: Vec::new(),
            }],
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
    async fn stale_context_generation_does_not_update_thread_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        channel.remember_context_generation("session", 2).await;
        let stale_turn = SlackContextTurn {
            session_key: "session".to_string(),
            generation: 1,
        };
        let message = SlackThreadMessage {
            ts: "111.000".to_string(),
            user: Some("U1".to_string()),
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
        assert!(
            !channel
                .context_cache
                .lock()
                .await
                .threads
                .contains_key(&slack_thread_cache_key("C1", "111.000"))
        );
    }

    #[tokio::test]
    async fn stale_context_generation_does_not_clear_thread_cache() {
        let channel = SlackSocketModeChannel::new(
            test_slack_config(true),
            Arc::new(ApprovalBroker::default()),
        );
        let key = slack_thread_cache_key("C1", "111.000");
        channel.context_cache.lock().await.threads.insert(
            key.clone(),
            vec![SlackThreadMessage {
                ts: "111.000".to_string(),
                user: Some("U1".to_string()),
                bot_id: None,
                text: "fresh context".to_string(),
                files: Vec::new(),
            }],
        );
        channel.remember_context_generation("session", 2).await;
        let stale_turn = SlackContextTurn {
            session_key: "session".to_string(),
            generation: 1,
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
        assert!(
            channel
                .context_cache
                .lock()
                .await
                .threads
                .contains_key(&key)
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
            SlackThreadMessage {
                ts: "113.000".to_string(),
                user: Some("BOT".to_string()),
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Progress\nI will inspect the config first.".to_string(),
                files: Vec::new(),
            },
            SlackThreadMessage {
                ts: "114.000".to_string(),
                user: Some("BOT".to_string()),
                bot_id: Some("B_SELF".to_string()),
                text: "[codex] Progress report for the release".to_string(),
                files: Vec::new(),
            },
        ];

        let filtered = filter_context_messages(messages, "BOT");

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].text, "root from another bot");
        assert_eq!(filtered[1].text, "[codex] Progress report for the release");
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
            fresh: true,
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

        assert!(router.preempted.lock().await.is_empty());
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
