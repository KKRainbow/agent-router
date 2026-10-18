use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    approval::SharedApprovalBroker,
    channel::{
        EventDeduper,
        output::{ChannelOutputPolicy, ChannelOutputSink, ChannelReplyPort, PostedMessage},
    },
    config::{ChannelEventMode, QqConfig},
    router::{
        ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome,
        ChannelRouteTicket, RouterService,
    },
    session::context::{
        ContextArtifactInput, ContextFileContent, ContextFileInput, ContextSyncIssueInput,
        ContextSyncRequest, sanitize_path_segment,
    },
};

const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_SANDBOX_API_BASE: &str = "https://sandbox.api.sgroup.qq.com";
const QQ_AUTH_URL: &str = "https://bots.qq.com/app/getAppAccessToken";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
const QQ_REPLY_CHECKPOINT_MIN_INTERVAL: Duration = Duration::from_secs(20);
const QQ_REPLY_CHECKPOINT_MIN_GROWTH: usize = 500;
// QQ serves message attachments from Tencent media hosts with a signed URL, so
// the download carries no credential; only these hosts are contacted.
const QQ_ATTACHMENT_HOSTS: [&str; 4] = ["qq.com", "qq.com.cn", "qpic.cn", "gtimg.cn"];
const QQ_ATTACHMENT_KIND: &str = "qq_file";
const QQ_ATTACHMENT_MAX_BYTES: usize = 20 * 1024 * 1024;
const QQ_ATTACHMENT_MESSAGE_MAX_BYTES: usize = 32 * 1024 * 1024;
const QQ_ATTACHMENT_MAX_CONCURRENT: usize = 2;
const QQ_ATTACHMENT_TIMEOUT: Duration = Duration::from_secs(30);
const QQ_ATTACHMENT_PLACEHOLDER: &str = "[attachment]";
const QQ_ATTACHMENT_NOTICE_LIMIT: usize = 3;
const QQ_REPLY_CHECKPOINT_PREVIEW_CHARS: usize = 240;
const QQ_MSG_TYPE_TEXT: u8 = 0;
const QQ_MSG_TYPE_MARKDOWN: u8 = 2;
#[derive(Debug, Clone)]
pub struct QqBotChannel {
    cfg: QqConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    attachment_http: Client,
    attachments: Arc<Semaphore>,
    access_token: Arc<Mutex<Option<CachedAccessToken>>>,
    gateway_session: Arc<Mutex<QqGatewaySession>>,
    seen_events: Arc<Mutex<EventDeduper>>,
    reply_contexts: Arc<Mutex<HashMap<String, QqReplyContext>>>,
    next_reply_context_sequence: Arc<AtomicU64>,
}

impl QqBotChannel {
    pub fn new(cfg: QqConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            attachment_http: qq_attachment_http_client(),
            attachments: Arc::new(Semaphore::new(QQ_ATTACHMENT_MAX_CONCURRENT)),
            access_token: Arc::new(Mutex::new(None)),
            gateway_session: Arc::new(Mutex::new(QqGatewaySession::default())),
            seen_events: Arc::new(Mutex::new(EventDeduper::new(1024))),
            reply_contexts: Arc::new(Mutex::new(HashMap::new())),
            next_reply_context_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        self.validate_config()?;
        let channel = Arc::new(self);
        channel.clone().spawn_approval_notifier();

        loop {
            match channel.run_once(router.clone()).await {
                Ok(()) => tracing::warn!("QQ gateway ended; reconnecting"),
                Err(err) => tracing::warn!(error = %err, "QQ gateway disconnected; reconnecting"),
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    async fn run_once(&self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        let token = self.token().await?;
        let gateway_url = self.gateway_url(&token).await?;
        tracing::info!("connecting QQ gateway");
        let (stream, _) = connect_async(gateway_url).await?;
        tracing::info!("connected QQ gateway");
        self.run_gateway(stream, &token, router).await
    }

    async fn run_gateway<S>(
        &self,
        stream: tokio_tungstenite::WebSocketStream<S>,
        token: &str,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut sink, mut stream) = stream.split();

        let hello = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("QQ gateway closed before hello"))??;
        let hello = message_text(hello)?.ok_or_else(|| anyhow::anyhow!("QQ hello was not text"))?;
        let hello: Value = serde_json::from_str(&hello)?;
        let heartbeat_interval = hello
            .get("d")
            .and_then(|d| d.get("heartbeat_interval"))
            .and_then(Value::as_u64)
            .unwrap_or(41_250);

        let stored_session = self.stored_gateway_session().await;
        let mut sequence = stored_session.sequence;
        if let (Some(session_id), Some(seq)) = (&stored_session.session_id, stored_session.sequence)
        {
            let resume = json!({
                "op": 6,
                "d": {
                    "token": format!("QQBot {token}"),
                    "session_id": session_id,
                    "seq": seq,
                },
            });
            sink.send(Message::Text(resume.to_string().into())).await?;
        } else {
            let identify = json!({
                "op": 2,
                "d": {
                    "token": format!("QQBot {token}"),
                    "intents": self.cfg.intents,
                    "properties": {
                        "os": "linux",
                        "browser": "agent-router",
                        "device": "agent-router",
                    },
                },
            });
            sink.send(Message::Text(identify.to_string().into()))
                .await?;
        }

        anyhow::ensure!(
            heartbeat_interval > 0,
            "QQ heartbeat interval must be positive"
        );
        let heartbeat_interval = Duration::from_millis(heartbeat_interval);
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut heartbeat_ack_deadline = None;

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if heartbeat_ack_deadline.is_none() {
                        send_heartbeat(&mut sink, sequence, heartbeat_interval).await?;
                        heartbeat_ack_deadline = Some(tokio::time::Instant::now() + heartbeat_interval);
                    }
                }
                _ = tokio::time::sleep_until(heartbeat_ack_deadline.unwrap_or_else(tokio::time::Instant::now)),
                    if heartbeat_ack_deadline.is_some() => {
                    anyhow::bail!("QQ heartbeat ACK timed out");
                }
                frame = stream.next() => {
                    let Some(frame) = frame else {
                        anyhow::bail!("QQ gateway stream ended");
                    };
                    match frame? {
                        Message::Text(text) => {
                            self.handle_gateway_payload(text.as_ref(), &mut sink, &mut sequence, &mut heartbeat_ack_deadline, heartbeat_interval, router.clone()).await?;
                        }
                        Message::Binary(bytes) => {
                            let text = String::from_utf8(bytes.to_vec())?;
                            self.handle_gateway_payload(&text, &mut sink, &mut sequence, &mut heartbeat_ack_deadline, heartbeat_interval, router.clone()).await?;
                        }
                        Message::Ping(payload) => {
                            tokio::time::timeout(heartbeat_interval, sink.send(Message::Pong(payload)))
                                .await.map_err(|_| anyhow::anyhow!("QQ pong write timed out"))??;
                        }
                        Message::Close(close) => {
                            anyhow::bail!("QQ gateway closed: {close:?}");
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn handle_gateway_payload<S>(
        &self,
        text: &str,
        sink: &mut S,
        sequence: &mut Option<i64>,
        heartbeat_ack_deadline: &mut Option<tokio::time::Instant>,
        heartbeat_interval: Duration,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()>
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let payload = match serde_json::from_str::<Value>(text) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(error = %err, "ignored malformed QQ gateway payload");
                return Ok(());
            }
        };
        if let Some(next_sequence) = payload.get("s").and_then(Value::as_i64) {
            *sequence = Some(next_sequence);
            self.record_gateway_sequence(next_sequence).await;
        }

        match payload.get("op").and_then(Value::as_u64).unwrap_or(0) {
            0 => {
                let event_type = payload
                    .get("t")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let Some(data) = payload.get("d") else {
                    return Ok(());
                };
                if matches!(event_type.as_str(), "READY" | "RESUMED") {
                    if let Some(session_id) = data.get("session_id").and_then(Value::as_str) {
                        tracing::info!(
                            event_type = %event_type,
                            session_id = %session_id,
                            "QQ gateway session established"
                        );
                        self.record_gateway_session_id(session_id.to_string()).await;
                    }
                    return Ok(());
                }
                self.handle_dispatch(&event_type, data, router).await?;
            }
            1 => {
                send_heartbeat(sink, *sequence, heartbeat_interval).await?;
                heartbeat_ack_deadline
                    .get_or_insert(tokio::time::Instant::now() + heartbeat_interval);
            }
            7 => anyhow::bail!("QQ gateway requested reconnect"),
            9 => {
                self.clear_gateway_session().await;
                anyhow::bail!("QQ gateway reported invalid session");
            }
            11 => *heartbeat_ack_deadline = None,
            _ => {}
        }
        Ok(())
    }

    async fn handle_dispatch(
        &self,
        event_type: &str,
        data: &Value,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        let Some(message) = parse_inbound_message(event_type, data) else {
            return Ok(());
        };
        if !self.should_accept(&message) {
            return Ok(());
        }
        if !self
            .seen_events
            .lock()
            .await
            .insert(message.event_key.clone())
        {
            return Ok(());
        }

        let outcome = router
            .begin_channel_input(ChannelInput {
                session_key: message.session_key.clone(),
                text: message.text.clone(),
                user_id: Some(message.user_id.clone()),
                source: "qq".to_string(),
                intent: ChannelInputIntent::Route,
                context_policy: ChannelContextPolicy {
                    source: "qq".to_string(),
                    enabled: true,
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
        let reply_context_sequence = self
            .next_reply_context_sequence
            .fetch_add(1, Ordering::Relaxed);
        let channel = self.clone();
        tokio::spawn(async move {
            let session_key = message.session_key.clone();
            if let Err(err) = channel
                .route_message(
                    message,
                    ticket,
                    reply_context_sequence,
                    context_allowed,
                    router,
                )
                .await
            {
                tracing::warn!(
                    error = %err,
                    session_key = %session_key,
                    "failed to handle QQ session message"
                );
            }
        });
        Ok(())
    }

    async fn route_message(
        &self,
        message: QqInboundMessage,
        ticket: ChannelRouteTicket,
        reply_context_sequence: u64,
        context_allowed: bool,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        self.remember_reply_context(&message, reply_context_sequence)
            .await;
        tracing::info!(
            session_key = %message.session_key,
            user_id = %message.user_id,
            group_id = ?message.group_id,
            text_len = message.text.len(),
            attachment_count = message.attachments.len(),
            "routing QQ message"
        );
        let QqAttachmentSync {
            request,
            failures,
            admission,
        } = if context_allowed {
            self.sync_attachments(&message).await
        } else {
            QqAttachmentSync::default()
        };
        let mut output = ChannelOutputSink::new(
            QqReplyPort {
                channel: self.clone(),
                session_key: message.session_key.clone(),
                msg_id: message.msg_id,
            },
            message.target,
            qq_output_policy(self.cfg.channel_events),
        );
        // The turn owns the context commit, so the admission lives until it
        // finishes: a superseded turn ends right away and frees it.
        let result = router
            .finish_channel_input(ticket, request, &mut output)
            .await;
        drop(admission);
        if !failures.is_empty() {
            self.report_attachment_failures(&message.session_key, &failures)
                .await;
        }
        result
    }

    async fn sync_attachments(&self, message: &QqInboundMessage) -> QqAttachmentSync {
        if message.attachments.is_empty() {
            return QqAttachmentSync::default();
        }
        // One admission covers the whole message: its attachments stay
        // buffered until the router has written them out.
        let admission = match self.attachments.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(err) => {
                tracing::warn!(error = %err, "QQ attachment admission is closed");
                return QqAttachmentSync {
                    failures: vec![format!("attachment sync unavailable: {err}")],
                    ..QqAttachmentSync::default()
                };
            }
        };
        let unit = qq_message_unit(&message.msg_id, message.msg_idx.as_deref());
        let mut downloaded = Vec::new();
        let mut unresolved = Vec::new();
        let mut failures = Vec::new();
        let mut committed = 0;
        for (index, attachment) in message.attachments.iter().enumerate() {
            let byte_cap = qq_attachment_byte_cap(committed);
            if byte_cap == 0 {
                tracing::warn!(
                    message_id = %message.msg_id,
                    skipped = message.attachments.len() - index,
                    "QQ attachment sync stopped at the message budget"
                );
                let (skipped_issues, skipped_failures) =
                    qq_budget_skipped_attachments(message, &unit, index);
                unresolved.extend(skipped_issues);
                failures.extend(skipped_failures);
                break;
            }
            match self
                .download_attachment(message, attachment, index, byte_cap)
                .await
            {
                Ok(file) => {
                    committed += file.bytes.len();
                    downloaded.push(file);
                }
                Err(err) => {
                    let reason = format!("{err:#}");
                    let name = qq_attachment_label(attachment, index);
                    tracing::warn!(
                        message_id = %message.msg_id,
                        file = %name,
                        error = %reason,
                        "QQ attachment sync failed"
                    );
                    unresolved.push(ContextSyncIssueInput {
                        kind: QQ_ATTACHMENT_KIND.to_string(),
                        reference: qq_attachment_reference(&unit, index),
                        reason: reason.clone(),
                    });
                    failures.push(format!("{name}: {reason}"));
                }
            }
        }
        QqAttachmentSync {
            request: Some(qq_context_request(message, downloaded, unresolved)),
            failures,
            admission: Some(admission),
        }
    }

    async fn download_attachment(
        &self,
        message: &QqInboundMessage,
        attachment: &QqAttachment,
        index: usize,
        byte_cap: usize,
    ) -> anyhow::Result<DownloadedQqAttachment> {
        let url = parse_qq_attachment_url(&attachment.url)?;
        anyhow::ensure!(
            attachment.size <= byte_cap as u64,
            "file exceeds the {byte_cap} byte attachment budget left for this message"
        );
        tracing::info!(
            message_id = %message.msg_id,
            file = %qq_attachment_label(attachment, index),
            size_bytes = attachment.size,
            host = url.host_str().unwrap_or_default(),
            "downloading QQ attachment"
        );
        // The URL is signed by the platform, so no credential is sent.
        let mut response = self
            .attachment_http
            .get(url)
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("download request failed: {}", err.without_url()))?;
        if !response.status().is_success() {
            anyhow::bail!("download rejected with {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > byte_cap as u64)
        {
            anyhow::bail!(
                "file exceeds the {byte_cap} byte attachment budget left for this message"
            );
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| anyhow::anyhow!("download read failed: {}", err.without_url()))?
        {
            if bytes.len() + chunk.len() > byte_cap {
                anyhow::bail!(
                    "file exceeds the {byte_cap} byte attachment budget left for this message"
                );
            }
            bytes.extend_from_slice(&chunk);
        }
        let name = qq_attachment_label(attachment, index);
        let relative_path = qq_attachment_relative_path(
            &qq_message_unit(&message.msg_id, message.msg_idx.as_deref()),
            &name,
            index,
        );
        Ok(DownloadedQqAttachment {
            attachment: attachment.clone(),
            index,
            name,
            relative_path,
            bytes,
        })
    }

    async fn report_attachment_failures(&self, session_key: &str, failures: &[String]) {
        let mut text = format!(
            "Attachment sync failed, {} file(s) skipped:",
            failures.len()
        );
        for failure in failures.iter().take(QQ_ATTACHMENT_NOTICE_LIMIT) {
            text.push_str(&format!("\n- {failure}"));
        }
        if failures.len() > QQ_ATTACHMENT_NOTICE_LIMIT {
            text.push_str("\n- ...");
        }
        if let Err(err) = self.send_session_message(session_key, &text).await {
            tracing::warn!(error = %err, "failed to post QQ attachment failure notice");
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
                if !prompt.session_key.starts_with("qq:") {
                    continue;
                }
                if !prompt_channel.approvals.has_pending(&prompt.id).await {
                    continue;
                }
                let text = prompt.render_text();
                tokio::select! {
                    biased;
                    _ = prompt.cancelled() => continue,
                    result = prompt_channel.send_session_message(&prompt.session_key, &text) => {
                        if let Err(err) = result {
                            tracing::warn!(error = %err, "failed to post QQ approval prompt");
                        }
                    }
                }
            }
        });

        // YOLO auto-approvals are high-frequency bookkeeping. Keep them out of
        // chat and let tool activity summaries carry the useful progress.
    }

    async fn remember_reply_context(
        &self,
        message: &QqInboundMessage,
        reply_context_sequence: u64,
    ) {
        let mut contexts = self.reply_contexts.lock().await;
        if let Some(context) = contexts.get_mut(&message.session_key) {
            if reply_context_sequence >= context.reply_context_sequence {
                context.target = message.target.clone();
                context.msg_id = message.msg_id.clone();
                context.reply_context_sequence = reply_context_sequence;
            }
            return;
        }
        contexts.insert(
            message.session_key.clone(),
            QqReplyContext {
                target: message.target.clone(),
                msg_id: message.msg_id.clone(),
                next_seq: 1,
                reply_context_sequence,
            },
        );
    }

    async fn send_session_message(&self, session_key: &str, text: &str) -> anyhow::Result<()> {
        let (target, msg_id, msg_seq) = {
            let mut contexts = self.reply_contexts.lock().await;
            let context = contexts.get_mut(session_key).ok_or_else(|| {
                anyhow::anyhow!("QQ reply context is missing for session `{session_key}`")
            })?;
            (
                context.target.clone(),
                context.msg_id.clone(),
                context.take_next_seq(),
            )
        };
        self.post_text_message(&target, &msg_id, msg_seq, text)
            .await
    }

    async fn send_reply_message(
        &self,
        session_key: &str,
        target: &QqReplyTarget,
        msg_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        let msg_seq = {
            let mut contexts = self.reply_contexts.lock().await;
            let context = contexts.get_mut(session_key).ok_or_else(|| {
                anyhow::anyhow!("QQ reply context is missing for session `{session_key}`")
            })?;
            context.take_next_seq()
        };
        self.post_text_message(target, msg_id, msg_seq, text).await
    }

    async fn send_markdown_reply_message(
        &self,
        session_key: &str,
        target: &QqReplyTarget,
        msg_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        let msg_seq = {
            let mut contexts = self.reply_contexts.lock().await;
            let context = contexts.get_mut(session_key).ok_or_else(|| {
                anyhow::anyhow!("QQ reply context is missing for session `{session_key}`")
            })?;
            context.take_next_seq()
        };
        self.post_markdown_message(target, msg_id, msg_seq, text)
            .await
    }

    pub(crate) async fn send_notification(&self, target: &str, text: &str) -> anyhow::Result<()> {
        let target = if let Some(openid) = target.strip_prefix("c2c:") {
            QqReplyTarget::C2c {
                openid: openid.to_string(),
            }
        } else if let Some(group_openid) = target.strip_prefix("group:") {
            QqReplyTarget::Group {
                group_openid: group_openid.to_string(),
            }
        } else {
            anyhow::bail!("invalid QQ message target");
        };
        // Notifications have no fresh incoming message to reply to. Use the
        // proactive API and surface platform permission/quota errors as delivery
        // failures, without rerunning the task or reusing expired message IDs.
        self.post_message_body(
            &target,
            0,
            text.len(),
            json!({ "content": text, "msg_type": 0 }),
            "text",
        )
        .await
    }

    async fn post_text_message(
        &self,
        target: &QqReplyTarget,
        msg_id: &str,
        msg_seq: u32,
        text: &str,
    ) -> anyhow::Result<()> {
        self.post_message_body(
            target,
            msg_seq,
            text.len(),
            qq_text_message_body(msg_id, msg_seq, text),
            "text",
        )
        .await
    }

    async fn post_markdown_message(
        &self,
        target: &QqReplyTarget,
        msg_id: &str,
        msg_seq: u32,
        text: &str,
    ) -> anyhow::Result<()> {
        let Some(body) = qq_markdown_message_body(msg_id, msg_seq, text) else {
            return self.post_text_message(target, msg_id, msg_seq, text).await;
        };
        match self
            .post_message_body(target, msg_seq, text.len(), body, "markdown")
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if qq_error_allows_text_fallback(&err) => {
                tracing::warn!(
                    error = %err,
                    "QQ markdown message was rejected; retrying final reply as plain text"
                );
                self.post_text_message(target, msg_id, msg_seq, text).await
            }
            Err(err) => Err(err),
        }
    }

    async fn post_message_body(
        &self,
        target: &QqReplyTarget,
        msg_seq: u32,
        text_len: usize,
        body: Value,
        message_kind: &'static str,
    ) -> anyhow::Result<()> {
        tracing::info!(
            target_scope = target.scope(),
            target_id = %target.id(),
            msg_seq,
            text_len,
            message_kind,
            "sending QQ message"
        );
        let token = self.token().await?;
        let url = format!(
            "{}/v2/{}/{}/messages",
            self.api_base(),
            target.scope(),
            target.id()
        );
        let resp = self
            .http
            .post(url)
            .header("Authorization", format!("QQBot {token}"))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = truncate_for_error(resp.text().await.unwrap_or_default());
            return Err(QqSendMessageError { status, body }.into());
        }
        tracing::info!(
            target_scope = target.scope(),
            target_id = %target.id(),
            msg_seq,
            text_len,
            message_kind,
            "sent QQ message"
        );
        Ok(())
    }

    async fn token(&self) -> anyhow::Result<String> {
        {
            let cached = self.access_token.lock().await;
            if let Some(cached) = cached.as_ref()
                && Instant::now() < cached.expires_at
            {
                return Ok(cached.value.clone());
            }
        }

        let token = self.fetch_access_token().await?;
        let mut cached = self.access_token.lock().await;
        *cached = Some(token.clone());
        Ok(token.value)
    }

    async fn fetch_access_token(&self) -> anyhow::Result<CachedAccessToken> {
        let resp = self
            .http
            .post(QQ_AUTH_URL)
            .json(&json!({
                "appId": self.cfg.app_id,
                "clientSecret": self.cfg.client_secret,
            }))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = truncate_for_error(resp.text().await.unwrap_or_default());
            anyhow::bail!("QQ token request failed ({status}): {body}");
        }

        let data = resp.json::<Value>().await?;
        let token = data
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("QQ token response omitted access_token"))?
            .to_string();
        let expires_in = data
            .get("expires_in")
            .and_then(value_as_u64)
            .unwrap_or(7_200);
        let ttl = Duration::from_secs(expires_in).saturating_sub(TOKEN_EXPIRY_MARGIN);
        Ok(CachedAccessToken {
            value: token,
            expires_at: Instant::now() + ttl.max(Duration::from_secs(1)),
        })
    }

    async fn gateway_url(&self, token: &str) -> anyhow::Result<String> {
        let resp = self
            .http
            .get(format!("{}/gateway", self.api_base()))
            .header("Authorization", format!("QQBot {token}"))
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = truncate_for_error(resp.text().await.unwrap_or_default());
            anyhow::bail!("QQ gateway request failed ({status}): {body}");
        }
        resp.json::<Value>()
            .await?
            .get("url")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("QQ gateway response omitted url"))
    }

    async fn stored_gateway_session(&self) -> QqGatewaySession {
        self.gateway_session.lock().await.clone()
    }

    async fn record_gateway_sequence(&self, sequence: i64) {
        if sequence >= 0 {
            self.gateway_session.lock().await.sequence = Some(sequence);
        }
    }

    async fn record_gateway_session_id(&self, session_id: String) {
        self.gateway_session.lock().await.session_id = Some(session_id);
    }

    async fn clear_gateway_session(&self) {
        *self.gateway_session.lock().await = QqGatewaySession::default();
    }

    fn api_base(&self) -> &'static str {
        if self.cfg.sandbox {
            QQ_SANDBOX_API_BASE
        } else {
            QQ_API_BASE
        }
    }

    fn should_accept(&self, message: &QqInboundMessage) -> bool {
        let user_allowed =
            self.cfg.allowed_users.is_empty() || self.cfg.allowed_users.contains(&message.user_id);
        let group_allowed = message.group_id.as_ref().is_none_or(|group_id| {
            self.cfg.allowed_groups.is_empty() || self.cfg.allowed_groups.contains(group_id)
        });
        user_allowed && group_allowed
    }

    fn validate_config(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.cfg.app_id.is_empty(), "QQ_APP_ID is required");
        anyhow::ensure!(
            !self.cfg.client_secret.is_empty(),
            "QQ_CLIENT_SECRET is required"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct QqReplyPort {
    channel: QqBotChannel,
    session_key: String,
    msg_id: String,
}

fn qq_output_policy(activity_mode: ChannelEventMode) -> ChannelOutputPolicy {
    let mut policy = ChannelOutputPolicy::checkpoint_messages(activity_mode);
    policy.checkpoint_min_interval = QQ_REPLY_CHECKPOINT_MIN_INTERVAL;
    policy.checkpoint_min_growth = QQ_REPLY_CHECKPOINT_MIN_GROWTH;
    policy.checkpoint_preview_chars = QQ_REPLY_CHECKPOINT_PREVIEW_CHARS;
    policy.checkpoint_prefix = "Still generating reply:\n";
    policy
}

#[async_trait::async_trait]
impl ChannelReplyPort for QqReplyPort {
    type Target = QqReplyTarget;

    async fn post_text(&self, target: &Self::Target, text: &str) -> anyhow::Result<PostedMessage> {
        self.channel
            .send_reply_message(&self.session_key, target, &self.msg_id, text)
            .await?;
        Ok(PostedMessage::without_id())
    }

    async fn post_markdown(
        &self,
        target: &Self::Target,
        text: &str,
    ) -> anyhow::Result<PostedMessage> {
        self.channel
            .send_markdown_reply_message(&self.session_key, target, &self.msg_id, text)
            .await?;
        Ok(PostedMessage::without_id())
    }

    async fn update_text(
        &self,
        _target: &Self::Target,
        _id: &str,
        _text: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_markdown(
        &self,
        _target: &Self::Target,
        _id: &str,
        _text: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete(&self, _target: &Self::Target, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

fn qq_text_message_body(msg_id: &str, msg_seq: u32, text: &str) -> Value {
    json!({
        "content": text,
        "msg_type": QQ_MSG_TYPE_TEXT,
        "msg_id": msg_id,
        "msg_seq": msg_seq,
    })
}

fn qq_markdown_message_body(msg_id: &str, msg_seq: u32, text: &str) -> Option<Value> {
    if text.trim().is_empty() {
        return None;
    }
    Some(json!({
        "markdown": {
            "content": text,
        },
        "msg_type": QQ_MSG_TYPE_MARKDOWN,
        "msg_id": msg_id,
        "msg_seq": msg_seq,
    }))
}

#[derive(Debug, Clone)]
struct CachedAccessToken {
    value: String,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default)]
struct QqGatewaySession {
    session_id: Option<String>,
    sequence: Option<i64>,
}

#[derive(Debug, Clone)]
struct QqReplyContext {
    target: QqReplyTarget,
    msg_id: String,
    next_seq: u32,
    reply_context_sequence: u64,
}

impl QqReplyContext {
    fn take_next_seq(&mut self) -> u32 {
        let msg_seq = self.next_seq;
        self.next_seq = self.next_seq.checked_add(1).unwrap_or(1);
        msg_seq
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QqReplyTarget {
    C2c { openid: String },
    Group { group_openid: String },
}

impl QqReplyTarget {
    fn scope(&self) -> &'static str {
        match self {
            Self::C2c { .. } => "users",
            Self::Group { .. } => "groups",
        }
    }

    fn id(&self) -> &str {
        match self {
            Self::C2c { openid } => openid,
            Self::Group { group_openid } => group_openid,
        }
    }
}

#[derive(Debug, Clone)]
struct QqInboundMessage {
    event_key: String,
    msg_id: String,
    msg_idx: Option<String>,
    session_key: String,
    user_id: String,
    group_id: Option<String>,
    text: String,
    attachments: Vec<QqAttachment>,
    target: QqReplyTarget,
}

/// A platform attachment. `size` is 0 when the event does not report it.
#[derive(Debug, Clone)]
struct QqAttachment {
    url: String,
    filename: String,
    size: u64,
    content_type: String,
}

#[derive(Debug)]
struct DownloadedQqAttachment {
    attachment: QqAttachment,
    index: usize,
    name: String,
    relative_path: PathBuf,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct QqAttachmentSync {
    request: Option<ContextSyncRequest>,
    failures: Vec<String>,
    // Held from the first download until the router has consumed the buffered
    // attachments, so retained attachment bytes stay bounded by
    // QQ_ATTACHMENT_MAX_CONCURRENT * QQ_ATTACHMENT_MESSAGE_MAX_BYTES.
    admission: Option<OwnedSemaphorePermit>,
}

fn parse_inbound_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    match event_type {
        "C2C_MESSAGE_CREATE" => parse_c2c_message(event_type, data),
        "GROUP_AT_MESSAGE_CREATE" => parse_group_at_message(event_type, data),
        _ => None,
    }
}

/// The same `msg_id` may cover several pushed messages, and a retry of one
/// message repeats its `msg_idx`; both are part of the identity.
fn qq_event_key(event_type: &str, msg_id: &str, msg_idx: Option<&str>) -> String {
    match msg_idx {
        Some(msg_idx) => format!("{event_type}:{msg_id}:{msg_idx}"),
        None => format!("{event_type}:{msg_id}"),
    }
}

fn qq_message_unit(msg_id: &str, msg_idx: Option<&str>) -> String {
    match msg_idx {
        Some(msg_idx) => format!("{msg_id}:{msg_idx}"),
        None => msg_id.to_string(),
    }
}

fn qq_parse_message_idx(data: &Value) -> Option<String> {
    data.get("message_scene")?
        .get("ext")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .find_map(|entry| entry.strip_prefix("msg_idx="))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// A message may carry several attachments; keep the whole message inside one
/// byte budget so a single event cannot reserve unbounded memory.
fn qq_attachment_byte_cap(committed: usize) -> usize {
    QQ_ATTACHMENT_MAX_BYTES.min(QQ_ATTACHMENT_MESSAGE_MAX_BYTES.saturating_sub(committed))
}

fn parse_attachments(data: &Value) -> Vec<QqAttachment> {
    data.get("attachments")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let url = item.get("url").and_then(Value::as_str)?.trim();
                    if url.is_empty() {
                        return None;
                    }
                    Some(QqAttachment {
                        url: url.to_string(),
                        filename: item
                            .get("filename")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        size: item.get("size").and_then(Value::as_u64).unwrap_or_default(),
                        content_type: item
                            .get("content_type")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn qq_attachment_label(attachment: &QqAttachment, index: usize) -> String {
    let raw = attachment.filename.trim();
    if raw.is_empty() {
        return format!("attachment-{index}");
    }
    raw.chars().filter(|ch| !ch.is_control()).take(80).collect()
}

fn qq_attachment_reference(unit: &str, index: usize) -> String {
    format!("{unit}:{index}")
}

/// Two attachments of one message may share a name, so the index keeps their
/// files apart even after the name is sanitized. The directory is a digest of
/// the full message identity: platform ids exceed the path-segment limit, and
/// a truncated id would make two messages share one directory.
fn qq_attachment_relative_path(unit: &str, name: &str, index: usize) -> PathBuf {
    PathBuf::from("qq")
        .join("files")
        .join(qq_message_unit_digest(unit))
        .join(index.to_string())
        .join(sanitize_path_segment(name))
}

fn qq_message_unit_digest(unit: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(unit.as_bytes());
    let digest = hasher.finalize();
    format!("{digest:x}")[..24].to_string()
}

/// Every attachment the message budget skipped gets its own issue, so the
/// notice and the manifest agree on how many were skipped.
fn qq_budget_skipped_attachments(
    message: &QqInboundMessage,
    unit: &str,
    from: usize,
) -> (Vec<ContextSyncIssueInput>, Vec<String>) {
    let reason =
        format!("skipped: message exceeds {QQ_ATTACHMENT_MESSAGE_MAX_BYTES} bytes of attachments");
    let mut unresolved = Vec::new();
    let mut failures = Vec::new();
    for index in from..message.attachments.len() {
        let name = qq_attachment_label(&message.attachments[index], index);
        unresolved.push(ContextSyncIssueInput {
            kind: QQ_ATTACHMENT_KIND.to_string(),
            reference: qq_attachment_reference(unit, index),
            reason: reason.clone(),
        });
        failures.push(format!("{name}: {reason}"));
    }
    (unresolved, failures)
}

/// Attachment-only messages carry no text; keep a placeholder so the turn still
/// routes and the agent knows what the synced files are.
fn qq_message_text(text: String, attachments: &[QqAttachment]) -> String {
    if !text.is_empty() {
        return text;
    }
    let names = attachments
        .iter()
        .enumerate()
        .map(|(index, attachment)| qq_attachment_label(attachment, index))
        .collect::<Vec<_>>()
        .join(", ");
    if names.is_empty() {
        QQ_ATTACHMENT_PLACEHOLDER.to_string()
    } else {
        format!("{QQ_ATTACHMENT_PLACEHOLDER} {names}")
    }
}

fn parse_c2c_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    let msg_id = data.get("id").and_then(Value::as_str)?.to_string();
    let msg_idx = qq_parse_message_idx(data);
    let author = data.get("author")?;
    let user_id = author
        .get("user_openid")
        .or_else(|| author.get("id"))
        .and_then(Value::as_str)?
        .to_string();
    let text = data
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let attachments = parse_attachments(data);
    if text.is_empty() && attachments.is_empty() {
        return None;
    }
    Some(QqInboundMessage {
        event_key: qq_event_key(event_type, &msg_id, msg_idx.as_deref()),
        msg_id,
        msg_idx,
        session_key: format!("qq:c2c:{user_id}"),
        user_id: user_id.clone(),
        group_id: None,
        text: qq_message_text(text, &attachments),
        attachments,
        target: QqReplyTarget::C2c { openid: user_id },
    })
}

fn parse_group_at_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    let msg_id = data.get("id").and_then(Value::as_str)?.to_string();
    let msg_idx = qq_parse_message_idx(data);
    let group_id = data
        .get("group_openid")
        .and_then(Value::as_str)?
        .to_string();
    let author = data.get("author")?;
    let user_id = author
        .get("member_openid")
        .or_else(|| author.get("id"))
        .and_then(Value::as_str)?
        .to_string();
    let text = data
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let attachments = parse_attachments(data);
    if text.is_empty() && attachments.is_empty() {
        return None;
    }
    Some(QqInboundMessage {
        event_key: qq_event_key(event_type, &msg_id, msg_idx.as_deref()),
        msg_id,
        msg_idx,
        session_key: format!("qq:group:{group_id}"),
        user_id,
        group_id: Some(group_id.clone()),
        text: qq_message_text(text, &attachments),
        attachments,
        target: QqReplyTarget::Group {
            group_openid: group_id,
        },
    })
}

fn qq_attachment_http_client() -> Client {
    Client::builder()
        .redirect(Policy::none())
        .timeout(QQ_ATTACHMENT_TIMEOUT)
        .build()
        .expect("QQ attachment HTTP client configuration must be valid")
}

fn parse_qq_attachment_url(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).map_err(|_| anyhow::anyhow!("attachment URL is invalid"))?;
    anyhow::ensure!(url.scheme() == "https", "attachment URL must use https");
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("attachment URL has no host"))?;
    anyhow::ensure!(
        is_allowed_qq_attachment_host(host),
        "attachment host `{host}` is not allowed"
    );
    Ok(url)
}

fn is_allowed_qq_attachment_host(host: &str) -> bool {
    QQ_ATTACHMENT_HOSTS.iter().any(|domain| {
        host == *domain
            || host
                .strip_suffix(domain)
                .is_some_and(|prefix| prefix.ends_with('.'))
    })
}

fn qq_context_request(
    message: &QqInboundMessage,
    downloaded: Vec<DownloadedQqAttachment>,
    unresolved: Vec<ContextSyncIssueInput>,
) -> ContextSyncRequest {
    let unit = qq_message_unit(&message.msg_id, message.msg_idx.as_deref());
    let artifacts = downloaded
        .into_iter()
        .map(|file| {
            let reference = qq_attachment_reference(&unit, file.index);
            let mut metadata = BTreeMap::new();
            metadata.insert("message_id".to_string(), json!(message.msg_id));
            if let Some(msg_idx) = message.msg_idx.as_deref() {
                metadata.insert("message_idx".to_string(), json!(msg_idx));
            }
            metadata.insert("filename".to_string(), json!(file.name));
            metadata.insert("size_bytes".to_string(), json!(file.bytes.len()));
            metadata.insert(
                "content_type".to_string(),
                json!(file.attachment.content_type),
            );
            metadata.insert(
                "resolves_unresolved".to_string(),
                json!([{"kind": QQ_ATTACHMENT_KIND, "reference": reference}]),
            );
            ContextArtifactInput {
                id: format!("qq:file:{unit}:{}", file.index),
                kind: QQ_ATTACHMENT_KIND.to_string(),
                title: format!("QQ attachment: {}", file.name),
                source_locator: None,
                files: vec![ContextFileInput {
                    relative_path: file.relative_path,
                    content: ContextFileContent::Bytes(file.bytes),
                }],
                metadata,
            }
        })
        .collect();
    ContextSyncRequest {
        session_key: message.session_key.clone(),
        source: "qq".to_string(),
        base_path: PathBuf::from("qq"),
        artifacts,
        remove_artifacts: Vec::new(),
        unresolved,
    }
}

async fn send_heartbeat<S>(
    sink: &mut S,
    sequence: Option<i64>,
    timeout: Duration,
) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout(
        timeout,
        sink.send(Message::Text(
            json!({"op": 1, "d": sequence}).to_string().into(),
        )),
    )
    .await
    .map_err(|_| anyhow::anyhow!("QQ heartbeat write timed out"))??;
    Ok(())
}

fn message_text(message: Message) -> anyhow::Result<Option<String>> {
    match message {
        Message::Text(text) => Ok(Some(text.to_string())),
        Message::Binary(bytes) => Ok(Some(String::from_utf8(bytes.to_vec())?)),
        _ => Ok(None),
    }
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
}

fn truncate_for_error(mut body: String) -> String {
    const MAX_ERROR_BODY: usize = 512;
    if body.len() > MAX_ERROR_BODY {
        body.truncate(MAX_ERROR_BODY);
        body.push_str("...");
    }
    body
}

#[derive(Debug)]
struct QqSendMessageError {
    status: StatusCode,
    body: String,
}

impl std::fmt::Display for QqSendMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QQ send message failed ({}): {}", self.status, self.body)
    }
}

impl std::error::Error for QqSendMessageError {}

fn qq_error_allows_text_fallback(err: &anyhow::Error) -> bool {
    err.downcast_ref::<QqSendMessageError>().is_some_and(|err| {
        err.status == StatusCode::BAD_REQUEST
            && qq_error_body_is_markdown_payload_rejection(&err.body)
    })
}

fn qq_error_body_is_markdown_payload_rejection(body: &str) -> bool {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        if value
            .get("code")
            .or_else(|| value.get("err_code"))
            .and_then(value_as_u64)
            == Some(11255)
        {
            return true;
        }
        let message = ["message", "msg", "error"]
            .into_iter()
            .filter_map(|key| value.get(key).and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        return message.contains("markdown")
            || message.contains("msg_type")
            || message.contains("body");
    }

    let body = body.to_ascii_lowercase();
    body.contains("markdown") || body.contains("msg_type") || body.contains("\"code\":11255")
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc, time::Duration};

    use crate::approval::ApprovalBroker;
    use crate::router::{
        ChannelRouteTicket, RouterInput, RouterOutputSink, TurnBeginMode, TurnReservation,
    };
    use crate::session::ContextSyncRequest;
    use crate::session::context::{read_context_artifacts_from_manifest, write_context_sync};
    use serde_json::json;
    use std::path::Path;
    use tokio::sync::Mutex;

    use super::*;

    async fn test_gateway() -> (
        tokio::task::JoinHandle<anyhow::Result<()>>,
        tokio_tungstenite::WebSocketStream<tokio::io::DuplexStream>,
    ) {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let (client, server) = tokio::io::duplex(4096);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let channel = QqBotChannel::new(
            QqConfig {
                enabled: true,
                app_id: String::new(),
                client_secret: String::new(),
                sandbox: false,
                intents: 0,
                channel_events: ChannelEventMode::Off,
                allowed_users: BTreeSet::new(),
                allowed_groups: BTreeSet::new(),
            },
            Arc::new(ApprovalBroker::default()),
        );
        let task = tokio::spawn(async move {
            channel
                .run_gateway(client, "test-token", Arc::new(RecordingRouter::default()))
                .await
        });
        server
            .send(Message::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 1000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let identify = server.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(identify.to_text().unwrap()).unwrap()["op"],
            2
        );
        server
            .send(Message::Text(
                json!({"op": 0, "t": "READY", "s": 42, "d": {"session_id": "test-session"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        (task, server)
    }

    #[tokio::test(start_paused = true)]
    async fn gateway_missing_heartbeat_ack_disconnects_even_with_other_frames() {
        let (task, mut server) = test_gateway().await;
        let heartbeat = server.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(heartbeat.to_text().unwrap()).unwrap(),
            json!({"op": 1, "d": 42})
        );
        server.send(Message::Ping(vec![1].into())).await.unwrap();
        assert!(matches!(
            server.next().await.unwrap().unwrap(),
            Message::Pong(_)
        ));
        let sent_at = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_millis(500)).await;
        server
            .send(Message::Text(json!({"op": 1}).to_string().into()))
            .await
            .unwrap();
        let heartbeat = server.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(heartbeat.to_text().unwrap()).unwrap()["op"],
            1
        );
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("heartbeat ACK timed out"));
        assert_eq!(sent_at.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn gateway_heartbeat_ack_keeps_connection_alive() {
        let (task, mut server) = test_gateway().await;
        for _ in 0..3 {
            let heartbeat = server.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(heartbeat.to_text().unwrap()).unwrap()["op"],
                1
            );
            server
                .send(Message::Text(json!({"op": 11}).to_string().into()))
                .await
                .unwrap();
        }
        server
            .send(Message::Text(json!({"op": 7}).to_string().into()))
            .await
            .unwrap();
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("requested reconnect"));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_write_times_out_when_peer_stops_reading() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let (client, _server) = tokio::io::duplex(1);
        let mut client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let error = send_heartbeat(&mut client, None, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("heartbeat write timed out"));
    }

    #[derive(Debug, Default)]
    struct RecordingRouter {
        handled: Mutex<Vec<RouterInput>>,
        reserved: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl RouterService for RecordingRouter {
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

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct IntakeOnlyRouter {
        began: Mutex<Vec<ChannelInput>>,
        finished: Mutex<usize>,
        contexts: Mutex<Vec<Option<ContextSyncRequest>>>,
        synced: Mutex<usize>,
        allow_context: bool,
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
                context_allowed: self.allow_context,
            })
        }

        async fn sync_context(&self, _request: ContextSyncRequest) -> anyhow::Result<()> {
            *self.synced.lock().await += 1;
            Ok(())
        }

        async fn finish_channel_input(
            &self,
            _ticket: ChannelRouteTicket,
            context: Option<ContextSyncRequest>,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            *self.finished.lock().await += 1;
            self.contexts.lock().await.push(context);
            Ok(())
        }

        async fn reserve_turn(
            &self,
            _session_key: &str,
            _mode: TurnBeginMode,
        ) -> anyhow::Result<Option<TurnReservation>> {
            panic!("QQ adapter should not reserve turns directly")
        }

        async fn handle(
            &self,
            _input: RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            panic!("QQ adapter should not handle routed input directly")
        }

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            panic!("QQ adapter should not observe input directly")
        }
    }

    #[test]
    fn parses_c2c_message() {
        let data = json!({
            "id": "m1",
            "content": " hello ",
            "author": {
                "id": "legacy",
                "user_openid": "u_open"
            }
        });

        let message = parse_inbound_message("C2C_MESSAGE_CREATE", &data).unwrap();

        assert_eq!(message.event_key, "C2C_MESSAGE_CREATE:m1");
        assert_eq!(message.session_key, "qq:c2c:u_open");
        assert_eq!(message.user_id, "u_open");
        assert_eq!(message.group_id, None);
        assert_eq!(message.text, "hello");
        assert_eq!(
            message.target,
            QqReplyTarget::C2c {
                openid: "u_open".to_string()
            }
        );
    }

    #[test]
    fn parses_group_at_message() {
        let data = json!({
            "id": "m2",
            "content": " /agent status ",
            "group_openid": "g_open",
            "author": {
                "member_openid": "member_open"
            }
        });

        let message = parse_inbound_message("GROUP_AT_MESSAGE_CREATE", &data).unwrap();

        assert_eq!(message.event_key, "GROUP_AT_MESSAGE_CREATE:m2");
        assert_eq!(message.session_key, "qq:group:g_open");
        assert_eq!(message.user_id, "member_open");
        assert_eq!(message.group_id.as_deref(), Some("g_open"));
        assert_eq!(message.text, "/agent status");
        assert_eq!(
            message.target,
            QqReplyTarget::Group {
                group_openid: "g_open".to_string()
            }
        );
    }

    #[test]
    fn ignores_empty_or_unknown_events() {
        let empty = json!({
            "id": "m1",
            "content": " ",
            "author": {
                "user_openid": "u_open"
            }
        });

        assert!(parse_inbound_message("C2C_MESSAGE_CREATE", &empty).is_none());
        assert!(parse_inbound_message("READY", &json!({})).is_none());
    }

    #[test]
    fn parses_attachment_only_c2c_message() {
        let data = json!({
            "id": "m3",
            "content": "",
            "author": {
                "user_openid": "u_open"
            },
            "attachments": [{
                "url": "https://multimedia.nt.qq.com.cn/download?fileid=abc",
                "filename": "协议草案 v2.pdf",
                "size": 4096,
                "content_type": "file"
            }]
        });

        let message = parse_inbound_message("C2C_MESSAGE_CREATE", &data).unwrap();

        assert_eq!(message.session_key, "qq:c2c:u_open");
        assert_eq!(message.text, "[attachment] 协议草案 v2.pdf");
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].size, 4096);
        assert_eq!(message.attachments[0].content_type, "file");
    }

    #[test]
    fn attachment_message_keeps_its_text() {
        let data = json!({
            "id": "m4",
            "content": " 看一下这个 ",
            "author": {
                "user_openid": "u_open"
            },
            "attachments": [{
                "url": "https://gchat.qpic.cn/shot.png",
                "filename": "shot.png"
            }]
        });

        let message = parse_inbound_message("C2C_MESSAGE_CREATE", &data).unwrap();

        assert_eq!(message.text, "看一下这个");
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].size, 0);
        assert_eq!(message.attachments[0].content_type, "");
    }

    #[test]
    fn qq_attachment_urls_are_restricted_to_tencent_hosts() {
        assert!(
            parse_qq_attachment_url("https://multimedia.nt.qq.com.cn/download?fileid=abc").is_ok()
        );
        assert!(parse_qq_attachment_url("https://gchat.qpic.cn/a/b.png").is_ok());
        assert!(parse_qq_attachment_url("https://qq.com/x").is_ok());
        assert!(parse_qq_attachment_url("http://multimedia.nt.qq.com.cn/x").is_err());
        assert!(parse_qq_attachment_url("https://evilqq.com/x").is_err());
        assert!(parse_qq_attachment_url("https://qq.com.evil.test/x").is_err());
        assert!(parse_qq_attachment_url("https://127.0.0.1/x").is_err());
        assert!(parse_qq_attachment_url("https://files.slack.com/x").is_err());
    }

    #[test]
    fn qq_context_request_carries_downloaded_files_and_failures() {
        let message = parse_inbound_message(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": "ROBOT1.0_msg",
                "content": "",
                "author": {
                    "user_openid": "u_open"
                },
                "attachments": [{
                    "url": "https://multimedia.nt.qq.com.cn/download?fileid=abc",
                    "filename": "../协议 v2.pdf",
                    "size": 8,
                    "content_type": "file"
                }]
            }),
        )
        .unwrap();

        let request = qq_context_request(
            &message,
            vec![DownloadedQqAttachment {
                attachment: message.attachments[0].clone(),
                index: 0,
                name: "协议 v2.pdf".to_string(),
                relative_path: PathBuf::from("qq/files/ROBOT1.0_msg/v2.pdf"),
                bytes: b"contents".to_vec(),
            }],
            vec![ContextSyncIssueInput {
                kind: QQ_ATTACHMENT_KIND.to_string(),
                reference: "ROBOT1.0_msg:1".to_string(),
                reason: "download rejected with 403 Forbidden".to_string(),
            }],
        );

        assert_eq!(request.session_key, "qq:c2c:u_open");
        assert_eq!(request.source, "qq");
        assert_eq!(request.base_path, PathBuf::from("qq"));
        assert!(request.remove_artifacts.is_empty());
        assert_eq!(request.unresolved.len(), 1);
        assert_eq!(request.artifacts.len(), 1);
        let artifact = &request.artifacts[0];
        assert_eq!(artifact.id, "qq:file:ROBOT1.0_msg:0");
        assert_eq!(artifact.kind, "qq_file");
        assert_eq!(artifact.title, "QQ attachment: 协议 v2.pdf");
        assert_eq!(artifact.files.len(), 1);
        assert_eq!(
            artifact.files[0].relative_path,
            PathBuf::from("qq/files/ROBOT1.0_msg/v2.pdf")
        );
        match &artifact.files[0].content {
            ContextFileContent::Bytes(bytes) => assert_eq!(bytes, b"contents"),
            other => panic!("unexpected attachment content: {other:?}"),
        }
        assert_eq!(artifact.metadata["filename"], json!("协议 v2.pdf"));
        assert_eq!(artifact.metadata["size_bytes"], json!(8));
        assert_eq!(
            artifact.metadata["resolves_unresolved"],
            json!([{"kind": "qq_file", "reference": "ROBOT1.0_msg:0"}])
        );
    }

    #[test]
    fn message_idx_separates_events_that_share_a_message_id() {
        let first = json!({
            "id": "m5",
            "content": "",
            "author": { "user_openid": "u_open" },
            "message_scene": { "source": "default", "ext": ["msg_idx=REFIDX_one=="] },
            "attachments": [{ "url": "https://gchat.qpic.cn/a.png", "filename": "a.png" }]
        });
        let second = json!({
            "id": "m5",
            "content": "",
            "author": { "user_openid": "u_open" },
            "message_scene": { "source": "default", "ext": ["msg_idx=REFIDX_two=="] },
            "attachments": [{ "url": "https://gchat.qpic.cn/b.png", "filename": "b.png" }]
        });
        let retry = second.clone();

        let first = parse_inbound_message("C2C_MESSAGE_CREATE", &first).unwrap();
        let second = parse_inbound_message("C2C_MESSAGE_CREATE", &second).unwrap();
        let retry = parse_inbound_message("C2C_MESSAGE_CREATE", &retry).unwrap();

        assert_eq!(first.msg_idx.as_deref(), Some("REFIDX_one=="));
        assert_ne!(first.event_key, second.event_key);
        // A retry of one message repeats its index and stays deduplicated.
        assert_eq!(second.event_key, retry.event_key);
        assert_ne!(
            qq_message_unit(&first.msg_id, first.msg_idx.as_deref()),
            qq_message_unit(&second.msg_id, second.msg_idx.as_deref())
        );
    }

    /// The message id observed in production, longer than the 96 byte path
    /// segment limit.
    const LONG_MSG_ID: &str = concat!(
        "ROBOT1.0_ERH.4md9HCFavedKYo4nU7202-W3.QbvQCmliJLKBXUYPPZHZE4.SmUgLjdSWqGic",
        "LeJ9zC4ObIWZ2x3kd1rivpYgaf4C5XyaLdtdKVzqgg!"
    );

    #[test]
    fn attachment_paths_keep_distinct_messages_and_files_apart() {
        let first = qq_message_unit(LONG_MSG_ID, Some("REFIDX_one=="));
        let second = qq_message_unit(LONG_MSG_ID, Some("REFIDX_two=="));

        // Platform ids exceed a path segment, so the directory is a digest of
        // the whole identity rather than a truncated id.
        assert!(LONG_MSG_ID.len() > 96);
        let digest = qq_message_unit_digest(&first);
        assert_eq!(digest.len(), 24);
        assert_ne!(digest, qq_message_unit_digest(&second));
        assert_ne!(digest, qq_message_unit_digest(LONG_MSG_ID));

        let first_pdf = qq_attachment_relative_path(&first, "协议.pdf", 0);
        let second_pdf = qq_attachment_relative_path(&second, "协议.pdf", 0);
        assert_ne!(first_pdf, second_pdf, "same id, different message index");
        assert_eq!(first_pdf, PathBuf::from(format!("qq/files/{digest}/0/pdf")));

        // Same-named files of one message stay apart through the index.
        assert_ne!(
            qq_attachment_relative_path(&first, "same.pdf", 0),
            qq_attachment_relative_path(&first, "same.pdf", 1)
        );
        // Chinese names sanitize to the same segment.
        assert_eq!(
            sanitize_path_segment("甲.pdf"),
            sanitize_path_segment("乙.pdf")
        );
    }

    #[test]
    fn attachment_files_survive_manifest_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let message = parse_inbound_message(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": LONG_MSG_ID,
                "content": "",
                "author": {
                    "user_openid": "u_open"
                },
                "message_scene": {
                    "source": "default",
                    "ext": ["msg_idx=REFIDX_real=="]
                },
                "attachments": [
                    { "url": "https://gchat.qpic.cn/a.pdf", "filename": "协议.pdf" },
                    { "url": "https://gchat.qpic.cn/b.pdf", "filename": "协议.pdf" }
                ]
            }),
        )
        .unwrap();
        let unit = qq_message_unit(&message.msg_id, message.msg_idx.as_deref());
        let downloaded = ["first", "second"]
            .iter()
            .enumerate()
            .map(|(index, body)| DownloadedQqAttachment {
                attachment: message.attachments[index].clone(),
                index,
                name: "协议.pdf".to_string(),
                relative_path: qq_attachment_relative_path(&unit, "协议.pdf", index),
                bytes: body.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>();

        write_context_sync(
            dir.path(),
            qq_context_request(&message, downloaded, Vec::new()),
            &[],
        )
        .unwrap();
        let recovered = read_context_artifacts_from_manifest(
            dir.path(),
            "qq",
            &message.session_key,
            Path::new("qq"),
        )
        .unwrap();

        let files = recovered
            .iter()
            .filter(|record| record.kind == QQ_ATTACHMENT_KIND)
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 2, "{recovered:?}");
        assert_ne!(files[0].paths, files[1].paths);
        assert_eq!(
            std::fs::read(dir.path().join(&files[0].paths[0])).unwrap(),
            b"first"
        );
        assert_eq!(
            std::fs::read(dir.path().join(&files[1].paths[0])).unwrap(),
            b"second"
        );
    }

    #[test]
    fn attachment_byte_cap_shrinks_with_the_message_budget() {
        assert_eq!(qq_attachment_byte_cap(0), QQ_ATTACHMENT_MAX_BYTES);
        assert_eq!(
            qq_attachment_byte_cap(QQ_ATTACHMENT_MESSAGE_MAX_BYTES - 1024),
            1024
        );
        assert_eq!(qq_attachment_byte_cap(QQ_ATTACHMENT_MESSAGE_MAX_BYTES), 0);
        assert_eq!(
            qq_attachment_byte_cap(QQ_ATTACHMENT_MESSAGE_MAX_BYTES + 1),
            0
        );
    }

    #[tokio::test]
    async fn slash_command_does_not_preempt() {
        let channel = QqBotChannel::new(
            QqConfig {
                enabled: true,
                app_id: String::new(),
                client_secret: String::new(),
                sandbox: false,
                intents: 0,
                channel_events: ChannelEventMode::Off,
                allowed_users: BTreeSet::new(),
                allowed_groups: BTreeSet::new(),
            },
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(RecordingRouter::default());
        let data = json!({
            "id": "m1",
            "content": "/status",
            "author": {
                "id": "legacy",
                "user_openid": "u_open"
            }
        });

        channel
            .handle_dispatch("C2C_MESSAGE_CREATE", &data, router.clone())
            .await
            .unwrap();

        assert!(router.reserved.lock().await.is_empty());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !router.handled.lock().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QQ slash command was not routed");
        let handled = router.handled.lock().await;
        assert_eq!(handled[0].text, "/status");
    }

    #[tokio::test]
    async fn qq_adapter_uses_channel_intake_interface_for_routed_messages() {
        let channel = QqBotChannel::new(
            QqConfig {
                enabled: true,
                app_id: String::new(),
                client_secret: String::new(),
                sandbox: false,
                intents: 0,
                channel_events: ChannelEventMode::Off,
                allowed_users: BTreeSet::new(),
                allowed_groups: BTreeSet::new(),
            },
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(IntakeOnlyRouter::default());
        let data = json!({
            "id": "m1",
            "content": "hello",
            "author": {
                "id": "legacy",
                "user_openid": "u_open"
            }
        });

        channel
            .handle_dispatch("C2C_MESSAGE_CREATE", &data, router.clone())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if *router.finished.lock().await > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QQ routed message did not finish through intake");
        let began = router.began.lock().await;
        assert_eq!(began.len(), 1);
        assert_eq!(began[0].intent, ChannelInputIntent::Route);
    }

    #[tokio::test]
    async fn attachment_context_travels_through_the_ticket_finish_path() {
        let channel = QqBotChannel::new(
            QqConfig {
                enabled: true,
                app_id: String::new(),
                client_secret: String::new(),
                sandbox: false,
                intents: 0,
                channel_events: ChannelEventMode::Off,
                allowed_users: BTreeSet::new(),
                allowed_groups: BTreeSet::new(),
            },
            Arc::new(ApprovalBroker::default()),
        );
        let router = Arc::new(IntakeOnlyRouter {
            allow_context: true,
            ..IntakeOnlyRouter::default()
        });
        // A host outside the QQ media domains fails before any request is sent,
        // so the turn still carries the failure as an unresolved issue.
        let data = json!({
            "id": "m2",
            "content": "",
            "author": {
                "user_openid": "u_open"
            },
            "attachments": [{
                "url": "https://example.com/协议.pdf",
                "filename": "协议.pdf",
                "size": 8,
                "content_type": "file"
            }]
        });

        channel
            .handle_dispatch("C2C_MESSAGE_CREATE", &data, router.clone())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if *router.finished.lock().await > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("QQ attachment message did not finish through intake");

        let contexts = router.contexts.lock().await;
        let request = contexts[0]
            .as_ref()
            .expect("the turn must receive the attachment context");
        assert_eq!(request.source, "qq");
        assert_eq!(request.base_path, PathBuf::from("qq"));
        assert!(request.artifacts.is_empty());
        assert_eq!(request.unresolved.len(), 1);
        assert_eq!(request.unresolved[0].kind, QQ_ATTACHMENT_KIND);
        assert!(
            request.unresolved[0].reason.contains("not allowed"),
            "{}",
            request.unresolved[0].reason
        );
        drop(contexts);
        // The turn owns the context commit; the channel must not sync it early.
        assert_eq!(*router.synced.lock().await, 0);
    }

    fn qq_attachment_message(msg_id: &str, names: &[&str]) -> QqInboundMessage {
        let attachments = names
            .iter()
            .map(|name| json!({ "url": "https://gchat.qpic.cn/x", "filename": name }))
            .collect::<Vec<_>>();
        parse_inbound_message(
            "C2C_MESSAGE_CREATE",
            &json!({
                "id": msg_id,
                "content": "",
                "author": {
                    "user_openid": "u_open"
                },
                "attachments": attachments
            }),
        )
        .unwrap()
    }

    #[test]
    fn budget_skips_are_reported_per_attachment() {
        let message = qq_attachment_message("m-budget", &["a.pdf", "b.pdf", "c.pdf"]);
        let unit = qq_message_unit(&message.msg_id, None);

        let (unresolved, failures) = qq_budget_skipped_attachments(&message, &unit, 1);

        assert_eq!(unresolved.len(), 2);
        assert_eq!(failures.len(), 2);
        assert_eq!(unresolved[0].reference, format!("{unit}:1"));
        assert_eq!(unresolved[1].reference, format!("{unit}:2"));
        assert!(unresolved[0].reason.contains("skipped"));
        assert!(failures[0].starts_with("b.pdf:"), "{}", failures[0]);
        assert!(failures[1].starts_with("c.pdf:"), "{}", failures[1]);
    }

    fn parses_numeric_and_string_expiry_values() {
        assert_eq!(value_as_u64(&json!(12)), Some(12));
        assert_eq!(value_as_u64(&json!("34")), Some(34));
        assert_eq!(value_as_u64(&json!("bad")), None);
    }

    #[test]
    fn qq_markdown_message_body_uses_markdown_payload() {
        let text = "# Title\n\n**bold**\n\n- item";

        let body = qq_markdown_message_body("msg-1", 7, text).unwrap();

        assert_eq!(body["msg_type"], QQ_MSG_TYPE_MARKDOWN);
        assert_eq!(body["msg_id"], "msg-1");
        assert_eq!(body["msg_seq"], 7);
        assert_eq!(body["markdown"]["content"], text);
        assert!(body.get("content").is_none());
    }

    #[test]
    fn qq_markdown_message_body_falls_back_for_empty_text() {
        assert!(qq_markdown_message_body("msg-1", 1, "").is_none());
        assert!(qq_markdown_message_body("msg-1", 1, "   ").is_none());
    }

    #[test]
    fn qq_text_message_body_uses_text_payload() {
        let body = qq_text_message_body("msg-1", 3, "**literal**");

        assert_eq!(body["msg_type"], QQ_MSG_TYPE_TEXT);
        assert_eq!(body["msg_id"], "msg-1");
        assert_eq!(body["msg_seq"], 3);
        assert_eq!(body["content"], "**literal**");
        assert!(body.get("markdown").is_none());
    }

    #[test]
    fn qq_markdown_errors_allow_text_fallback_only_for_invalid_payloads() {
        let invalid_markdown = anyhow::Error::new(QqSendMessageError {
            status: StatusCode::BAD_REQUEST,
            body: r#"{"message":"invalid request","code":11255}"#.to_string(),
        });
        assert!(qq_error_allows_text_fallback(&invalid_markdown));

        let rate_limited = anyhow::Error::new(QqSendMessageError {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: r#"{"message":"msg limit exceed"}"#.to_string(),
        });
        assert!(!qq_error_allows_text_fallback(&rate_limited));

        let invalid_msg_seq = anyhow::Error::new(QqSendMessageError {
            status: StatusCode::BAD_REQUEST,
            body: r#"{"message":"invalid msg_seq"}"#.to_string(),
        });
        assert!(!qq_error_allows_text_fallback(&invalid_msg_seq));

        let invalid_markdown_message = anyhow::Error::new(QqSendMessageError {
            status: StatusCode::BAD_REQUEST,
            body: r#"{"message":"invalid markdown body"}"#.to_string(),
        });
        assert!(qq_error_allows_text_fallback(&invalid_markdown_message));
    }

    #[tokio::test]
    async fn older_reply_context_sequence_does_not_overwrite_reply_context() {
        let channel = QqBotChannel::new(
            QqConfig {
                enabled: true,
                app_id: String::new(),
                client_secret: String::new(),
                sandbox: false,
                intents: 0,
                channel_events: ChannelEventMode::Off,
                allowed_users: BTreeSet::new(),
                allowed_groups: BTreeSet::new(),
            },
            Arc::new(ApprovalBroker::default()),
        );
        let older = QqInboundMessage {
            event_key: "old".to_string(),
            session_key: "qq:group:g1".to_string(),
            user_id: "u1".to_string(),
            group_id: Some("g1".to_string()),
            text: "old".to_string(),
            msg_idx: None,
            attachments: Vec::new(),
            target: QqReplyTarget::Group {
                group_openid: "old_group".to_string(),
            },
            msg_id: "old_msg".to_string(),
        };
        let newer = QqInboundMessage {
            event_key: "new".to_string(),
            session_key: "qq:group:g1".to_string(),
            user_id: "u1".to_string(),
            group_id: Some("g1".to_string()),
            text: "new".to_string(),
            msg_idx: None,
            attachments: Vec::new(),
            target: QqReplyTarget::Group {
                group_openid: "new_group".to_string(),
            },
            msg_id: "new_msg".to_string(),
        };

        channel.remember_reply_context(&newer, 2).await;
        channel.remember_reply_context(&older, 1).await;

        let contexts = channel.reply_contexts.lock().await;
        let context = contexts.get("qq:group:g1").unwrap();
        assert_eq!(
            context.target,
            QqReplyTarget::Group {
                group_openid: "new_group".to_string()
            }
        );
        assert_eq!(context.msg_id, "new_msg");
        assert_eq!(context.reply_context_sequence, 2);
    }
}
