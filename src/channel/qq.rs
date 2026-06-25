use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    approval::{SharedApprovalBroker, is_approval_command},
    channel::EventDeduper,
    config::{ChannelEventMode, QqConfig},
    router::{
        RouterChannelEvent, RouterInput, RouterOutputSink, RouterService, TurnBeginMode,
        TurnReservation, render_compact_channel_events,
    },
};

const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_SANDBOX_API_BASE: &str = "https://sandbox.api.sgroup.qq.com";
const QQ_AUTH_URL: &str = "https://bots.qq.com/app/getAppAccessToken";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
const QQ_MSG_TYPE_TEXT: u8 = 0;
const QQ_MSG_TYPE_MARKDOWN: u8 = 2;
#[derive(Debug, Clone)]
pub struct QqBotChannel {
    cfg: QqConfig,
    approvals: SharedApprovalBroker,
    http: Client,
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

        let mut heartbeat = tokio::time::interval(Duration::from_millis(heartbeat_interval));
        heartbeat.tick().await;

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    send_heartbeat(&mut sink, sequence).await?;
                }
                frame = stream.next() => {
                    let Some(frame) = frame else {
                        anyhow::bail!("QQ gateway stream ended");
                    };
                    match frame? {
                        Message::Text(text) => {
                            self.handle_gateway_payload(text.as_ref(), &mut sink, &mut sequence, router.clone()).await?;
                        }
                        Message::Binary(bytes) => {
                            let text = String::from_utf8(bytes.to_vec())?;
                            self.handle_gateway_payload(&text, &mut sink, &mut sequence, router.clone()).await?;
                        }
                        Message::Ping(payload) => sink.send(Message::Pong(payload)).await?,
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
            1 => send_heartbeat(sink, *sequence).await?,
            7 => anyhow::bail!("QQ gateway requested reconnect"),
            9 => {
                self.clear_gateway_session().await;
                anyhow::bail!("QQ gateway reported invalid session");
            }
            11 => {}
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

        if is_approval_command(&message.text) {
            let reply_context_sequence = self
                .next_reply_context_sequence
                .fetch_add(1, Ordering::Relaxed);
            let channel = self.clone();
            tokio::spawn(async move {
                if let Err(err) = channel
                    .route_message(message, None, reply_context_sequence, router)
                    .await
                {
                    tracing::warn!(error = %err, "failed to handle QQ approval command");
                }
            });
            return Ok(());
        }

        let command = message.text.split_whitespace().next().unwrap_or("");
        let reply_context_sequence = self
            .next_reply_context_sequence
            .fetch_add(1, Ordering::Relaxed);
        let turn_reservation = if matches!(command, "/stop" | "/agent" | "/yolo") {
            None
        } else {
            router
                .reserve_turn(&message.session_key, TurnBeginMode::ReplaceActive)
                .await?
        };
        let channel = self.clone();
        tokio::spawn(async move {
            let session_key = message.session_key.clone();
            if let Err(err) = channel
                .route_message(message, turn_reservation, reply_context_sequence, router)
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
        turn_reservation: Option<TurnReservation>,
        reply_context_sequence: u64,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        self.remember_reply_context(&message, reply_context_sequence)
            .await;
        tracing::info!(
            session_key = %message.session_key,
            user_id = %message.user_id,
            group_id = ?message.group_id,
            text_len = message.text.len(),
            "routing QQ message"
        );
        let mut output = QqRouterOutputSink {
            channel: self.clone(),
            session_key: message.session_key.clone(),
            target: message.target,
            msg_id: message.msg_id,
            channel_events: self.cfg.channel_events,
            pending_events: Vec::new(),
        };
        let input = RouterInput {
            session_key: message.session_key,
            text: message.text,
            user_id: Some(message.user_id),
        };
        if let Some(reservation) = turn_reservation {
            router
                .handle_reserved(input, reservation, None, &mut output)
                .await
        } else {
            router.handle(input, &mut output).await
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

struct QqRouterOutputSink {
    channel: QqBotChannel,
    session_key: String,
    target: QqReplyTarget,
    msg_id: String,
    channel_events: ChannelEventMode,
    pending_events: Vec<RouterChannelEvent>,
}

#[async_trait::async_trait]
impl RouterOutputSink for QqRouterOutputSink {
    fn send_channel_event(&mut self, event: RouterChannelEvent) {
        match self.channel_events {
            ChannelEventMode::Off => {}
            ChannelEventMode::Compact => self.pending_events.push(event),
            ChannelEventMode::Verbose => {
                let channel = self.channel.clone();
                let session_key = self.session_key.clone();
                let target = self.target.clone();
                let msg_id = self.msg_id.clone();
                tokio::spawn(async move {
                    if let Err(err) = channel
                        .send_reply_message(&session_key, &target, &msg_id, &event.render_text())
                        .await
                    {
                        tracing::warn!(error = %err, "failed to post QQ channel event");
                    }
                });
            }
        }
    }

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        if let Some(summary) = render_compact_channel_events(&self.pending_events)
            && let Err(err) = self
                .channel
                .send_reply_message(&self.session_key, &self.target, &self.msg_id, &summary)
                .await
        {
            tracing::warn!(error = %err, "failed to post compact QQ channel event summary");
        }
        self.channel
            .send_markdown_reply_message(&self.session_key, &self.target, &self.msg_id, &text)
            .await
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
    session_key: String,
    user_id: String,
    group_id: Option<String>,
    text: String,
    target: QqReplyTarget,
}

fn parse_inbound_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    match event_type {
        "C2C_MESSAGE_CREATE" => parse_c2c_message(event_type, data),
        "GROUP_AT_MESSAGE_CREATE" => parse_group_at_message(event_type, data),
        _ => None,
    }
}

fn parse_c2c_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    let msg_id = data.get("id").and_then(Value::as_str)?.to_string();
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
    if text.is_empty() {
        return None;
    }
    Some(QqInboundMessage {
        event_key: format!("{event_type}:{msg_id}"),
        msg_id,
        session_key: format!("qq:c2c:{user_id}"),
        user_id: user_id.clone(),
        group_id: None,
        text,
        target: QqReplyTarget::C2c { openid: user_id },
    })
}

fn parse_group_at_message(event_type: &str, data: &Value) -> Option<QqInboundMessage> {
    let msg_id = data.get("id").and_then(Value::as_str)?.to_string();
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
    if text.is_empty() {
        return None;
    }
    Some(QqInboundMessage {
        event_key: format!("{event_type}:{msg_id}"),
        msg_id,
        session_key: format!("qq:group:{group_id}"),
        user_id,
        group_id: Some(group_id.clone()),
        text,
        target: QqReplyTarget::Group {
            group_openid: group_id,
        },
    })
}

async fn send_heartbeat<S>(sink: &mut S, sequence: Option<i64>) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    sink.send(Message::Text(
        json!({"op": 1, "d": sequence}).to_string().into(),
    ))
    .await?;
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
    use std::collections::BTreeSet;

    use crate::approval::ApprovalBroker;
    use serde_json::json;

    use super::*;

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
