use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    approval::{SharedApprovalBroker, is_approval_command},
    channel::EventDeduper,
    config::QqConfig,
    router::{RouterInput, RouterService},
};

const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_SANDBOX_API_BASE: &str = "https://sandbox.api.sgroup.qq.com";
const QQ_AUTH_URL: &str = "https://bots.qq.com/app/getAppAccessToken";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
const SESSION_QUEUE_CAPACITY: usize = 16;
const SESSION_WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct QqBotChannel {
    cfg: QqConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    access_token: Arc<Mutex<Option<CachedAccessToken>>>,
    gateway_session: Arc<Mutex<QqGatewaySession>>,
    seen_events: Arc<Mutex<EventDeduper>>,
    reply_contexts: Arc<Mutex<HashMap<String, QqReplyContext>>>,
    session_workers: Arc<Mutex<HashMap<String, mpsc::Sender<QqInboundMessage>>>>,
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
            session_workers: Arc::new(Mutex::new(HashMap::new())),
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
            let channel = self.clone();
            tokio::spawn(async move {
                if let Err(err) = channel.route_message(message, router).await {
                    tracing::warn!(error = %err, "failed to handle QQ approval command");
                }
            });
            return Ok(());
        }

        self.enqueue_session_message(message, router).await
    }

    async fn enqueue_session_message(
        &self,
        message: QqInboundMessage,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        let session_key = message.session_key.clone();
        let sender = self.session_worker(&session_key, router).await;
        match sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.session_workers.lock().await.remove(&session_key);
                anyhow::bail!("QQ session worker stopped for session `{session_key}`");
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    session_key = %session_key,
                    capacity = SESSION_QUEUE_CAPACITY,
                    "QQ session queue is full; dropping inbound message"
                );
                Ok(())
            }
        }
    }

    async fn session_worker(
        &self,
        session_key: &str,
        router: Arc<dyn RouterService>,
    ) -> mpsc::Sender<QqInboundMessage> {
        let mut workers = self.session_workers.lock().await;
        if let Some(sender) = workers.get(session_key) {
            return sender.clone();
        }

        let (sender, receiver) = mpsc::channel(SESSION_QUEUE_CAPACITY);
        workers.insert(session_key.to_string(), sender.clone());
        self.clone()
            .spawn_session_worker(session_key.to_string(), router, receiver);
        sender
    }

    fn spawn_session_worker(
        self,
        session_key: String,
        router: Arc<dyn RouterService>,
        mut receiver: mpsc::Receiver<QqInboundMessage>,
    ) {
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(SESSION_WORKER_IDLE_TIMEOUT, receiver.recv()).await {
                    Ok(Some(message)) => {
                        if let Err(err) = self.route_message(message, router.clone()).await {
                            tracing::warn!(error = %err, session_key = %session_key, "failed to handle QQ session message");
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        self.session_workers.lock().await.remove(&session_key);
                        break;
                    }
                }
            }
        });
    }

    async fn route_message(
        &self,
        message: QqInboundMessage,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        self.remember_reply_context(&message).await;
        let reply = router
            .handle(RouterInput {
                session_key: message.session_key.clone(),
                text: message.text,
                user_id: Some(message.user_id),
            })
            .await?;
        self.send_reply_message(
            &message.session_key,
            &message.target,
            &message.msg_id,
            &reply.text,
        )
        .await
    }

    fn spawn_approval_notifier(self: Arc<Self>) {
        let mut prompts = self.approvals.subscribe();
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
                if let Err(err) = self
                    .send_session_message(&prompt.session_key, &prompt.render_text())
                    .await
                {
                    tracing::warn!(error = %err, "failed to post QQ approval prompt");
                }
            }
        });
    }

    async fn remember_reply_context(&self, message: &QqInboundMessage) {
        let mut contexts = self.reply_contexts.lock().await;
        let next_seq = contexts
            .get(&message.session_key)
            .map(|context| context.next_seq)
            .unwrap_or(1);
        contexts.insert(
            message.session_key.clone(),
            QqReplyContext {
                target: message.target.clone(),
                msg_id: message.msg_id.clone(),
                next_seq,
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

    async fn post_text_message(
        &self,
        target: &QqReplyTarget,
        msg_id: &str,
        msg_seq: u32,
        text: &str,
    ) -> anyhow::Result<()> {
        let token = self.token().await?;
        let url = format!(
            "{}/v2/{}/{}/messages",
            self.api_base(),
            target.scope(),
            target.id()
        );
        let body = json!({
            "content": text,
            "msg_type": 0,
            "msg_id": msg_id,
            "msg_seq": msg_seq,
        });
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
            anyhow::bail!("QQ send message failed ({status}): {body}");
        }
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

#[cfg(test)]
mod tests {
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
}
