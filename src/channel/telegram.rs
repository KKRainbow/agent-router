use std::{collections::BTreeSet, sync::Arc, time::Duration};

use reqwest::Client;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{
    approval::SharedApprovalBroker,
    channel::{
        EventDeduper,
        output::{ChannelOutputPolicy, ChannelOutputSink, ChannelReplyPort, PostedMessage},
    },
    config::{ChannelEventMode, TelegramConfig},
    router::{
        ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome,
        ChannelRouteTicket, RouterService,
    },
};

const TELEGRAM_API_BASE: &str = "https://api.telegram.org";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const TELEGRAM_MESSAGE_CHAR_LIMIT: usize = 4096;
const TELEGRAM_REPLY_DRAFT_PREVIEW_MAX_BYTES: usize = 3500;
const TELEGRAM_REPLY_DRAFT_TRUNCATED_PREFIX: &str = "...\n";
const TELEGRAM_REPLY_DRAFT_MARKER: &str = "[router-draft]";

#[derive(Debug, Clone)]
pub struct TelegramBotChannel {
    cfg: TelegramConfig,
    approvals: SharedApprovalBroker,
    http: Client,
    seen_updates: Arc<Mutex<EventDeduper>>,
}

impl TelegramBotChannel {
    pub fn new(cfg: TelegramConfig, approvals: SharedApprovalBroker) -> Self {
        Self {
            cfg,
            approvals,
            http: Client::new(),
            seen_updates: Arc::new(Mutex::new(EventDeduper::new(1024))),
        }
    }

    pub async fn run(self, router: Arc<dyn RouterService>) -> anyhow::Result<()> {
        self.validate_config()?;
        let channel = Arc::new(self);
        let bot = channel.startup_handshake_until_ready().await?;
        channel.clone().spawn_approval_notifier();
        let mut offset = None;

        loop {
            match channel.poll_once(router.clone(), &bot, offset).await {
                Ok(next_offset) => offset = next_offset.or(offset),
                Err(err) => {
                    tracing::warn!(error = %err, "Telegram long polling failed; retrying");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    async fn startup_handshake_until_ready(&self) -> anyhow::Result<TelegramBotIdentity> {
        loop {
            match self.startup_handshake().await {
                Ok(bot) => return Ok(bot),
                Err(err) if telegram_error_is_auth_failure(&err) => return Err(err),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "Telegram startup handshake failed; retrying"
                    );
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            }
        }
    }

    async fn startup_handshake(&self) -> anyhow::Result<TelegramBotIdentity> {
        let bot = self.get_me().await?;
        self.delete_webhook().await?;
        Ok(bot)
    }

    async fn poll_once(
        self: &Arc<Self>,
        router: Arc<dyn RouterService>,
        bot: &TelegramBotIdentity,
        offset: Option<i64>,
    ) -> anyhow::Result<Option<i64>> {
        let updates = self.get_updates(offset).await?;
        let mut next_offset = offset;
        for update in updates {
            let update_id = update.update_id;
            next_offset = Some(update.update_id + 1);
            if let Err(err) = self.handle_update(update, router.clone(), bot).await {
                tracing::warn!(
                    error = %err,
                    update_id,
                    "failed to handle Telegram update"
                );
            }
        }
        Ok(next_offset)
    }

    async fn handle_update(
        self: &Arc<Self>,
        update: TelegramUpdate,
        router: Arc<dyn RouterService>,
        bot: &TelegramBotIdentity,
    ) -> anyhow::Result<()> {
        if !self
            .seen_updates
            .lock()
            .await
            .insert(update.update_id.to_string())
        {
            return Ok(());
        }

        let Some(message) = parse_inbound_update(update, &self.cfg, bot) else {
            return Ok(());
        };
        let input = ChannelInput {
            session_key: message.session_key.clone(),
            text: message.text.clone(),
            user_id: message.user_id.clone(),
            source: "telegram".to_string(),
            intent: ChannelInputIntent::Route,
            context_policy: ChannelContextPolicy::disabled("telegram"),
        };
        let outcome = router.begin_channel_input(input).await?;
        let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
            return Ok(());
        };
        let channel = self.clone();
        tokio::spawn(async move {
            let session_key = message.session_key.clone();
            if let Err(err) = channel.route_message(message, ticket, router).await {
                tracing::warn!(
                    error = %err,
                    session_key = %session_key,
                    "failed to route Telegram message"
                );
            }
        });
        Ok(())
    }

    async fn route_message(
        &self,
        message: TelegramInboundMessage,
        ticket: ChannelRouteTicket,
        router: Arc<dyn RouterService>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            chat_id = %message.target.chat_id,
            message_thread_id = ?message.target.message_thread_id,
            session_key = %message.session_key,
            user_id = ?message.user_id,
            text_len = message.text.len(),
            "routing Telegram message"
        );
        let mut output = ChannelOutputSink::new(
            TelegramReplyPort {
                channel: self.clone(),
            },
            message.target,
            telegram_output_policy(self.cfg.channel_events),
        );
        router.finish_channel_input(ticket, None, &mut output).await
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
                let Some(target) = TelegramReplyTarget::from_session_key(&prompt.session_key)
                else {
                    continue;
                };
                if !prompt_channel.approvals.has_pending(&prompt.id).await {
                    continue;
                }
                let text = prompt.render_text();
                tokio::select! {
                    biased;
                    _ = prompt.cancelled() => continue,
                    result = prompt_channel.post_text_message(&target, &text) => {
                        if let Err(err) = result {
                            tracing::warn!(error = %err, "failed to post Telegram approval prompt");
                        }
                    }
                }
            }
        });
    }

    async fn get_me(&self) -> anyhow::Result<TelegramBotIdentity> {
        #[derive(Debug, Deserialize)]
        struct GetMeResult {
            id: i64,
            username: Option<String>,
        }

        let result: GetMeResult = self.api_request("getMe", json!({})).await?;
        let username = result
            .username
            .filter(|username| !username.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("Telegram getMe response omitted bot username"))?;
        Ok(TelegramBotIdentity {
            id: result.id.to_string(),
            username,
        })
    }

    async fn delete_webhook(&self) -> anyhow::Result<()> {
        let _: bool = self
            .api_request(
                "deleteWebhook",
                json!({
                    "drop_pending_updates": false,
                }),
            )
            .await?;
        Ok(())
    }

    async fn get_updates(&self, offset: Option<i64>) -> anyhow::Result<Vec<TelegramUpdate>> {
        let mut body = json!({
            "timeout": self.cfg.poll_timeout_secs,
            "allowed_updates": ["message"],
        });
        if let Some(offset) = offset {
            body["offset"] = Value::from(offset);
        }
        self.api_request("getUpdates", body).await
    }

    async fn post_text_message(
        &self,
        target: &TelegramReplyTarget,
        text: &str,
    ) -> anyhow::Result<Option<String>> {
        let mut first_id = None;
        for body in telegram_send_message_bodies(target, text) {
            let sent: TelegramSentMessage = self.api_request("sendMessage", body).await?;
            first_id.get_or_insert_with(|| sent.message_id.to_string());
        }
        Ok(first_id)
    }

    async fn update_text_message(
        &self,
        target: &TelegramReplyTarget,
        message_id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        if text.chars().count() > TELEGRAM_MESSAGE_CHAR_LIMIT {
            self.delete_message(target, message_id).await?;
            self.post_text_message(target, text).await?;
            return Ok(());
        }
        let body = telegram_edit_message_text_body(target, message_id, text)?;
        let _: Value = self.api_request("editMessageText", body).await?;
        Ok(())
    }

    async fn delete_message(
        &self,
        target: &TelegramReplyTarget,
        message_id: &str,
    ) -> anyhow::Result<()> {
        let body = telegram_delete_message_body(target, message_id)?;
        let _: bool = self.api_request("deleteMessage", body).await?;
        Ok(())
    }

    async fn api_request<T>(&self, method: &'static str, body: Value) -> anyhow::Result<T>
    where
        T: DeserializeOwned,
    {
        let resp = self
            .http
            .post(self.api_url(method))
            .json(&body)
            .send()
            .await
            .map_err(|err| TelegramTransportError::new(method, "request", err))?
            .json::<TelegramApiResponse<T>>()
            .await
            .map_err(|err| TelegramTransportError::new(method, "response decode", err))?;
        if !resp.ok {
            return Err(TelegramApiError {
                method,
                error_code: resp.error_code,
                description: resp
                    .description
                    .unwrap_or_else(|| "unknown_error".to_string()),
            }
            .into());
        }
        resp.result
            .ok_or_else(|| anyhow::anyhow!("Telegram {method} response omitted result"))
    }

    fn api_url(&self, method: &str) -> String {
        format!("{TELEGRAM_API_BASE}/bot{}/{method}", self.cfg.bot_token)
    }

    fn validate_config(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.cfg.bot_token.is_empty(),
            "TELEGRAM_BOT_TOKEN is required"
        );
        anyhow::ensure!(
            self.cfg.poll_timeout_secs > 0,
            "telegram.poll_timeout_secs must be greater than 0"
        );
        Ok(())
    }
}

#[derive(Clone)]
struct TelegramReplyPort {
    channel: TelegramBotChannel,
}

fn telegram_output_policy(activity_mode: ChannelEventMode) -> ChannelOutputPolicy {
    let mut policy = ChannelOutputPolicy::streaming_draft(activity_mode);
    policy.draft_preview_max_bytes = TELEGRAM_REPLY_DRAFT_PREVIEW_MAX_BYTES;
    policy.draft_truncated_prefix = TELEGRAM_REPLY_DRAFT_TRUNCATED_PREFIX;
    policy.draft_marker = TELEGRAM_REPLY_DRAFT_MARKER;
    policy
}

#[async_trait::async_trait]
impl ChannelReplyPort for TelegramReplyPort {
    type Target = TelegramReplyTarget;

    async fn post_text(&self, target: &Self::Target, text: &str) -> anyhow::Result<PostedMessage> {
        Ok(self
            .channel
            .post_text_message(target, text)
            .await?
            .map_or_else(PostedMessage::without_id, PostedMessage::with_id))
    }

    async fn post_markdown(
        &self,
        target: &Self::Target,
        text: &str,
    ) -> anyhow::Result<PostedMessage> {
        self.post_text(target, text).await
    }

    async fn update_text(&self, target: &Self::Target, id: &str, text: &str) -> anyhow::Result<()> {
        self.channel.update_text_message(target, id, text).await
    }

    async fn update_markdown(
        &self,
        target: &Self::Target,
        id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        self.update_text(target, id, text).await
    }

    async fn delete(&self, target: &Self::Target, id: &str) -> anyhow::Result<()> {
        self.channel.delete_message(target, id).await
    }
}

#[derive(Debug, Deserialize)]
struct TelegramApiResponse<T> {
    ok: bool,
    result: Option<T>,
    error_code: Option<i64>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TelegramSentMessage {
    message_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TelegramBotIdentity {
    id: String,
    username: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramMessage {
    message_id: i64,
    message_thread_id: Option<i64>,
    from: Option<TelegramUser>,
    chat: TelegramChat,
    text: Option<String>,
    reply_to_message: Option<TelegramReplyMessage>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramReplyMessage {
    from: Option<TelegramUser>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramUser {
    id: i64,
    is_bot: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramChat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TelegramReplyTarget {
    chat_id: String,
    message_thread_id: Option<String>,
}

impl TelegramReplyTarget {
    fn from_session_key(session_key: &str) -> Option<Self> {
        let parts = session_key.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["telegram", "private", chat_id] => Some(Self {
                chat_id: (*chat_id).to_string(),
                message_thread_id: None,
            }),
            ["telegram", "chat", chat_id] => Some(Self {
                chat_id: (*chat_id).to_string(),
                message_thread_id: None,
            }),
            ["telegram", "chat", chat_id, "topic", message_thread_id] => Some(Self {
                chat_id: (*chat_id).to_string(),
                message_thread_id: Some((*message_thread_id).to_string()),
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct TelegramInboundMessage {
    session_key: String,
    user_id: Option<String>,
    text: String,
    target: TelegramReplyTarget,
}

fn parse_inbound_update(
    update: TelegramUpdate,
    cfg: &TelegramConfig,
    bot: &TelegramBotIdentity,
) -> Option<TelegramInboundMessage> {
    let message = update.message?;
    let user_id = message.from.as_ref().map(|user| user.id.to_string());
    if user_id.as_deref() == Some(bot.id.as_str()) {
        return None;
    }
    if !allowed_by_set(&cfg.allowed_users, user_id.as_deref()) {
        return None;
    }

    let chat_id = message.chat.id.to_string();
    if !allowed_by_set(&cfg.allowed_chats, Some(&chat_id)) {
        return None;
    }
    let session_key = telegram_session_key(&message.chat, message.message_thread_id)?;
    let is_private = message.chat.kind == "private";
    let raw_text = message.text.as_deref().unwrap_or("");
    if raw_text.trim().is_empty() {
        return None;
    }

    let mentioned = telegram_text_mentions_bot(raw_text, &bot.username);
    let reply_to_bot = message
        .reply_to_message
        .as_ref()
        .and_then(|reply| reply.from.as_ref())
        .is_some_and(|user| user.is_bot && user.id.to_string() == bot.id);
    let normalized = normalize_telegram_router_command_suffix(raw_text, &bot.username);
    let router_command = telegram_text_starts_with_router_command(&normalized);
    let text = strip_telegram_bot_mentions(&normalized, &bot.username);
    if text.is_empty() {
        return None;
    }
    let should_route =
        is_private || !cfg.require_mention || mentioned || reply_to_bot || router_command;
    if !should_route {
        return None;
    }

    Some(TelegramInboundMessage {
        session_key,
        user_id,
        text,
        target: TelegramReplyTarget {
            chat_id,
            message_thread_id: message.message_thread_id.map(|id| id.to_string()),
        },
    })
}

fn telegram_session_key(chat: &TelegramChat, message_thread_id: Option<i64>) -> Option<String> {
    let chat_id = chat.id;
    match chat.kind.as_str() {
        "private" => Some(format!("telegram:private:{chat_id}")),
        "group" | "supergroup" => match message_thread_id {
            Some(message_thread_id) => {
                Some(format!("telegram:chat:{chat_id}:topic:{message_thread_id}"))
            }
            None => Some(format!("telegram:chat:{chat_id}")),
        },
        _ => None,
    }
}

fn allowed_by_set(allowed: &BTreeSet<String>, value: Option<&str>) -> bool {
    allowed.is_empty() || value.is_some_and(|value| allowed.contains(value))
}

fn normalize_telegram_router_command_suffix(text: &str, bot_username: &str) -> String {
    let text = text.trim();
    let Some((first, rest)) = split_first_token(text) else {
        return String::new();
    };
    let normalized = normalize_telegram_command_token(first, bot_username);
    if normalized == first {
        return text.to_string();
    }
    if rest.is_empty() {
        normalized
    } else {
        format!("{normalized}{rest}")
    }
}

fn split_first_token(text: &str) -> Option<(&str, &str)> {
    let first = text.split_whitespace().next()?;
    let rest = text.get(first.len()..).unwrap_or("");
    Some((first, rest))
}

fn normalize_telegram_command_token(token: &str, bot_username: &str) -> String {
    let Some(command) = token.strip_prefix('/') else {
        return token.to_string();
    };
    let (name, suffix) = command.split_once('@').unwrap_or((command, ""));
    if !is_router_command_name(name) {
        return token.to_string();
    }
    if suffix.is_empty() || suffix.eq_ignore_ascii_case(bot_username) {
        format!("/{name}")
    } else {
        token.to_string()
    }
}

fn telegram_text_starts_with_router_command(text: &str) -> bool {
    text.split_whitespace()
        .next()
        .and_then(|token| token.strip_prefix('/'))
        .is_some_and(is_router_command_name)
}

fn is_router_command_name(name: &str) -> bool {
    matches!(name, "agent" | "stop" | "new" | "yolo" | "approve" | "deny")
}

fn telegram_text_mentions_bot(text: &str, bot_username: &str) -> bool {
    let mention = format!("@{bot_username}");
    text.split_whitespace().any(|token| {
        let token = token.trim_matches(is_telegram_mention_punctuation);
        token.eq_ignore_ascii_case(&mention)
            || token
                .split_once('@')
                .is_some_and(|(_, suffix)| suffix.eq_ignore_ascii_case(bot_username))
    })
}

fn strip_telegram_bot_mentions(text: &str, bot_username: &str) -> String {
    let mention = format!("@{bot_username}");
    let mut stripped = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in non_whitespace_spans(text) {
        stripped.push_str(&text[cursor..start]);
        let token = &text[start..end];
        let normalized = token.trim_matches(is_telegram_mention_punctuation);
        if !normalized.eq_ignore_ascii_case(&mention) {
            stripped.push_str(token);
        }
        cursor = end;
    }
    stripped.push_str(&text[cursor..]);
    stripped.trim().to_string()
}

fn non_whitespace_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = None;
    for (idx, ch) in text.char_indices() {
        if ch.is_whitespace() {
            if let Some(token_start) = start.take() {
                spans.push((token_start, idx));
            }
        } else if start.is_none() {
            start = Some(idx);
        }
    }
    if let Some(token_start) = start {
        spans.push((token_start, text.len()));
    }
    spans
}

fn is_telegram_mention_punctuation(ch: char) -> bool {
    matches!(ch, ',' | ':' | ';' | '.' | '!' | '?')
}

fn telegram_send_message_bodies(target: &TelegramReplyTarget, text: &str) -> Vec<Value> {
    telegram_message_chunks(text)
        .into_iter()
        .map(|chunk| telegram_send_message_body(target, &chunk))
        .collect()
}

fn telegram_send_message_body(target: &TelegramReplyTarget, text: &str) -> Value {
    let mut body = json!({
        "chat_id": telegram_id_value(&target.chat_id),
        "text": telegram_nonempty_text(text),
    });
    if let Some(message_thread_id) = &target.message_thread_id {
        body["message_thread_id"] = telegram_id_value(message_thread_id);
    }
    body
}

fn telegram_edit_message_text_body(
    target: &TelegramReplyTarget,
    message_id: &str,
    text: &str,
) -> anyhow::Result<Value> {
    let message_id = message_id
        .parse::<i64>()
        .map_err(|err| anyhow::anyhow!("Telegram message_id `{message_id}` is invalid: {err}"))?;
    Ok(json!({
        "chat_id": telegram_id_value(&target.chat_id),
        "message_id": message_id,
        "text": telegram_nonempty_text(text),
    }))
}

fn telegram_delete_message_body(
    target: &TelegramReplyTarget,
    message_id: &str,
) -> anyhow::Result<Value> {
    let message_id = message_id
        .parse::<i64>()
        .map_err(|err| anyhow::anyhow!("Telegram message_id `{message_id}` is invalid: {err}"))?;
    Ok(json!({
        "chat_id": telegram_id_value(&target.chat_id),
        "message_id": message_id,
    }))
}

fn telegram_message_chunks(text: &str) -> Vec<String> {
    if text.chars().count() <= TELEGRAM_MESSAGE_CHAR_LIMIT {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_len = 0;
    for ch in text.chars() {
        if current_len == TELEGRAM_MESSAGE_CHAR_LIMIT {
            chunks.push(current);
            current = String::new();
            current_len = 0;
        }
        current.push(ch);
        current_len += 1;
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn telegram_nonempty_text(text: &str) -> &str {
    if text.is_empty() { " " } else { text }
}

fn telegram_id_value(id: &str) -> Value {
    id.parse::<i64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::String(id.to_string()))
}

#[derive(Debug)]
struct TelegramApiError {
    method: &'static str,
    error_code: Option<i64>,
    description: String,
}

impl std::fmt::Display for TelegramApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.error_code {
            Some(code) => write!(
                f,
                "Telegram {} failed ({}): {}",
                self.method, code, self.description
            ),
            None => write!(f, "Telegram {} failed: {}", self.method, self.description),
        }
    }
}

impl std::error::Error for TelegramApiError {}

fn telegram_error_is_auth_failure(err: &anyhow::Error) -> bool {
    err.downcast_ref::<TelegramApiError>()
        .is_some_and(TelegramApiError::is_auth_failure)
}

impl TelegramApiError {
    fn is_auth_failure(&self) -> bool {
        let description = self.description.to_ascii_lowercase();
        matches!(self.error_code, Some(401 | 404))
            || description.contains("unauthorized")
            || description.contains("not found")
    }
}

#[derive(Debug)]
struct TelegramTransportError {
    method: &'static str,
    phase: &'static str,
    details: String,
}

impl TelegramTransportError {
    fn new(method: &'static str, phase: &'static str, err: reqwest::Error) -> Self {
        Self {
            method,
            phase,
            details: err.without_url().to_string(),
        }
    }
}

impl std::fmt::Display for TelegramTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Telegram {} {} failed: {}",
            self.method, self.phase, self.details
        )
    }
}

impl std::error::Error for TelegramTransportError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    fn test_config(require_mention: bool) -> TelegramConfig {
        TelegramConfig {
            enabled: true,
            bot_token: String::new(),
            require_mention,
            channel_events: ChannelEventMode::Compact,
            allowed_users: BTreeSet::new(),
            allowed_chats: BTreeSet::new(),
            poll_timeout_secs: 30,
        }
    }

    fn bot() -> TelegramBotIdentity {
        TelegramBotIdentity {
            id: "42".to_string(),
            username: "router_bot".to_string(),
        }
    }

    fn update(value: Value) -> TelegramUpdate {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn private_message_routes_to_private_session() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": 123, "type": "private"},
                    "from": {"id": 7, "is_bot": false},
                    "text": " hello "
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.session_key, "telegram:private:123");
        assert_eq!(message.text, "hello");
        assert_eq!(
            message.target,
            TelegramReplyTarget {
                chat_id: "123".to_string(),
                message_thread_id: None,
            }
        );
    }

    #[test]
    fn group_requires_mention_by_default_and_strips_bot_mention() {
        let cfg = test_config(true);
        let unmentioned = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "hello"
                }
            })),
            &cfg,
            &bot(),
        );
        assert!(unmentioned.is_none());

        let mentioned = parse_inbound_update(
            update(json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "@router_bot hello"
                }
            })),
            &cfg,
            &bot(),
        )
        .unwrap();

        assert_eq!(mentioned.session_key, "telegram:chat:-100");
        assert_eq!(mentioned.text, "hello");
    }

    #[test]
    fn topic_message_uses_thread_id_in_session_key_and_target() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "message_thread_id": 77,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "@router_bot hello"
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.session_key, "telegram:chat:-100:topic:77");
        assert_eq!(
            message.target,
            TelegramReplyTarget {
                chat_id: "-100".to_string(),
                message_thread_id: Some("77".to_string()),
            }
        );

        let other_topic = parse_inbound_update(
            update(json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "message_thread_id": 88,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "@router_bot hello"
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();
        assert_eq!(other_topic.session_key, "telegram:chat:-100:topic:88");
    }

    #[test]
    fn router_command_suffix_is_normalized_and_routes_group_message() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "/agent@router_bot codex"
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.text, "/agent codex");
    }

    #[test]
    fn new_command_suffix_is_normalized_and_routes_group_message() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "/new@router_bot"
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.text, "/new");
    }

    #[test]
    fn mention_stripping_preserves_multiline_text() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "@router_bot please inspect:\n\n```rust\nfn main() {}\n```"
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(
            message.text,
            "please inspect:\n\n```rust\nfn main() {}\n```"
        );
    }

    #[test]
    fn unmentioned_text_preserves_internal_whitespace_when_routed() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "line one\n\n    indented"
                }
            })),
            &test_config(false),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.text, "line one\n\n    indented");
    }

    #[test]
    fn reply_to_bot_routes_group_message_without_mention() {
        let message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "continue",
                    "reply_to_message": {
                        "from": {"id": 42, "is_bot": true}
                    }
                }
            })),
            &test_config(true),
            &bot(),
        )
        .unwrap();

        assert_eq!(message.text, "continue");
        assert_eq!(message.session_key, "telegram:chat:-100");
    }

    #[test]
    fn allowed_user_and_chat_filters_are_applied() {
        let mut cfg = test_config(false);
        cfg.allowed_users = ["7".to_string()].into_iter().collect();
        cfg.allowed_chats = ["-100".to_string()].into_iter().collect();

        let accepted = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "hello"
                }
            })),
            &cfg,
            &bot(),
        );
        assert!(accepted.is_some());

        let rejected_user = parse_inbound_update(
            update(json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 8, "is_bot": false},
                    "text": "hello"
                }
            })),
            &cfg,
            &bot(),
        );
        assert!(rejected_user.is_none());

        let rejected_chat = parse_inbound_update(
            update(json!({
                "update_id": 3,
                "message": {
                    "message_id": 12,
                    "chat": {"id": -200, "type": "supergroup"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "hello"
                }
            })),
            &cfg,
            &bot(),
        );
        assert!(rejected_chat.is_none());
    }

    #[test]
    fn bot_self_and_empty_text_are_ignored() {
        let self_message = parse_inbound_update(
            update(json!({
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "chat": {"id": 123, "type": "private"},
                    "from": {"id": 42, "is_bot": true},
                    "text": "hello"
                }
            })),
            &test_config(true),
            &bot(),
        );
        assert!(self_message.is_none());

        let empty = parse_inbound_update(
            update(json!({
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "chat": {"id": 123, "type": "private"},
                    "from": {"id": 7, "is_bot": false},
                    "text": "   "
                }
            })),
            &test_config(true),
            &bot(),
        );
        assert!(empty.is_none());
    }

    #[test]
    fn topic_send_body_includes_message_thread_id_but_edit_delete_do_not() {
        let target = TelegramReplyTarget {
            chat_id: "-100".to_string(),
            message_thread_id: Some("77".to_string()),
        };

        let send = telegram_send_message_body(&target, "hello");
        assert_eq!(send["chat_id"], -100);
        assert_eq!(send["message_thread_id"], 77);
        assert_eq!(send["text"], "hello");

        let edit = telegram_edit_message_text_body(&target, "5", "updated").unwrap();
        assert_eq!(edit["chat_id"], -100);
        assert_eq!(edit["message_id"], 5);
        assert!(edit.get("message_thread_id").is_none());
        assert_eq!(edit["text"], "updated");

        let delete = telegram_delete_message_body(&target, "5").unwrap();
        assert_eq!(delete["chat_id"], -100);
        assert_eq!(delete["message_id"], 5);
        assert!(delete.get("message_thread_id").is_none());
    }

    #[test]
    fn long_send_message_is_split_and_each_body_keeps_topic_target() {
        let target = TelegramReplyTarget {
            chat_id: "-100".to_string(),
            message_thread_id: Some("77".to_string()),
        };
        let text = format!("{}{}", "a".repeat(TELEGRAM_MESSAGE_CHAR_LIMIT), "b");

        let bodies = telegram_send_message_bodies(&target, &text);

        assert_eq!(bodies.len(), 2);
        assert_eq!(
            bodies[0]["text"].as_str().unwrap().chars().count(),
            TELEGRAM_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(bodies[1]["text"], "b");
        assert!(
            bodies
                .iter()
                .all(|body| body["message_thread_id"] == json!(77))
        );
    }

    #[test]
    fn unicode_chunks_preserve_character_boundaries() {
        let text = "你".repeat(TELEGRAM_MESSAGE_CHAR_LIMIT + 1);

        let chunks = telegram_message_chunks(&text);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chars().count(), TELEGRAM_MESSAGE_CHAR_LIMIT);
        assert_eq!(chunks[1], "你");
    }

    #[tokio::test]
    async fn transport_error_display_omits_bot_token_url() {
        let client = Client::new();
        let token = "123456:SECRET";
        let err = client
            .post(format!("http://127.0.0.1:9/bot{token}/getMe"))
            .send()
            .await
            .unwrap_err();

        let sanitized = TelegramTransportError::new("getMe", "request", err).to_string();

        assert!(!sanitized.contains(token));
        assert!(!sanitized.contains("/bot"));
        assert!(sanitized.contains("Telegram getMe request failed"));
    }

    #[test]
    fn only_telegram_auth_errors_are_startup_fatal() {
        let unauthorized = anyhow::Error::new(TelegramApiError {
            method: "getMe",
            error_code: Some(401),
            description: "Unauthorized".to_string(),
        });
        assert!(telegram_error_is_auth_failure(&unauthorized));

        let bad_gateway = anyhow::Error::new(TelegramApiError {
            method: "getMe",
            error_code: Some(502),
            description: "Bad Gateway".to_string(),
        });
        assert!(!telegram_error_is_auth_failure(&bad_gateway));

        let malformed_token = anyhow::Error::new(TelegramApiError {
            method: "getMe",
            error_code: Some(404),
            description: "Not Found".to_string(),
        });
        assert!(telegram_error_is_auth_failure(&malformed_token));

        let transport = anyhow::anyhow!("Telegram getMe request failed: error sending request");
        assert!(!telegram_error_is_auth_failure(&transport));
    }
}
