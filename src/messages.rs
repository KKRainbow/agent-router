//! Agent-callable outbound messages, independent of scheduled task execution.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Extension, Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    agent_api::AgentScope,
    channel::notifications::{MessageChannel, MessageTarget, NotificationDelivery},
    router::{RouterService, SessionNotification},
};

#[derive(Clone)]
struct Messages {
    router: Arc<dyn RouterService>,
    delivery: Arc<dyn NotificationDelivery>,
    targets: BTreeMap<String, MessageTarget>,
}

pub fn routes(
    router: Arc<dyn RouterService>,
    delivery: Arc<dyn NotificationDelivery>,
    targets: BTreeMap<String, MessageTarget>,
) -> Router {
    Router::new()
        .route("/api/channels", get(channels))
        .route("/api/messages", post(send))
        .with_state(Messages {
            router,
            delivery,
            targets,
        })
}

async fn channels(
    State(messages): State<Messages>,
    Extension(scope): Extension<AgentScope>,
) -> Json<serde_json::Value> {
    let current = MessageTarget::from_session_key(&scope.owner_session_key)
        .ok()
        .filter(|target| messages.delivery.supports(target.channel));
    let targets: BTreeMap<_, _> = messages
        .targets
        .iter()
        .filter(|(_, target)| messages.delivery.supports(target.channel))
        .collect();
    Json(json!({"current": current, "targets": targets}))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendMessage {
    text: String,
    channel: Option<MessageChannel>,
    target: Option<String>,
    thread: Option<String>,
}

async fn send(
    State(messages): State<Messages>,
    Extension(scope): Extension<AgentScope>,
    Json(request): Json<SendMessage>,
) -> Response {
    if request.text.trim().is_empty() || request.text.len() > 32_768 {
        return failure(
            StatusCode::BAD_REQUEST,
            "text must contain 1 to 32768 UTF-8 bytes",
            false,
        );
    }
    let current = MessageTarget::from_session_key(&scope.owner_session_key).ok();
    let target = match (request.channel, request.target, request.thread) {
        (None, None, None) => match &current {
            Some(target) => target.clone(),
            None => {
                return failure(
                    StatusCode::BAD_REQUEST,
                    "session has no message destination",
                    false,
                );
            }
        },
        (Some(channel), Some(target), thread) => MessageTarget {
            channel,
            target,
            thread,
        },
        _ => {
            return failure(
                StatusCode::BAD_REQUEST,
                "channel and target must be supplied together; thread requires an explicit target",
                false,
            );
        }
    };
    if let Err(error) = target.validate() {
        return failure(StatusCode::BAD_REQUEST, &error.to_string(), false);
    }
    let is_current = current.as_ref() == Some(&target);
    if !is_current
        && !messages.targets.values().any(|allowed| {
            allowed.channel == target.channel
                && allowed.target == target.target
                && (allowed.thread.is_none() || allowed.thread == target.thread)
        })
    {
        return failure(
            StatusCode::FORBIDDEN,
            "message target is not authorized",
            false,
        );
    }
    if !messages.delivery.supports(target.channel) {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "message channel is not enabled",
            false,
        );
    }

    // Local conversations have a stable session identity. An arbitrary external
    // destination does not (for example, a new Slack top-level message creates
    // a new thread), so do not invent a transcript or notify the sender instead.
    let session_key = if is_current {
        Some(scope.owner_session_key)
    } else {
        match target.channel {
            MessageChannel::Web => Some(format!("web:{}", target.target)),
            MessageChannel::Tui => Some(format!("tui:{}", target.target)),
            _ => None,
        }
    };
    let recorded = session_key.is_some();
    if let Some(session_key) = session_key
        && let Err(error) = messages
            .router
            .append_notification(SessionNotification {
                session_key,
                executor: scope.executor,
                text: request.text.clone(),
            })
            .await
    {
        tracing::error!(error = %error, "message persistence failed");
        return failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not record message",
            false,
        );
    }
    if let Err(error) = messages.delivery.deliver(&target, &request.text).await {
        tracing::warn!(error = %error, "message delivery failed");
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error":"channel delivery failed; delivery may be partial or unknown, do not retry blindly", "recorded":recorded, "delivery":"unknown", "target":target})),
        ).into_response();
    }
    Json(json!({"target":target, "recorded":recorded, "delivery":"accepted"})).into_response()
}

fn failure(status: StatusCode, error: &str, recorded: bool) -> Response {
    (
        status,
        Json(json!({"error":error, "recorded":recorded, "delivery":"not_attempted"})),
    )
        .into_response()
}

#[cfg(test)]
mod tests;
