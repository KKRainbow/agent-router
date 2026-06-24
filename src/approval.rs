use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, broadcast, oneshot, watch},
    time,
};

#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub session_key: String,
    pub executor: String,
    pub requester_user_id: Option<String>,
    pub title: String,
    pub body: String,
    pub options: Vec<ApprovalOption>,
}

impl ApprovalRequest {
    fn allow_option_id(&self) -> Option<String> {
        self.options
            .iter()
            .find(|option| option.id == "allow_once")
            .or_else(|| {
                self.options
                    .iter()
                    .find(|option| option.kind.starts_with("allow"))
            })
            .map(|option| option.id.clone())
    }

    pub fn allow_once_option_id(&self) -> Option<String> {
        self.options
            .iter()
            .find(|option| option.auto_approvable && option.kind == "allow_once")
            .map(|option| option.id.clone())
    }

    fn deny_option_id(&self) -> Option<String> {
        self.options
            .iter()
            .find(|option| option.id == "deny")
            .or_else(|| {
                self.options
                    .iter()
                    .find(|option| option.kind.starts_with("reject") || option.id.contains("deny"))
            })
            .map(|option| option.id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalOption {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub auto_approvable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalSelection {
    Selected(String),
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct ApprovalPrompt {
    pub id: String,
    pub session_key: String,
    pub executor: String,
    pub requester_user_id: Option<String>,
    pub title: String,
    pub body: String,
}

impl ApprovalPrompt {
    pub fn render_text(&self) -> String {
        let mut lines = vec![
            format!("Approval required: {}", self.title),
            format!("Executor: {}", self.executor),
        ];
        if !self.body.trim().is_empty() {
            lines.push(String::new());
            lines.push(self.body.clone());
        }
        lines.push(String::new());
        lines.push(format!("Approve: /approve {}", self.id));
        lines.push(format!("Deny: /deny {}", self.id));
        lines.join("\n")
    }
}

#[derive(Debug, Clone)]
pub struct ApprovalAutoSelection {
    pub session_key: String,
    pub executor: String,
    pub title: String,
    pub option_id: String,
}

impl ApprovalAutoSelection {
    pub fn render_text(&self) -> String {
        format!(
            "Auto-approved in YOLO mode: {}\nExecutor: {}\nSelected: {}",
            self.title, self.executor, self.option_id
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCommandReply {
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct ApprovalCancellation {
    inner: Arc<ApprovalCancellationInner>,
}

#[derive(Debug)]
struct ApprovalCancellationInner {
    cancelled: StdMutex<bool>,
    changed: watch::Sender<bool>,
}

impl ApprovalCancellation {
    pub fn new() -> Self {
        let (changed, _) = watch::channel(false);
        Self {
            inner: Arc::new(ApprovalCancellationInner {
                cancelled: StdMutex::new(false),
                changed,
            }),
        }
    }

    pub fn cancel(&self) {
        let mut cancelled = self.inner.cancelled.lock().unwrap();
        if *cancelled {
            return;
        }
        *cancelled = true;
        let _ = self.inner.changed.send(true);
    }

    pub async fn cancelled(&self) {
        let mut changed = self.inner.changed.subscribe();
        if *changed.borrow() {
            return;
        }
        let _ = changed.changed().await;
    }
}

impl Default for ApprovalCancellation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApprovalDecision {
    Approve,
    Deny,
}

#[derive(Debug)]
struct PendingApproval {
    request: ApprovalRequest,
    responder: oneshot::Sender<ApprovalSelection>,
}

#[async_trait]
pub trait ApprovalPolicy: Send + Sync + 'static {
    async fn auto_selection(&self, request: &ApprovalRequest) -> Option<ApprovalSelection>;
}

#[derive(Debug, Default)]
struct ManualApprovalPolicy;

#[async_trait]
impl ApprovalPolicy for ManualApprovalPolicy {
    async fn auto_selection(&self, _request: &ApprovalRequest) -> Option<ApprovalSelection> {
        None
    }
}

#[derive(Debug, Default)]
struct ApprovalState {
    pending: HashMap<String, PendingApproval>,
    session_order: HashMap<String, VecDeque<String>>,
}

pub struct ApprovalBroker {
    next_id: AtomicU64,
    timeout: Duration,
    state: Mutex<ApprovalState>,
    prompts: broadcast::Sender<ApprovalPrompt>,
    auto_selections: broadcast::Sender<ApprovalAutoSelection>,
    policy: Arc<dyn ApprovalPolicy>,
}

impl fmt::Debug for ApprovalBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApprovalBroker")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl Default for ApprovalBroker {
    fn default() -> Self {
        Self::new(Duration::from_secs(120))
    }
}

impl ApprovalBroker {
    pub fn new(timeout: Duration) -> Self {
        Self::with_policy(timeout, Arc::new(ManualApprovalPolicy))
    }

    pub fn with_policy(timeout: Duration, policy: Arc<dyn ApprovalPolicy>) -> Self {
        let (prompts, _) = broadcast::channel(256);
        let (auto_selections, _) = broadcast::channel(256);
        Self {
            next_id: AtomicU64::new(1),
            timeout,
            state: Mutex::new(ApprovalState::default()),
            prompts,
            auto_selections,
            policy,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ApprovalPrompt> {
        self.prompts.subscribe()
    }

    pub fn subscribe_auto_selections(&self) -> broadcast::Receiver<ApprovalAutoSelection> {
        self.auto_selections.subscribe()
    }

    pub async fn has_pending_for_session(&self, session_key: &str) -> bool {
        let state = self.state.lock().await;
        state
            .session_order
            .get(session_key)
            .is_some_and(|ids| ids.iter().any(|id| state.pending.contains_key(id)))
    }

    pub async fn request(&self, request: ApprovalRequest) -> ApprovalSelection {
        self.request_until_cancelled(request, ApprovalCancellation::new())
            .await
            .unwrap_or(ApprovalSelection::Cancelled)
    }

    pub async fn request_until_cancelled(
        &self,
        request: ApprovalRequest,
        cancel: ApprovalCancellation,
    ) -> Option<ApprovalSelection> {
        let auto_selection = tokio::select! {
            selection = self.policy.auto_selection(&request) => selection,
            _ = cancel.cancelled() => return None,
        };
        if let Some(selection) = auto_selection {
            if let ApprovalSelection::Selected(option_id) = &selection {
                let _ = self.auto_selections.send(ApprovalAutoSelection {
                    session_key: request.session_key.clone(),
                    executor: request.executor.clone(),
                    title: request.title.clone(),
                    option_id: option_id.clone(),
                });
            }
            return Some(selection);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let (tx, rx) = oneshot::channel();
        let prompt = ApprovalPrompt {
            id: id.clone(),
            session_key: request.session_key.clone(),
            executor: request.executor.clone(),
            requester_user_id: request.requester_user_id.clone(),
            title: request.title.clone(),
            body: request.body.clone(),
        };
        {
            let mut state = self.state.lock().await;
            let cancelled = cancel.inner.cancelled.lock().unwrap();
            if *cancelled {
                return None;
            }
            state
                .session_order
                .entry(request.session_key.clone())
                .or_default()
                .push_back(id.clone());
            state.pending.insert(
                id.clone(),
                PendingApproval {
                    request,
                    responder: tx,
                },
            );
            let _ = self.prompts.send(prompt);
            drop(cancelled);
        }

        let selection = tokio::select! {
            selection = time::timeout(self.timeout, rx) => {
                Some(match selection {
                    Ok(Ok(selection)) => selection,
                    Ok(Err(_)) | Err(_) => ApprovalSelection::Cancelled,
                })
            }
            _ = cancel.cancelled() => None,
        };
        self.remove_pending(&id).await;
        selection
    }

    pub async fn resolve_command(
        &self,
        session_key: &str,
        text: &str,
        user_id: Option<&str>,
    ) -> Option<ApprovalCommandReply> {
        let command = parse_approval_command(text)?;
        let explicit_target = command.target_id.is_some();
        let (pending, target_id, selection) = {
            let mut state = self.state.lock().await;
            let target_id = match command.target_id {
                Some(id) => id,
                None => state
                    .session_order
                    .get(session_key)
                    .and_then(|ids| ids.front())
                    .cloned()
                    .unwrap_or_default(),
            };
            if target_id.is_empty() {
                return Some(ApprovalCommandReply {
                    text: "No pending approval for this session.".to_string(),
                });
            }
            let Some(pending) = state.pending.get(&target_id) else {
                return Some(ApprovalCommandReply {
                    text: format!("Approval {target_id} is not pending."),
                });
            };

            if let Some(requester) = pending.request.requester_user_id.as_deref() {
                match user_id {
                    Some(user_id) if user_id == requester => {}
                    Some(_) => {
                        return Some(ApprovalCommandReply {
                            text: format!(
                                "Approval {target_id} can only be resolved by the requester."
                            ),
                        });
                    }
                    None => {
                        return Some(ApprovalCommandReply {
                            text: format!(
                                "Approval {target_id} requires requester identity to resolve."
                            ),
                        });
                    }
                }
            }

            let same_session = pending.request.session_key == session_key;
            let allowed_slack_slash = explicit_target
                && pending.request.requester_user_id.is_some()
                && slack_slash_session_matches(&pending.request.session_key, session_key);
            if !same_session && !allowed_slack_slash {
                return Some(ApprovalCommandReply {
                    text: format!("Approval {target_id} belongs to a different session."),
                });
            }

            let selection = match command.decision {
                ApprovalDecision::Approve => pending
                    .request
                    .allow_option_id()
                    .map(ApprovalSelection::Selected)
                    .unwrap_or(ApprovalSelection::Cancelled),
                ApprovalDecision::Deny => pending
                    .request
                    .deny_option_id()
                    .map(ApprovalSelection::Selected)
                    .unwrap_or(ApprovalSelection::Cancelled),
            };
            let session_key = pending.request.session_key.clone();
            let pending = state.pending.remove(&target_id).unwrap();
            remove_session_order(&mut state, &session_key, &target_id);
            (pending, target_id, selection)
        };
        let resolved = pending.responder.send(selection).is_ok();
        if !resolved {
            return Some(ApprovalCommandReply {
                text: format!("Approval {target_id} is no longer active."),
            });
        }

        Some(ApprovalCommandReply {
            text: match command.decision {
                ApprovalDecision::Approve => format!("Approved {target_id}."),
                ApprovalDecision::Deny => format!("Denied {target_id}."),
            },
        })
    }

    async fn remove_pending(&self, id: &str) {
        let mut state = self.state.lock().await;
        if let Some(pending) = state.pending.remove(id) {
            remove_session_order(&mut state, &pending.request.session_key, id);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ApprovalCommand {
    decision: ApprovalDecision,
    target_id: Option<String>,
}

fn parse_approval_command(text: &str) -> Option<ApprovalCommand> {
    let mut parts = text.split_whitespace();
    let command = parts.next()?;
    let decision = match command {
        "/approve" => ApprovalDecision::Approve,
        "/deny" => ApprovalDecision::Deny,
        _ => return None,
    };
    let target_id = parts.next().map(ToOwned::to_owned);
    Some(ApprovalCommand {
        decision,
        target_id,
    })
}

pub fn is_approval_command(text: &str) -> bool {
    parse_approval_command(text.trim()).is_some()
}

fn remove_session_order(state: &mut ApprovalState, session_key: &str, id: &str) {
    if let Some(order) = state.session_order.get_mut(session_key) {
        order.retain(|item| item != id);
        if order.is_empty() {
            state.session_order.remove(session_key);
        }
    }
}

fn slack_slash_session_matches(pending_session: &str, command_session: &str) -> bool {
    let Some(command_channel) = parse_slack_slash_channel(command_session) else {
        return false;
    };
    match pending_session.split(':').collect::<Vec<_>>().as_slice() {
        ["slack", "channel", pending_channel, _thread_ts] => *pending_channel == command_channel,
        ["slack", "dm", pending_channel, _thread_ts] => *pending_channel == command_channel,
        _ => false,
    }
}

fn parse_slack_slash_channel(session_key: &str) -> Option<&str> {
    match session_key.split(':').collect::<Vec<_>>().as_slice() {
        ["slack", channel, "slash", _user] => Some(*channel),
        _ => None,
    }
}

pub type SharedApprovalBroker = Arc<ApprovalBroker>;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct AllowOncePolicy;

    #[async_trait]
    impl ApprovalPolicy for AllowOncePolicy {
        async fn auto_selection(&self, request: &ApprovalRequest) -> Option<ApprovalSelection> {
            request
                .allow_once_option_id()
                .map(ApprovalSelection::Selected)
        }
    }

    fn request(session_key: &str) -> ApprovalRequest {
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

    fn request_without_requester(session_key: &str) -> ApprovalRequest {
        ApprovalRequest {
            requester_user_id: None,
            ..request(session_key)
        }
    }

    #[tokio::test]
    async fn has_pending_for_session_tracks_active_requests() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        assert!(broker.has_pending_for_session("s1").await);
        assert!(!broker.has_pending_for_session("s2").await);

        let reply = broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
        assert!(!broker.has_pending_for_session("s1").await);
    }

    #[tokio::test]
    async fn cancelable_request_removes_pending_without_resolution() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let cancellation = ApprovalCancellation::new();
        let request_cancellation = cancellation.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request_until_cancelled(request("s1"), request_cancellation)
                .await
        });

        let prompt = prompts.recv().await.unwrap();
        assert!(broker.has_pending_for_session("s1").await);

        cancellation.cancel();

        assert_eq!(pending.await.unwrap(), None);
        assert!(!broker.has_pending_for_session("s1").await);
        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), Some("U1"))
            .await
            .unwrap();
        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", prompt.id)
        );
    }

    #[tokio::test]
    async fn already_cancelled_request_does_not_publish_prompt() {
        let broker = ApprovalBroker::new(Duration::from_secs(5));
        let mut prompts = broker.subscribe();

        let selection = broker
            .request_until_cancelled(request("s1"), {
                let cancellation = ApprovalCancellation::new();
                cancellation.cancel();
                cancellation
            })
            .await;

        assert_eq!(selection, None);
        assert!(!broker.has_pending_for_session("s1").await);
        assert!(prompts.try_recv().is_err());
    }

    #[tokio::test]
    async fn auto_policy_selects_allow_once_without_pending_prompt() {
        let broker = ApprovalBroker::with_policy(Duration::from_secs(5), Arc::new(AllowOncePolicy));
        let mut prompts = broker.subscribe();
        let mut notices = broker.subscribe_auto_selections();

        let selection = broker.request(request("s1")).await;

        assert_eq!(
            selection,
            ApprovalSelection::Selected("allow_once".to_string())
        );
        assert!(prompts.try_recv().is_err());
        let notice = notices.try_recv().unwrap();
        assert_eq!(notice.session_key, "s1");
        assert_eq!(notice.option_id, "allow_once");
        assert!(notice.render_text().contains("Auto-approved in YOLO mode"));
    }

    #[tokio::test]
    async fn auto_policy_without_allow_once_falls_back_to_manual_prompt() {
        let broker = Arc::new(ApprovalBroker::with_policy(
            Duration::from_secs(5),
            Arc::new(AllowOncePolicy),
        ));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = request("s1");
            request.options = vec![
                ApprovalOption {
                    id: "allow_once".to_string(),
                    kind: "allow_always".to_string(),
                    name: "Always allow".to_string(),
                    auto_approvable: true,
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                    auto_approvable: false,
                },
            ];
            request_broker.request(request).await
        });

        let prompt = prompts.recv().await.unwrap();
        assert_eq!(prompt.session_key, "s1");
        let reply = broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[tokio::test]
    async fn request_is_resolved_by_text_approval() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        assert_eq!(prompt.session_key, "s1");
        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn different_user_cannot_resolve_requester_approval() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), Some("U2"))
            .await
            .unwrap();

        assert!(reply.text.contains("requester"));

        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn missing_user_id_cannot_resolve_bound_approval() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), None)
            .await
            .unwrap();

        assert!(reply.text.contains("requires requester identity"));
        let reply = broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[tokio::test]
    async fn explicit_id_can_be_resolved_from_requester_slash_session() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(request("slack:channel:C1:123.456"))
                .await
        });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command(
                "slack:C1:slash:U1",
                &format!("/approve {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn explicit_id_can_be_resolved_from_requester_dm_slash_session() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending =
            tokio::spawn(
                async move { request_broker.request(request("slack:dm:D1:123.456")).await },
            );
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command(
                "slack:D1:slash:U1",
                &format!("/approve {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn explicit_id_from_unrelated_session_is_rejected() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(request("slack:channel:C1:123.456"))
                .await
        });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command(
                "slack:channel:C2:999.000",
                &format!("/approve {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("different session"));
        let reply = broker
            .resolve_command(
                "slack:C1:slash:U1",
                &format!("/approve {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn slash_session_cannot_cross_resolve_without_requester_identity() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            request_broker
                .request(request_without_requester("slack:channel:C1:123.456"))
                .await
        });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command(
                "slack:C1:slash:U1",
                &format!("/approve {}", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("different session"));
        let reply = broker
            .resolve_command(
                "slack:channel:C1:123.456",
                &format!("/approve {}", prompt.id),
                None,
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn dropped_request_receiver_is_reported_as_inactive() {
        let broker = ApprovalBroker::new(Duration::from_secs(5));
        let (tx, rx) = oneshot::channel();
        drop(rx);
        {
            let mut state = broker.state.lock().await;
            state
                .session_order
                .entry("s1".to_string())
                .or_default()
                .push_back("1".to_string());
            state.pending.insert(
                "1".to_string(),
                PendingApproval {
                    request: request("s1"),
                    responder: tx,
                },
            );
        }

        let reply = broker
            .resolve_command("s1", "/approve 1", Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("no longer active"));
    }
}
