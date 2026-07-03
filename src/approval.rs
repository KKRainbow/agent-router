use std::{
    collections::{BTreeSet, HashMap, VecDeque},
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
    pub resolver_policy: ApprovalResolverPolicy,
    pub scope: ApprovalScope,
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
                    .find(|option| option.kind.starts_with("reject"))
            })
            .map(|option| option.id.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalResolverPolicy {
    Requester,
    AllowedUserIds(BTreeSet<String>),
}

impl ApprovalResolverPolicy {
    pub fn allowed_user_ids(user_ids: BTreeSet<String>) -> Self {
        Self::AllowedUserIds(user_ids)
    }

    pub fn explicit_allowed_user_ids(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Requester => None,
            Self::AllowedUserIds(user_ids) => Some(user_ids),
        }
    }

    fn allows_cross_session_resolution(&self) -> bool {
        matches!(self, Self::AllowedUserIds(_))
    }
}

impl Default for ApprovalResolverPolicy {
    fn default() -> Self {
        Self::Requester
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalScope {
    ToolPermission,
    ChannelInput,
}

impl Default for ApprovalScope {
    fn default() -> Self {
        Self::ToolPermission
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalOption {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub auto_approvable: bool,
}

impl ApprovalOption {
    pub fn is_deny(&self) -> bool {
        approval_option_is_deny(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalSelection {
    Selected(String),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalResolution {
    pub selection: ApprovalSelection,
    pub resolver_user_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ApprovalPrompt {
    pub id: String,
    pub session_key: String,
    pub executor: String,
    pub requester_user_id: Option<String>,
    pub resolver_policy: ApprovalResolverPolicy,
    pub scope: ApprovalScope,
    pub title: String,
    pub body: String,
    pub options: Vec<ApprovalOption>,
    cancellation: ApprovalCancellation,
}

impl ApprovalPrompt {
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

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
        let selectable_options = self
            .options
            .iter()
            .filter(|option| !approval_option_is_deny(option))
            .collect::<Vec<_>>();
        let requires_explicit_option = selectable_options
            .first()
            .is_some_and(|option| self.options.len() > 2 || !option.kind.starts_with("allow"));
        if selectable_options.len() > 1 || requires_explicit_option {
            lines.push("Options:".to_string());
            for option in selectable_options {
                lines.push(format!(
                    "- {}: /approve {} {}",
                    option.name, self.id, option.id
                ));
            }
        } else {
            lines.push(format!("Approve: /approve {}", self.id));
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalResolveAction {
    Approve { option_id: Option<String> },
    Deny,
}

impl ApprovalResolveAction {
    fn decision(&self) -> ApprovalDecision {
        match self {
            Self::Approve { .. } => ApprovalDecision::Approve,
            Self::Deny => ApprovalDecision::Deny,
        }
    }
}

#[derive(Debug)]
struct PendingApproval {
    request: ApprovalRequest,
    responder: oneshot::Sender<ApprovalResolution>,
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

    pub async fn has_pending(&self, id: &str) -> bool {
        self.state.lock().await.pending.contains_key(id)
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
        self.request_resolution_until_cancelled(request, cancel)
            .await
            .map(|resolution| resolution.selection)
    }

    pub async fn request_resolution(&self, request: ApprovalRequest) -> ApprovalResolution {
        self.request_resolution_until_cancelled(request, ApprovalCancellation::new())
            .await
            .unwrap_or(ApprovalResolution {
                selection: ApprovalSelection::Cancelled,
                resolver_user_id: None,
            })
    }

    pub async fn request_resolution_until_cancelled(
        &self,
        request: ApprovalRequest,
        cancel: ApprovalCancellation,
    ) -> Option<ApprovalResolution> {
        let auto_selection = tokio::select! {
            biased;
            _ = cancel.cancelled() => return None,
            selection = self.policy.auto_selection(&request) => selection,
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
            return Some(ApprovalResolution {
                selection,
                resolver_user_id: None,
            });
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let (tx, rx) = oneshot::channel();
        let prompt = ApprovalPrompt {
            id: id.clone(),
            session_key: request.session_key.clone(),
            executor: request.executor.clone(),
            requester_user_id: request.requester_user_id.clone(),
            resolver_policy: request.resolver_policy.clone(),
            scope: request.scope,
            title: request.title.clone(),
            body: request.body.clone(),
            options: request.options.clone(),
            cancellation: cancel.clone(),
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
            biased;
            _ = cancel.cancelled() => None,
            selection = time::timeout(self.timeout, rx) => {
                Some(match selection {
                    Ok(Ok(resolution)) => resolution,
                    Ok(Err(_)) | Err(_) => ApprovalResolution {
                        selection: ApprovalSelection::Cancelled,
                        resolver_user_id: None,
                    },
                })
            }
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
        let action = match command.decision {
            ApprovalDecision::Approve => ApprovalResolveAction::Approve {
                option_id: command.option_id,
            },
            ApprovalDecision::Deny => ApprovalResolveAction::Deny,
        };
        Some(
            self.resolve_action_inner(
                session_key,
                command.target_id,
                explicit_target,
                action,
                user_id,
            )
            .await,
        )
    }

    pub async fn resolve_action(
        &self,
        session_key: &str,
        approval_id: &str,
        action: ApprovalResolveAction,
        user_id: Option<&str>,
    ) -> ApprovalCommandReply {
        self.resolve_action_inner(
            session_key,
            Some(approval_id.to_string()),
            false,
            action,
            user_id,
        )
        .await
    }

    async fn resolve_action_inner(
        &self,
        session_key: &str,
        target_id: Option<String>,
        explicit_target: bool,
        action: ApprovalResolveAction,
        user_id: Option<&str>,
    ) -> ApprovalCommandReply {
        let decision = action.decision();
        let (pending, target_id, selection) = {
            let mut state = self.state.lock().await;
            let target_id = match target_id {
                Some(id) => id,
                None => state
                    .session_order
                    .get(session_key)
                    .and_then(|ids| ids.front())
                    .cloned()
                    .unwrap_or_default(),
            };
            if target_id.is_empty() {
                return ApprovalCommandReply {
                    text: "No pending approval for this session.".to_string(),
                };
            }
            let Some(pending) = state.pending.get(&target_id) else {
                return ApprovalCommandReply {
                    text: format!("Approval {target_id} is not pending."),
                };
            };

            if let Some(reply) = validate_resolver_user(&target_id, &pending.request, user_id) {
                return reply;
            }

            let same_session = pending.request.session_key == session_key;
            let allowed_slack_slash = explicit_target
                && pending.request.requester_user_id.is_some()
                && slack_slash_session_matches(&pending.request.session_key, session_key);
            let allowed_explicit_cross_session = explicit_target
                && pending
                    .request
                    .resolver_policy
                    .allows_cross_session_resolution();
            if !same_session && !allowed_slack_slash && !allowed_explicit_cross_session {
                return ApprovalCommandReply {
                    text: format!("Approval {target_id} belongs to a different session."),
                };
            }

            let selection = match action {
                ApprovalResolveAction::Approve { option_id } => {
                    if let Some(option_id) = option_id {
                        if pending.request.options.iter().any(|option| {
                            option.id == option_id && !approval_option_is_deny(option)
                        }) {
                            ApprovalSelection::Selected(option_id)
                        } else {
                            return ApprovalCommandReply {
                                text: format!(
                                    "Approval {target_id} option `{option_id}` is not available."
                                ),
                            };
                        }
                    } else if let Some(option_id) = pending.request.allow_option_id() {
                        ApprovalSelection::Selected(option_id)
                    } else {
                        return ApprovalCommandReply {
                            text: format!(
                                "Approval {target_id} requires an option. Use `/approve {target_id} <option-id>`."
                            ),
                        };
                    }
                }
                ApprovalResolveAction::Deny => pending
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
        let resolved = pending
            .responder
            .send(ApprovalResolution {
                selection,
                resolver_user_id: user_id.map(ToOwned::to_owned),
            })
            .is_ok();
        if !resolved {
            return ApprovalCommandReply {
                text: format!("Approval {target_id} is no longer active."),
            };
        }

        ApprovalCommandReply {
            text: match decision {
                ApprovalDecision::Approve => format!("Approved {target_id}."),
                ApprovalDecision::Deny => format!("Denied {target_id}."),
            },
        }
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
    option_id: Option<String>,
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
    let option_id = parts.next().map(ToOwned::to_owned);
    Some(ApprovalCommand {
        decision,
        target_id,
        option_id,
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

fn approval_option_is_deny(option: &ApprovalOption) -> bool {
    option.id == "deny" || option.kind.starts_with("reject")
}

fn validate_resolver_user(
    target_id: &str,
    request: &ApprovalRequest,
    user_id: Option<&str>,
) -> Option<ApprovalCommandReply> {
    match &request.resolver_policy {
        ApprovalResolverPolicy::Requester => {
            let requester = request.requester_user_id.as_deref()?;
            match user_id {
                Some(user_id) if user_id == requester => None,
                Some(_) => Some(ApprovalCommandReply {
                    text: format!("Approval {target_id} can only be resolved by the requester."),
                }),
                None => Some(ApprovalCommandReply {
                    text: format!("Approval {target_id} requires requester identity to resolve."),
                }),
            }
        }
        ApprovalResolverPolicy::AllowedUserIds(allowed_user_ids) => match user_id {
            Some(user_id) if allowed_user_ids.contains(user_id) => None,
            Some(_) => Some(ApprovalCommandReply {
                text: format!("Approval {target_id} can only be resolved by an allowed user."),
            }),
            None => Some(ApprovalCommandReply {
                text: format!("Approval {target_id} requires allowed user identity to resolve."),
            }),
        },
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
            resolver_policy: ApprovalResolverPolicy::default(),
            scope: ApprovalScope::ToolPermission,
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

    fn select_request(session_key: &str) -> ApprovalRequest {
        ApprovalRequest {
            session_key: session_key.to_string(),
            executor: "pi".to_string(),
            requester_user_id: Some("U1".to_string()),
            resolver_policy: ApprovalResolverPolicy::default(),
            scope: ApprovalScope::ToolPermission,
            title: "Pick target".to_string(),
            body: "Choose one option.".to_string(),
            options: vec![
                ApprovalOption {
                    id: "first".to_string(),
                    kind: "select".to_string(),
                    name: "First".to_string(),
                    auto_approvable: false,
                },
                ApprovalOption {
                    id: "second".to_string(),
                    kind: "select".to_string(),
                    name: "Second".to_string(),
                    auto_approvable: false,
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

    #[tokio::test]
    async fn has_pending_for_session_tracks_active_requests() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        assert!(broker.has_pending_for_session("s1").await);
        assert!(broker.has_pending(&prompt.id).await);
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
        assert!(!broker.has_pending(&prompt.id).await);
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
        assert!(broker.has_pending(&prompt.id).await);

        cancellation.cancel();

        assert_eq!(pending.await.unwrap(), None);
        assert!(!broker.has_pending_for_session("s1").await);
        assert!(!broker.has_pending(&prompt.id).await);
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
    async fn request_is_resolved_by_structured_approval_action() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_action(
                "s1",
                &prompt.id,
                ApprovalResolveAction::Approve {
                    option_id: Some("allow_once".to_string()),
                },
                Some("U1"),
            )
            .await;

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("allow_once".to_string())
        );
    }

    #[tokio::test]
    async fn request_is_resolved_by_structured_deny_action() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_action("s1", &prompt.id, ApprovalResolveAction::Deny, Some("U1"))
            .await;

        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[tokio::test]
    async fn multi_option_prompt_renders_explicit_approval_choices() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending =
            tokio::spawn(async move { request_broker.request(select_request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let text = prompt.render_text();
        assert!(text.contains(&format!("/approve {} first", prompt.id)));
        assert!(text.contains(&format!("/approve {} second", prompt.id)));
        assert!(text.contains(&format!("/deny {}", prompt.id)));

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
    async fn single_non_allow_option_prompt_renders_explicit_approval_choice() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = select_request("s1");
            request.options = vec![
                ApprovalOption {
                    id: "option_1".to_string(),
                    kind: "select".to_string(),
                    name: "Only target".to_string(),
                    auto_approvable: false,
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
        let text = prompt.render_text();
        assert!(text.contains(&format!("/approve {} option_1", prompt.id)));

        let reply = broker
            .resolve_command(
                "s1",
                &format!("/approve {} option_1", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("option_1".to_string())
        );
    }

    #[tokio::test]
    async fn multi_option_approval_selects_explicit_option() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending =
            tokio::spawn(async move { request_broker.request(select_request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_command("s1", &format!("/approve {} second", prompt.id), Some("U1"))
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("second".to_string())
        );
    }

    #[tokio::test]
    async fn multi_option_structured_approval_selects_explicit_option() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending =
            tokio::spawn(async move { request_broker.request(select_request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_action(
                "s1",
                &prompt.id,
                ApprovalResolveAction::Approve {
                    option_id: Some("second".to_string()),
                },
                Some("U1"),
            )
            .await;

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("second".to_string())
        );
    }

    #[tokio::test]
    async fn multi_option_approval_allows_non_reject_option_id_containing_deny() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = select_request("s1");
            request.options.insert(
                1,
                ApprovalOption {
                    id: "deny_value".to_string(),
                    kind: "select".to_string(),
                    name: "Deny value".to_string(),
                    auto_approvable: false,
                },
            );
            request_broker.request(request).await
        });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_command(
                "s1",
                &format!("/approve {} deny_value", prompt.id),
                Some("U1"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny_value".to_string())
        );
    }

    #[tokio::test]
    async fn multi_option_approval_requires_explicit_option() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending =
            tokio::spawn(async move { request_broker.request(select_request("s1")).await });

        let prompt = prompts.recv().await.unwrap();
        let reply = broker
            .resolve_command("s1", &format!("/approve {}", prompt.id), Some("U1"))
            .await
            .unwrap();
        assert!(reply.text.contains("requires an option"));
        assert!(broker.has_pending(&prompt.id).await);

        let reply = broker
            .resolve_command("s1", &format!("/approve {} missing", prompt.id), Some("U1"))
            .await
            .unwrap();
        assert!(reply.text.contains("not available"));
        assert!(broker.has_pending(&prompt.id).await);

        broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), Some("U1"))
            .await
            .unwrap();
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[tokio::test]
    async fn structured_action_rejects_unauthorized_user_and_stays_pending() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move { request_broker.request(request("s1")).await });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_action(
                "s1",
                &prompt.id,
                ApprovalResolveAction::Approve {
                    option_id: Some("allow_once".to_string()),
                },
                Some("U2"),
            )
            .await;

        assert!(reply.text.contains("requester"));
        assert!(broker.has_pending(&prompt.id).await);

        broker
            .resolve_action("s1", &prompt.id, ApprovalResolveAction::Deny, Some("U1"))
            .await;
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
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
    async fn explicit_allowed_user_can_resolve_from_different_session() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = select_request("slack:channel:C1:123.456");
            request.requester_user_id = Some("U_REQUESTER".to_string());
            request.resolver_policy = ApprovalResolverPolicy::allowed_user_ids(
                ["U_OWNER".to_string()].into_iter().collect(),
            );
            request_broker.request_resolution(request).await
        });
        let prompt = prompts.recv().await.unwrap();
        assert_eq!(prompt.scope, ApprovalScope::ToolPermission);

        let rejected = broker
            .resolve_command(
                "slack:dm:D2:999.000",
                &format!("/approve {} second", prompt.id),
                Some("U_REQUESTER"),
            )
            .await
            .unwrap();
        assert!(rejected.text.contains("allowed user"));
        assert!(broker.has_pending(&prompt.id).await);

        let reply = broker
            .resolve_command(
                "slack:dm:D1:999.000",
                &format!("/approve {} second", prompt.id),
                Some("U_OWNER"),
            )
            .await
            .unwrap();

        assert!(reply.text.contains("Approved"));
        let resolution = pending.await.unwrap();
        assert_eq!(
            resolution.selection,
            ApprovalSelection::Selected("second".to_string())
        );
        assert_eq!(resolution.resolver_user_id.as_deref(), Some("U_OWNER"));
    }

    #[tokio::test]
    async fn structured_action_requires_matching_saved_session() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = select_request("slack:channel:C1:123.456");
            request.resolver_policy = ApprovalResolverPolicy::allowed_user_ids(
                ["U_OWNER".to_string()].into_iter().collect(),
            );
            request_broker.request(request).await
        });
        let prompt = prompts.recv().await.unwrap();

        let rejected = broker
            .resolve_action(
                "slack:dm:D_OWNER:999.000",
                &prompt.id,
                ApprovalResolveAction::Approve {
                    option_id: Some("second".to_string()),
                },
                Some("U_OWNER"),
            )
            .await;
        assert!(rejected.text.contains("different session"));
        assert!(broker.has_pending(&prompt.id).await);

        let reply = broker
            .resolve_action(
                "slack:channel:C1:123.456",
                &prompt.id,
                ApprovalResolveAction::Deny,
                Some("U_OWNER"),
            )
            .await;
        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
        );
    }

    #[tokio::test]
    async fn explicit_allowed_user_policy_requires_identity() {
        let broker = Arc::new(ApprovalBroker::new(Duration::from_secs(5)));
        let mut prompts = broker.subscribe();
        let request_broker = broker.clone();
        let pending = tokio::spawn(async move {
            let mut request = request("s1");
            request.resolver_policy = ApprovalResolverPolicy::allowed_user_ids(
                ["U_OWNER".to_string()].into_iter().collect(),
            );
            request_broker.request(request).await
        });
        let prompt = prompts.recv().await.unwrap();

        let reply = broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), None)
            .await
            .unwrap();

        assert!(reply.text.contains("requires allowed user identity"));
        let reply = broker
            .resolve_command("s1", &format!("/deny {}", prompt.id), Some("U_OWNER"))
            .await
            .unwrap();
        assert!(reply.text.contains("Denied"));
        assert_eq!(
            pending.await.unwrap(),
            ApprovalSelection::Selected("deny".to_string())
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

    #[tokio::test]
    async fn structured_action_reports_cancelled_approval_as_not_pending() {
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
        cancellation.cancel();
        assert_eq!(pending.await.unwrap(), None);

        let reply = broker
            .resolve_action(
                "s1",
                &prompt.id,
                ApprovalResolveAction::Approve {
                    option_id: Some("allow_once".to_string()),
                },
                Some("U1"),
            )
            .await;

        assert_eq!(
            reply.text,
            format!("Approval {} is not pending.", prompt.id)
        );
    }
}
