use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Mutex, broadcast, oneshot},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalCommandReply {
    pub text: String,
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

#[derive(Debug, Default)]
struct ApprovalState {
    pending: HashMap<String, PendingApproval>,
    session_order: HashMap<String, VecDeque<String>>,
}

#[derive(Debug)]
pub struct ApprovalBroker {
    next_id: AtomicU64,
    timeout: Duration,
    state: Mutex<ApprovalState>,
    prompts: broadcast::Sender<ApprovalPrompt>,
}

impl Default for ApprovalBroker {
    fn default() -> Self {
        Self::new(Duration::from_secs(120))
    }
}

impl ApprovalBroker {
    pub fn new(timeout: Duration) -> Self {
        let (prompts, _) = broadcast::channel(256);
        Self {
            next_id: AtomicU64::new(1),
            timeout,
            state: Mutex::new(ApprovalState::default()),
            prompts,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ApprovalPrompt> {
        self.prompts.subscribe()
    }

    pub async fn request(&self, request: ApprovalRequest) -> ApprovalSelection {
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
        }
        let _ = self.prompts.send(prompt);

        let selection = match time::timeout(self.timeout, rx).await {
            Ok(Ok(selection)) => selection,
            Ok(Err(_)) | Err(_) => ApprovalSelection::Cancelled,
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
            let same_requester_explicit =
                explicit_target && pending.request.requester_user_id.is_some();
            if !same_session && !same_requester_explicit {
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
        let _ = pending.responder.send(selection);

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

pub type SharedApprovalBroker = Arc<ApprovalBroker>;

#[cfg(test)]
mod tests {
    use super::*;

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
                },
                ApprovalOption {
                    id: "deny".to_string(),
                    kind: "reject_once".to_string(),
                    name: "Deny".to_string(),
                },
            ],
        }
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
}
