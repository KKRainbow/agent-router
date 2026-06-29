use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::Mutex;

use crate::executor::{ExecutorTurnRef, InterruptReason, TurnCancellation};

#[derive(Debug)]
pub(crate) struct TurnRegistry {
    active: Mutex<HashMap<String, ActiveTurn>>,
    next_generation: AtomicU64,
}

#[derive(Debug, Clone)]
struct ActiveTurn {
    generation: u64,
    executor: Option<String>,
    executor_session_key: Option<String>,
    cancel: TurnCancellation,
}

#[derive(Debug)]
pub(crate) struct ReservedTurn {
    pub(crate) reservation: TurnReservation,
    pub(crate) interrupted: Option<InterruptedTurn>,
}

#[derive(Debug)]
pub(crate) struct BegunTurn {
    pub(crate) guard: TurnGuard,
    pub(crate) interrupted: Option<InterruptedTurn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnBeginMode {
    ReplaceActive,
    NoPreempt,
}

#[derive(Debug, Clone)]
pub struct TurnReservation {
    registry: Arc<TurnRegistry>,
    session_key: String,
    generation: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TurnGuard {
    registry: Arc<TurnRegistry>,
    session_key: String,
    executor_session_key: String,
    generation: u64,
    executor: String,
    cancel: TurnCancellation,
}

#[derive(Debug, Clone)]
pub(crate) struct InterruptedTurn {
    pub(crate) session_key: String,
    pub(crate) generation: u64,
    pub(crate) executor: Option<String>,
    pub(crate) executor_session_key: Option<String>,
    pub(crate) reason: InterruptReason,
    cancel: TurnCancellation,
}

impl TurnRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(1),
        })
    }

    pub(crate) async fn begin(self: &Arc<Self>, session_key: &str, executor: String) -> BegunTurn {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let cancel = TurnCancellation::new();
        let replaced = {
            let mut active = self.active.lock().await;
            active.insert(
                session_key.to_string(),
                ActiveTurn {
                    generation,
                    executor: Some(executor.clone()),
                    executor_session_key: Some(session_key.to_string()),
                    cancel: cancel.clone(),
                },
            )
        }
        .map(|turn| interrupted_turn(session_key, turn, InterruptReason::ReplacedByNewMessage));
        cancel_interrupted_turn(replaced.as_ref()).await;

        BegunTurn {
            guard: TurnGuard {
                registry: self.clone(),
                session_key: session_key.to_string(),
                executor_session_key: session_key.to_string(),
                generation,
                executor,
                cancel,
            },
            interrupted: replaced,
        }
    }

    pub(crate) async fn reserve_replacement(self: &Arc<Self>, session_key: &str) -> ReservedTurn {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let cancel = TurnCancellation::new();
        let replaced = {
            let mut active = self.active.lock().await;
            active.insert(
                session_key.to_string(),
                ActiveTurn {
                    generation,
                    executor: None,
                    executor_session_key: None,
                    cancel: cancel.clone(),
                },
            )
        }
        .map(|turn| interrupted_turn(session_key, turn, InterruptReason::ReplacedByNewMessage));
        cancel_interrupted_turn(replaced.as_ref()).await;

        ReservedTurn {
            reservation: TurnReservation {
                registry: self.clone(),
                session_key: session_key.to_string(),
                generation,
            },
            interrupted: replaced,
        }
    }

    pub(crate) async fn stop(self: &Arc<Self>, session_key: &str) -> Option<InterruptedTurn> {
        let removed = self.active.lock().await.remove(session_key)?;
        let interrupted = interrupted_turn(session_key, removed, InterruptReason::UserStop);
        cancel_interrupted_turn(Some(&interrupted)).await;
        Some(interrupted)
    }

    pub(crate) async fn is_current(&self, session_key: &str, generation: u64) -> bool {
        self.active
            .lock()
            .await
            .get(session_key)
            .is_some_and(|turn| turn.generation == generation)
    }

    pub(crate) async fn has_active(&self, session_key: &str) -> bool {
        self.active.lock().await.contains_key(session_key)
    }

    pub(crate) async fn discard_if_current(&self, session_key: &str, generation: u64) -> bool {
        let mut active = self.active.lock().await;
        if active
            .get(session_key)
            .is_some_and(|turn| turn.generation == generation)
        {
            active.remove(session_key);
            return true;
        }
        false
    }

    #[cfg(test)]
    pub(crate) async fn has_current(&self, session_key: &str) -> bool {
        self.active.lock().await.contains_key(session_key)
    }
}

impl TurnReservation {
    pub(in crate::router) fn log_generation(&self) -> u64 {
        self.generation
    }

    pub(in crate::router) async fn abandon_if_current(&self) -> bool {
        self.registry
            .discard_if_current(&self.session_key, self.generation)
            .await
    }

    pub(in crate::router) async fn adopt(&self, executor: String) -> Option<TurnGuard> {
        self.adopt_with_session_key(executor, self.session_key.clone())
            .await
    }

    pub(in crate::router) async fn adopt_with_session_key(
        &self,
        executor: String,
        executor_session_key: String,
    ) -> Option<TurnGuard> {
        let mut active = self.registry.active.lock().await;
        let turn = active.get_mut(&self.session_key)?;
        if turn.generation != self.generation {
            return None;
        }
        turn.executor = Some(executor.clone());
        turn.executor_session_key = Some(executor_session_key.clone());
        Some(TurnGuard {
            registry: self.registry.clone(),
            session_key: self.session_key.clone(),
            executor_session_key,
            generation: self.generation,
            executor,
            cancel: turn.cancel.clone(),
        })
    }
}

impl TurnGuard {
    pub(crate) fn session_key(&self) -> &str {
        &self.session_key
    }

    pub(crate) fn executor(&self) -> &str {
        &self.executor
    }

    pub(crate) fn log_generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn cancellation(&self) -> TurnCancellation {
        self.cancel.clone()
    }

    pub(crate) fn executor_turn_ref(&self) -> ExecutorTurnRef {
        ExecutorTurnRef {
            session_key: self.executor_session_key.clone(),
            executor: self.executor.clone(),
            generation: self.generation,
        }
    }

    async fn is_current(&self) -> bool {
        self.registry
            .is_current(&self.session_key, self.generation)
            .await
    }

    async fn is_current_and_uncancelled(&self) -> bool {
        !self.cancel.is_cancelled().await && self.is_current().await
    }

    pub(crate) async fn is_output_allowed(&self) -> bool {
        self.is_current_and_uncancelled().await
    }

    pub(crate) async fn is_context_commit_allowed(&self) -> bool {
        self.is_current_and_uncancelled().await
    }

    async fn remove_if_current(&self) -> bool {
        self.registry
            .discard_if_current(&self.session_key, self.generation)
            .await
    }

    pub(crate) async fn abandon_if_current(&self) -> bool {
        self.remove_if_current().await
    }

    pub(crate) async fn commit_if_current<F, Fut, T>(&self, commit: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if !self.remove_if_current().await {
            return None;
        }
        Some(commit().await)
    }
}

impl InterruptedTurn {
    pub(crate) fn executor_turn_ref(&self) -> Option<ExecutorTurnRef> {
        self.executor.as_ref().map(|executor| {
            let session_key = self
                .executor_session_key
                .clone()
                .unwrap_or_else(|| self.session_key.clone());
            ExecutorTurnRef {
                session_key,
                executor: executor.clone(),
                generation: self.generation,
            }
        })
    }
}

fn interrupted_turn(
    session_key: &str,
    turn: ActiveTurn,
    reason: InterruptReason,
) -> InterruptedTurn {
    InterruptedTurn {
        session_key: session_key.to_string(),
        generation: turn.generation,
        executor: turn.executor,
        executor_session_key: turn.executor_session_key,
        reason,
        cancel: turn.cancel,
    }
}

async fn cancel_interrupted_turn(turn: Option<&InterruptedTurn>) {
    if let Some(turn) = turn {
        let _ = turn.cancel.cancel(turn.reason).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replacing_active_turn_cancels_old_turn() {
        let turns = TurnRegistry::new();
        let first = turns.begin("session", "kimi".to_string()).await.guard;
        let reserved = turns.reserve_replacement("session").await;

        assert!(reserved.interrupted.is_some());
        assert!(first.cancellation().cancelled().await == InterruptReason::ReplacedByNewMessage);
        assert!(turns.has_current("session").await);
    }

    #[tokio::test]
    async fn stale_reservation_cannot_be_adopted() {
        let turns = TurnRegistry::new();
        let stale = turns.reserve_replacement("session").await.reservation;
        let current = turns.reserve_replacement("session").await.reservation;

        assert!(stale.adopt("kimi".to_string()).await.is_none());
        assert!(current.adopt("kimi".to_string()).await.is_some());
    }

    #[tokio::test]
    async fn stale_guard_cannot_abandon_current_turn() {
        let turns = TurnRegistry::new();
        let stale = turns.begin("session", "kimi".to_string()).await.guard;
        let current = turns.begin("session", "kimi".to_string()).await.guard;

        assert!(!stale.abandon_if_current().await);
        assert!(current.abandon_if_current().await);
        assert!(!turns.has_current("session").await);
    }

    #[tokio::test]
    async fn stale_guard_cannot_run_commit() {
        let turns = TurnRegistry::new();
        let stale = turns.begin("session", "kimi".to_string()).await.guard;
        let current = turns.begin("session", "kimi".to_string()).await.guard;
        let committed = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let result = stale
            .commit_if_current({
                let committed = committed.clone();
                || async move {
                    committed.lock().await.push("stale");
                }
            })
            .await;

        assert!(result.is_none());
        assert!(committed.lock().await.is_empty());
        assert!(current.abandon_if_current().await);
    }

    #[tokio::test]
    async fn current_guard_runs_commit_once() {
        let turns = TurnRegistry::new();
        let current = turns.begin("session", "kimi".to_string()).await.guard;
        let committed = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let result = current
            .commit_if_current({
                let committed = committed.clone();
                || async move {
                    committed.lock().await.push("current");
                    7
                }
            })
            .await;

        assert_eq!(result, Some(7));
        assert_eq!(*committed.lock().await, vec!["current"]);
        assert!(!current.abandon_if_current().await);
    }

    #[tokio::test]
    async fn stop_cancels_without_creating_replacement() {
        let turns = TurnRegistry::new();
        let turn = turns.begin("session", "kimi".to_string()).await.guard;
        let stopped = turns.stop("session").await.unwrap();

        assert_eq!(stopped.reason, InterruptReason::UserStop);
        assert_eq!(
            turn.cancellation().cancelled().await,
            InterruptReason::UserStop
        );
        assert!(!turns.has_current("session").await);
    }

    #[tokio::test]
    async fn stop_is_idempotent_for_missing_turn() {
        let turns = TurnRegistry::new();
        let _ = turns.begin("session", "kimi".to_string()).await.guard;

        assert!(turns.stop("session").await.is_some());
        assert!(turns.stop("session").await.is_none());
    }
}
