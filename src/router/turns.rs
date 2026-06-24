use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::Mutex;

use crate::executor::{InterruptReason, TurnCancellation};

#[derive(Debug)]
pub(crate) struct TurnRegistry {
    active: Mutex<HashMap<String, ActiveTurn>>,
    next_generation: AtomicU64,
}

#[derive(Debug, Clone)]
struct ActiveTurn {
    generation: u64,
    executor: Option<String>,
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

#[derive(Debug, Clone)]
pub(crate) struct TurnReservation {
    registry: Arc<TurnRegistry>,
    session_key: String,
    generation: u64,
    cancel: TurnCancellation,
}

#[derive(Debug, Clone)]
pub(crate) struct TurnGuard {
    registry: Arc<TurnRegistry>,
    session_key: String,
    generation: u64,
    executor: String,
    cancel: TurnCancellation,
}

#[derive(Debug, Clone)]
pub(crate) struct InterruptedTurn {
    pub(crate) session_key: String,
    pub(crate) generation: u64,
    pub(crate) executor: Option<String>,
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
                cancel,
            },
            interrupted: replaced,
        }
    }

    pub(crate) async fn reservation_for(
        self: &Arc<Self>,
        session_key: &str,
        generation: u64,
    ) -> Option<TurnReservation> {
        let active = self.active.lock().await;
        let turn = active.get(session_key)?;
        (turn.generation == generation).then(|| TurnReservation {
            registry: self.clone(),
            session_key: session_key.to_string(),
            generation,
            cancel: turn.cancel.clone(),
        })
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
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn cancellation(&self) -> TurnCancellation {
        self.cancel.clone()
    }

    pub(crate) async fn is_current(&self) -> bool {
        self.registry
            .is_current(&self.session_key, self.generation)
            .await
    }

    pub(crate) async fn discard_if_current(&self) -> bool {
        self.registry
            .discard_if_current(&self.session_key, self.generation)
            .await
    }

    pub(crate) async fn adopt(&self, executor: String) -> Option<TurnGuard> {
        let mut active = self.registry.active.lock().await;
        let turn = active.get_mut(&self.session_key)?;
        if turn.generation != self.generation {
            return None;
        }
        turn.executor = Some(executor.clone());
        Some(TurnGuard {
            registry: self.registry.clone(),
            session_key: self.session_key.clone(),
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

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn cancellation(&self) -> TurnCancellation {
        self.cancel.clone()
    }

    pub(crate) async fn is_current(&self) -> bool {
        self.registry
            .is_current(&self.session_key, self.generation)
            .await
    }

    pub(crate) async fn finish_if_current(&self) -> bool {
        self.registry
            .discard_if_current(&self.session_key, self.generation)
            .await
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
        assert!(reserved.reservation.is_current().await);
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
    async fn stale_guard_cannot_finish_current_turn() {
        let turns = TurnRegistry::new();
        let stale = turns.begin("session", "kimi".to_string()).await.guard;
        let current = turns.begin("session", "kimi".to_string()).await.guard;

        assert!(!stale.finish_if_current().await);
        assert!(current.finish_if_current().await);
        assert!(!turns.has_current("session").await);
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
