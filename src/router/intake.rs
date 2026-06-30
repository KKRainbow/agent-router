use crate::{
    approval::is_approval_command,
    router::{
        RouterInput, RouterOutputSink, RouterService, TurnBeginMode, TurnReservation,
        is_slash_command_input,
    },
    session::ContextSyncRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelInput {
    pub session_key: String,
    pub text: String,
    pub user_id: Option<String>,
    pub source: String,
    pub intent: ChannelInputIntent,
    pub context_policy: ChannelContextPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelInputIntent {
    Route,
    RouteIfPendingApprovalElseObserve,
    Observe,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelContextPolicy {
    pub source: String,
    pub enabled: bool,
}

impl ChannelContextPolicy {
    pub fn disabled(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            enabled: false,
        }
    }
}

#[derive(Debug)]
pub enum ChannelIntakeOutcome {
    Done,
    Route {
        ticket: ChannelRouteTicket,
        context_allowed: bool,
    },
}

#[derive(Debug)]
pub struct ChannelRouteTicket {
    input: ChannelInput,
    reservation: Option<TurnReservation>,
    context_sequence: Option<u64>,
}

#[cfg(test)]
impl ChannelRouteTicket {
    pub(crate) fn for_test(input: ChannelInput) -> Self {
        Self {
            input,
            reservation: None,
            context_sequence: None,
        }
    }
}

impl ChannelRouteTicket {
    pub fn context_sequence(&self) -> Option<u64> {
        self.context_sequence
    }
}

impl ChannelInput {
    fn router_input(&self) -> RouterInput {
        RouterInput {
            session_key: self.session_key.clone(),
            text: self.text.clone(),
            user_id: self.user_id.clone(),
        }
    }
}

pub(crate) async fn begin_channel_input<S>(
    service: &S,
    input: ChannelInput,
) -> anyhow::Result<ChannelIntakeOutcome>
where
    S: RouterService + ?Sized,
{
    match input.intent {
        ChannelInputIntent::Ignore => Ok(ChannelIntakeOutcome::Done),
        ChannelInputIntent::Observe => {
            service.observe(input.router_input()).await?;
            Ok(ChannelIntakeOutcome::Done)
        }
        ChannelInputIntent::Route => route_input(service, input).await,
        ChannelInputIntent::RouteIfPendingApprovalElseObserve => {
            if is_approval_command(&input.text)
                && service.has_pending_approval(&input.session_key).await?
            {
                return route_input(service, input).await;
            }
            if is_slash_command_input(&input.text) {
                return Ok(ChannelIntakeOutcome::Done);
            }
            service.observe(input.router_input()).await?;
            Ok(ChannelIntakeOutcome::Done)
        }
    }
}

pub(crate) async fn finish_channel_input<S>(
    service: &S,
    ticket: ChannelRouteTicket,
    context: Option<ContextSyncRequest>,
    output: &mut dyn RouterOutputSink,
) -> anyhow::Result<()>
where
    S: RouterService + ?Sized,
{
    let input = ticket.input.router_input();
    if let Some(reservation) = ticket.reservation {
        service
            .handle_reserved(input, reservation, context, output)
            .await
    } else {
        service.handle_with_context(input, context, output).await
    }
}

async fn route_input<S>(service: &S, input: ChannelInput) -> anyhow::Result<ChannelIntakeOutcome>
where
    S: RouterService + ?Sized,
{
    let should_reserve = should_reserve_replacement_turn(&input.text);
    let reservation = if should_reserve {
        service
            .reserve_turn(&input.session_key, TurnBeginMode::ReplaceActive)
            .await?
    } else {
        None
    };
    let context_allowed = input.context_policy.enabled && should_reserve;
    let context_sequence = reservation.as_ref().map(TurnReservation::log_generation);
    Ok(ChannelIntakeOutcome::Route {
        ticket: ChannelRouteTicket {
            input,
            reservation,
            context_sequence,
        },
        context_allowed,
    })
}

fn should_reserve_replacement_turn(text: &str) -> bool {
    let text = text.trim();
    let command = text.split_whitespace().next().unwrap_or("");
    !is_approval_command(text)
        && !matches!(command, "/stop" | "/agent" | "/yolo")
        && !is_slash_command_input(text)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::router::RouterOutputSink;
    use crate::router::turns::TurnRegistry;

    #[derive(Debug, Default)]
    struct RecordingRouter {
        pending_approval: bool,
        reserved: Mutex<Vec<String>>,
        handled: Mutex<Vec<RouterInput>>,
        observed: Mutex<Vec<RouterInput>>,
    }

    #[derive(Debug)]
    struct ReservingRouter {
        registry: Arc<TurnRegistry>,
        reserved_generations: Mutex<Vec<u64>>,
    }

    impl Default for ReservingRouter {
        fn default() -> Self {
            Self {
                registry: TurnRegistry::new(),
                reserved_generations: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RouterService for ReservingRouter {
        async fn reserve_turn(
            &self,
            session_key: &str,
            _mode: TurnBeginMode,
        ) -> anyhow::Result<Option<TurnReservation>> {
            let reserved = self.registry.reserve_replacement(session_key).await;
            self.reserved_generations
                .lock()
                .await
                .push(reserved.reservation.log_generation());
            Ok(Some(reserved.reservation))
        }

        async fn handle(
            &self,
            _input: RouterInput,
            _output: &mut dyn RouterOutputSink,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn observe(&self, _input: RouterInput) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl RouterService for RecordingRouter {
        async fn has_pending_approval(&self, _session_key: &str) -> anyhow::Result<bool> {
            Ok(self.pending_approval)
        }

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

        async fn observe(&self, input: RouterInput) -> anyhow::Result<()> {
            self.observed.lock().await.push(input);
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct CollectingOutput {
        replies: Vec<String>,
    }

    #[async_trait]
    impl RouterOutputSink for CollectingOutput {
        fn send_channel_event(&mut self, _event: crate::router::RouterChannelEvent) {}

        async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
            self.replies.push(text);
            Ok(())
        }
    }

    fn input(text: &str, intent: ChannelInputIntent) -> ChannelInput {
        ChannelInput {
            session_key: "slack:C1:T1".to_string(),
            text: text.to_string(),
            user_id: Some("U1".to_string()),
            source: "slack".to_string(),
            intent,
            context_policy: ChannelContextPolicy {
                source: "slack".to_string(),
                enabled: true,
            },
        }
    }

    #[tokio::test]
    async fn routed_ordinary_text_requests_replacement_reservation() {
        let router = Arc::new(RecordingRouter::default());

        let outcome =
            begin_channel_input(router.as_ref(), input("hello", ChannelInputIntent::Route))
                .await
                .unwrap();

        assert_eq!(
            router.reserved.lock().await.as_slice(),
            &["slack:C1:T1".to_string()]
        );
        let ChannelIntakeOutcome::Route {
            ticket,
            context_allowed,
        } = outcome
        else {
            panic!("expected routed intake");
        };
        assert!(context_allowed);
        let mut output = CollectingOutput::default();
        finish_channel_input(router.as_ref(), ticket, None, &mut output)
            .await
            .unwrap();
        assert_eq!(router.handled.lock().await[0].text, "hello");
    }

    #[tokio::test]
    async fn route_ticket_context_sequence_uses_router_reservation_order() {
        let router = Arc::new(ReservingRouter::default());

        let outcome =
            begin_channel_input(router.as_ref(), input("hello", ChannelInputIntent::Route))
                .await
                .unwrap();

        let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
            panic!("expected routed intake");
        };
        let reserved_generations = router.reserved_generations.lock().await;
        assert_eq!(reserved_generations.len(), 1);
        assert_eq!(ticket.context_sequence(), Some(reserved_generations[0]));
    }

    #[tokio::test]
    async fn pending_approval_routes_without_replacement_reservation() {
        let router = Arc::new(RecordingRouter {
            pending_approval: true,
            ..Default::default()
        });

        let outcome = begin_channel_input(
            router.as_ref(),
            input(
                "/approve 1",
                ChannelInputIntent::RouteIfPendingApprovalElseObserve,
            ),
        )
        .await
        .unwrap();

        assert!(router.reserved.lock().await.is_empty());
        let ChannelIntakeOutcome::Route { ticket, .. } = outcome else {
            panic!("expected routed approval");
        };
        let mut output = CollectingOutput::default();
        finish_channel_input(router.as_ref(), ticket, None, &mut output)
            .await
            .unwrap();
        assert_eq!(router.handled.lock().await[0].text, "/approve 1");
    }

    #[tokio::test]
    async fn router_and_agent_slash_commands_route_without_reservation() {
        for text in ["/stop", "/agent status", "/yolo on", "//status", "/status"] {
            let router = Arc::new(RecordingRouter::default());

            let outcome =
                begin_channel_input(router.as_ref(), input(text, ChannelInputIntent::Route))
                    .await
                    .unwrap();

            assert!(router.reserved.lock().await.is_empty(), "{text}");
            assert!(
                matches!(outcome, ChannelIntakeOutcome::Route { .. }),
                "{text}"
            );
        }
    }

    #[tokio::test]
    async fn route_if_pending_approval_observes_normal_text_and_ignores_slash_text() {
        let router = Arc::new(RecordingRouter::default());

        let outcome = begin_channel_input(
            router.as_ref(),
            input(
                "middle context",
                ChannelInputIntent::RouteIfPendingApprovalElseObserve,
            ),
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ChannelIntakeOutcome::Done));
        assert_eq!(router.observed.lock().await[0].text, "middle context");

        let outcome = begin_channel_input(
            router.as_ref(),
            input(
                "/status",
                ChannelInputIntent::RouteIfPendingApprovalElseObserve,
            ),
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ChannelIntakeOutcome::Done));
        assert_eq!(router.observed.lock().await.len(), 1);
        assert!(router.reserved.lock().await.is_empty());
        assert!(router.handled.lock().await.is_empty());
    }
}
