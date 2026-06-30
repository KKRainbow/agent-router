#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod approval;
pub mod channel;
pub mod config;
pub mod executor;
pub mod machine;
pub mod router;
pub mod session;
mod text;

pub use router::{
    AgentRouter, ChannelContextPolicy, ChannelInput, ChannelInputIntent, ChannelIntakeOutcome,
    ChannelRouteTicket, RouterChannelEvent, RouterChannelEventKind, RouterInput, RouterOutputEvent,
    RouterOutputSink, RouterService,
};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_docs_are_available() {
        assert!(!env!("CARGO_PKG_DESCRIPTION").is_empty());
    }
}
