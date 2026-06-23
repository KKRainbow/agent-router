#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod approval;
pub mod channel;
pub mod config;
pub mod executor;
pub mod router;
pub mod session;

pub use router::{AgentRouter, RouterInput, RouterReply, RouterService};

#[cfg(test)]
mod tests {
    #[test]
    fn crate_docs_are_available() {
        assert!(!env!("CARGO_PKG_DESCRIPTION").is_empty());
    }
}
