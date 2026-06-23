#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

#[cfg(test)]
mod tests {
    #[test]
    fn crate_docs_are_available() {
        assert!(!env!("CARGO_PKG_DESCRIPTION").is_empty());
    }
}
