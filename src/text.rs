pub(crate) fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut truncated = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    truncated.push_str("...");
    truncated
}

pub(crate) fn truncate_bytes_on_char_boundary(mut text: String, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text;
    }

    let suffix = "...";
    let mut cutoff = max_bytes.saturating_sub(suffix.len());
    while !text.is_char_boundary(cutoff) {
        cutoff -= 1;
    }
    text.truncate(cutoff);
    text.push_str(suffix);
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_chars_is_utf8_safe() {
        assert_eq!(truncate_chars("ab🙂cd", 4), "a...");
    }

    #[test]
    fn truncate_bytes_is_utf8_safe() {
        let text = format!("{}🙂z", "a".repeat(1196));

        let truncated = truncate_bytes_on_char_boundary(text, 1200);

        assert!(truncated.ends_with("..."));
    }
}
