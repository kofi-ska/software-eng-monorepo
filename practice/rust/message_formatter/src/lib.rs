/// Formats message with a standard prefix
///
/// # Guarantees
/// - Output always starts with "Message: "
/// - Output always contains the original message verbatim
pub fn format_message(msg: &str) -> String {
    build_message(msg)
}

fn build_message(msg: &str) -> String {
    format!("Message: {}", msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_prefix() {
        let result = format_message("hello");
        assert!(result.starts_with("Message: "));
    }

    #[test]
    fn preserves_input() {
        let msg = "hello";
        let result = format_message(msg);
        assert!(result.ends_with(msg));
    }

    #[test]
    fn deterministic() {
        let a = format_message("x");
        let b = format_message("x");
        assert_eq!(a, b);
    }

    #[test]
    fn helper_matches_public_api() {
        let msg = "test";
        assert_eq!(build_message(msg), format_message(msg));
    }
}
