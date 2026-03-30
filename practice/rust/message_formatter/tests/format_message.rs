use message_formatter::format_message;

#[test]
fn formats_message_for_external_user() {
    let result = format_message("hello");
    assert_eq!(result, "Message: hello");
}