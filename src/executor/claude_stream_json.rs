use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClaudeEvent {
    System {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        model: Option<String>,
    },
    Assistant {
        message: AssistantMessage,
    },
    User {
        message: UserMessage,
    },
    Result {
        #[serde(default)]
        result: Option<String>,
        #[serde(default)]
        subtype: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        usage: Option<serde_json::Value>,
    },
    ControlRequest {
        request_id: String,
        #[serde(default)]
        request: Option<serde_json::Value>,
    },
    ControlCancelRequest {
        request_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AssistantMessage {
    #[serde(default)]
    pub content: Vec<AssistantContent>,
    #[serde(default)]
    pub usage: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantContent {
    Text { text: String },
    Thinking { thinking: String },
    ToolUse { name: String, input: serde_json::Value },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct UserMessage {
    pub content: Vec<UserContent>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserContent {
    ToolResult {
        content: serde_json::Value,
        #[serde(default)]
        is_error: Option<bool>,
    },
    Text { text: String },
}

pub fn parse_event_line(line: &str) -> Option<ClaudeEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    match serde_json::from_str(trimmed) {
        Ok(event) => Some(event),
        Err(err) => {
            tracing::debug!(
                target: "agent_router::claude",
                line,
                error = %err,
                "ignoring non-event JSON line"
            );
            None
        }
    }
}

pub fn is_compaction_result(subtype: Option<&str>) -> bool {
    matches!(subtype, Some("compact") | Some("compaction"))
}

#[cfg(test)]
mod event_tests {
    use super::*;

    #[test]
    fn parses_system_event_with_session_id() {
        let line = r#"{"type":"system","session_id":"sess-123","model":"claude-sonnet-4"}"#;
        let event = parse_event_line(line).expect("valid system event");
        match event {
            ClaudeEvent::System { session_id, model } => {
                assert_eq!(session_id, Some("sess-123".to_string()));
                assert_eq!(model, Some("claude-sonnet-4".to_string()));
            }
            _ => panic!("expected System event"),
        }
    }

    #[test]
    fn parses_system_event_without_optional_fields() {
        let line = r#"{"type":"system"}"#;
        let event = parse_event_line(line).expect("valid system event");
        match event {
            ClaudeEvent::System { session_id, model } => {
                assert_eq!(session_id, None);
                assert_eq!(model, None);
            }
            _ => panic!("expected System event"),
        }
    }

    #[test]
    fn parses_assistant_text_and_thinking() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"hello"},{"type":"thinking","thinking":"pondering"}]}}"#;
        let event = parse_event_line(line).expect("valid assistant event");
        match event {
            ClaudeEvent::Assistant { message } => {
                assert_eq!(message.content.len(), 2);
                assert_eq!(
                    message.content[0],
                    AssistantContent::Text {
                        text: "hello".to_string()
                    }
                );
                assert_eq!(
                    message.content[1],
                    AssistantContent::Thinking {
                        thinking: "pondering".to_string()
                    }
                );
            }
            _ => panic!("expected Assistant event"),
        }
    }

    #[test]
    fn parses_assistant_message_without_content() {
        let line = r#"{"type":"assistant","message":{}}"#;
        let event = parse_event_line(line).expect("valid assistant event");
        match event {
            ClaudeEvent::Assistant { message } => {
                assert!(message.content.is_empty());
                assert_eq!(message.usage, None);
            }
            _ => panic!("expected Assistant event"),
        }
    }

    #[test]
    fn recognizes_compaction_result() {
        let line = r#"{"type":"result","result":"compact summary","subtype":"compact","session_id":"sess-123","usage":null}"#;
        let event = parse_event_line(line).expect("valid result event");
        match &event {
            ClaudeEvent::Result { subtype, .. } => {
                assert!(is_compaction_result(subtype.as_deref()));
            }
            _ => panic!("expected Result event"),
        }
        assert!(!is_compaction_result(Some("final")));
        assert!(!is_compaction_result(None));
    }

    #[test]
    fn parses_result_event_with_minimal_fields() {
        let line = r#"{"type":"result"}"#;
        let event = parse_event_line(line).expect("valid result event");
        match event {
            ClaudeEvent::Result {
                result,
                subtype,
                session_id,
                usage,
            } => {
                assert_eq!(result, None);
                assert_eq!(subtype, None);
                assert_eq!(session_id, None);
                assert_eq!(usage, None);
            }
            _ => panic!("expected Result event"),
        }
    }

    #[test]
    fn parses_user_tool_result_with_error() {
        let line = r#"{"type":"user","message":{"content":[{"type":"tool_result","content":"ok","is_error":true}]}}"#;
        let event = parse_event_line(line).expect("valid user event");
        match event {
            ClaudeEvent::User { message } => {
                assert_eq!(message.content.len(), 1);
                assert_eq!(
                    message.content[0],
                    UserContent::ToolResult {
                        content: serde_json::Value::String("ok".to_string()),
                        is_error: Some(true),
                    }
                );
            }
            _ => panic!("expected User event"),
        }
    }

    #[test]
    fn parses_control_request_without_request_body() {
        let line = r#"{"type":"control_request","request_id":"req-1"}"#;
        let event = parse_event_line(line).expect("valid control request event");
        match event {
            ClaudeEvent::ControlRequest {
                request_id,
                request,
            } => {
                assert_eq!(request_id, "req-1");
                assert_eq!(request, None);
            }
            _ => panic!("expected ControlRequest event"),
        }
    }

    #[test]
    fn ignores_non_json_line() {
        assert!(parse_event_line("not a json line").is_none());
        assert!(parse_event_line("").is_none());
    }
}
