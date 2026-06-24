use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClaudeEvent {
    System {
        session_id: String,
        model: String,
    },
    Assistant {
        message: AssistantMessage,
    },
    User {
        message: UserMessage,
    },
    Result {
        result: serde_json::Value,
        #[serde(default)]
        subtype: Option<String>,
        session_id: String,
        #[serde(default)]
        usage: Option<serde_json::Value>,
    },
    ControlRequest {
        request_id: String,
        request: serde_json::Value,
    },
    ControlCancelRequest {
        request_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct AssistantMessage {
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
    ToolResult { content: String, is_error: bool },
    Text { text: String },
}

pub fn parse_event_line(line: &str) -> Option<ClaudeEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
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
                assert_eq!(session_id, "sess-123");
                assert_eq!(model, "claude-sonnet-4");
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
    fn recognizes_compaction_result() {
        let line = r#"{"type":"result","result":{},"subtype":"compact","session_id":"sess-123","usage":null}"#;
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
    fn ignores_non_json_line() {
        assert!(parse_event_line("not a json line").is_none());
        assert!(parse_event_line("").is_none());
    }
}
