use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{qq::QqBotChannel, slack::SlackSocketModeChannel, telegram::TelegramBotChannel};
use crate::{approval::SharedApprovalBroker, config::AppConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageChannel {
    Slack,
    Telegram,
    Qq,
    Web,
    Tui,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageTarget {
    pub channel: MessageChannel,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
}

impl MessageTarget {
    pub fn from_session_key(session_key: &str) -> anyhow::Result<Self> {
        let parts = session_key.split(':').collect::<Vec<_>>();
        let (channel, target, thread) = match parts.as_slice() {
            ["slack", "channel" | "dm", target, thread] => {
                (MessageChannel::Slack, *target, Some(*thread))
            }
            ["slack", target, "slash", _] => (MessageChannel::Slack, *target, None),
            ["telegram", "private" | "chat", target] => (MessageChannel::Telegram, *target, None),
            ["telegram", "private", target, "thread", thread]
            | ["telegram", "chat", target, "topic", thread] => {
                (MessageChannel::Telegram, *target, Some(*thread))
            }
            ["qq", "c2c" | "group", _] => (MessageChannel::Qq, &session_key[3..], None),
            ["web", _, ..] => (MessageChannel::Web, &session_key[4..], None),
            ["tui", _, ..] => (MessageChannel::Tui, &session_key[4..], None),
            _ => anyhow::bail!("session has no message destination"),
        };
        let target = Self {
            channel,
            target: target.into(),
            thread: thread.map(str::to_owned),
        };
        target.validate()?;
        Ok(target)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.target.is_empty()
                && (self.channel == MessageChannel::Tui
                    || !self.target.chars().any(char::is_whitespace)),
            "target must be nonempty; only terminal session names may contain whitespace"
        );
        if let Some(thread) = &self.thread {
            anyhow::ensure!(
                matches!(
                    self.channel,
                    MessageChannel::Slack | MessageChannel::Telegram
                ) && !thread.is_empty()
                    && !thread.chars().any(char::is_whitespace),
                "thread must be nonempty and is supported only for Slack and Telegram"
            );
        }
        if self.channel == MessageChannel::Qq {
            anyhow::ensure!(
                self.target.split_once(':').is_some_and(|(kind, id)| {
                    matches!(kind, "c2c" | "group")
                        && !id.is_empty()
                        && id.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'
                        })
                }),
                "QQ target must be c2c:<openid> or group:<group_openid>"
            );
        }
        Ok(())
    }
}

#[async_trait]
pub trait NotificationDelivery: Send + Sync + 'static {
    fn supports(&self, channel: MessageChannel) -> bool;
    async fn deliver(&self, target: &MessageTarget, text: &str) -> anyhow::Result<()>;
}

pub struct ChannelNotifications {
    slack: Option<SlackSocketModeChannel>,
    telegram: Option<TelegramBotChannel>,
    qq: Option<QqBotChannel>,
}

impl ChannelNotifications {
    pub fn new(config: &AppConfig, approvals: SharedApprovalBroker, external: bool) -> Self {
        Self {
            slack: (external && config.slack.enabled)
                .then(|| SlackSocketModeChannel::new(config.slack.clone(), approvals.clone())),
            telegram: (external && config.telegram.enabled)
                .then(|| TelegramBotChannel::new(config.telegram.clone(), approvals.clone())),
            qq: (external && config.qq.enabled)
                .then(|| QqBotChannel::new(config.qq.clone(), approvals)),
        }
    }
}

#[async_trait]
impl NotificationDelivery for ChannelNotifications {
    fn supports(&self, channel: MessageChannel) -> bool {
        match channel {
            MessageChannel::Slack => self.slack.is_some(),
            MessageChannel::Telegram => self.telegram.is_some(),
            MessageChannel::Qq => self.qq.is_some(),
            MessageChannel::Web | MessageChannel::Tui => true,
        }
    }

    async fn deliver(&self, target: &MessageTarget, text: &str) -> anyhow::Result<()> {
        match target.channel {
            MessageChannel::Slack if self.slack.is_some() => {
                self.slack
                    .as_ref()
                    .unwrap()
                    .send_notification(&target.target, target.thread.as_deref(), text)
                    .await
            }
            MessageChannel::Telegram if self.telegram.is_some() => {
                self.telegram
                    .as_ref()
                    .unwrap()
                    .send_notification(&target.target, target.thread.as_deref(), text)
                    .await
            }
            MessageChannel::Qq if self.qq.is_some() => {
                self.qq
                    .as_ref()
                    .unwrap()
                    .send_notification(&target.target, text)
                    .await
            }
            // Web reads the persisted transcript; the terminal subscribes to
            // Router notifications. Neither needs an external API call.
            MessageChannel::Web | MessageChannel::Tui => Ok(()),
            _ => anyhow::bail!("message channel is not enabled"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn originating_sessions_keep_their_native_destination() {
        for (session, channel, target, thread) in [
            (
                "slack:channel:C1:123.456",
                MessageChannel::Slack,
                "C1",
                Some("123.456"),
            ),
            (
                "slack:dm:D1:123.456",
                MessageChannel::Slack,
                "D1",
                Some("123.456"),
            ),
            ("slack:C1:slash:U1", MessageChannel::Slack, "C1", None),
            (
                "telegram:private:123",
                MessageChannel::Telegram,
                "123",
                None,
            ),
            (
                "telegram:private:123:thread:35",
                MessageChannel::Telegram,
                "123",
                Some("35"),
            ),
            ("telegram:chat:-100", MessageChannel::Telegram, "-100", None),
            (
                "telegram:chat:-100:topic:42",
                MessageChannel::Telegram,
                "-100",
                Some("42"),
            ),
            ("qq:c2c:USER", MessageChannel::Qq, "c2c:USER", None),
            ("qq:group:GROUP", MessageChannel::Qq, "group:GROUP", None),
            (
                "web:user:local:s1",
                MessageChannel::Web,
                "user:local:s1",
                None,
            ),
            (
                "tui:user:local:s2",
                MessageChannel::Tui,
                "user:local:s2",
                None,
            ),
            (
                "tui:user:local:my work",
                MessageChannel::Tui,
                "user:local:my work",
                None,
            ),
        ] {
            assert_eq!(
                MessageTarget::from_session_key(session).unwrap(),
                MessageTarget {
                    channel,
                    target: target.into(),
                    thread: thread.map(str::to_owned),
                }
            );
        }
        for invalid in [
            "web",
            "tui",
            "web:",
            "tui:",
            "schedule:task",
            "slack:channel:C1",
            "qq:group:",
        ] {
            assert!(
                MessageTarget::from_session_key(invalid).is_err(),
                "{invalid}"
            );
        }
    }
}
