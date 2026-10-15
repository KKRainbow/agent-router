use async_trait::async_trait;

use super::{qq::QqBotChannel, slack::SlackSocketModeChannel, telegram::TelegramBotChannel};
use crate::{approval::SharedApprovalBroker, config::AppConfig, schedules::ScheduleDelivery};

pub struct ScheduledNotifications {
    slack: Option<SlackSocketModeChannel>,
    telegram: Option<TelegramBotChannel>,
    qq: Option<QqBotChannel>,
}

impl ScheduledNotifications {
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
impl ScheduleDelivery for ScheduledNotifications {
    async fn deliver(&self, session_key: &str, text: &str) -> anyhow::Result<()> {
        match session_key.split(':').next() {
            Some("slack") if self.slack.is_some() => {
                self.slack
                    .as_ref()
                    .unwrap()
                    .send_notification(session_key, text)
                    .await
            }
            Some("telegram") if self.telegram.is_some() => {
                self.telegram
                    .as_ref()
                    .unwrap()
                    .send_notification(session_key, text)
                    .await
            }
            Some("qq") if self.qq.is_some() => {
                self.qq
                    .as_ref()
                    .unwrap()
                    .send_notification(session_key, text)
                    .await
            }
            // Web reads the persisted transcript; the terminal subscribes to
            // Router notifications. Neither needs an external API call.
            Some("web" | "tui") => Ok(()),
            _ => anyhow::bail!("result saved, but the originating channel is not enabled"),
        }
    }
}
