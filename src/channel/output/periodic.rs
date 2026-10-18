use std::time::Duration;

use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};

use super::ChannelReplyPort;
use crate::{
    router::{RouterChannelEvent, RouterChannelEventKind},
    text::truncate_chars,
};

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct PeriodicActivity {
    updates: watch::Sender<Activity>,
    stop: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct Activity {
    tool: Option<(RouterChannelEvent, Instant)>,
    progress: Option<(String, Instant)>,
}

impl Activity {
    fn update(&mut self, event: RouterChannelEvent) {
        match event.kind {
            RouterChannelEventKind::ToolCall => self.tool = Some((event, Instant::now())),
            RouterChannelEventKind::AgentProgress if !event.text.trim().is_empty() => {
                self.progress = Some((event.text, Instant::now()));
            }
            _ => {}
        }
    }

    fn render(&self, started: Instant) -> String {
        let mut lines = vec![format!(
            "任务仍在处理中，已用时 {} 秒。",
            started.elapsed().as_secs()
        )];
        // Report the last observed event, not a guessed current phase. A tool
        // may have finished, or several tools may be running concurrently.
        if let Some((event, at)) = &self.tool {
            lines.push(format!(
                "最近工具事件（{} 秒前）：{}\n{}",
                at.elapsed().as_secs(),
                truncate_chars(&event.title, 80),
                truncate_chars(&event.text, 320),
            ));
        }
        if let Some((text, at)) = &self.progress {
            lines.push(format!(
                "最新说明（{} 秒前）：{}",
                at.elapsed().as_secs(),
                truncate_chars(text, 320),
            ));
        }
        if self.tool.is_none() && self.progress.is_none() {
            lines.push("尚未收到 agent 的执行进度。".to_string());
        }
        lines.join("\n")
    }
}

impl PeriodicActivity {
    pub(super) fn new<P: ChannelReplyPort>(port: P, target: P::Target, interval: Duration) -> Self {
        let started = Instant::now();
        let (updates, latest) = watch::channel(Activity::default());
        let (stop, mut stopped) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let mut next = started + interval;
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => return,
                    _ = sleep_until(next) => {}
                }
                let text = latest.borrow().render(started);
                let sent = tokio::select! {
                    biased;
                    _ = &mut stopped => return,
                    sent = timeout(SEND_TIMEOUT, port.post_activity(&target, &text)) => sent,
                };
                match sent {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        tracing::warn!(error = %err, "failed to post periodic channel activity");
                        return;
                    }
                    Err(_) => {
                        tracing::warn!("timed out posting periodic channel activity");
                        return;
                    }
                }
                // Do not catch up with a burst if a send or the runtime stalls.
                next = Instant::now() + interval;
            }
        });
        Self {
            updates,
            stop: Some(stop),
            handle: Some(handle),
        }
    }

    pub(super) fn update(&mut self, event: RouterChannelEvent) {
        self.updates.send_modify(|activity| activity.update(event));
    }

    pub(super) async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for PeriodicActivity {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}
