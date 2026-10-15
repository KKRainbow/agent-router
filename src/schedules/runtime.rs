use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::task::JoinSet;

use super::{HISTORY_LIMIT, RunStatus, ScheduledRun, ScheduledTask, Scheduler, SchedulesConfig};
use crate::{
    router::{
        RouterChannelEvent, RouterInput, RouterOutputSink, RouterService, SessionNotification,
    },
    session::now_ms,
    text::truncate_chars,
};

#[async_trait]
pub trait ScheduleDelivery: Send + Sync + 'static {
    async fn deliver(&self, session_key: &str, text: &str) -> anyhow::Result<()>;
}

impl Scheduler {
    pub async fn run(
        self: Arc<Self>,
        router: Arc<dyn RouterService>,
        delivery: Arc<dyn ScheduleDelivery>,
        config: SchedulesConfig,
    ) -> anyhow::Result<()> {
        let mut running = JoinSet::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let available = config.max_concurrent.saturating_sub(running.len());
            for (task, run_id) in self.claim_ready(now_ms(), available).await? {
                let scheduler = self.clone();
                let router = router.clone();
                let delivery = delivery.clone();
                running.spawn(async move {
                    scheduler
                        .execute(router, delivery, task, run_id, config.timeout_secs)
                        .await
                });
            }
            tokio::select! {
                _ = interval.tick() => {},
                _ = self.changed.notified() => {},
                result = running.join_next(), if !running.is_empty() => {
                    if let Some(result) = result { result??; }
                }
            }
        }
    }

    pub(super) async fn claim_ready(
        &self,
        now: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<(ScheduledTask, String)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        let mut claimed = Vec::new();
        for task in next.tasks.values_mut() {
            if claimed.len() == limit {
                break;
            }
            if task.running()
                || !(task.run_requested
                    || task.enabled && task.next_run_at_ms.is_some_and(|at| at <= now))
            {
                continue;
            }
            task.run_requested = false;
            task.next_run_at_ms = if !task.enabled || task.schedule.is_once() {
                if task.schedule.is_once() {
                    task.enabled = false;
                }
                None
            } else {
                Some(task.schedule.next_after(now)?)
            };
            let run_id = uuid::Uuid::new_v4().to_string();
            task.runs.push(ScheduledRun {
                id: run_id.clone(),
                started_at_ms: now,
                finished_at_ms: None,
                status: RunStatus::Running,
                output: String::new(),
                error: None,
                delivery_error: None,
            });
            if task.runs.len() > HISTORY_LIMIT {
                task.runs.remove(0);
            }
            claimed.push((task.clone(), run_id));
        }
        if !claimed.is_empty() {
            self.commit(&mut state, next)?;
        }
        Ok(claimed)
    }

    pub(super) async fn execute(
        &self,
        router: Arc<dyn RouterService>,
        delivery: Arc<dyn ScheduleDelivery>,
        task: ScheduledTask,
        run_id: String,
        timeout_secs: u64,
    ) -> anyhow::Result<()> {
        let mut output = ScheduledOutput::default();
        let result = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            router.run_scheduled(&task, &mut output),
        )
        .await;
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(format!("{error:#}")),
            Err(_) => {
                let mut ignored = ScheduledOutput::default();
                let _ = router
                    .handle(
                        RouterInput {
                            session_key: task.session_key(),
                            text: "/stop".into(),
                            user_id: task.requester_user_id.clone(),
                        },
                        &mut ignored,
                    )
                    .await;
                Some(format!(
                    "Task exceeded its {timeout_secs}s execution timeout."
                ))
            }
        };
        let status = if error.is_some() {
            RunStatus::Failed
        } else {
            RunStatus::Succeeded
        };
        // Save the result before sending a notification. Delivery failures must
        // never cause the agent's potentially side-effecting work to run again.
        self.finish_run(&task.id, &run_id, status, &output.text, error.as_deref())
            .await?;
        let text = match &error {
            Some(error) => format!("Scheduled task “{}” failed:\n\n{}", task.name, error),
            None => format!("Scheduled task “{}”:\n\n{}", task.name, output.text),
        };
        let notification = SessionNotification {
            session_key: task.owner_session_key.clone(),
            executor: task.executor.clone(),
            text: text.clone(),
        };
        let delivered = async {
            router.append_notification(notification).await?;
            delivery.deliver(&task.owner_session_key, &text).await
        }
        .await;
        if let Err(error) = delivered {
            tracing::warn!(task_id = %task.id, error = %error, "scheduled result delivery failed");
            let mut state = self.state.lock().await;
            let mut next = state.clone();
            if let Some(run) = next
                .tasks
                .get_mut(&task.id)
                .and_then(|task| task.runs.iter_mut().find(|run| run.id == run_id))
            {
                run.delivery_error = Some(truncate_chars(&format!("{error:#}"), 4096));
                self.commit(&mut state, next)?;
            }
        }
        Ok(())
    }

    pub(super) async fn finish_run(
        &self,
        id: &str,
        run_id: &str,
        status: RunStatus,
        output: &str,
        error: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        if let Some(task) = next.tasks.get_mut(id) {
            if let Some(run) = task.runs.iter_mut().find(|run| run.id == run_id) {
                run.status = status;
                run.finished_at_ms = Some(now_ms());
                run.output = truncate_chars(output, 32_768);
                run.error = error.map(|error| truncate_chars(error, 4096));
            }
            if task.enabled
                && !task.schedule.is_once()
                && task.next_run_at_ms.is_some_and(|at| at <= now_ms())
            {
                task.next_run_at_ms = Some(task.schedule.next_after(now_ms())?);
            }
            self.commit(&mut state, next)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct ScheduledOutput {
    text: String,
}

#[async_trait]
impl RouterOutputSink for ScheduledOutput {
    fn send_channel_event(&mut self, _event: RouterChannelEvent) {}

    async fn send_final_reply(&mut self, text: String) -> anyhow::Result<()> {
        if !self.text.is_empty() {
            self.text.push_str("\n\n");
        }
        self.text.push_str(&text);
        Ok(())
    }
}
