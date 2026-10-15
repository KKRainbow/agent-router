mod api;
mod runtime;

use std::{
    collections::BTreeMap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::{agent_api::AgentScope, session::now_ms};

pub use api::routes;
pub use runtime::ScheduleDelivery;

const HISTORY_LIMIT: usize = 20;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulesConfig {
    pub enabled: bool,
    pub max_concurrent: usize,
    pub timeout_secs: u64,
}

impl Default for SchedulesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_concurrent: 4,
            timeout_secs: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Schedule {
    Once {
        at: String,
    },
    Interval {
        seconds: u64,
    },
    Cron {
        expression: String,
        timezone: String,
    },
}

impl Schedule {
    fn next_after(&self, now: u64) -> anyhow::Result<u64> {
        match self {
            Self::Once { at } => {
                let at = DateTime::parse_from_rfc3339(at)
                    .context("at must be RFC 3339 with an explicit UTC offset")?
                    .timestamp_millis();
                let at = u64::try_from(at).context("at must be after the Unix epoch")?;
                anyhow::ensure!(at > now, "at must be in the future");
                Ok(at)
            }
            Self::Interval { seconds } => {
                anyhow::ensure!(*seconds > 0, "interval seconds must be positive");
                seconds
                    .checked_mul(1000)
                    .and_then(|interval| now.checked_add(interval))
                    .filter(|value| *value <= i64::MAX as u64)
                    .context("interval is too large")
            }
            Self::Cron {
                expression,
                timezone,
            } => {
                anyhow::ensure!(
                    expression.split_whitespace().count() == 5,
                    "cron must have five fields (minute hour day month weekday)"
                );
                let cron: Cron = expression.parse().context("invalid cron expression")?;
                let timezone: Tz = timezone.parse().context("invalid IANA timezone")?;
                let now = DateTime::<Utc>::from_timestamp_millis(i64::try_from(now)?)
                    .context("timestamp is out of range")?
                    .with_timezone(&timezone);
                let next = cron.find_next_occurrence(&now, false)?;
                Ok(u64::try_from(next.timestamp_millis())?)
            }
        }
    }

    fn is_once(&self) -> bool {
        matches!(self, Self::Once { .. })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTask {
    pub id: String,
    pub name: String,
    pub prompt: String,
    pub executor: String,
    pub schedule: Schedule,
    pub enabled: bool,
    pub owner_session_key: String,
    pub requester_user_id: Option<String>,
    pub next_run_at_ms: Option<u64>,
    pub run_requested: bool,
    pub runs: Vec<ScheduledRun>,
}

impl ScheduledTask {
    pub fn session_key(&self) -> String {
        format!("schedule:{}", self.id)
    }

    fn running(&self) -> bool {
        self.runs
            .last()
            .is_some_and(|run| run.status == RunStatus::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledRun {
    pub id: String,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub status: RunStatus,
    pub output: String,
    pub error: Option<String>,
    pub delivery_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Succeeded,
    Failed,
    Interrupted,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTask {
    pub name: String,
    pub prompt: String,
    pub schedule: Schedule,
    pub executor: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTask {
    pub name: Option<String>,
    pub prompt: Option<String>,
    pub schedule: Option<Schedule>,
    pub executor: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ScheduleState {
    tasks: BTreeMap<String, ScheduledTask>,
}

pub struct Scheduler {
    path: PathBuf,
    // The lock lives in a separate file: replacing schedules.json must not
    // release single-process ownership of the state directory.
    _lock: File,
    state: Mutex<ScheduleState>,
    executors: Vec<String>,
    changed: Notify,
}

impl Scheduler {
    pub fn open(workspace_root: &Path, executors: Vec<String>) -> anyhow::Result<Arc<Self>> {
        let directory = workspace_root.join(".agent-router");
        std::fs::create_dir_all(&directory)?;
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("schedules.lock"))?;
        lock.try_lock()
            .context("another router owns the scheduled tasks in this workspace")?;
        let path = directory.join("schedules.json");
        let mut state: ScheduleState = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).context("invalid schedules.json")?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ScheduleState::default(),
            Err(error) => return Err(error.into()),
        };
        let now = now_ms();
        for task in state.tasks.values_mut() {
            for run in &mut task.runs {
                if run.status == RunStatus::Running {
                    run.status = RunStatus::Interrupted;
                    run.finished_at_ms = Some(now);
                    run.error =
                        Some("Router stopped during execution; this run was not retried.".into());
                }
            }
            if task.enabled
                && !task.schedule.is_once()
                && task.next_run_at_ms.is_some_and(|next| next <= now)
            {
                task.next_run_at_ms = Some(task.schedule.next_after(now)?);
            }
        }
        let scheduler = Arc::new(Self {
            path,
            _lock: lock,
            state: Mutex::new(state.clone()),
            executors,
            changed: Notify::new(),
        });
        scheduler.persist(&state)?;
        Ok(scheduler)
    }

    fn validate_content(&self, name: &str, prompt: &str, executor: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            !name.trim().is_empty() && name.len() <= 200,
            "name must contain 1–200 bytes"
        );
        anyhow::ensure!(
            !prompt.trim().is_empty() && prompt.len() <= 32_768,
            "prompt must contain 1–32768 bytes"
        );
        anyhow::ensure!(
            self.executors.iter().any(|name| name == executor),
            "unknown or restricted executor"
        );
        Ok(())
    }

    async fn create(
        &self,
        scope: &AgentScope,
        request: CreateTask,
    ) -> anyhow::Result<ScheduledTask> {
        let executor = request.executor.unwrap_or_else(|| scope.executor.clone());
        self.validate_content(&request.name, &request.prompt, &executor)?;
        let next_run_at_ms = request.schedule.next_after(now_ms())?;
        let task = ScheduledTask {
            id: Uuid::new_v4().to_string(),
            name: request.name,
            prompt: request.prompt,
            executor,
            schedule: request.schedule,
            enabled: true,
            owner_session_key: scope.owner_session_key.clone(),
            requester_user_id: scope.requester_user_id.clone(),
            next_run_at_ms: Some(next_run_at_ms),
            run_requested: false,
            runs: Vec::new(),
        };
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        next.tasks.insert(task.id.clone(), task.clone());
        self.commit(&mut state, next)?;
        Ok(task)
    }

    fn persist(&self, state: &ScheduleState) -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(self.path.parent().unwrap())?;
        serde_json::to_writer_pretty(file.as_file_mut(), state)?;
        file.write_all(b"\n")?;
        file.as_file().sync_all()?;
        file.persist(&self.path)
            .context("persist scheduled tasks")?;
        Ok(())
    }

    fn commit(&self, state: &mut ScheduleState, next: ScheduleState) -> anyhow::Result<()> {
        self.persist(&next)?;
        *state = next;
        self.changed.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
