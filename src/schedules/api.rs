use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;

use super::{CreateTask, ScheduledTask, Scheduler, UpdateTask};
use crate::{agent_api::AgentScope, session::now_ms};

pub fn routes(scheduler: Arc<Scheduler>) -> Router {
    Router::new()
        .route("/api/schedules/tasks", get(list).post(create))
        .route(
            "/api/schedules/tasks/{id}",
            get(get_task).patch(update).delete(delete),
        )
        .route("/api/schedules/tasks/{id}/run", post(run_now))
        .with_state(scheduler)
}

async fn list(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
) -> Json<Vec<ScheduledTask>> {
    Json(
        scheduler
            .state
            .lock()
            .await
            .tasks
            .values()
            .filter(|task| task.owner_session_key == scope.owner_session_key)
            .cloned()
            .collect(),
    )
}

async fn create(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
    Json(request): Json<CreateTask>,
) -> Result<impl IntoResponse, ApiError> {
    scheduler
        .validate_content(
            &request.name,
            &request.prompt,
            request.executor.as_deref().unwrap_or(&scope.executor),
        )
        .map_err(ApiError::bad_request)?;
    request
        .schedule
        .next_after(now_ms())
        .map_err(ApiError::bad_request)?;
    let task = scheduler.create(&scope, request).await?;
    Ok((StatusCode::CREATED, Json(task)))
}

async fn get_task(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
    Path(id): Path<String>,
) -> Result<Json<ScheduledTask>, ApiError> {
    let state = scheduler.state.lock().await;
    Ok(Json(owned_task(&state.tasks, &scope, &id)?.clone()))
}

async fn update(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
    Path(id): Path<String>,
    Json(request): Json<UpdateTask>,
) -> Result<Json<ScheduledTask>, ApiError> {
    let mut state = scheduler.state.lock().await;
    let mut task = owned_task(&state.tasks, &scope, &id)?.clone();
    let reschedule = request.schedule.is_some() || request.enabled == Some(true) && !task.enabled;
    if let Some(name) = request.name {
        task.name = name;
    }
    if let Some(prompt) = request.prompt {
        task.prompt = prompt;
    }
    if let Some(executor) = request.executor {
        task.executor = executor;
    }
    if let Some(notify) = request.notify {
        task.notify = notify;
    }
    if let Some(schedule) = request.schedule {
        schedule
            .next_after(now_ms())
            .map_err(ApiError::bad_request)?;
        task.schedule = schedule;
    }
    if let Some(enabled) = request.enabled {
        task.enabled = enabled;
    }
    scheduler
        .validate_content(&task.name, &task.prompt, &task.executor)
        .map_err(ApiError::bad_request)?;
    if !task.enabled {
        task.next_run_at_ms = None;
        task.run_requested = false;
    } else if reschedule {
        task.next_run_at_ms = Some(
            task.schedule
                .next_after(now_ms())
                .map_err(ApiError::bad_request)?,
        );
    }
    let mut next = state.clone();
    next.tasks.insert(id, task.clone());
    scheduler.commit(&mut state, next)?;
    Ok(Json(task))
}

async fn delete(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let mut state = scheduler.state.lock().await;
    owned_task(&state.tasks, &scope, &id)?;
    let mut next = state.clone();
    next.tasks.remove(&id);
    scheduler.commit(&mut state, next)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn run_now(
    State(scheduler): State<Arc<Scheduler>>,
    Extension(scope): Extension<AgentScope>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let mut state = scheduler.state.lock().await;
    let mut task = owned_task(&state.tasks, &scope, &id)?.clone();
    if task.running() || task.run_requested {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "task is already running or queued".into(),
        ));
    }
    task.run_requested = true;
    let mut next = state.clone();
    next.tasks.insert(id, task.clone());
    scheduler.commit(&mut state, next)?;
    Ok((StatusCode::ACCEPTED, Json(task)))
}

fn owned_task<'a>(
    tasks: &'a std::collections::BTreeMap<String, ScheduledTask>,
    scope: &AgentScope,
    id: &str,
) -> Result<&'a ScheduledTask, ApiError> {
    tasks
        .get(id)
        .filter(|task| task.owner_session_key == scope.owner_session_key)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "task not found".into()))
}

struct ApiError(StatusCode, String);

impl ApiError {
    fn bad_request(error: anyhow::Error) -> Self {
        Self(StatusCode::BAD_REQUEST, format!("{error:#}"))
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        tracing::error!(error = %error, "scheduled task API failed");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "could not persist scheduled tasks".into(),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}
