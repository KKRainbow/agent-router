//! Shared HTTP entry point for agent-callable capabilities.
//!
//! Feature modules supply a Router with their own State. `build_app` merges
//! those routes and applies authentication once; handlers consume AgentScope
//! through Extension and enforce their own resource ownership rules.

pub(crate) mod skill;

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

pub const API_URL_ENV: &str = "AGENT_ROUTER_API_URL";
pub const API_TOKEN_ENV: &str = "AGENT_ROUTER_API_TOKEN";

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentApiConfig {
    pub enabled: bool,
    pub bind: SocketAddr,
}

impl Default for AgentApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:8790".parse().unwrap(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentScope {
    pub owner_session_key: String,
    pub executor: String,
    pub requester_user_id: Option<String>,
}

struct SessionCredential {
    token: String,
    scope: AgentScope,
}

pub struct AgentApi {
    credentials: Mutex<HashMap<String, SessionCredential>>,
    executors: Vec<String>,
}

impl AgentApi {
    pub fn new(executors: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            credentials: Mutex::new(HashMap::new()),
            executors,
        })
    }

    // Credentials remain stable while an executor process can be reused. A
    // changed requester/executor gets a new token and revokes the old one,
    // causing the existing process identity comparison to restart that process.
    pub(crate) fn environment(
        &self,
        session_key: &str,
        owner_session_key: &str,
        executor: &str,
        requester_user_id: Option<String>,
    ) -> BTreeMap<String, String> {
        let scope = AgentScope {
            owner_session_key: owner_session_key.into(),
            executor: executor.into(),
            requester_user_id,
        };
        let mut credentials = self.credentials.lock().unwrap();
        let entry = credentials
            .entry(session_key.into())
            .or_insert_with(|| SessionCredential {
                token: Uuid::new_v4().to_string(),
                scope: scope.clone(),
            });
        if entry.scope != scope {
            *entry = SessionCredential {
                token: Uuid::new_v4().to_string(),
                scope,
            };
        }
        BTreeMap::from([(API_TOKEN_ENV.into(), entry.token.clone())])
    }

    pub(crate) fn authenticate(&self, token: &str) -> Option<AgentScope> {
        self.credentials
            .lock()
            .unwrap()
            .values()
            .find(|credential| credential.token == token)
            .map(|credential| credential.scope.clone())
    }
}

pub fn build_app(api: Arc<AgentApi>, feature_routes: Router) -> Router {
    Router::new()
        .route("/api/context", get(context))
        .with_state(api.clone())
        .merge(feature_routes)
        .route_layer(middleware::from_fn_with_state(api, authenticate))
        .layer(DefaultBodyLimit::max(65_536))
}

async fn authenticate(
    State(api): State<Arc<AgentApi>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let scope = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| api.authenticate(token));
    let Some(scope) = scope else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"invalid session credential"})),
        )
            .into_response();
    };
    request.extensions_mut().insert(scope);
    next.run(request).await
}

async fn context(
    State(api): State<Arc<AgentApi>>,
    Extension(scope): Extension<AgentScope>,
) -> Json<serde_json::Value> {
    Json(
        json!({"session_key":scope.owner_session_key, "executor":scope.executor,
        "executors":api.executors, "now_ms":crate::session::now_ms()}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use tower::ServiceExt;

    #[tokio::test]
    async fn independently_registered_routes_share_authentication_and_identity() {
        async fn example(
            State(value): State<&'static str>,
            Extension(scope): Extension<AgentScope>,
        ) -> Json<serde_json::Value> {
            Json(
                json!({"value":value,"session":scope.owner_session_key,"requester":scope.requester_user_id}),
            )
        }
        let api = AgentApi::new(vec!["kimi".into()]);
        let token = api.environment("execution", "origin", "kimi", Some("U1".into()))
            [API_TOKEN_ENV]
            .clone();
        // No scheduler is present: a feature can supply its own State and use
        // the shared API without changing any executor or authentication code.
        let feature = Router::new()
            .route("/api/example", get(example))
            .with_state("feature-state");
        let app = build_app(api, feature);
        for path in ["/api/context", "/api/example"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap())
                    .unwrap();
            if path == "/api/example" {
                assert_eq!(
                    body,
                    json!({"value":"feature-state","session":"origin","requester":"U1"})
                );
            } else {
                assert_eq!(body["session_key"], "origin");
            }
        }
    }
    #[test]
    fn skill_install_is_idempotent_and_preserves_unmanaged_files() {
        let root = tempfile::tempdir().unwrap();
        skill::install(root.path(), root.path()).unwrap();
        skill::install(root.path(), root.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.path().join(".claude/skills/agent-router/SKILL.md"))
                .unwrap(),
            skill::SKILL
        );
        std::fs::write(
            root.path().join(".agents/skills/agent-router/SKILL.md"),
            "user's own skill",
        )
        .unwrap();
        assert!(skill::install(root.path(), root.path()).is_err());
        assert_eq!(
            std::fs::read_to_string(root.path().join(".agents/skills/agent-router/SKILL.md"))
                .unwrap(),
            "user's own skill"
        );
    }
}
