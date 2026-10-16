use super::*;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use serde_json::Value;
use tower::ServiceExt;

use crate::agent_api::{API_TOKEN_ENV, AgentApi, build_app};

fn file(content: impl AsRef<[u8]>) -> SkillFile {
    SkillFile {
        content_base64: STANDARD.encode(content),
        executable: false,
    }
}

fn package(name: &str) -> CreateSkill {
    CreateSkill {
        name: name.into(),
        files: BTreeMap::from([(
            "SKILL.md".into(),
            file(format!(
                "---\nname: {name}\ndescription: Create a test report.\nlicense: MIT\n---\n\n# Report\nUse scripts/report.sh.\n"
            )),
        )]),
    }
}

fn body(package: &CreateSkill) -> Value {
    json!({"name": package.name, "files": package.files})
}

fn credential(api: &AgentApi, session: &str) -> String {
    api.environment(session, session, "omp", None)[API_TOKEN_ENV].clone()
}

async fn request(
    app: &Router,
    token: &str,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(path)
                .method(method)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 2_097_152).await.unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn authenticated_sessions_share_published_skills_across_restart() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SkillStore::open(root.path()).unwrap());
    let api = AgentApi::new(vec!["omp".into()]);
    let qq = credential(&api, "qq:c2c:owner");
    let slack = credential(&api, "slack:dm:owner");
    let app = build_app(api, routes(store.clone()));
    let mut skill = package("test-report");
    skill
        .files
        .insert("assets/binary.dat".into(), file([0, 255, 128, 10]));
    skill.files.insert(
        "scripts/report.sh".into(),
        SkillFile {
            executable: true,
            ..file("#!/bin/sh\necho report\n")
        },
    );
    for (method, path) in [
        ("GET", "/api/skills"),
        ("POST", "/api/skills"),
        ("GET", "/api/skills/test-report"),
    ] {
        assert_eq!(
            request(&app, "invalid", method, path, body(&skill)).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        request(&app, &qq, "POST", "/api/skills", body(&skill)).await,
        (
            StatusCode::CREATED,
            json!({"name":"test-report", "description":"Create a test report."})
        )
    );
    assert_eq!(
        request(&app, &slack, "GET", "/api/skills", Value::Null).await,
        (
            StatusCode::OK,
            json!({"skills":[{"name":"test-report", "description":"Create a test report."}]})
        )
    );
    let (status, published) =
        request(&app, &slack, "GET", "/api/skills/test-report", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        published["files"]["assets/binary.dat"],
        json!(skill.files["assets/binary.dat"])
    );
    assert_eq!(published["files"]["scripts/report.sh"]["executable"], true);
    assert!(published["files"].get(METADATA_FILE).is_none());
    let reopened = SkillStore::open(root.path()).unwrap();
    assert_eq!(
        json!(reopened.read("test-report").unwrap().unwrap()),
        published
    );
    assert_eq!(
        request(&app, &slack, "GET", "/api/skills/missing", Value::Null)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn publishing_is_create_only_and_builtin_is_reserved() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SkillStore::open(root.path()).unwrap());
    let api = AgentApi::new(vec!["omp".into()]);
    let token = credential(&api, "qq:c2c:owner");
    let app = build_app(api, routes(store.clone()));
    let skill = package("test-report");
    assert_eq!(
        request(&app, &token, "POST", "/api/skills", body(&skill))
            .await
            .0,
        StatusCode::CREATED
    );
    let original = store.read("test-report").unwrap().unwrap();
    let mut changed = package("test-report");
    changed
        .files
        .insert("references/new.txt".into(), file("replacement"));
    assert_eq!(
        request(&app, &token, "POST", "/api/skills", body(&changed))
            .await
            .0,
        StatusCode::CONFLICT
    );
    assert_eq!(store.read("test-report").unwrap().unwrap(), original);
    assert_eq!(
        request(
            &app,
            &token,
            "POST",
            "/api/skills",
            body(&package("agent-router"))
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        request(&app, &token, "GET", "/api/skills/agent-router", Value::Null)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    // Existing built-in files and incomplete staging trees are not custom skills.
    fs::create_dir(store.root().join("agent-router")).unwrap();
    fs::write(store.root().join("agent-router/SKILL.md"), "builtin").unwrap();
    fs::create_dir(store.root().join(".staging-interrupted")).unwrap();
    fs::write(
        store.root().join(".staging-interrupted/SKILL.md"),
        "partial",
    )
    .unwrap();
    assert_eq!(store.list().unwrap(), vec![original.summary()]);
}

#[tokio::test]
async fn skill_upload_has_a_route_specific_body_limit() {
    let root = tempfile::tempdir().unwrap();
    let api = AgentApi::new(vec!["omp".into()]);
    let token = credential(&api, "qq:c2c:owner");
    let app = build_app(
        api,
        routes(Arc::new(SkillStore::open(root.path()).unwrap())),
    );
    let mut skill = package("large-report");
    skill
        .files
        .insert("assets/data.bin".into(), file(vec![42; 100_000]));
    assert!(body(&skill).to_string().len() > 65_536);
    assert_eq!(
        request(&app, &token, "POST", "/api/skills", body(&skill))
            .await
            .0,
        StatusCode::CREATED
    );

    let mut too_large = package("too-large");
    too_large
        .files
        .insert("assets/data.bin".into(), file(vec![42; MAX_BYTES]));
    assert_eq!(
        request(&app, &token, "POST", "/api/skills", body(&too_large))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    too_large
        .files
        .get_mut("assets/data.bin")
        .unwrap()
        .content_base64 = "A".repeat(2_097_153);
    assert_eq!(
        request(&app, &token, "POST", "/api/skills", body(&too_large))
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[test]
fn invalid_names_and_metadata_are_rejected_before_publication() {
    for name in [
        "",
        "Upper",
        "-start",
        "end-",
        "two--hyphens",
        "../escape",
        "agent-router",
        "con",
        "lpt1",
    ] {
        assert!(
            validate_skill(package(name)).is_err(),
            "accepted name {name}"
        );
    }
    assert!(validate_skill(package(&"a".repeat(65))).is_err());
    for document in [
        "name: test-report\ndescription: missing delimiter",
        "---\nname: test-report\ndescription: missing end",
        "---\nname: other\ndescription: mismatch\n---\n",
        "---\nname: test-report\n---\n",
        "---\nname: test-report\ndescription: '  '\n---\n",
        "---\nname: test-report\ndescription: [array]\n---\n",
    ] {
        let mut skill = package("test-report");
        skill.files.insert("SKILL.md".into(), file(document));
        assert!(
            validate_skill(skill).is_err(),
            "accepted document {document}"
        );
    }
    let mut skill = package("test-report");
    skill.files.insert("SKILL.md".into(), file([0xff, 0xfe]));
    assert!(validate_skill(skill).is_err());
    let mut skill = package("test-report");
    skill.files.insert(
        "SKILL.md".into(),
        file(format!(
            "---\nname: test-report\ndescription: {}\n---\n",
            "a".repeat(1025)
        )),
    );
    assert!(validate_skill(skill).is_err());
    let mut skill = package("test-report");
    skill.files.remove("SKILL.md");
    assert!(validate_skill(skill).is_err());
}

#[test]
fn file_paths_are_portable_and_cannot_escape_or_collide() {
    for path in [
        "../escape",
        "/absolute",
        "scripts/../../escape",
        "C:/escape",
        "C:\\escape",
        "scripts\\escape",
        "scripts//empty",
        "scripts/./file",
        "scripts/../file",
        "scripts/",
        "scripts/CON.txt",
        "AUX",
        "lPt9.log",
        "file.",
        "space name",
        "stream:name",
        "scripts/*",
        "",
        ".agent-router-skill.json",
        ".AGENT-ROUTER-SKILL.JSON",
    ] {
        let mut skill = package("test-report");
        skill.files.insert(path.into(), file("data"));
        assert!(validate_skill(skill).is_err(), "accepted path {path}");
    }
    for paths in [
        ["SKILL.md", "skill.md"],
        ["scripts/run", "scripts/run/file"],
        ["Scripts/a", "scripts/b"],
        ["scripts/Run", "scripts/run/file"],
    ] {
        let mut skill = package("test-report");
        for path in paths {
            skill
                .files
                .entry(path.into())
                .or_insert_with(|| file("data"));
        }
        assert!(
            validate_skill(skill).is_err(),
            "accepted collision {paths:?}"
        );
    }
    let mut skill = package("test-report");
    skill
        .files
        .insert("a/".repeat(MAX_DEPTH) + "file", file("data"));
    assert!(validate_skill(skill).is_err());
    let mut skill = package("test-report");
    skill
        .files
        .insert("x".repeat(MAX_PATH_BYTES + 1), file("data"));
    assert!(validate_skill(skill).is_err());
}

#[test]
fn decoded_bytes_and_file_count_are_bounded() {
    let mut skill = package("test-report");
    let document_bytes = STANDARD
        .decode(&skill.files["SKILL.md"].content_base64)
        .unwrap()
        .len();
    skill.files.insert(
        "assets/data.bin".into(),
        file(vec![42; MAX_BYTES - document_bytes]),
    );
    assert!(
        validate_skill(CreateSkill {
            name: skill.name.clone(),
            files: skill.files.clone()
        })
        .is_ok()
    );
    skill.files.insert("assets/extra.bin".into(), file([1]));
    assert!(validate_skill(skill).is_err());
    let mut skill = package("test-report");
    for index in 0..MAX_FILES {
        skill
            .files
            .insert(format!("references/{index}.txt"), file(""));
    }
    assert!(validate_skill(skill).is_err());
    let mut skill = package("test-report");
    skill.files.get_mut("SKILL.md").unwrap().content_base64 = "not base64!".into();
    assert!(validate_skill(skill).is_err());
}

#[test]
fn concurrent_publishers_cannot_replace_a_skill() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SkillStore::open(root.path()).unwrap());
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let outcomes = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|index| {
                let store = store.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let mut skill = package("test-report");
                    skill
                        .files
                        .insert("winner.txt".into(), file(index.to_string()));
                    let skill = validate_skill(skill).unwrap();
                    barrier.wait();
                    (index, store.publish(skill).unwrap().is_some())
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    let winners: Vec<_> = outcomes
        .iter()
        .filter(|(_, published)| *published)
        .collect();
    assert_eq!(winners.len(), 1);
    let skill = store.read("test-report").unwrap().unwrap();
    assert_eq!(
        STANDARD
            .decode(&skill.files["winner.txt"].content_base64)
            .unwrap(),
        winners[0].0.to_string().as_bytes()
    );
    assert_eq!(store.snapshot().unwrap(), vec![skill]);
    assert_eq!(fs::read_dir(store.root()).unwrap().count(), 1);
}

#[test]
fn opening_a_new_workspace_creates_persistent_storage() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("new/workspaces");
    let store = SkillStore::open(&workspace).unwrap();
    assert!(store.create(package("test-report")).unwrap().is_some());
    let reopened = SkillStore::open(&workspace).unwrap();
    assert_eq!(reopened.list().unwrap()[0].name, "test-report");
}

#[test]
fn executable_flags_are_persisted_independently_of_native_permissions() {
    let root = tempfile::tempdir().unwrap();
    let store = SkillStore::open(root.path()).unwrap();
    let mut skill = package("test-report");
    skill.files.insert(
        "scripts/report.sh".into(),
        SkillFile {
            executable: true,
            ..file("#!/bin/sh\necho report\n")
        },
    );
    store.create(skill).unwrap().unwrap();
    let directory = store.root().join("test-report");
    assert_eq!(
        fs::read_to_string(directory.join(METADATA_FILE)).unwrap(),
        "[\"scripts/report.sh\"]"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            directory.join("scripts/report.sh"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
    }
    let reopened = SkillStore::open(root.path()).unwrap();
    assert!(reopened.read("test-report").unwrap().unwrap().files["scripts/report.sh"].executable);
    fs::write(directory.join(METADATA_FILE), "[\"scripts/missing.sh\"]").unwrap();
    assert!(reopened.read("test-report").is_err());
}

#[cfg(unix)]
#[test]
fn symbolic_links_are_rejected_in_store_and_distribution() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), root.path().join(".agent-router")).unwrap();
    assert!(SkillStore::open(root.path()).is_err());
    fs::remove_file(root.path().join(".agent-router")).unwrap();
    let store = SkillStore::open(root.path()).unwrap();
    store
        .publish(validate_skill(package("test-report")).unwrap())
        .unwrap();
    symlink(outside.path(), store.root().join("test-report/references")).unwrap();
    assert!(store.read("test-report").is_err());
    assert!(store.snapshot().is_err());
    fs::remove_file(store.root().join("test-report/references")).unwrap();
    fs::write(outside.path().join("secret"), "unchanged").unwrap();
    symlink(
        outside.path().join("secret"),
        store.root().join("test-report/reference.txt"),
    )
    .unwrap();
    assert!(store.read("test-report").is_err());

    let destination = tempfile::tempdir().unwrap();
    symlink(outside.path(), destination.path().join("references")).unwrap();
    assert!(
        write_files(
            destination.path(),
            &BTreeMap::from([("references/secret".into(), file("replacement"))])
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(outside.path().join("secret")).unwrap(),
        "unchanged"
    );
}

#[test]
fn distribution_writes_new_files_without_replacing_existing_content() {
    let destination = tempfile::tempdir().unwrap();
    let skill = package("test-report");
    write_files(destination.path(), &skill.files).unwrap();
    let original = fs::read(destination.path().join("SKILL.md")).unwrap();
    assert!(
        write_files(
            destination.path(),
            &BTreeMap::from([("SKILL.md".into(), file("replacement"))])
        )
        .is_err()
    );
    assert_eq!(
        fs::read(destination.path().join("SKILL.md")).unwrap(),
        original
    );
}
