use std::{io::Write, path::Path};

pub(crate) const SKILL: &str = include_str!("../../skills/agent-router/SKILL.md");
pub(crate) const SHARED_PATH: &str = ".agent-router/skills/agent-router";
pub(crate) const WORKSPACE_PATHS: [&str; 2] =
    [".agents/skills/agent-router", ".claude/skills/agent-router"];
pub(crate) const MARKER: &str = "<!-- agent-router:managed-skill -->";

pub(crate) fn install(cwd: &Path, workspace_root: &Path) -> anyhow::Result<()> {
    // Windows cannot atomically replace the same destination concurrently.
    // Publish shared content and links serially; every session uses this path.
    static INSTALL_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = INSTALL_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("shared skill installation lock is poisoned"))?;
    for relative in [".agent-router", ".agent-router/skills", SHARED_PATH] {
        ensure_directory(&workspace_root.join(relative))?;
    }
    let shared = workspace_root.join(SHARED_PATH).canonicalize()?;
    write_skill(&shared.join("SKILL.md"))?;
    for relative in WORKSPACE_PATHS {
        let alias = cwd.join(relative);
        let parent = alias.parent().unwrap();
        ensure_directory(parent.parent().unwrap())?;
        ensure_directory(parent)?;
        install_link(&alias, &shared)?;
    }
    Ok(())
}

fn install_link(alias: &Path, shared: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(alias) {
        Ok(_) => {
            anyhow::ensure!(
                std::fs::read_link(alias).is_ok()
                    && alias.canonicalize().is_ok_and(|target| target == shared),
                "skill link conflicts with {}",
                alias.display()
            );
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    create_link(alias, shared)
}

pub(crate) fn is_workspace_path(relative: &Path) -> bool {
    WORKSPACE_PATHS
        .iter()
        .any(|path| relative == Path::new(path))
}

fn ensure_directory(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => anyhow::ensure!(
            metadata.is_dir() && std::fs::read_link(path).is_err(),
            "skill directory must be a plain directory: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => std::fs::create_dir(path)?,
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn write_skill(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "invalid skill file: {}",
                path.display()
            );
            let existing = std::fs::read_to_string(path)?;
            if existing == SKILL {
                return Ok(());
            }
            anyhow::ensure!(
                existing.contains(MARKER),
                "refusing to replace an unmanaged skill: {}",
                path.display()
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    file.write_all(SKILL.as_bytes())?;
    file.persist(path)?;
    Ok(())
}

#[cfg(unix)]
fn create_link(alias: &Path, shared: &Path) -> anyhow::Result<()> {
    std::os::unix::fs::symlink(shared, alias)?;
    Ok(())
}

#[cfg(windows)]
fn create_link(alias: &Path, shared: &Path) -> anyhow::Result<()> {
    // Junctions share the directory without requiring symlink privileges.
    junction::create(shared, alias)?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn create_link(_alias: &Path, _shared: &Path) -> anyhow::Result<()> {
    anyhow::bail!("shared skills require directory link support")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_share_the_same_skill_directory_and_updates() {
        let root = tempfile::tempdir().unwrap();
        let shared = root.path().join(SHARED_PATH);
        let sessions = [root.path().join("first"), root.path().join("second")];
        for cwd in &sessions {
            std::fs::create_dir(cwd).unwrap();
            install(cwd, root.path()).unwrap();
            install(cwd, root.path()).unwrap();
            for relative in WORKSPACE_PATHS {
                assert!(same_file::is_same_file(cwd.join(relative), &shared).unwrap());
            }
        }

        let updated = format!("{SKILL}\nShared update\n");
        std::fs::write(shared.join("SKILL.md"), &updated).unwrap();
        for cwd in &sessions {
            for relative in WORKSPACE_PATHS {
                assert_eq!(
                    std::fs::read_to_string(cwd.join(relative).join("SKILL.md")).unwrap(),
                    updated
                );
            }
        }
        install(&sessions[0], root.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(sessions[1].join(WORKSPACE_PATHS[0]).join("SKILL.md")).unwrap(),
            SKILL
        );
    }

    #[test]
    fn concurrent_sessions_can_install_the_shared_skill() {
        let root = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            for index in 0..8 {
                let cwd = root.path().join(index.to_string());
                std::fs::create_dir(&cwd).unwrap();
                let root = root.path();
                scope.spawn(move || install(&cwd, root).unwrap());
            }
        });
        assert_eq!(
            std::fs::read_to_string(root.path().join(SHARED_PATH).join("SKILL.md")).unwrap(),
            SKILL
        );
    }

    #[test]
    fn workspace_skill_conflicts_are_preserved() {
        let root = tempfile::tempdir().unwrap();
        let cwd = root.path().join("session");
        let conflict = cwd.join(WORKSPACE_PATHS[0]);
        std::fs::create_dir_all(&conflict).unwrap();
        std::fs::write(conflict.join("SKILL.md"), "user's skill").unwrap();
        assert!(install(&cwd, root.path()).is_err());
        assert_eq!(
            std::fs::read_to_string(conflict.join("SKILL.md")).unwrap(),
            "user's skill"
        );
    }
}
