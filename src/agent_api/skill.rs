use std::{io::Write, path::Path};

pub(crate) const SKILL: &str = include_str!("../../skills/agent-router/SKILL.md");
const SOURCE: &str = ".agents/skills/agent-router";
const ALIAS: &str = ".claude/skills/agent-router";
const TARGET: &str = "../../.agents/skills/agent-router";
const MARKER: &str = "<!-- agent-router:managed-skill -->";

pub(crate) fn install(cwd: &Path) -> anyhow::Result<()> {
    for relative in [
        ".agents",
        ".agents/skills",
        SOURCE,
        ".claude",
        ".claude/skills",
    ] {
        ensure_directory(&cwd.join(relative))?;
    }
    write_skill(&cwd.join(SOURCE).join("SKILL.md"))?;
    let alias = cwd.join(ALIAS);
    match std::fs::symlink_metadata(&alias) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            anyhow::ensure!(
                is_managed_link(&alias),
                "skill link conflicts with {}",
                alias.display()
            );
            return Ok(());
        }
        Ok(metadata) if metadata.is_dir() => {
            // Windows without symlink privileges uses a generated copy. It is
            // refreshed from the same embedded source, never maintained twice.
            return write_skill(&alias.join("SKILL.md"));
        }
        Ok(_) => anyhow::bail!("skill path conflicts with {}", alias.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    create_link_or_copy(&alias)
}

pub(crate) fn is_managed_link(path: &Path) -> bool {
    path.ends_with(ALIAS)
        && std::fs::read_link(path).is_ok_and(|target| target == Path::new(TARGET))
}

fn ensure_directory(path: &Path) -> anyhow::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => anyhow::ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
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
fn create_link_or_copy(alias: &Path) -> anyhow::Result<()> {
    std::os::unix::fs::symlink(TARGET, alias)?;
    Ok(())
}

#[cfg(windows)]
fn create_link_or_copy(alias: &Path) -> anyhow::Result<()> {
    match std::os::windows::fs::symlink_dir(TARGET, alias) {
        Ok(()) => Ok(()),
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(1314) =>
        {
            ensure_directory(alias)?;
            write_skill(&alias.join("SKILL.md"))
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(any(unix, windows)))]
fn create_link_or_copy(alias: &Path) -> anyhow::Result<()> {
    ensure_directory(alias)?;
    write_skill(&alias.join("SKILL.md"))
}
