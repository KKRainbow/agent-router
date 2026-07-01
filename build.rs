use std::{
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

fn main() -> io::Result<()> {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let source = manifest_dir.join("web").join("dist");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let embedded = out_dir.join("web-dist-embed");

    if embedded.exists() {
        fs::remove_dir_all(&embedded)?;
    }
    fs::create_dir_all(&embedded)?;

    if source.join("index.html").is_file() {
        copy_dir(&source, &embedded)?;
        emit_rerun_for_dir(&source)?;
    } else {
        write_missing_dist_placeholder(&embedded)?;
        println!(
            "cargo:warning=web/dist was not found; embedded web UI placeholder. Run `npm --prefix web run build` before packaging the binary."
        );
        println!("cargo:rerun-if-changed={}", source.display());
    }

    Ok(())
}

fn copy_dir(source: &Path, destination: &Path) -> io::Result<()> {
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());

        if entry.file_type()?.is_dir() {
            fs::create_dir_all(&destination_path)?;
            copy_dir(&source_path, &destination_path)?;
        } else {
            fs::copy(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn emit_rerun_for_dir(path: &Path) -> io::Result<()> {
    println!("cargo:rerun-if-changed={}", path.display());
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        if entry.file_type()?.is_dir() {
            emit_rerun_for_dir(&entry_path)?;
        } else {
            println!("cargo:rerun-if-changed={}", entry_path.display());
        }
    }
    Ok(())
}

fn write_missing_dist_placeholder(destination: &Path) -> io::Result<()> {
    let mut file = fs::File::create(destination.join("index.html"))?;
    file.write_all(
        br#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Agent Router</title>
  </head>
  <body>
    <main>
      <h1>Agent Router web UI is not bundled</h1>
      <p>Run <code>npm --prefix web run build</code> before building the Rust binary.</p>
    </main>
  </body>
</html>
"#,
    )
}
