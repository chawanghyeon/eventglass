use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_EMBED_UI");
    if env::var_os("CARGO_FEATURE_EMBED_UI").is_none() {
        return;
    }
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest directory"));
    let dist = root.join("web/dist");
    assert!(
        dist.join("index.html").is_file(),
        "embed-ui requires web/dist/index.html; run npm ci and npm run build in web first"
    );
    println!("cargo:rerun-if-changed={}", dist.display());
    let mut files = Vec::new();
    collect(&dist, &mut files);
    files.sort();
    let mut source = String::from("pub static ASSETS: &[(&str, &[u8])] = &[\n");
    for file in files {
        let name = file
            .strip_prefix(&dist)
            .expect("asset inside dist")
            .to_str()
            .expect("UTF-8 asset path")
            .replace('\\', "/");
        source.push_str(&format!(
            "({name:?}, include_bytes!({:?})),\n",
            file.to_str().expect("UTF-8 asset path")
        ));
    }
    source.push_str("];\n");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").expect("output directory")).join("web_assets.rs"),
        source,
    )
    .expect("write embedded assets");
}

fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read web assets") {
        let entry = entry.expect("asset entry");
        let kind = entry.file_type().expect("asset type");
        assert!(
            !kind.is_symlink(),
            "web assets may not contain symbolic links"
        );
        if kind.is_dir() {
            collect(&entry.path(), files);
        } else if kind.is_file() {
            files.push(entry.path());
        }
    }
}
