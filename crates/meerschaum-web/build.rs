//! Compress the vendored console assets at build time.
//!
//! Every file in `assets/` (except licenses and `versions.json`) is gzip-compressed at the best
//! level into `OUT_DIR/assets/<name>.gz`, and `OUT_DIR/assets_gen.rs` gets a table with the name,
//! MIME type, content hash and the embedded bytes, which `src/assets.rs` includes. The source
//! files stay uncompressed in git; only the compressed copies are embedded in the binary.
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use std::{env, fmt::Write as _, fs, io::Write as _, path::PathBuf};

fn mime(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("js" | "mjs") => "text/javascript",
        Some("css") => "text/css",
        other => panic!("no MIME type for asset {name} ({other:?})"),
    }
}

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let assets = manifest.join("assets");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=assets");
    let gz_dir = out.join("assets");
    fs::create_dir_all(&gz_dir).expect("create OUT_DIR/assets");

    let mut names: Vec<String> = fs::read_dir(&assets)
        .expect("read assets/")
        .map(|entry| {
            entry
                .expect("assets/ entry")
                .file_name()
                .into_string()
                .expect("UTF-8 asset name")
        })
        .filter(|name| !name.ends_with(".LICENSE") && name != "versions.json")
        .collect();
    names.sort();

    let mut table = String::from("pub(crate) static ASSETS: &[Asset] = &[\n");
    for name in &names {
        println!("cargo:rerun-if-changed=assets/{name}");
        let raw = fs::read(assets.join(name)).expect("read asset");
        let hash: String =
            Sha256::digest(&raw)
                .iter()
                .take(8)
                .fold(String::new(), |mut acc, byte| {
                    let _ = write!(acc, "{byte:02x}");
                    acc
                });
        // The gzip header carries no modification time, so the output is reproducible.
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(&raw).expect("compress asset");
        let gzipped = encoder.finish().expect("finish gzip");
        fs::write(gz_dir.join(format!("{name}.gz")), &gzipped).expect("write compressed asset");
        let _ = writeln!(
            table,
            "    Asset {{ name: {name:?}, mime: {mime:?}, hash: {hash:?}, raw_len: {raw_len}, \
gzip: include_bytes!(concat!(env!(\"OUT_DIR\"), \"/assets/{name}.gz\")) }},",
            mime = mime(name),
            raw_len = raw.len(),
        );
    }
    table.push_str("];\n");
    fs::write(out.join("assets_gen.rs"), table).expect("write assets_gen.rs");
}
