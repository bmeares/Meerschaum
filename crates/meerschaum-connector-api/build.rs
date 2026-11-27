//! Preserve Cargo's exact compilation target for native plugin archive negotiation.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rustc-env=MRSM_PLUGIN_TARGET={}",
        std::env::var("TARGET").unwrap()
    );
}
