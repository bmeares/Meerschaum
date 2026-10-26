//! v4 tells "no rows" from "no data": a Python plugin's `fetch()` returning `[]` syncs as
//! "No new rows were returned", `None` as "No data were fetched"; an instance's
//! `get_pipe_data` returns an empty DataFrame for a pipe without rows in range and `None` when
//! there is no data at all. A single-file plugin's empty `Vec<Value>` crosses the rows ABI as
//! zero rows (`[]`), and `Option<Vec<Value>>` expresses `None` (`null`). All data is synthetic.
use meerschaum_connector::{Connector, DataQuery, DateBound, FetchedKind, InstanceConnector};
use meerschaum_core::Pipe;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_plugin::{LoadedPlugin, discovery, serde_json::json};
use std::sync::Arc;
mod support {
    pub mod compile_single_file;
}

const SOURCE: &str = r##"//! Returns no rows, or no data.
fn fetch() -> Vec<Value> {
    vec![]
}

#[make_connector]
mod maybe {
    use super::*;

    fn fetch(connector: &Connector) -> Option<Vec<Value>> {
        (connector.label() == "empty").then(Vec::new)
    }
}

#[make_connector(instance)]
mod store {
    use super::*;

    /// No table before `begin` is known (`None`), no rows in range after (`[]`).
    fn get_pipe_data(begin: Option<Bound>) -> Option<Vec<Value>> {
        begin.map(|_| Vec::new())
    }
}
"##;

fn compiled() -> (tempfile::TempDir, Arc<LoadedPlugin>) {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("empty.rs"), SOURCE).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this crate's runtime.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    (root, plugin)
}

/// What `fetch_chunked` hands the sync: the single chunk and how the fetch finished.
async fn fetched(source: &dyn Connector, pipe: &Pipe) -> (Option<PipeInput>, FetchedKind) {
    let mut chunked = source
        .fetch_chunked(pipe, &DataQuery::default(), true)
        .await
        .unwrap()
        .expect("rows-ABI plugins fetch documents");
    let chunk = chunked.chunks.next_chunk().await.unwrap();
    assert!(chunked.chunks.next_chunk().await.unwrap().is_none());
    (chunk, chunked.chunks.finish().await.unwrap())
}

#[test]
fn empty_rows_are_zero_rows_and_none_is_no_data() {
    let (_root, plugin) = compiled();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pipe = Pipe::builder("plugin:empty", "readings")
        .instance("store:abi")
        .build()
        .unwrap();
    rt.block_on(async {
        // `fn fetch() -> Vec<Value>` returning `vec![]`, and `Option<Vec<Value>>` returning
        // `Some(vec![])`: zero documents, which the sync reports as "No new rows".
        for source in [
            plugin.source(json!({})),
            plugin.connector("maybe", "empty", json!({})).unwrap(),
        ] {
            let (chunk, kind) = fetched(&source, &pipe).await;
            assert!(
                matches!(chunk, Some(PipeInput::Docs(ref docs)) if docs.is_empty()),
                "{chunk:?}"
            );
            assert!(matches!(kind, FetchedKind::Data { generator: false }));
            let frame = source.fetch(&pipe, &DataQuery::default()).await.unwrap();
            assert_eq!(frame.map(|f| f.height()), Some(0));
        }
        // `None`: no data at all ("No data were fetched").
        let none = plugin.connector("maybe", "none", json!({})).unwrap();
        let (chunk, kind) = fetched(&none, &pipe).await;
        assert!(chunk.is_none());
        assert!(matches!(kind, FetchedKind::Nothing));
        assert!(
            none.fetch(&pipe, &DataQuery::default())
                .await
                .unwrap()
                .is_none()
        );

        // An instance's `get_pipe_data`: an empty frame vs `None`.
        let store = plugin.connector("store", "abi", json!({})).unwrap();
        let in_range = DataQuery {
            begin: Some(DateBound::Int(2)),
            ..Default::default()
        };
        let data = store.get_pipe_data(&pipe, &in_range).await.unwrap();
        assert_eq!(data.map(|f| f.height()), Some(0));
        let data = store
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .unwrap();
        assert!(data.is_none());
    });
}
