//! A pipe whose source is an `api:` pipe is fetched in windows read one at a time (v4:
//! `get_data(as_iterator=True)`), never past the server's row limit: a full read is split in
//! half and read again (tied axis values included), one that cannot be split is an error, a
//! smaller server limit is learned from its refusal, the source's `fetch.params` filter every
//! read, and each window is read only when the sync asks for the next chunk.
//!
//! The server is a routing fake of the four routes involved (`sync_time`, `chunk_bounds`,
//! `data` and the pipe's metadata), answering at most `max_rows` rows per read as `/data` does.

use std::sync::{Arc, Mutex};

use meerschaum_connector::traits::{Connector, DataQuery, FetchedKind};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::PipeInput;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

/// One source row: its axis value and station.
type Row = (i64, &'static str);

/// One `/data` request: `(begin, end, limit, station filter)`.
type Read = (Option<i64>, Option<i64>, Option<i64>, Option<String>);

struct Source {
    rows: Vec<Row>,
    /// The server's `max_response_row_limit`.
    max_rows: i64,
    /// The width of the server's chunk bounds.
    chunk: i64,
    /// `/data` answers `500` (a failed read).
    broken: bool,
    /// Each `/data` request: `(begin, end, limit, station filter)`.
    reads: Mutex<Vec<Read>>,
}

fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("20");
                out.push(u8::from_str_radix(hex, 16).unwrap_or(b' '));
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_of(target: &str) -> (String, Map) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut map = Map::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(decode(k), Value::Str(decode(v)));
    }
    (path.to_string(), map)
}

fn int(map: &Map, key: &str) -> Option<i64> {
    match map.get(key) {
        Some(Value::Str(s)) => s.parse().ok(),
        _ => None,
    }
}

impl Source {
    fn answer(&self, target: &str) -> (u16, String) {
        let (path, q) = query_of(target);
        if path.ends_with("/sync_time") {
            let newest =
                matches!(q.get("newest"), Some(Value::Str(s)) if s.eq_ignore_ascii_case("true"));
            let axis = self.rows.iter().map(|(n, _)| *n);
            let value = if newest { axis.max() } else { axis.min() };
            return (200, value.map_or("null".into(), |v| v.to_string()));
        }
        if path.ends_with("/chunk_bounds") {
            let (begin, end) = (int(&q, "begin").unwrap(), int(&q, "end").unwrap());
            let mut bounds = Vec::new();
            let mut b = begin;
            while b < end {
                let e = (b + self.chunk).min(end);
                bounds.push(format!("[{b}, {e}]"));
                b = e;
            }
            return (200, format!("[{}]", bounds.join(", ")));
        }
        if path.ends_with("/data") {
            let (begin, end, limit) = (int(&q, "begin"), int(&q, "end"), int(&q, "limit"));
            let station = match q.get("params") {
                Some(Value::Str(s)) => serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .and_then(|p| p.get("station")?.as_str().map(str::to_string)),
                _ => None,
            };
            self.reads
                .lock()
                .unwrap()
                .push((begin, end, limit, station.clone()));
            if self.broken {
                return (500, r#"{"detail":"the database is down"}"#.into());
            }
            let limit = limit.unwrap_or(self.max_rows);
            if limit > self.max_rows {
                return (
                    413,
                    format!(
                        r#"{{"detail":"Requested limit {limit} exceeds the maximum response size of {} rows."}}"#,
                        self.max_rows
                    ),
                );
            }
            let docs: Vec<String> = self
                .rows
                .iter()
                .filter(|(n, s)| {
                    begin.is_none_or(|b| *n >= b)
                        && end.is_none_or(|e| *n < e)
                        && station.as_deref().is_none_or(|want| want == *s)
                })
                .take(limit as usize)
                .map(|(n, s)| format!(r#"{{"n": {n}, "station": "{s}"}}"#))
                .collect();
            return (200, format!("[{}]", docs.join(", ")));
        }
        if path.ends_with("/exists") {
            return (200, "true".into());
        }
        (200, "{}".into())
    }
}

async fn serve(source: Arc<Source>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let source = source.clone();
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);
                loop {
                    let mut line = String::new();
                    if read.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let target = line.split_whitespace().nth(1).unwrap_or("").to_string();
                    let mut length = 0usize;
                    loop {
                        let mut header = String::new();
                        if read.read_line(&mut header).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let header = header.trim_end();
                        if header.is_empty() {
                            break;
                        }
                        if let Some((name, value)) = header.split_once(':')
                            && name.trim().eq_ignore_ascii_case("content-length")
                        {
                            length = value.trim().parse().unwrap_or(0);
                        }
                    }
                    let mut body = vec![0u8; length];
                    if read.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let (status, reply) = source.answer(&target);
                    let response = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{reply}",
                        reply.len()
                    );
                    if write.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    port
}

fn source(rows: Vec<Row>, max_rows: i64, chunk: i64) -> Arc<Source> {
    Arc::new(Source {
        rows,
        max_rows,
        chunk,
        broken: false,
        reads: Mutex::new(Vec::new()),
    })
}

fn connector(port: u16) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("api_key".into(), Value::Str("k".into()));
    APIConnector::new(Some("chunks"), attrs).unwrap()
}

/// The pipe fetched from the API: its source is `plugin:src.temp`, filtered by `station`.
fn pipe(station: Option<&str>) -> Pipe {
    let params = station.map_or(String::new(), |s| {
        format!(r#", "params": {{"station": "{s}"}}"#)
    });
    let parameters: serde_json::Value = serde_json::from_str(&format!(
        r#"{{"columns": {{"datetime": "n"}}, "fetch": {{"pipe": {{"connector": "plugin:src", "metric": "temp"}}{params}}}}}"#
    ))
    .unwrap();
    Pipe::builder("api:chunks", "temp")
        .parameters(meerschaum_connector::pipe_ops::json_to_core(&parameters).to_map())
        .build()
        .unwrap()
}

fn values(input: &PipeInput) -> Vec<i64> {
    let PipeInput::Frame(df) = input else {
        panic!("a frame");
    };
    df.data("n")
        .unwrap()
        .values()
        .into_iter()
        .map(|v| match v {
            meerschaum_kernel::value::Value::Int(i) => i,
            meerschaum_kernel::value::Value::Float(f) => f as i64,
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Every chunk the sync would get, in order.
async fn all_chunks(conn: &APIConnector, pipe: &Pipe) -> Result<Vec<Vec<i64>>, String> {
    let mut chunked = conn
        .fetch_chunked(pipe, &DataQuery::default(), true)
        .await
        .map_err(|e| e.to_string())?
        .expect("an api: source is fetched in chunks");
    let mut chunks = Vec::new();
    while let Some(chunk) = chunked
        .chunks
        .next_chunk()
        .await
        .map_err(|e| e.to_string())?
    {
        chunks.push(values(&chunk));
    }
    assert_eq!(
        chunked.chunks.finish().await.unwrap(),
        FetchedKind::Data { generator: true }
    );
    Ok(chunks)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_first_load_over_the_servers_limit_is_read_whole() {
    // Forty rows (three of them tied on one axis value), a server sending five rows per read,
    // chunks of ten.
    let mut rows: Vec<Row> = (0..40).map(|n| (n, "a")).collect();
    rows.extend([(7, "a"), (7, "a")]);
    let src = source(rows.clone(), 5, 10);
    let conn = connector(serve(src.clone()).await);
    let chunks = all_chunks(&conn, &pipe(None)).await.unwrap();
    let mut got: Vec<i64> = chunks.concat();
    got.sort();
    let mut want: Vec<i64> = rows.iter().map(|(n, _)| *n).collect();
    want.sort();
    assert_eq!(
        got, want,
        "rows past the server's limit were lost or repeated"
    );
    assert!(chunks.iter().all(|c| c.len() < 5));
    // The first read asked for the client's limit; the server's own was used after its refusal.
    let reads = src.reads.lock().unwrap().clone();
    assert_eq!(reads[0].2, Some(100_000));
    assert!(reads[1..].iter().all(|r| r.2 == Some(5)), "{reads:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_are_read_as_the_sync_asks_for_them_with_the_sources_filter() {
    let rows: Vec<Row> = (0..30).flat_map(|n| [(n, "a"), (n, "b")]).collect();
    let src = source(rows, 100_000, 10);
    let conn = connector(serve(src.clone()).await);
    let mut chunked = conn
        .fetch_chunked(&pipe(Some("a")), &DataQuery::default(), true)
        .await
        .unwrap()
        .unwrap();
    let first = chunked.chunks.next_chunk().await.unwrap().unwrap();
    assert_eq!(values(&first), (0..10).collect::<Vec<_>>());
    // One window read so far: the next is read only when the sync asks for it.
    assert_eq!(src.reads.lock().unwrap().len(), 1);
    let mut rest = Vec::new();
    while let Some(chunk) = chunked.chunks.next_chunk().await.unwrap() {
        rest.extend(values(&chunk));
    }
    assert_eq!(rest, (10..30).collect::<Vec<_>>());
    let reads = src.reads.lock().unwrap().clone();
    assert_eq!(reads.len(), 3);
    assert!(
        reads.iter().all(|r| r.3.as_deref() == Some("a")),
        "a read lost the source's filter: {reads:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_that_cannot_be_read_whole_and_failed_reads_are_errors() {
    // Six rows at one axis value, a server sending five: no smaller window holds fewer.
    let src = source(vec![(3, "a"); 6], 5, 10);
    let conn = connector(serve(src).await);
    let error = all_chunks(&conn, &pipe(None)).await.unwrap_err();
    assert!(error.contains("the rest would be lost"), "{error}");
    // The single-frame `fetch` never answers a truncated frame either.
    let error = conn
        .fetch(&pipe(None), &DataQuery::default())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("the rest would be lost"),
        "{error}"
    );

    let broken = Arc::new(Source {
        broken: true,
        ..Arc::try_unwrap(source(vec![(1, "a")], 5, 10)).ok().unwrap()
    });
    let conn = connector(serve(broken).await);
    let error = all_chunks(&conn, &pipe(None)).await.unwrap_err();
    assert!(error.contains("the database is down"), "{error}");
}
