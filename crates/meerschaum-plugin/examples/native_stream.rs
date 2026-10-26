//! A streaming connector `ticker:<label>` and the plugin-level stream `plugin:native_stream`:
//! each subscription emits `count` one-row batches (`id`, `value`), one every `interval_ms`,
//! then waits (timeouts) until the host closes it. The pipe's `parameters.fetch` sets `count`
//! (default 5), `interval_ms` (default 200) and `start` (the first `id`, default 1).
//! `ticker` batches carry acknowledgement tokens (the `id`); ids listed in `fetch.skip` are
//! "filtered": they arrive as empty batches that still carry their token. `fetch.max_unacked`
//! (a broker's in-flight window) holds the next batch back while that many tokens await their
//! acknowledgement. When
//! `NATIVE_STREAM_ACK_LOG` is set, every acknowledged token is appended to that file; when
//! `NATIVE_STREAM_KWARGS_LOG` is set, each `subscribe` appends the sync's `kwargs` (one JSON line).
//! All data is synthetic.
use meerschaum_plugin::{
    Error, Frame, Plugin, Request, Result,
    arrow::{
        array::{Float64Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    serde_json::{Value, json},
    subscribe::{Batch, ChannelSubscription, Subscription},
};
use std::{
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Default)]
struct Stream;

impl Plugin for Stream {
    const NAME: &'static str = "native_stream";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> Value {
        json!({"schema_version": meerschaum_plugin::MANIFEST_SCHEMA_VERSION,
            "meerschaum_version": "5.0.0-rc.14", "python_version": "", "fingerprint": null,
            "plugins": {"native_stream": {"name": "native_stream", "version": "1.0.0",
                "data_functions": ["subscribe"], "connectors": ["ticker"]}},
            "connector_types": {"ticker": {"type": "ticker", "plugin": "native_stream",
                "class_name": "Ticker", "is_instance": false, "is_thread_safe": true,
                "is_streaming": true, "methods": ["subscribe"]}}})
    }
    fn subscribe(&self, request: &Request) -> Result<Box<dyn Subscription>> {
        let fetch = &request.context["pipe"]["parameters"]["fetch"];
        let count = fetch["count"].as_i64().unwrap_or(5);
        let interval = Duration::from_millis(fetch["interval_ms"].as_u64().unwrap_or(200));
        let start = fetch["start"].as_i64().unwrap_or(1);
        if let Some(path) = std::env::var_os("NATIVE_STREAM_KWARGS_LOG") {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| Error::Message(e.to_string()))?;
            writeln!(file, "{}", request.context["kwargs"])
                .map_err(|e| Error::Message(e.to_string()))?;
        }
        if request.connector_type == "ticker" {
            // Pull model on the host's thread: no producer thread at all.
            return Ok(Box::new(Ticker {
                next: start,
                end: start + count,
                skip: fetch["skip"]
                    .as_array()
                    .map(|ids| ids.iter().filter_map(Value::as_i64).collect())
                    .unwrap_or_default(),
                max_unacked: fetch["max_unacked"].as_u64().map(|n| n as usize),
                unacked: Vec::new(),
                interval,
                due: Instant::now() + interval,
            }));
        }
        // `plugin:native_stream`: a producer thread behind a bounded channel.
        let (sender, subscription) = ChannelSubscription::new(1);
        std::thread::spawn(move || {
            for id in start..start + count {
                std::thread::sleep(interval);
                let Ok(frame) = row(id) else { return };
                if sender.send(frame).is_err() {
                    return;
                }
            }
            // Keep the subscription open (no `end`) until the host closes it.
            while let Ok(frame) = rows(&[])
                && sender.send(frame).is_ok()
            {
                std::thread::sleep(interval);
            }
        });
        Ok(Box::new(subscription))
    }
}

fn row(id: i64) -> Result<Frame> {
    rows(&[id])
}

fn rows(ids: &[i64]) -> Result<Frame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(
                ids.iter().map(|id| *id as f64 / 2.0).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(|e| Error::Message(e.to_string()))?;
    Frame::from_record_batch(&batch).map_err(|e| Error::Message(e.to_string()))
}

struct Ticker {
    next: i64,
    end: i64,
    skip: Vec<i64>,
    max_unacked: Option<usize>,
    unacked: Vec<u64>,
    interval: Duration,
    due: Instant,
}

impl Subscription for Ticker {
    fn next_batch(&mut self, timeout: Duration) -> Result<Batch> {
        let deadline = Instant::now() + timeout;
        let window_full = self.max_unacked.is_some_and(|n| self.unacked.len() >= n);
        if self.next >= self.end || self.due > deadline || window_full {
            std::thread::sleep(timeout);
            return Ok(Batch::Timeout);
        }
        std::thread::sleep(self.due.saturating_duration_since(Instant::now()));
        let id = self.next;
        self.next += 1;
        self.due = Instant::now() + self.interval;
        let frame = if self.skip.contains(&id) {
            rows(&[])?
        } else {
            row(id)?
        };
        self.unacked.push(id as u64);
        Ok(Batch::with_ack(frame, id as u64))
    }
    fn ack(&mut self, tokens: &[u64]) -> Result<()> {
        self.unacked.retain(|t| !tokens.contains(t));
        if let Some(path) = std::env::var_os("NATIVE_STREAM_ACK_LOG") {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| Error::Message(e.to_string()))?;
            for token in tokens {
                writeln!(file, "{token}").map_err(|e| Error::Message(e.to_string()))?;
            }
        }
        Ok(())
    }
}

meerschaum_plugin::export_plugin!(Stream);
