//! The API client's warnings (`meerschaum_api::client::warnings`), printed through the CLI's
//! formatter on stderr like v4's `warn(msg, stack=False)`.
//!
//! The client may run on another thread of the session's runtime, so its sink only queues the
//! message; [`flush`] (called before `Runtime::print` / `eprint` write anything) prints what is
//! queued, so a warning appears before the output that follows the request that raised it.

use std::sync::{Arc, Mutex, Once};

use crate::dispatch::Runtime;

static PENDING: Mutex<Vec<String>> = Mutex::new(Vec::new());
static INSTALL: Once = Once::new();

/// The most warnings kept before the oldest are dropped (a process that never prints, such as a
/// server, must not grow the queue without bound).
const MAX_PENDING: usize = 256;

/// Route the client's warnings into the queue (once per process). Only the `mrsm` binary's
/// `main` calls this: an embedding host (the Python extension) and the long-running server
/// have no one to print the queue, so there the client's warnings stay on `tracing`.
pub fn install() {
    INSTALL.call_once(|| {
        meerschaum_api::client::warnings::set_warning_sink(Some(Arc::new(enqueue)));
    });
}

fn enqueue(message: &str) {
    enqueue_into(&PENDING, message);
}

/// Append to `queue`, dropping the oldest message at the cap.
fn enqueue_into(queue: &Mutex<Vec<String>>, message: &str) {
    if let Ok(mut pending) = queue.lock() {
        if pending.len() >= MAX_PENDING {
            pending.remove(0);
        }
        pending.push(message.to_string());
    }
}

/// Remove the queuing sink (the server calls this: nothing would drain the queue) and drop what
/// is queued.
pub fn uninstall() {
    meerschaum_api::client::warnings::set_warning_sink(None);
    if let Ok(mut pending) = PENDING.lock() {
        pending.clear();
    }
}

/// Print the queued warnings (`warn`: the warning icon and style, a trailing newline).
pub fn flush(rt: &mut Runtime<'_>) {
    let messages = match PENDING.lock() {
        Ok(mut pending) if !pending.is_empty() => std::mem::take(&mut *pending),
        _ => return,
    };
    for message in messages {
        let text = rt.with_formatter(|f| f.warn(&message));
        use std::io::Write;
        let _ = rt.err.write_all(text.as_bytes());
        let _ = rt.err.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_queue_is_capped_and_keeps_the_newest() {
        // A queue of its own: the process-wide one is shared with every other test.
        let queue = Mutex::new(Vec::new());
        for i in 0..(MAX_PENDING + 10) {
            enqueue_into(&queue, &format!("warning {i}"));
        }
        let pending = queue.lock().unwrap();
        assert_eq!(pending.len(), MAX_PENDING);
        assert_eq!(pending[0], "warning 10");
        assert_eq!(
            pending[MAX_PENDING - 1],
            format!("warning {}", MAX_PENDING + 9)
        );
    }
}
