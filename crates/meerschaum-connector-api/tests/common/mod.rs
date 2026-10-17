//! A scripted HTTP/1.1 + websocket server for contract tests: it records every request and
//! answers from a queue of canned responses (or forwards to an upstream server).

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

/// A request as the server saw it.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// Path and query, as sent on the request line.
    pub target: String,
    /// Lower-cased header names with values.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// A canned response.
#[derive(Debug, Clone)]
pub struct Canned {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
}

/// A scripted websocket exchange: `("send", text)` is expected from the client (recorded),
/// `("recv", text)` is sent to the client, `("close", _)` closes.
pub type WsScript = Vec<(String, Option<String>)>;

#[derive(Default)]
struct State {
    responses: VecDeque<Canned>,
    recorded: Vec<Recorded>,
    ws_scripts: VecDeque<WsScript>,
    ws_received: Vec<Vec<String>>,
    upstream: Option<String>,
}

/// The mock server.
#[derive(Clone)]
pub struct MockServer {
    pub port: u16,
    state: Arc<Mutex<State>>,
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

async fn read_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Option<(String, String, Vec<(String, String)>, Vec<u8>, Vec<u8>)> {
    let mut tmp = [0u8; 8192];
    let head_end = loop {
        if let Some(e) = find_header_end(buf) {
            break e;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let raw_head = buf[..head_end].to_vec();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?.to_string();
    let mut parts = request_line.split(' ');
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let headers: Vec<(String, String)> = lines
        .filter(|l| !l.is_empty())
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
        .collect();
    let rest = buf[head_end..].to_vec();
    buf.clear();
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let mut body = rest;
    if let Some(len) = get("content-length").and_then(|l| l.parse::<usize>().ok()) {
        while body.len() < len {
            let n = stream.read(&mut tmp).await.ok()?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        let extra = body.split_off(len.min(body.len()));
        buf.extend_from_slice(&extra);
    } else if get("transfer-encoding").is_some_and(|t| t.contains("chunked")) {
        let mut decoded = Vec::new();
        loop {
            while !body.windows(2).any(|w| w == b"\r\n") {
                let n = stream.read(&mut tmp).await.ok()?;
                if n == 0 {
                    return None;
                }
                body.extend_from_slice(&tmp[..n]);
            }
            let pos = body.windows(2).position(|w| w == b"\r\n")?;
            let size_line = String::from_utf8_lossy(&body[..pos]).to_string();
            let size = usize::from_str_radix(size_line.trim(), 16).ok()?;
            let start = pos + 2;
            while body.len() < start + size + 2 {
                let n = stream.read(&mut tmp).await.ok()?;
                if n == 0 {
                    return None;
                }
                body.extend_from_slice(&tmp[..n]);
            }
            decoded.extend_from_slice(&body[start..start + size]);
            body.drain(..start + size + 2);
            if size == 0 {
                break;
            }
        }
        buf.extend_from_slice(&body);
        body = decoded;
    } else {
        buf.extend_from_slice(&body);
        body = Vec::new();
    }
    Some((method, target, headers, body, raw_head))
}

impl MockServer {
    /// Start a server on a free port.
    pub async fn start() -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().unwrap().port();
        let server = MockServer {
            port,
            state: Arc::new(Mutex::new(State::default())),
        };
        let s2 = server.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let s3 = s2.clone();
                tokio::spawn(async move { s3.serve(stream).await });
            }
        });
        server
    }

    /// Forward every request to `upstream` (`host:port`) instead of answering from the queue.
    pub fn set_upstream(&self, upstream: Option<String>) {
        self.state.lock().unwrap().upstream = upstream;
    }

    pub fn push_response(&self, canned: Canned) {
        self.state.lock().unwrap().responses.push_back(canned);
    }

    pub fn push_ws_script(&self, script: WsScript) {
        self.state.lock().unwrap().ws_scripts.push_back(script);
    }

    /// Take the recorded requests (and pending canned responses, which must be empty).
    pub fn take(&self) -> (Vec<Recorded>, usize, Vec<Vec<String>>) {
        let mut st = self.state.lock().unwrap();
        let pending = st.responses.len();
        st.responses.clear();
        st.ws_scripts.clear();
        (
            std::mem::take(&mut st.recorded),
            pending,
            std::mem::take(&mut st.ws_received),
        )
    }

    async fn serve(&self, mut stream: TcpStream) {
        let mut buf = Vec::new();
        while let Some((method, target, headers, body, raw_head)) =
            read_request(&mut stream, &mut buf).await
        {
            let is_ws = headers
                .iter()
                .any(|(k, v)| k == "upgrade" && v.eq_ignore_ascii_case("websocket"));
            self.state.lock().unwrap().recorded.push(Recorded {
                method: method.clone(),
                target: target.clone(),
                headers: headers.clone(),
                body: body.clone(),
            });
            if is_ws {
                let upstream = self.state.lock().unwrap().upstream.clone();
                match upstream {
                    Some(up) => self.proxy_ws(stream, &headers, &up, &target).await,
                    None => self.serve_ws(stream, &headers).await,
                }
                return;
            }
            let upstream = self.state.lock().unwrap().upstream.clone();
            let bytes = match upstream {
                Some(up) => match forward(&up, &raw_head, &body).await {
                    Some(b) => b,
                    None => return,
                },
                None => {
                    let canned = self.state.lock().unwrap().responses.pop_front();
                    let canned = canned.unwrap_or(Canned {
                        status: 599,
                        content_type: "text/plain".into(),
                        body: b"unexpected request".to_vec(),
                    });
                    if canned.status == 0 {
                        // A recorded connection reset.
                        return;
                    }
                    let mut out = format!(
                        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\n\r\n",
                        canned.status,
                        reason(canned.status),
                        canned.content_type,
                        canned.body.len()
                    )
                    .into_bytes();
                    out.extend_from_slice(&canned.body);
                    out
                }
            };
            if stream.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }

    async fn accept_ws(stream: &mut TcpStream, headers: &[(String, String)]) -> Option<()> {
        let key = headers
            .iter()
            .find(|(k, _)| k == "sec-websocket-key")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
        );
        stream.write_all(response.as_bytes()).await.ok()
    }

    /// Forward a websocket to the upstream server, recording the client's text messages.
    async fn proxy_ws(
        &self,
        mut stream: TcpStream,
        headers: &[(String, String)],
        upstream: &str,
        target: &str,
    ) {
        let Ok((up, _)) =
            tokio_tungstenite::connect_async(format!("ws://{upstream}{target}")).await
        else {
            return;
        };
        if Self::accept_ws(&mut stream, headers).await.is_none() {
            return;
        }
        let client = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
        let (mut ctx_, mut crx) = client.split();
        let (mut utx, mut urx) = up.split();
        let received = Arc::new(Mutex::new(Vec::new()));
        let rec = received.clone();
        let c2u = async move {
            while let Some(Ok(msg)) = crx.next().await {
                if let Message::Text(t) = &msg {
                    rec.lock().unwrap().push(t.to_string());
                }
                let close = matches!(msg, Message::Close(_));
                if utx.send(msg).await.is_err() || close {
                    break;
                }
            }
        };
        let u2c = async move {
            while let Some(Ok(msg)) = urx.next().await {
                let close = matches!(msg, Message::Close(_));
                if ctx_.send(msg).await.is_err() || close {
                    break;
                }
            }
        };
        tokio::select! {
            _ = c2u => {},
            _ = u2c => {},
        }
        let got = received.lock().unwrap().clone();
        self.state.lock().unwrap().ws_received.push(got);
    }

    async fn serve_ws(&self, mut stream: TcpStream, headers: &[(String, String)]) {
        let key = headers
            .iter()
            .find(|(k, _)| k == "sec-websocket-key")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: {accept}\r\n\r\n"
        );
        if stream.write_all(response.as_bytes()).await.is_err() {
            return;
        }
        let ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
        let (mut tx, mut rx) = ws.split();
        let script = self
            .state
            .lock()
            .unwrap()
            .ws_scripts
            .pop_front()
            .unwrap_or_default();
        let mut received = Vec::new();
        for (kind, data) in script {
            match kind.as_str() {
                "send" => loop {
                    match rx.next().await {
                        Some(Ok(Message::Text(t))) => {
                            received.push(t.to_string());
                            break;
                        }
                        Some(Ok(_)) => continue,
                        _ => break,
                    }
                },
                "recv" => {
                    let _ = tx.send(Message::text(data.unwrap_or_default())).await;
                }
                _ => {
                    // `close`: wait for the client's close frame or the end of the stream.
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(2), rx.next()).await;
                    break;
                }
            }
        }
        let _ = tx.send(Message::Close(None)).await;
        self.state.lock().unwrap().ws_received.push(received);
    }
}

/// Forward one request to `upstream` with `Connection: close` and return the raw response.
async fn forward(upstream: &str, raw_head: &[u8], body: &[u8]) -> Option<Vec<u8>> {
    let head = String::from_utf8_lossy(raw_head).to_string();
    let mut lines: Vec<String> = head
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .filter(|l| {
            let lower = l.to_lowercase();
            !lower.starts_with("connection:")
                && !lower.starts_with("transfer-encoding:")
                && !lower.starts_with("content-length:")
        })
        .map(String::from)
        .collect();
    lines.push("connection: close".into());
    lines.push(format!("content-length: {}", body.len()));
    let mut req = lines.join("\r\n").into_bytes();
    req.extend_from_slice(b"\r\n\r\n");
    req.extend_from_slice(body);
    let mut up = TcpStream::connect(upstream).await.ok()?;
    up.write_all(&req).await.ok()?;
    let mut out = Vec::new();
    up.read_to_end(&mut out).await.ok()?;
    // Re-frame the response with a content length so the client may keep the connection.
    let head_end = find_header_end(&out)?;
    let head = String::from_utf8_lossy(&out[..head_end]).to_string();
    let mut body = out[head_end..].to_vec();
    let chunked = head.to_lowercase().contains("transfer-encoding: chunked");
    if chunked {
        let mut decoded = Vec::new();
        let mut rest = body.as_slice();
        while let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
            let size = usize::from_str_radix(String::from_utf8_lossy(&rest[..pos]).trim(), 16)
                .unwrap_or(0);
            if size == 0 {
                break;
            }
            decoded.extend_from_slice(&rest[pos + 2..pos + 2 + size]);
            rest = &rest[pos + 2 + size + 2..];
        }
        body = decoded;
    }
    let mut lines: Vec<String> = head
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .filter(|l| {
            let lower = l.to_lowercase();
            !lower.starts_with("connection:")
                && !lower.starts_with("transfer-encoding:")
                && !lower.starts_with("content-length:")
        })
        .map(String::from)
        .collect();
    lines.push(format!("content-length: {}", body.len()));
    let mut res = lines.join("\r\n").into_bytes();
    res.extend_from_slice(b"\r\n\r\n");
    res.extend_from_slice(&body);
    Some(res)
}
