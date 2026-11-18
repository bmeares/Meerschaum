//! Asynchronous job logs and stdin, without a blocking input callback on an API runtime.

use futures_util::{SinkExt, StreamExt};
use meerschaum_core::SuccessTuple;
use tokio_tungstenite::tungstenite::Message;

use super::APIConnector;
use crate::routes::{JOBS_STDIN_MESSAGE, JOBS_STOP_MESSAGE};
use crate::traits::{ConnectorError, ConnectorResult};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A bounded, authenticated channel using the ordinary jobs websocket protocol.
pub struct LogChannel {
    socket: Socket,
    stopped: bool,
    result_pending: bool,
    input_pending: bool,
}

/// One job-log event. Values from an executor remain untrusted application data.
#[derive(Debug)]
pub enum LogMessage {
    /// Log text (possibly several lines).
    Line(String),
    /// The job currently requests stdin.
    InputRequested,
    /// A final result.
    Stopped(SuccessTuple),
}

impl APIConnector {
    /// Open logs with request authentication and the legacy first-frame bearer credential.
    pub async fn open_log_channel(&self, name: &str) -> ConnectorResult<LogChannel> {
        let url = self.logs_websocket_url(name);
        let mut bounded = self.clone();
        bounded.response_limit = Some(self.response_limit.unwrap_or(64 * 1024).min(64 * 1024));
        let (mut socket, _) = bounded.connect_websocket(&url).await?;
        let parsed = reqwest::Url::parse(&url)
            .map_err(|_| ConnectorError::Invalid("Invalid logs websocket URL.".into()))?;
        let token = match self.custom_authorization("GET", &parsed, Some(&[]))? {
            Some(authorization) => authorization
                .strip_prefix("Bearer ")
                .ok_or_else(|| {
                    ConnectorError::Invalid("Logs require bearer authentication.".into())
                })?
                .to_string(),
            None => self.token().await.unwrap_or_else(|| "no-login".into()),
        };
        socket.send(Message::text(token)).await.map_err(|_| {
            ConnectorError::Backend("Could not authenticate the logs websocket.".into())
        })?;
        Ok(LogChannel {
            socket,
            stopped: false,
            result_pending: false,
            input_pending: false,
        })
    }
}

impl LogChannel {
    /// Receive without blocking the API's executor thread. Cancellation preserves protocol
    /// state, including a stop marker whose result arrives in the next websocket message.
    pub async fn next(&mut self) -> ConnectorResult<Option<LogMessage>> {
        if self.stopped {
            return Ok(None);
        }
        loop {
            let text = match self.socket.next().await {
                Some(Ok(Message::Text(text))) => text.to_string(),
                Some(Ok(Message::Binary(bytes))) => String::from_utf8_lossy(&bytes).into_owned(),
                Some(Ok(Message::Close(_))) | None => {
                    self.stopped = true;
                    return Ok(None);
                }
                Some(Ok(_)) => continue,
                Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed
                    | tokio_tungstenite::tungstenite::Error::AlreadyClosed
                    | tokio_tungstenite::tungstenite::Error::Protocol(tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake))) => {
                    self.stopped = true;
                    return Ok(None);
                }
                Some(Err(_)) => {
                    self.stopped = true;
                    return Err(ConnectorError::Backend(
                        "The logs websocket was interrupted.".into(),
                    ));
                }
            };
            if self.result_pending {
                self.stopped = true;
                let result = crate::models::parse_success_tuple(false, &text);
                return Ok(Some(LogMessage::Stopped(result)));
            }
            if text == JOBS_STOP_MESSAGE {
                self.result_pending = true;
                continue;
            }
            if text == JOBS_STDIN_MESSAGE {
                self.input_pending = true;
                return Ok(Some(LogMessage::InputRequested));
            }
            return Ok(Some(LogMessage::Line(text)));
        }
    }

    /// Send stdin only after a request. The main API must separately recheck current caller
    /// authorization before forwarding input to an executor.
    pub async fn send_input(&mut self, input: String) -> ConnectorResult<()> {
        if !self.input_pending || self.stopped || input.len() > 1024 * 1024 {
            return Err(ConnectorError::Invalid(
                "The job is not accepting this input.".into(),
            ));
        }
        self.socket
            .send(Message::text(input))
            .await
            .map_err(|_| ConnectorError::Backend("Could not send the job's input.".into()))?;
        self.input_pending = false;
        Ok(())
    }

    /// Close this monitor without stopping its job.
    pub async fn close(&mut self) {
        self.stopped = true;
        let _ = self.socket.close(None).await;
    }
}
