//! Bounded asynchronous websocket transport for signed executor terminals and plugin routes.

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

use super::APIConnector;
use crate::traits::{ConnectorError, ConnectorResult};

/// Bound both directions, including a terminal paste. Higher application layers may use less.
pub const MAX_SOCKET_MESSAGE_BYTES: usize = 1024 * 1024;

/// An authenticated connection; no public password login, retry, redirect or proxy fallback.
pub struct AuthenticatedSocket {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
}

/// Opaque application data. Never deserialize executor messages as executable objects.
#[derive(Debug, PartialEq, Eq)]
pub enum SocketMessage {
    /// UTF-8 application protocol message.
    Text(String),
    /// Bounded binary application data.
    Binary(Vec<u8>),
}

impl APIConnector {
    /// Open a same-origin websocket with the connector's request-specific authentication.
    /// The encoded path and query participate in signatures exactly as sent on the wire.
    pub async fn open_authenticated_socket(
        &self,
        path: &str,
    ) -> ConnectorResult<AuthenticatedSocket> {
        if self.request_authentication.is_none() {
            return Err(ConnectorError::Invalid(
                "This websocket requires request-specific authentication.".into(),
            ));
        }
        if !path.starts_with('/') || path.starts_with("//") || path.contains(['\\', '#']) {
            return Err(ConnectorError::Invalid(
                "Invalid authenticated websocket path.".into(),
            ));
        }
        let mut url = reqwest::Url::parse(&self.uri()).map_err(|_| {
            ConnectorError::Invalid("Invalid authenticated websocket address.".into())
        })?;
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => {
                return Err(ConnectorError::Invalid(
                    "Invalid authenticated websocket protocol.".into(),
                ));
            }
        };
        url.set_scheme(scheme).map_err(|_| {
            ConnectorError::Invalid("Invalid authenticated websocket protocol.".into())
        })?;
        url.set_username("").map_err(|_| {
            ConnectorError::Invalid("Invalid authenticated websocket address.".into())
        })?;
        url.set_password(None).map_err(|_| {
            ConnectorError::Invalid("Invalid authenticated websocket address.".into())
        })?;
        let (path, query) = path
            .split_once('?')
            .map_or((path, None), |(p, q)| (p, Some(q)));
        url.set_path(path);
        url.set_query(query);
        let mut bounded = self.clone();
        bounded.response_limit = Some(
            self.response_limit
                .unwrap_or(MAX_SOCKET_MESSAGE_BYTES)
                .min(MAX_SOCKET_MESSAGE_BYTES),
        );
        let (socket, _) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            bounded.connect_websocket(url.as_str()),
        )
        .await
        .map_err(|_| {
            ConnectorError::Backend("The authenticated websocket handshake timed out.".into())
        })?
        .map_err(|_| {
            ConnectorError::Backend("Could not open the authenticated websocket.".into())
        })?;
        Ok(AuthenticatedSocket { socket })
    }
}

impl AuthenticatedSocket {
    /// Receive one application message. Cancellation does not discard partially read frames.
    pub async fn next(&mut self) -> ConnectorResult<Option<SocketMessage>> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };
            match message {
                Ok(Message::Text(text)) => return Ok(Some(SocketMessage::Text(text.to_string()))),
                Ok(Message::Binary(bytes)) => {
                    return Ok(Some(SocketMessage::Binary(bytes.to_vec())));
                }
                Ok(Message::Close(_)) => return Ok(None),
                Ok(Message::Ping(bytes)) => {
                    self.socket.send(Message::Pong(bytes)).await.map_err(|_| {
                        ConnectorError::Backend(
                            "The authenticated websocket was interrupted.".into(),
                        )
                    })?
                }
                Ok(Message::Pong(_) | Message::Frame(_)) => {}
                Err(
                    tokio_tungstenite::tungstenite::Error::ConnectionClosed
                    | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
                ) => return Ok(None),
                Err(_) => return Err(ConnectorError::Backend(
                    "The authenticated websocket was interrupted or exceeded its message limit."
                        .into(),
                )),
            }
        }
    }

    /// Send one bounded application message; callers control backpressure by awaiting it.
    pub async fn send(&mut self, message: SocketMessage) -> ConnectorResult<()> {
        let (len, message) = match message {
            SocketMessage::Text(text) => (text.len(), Message::Text(text.into())),
            SocketMessage::Binary(bytes) => (bytes.len(), Message::Binary(bytes.into())),
        };
        if len > MAX_SOCKET_MESSAGE_BYTES {
            return Err(ConnectorError::Invalid(
                "Websocket input exceeds its message limit.".into(),
            ));
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), self.socket.send(message))
            .await
            .map_err(|_| {
                ConnectorError::Backend("The authenticated websocket write timed out.".into())
            })?
            .map_err(|_| {
                ConnectorError::Backend("The authenticated websocket was interrupted.".into())
            })
    }

    /// Close without forwarding executor-selected close codes or text to another connection.
    pub async fn close(&mut self) {
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(3), self.socket.close(None)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::authentication::{Request, RequestAuthentication};
    use super::*;
    use std::sync::Arc;

    struct Authentication;
    impl RequestAuthentication for Authentication {
        fn authorization(&self, request: Request<'_>) -> ConnectorResult<String> {
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, "/socket?window=1");
            assert_eq!(request.body, Some(&[][..]));
            Ok("Bearer synthetic-request-credential".into())
        }
    }

    fn client(port: u16) -> APIConnector {
        APIConnector::new(
            Some("fixture"),
            meerschaum_core::Map::from_iter([
                ("host".into(), "127.0.0.1".into()),
                ("port".into(), meerschaum_core::Value::Int(port.into())),
                ("username".into(), "unused".into()),
                ("password".into(), "must-not-login".into()),
            ]),
        )
        .unwrap()
        .with_request_authentication(Arc::new(Authentication), 2 * MAX_SOCKET_MESSAGE_BYTES)
        .unwrap()
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)] // Tungstenite's handshake callback fixes this error type.
    async fn handshake_and_bounded_data_use_only_the_request_credential() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().to_string(), "/socket?window=1");
                    assert_eq!(
                        request.headers()["authorization"],
                        "Bearer synthetic-request-credential"
                    );
                    assert!(!request.headers().contains_key("cookie"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            socket
                .send(Message::Text("terminal output".into()))
                .await
                .unwrap();
            assert_eq!(
                socket.next().await.unwrap().unwrap(),
                Message::Binary(vec![1, 2, 3].into())
            );
            socket.close(None).await.unwrap();
        });
        let mut socket = client(port)
            .open_authenticated_socket("/socket?window=1")
            .await
            .unwrap();
        assert_eq!(
            socket.next().await.unwrap(),
            Some(SocketMessage::Text("terminal output".into()))
        );
        assert!(
            socket
                .send(SocketMessage::Binary(vec![0; MAX_SOCKET_MESSAGE_BYTES + 1]))
                .await
                .is_err()
        );
        socket
            .send(SocketMessage::Binary(vec![1, 2, 3]))
            .await
            .unwrap();
        assert!(socket.next().await.unwrap().is_none());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_peer_frames_and_unsafe_paths_are_refused() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut unsigned = client(port);
        unsigned.request_authentication = None;
        assert!(
            unsigned
                .open_authenticated_socket("/socket?window=1")
                .await
                .is_err()
        );
        for path in [
            "//another-host/socket",
            "/\\another-host/socket",
            "https://another-host",
            "/socket#fragment",
        ] {
            assert!(client(port).open_authenticated_socket(path).await.is_err());
        }
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = socket
                .send(Message::Binary(
                    vec![0; MAX_SOCKET_MESSAGE_BYTES + 1].into(),
                ))
                .await;
        });
        let mut socket = client(port)
            .open_authenticated_socket("/socket?window=1")
            .await
            .unwrap();
        assert!(socket.next().await.is_err());
        peer.await.unwrap();
    }
}
