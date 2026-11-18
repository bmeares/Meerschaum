//! Public console page and websocket against the installed native Main and its selected executor.
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_web::sessions::{COOKIE, InstanceSessions, SessionStore};
use serde_json::{Value as Json, json};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Placement {
    Host,
    Remote,
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn frame(socket: &mut Socket) -> Json {
    loop {
        match socket
            .next()
            .await
            .expect("The console websocket ended")
            .unwrap()
        {
            Message::Text(raw) => {
                let value: Json = serde_json::from_str(&raw).unwrap();
                assert_ne!(value[0], "disconnect", "The native shell disconnected");
                return value;
            }
            Message::Close(_) => {
                panic!("The console closed the terminal before the command completed")
            }
            _ => continue,
        }
    }
}

async fn show_version(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(90), async {
        let request = Uuid::new_v4().simple().to_string();
        loop {
            socket
                .send(Message::Text(
                    json!(["shell_state", request]).to_string().into(),
                ))
                .await
                .unwrap();
            loop {
                let value = frame(socket).await;
                if value[0] == "shell_state" && value[1] == request {
                    if value[2]["idle"] == true && value[2]["empty"] == true {
                        return;
                    }
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("The native shell did not become ready for input");
    socket
        .send(Message::Text(
            json!(["stdin", "show version\r"]).to_string().into(),
        ))
        .await
        .unwrap();
    let expected = format!("Meerschaum v{}", env!("CARGO_PKG_VERSION"));
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut output = String::new();
        while !output.contains(&expected) {
            let value = frame(socket).await;
            if value[0] == "stdout" {
                output.push_str(value[1].as_str().unwrap());
                assert!(
                    output.len() < 512 * 1024,
                    "The command exceeded its output budget"
                );
            }
        }
    })
    .await
    .expect("The native show version command did not return the candidate version");
}

pub(super) async fn round_trip(
    placement: Placement,
    client: &reqwest::Client,
    origin: &str,
    root: &Path,
    instance: Arc<SqlConnector>,
) {
    let (user, team, catalog) = match placement {
        Placement::Host => (
            "host_operator",
            "HostFixture",
            "executors/host-terminals/catalog.json",
        ),
        Placement::Remote => ("alice", "ApiFixture", "executors/terminals.json"),
    };
    // Establish the same password-bound session as the console after its successful login.
    // Only the login result from this owned Main supplies the session's credential fingerprint.
    let token = super::login(client, origin, user).await;
    let payload = URL_SAFE_NO_PAD
        .decode(token.split('.').nth(1).unwrap())
        .unwrap();
    let claims: Json = serde_json::from_slice(&payload).unwrap();
    let credential = claims["mrsm_credential"].as_str().unwrap();
    let id = Uuid::new_v4().to_string();
    let sessions = InstanceSessions::new(instance);
    sessions
        .set(&id, json!({"username":user,"credential":credential}), 600)
        .await
        .unwrap();
    let (status, capability) = super::response(
        client
            .get(format!("{origin}/execute/capabilities"))
            .header("cookie", format!("{COOKIE}={id}")),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(capability["source"]["primary_team"], team);
    assert_eq!(capability["source"]["isolated"], true);
    assert_eq!(capability["source"]["runs_code"], false);
    assert_eq!(capability["permissions"]["webterm_use"], true);
    assert_eq!(capability["capabilities"]["api_terminals"], true);
    let page = client
        .get(format!("{origin}/webterm/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
    assert!(
        page.text()
            .await
            .unwrap()
            .contains("/webterm/assets/xterm.js")
    );
    let url = format!(
        "ws://{}/websocket/{id}",
        origin.strip_prefix("http://").unwrap()
    );
    let mut identities = Vec::new();
    for _ in 0..2 {
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        show_version(&mut socket).await;
        let records: Vec<Json> =
            serde_json::from_slice(&std::fs::read(root.join(catalog)).unwrap()).unwrap();
        let mut owned = records
            .iter()
            .filter(|record| record["grant"]["owner"] == user);
        let record = owned.next().unwrap();
        assert!(owned.next().is_none());
        assert_eq!(record["team_id"], capability["source"]["team_id"]);
        assert!(
            record["credential"].as_str() == Some(credential),
            "The recorded owner changed"
        );
        if placement == Placement::Remote {
            assert_eq!(record["profile"], "api");
        }
        let launch = Uuid::parse_str(record["grant"]["id"].as_str().unwrap()).unwrap();
        let callback = Uuid::parse_str(record["callback"].as_str().unwrap()).unwrap();
        identities.push((launch, callback));
        socket.close(None).await.unwrap();
    }
    assert_eq!(
        identities[0], identities[1],
        "The reconnect replaced the native terminal"
    );
    sessions.delete(&id).await.unwrap();
}
