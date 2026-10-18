//! `wss://` for the logs websocket (`monitor_logs`).
//!
//! `tokio-tungstenite` builds its own TLS configuration with `ClientConfig::builder()`, which
//! panics when no process-wide crypto provider is installed and two are compiled in (ring through
//! `sqlx`/Polars, aws-lc-rs through `tiberius`/`reqwest`). The configuration is therefore built
//! here, with an explicit provider and, like `reqwest` 0.13, the operating system's verifier. The
//! connector's `verify: false` attribute (`danger_accept_invalid_certs` for HTTP) accepts any
//! certificate.

use std::sync::Arc;

use rustls::ClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, SignatureScheme};
use rustls_platform_verifier::BuilderVerifierExt;
use tokio_tungstenite::Connector;

/// Whether `url` is a `wss://` URL.
pub(crate) fn is_secure(url: &str) -> bool {
    url.get(..6)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("wss://"))
}

/// The TLS connector for a websocket URL: `None` for `ws://` (plain TCP), a rustls connector for
/// `wss://`.
pub(crate) fn connector_for(url: &str, verify: bool) -> Result<Option<Connector>, String> {
    if !is_secure(url) {
        return Ok(None);
    }
    Ok(Some(Connector::Rustls(Arc::new(client_config(verify)?))))
}

fn client_config(verify: bool) -> Result<ClientConfig, String> {
    let provider = Arc::new(aws_lc_rs::default_provider());
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    let config = if verify {
        builder
            .with_platform_verifier()
            .map_err(|e| e.to_string())?
            .with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate { provider }))
            .with_no_client_auth()
    };
    Ok(config)
}

/// The verifier of `verify: false`: the certificate is not checked, the handshake signatures are
/// accepted (the same trust as `danger_accept_invalid_certs`).
#[derive(Debug)]
struct AcceptAnyCertificate {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_websockets_take_no_tls_connector() {
        for url in ["ws://h:80/logs/j/ws", "WS://h/x", "http://h", ""] {
            assert!(connector_for(url, true).unwrap().is_none(), "{url}");
        }
    }

    #[test]
    fn secure_websockets_take_a_rustls_connector() {
        for (url, verify) in [("wss://h:443/logs/j/ws", true), ("WSS://h/x", false)] {
            assert!(
                matches!(
                    connector_for(url, verify).unwrap(),
                    Some(Connector::Rustls(_))
                ),
                "{url} verify={verify}"
            );
        }
    }
}
