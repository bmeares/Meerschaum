//! Main-only description of a configured API transport.
//!
//! A subsequent trusted native server constructor must prove this transport addresses its
//! exact Main instance. No URL, label, credential or client callback creates that binding.

use meerschaum_connector::api_target::{NativeApiTarget, ServiceCredential};

use super::APIConnector;

impl APIConnector {
    pub(super) fn configured_native_target(&self) -> Option<NativeApiTarget> {
        // Even an execution alias whose displayed type is `api` is a caller credential,
        // never the administrator-configured service principal for a self-API binding.
        if self.logical_identity.is_some() || self.request_authentication.is_some() {
            return None;
        }
        let origin = reqwest::Url::parse(&self.url).ok()?;
        if !matches!(origin.scheme(), "http" | "https")
            || origin.host_str().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || (origin.scheme() == "https" && !self.verify)
        {
            return None;
        }
        let credential = match self.login_scheme() {
            "password" => match (self.attr("username"), self.attr("password")) {
                (Some(username), Some(password)) if !username.is_empty() => {
                    ServiceCredential::Password { username, password }
                }
                (None, None) => ServiceCredential::None,
                _ => return None,
            },
            "api_key" => ServiceCredential::ApiKey {
                value: self.attr("api_key").filter(|value| !value.is_empty())?,
            },
            "client_credentials" => ServiceCredential::Client {
                id: self.attr("client_id").filter(|value| !value.is_empty())?,
                secret: self
                    .attr("client_secret")
                    .filter(|value| !value.is_empty())?,
            },
            _ => return None,
        };
        NativeApiTarget::configured(
            format!("api:{}", self.label),
            origin.origin().ascii_serialization(),
            self.instance_keys(),
            credential,
            &self.attributes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::Value;

    fn client() -> APIConnector {
        APIConnector::new(
            Some("selected"),
            [
                ("host".into(), Value::Str("main.invalid".into())),
                ("port".into(), Value::Int(80)),
                ("instance_keys".into(), Value::Str("sql:main".into())),
                ("username".into(), Value::Str("service".into())),
                ("password".into(), Value::Str("synthetic-service".into())),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap()
    }

    #[test]
    fn configured_target_normalizes_origin_and_hides_credentials() {
        let client = client();
        let target = client.configured_native_target().unwrap();
        assert_eq!(target.origin, "http://main.invalid");
        assert_eq!(target.instance.as_deref(), Some("sql:main"));
        assert_eq!(target.logical, "api:selected");
        assert!(!format!("{target:?}").contains("synthetic-service"));
        assert!(!format!("{:?}", target.credential).contains("synthetic-service"));
    }

    #[test]
    fn logical_callback_identity_never_becomes_a_service_target() {
        let mut client = client();
        client.logical_identity = Some(("api".into(), "selected".into()));
        assert!(client.configured_native_target().is_none());
    }

    #[test]
    fn rotating_request_credentials_never_become_a_service_target() {
        let client = client()
            .with_request_authentication(
                std::sync::Arc::new(super::super::authentication::CredentialFile::new(
                    std::path::PathBuf::from("/unavailable-fixture-callback-credential"),
                )),
                1024,
            )
            .unwrap();
        assert!(client.configured_native_target().is_none());
    }

    #[test]
    fn unverified_tls_and_non_origin_targets_cannot_be_native_bindings() {
        let mut client = client();
        client.url = "https://main.invalid".into();
        client.verify = false;
        assert!(client.configured_native_target().is_none());
        client.verify = true;
        for url in [
            "https://main.invalid/foreign-api",
            "https://user:password@main.invalid",
            "https://main.invalid?instance_keys=sql:other",
            "https://main.invalid/#selected",
        ] {
            client.url = url.into();
            assert!(client.configured_native_target().is_none());
        }
    }

    #[test]
    fn changed_configured_credential_changes_the_private_definition_binding() {
        let mut client = client();
        let original = client.configured_native_target().unwrap();
        client.attributes.insert(
            "password".into(),
            Value::Str("synthetic-replacement-service".into()),
        );
        let replacement = client.configured_native_target().unwrap();
        assert_ne!(original.definition_sha256, replacement.definition_sha256);
    }
}
