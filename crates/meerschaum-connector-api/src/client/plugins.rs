//! Plugin routes (`meerschaum/connectors/api/_plugins.py`). Installing a downloaded archive
//! stays in Python (plugins are Python, §6.1); the client uploads and downloads archives.

use meerschaum_core::{Map, SuccessTuple, Value};

use crate::pipe_ops;
use crate::plugin_artifacts::{HOST_TARGET, TARGET_HEADER, valid_source_digest, valid_target};
use crate::routes::{self, plugin_r_url};
use crate::traits::{ConnectorError, ConnectorResult, PluginRecord};
use crate::wire::QueryValue;

use super::pipes::py_json;
use super::{APIConnector, Body, RequestOptions};

fn parse_tuple_or_text(text: String) -> SuccessTuple {
    match serde_json::from_str::<serde_json::Value>(&text) {
        Ok(serde_json::Value::Array(a)) if a.len() == 2 => SuccessTuple {
            success: a[0].as_bool().unwrap_or(false),
            message: a[1].as_str().map(String::from).unwrap_or_default(),
        },
        _ => SuccessTuple::fail(text),
    }
}

impl APIConnector {
    /// `register_plugin(plugin)`: `POST /plugins/{name}?version=&attributes=` with the archive as
    /// the multipart file `archive` (`archive_name` is the archive's file name, e.g.
    /// `{name}.tar.gz`).
    pub async fn register_plugin(
        &self,
        plugin: &PluginRecord,
        archive: Vec<u8>,
        archive_name: &str,
    ) -> SuccessTuple {
        let query = vec![
            (
                "version".to_string(),
                QueryValue::opt(plugin.version.clone()),
            ),
            (
                "attributes".to_string(),
                QueryValue::One(py_json(&Value::Map(plugin.attributes.clone()))),
            ),
        ];
        self.upload_plugin_archive(
            &plugin.name,
            &plugin_r_url(&plugin.name),
            query,
            archive,
            archive_name,
        )
        .await
    }

    /// Attach a binary-only plugin archive to the named, exact source release.
    pub async fn register_plugin_artifact(
        &self,
        name: &str,
        version: &str,
        source_sha256: &str,
        target: &str,
        archive: Vec<u8>,
    ) -> SuccessTuple {
        if !valid_target(target) || !valid_source_digest(source_sha256) || version.is_empty() {
            return SuccessTuple::fail(
                "A native plugin artifact needs a Rust target, version and lowercase source SHA-256.",
            );
        }
        let url = format!("{}/artifacts/{target}", plugin_r_url(name));
        let query = vec![
            ("version".into(), QueryValue::One(version.into())),
            (
                "source_sha256".into(),
                QueryValue::One(source_sha256.into()),
            ),
        ];
        self.upload_plugin_archive(name, &url, query, archive, &format!("{name}.tar.gz"))
            .await
    }

    async fn upload_plugin_archive(
        &self,
        name: &str,
        url: &str,
        query: Vec<(String, QueryValue)>,
        archive: Vec<u8>,
        archive_name: &str,
    ) -> SuccessTuple {
        let r = self
            .post(
                url,
                RequestOptions {
                    query,
                    body: Body::File {
                        field: "archive".into(),
                        file_name: archive_name.into(),
                        data: archive,
                    },
                    ..Default::default()
                },
            )
            .await;
        match r {
            Ok(r) => parse_tuple_or_text(r.text()),
            Err(_) => SuccessTuple::fail(format!("Failed to register plugin '{name}'.")),
        }
    }

    /// Legacy servers ignore the target header. Confined callers always request canonical source.
    pub fn plugin_install_options() -> RequestOptions {
        let mut options = RequestOptions::default();
        if meerschaum_core::team_scope::sandbox::mode_from_env().is_none()
            && matches!(
                meerschaum_core::team_scope::source_namespace::launch(),
                Ok(None)
            )
            && valid_target(HOST_TARGET)
        {
            options
                .headers
                .push((TARGET_HEADER.into(), HOST_TARGET.into()));
        }
        options
    }

    /// Download a plugin's archive (`GET /plugins/{name}`), the first half of
    /// `install_plugin()`. A JSON body instead of an archive is returned as `Err`.
    pub async fn download_plugin(&self, name: &str) -> ConnectorResult<Vec<u8>> {
        self.download_plugin_with(name, RequestOptions::default())
            .await
    }

    /// Download the archive for an installation using the same error contract as a plain download.
    pub async fn download_installable_plugin(&self, name: &str) -> ConnectorResult<Vec<u8>> {
        self.download_plugin_with(name, Self::plugin_install_options())
            .await
    }

    async fn download_plugin_with(
        &self,
        name: &str,
        options: RequestOptions,
    ) -> ConnectorResult<Vec<u8>> {
        let r = self.get(&plugin_r_url(name), options).await?;
        let is_text = r.content_type.contains("json");
        if is_text || !r.ok() {
            let fail = format!("Failed to download binary for plugin '{name}'.");
            return Err(match r.json() {
                Ok(serde_json::Value::Array(a)) if a.len() == 2 => {
                    ConnectorError::Detail(a[1].as_str().map(String::from).unwrap_or(fail))
                }
                _ => ConnectorError::Detail(fail),
            });
        }
        Ok(r.bytes)
    }

    /// `get_plugins(user_id, search_term)`: `GET /plugins` (`[]` on errors).
    pub async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        let opts = RequestOptions::query(vec![
            (
                "user_id",
                user_id
                    .filter(|v| !v.is_null())
                    .map(|v| QueryValue::One(meerschaum_core::pyfmt::py_str(v)))
                    .unwrap_or(QueryValue::None),
            ),
            (
                "search_term",
                QueryValue::opt(search_term.map(String::from)),
            ),
        ]);
        let r = self.get(routes::PLUGINS, opts).await?;
        if !r.ok() {
            return Ok(Vec::new());
        }
        match r.json()? {
            serde_json::Value::Array(a) => Ok(a
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(String::from)
                        .unwrap_or_else(|| v.to_string())
                })
                .collect()),
            _ => Err(ConnectorError::Invalid(r.text())),
        }
    }

    /// `get_plugin_attributes(plugin)`: `GET /plugins/{name}/attributes`.
    pub async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        let r = self
            .get(
                &format!("{}/attributes", plugin_r_url(&plugin.name)),
                RequestOptions::default(),
            )
            .await?;
        let mut attributes = r.json()?;
        if let serde_json::Value::String(s) = &attributes
            && s.starts_with('{')
            && let Ok(parsed) = serde_json::from_str(s)
        {
            attributes = parsed;
        }
        match pipe_ops::json_to_core(&attributes) {
            Value::Map(m) => {
                if !r.ok() && m.contains_key("detail") {
                    super::warnings::warn(&meerschaum_core::pyfmt::py_str(&m["detail"]));
                    return Ok(Map::new());
                }
                Ok(m)
            }
            _ => Err(ConnectorError::Invalid(r.text())),
        }
    }

    /// `get_plugin_version(plugin)`: `GET /plugins/{name}/version` (without a token for the
    /// `api:mrsm` repository).
    pub async fn get_plugin_version(
        &self,
        plugin: &PluginRecord,
    ) -> ConnectorResult<Option<String>> {
        let opts = RequestOptions {
            use_token: self.label != "mrsm",
            ..Default::default()
        };
        let r = self
            .get(&format!("{}/version", plugin_r_url(&plugin.name)), opts)
            .await?;
        if !r.ok() {
            return Ok(None);
        }
        Ok(match r.json() {
            Ok(serde_json::Value::String(s)) if !s.is_empty() => Some(s),
            Ok(serde_json::Value::Null) | Ok(serde_json::Value::String(_)) => None,
            Ok(other) => Some(other.to_string()),
            Err(e) => {
                super::warnings::warn_fmt!("Cannot parse version for {plugin}:\n{e}");
                None
            }
        })
    }

    /// `delete_plugin(plugin)`: `DELETE /plugins/{name}`.
    pub async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        match self
            .delete(&plugin_r_url(&plugin.name), RequestOptions::default())
            .await
        {
            Ok(r) => parse_tuple_or_text(r.text()),
            Err(_) => SuccessTuple::fail(format!("Failed to delete plugin '{plugin}'.")),
        }
    }
}
