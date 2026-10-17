//! `/version`, `/chaining`, `/info`, `/healthcheck`, `/id`, `/connectors`
//! (`meerschaum/connectors/api/_misc.py` plus the routes v4 has no client method for).

use indexmap::IndexMap;

use crate::models::{Healthcheck, InstanceInfo, ServerIds};
use crate::routes;
use crate::traits::ConnectorResult;
use crate::wire::QueryValue;

use super::{APIConnector, RequestOptions};

impl APIConnector {
    /// `get_mrsm_version()`: `GET /version/mrsm` without a token (`None` on errors).
    pub async fn get_mrsm_version(&self) -> Option<String> {
        let r = self
            .get(
                &format!("{}/mrsm", routes::VERSION),
                RequestOptions::default().without_token(),
            )
            .await
            .ok()?;
        match r.json().ok()? {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Object(o) if o.contains_key("detail") => None,
            other => Some(other.to_string()),
        }
    }

    /// `GET /version`: the API version.
    pub async fn get_api_version(&self) -> ConnectorResult<String> {
        let j = self
            .get(routes::VERSION, RequestOptions::default())
            .await?
            .json_or_detail()?;
        Ok(j.as_str()
            .map(String::from)
            .unwrap_or_else(|| j.to_string()))
    }

    /// `get_chaining_status()`: `GET /chaining` (`None` on errors).
    pub async fn get_chaining_status(&self) -> Option<bool> {
        let r = self
            .get(routes::CHAINING, RequestOptions::default())
            .await
            .ok()?;
        if !r.ok() {
            return None;
        }
        r.json().ok()?.as_bool()
    }

    /// `GET /info`.
    pub async fn get_info(&self) -> ConnectorResult<InstanceInfo> {
        let opts = RequestOptions::query(vec![(
            "instance_keys",
            QueryValue::opt(self.instance_keys()),
        )]);
        let j = self.get(routes::INFO, opts).await?.json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `GET /healthcheck` (200 or 503 with the same body).
    pub async fn healthcheck(&self) -> ConnectorResult<Healthcheck> {
        let opts = RequestOptions::query(vec![(
            "instance_keys",
            QueryValue::opt(self.instance_keys()),
        )]);
        let j = self.get(routes::HEALTHCHECK, opts).await?.json()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `GET /id` (debug servers only).
    pub async fn get_ids(&self) -> ConnectorResult<ServerIds> {
        let j = self
            .get(routes::ID, RequestOptions::default())
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `GET /connectors`: connector labels by type.
    pub async fn get_connectors(&self) -> ConnectorResult<IndexMap<String, Vec<String>>> {
        let j = self
            .get(routes::CONNECTORS, RequestOptions::default())
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }

    /// `GET /connectors/{type}`: the labels of one connector type.
    pub async fn get_connectors_by_type(&self, typ: &str) -> ConnectorResult<Vec<String>> {
        let j = self
            .get(
                &format!("{}/{typ}", routes::CONNECTORS),
                RequestOptions::default(),
            )
            .await?
            .json_or_detail()?;
        Ok(serde_json::from_value(j)?)
    }
}
