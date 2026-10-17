//! Token routes (`meerschaum/connectors/api/_tokens.py`).

use chrono::{DateTime, Utc};
use meerschaum_core::users::Token;
use meerschaum_core::{SuccessTuple, Value};
use meerschaum_kernel::value::DateTimeValue;
use uuid::Uuid;

use crate::models::{GetTokenResponse, TokenModel};
use crate::routes;
use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::QueryValue;

use super::pipes::py_json;
use super::{APIConnector, Body, RequestOptions};

/// `dt.isoformat()` for an aware UTC timestamp.
pub fn utc_isoformat(dt: &DateTime<Utc>) -> String {
    DateTimeValue::utc(dt.naive_utc()).isoformat()
}

fn token_id_str(id: Option<Uuid>) -> String {
    id.map(|u| u.to_string()).unwrap_or_else(|| "None".into())
}

fn parse_tuple(text: &str) -> Option<SuccessTuple> {
    let j: serde_json::Value = serde_json::from_str(text).ok()?;
    let a = j.as_array()?;
    Some(SuccessTuple {
        success: a.first()?.as_bool()?,
        message: a.get(1)?.as_str().map(String::from).unwrap_or_default(),
    })
}

impl APIConnector {
    /// `register_token(token)`: `POST /tokens/register`, then take the label, secret, ID and
    /// expiration from the response.
    ///
    /// PARITY: the JSON body is sent as `data=` without a `Content-Type`, which recent FastAPI
    /// versions reject with a 500 (the v4 server's validation handler then fails to serialize
    /// the raw bytes). The Rust server should parse the body as JSON regardless.
    pub async fn register_token(&self, token: &mut Token) -> SuccessTuple {
        let mut body = meerschaum_core::Map::new();
        body.insert("label".into(), token.label.clone().into());
        body.insert(
            "scopes".into(),
            Value::List(token.scopes.iter().map(|s| Value::Str(s.clone())).collect()),
        );
        body.insert(
            "expiration".into(),
            token
                .expiration
                .as_ref()
                .map(|e| Value::Str(utc_isoformat(e)))
                .unwrap_or(Value::Null),
        );
        let r = match self
            .post(
                &format!("{}/register", routes::TOKENS),
                RequestOptions {
                    body: Body::Text(py_json(&Value::Map(body))),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(format!("Failed to register token:\n{e}")),
        };
        if !r.ok() {
            return SuccessTuple::fail(format!("Failed to register token:\n{}", r.text()));
        }
        let Ok(data) = r.json() else {
            return SuccessTuple::fail(format!("Failed to register token:\n{}", r.text()));
        };
        token.label = data["label"].as_str().map(String::from);
        token.secret = data["secret"].as_str().map(String::from);
        token.id = data["id"].as_str().and_then(|s| Uuid::parse_str(s).ok());
        if let Some(exp) = data["expiration"].as_str() {
            if let Some(d) = meerschaum_kernel::value::parse_datetime_value(exp) {
                token.expiration = Some(DateTime::<Utc>::from_naive_utc_and_offset(d.utc, Utc));
            }
        }
        SuccessTuple::ok(format!(
            "Registered token '{}'.",
            token.label.clone().unwrap_or_default()
        ))
    }

    /// `get_token_model(token_id)`: `GET /tokens/{id}` (`None` on errors).
    pub async fn get_token_model(
        &self,
        token_id: Option<Uuid>,
    ) -> ConnectorResult<Option<TokenModel>> {
        let r = self
            .get(
                &format!("{}/{}", routes::TOKENS, token_id_str(token_id)),
                RequestOptions::default(),
            )
            .await?;
        if !r.ok() {
            return Ok(None);
        }
        let model: GetTokenResponse = serde_json::from_value(r.json()?)?;
        Ok(Some(model.into()))
    }

    /// `get_tokens(labels)`: `GET /tokens?labels=...` (one `labels` pair per label).
    ///
    /// PARITY: v4 builds a comma-joined `labels` param and then sends the list instead, so the
    /// label list goes out as repeated keys (`_tokens.py`, `get_tokens`).
    pub async fn get_tokens(&self, labels: Option<&[String]>) -> ConnectorResult<Vec<TokenModel>> {
        let labels_q = match labels {
            Some(l) => QueryValue::Many(l.to_vec()),
            None => QueryValue::None,
        };
        let r = self
            .get(
                routes::TOKENS,
                RequestOptions::query(vec![("labels", labels_q)]),
            )
            .await?;
        if !r.ok() {
            super::warnings::warn_fmt!("Could not get tokens from '{self}':\n{}", r.text());
            return Ok(Vec::new());
        }
        let models: Vec<GetTokenResponse> = serde_json::from_value(r.json()?)?;
        Ok(models.into_iter().map(TokenModel::from).collect())
    }

    /// `edit_token(token)`: `POST /tokens/{id}/edit` with the token's fields.
    pub async fn edit_token(&self, token: &Token) -> SuccessTuple {
        let iso = |d: &Option<DateTime<Utc>>| {
            d.as_ref()
                .map(|d| Value::Str(utc_isoformat(d)))
                .unwrap_or(Value::Null)
        };
        let mut body = meerschaum_core::Map::new();
        body.insert("creation".into(), iso(&token.creation));
        body.insert("expiration".into(), iso(&token.expiration));
        body.insert("label".into(), token.label.clone().into());
        body.insert("is_valid".into(), Value::Bool(token.is_valid));
        body.insert(
            "scopes".into(),
            Value::List(token.scopes.iter().map(|s| Value::Str(s.clone())).collect()),
        );
        let r = match self
            .post(
                &format!("{}/{}/edit", routes::TOKENS, token_id_str(token.id)),
                RequestOptions {
                    body: Body::Json(py_json(&Value::Map(body))),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(format!("Failed to edit token:\n{e}")),
        };
        if !r.ok() {
            return SuccessTuple::fail(format!("Failed to edit token:\n{}", r.text()));
        }
        parse_tuple(&r.text()).unwrap_or_else(|| SuccessTuple::fail(r.text()))
    }

    /// `invalidate_token(token)`: `POST /tokens/{id}/invalidate`.
    pub async fn invalidate_token(&self, token: &Token) -> SuccessTuple {
        let r = match self
            .post(
                &format!("{}/{}/invalidate", routes::TOKENS, token_id_str(token.id)),
                RequestOptions::default(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(format!("Failed to invalidate token:\n{e}")),
        };
        if !r.ok() {
            return SuccessTuple::fail(format!("Failed to invalidate token:\n{}", r.text()));
        }
        parse_tuple(&r.text()).unwrap_or_else(|| SuccessTuple::fail(r.text()))
    }

    /// `get_token_scopes(token_id)`.
    ///
    /// PARITY: v4 reads `model.scopes` and then `getattr(<that list>, 'scopes', [])`, so it
    /// returns `[]` for an existing token and raises for a missing one (`_tokens.py:105`).
    pub async fn get_token_scopes(&self, token_id: Option<Uuid>) -> ConnectorResult<Vec<String>> {
        match self.get_token_model(token_id).await? {
            Some(_) => Ok(Vec::new()),
            None => Err(ConnectorError::Invalid(
                "'NoneType' object has no attribute 'scopes'".into(),
            )),
        }
    }

    /// `token_exists(token_id)`: the token model exists and has a creation timestamp.
    pub async fn token_exists(&self, token_id: Option<Uuid>) -> ConnectorResult<bool> {
        Ok(self
            .get_token_model(token_id)
            .await?
            .is_some_and(|m| m.creation.is_some()))
    }

    /// `delete_token(token)`: `DELETE /tokens/{id}`.
    pub async fn delete_token(&self, token: &Token) -> SuccessTuple {
        let r = match self
            .delete(
                &format!("{}/{}", routes::TOKENS, token_id_str(token.id)),
                RequestOptions::default(),
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(format!("Failed to delete token:\n{e}")),
        };
        if !r.ok() {
            return SuccessTuple::fail(format!("Failed to delete token:\n{}", r.text()));
        }
        parse_tuple(&r.text()).unwrap_or_else(|| SuccessTuple::fail(r.text()))
    }
}
