//! Bounded native-source broker models, shared without the HTTP client feature.
//!
//! The server loads the registered pipe and authorizes its stored definition. A request cannot
//! supply SQL, connector credentials, templates, hooks, or plugin parameters. A bounded result
//! must explicitly prove completeness; partial data is never treated as a successful fetch.

use indexmap::IndexMap;
use meerschaum_core::{Map, Pipe};
use serde::{Deserialize, Serialize};

use crate::{DataQuery, DateBound};

/// Registered pipe route, intentionally distinct from unrestricted SQL/action dispatch.
pub fn path(pipe: &PipeKeys) -> String {
    format!(
        "{}/fetch",
        crate::routes::pipe_r_url(
            &pipe.connector_keys,
            &pipe.metric_key,
            pipe.location_key.as_deref()
        )
    )
}
/// Current wire version.
pub const VERSION: u32 = 1;
/// Maximum request bytes, including filters.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum response bytes, enforced before decoding by the authenticated HTTP client.
pub const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
/// Maximum rows per bounded response. Larger sources require a real paging protocol.
pub const MAX_ROWS: usize = 100_000;
/// Maximum columns, including projected and dtype names.
pub const MAX_COLUMNS: usize = 1024;

/// A bound retains integer versus datetime identity (a timestamp string is never SQL text).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Bound {
    /// Integer axis.
    Integer(i64),
    /// ISO timestamp.
    Datetime(String),
}

impl Bound {
    pub(crate) fn from_bound(bound: &DateBound) -> Self {
        match bound {
            DateBound::Int(value) => Self::Integer(*value),
            DateBound::Datetime(_) => Self::Datetime(bound.isoformat()),
        }
    }

    pub(crate) fn to_bound(&self) -> Result<DateBound, String> {
        match self {
            Self::Integer(value) => Ok(DateBound::Int(*value)),
            Self::Datetime(value) if value.len() <= 128 => match DateBound::parse(value) {
                Some(bound @ DateBound::Datetime(_)) => Ok(bound),
                _ => Err("Invalid source fetch datetime bound.".into()),
            },
            _ => Err("Invalid source fetch bound.".into()),
        }
    }
}

/// Canonical registered pipe identity, not a client-supplied definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipeKeys {
    /// Exact source connector keys.
    pub connector_keys: String,
    /// Registered metric key.
    pub metric_key: String,
    /// Registered location key.
    pub location_key: Option<String>,
}

/// A bounded read over a registered native SQL source. Server permission and SQL guards remain
/// mandatory, even if the definition was previously stored by this caller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Wire version.
    pub version: u32,
    /// Exact logical source alias; must equal pipe.connector_keys.
    pub source_keys: String,
    /// Logical instance containing the registered pipe.
    pub instance_keys: String,
    /// The canonical pipe to load and authorize.
    pub pipe: PipeKeys,
    /// Lower axis bound, inclusive.
    pub begin: Option<Bound>,
    /// Upper axis bound, exclusive.
    pub end: Option<Bound>,
    /// Ordinary structured row filters.
    pub params: Option<Map>,
    /// Only return these columns.
    pub select_columns: Option<Vec<String>>,
    /// Exclude these columns.
    pub omit_columns: Option<Vec<String>>,
    /// Explicit query result limit, including zero. This is an intentional SQL/read limit,
    /// distinct from the transport cap; a larger requested result may need future paging.
    pub limit: Option<usize>,
    /// Positive transport cap. Server must detect and mark any truncated requested result.
    pub row_limit: usize,
    /// Axis ordering, asc or desc.
    pub order: Option<String>,
}

fn key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}

impl Request {
    /// Form a request without reading any local pipe parameters.
    pub fn for_pipe(source_keys: &str, pipe: &Pipe, query: &DataQuery) -> Result<Self, String> {
        if query.remote || query.as_chunks {
            return Err("This source broker requires an ordinary bounded fetch query.".into());
        }
        let limit = query
            .limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| "Invalid source fetch row limit.")?;
        let request = Self {
            version: VERSION,
            source_keys: source_keys.into(),
            instance_keys: pipe.instance_keys().into(),
            pipe: PipeKeys {
                connector_keys: pipe.connector_keys().into(),
                metric_key: pipe.metric_key().into(),
                location_key: pipe.location_key().map(str::to_string),
            },
            begin: query.begin.as_ref().map(Bound::from_bound),
            end: query.end.as_ref().map(Bound::from_bound),
            params: query.params.clone(),
            select_columns: query.select_columns.clone(),
            omit_columns: query.omit_columns.clone(),
            limit,
            row_limit: MAX_ROWS,
            order: query.order.clone(),
        };
        request.validate()?;
        Ok(request)
    }

    /// Structural bounds only; this grants no database or pipe authority.
    pub fn validate(&self) -> Result<(), String> {
        if self.version != VERSION
            || self.source_keys != self.pipe.connector_keys
            || !self.source_keys.starts_with("sql:")
            || !key(&self.source_keys)
            || !key(&self.instance_keys)
            || !key(&self.pipe.metric_key)
            || self.pipe.location_key.as_deref().is_some_and(|s| !key(s))
            || self.row_limit == 0
            || self.row_limit > MAX_ROWS
            || self
                .limit
                .is_some_and(|limit| i64::try_from(limit).is_err())
            || self
                .order
                .as_deref()
                .is_some_and(|s| !matches!(s, "asc" | "desc"))
        {
            return Err("Invalid canonical source fetch request.".into());
        }
        for columns in [&self.select_columns, &self.omit_columns]
            .into_iter()
            .flatten()
        {
            if columns.len() > MAX_COLUMNS || columns.iter().any(|name| !key(name)) {
                return Err("Invalid source fetch column selection.".into());
            }
        }
        self.begin.as_ref().map(Bound::to_bound).transpose()?;
        self.end.as_ref().map(Bound::to_bound).transpose()?;
        if serde_json::to_vec(self)
            .map_err(|_| "Invalid source fetch request.")?
            .len()
            > MAX_REQUEST_BYTES
        {
            return Err("The source fetch request exceeds its byte limit.".into());
        }
        Ok(())
    }

    /// Native backend filters after route-level fresh source/SQL authorization.
    pub fn data_query(&self) -> Result<DataQuery, String> {
        self.validate()?;
        Ok(DataQuery {
            begin: self.begin.as_ref().map(Bound::to_bound).transpose()?,
            end: self.end.as_ref().map(Bound::to_bound).transpose()?,
            params: self.params.clone(),
            select_columns: self.select_columns.clone(),
            omit_columns: self.omit_columns.clone(),
            limit: self.limit.map(|limit| limit as i64),
            order: self.order.clone(),
            ..Default::default()
        })
    }
}

/// One explicitly complete bounded source result. No executable object or remote path is read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    /// Wire version.
    pub version: u32,
    /// Typed column hints for ordinary JSON records.
    pub dtypes: IndexMap<String, String>,
    /// Ordinary JSON records, never pickle or plugin objects.
    pub rows: Vec<Map>,
    /// False when the requested query (including its explicit limit) has another row beyond
    /// row_limit. Client must refuse it; an intentional query limit is not truncation.
    pub complete: bool,
}

impl Response {
    /// Require explicit completeness and bounded data before allocating a kernel frame.
    pub fn validate(&self, request: &Request) -> Result<(), String> {
        request.validate()?;
        if self.version != VERSION || !self.complete {
            return Err(
                "The source broker did not prove a complete result; fetch a smaller window.".into(),
            );
        }
        if self.rows.len() > request.row_limit
            || self.dtypes.len() > MAX_COLUMNS
            || request.limit.is_some_and(|limit| self.rows.len() > limit)
            || self
                .dtypes
                .iter()
                .any(|(name, dtype)| !key(name) || !key(dtype))
            || self
                .rows
                .iter()
                .any(|row| row.len() > MAX_COLUMNS || row.keys().any(|name| !key(name)))
        {
            return Err("The source broker exceeded its data bounds.".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_cannot_carry_definitions_and_preserves_typed_bounds() {
        let pipe = Pipe::builder("sql:source", "metric")
            .instance("sql:main")
            .build()
            .unwrap();
        let request = Request::for_pipe(
            "sql:source",
            &pipe,
            &DataQuery {
                begin: Some(DateBound::Int(1)),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(request.data_query().unwrap().begin, Some(DateBound::Int(1)));
        assert_eq!(request.data_query().unwrap().limit, None);
        let limited = Request::for_pipe(
            "sql:source",
            &pipe,
            &DataQuery {
                limit: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(limited.limit, Some(7));
        assert_eq!(limited.row_limit, MAX_ROWS);
        assert_eq!(limited.data_query().unwrap().limit, Some(7));
        let mut encoded = serde_json::to_value(&request).unwrap();
        encoded["definition"] = "SELECT * FROM private".into();
        assert!(serde_json::from_value::<Request>(encoded).is_err());
        let mut wrong = request.clone();
        wrong.source_keys = "sql:other".into();
        assert!(wrong.validate().is_err());
        let response = Response {
            version: VERSION,
            rows: Vec::new(),
            dtypes: IndexMap::new(),
            complete: false,
        };
        assert!(response.validate(&request).is_err());
    }
}

/// Snapshot paging is a distinct protocol: a complete page is not a complete source result.
pub const PAGE_VERSION: u32 = 1;
/// Maximum rows buffered while the native snapshot is encoded.
pub const PAGE_ROWS: usize = 2048;
/// Maximum encoded bytes in one private snapshot page or page response.
pub const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;

/// Exact registered fetch route for the snapshot protocol.
pub fn page_path(pipe: &PipeKeys) -> String {
    format!("{}/pages", path(pipe))
}

/// Opaque position within one main-owned snapshot; this is never authorization by itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    /// Random snapshot ID, with no path or source definition.
    pub id: String,
    /// Next exact page. Replaying or skipping a page is refused.
    pub page: u32,
}

impl Cursor {
    /// Strict identifiers prevent client-selected filenames or unbounded parser input.
    pub fn validate(&self) -> Result<(), String> {
        let id = uuid::Uuid::parse_str(&self.id).map_err(|_| "Invalid source snapshot cursor.")?;
        if id.is_nil() || self.id != id.simple().to_string() || self.page > 1_000_000 {
            return Err("Invalid source snapshot cursor.".into());
        }
        Ok(())
    }
}

/// Open, advance or close a snapshot. Query fields must remain exactly equal on every page.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    /// Snapshot protocol version.
    pub version: u32,
    /// Original canonical source selector and ordinary structured filters.
    pub request: Request,
    /// None opens a snapshot; otherwise it names the next exact page.
    pub cursor: Option<Cursor>,
    /// Release private snapshot state without reading another page.
    #[serde(default)]
    pub close: bool,
}

impl PageRequest {
    /// Structural bounds only. Every page still requires fresh main-API authorization.
    pub fn validate(&self) -> Result<(), String> {
        self.request.validate()?;
        if self.version != PAGE_VERSION || (self.close && self.cursor.is_none()) {
            return Err("Invalid source snapshot request.".into());
        }
        if let Some(cursor) = &self.cursor {
            cursor.validate()?;
        }
        if serde_json::to_vec(self)
            .map_err(|_| "Invalid source snapshot request.")?
            .len()
            > MAX_REQUEST_BYTES
        {
            return Err("The source snapshot request exceeds its byte limit.".into());
        }
        Ok(())
    }
}

/// One bounded page. Only the terminal page proves the end of the complete requested result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageResponse {
    /// Snapshot protocol version.
    pub version: u32,
    /// Complete bounded page data, retaining the existing typed JSON encoding.
    pub data: Response,
    /// Some continues the same snapshot; None means its source result has ended.
    pub cursor: Option<Cursor>,
    /// True only at actual EOF of the original snapshot query.
    pub complete: bool,
}

impl PageResponse {
    /// Reject ambiguous termination, oversized pages and invalid/repeated cursor positions.
    pub fn validate(&self, request: &PageRequest) -> Result<(), String> {
        request.validate()?;
        self.data.validate(&request.request)?;
        if self.version != PAGE_VERSION
            || self.data.rows.len() > PAGE_ROWS
            || self.complete != self.cursor.is_none()
        {
            return Err("Invalid source snapshot response.".into());
        }
        if let Some(next) = &self.cursor {
            next.validate()?;
            match &request.cursor {
                Some(previous) if next.id != previous.id || next.page != previous.page + 1 => {
                    return Err("The source snapshot did not advance exactly one page.".into());
                }
                None if next.page != 1 => {
                    return Err("The source snapshot has an invalid initial position.".into());
                }
                _ => {}
            }
        }
        Ok(())
    }
}
