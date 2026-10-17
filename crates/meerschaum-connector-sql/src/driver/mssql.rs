//! MSSQL (`mssql`) over `tiberius` (TDS 7.3, rustls).
//!
//! Connection options follow the ODBC keywords v4 passes through its URI
//! (`TrustServerCertificate`, `Encrypt`, `ApplicationName`, `driver` is ignored). Statements run
//! through `sp_executesql` (bound parameters are `@P1`, `@P2`, ...); transaction control is sent
//! as plain SQL batches so the transaction outlives the RPC call.

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::TryStreamExt;
use rust_decimal::Decimal;
use tiberius::{
    AuthMethod, Client, ColumnData, ColumnType, Config, EncryptionLevel, FromSql, Query, QueryItem,
};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::SqlValue;
use super::batch::{ColumnKind, ColumnPlan};
use super::native::{NativeConn, NativeConnector, NativeEvent, NativePool};
use super::params::SqlParam;
use crate::error::{Result, SqlError};
use crate::uri::ConnectionSpec;

type MssqlClient = Client<Compat<TcpStream>>;

fn db_err(e: tiberius::error::Error) -> SqlError {
    SqlError::Database(e.to_string())
}

fn truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "yes" | "true" | "1" | "mandatory" | "strict" | "on"
    )
}

/// Build the `tiberius` configuration from a connection spec.
pub(crate) fn config(spec: &ConnectionSpec) -> Result<Config> {
    let mut config = Config::new();
    config.host(spec.host.as_deref().unwrap_or("localhost"));
    config.port(spec.port.unwrap_or(1433));
    if let Some(db) = spec.database.as_deref().filter(|d| !d.is_empty()) {
        config.database(db);
    }
    config.authentication(AuthMethod::sql_server(
        spec.username.as_deref().unwrap_or(""),
        spec.password.as_deref().unwrap_or(""),
    ));
    config.application_name("meerschaum");
    // ODBC Driver 18 defaults to `Encrypt=yes`; v4's flavor defaults pass `Encrypt=no`, which
    // still encrypts the login packet (`EncryptionLevel::Off`).
    let mut encryption = EncryptionLevel::Off;
    let mut trust = false;
    let mut pairs = spec.option_pairs();
    pairs.extend(spec.params.iter().map(|(k, v)| (k.clone(), v.clone())));
    for (key, value) in pairs {
        match key.to_ascii_lowercase().as_str() {
            "trustservercertificate" | "trust_server_certificate" => trust = truthy(&value),
            "encrypt" => {
                encryption = match value.trim().to_ascii_lowercase().as_str() {
                    "no" | "false" | "0" | "optional" | "off" => EncryptionLevel::Off,
                    "notsupported" | "disabled" => EncryptionLevel::NotSupported,
                    _ => EncryptionLevel::Required,
                }
            }
            "applicationname" | "app" | "application_name" => config.application_name(&value),
            "instance" | "instancename" => config.instance_name(&value),
            "readonly" | "applicationintent" => {
                config.readonly(value.eq_ignore_ascii_case("readonly") || truthy(&value))
            }
            _ => tracing::debug!("Ignoring MSSQL connection parameter '{key}'."),
        }
    }
    config.encryption(encryption);
    if trust {
        config.trust_cert();
    }
    Ok(config)
}

async fn open(config: Config) -> Result<MssqlClient> {
    let addr = config.get_addr();
    let tcp = TcpStream::connect(&addr)
        .await
        .map_err(|e| SqlError::Database(format!("Could not connect to {addr}: {e}")))?;
    let _ = tcp.set_nodelay(true);
    match Client::connect(config.clone(), tcp.compat_write()).await {
        Ok(c) => Ok(c),
        // Azure SQL may redirect the login to another node.
        Err(tiberius::error::Error::Routing { host, port }) => {
            let mut config = config;
            config.host(&host);
            config.port(port);
            let tcp = TcpStream::connect(config.get_addr())
                .await
                .map_err(|e| SqlError::Database(e.to_string()))?;
            let _ = tcp.set_nodelay(true);
            Client::connect(config, tcp.compat_write())
                .await
                .map_err(db_err)
        }
        Err(e) => Err(db_err(e)),
    }
}

struct MssqlConnector {
    config: Config,
}

#[async_trait]
impl NativeConnector for MssqlConnector {
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        let client = open(self.config.clone()).await?;
        Ok(Box::new(MssqlConn {
            client,
            broken: false,
        }))
    }
}

/// Open a pool, verifying that one connection can be established.
pub(crate) async fn connect(spec: &ConnectionSpec) -> Result<NativePool> {
    let pool = connect_lazy(spec)?;
    drop(pool.acquire().await?);
    Ok(pool)
}

/// Build the pool without connecting.
pub(crate) fn connect_lazy(spec: &ConnectionSpec) -> Result<NativePool> {
    let config = config(spec)?;
    Ok(NativePool::new(
        Box::new(MssqlConnector { config }),
        spec.max_connections(),
    ))
}

/// Convert a driver error, flagging the connection broken on I/O and protocol failures.
fn track<T>(broken: &mut bool, r: std::result::Result<T, tiberius::error::Error>) -> Result<T> {
    r.map_err(|e| {
        if matches!(
            e,
            tiberius::error::Error::Io { .. } | tiberius::error::Error::Protocol(_)
        ) {
            *broken = true;
        }
        db_err(e)
    })
}

struct MssqlConn {
    client: MssqlClient,
    broken: bool,
}

impl MssqlConn {
    fn track<T>(&mut self, r: std::result::Result<T, tiberius::error::Error>) -> Result<T> {
        track(&mut self.broken, r)
    }

    async fn batch(&mut self, sql: &str) -> Result<()> {
        let r = match self.client.simple_query(sql).await {
            Ok(stream) => stream.into_results().await.map(|_| ()),
            Err(e) => Err(e),
        };
        self.track(r)
    }
}

fn bind(q: &mut Query<'_>, p: &SqlParam) {
    match p {
        SqlParam::Null => q.bind(Option::<String>::None),
        SqlParam::Bool(b) => q.bind(*b),
        SqlParam::Int(i) => q.bind(*i),
        SqlParam::Float(f) => q.bind(*f),
        SqlParam::Text(s) => q.bind(s.clone()),
        SqlParam::Bytes(b) => q.bind(b.clone()),
        SqlParam::Json(j) => q.bind(serde_json::to_string(j).unwrap_or_default()),
        SqlParam::Timestamp(t) => q.bind(*t),
        SqlParam::TimestampTz(t) => q.bind(*t),
        SqlParam::Date(d) => q.bind(*d),
    }
}

fn query_for<'a>(sql: &'a str, params: &[SqlParam]) -> Query<'a> {
    if params.iter().any(|p| matches!(p, SqlParam::Null)) {
        let (sql, kept) = inline_nulls(sql, params);
        let mut q = Query::new(sql);
        for p in kept {
            bind(&mut q, p);
        }
        return q;
    }
    let mut q = Query::new(sql);
    for p in params {
        bind(&mut q, p);
    }
    q
}

/// Whether a statement runs as a plain batch: a session-level `SET` (not `UPDATE ... SET`), or
/// a statement that must be the first in its batch.
fn runs_as_batch(sql: &str) -> bool {
    let words: Vec<String> = sql
        .split_whitespace()
        .take(3)
        .map(|w| w.to_ascii_uppercase())
        .collect();
    let w = |i: usize| words.get(i).map(String::as_str).unwrap_or("");
    match w(0) {
        "SET" => true,
        "CREATE" | "ALTER" => {
            let kind = if w(1) == "OR" { w(3) } else { w(1) };
            matches!(
                kind,
                "SCHEMA" | "VIEW" | "PROCEDURE" | "PROC" | "FUNCTION" | "TRIGGER"
            )
        }
        _ => false,
    }
}

/// Replace the `@Pn` placeholders bound to `NULL` with a literal `NULL` and renumber the rest.
///
/// A TDS parameter always carries a type, and SQL Server refuses some implicit conversions from
/// a typed `NULL` (e.g. `NVARCHAR` into `VARBINARY`), whereas a literal `NULL` converts to
/// anything (pyodbc describes the parameter instead).
pub(crate) fn inline_nulls<'p>(sql: &str, params: &'p [SqlParam]) -> (String, Vec<&'p SqlParam>) {
    let mut out = String::with_capacity(sql.len());
    let mut kept: Vec<&SqlParam> = Vec::new();
    let mut renumbered: std::collections::HashMap<usize, usize> = Default::default();
    let chars: Vec<char> = sql.chars().collect();
    let mut quote: Option<char> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            '\'' | '"' => quote = Some(c),
            '[' => quote = Some(']'),
            '@' if chars.get(i + 1) == Some(&'P')
                && chars.get(i + 2).is_some_and(|d| d.is_ascii_digit())
                && !(i > 0
                    && (chars[i - 1].is_alphanumeric() || matches!(chars[i - 1], '_' | '@'))) =>
            {
                let mut j = i + 2;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                let n: usize = chars[i + 2..j]
                    .iter()
                    .collect::<String>()
                    .parse()
                    .unwrap_or(0);
                match n.checked_sub(1).and_then(|k| params.get(k)) {
                    Some(SqlParam::Null) => out.push_str("NULL"),
                    Some(p) => {
                        let next = renumbered.len() + 1;
                        let m = *renumbered.entry(n).or_insert_with(|| {
                            kept.push(p);
                            next
                        });
                        out.push_str(&format!("@P{m}"));
                    }
                    None => out.extend(&chars[i..j]),
                }
                i = j;
                continue;
            }
            _ => {}
        }
        out.push(c);
        i += 1;
    }
    (out, kept)
}

#[async_trait]
impl NativeConn for MssqlConn {
    async fn execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        // Session `SET` options (e.g. `SET IDENTITY_INSERT`) set inside `sp_executesql` revert
        // when the call returns, and `CREATE SCHEMA` / `VIEW` / ... must open their batch, so
        // these run as plain batches (pyodbc's `SQLExecDirect`).
        if params.is_empty() && runs_as_batch(sql) {
            self.batch(sql).await?;
            return Ok(0);
        }
        let q = query_for(sql, params);
        let r = q.execute(&mut self.client).await.map(|r| r.total());
        self.track(r)
    }

    async fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam],
        chunksize: usize,
        sink: mpsc::Sender<NativeEvent>,
    ) -> Result<()> {
        let q = query_for(sql, params);
        let broken = &mut self.broken;
        let mut stream = match q.query(&mut self.client).await {
            Ok(s) => s,
            Err(e) => return track(broken, Err(e)),
        };
        let mut plans: Option<Vec<ColumnPlan>> = None;
        let mut buf: Vec<Vec<SqlValue>> = Vec::new();
        let mut failure = None;
        loop {
            let item = match stream.try_next().await {
                Ok(Some(item)) => item,
                Ok(None) => break,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            match item {
                QueryItem::Metadata(meta) => {
                    if plans.is_some() {
                        // Only the first result set is returned (pyodbc's `fetchall`).
                        break;
                    }
                    let p = plan(meta.columns());
                    if sink.send(NativeEvent::Columns(p.clone())).await.is_err() {
                        break;
                    }
                    plans = Some(p);
                }
                QueryItem::Row(row) => {
                    let Some(p) = plans.as_ref() else {
                        continue;
                    };
                    let cells = row
                        .cells()
                        .zip(p)
                        .map(|((_, data), plan)| cell(data, plan))
                        .collect::<Result<Vec<_>>>();
                    match cells {
                        Ok(c) => buf.push(c),
                        Err(e) => {
                            drop(stream);
                            return Err(e);
                        }
                    }
                    if buf.len() >= chunksize
                        && sink
                            .send(NativeEvent::Rows(std::mem::take(&mut buf)))
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
            }
        }
        drop(stream);
        if let Some(e) = failure {
            return track(broken, Err(e));
        }
        if plans.is_none() {
            let _ = sink.send(NativeEvent::Columns(Vec::new())).await;
        }
        if !buf.is_empty() {
            let _ = sink.send(NativeEvent::Rows(buf)).await;
        }
        Ok(())
    }

    async fn begin(&mut self) -> Result<()> {
        self.batch("BEGIN TRANSACTION").await
    }

    async fn commit(&mut self) -> Result<()> {
        self.batch("COMMIT TRANSACTION").await
    }

    async fn rollback(&mut self) -> Result<()> {
        // A failed statement may already have rolled the transaction back (`XACT_ABORT`).
        self.batch("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await
    }

    fn is_broken(&self) -> bool {
        self.broken
    }
}

/// The database type name and Arrow kind of a TDS column type.
pub(crate) fn kind_for(ty: ColumnType) -> (ColumnKind, &'static str) {
    match ty {
        ColumnType::Bit | ColumnType::Bitn => (ColumnKind::Boolean, "BIT"),
        ColumnType::Int1 => (ColumnKind::UInt8, "TINYINT"),
        ColumnType::Int2 => (ColumnKind::Int16, "SMALLINT"),
        ColumnType::Int4 => (ColumnKind::Int32, "INT"),
        ColumnType::Int8 | ColumnType::Intn => (ColumnKind::Int64, "BIGINT"),
        ColumnType::Float4 => (ColumnKind::Float32, "REAL"),
        ColumnType::Float8 | ColumnType::Floatn => (ColumnKind::Float64, "FLOAT"),
        ColumnType::Money | ColumnType::Money4 => (ColumnKind::Float64, "MONEY"),
        ColumnType::Datetime4 => (ColumnKind::Timestamp, "SMALLDATETIME"),
        ColumnType::Datetime | ColumnType::Datetimen => (ColumnKind::Timestamp, "DATETIME"),
        ColumnType::Datetime2 => (ColumnKind::Timestamp, "DATETIME2"),
        ColumnType::DatetimeOffsetn => (ColumnKind::TimestampTz, "DATETIMEOFFSET"),
        ColumnType::Daten => (ColumnKind::Date, "DATE"),
        ColumnType::Timen => (ColumnKind::Time, "TIME"),
        ColumnType::Guid => (ColumnKind::Uuid, "UNIQUEIDENTIFIER"),
        ColumnType::Decimaln | ColumnType::Numericn => (ColumnKind::Decimal, "NUMERIC"),
        ColumnType::BigVarBin => (ColumnKind::Bytes, "VARBINARY"),
        ColumnType::BigBinary => (ColumnKind::Bytes, "BINARY"),
        ColumnType::Image => (ColumnKind::Bytes, "IMAGE"),
        ColumnType::BigVarChar => (ColumnKind::Utf8, "VARCHAR"),
        ColumnType::BigChar => (ColumnKind::Utf8, "CHAR"),
        ColumnType::NVarchar => (ColumnKind::Utf8, "NVARCHAR"),
        ColumnType::NChar => (ColumnKind::Utf8, "NCHAR"),
        ColumnType::Text => (ColumnKind::Utf8, "TEXT"),
        ColumnType::NText => (ColumnKind::Utf8, "NTEXT"),
        ColumnType::Xml => (ColumnKind::Utf8, "XML"),
        ColumnType::Null => (ColumnKind::Utf8, "NULL"),
        ColumnType::Udt => (ColumnKind::Raw, "UDT"),
        ColumnType::SSVariant => (ColumnKind::Raw, "SQL_VARIANT"),
    }
}

fn plan(columns: &[tiberius::Column]) -> Vec<ColumnPlan> {
    columns
        .iter()
        .map(|c| {
            let (kind, ty) = kind_for(c.column_type());
            ColumnPlan::new(c.name(), kind, ty)
        })
        .collect()
}

fn conv_err(plan: &ColumnPlan, e: impl std::fmt::Display) -> SqlError {
    SqlError::Conversion {
        column: plan.name.clone(),
        reason: e.to_string(),
    }
}

/// A TDS `NUMERIC` as a decimal (`rust_decimal` keeps 28 digits; wider values stay text).
fn numeric_value(n: tiberius::numeric::Numeric) -> SqlValue {
    match Decimal::try_from_i128_with_scale(n.value(), n.scale() as u32) {
        Ok(d) => SqlValue::Decimal(d),
        Err(_) => SqlValue::Text(n.to_string()),
    }
}

/// Decode one TDS cell.
fn cell(data: &ColumnData<'static>, plan: &ColumnPlan) -> Result<SqlValue> {
    let err = |e: tiberius::error::Error| conv_err(plan, e);
    Ok(match data {
        ColumnData::U8(v) => v.map_or(SqlValue::Null, |v| SqlValue::Int(v as i64)),
        ColumnData::I16(v) => v.map_or(SqlValue::Null, |v| SqlValue::Int(v as i64)),
        ColumnData::I32(v) => v.map_or(SqlValue::Null, |v| SqlValue::Int(v as i64)),
        ColumnData::I64(v) => v.map_or(SqlValue::Null, SqlValue::Int),
        ColumnData::F32(v) => v.map_or(SqlValue::Null, |v| SqlValue::Float(v as f64)),
        ColumnData::F64(v) => v.map_or(SqlValue::Null, SqlValue::Float),
        ColumnData::Bit(v) => v.map_or(SqlValue::Null, SqlValue::Bool),
        ColumnData::String(v) => v
            .as_ref()
            .map_or(SqlValue::Null, |s| SqlValue::Text(s.to_string())),
        ColumnData::Guid(v) => v.map_or(SqlValue::Null, SqlValue::Uuid),
        ColumnData::Binary(v) => v
            .as_ref()
            .map_or(SqlValue::Null, |b| SqlValue::Bytes(b.to_vec())),
        ColumnData::Numeric(v) => v.map_or(SqlValue::Null, numeric_value),
        ColumnData::Xml(v) => v
            .as_ref()
            .map_or(SqlValue::Null, |x| SqlValue::Text(x.to_string())),
        ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) | ColumnData::DateTime2(_) => {
            NaiveDateTime::from_sql(data)
                .map_err(err)?
                .map_or(SqlValue::Null, SqlValue::Timestamp)
        }
        ColumnData::DateTimeOffset(_) => DateTime::<FixedOffset>::from_sql(data)
            .map_err(err)?
            .map_or(SqlValue::Null, |d| {
                SqlValue::TimestampTz(d.with_timezone(&Utc))
            }),
        ColumnData::Date(_) => NaiveDate::from_sql(data)
            .map_err(err)?
            .map_or(SqlValue::Null, SqlValue::Date),
        ColumnData::Time(_) => NaiveTime::from_sql(data)
            .map_err(err)?
            .map_or(SqlValue::Null, SqlValue::Time),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_types() {
        assert_eq!(kind_for(ColumnType::Int8).0, ColumnKind::Int64);
        assert_eq!(
            kind_for(ColumnType::DatetimeOffsetn).0,
            ColumnKind::TimestampTz
        );
        assert_eq!(kind_for(ColumnType::Guid).0, ColumnKind::Uuid);
        assert_eq!(kind_for(ColumnType::NVarchar).1, "NVARCHAR");
    }

    #[test]
    fn odbc_options() {
        let spec = ConnectionSpec::from_uri(
            "mssql+pyodbc://sa:pw@localhost:1439/master?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&Encrypt=no",
            Some("t"),
            None,
        )
        .unwrap();
        let cfg = config(&spec).unwrap();
        assert_eq!(cfg.get_addr(), "localhost:1439");
    }

    #[test]
    fn batches() {
        assert!(runs_as_batch("SET IDENTITY_INSERT [t] ON"));
        assert!(runs_as_batch("CREATE SCHEMA x"));
        assert!(runs_as_batch("create view v as select 1"));
        assert!(!runs_as_batch("UPDATE t SET a = 1"));
        assert!(!runs_as_batch("CREATE TABLE t (a INT)"));
    }

    #[test]
    fn nulls_inlined() {
        let params = [SqlParam::Int(1), SqlParam::Null, SqlParam::Text("x".into())];
        let (sql, kept) = inline_nulls(
            "INSERT INTO [t@P1] (a, b, c) VALUES (@P1, @P2, @P3), ('@P2', @P3, @P2)",
            &params,
        );
        assert_eq!(
            sql,
            "INSERT INTO [t@P1] (a, b, c) VALUES (@P1, NULL, @P2), ('@P2', @P2, NULL)"
        );
        assert_eq!(kept, vec![&params[0], &params[2]]);
    }

    #[test]
    fn numerics() {
        let n = tiberius::numeric::Numeric::new_with_scale(12345, 2);
        assert_eq!(numeric_value(n), SqlValue::Decimal(Decimal::new(12345, 2)));
    }
}
