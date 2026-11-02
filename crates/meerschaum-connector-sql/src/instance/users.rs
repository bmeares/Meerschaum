//! Users in the `mrsm_users` table (`meerschaum/connectors/sql/_users.py`). Password hashes use
//! `meerschaum_core::users` (passlib's `pbkdf2-sha256` modular-crypt format), so v4 user rows
//! validate unchanged.

use meerschaum_connector::{ConnectorError, ConnectorResult, InstanceConnector, passwords};
use meerschaum_core::users::{User, valid_username};
use meerschaum_core::{Map, SuccessTuple, Value};

use crate::driver::params::{placeholder, placeholders};
use crate::driver::{SqlParam, SqlValue};
use crate::flavor::{DriverFamily, Flavor};

use meerschaum_kernel::value::Value as KValue;

use super::{SqlConnector, be, frame_docs, kv_str};

/// Fresh transactions [`SqlConnector::register_first_user`] tries before returning a failure.
const FIRST_USER_ATTEMPTS: usize = 3;

/// How a first-user transaction failed: before its commit (rolled back, safe to try again) or
/// at or after it (never tried again; a missing lock row or a wrong row count counts here too).
enum FirstUserError {
    BeforeCommit(ConnectorError),
    Commit(ConnectorError),
}

/// A user ID cell as an integer (drivers return integer columns as integers, Oracle's `NUMBER`
/// as a decimal or float, some as text).
fn user_id_of(v: &KValue) -> Option<i64> {
    match v {
        KValue::Int(i) => Some(*i),
        // Only integers a float holds exactly (|f| <= 2^53): never a rounded identity.
        KValue::Float(f)
            if f.is_finite() && f.fract() == 0.0 && f.abs() <= 9_007_199_254_740_992.0 =>
        {
            Some(*f as i64)
        }
        KValue::Decimal(d) => d.to_string().parse().ok(),
        KValue::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A user ID value as a bound parameter.
fn id_param(v: &Value) -> SqlParam {
    match v {
        Value::Int(i) => SqlParam::Int(*i),
        Value::Null => SqlParam::Null,
        other => match other.py_str().parse::<i64>() {
            Ok(i) => SqlParam::Int(i),
            Err(_) => SqlParam::Text(other.py_str()),
        },
    }
}

/// Parse a stored attributes value (JSONB, or JSON text).
pub(crate) fn parse_attributes(v: &SqlValue) -> Option<Map> {
    match v {
        SqlValue::Json(s) | SqlValue::Text(s) => match Value::from_json_str(s) {
            Ok(Value::Map(m)) => Some(m),
            Ok(Value::Str(inner)) => Value::from_json_str(&inner).ok().map(|v| v.to_map()),
            _ => {
                tracing::warn!("Received unexpected type for attributes: {s}");
                None
            }
        },
        _ => None,
    }
}

impl SqlConnector {
    /// `register_user(user)`.
    pub async fn register_user(&self, user: &mut User) -> SuccessTuple {
        let valid = valid_username(&user.username);
        if !valid.success {
            return valid;
        }
        match self.get_user_id(user).await {
            Ok(Some(_)) => {
                return SuccessTuple::fail(format!("User '{}' already exists.", user.username));
            }
            Ok(None) => {}
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let _ = self.ensure_instance_tables(true).await;
        // Without the unique key on usernames (rows from before it, held twice), a new
        // registration could add another ambiguous account: refuse until they are repaired.
        self.ensure_unique_key(&super::unique_keys::USERS_KEY).await;
        match self.unique_key_status(&super::unique_keys::USERS_KEY).await {
            Ok(super::unique_keys::KeyStatus::Present | super::unique_keys::KeyStatus::NoTable) => {
            }
            Ok(super::unique_keys::KeyStatus::Missing { .. }) => {
                return SuccessTuple::fail(format!(
                    "Registering users on {self} is refused while a username is held by more than \
                     one user. Run `mrsm doctor` for the duplicates and remove the extra rows."
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let password_hash = match passwords::user_password_hash(user).await {
            Ok(h) => h,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let attributes = user.attributes.clone().unwrap_or_default();
        let table = self.instance_table_name("mrsm_users");
        // SQLite hands a new row the highest deleted rowid again: a SQLite-family user's ID is
        // reserved first, never to be handed out twice (`reserve_user_id`).
        let reserved = if self.flavor.driver_family() == DriverFamily::Sqlite {
            match self.reserve_user_id().await {
                Ok(id) => Some(id),
                Err(e) => return SuccessTuple::fail(e.to_string()),
            }
        } else {
            None
        };
        let query = match reserved {
            Some(_) => format!(
                "INSERT INTO {table} (user_id, username, password_hash, email, user_type, attributes) VALUES ({})",
                placeholders(self.flavor, 1, 6)
            ),
            None => format!(
                "INSERT INTO {table} (username, password_hash, email, user_type, attributes) VALUES ({})",
                placeholders(self.flavor, 1, 5)
            ),
        };
        let mut params: Vec<SqlParam> = reserved.map(SqlParam::Int).into_iter().collect();
        params.extend([
            SqlParam::Text(user.username.clone()),
            SqlParam::Text(password_hash),
            SqlParam::opt_text(user.email.as_deref()),
            SqlParam::opt_text(user.user_type.as_deref()),
            self.json_param(&attributes),
        ]);
        match self.exec_params(&query, &params).await {
            Some(_) => {
                SuccessTuple::ok(format!("Successfully registered user '{}'.", user.username))
            }
            // Another process registered the username after the check above (the unique key
            // refused the second row; `unique_keys.rs`).
            None if matches!(self.get_user_id(user).await, Ok(Some(_))) => {
                SuccessTuple::fail(format!("User '{}' already exists.", user.username))
            }
            None => SuccessTuple::fail(format!("Failed to register user '{}'.", user.username)),
        }
    }

    /// `valid_username(username)`.
    pub fn valid_username(username: &str) -> SuccessTuple {
        valid_username(username)
    }

    /// `edit_user(user)`: update the user's row (password, email, type, attributes).
    ///
    /// PARITY: an unset email or type (`None != ''`) is written as `NULL` (`_users.py:157`).
    pub async fn edit_user(&self, user: &User) -> SuccessTuple {
        let user_id = match &user.user_id {
            Some(id) => Some(id.clone()),
            None => match self.get_user_id(user).await {
                Ok(id) => id,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            },
        };
        let Some(user_id) = user_id else {
            return SuccessTuple::fail(format!(
                "User '{0}' does not exist. Register user '{0}' before editing.",
                user.username
            ));
        };
        let valid = valid_username(&user.username);
        if !valid.success {
            return valid;
        }
        let mut sets: Vec<(&str, SqlParam)> = vec![
            ("user_id", id_param(&user_id)),
            ("username", SqlParam::Text(user.username.clone())),
        ];
        // PARITY deviation: v4 (`_users.py:163`) also sets `user_id` on MSSQL, where it is an
        // `IDENTITY` column ("Cannot update identity column"), so every edit failed there.
        if self.flavor == Flavor::Mssql {
            sets.remove(0);
        }
        if !user.password.is_empty() {
            let mut u = user.clone();
            match passwords::user_password_hash(&mut u).await {
                Ok(h) => sets.push(("password_hash", SqlParam::Text(h))),
                Err(e) => return SuccessTuple::fail(e.to_string()),
            }
        }
        if user.email.as_deref() != Some("") {
            sets.push(("email", SqlParam::opt_text(user.email.as_deref())));
        }
        if let Some(attrs) = user.attributes.as_ref().filter(|a| !a.is_empty()) {
            sets.push(("attributes", self.json_param(attrs)));
        }
        if user.user_type.as_deref() != Some("") {
            sets.push(("user_type", SqlParam::opt_text(user.user_type.as_deref())));
        }
        let table = self.instance_table_name("mrsm_users");
        let f = self.flavor;
        let set_str: Vec<String> = sets
            .iter()
            .enumerate()
            .map(|(i, (k, _))| format!("{k}={}", placeholder(f, i + 1)))
            .collect();
        let query = format!(
            "UPDATE {table} SET {} WHERE {table}.user_id = {}",
            set_str.join(", "),
            placeholder(f, sets.len() + 1)
        );
        let mut params: Vec<SqlParam> = sets.into_iter().map(|(_, p)| p).collect();
        params.push(id_param(&user_id));
        match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully edited user '{}'.", user.username)),
            None => SuccessTuple::fail(format!("Failed to edit user '{}'.", user.username)),
        }
    }

    /// `get_user_id(user)`: the user's `user_id` if registered.
    pub async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        if !self.ensure_instance_tables(true).await {
            return Ok(None);
        }
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.user_id \nFROM {table} \nWHERE {table}.username = {}",
            placeholder(self.flavor, 1)
        );
        let df = self
            .read_params(&query, &[SqlParam::Text(user.username.clone())])
            .await
            .map_err(be)?;
        let docs = frame_docs(&df);
        // A username held by more than one row (an instance from before the unique key) names
        // none of them: no credential may pick one.
        if docs.len() > 1 {
            return Err(be(format!(
                "The username '{}' is held by {} users, so it names none of them. Run `mrsm doctor` \
                 for the duplicates and remove the extra rows.",
                user.username,
                docs.len()
            )));
        }
        Ok(docs
            .first()
            .and_then(|doc| doc.get("user_id").and_then(user_id_of))
            .map(Value::Int))
    }

    async fn resolve_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        match &user.user_id {
            Some(id) => Ok(Some(id.clone())),
            None => self.get_user_id(user).await,
        }
    }

    async fn user_column(&self, user: &User, column: &str) -> ConnectorResult<Option<SqlValue>> {
        let Some(user_id) = self.resolve_user_id(user).await? else {
            return Ok(None);
        };
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.{column} \nFROM {table} \nWHERE {table}.user_id = {}",
            placeholder(self.flavor, 1)
        );
        self.value_params(&query, &[id_param(&user_id)]).await
    }

    /// `get_user_attributes(user)`.
    pub async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        Ok(self
            .user_column(user, "attributes")
            .await?
            .and_then(|v| parse_attributes(&v)))
    }

    /// `get_user_password_hash(user)`.
    pub async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "password_hash")
            .await?
            .map(|v| v.to_string()))
    }

    /// `get_user_type(user)`.
    pub async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "user_type")
            .await?
            .map(|v| v.to_string()))
    }

    /// The user's email address (`None` when unset or empty).
    pub async fn get_user_email(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self
            .user_column(user, "email")
            .await?
            .filter(|v| !matches!(v, SqlValue::Null))
            .map(|v| v.to_string())
            .filter(|e| !e.trim().is_empty()))
    }

    /// `get_username(user_id)` (the `InstanceConnector` method, over `mrsm_users`).
    pub async fn get_username(&self, user_id: &Value) -> ConnectorResult<Option<Value>> {
        let table = self.instance_table_name("mrsm_users");
        let query = format!(
            "SELECT {table}.username \nFROM {table} \nWHERE {table}.user_id = {}",
            placeholder(self.flavor, 1)
        );
        Ok(self
            .value_params(&query, &[id_param(user_id)])
            .await?
            .map(|v| Value::Str(v.to_string())))
    }

    /// [`InstanceConnector::register_first_user`]: one transaction on one connection (SQLite:
    /// `BEGIN IMMEDIATE`) changes the `mrsm_bootstrap` row with an `UPDATE`, then counts the users
    /// in a later statement and inserts `user` only when there are none. A concurrent first
    /// registration waits on the row, then sees the winner (READ COMMITTED) or fails with a
    /// serialization error (snapshot isolation). Any error rolls the whole transaction back and is
    /// returned (fail closed: never a promotion from a stale count). The lock row is seeded
    /// idempotently, and a missing row is an error, never a lock.
    ///
    /// [`InstanceConnector::register_first_user`]: meerschaum_connector::InstanceConnector::register_first_user
    pub async fn register_first_user(&self, user: &mut User) -> ConnectorResult<bool> {
        let valid = valid_username(&user.username);
        if !valid.success {
            return Err(be(valid.message));
        }
        // Before any database work, so no ID is reserved for a password that cannot be hashed.
        let password_hash = passwords::user_password_hash(user).await?;
        if !self.ensure_instance_tables(true).await {
            return Err(be(format!(
                "Could not create the instance tables of {self}."
            )));
        }
        self.ensure_unique_key(&super::unique_keys::USERS_KEY).await;
        if let super::unique_keys::KeyStatus::Missing { .. } = self
            .unique_key_status(&super::unique_keys::USERS_KEY)
            .await?
        {
            return Err(be(format!(
                "Registering users on {self} is refused while a username is held by more than \
                 one user. Run `mrsm doctor` for the duplicates and remove the extra rows."
            )));
        }
        let lock = self.seed_bootstrap_lock().await?;
        let reserved = if self.flavor.driver_family() == DriverFamily::Sqlite {
            Some(self.reserve_user_id().await?)
        } else {
            None
        };
        let attributes = user.attributes.clone().unwrap_or_default();
        let users = self.instance_table_name("mrsm_users");
        let insert = match reserved {
            Some(_) => format!(
                "INSERT INTO {users} (user_id, username, password_hash, email, user_type, attributes) VALUES ({})",
                placeholders(self.flavor, 1, 6)
            ),
            None => format!(
                "INSERT INTO {users} (username, password_hash, email, user_type, attributes) VALUES ({})",
                placeholders(self.flavor, 1, 5)
            ),
        };
        let mut params: Vec<SqlParam> = reserved.map(SqlParam::Int).into_iter().collect();
        params.extend([
            SqlParam::Text(user.username.clone()),
            SqlParam::Text(password_hash),
            SqlParam::opt_text(user.email.as_deref()),
            SqlParam::opt_text(user.user_type.as_deref()),
            self.json_param(&attributes),
        ]);

        // A failure before the commit (a serialization failure under snapshot isolation, a
        // deadlock victim) rolled everything back: a fresh transaction sees the winner. A failed
        // commit is never retried (its outcome is unknown).
        let mut attempt = 1;
        loop {
            match self
                .first_user_attempt(&lock, &users, &insert, &params)
                .await
            {
                Err(FirstUserError::BeforeCommit(e)) if attempt < FIRST_USER_ATTEMPTS => {
                    self.log(&format!("First-user attempt {attempt} rolled back: {e}"));
                    tokio::time::sleep(std::time::Duration::from_millis(25 * attempt as u64)).await;
                    attempt += 1;
                }
                Err(FirstUserError::BeforeCommit(e) | FirstUserError::Commit(e)) => return Err(e),
                Ok(first) => return Ok(first),
            }
        }
    }

    /// One transaction of [`Self::register_first_user`]. Every early return drops `tx`, which
    /// rolls it back. The lock is an `UPDATE` that changes the row (MySQL counts changed rows
    /// only): it waits for a concurrent bootstrap and, under snapshot isolation (PostgreSQL
    /// REPEATABLE READ, Oracle SERIALIZABLE, DuckDB), fails instead of reading a stale "no users".
    /// Exactly one row must change, so a missing row is an error, never a lock.
    async fn first_user_attempt(
        &self,
        lock: &str,
        users: &str,
        insert: &str,
        params: &[SqlParam],
    ) -> Result<bool, FirstUserError> {
        let before = |e: String| FirstUserError::BeforeCommit(be(e));
        let mut tx = self
            .driver
            .begin_write()
            .await
            .map_err(|e| before(e.to_string()))?;
        let lock_statement =
            format!("UPDATE {lock} SET generation = 1 - generation WHERE claim = 1");
        self.log(&lock_statement);
        let locked = tx
            .exec(&lock_statement)
            .await
            .map_err(|e| before(e.to_string()))?;
        if locked != 1 {
            return Err(FirstUserError::Commit(be(format!(
                "The bootstrap lock row of {self} is missing."
            ))));
        }
        let count = format!("SELECT COUNT(*) AS users FROM {users}");
        self.log(&count);
        let counted = tx
            .read_frame(&count)
            .await
            .map_err(|e| before(e.to_string()))?;
        let users_now = frame_docs(&counted)
            .first()
            .and_then(|doc| doc.get("users"))
            .and_then(user_id_of)
            .ok_or_else(|| before(format!("Could not count the users of {self}.")))?;
        if users_now > 0 {
            tx.rollback().await.map_err(|e| before(e.to_string()))?;
            return Ok(false);
        }
        self.log(insert);
        let inserted = tx
            .raw_execute(insert, params)
            .await
            .map_err(|e| before(e.to_string()))?;
        if inserted != 1 {
            return Err(FirstUserError::Commit(be(format!(
                "Registering the first user of {self} wrote {inserted} rows."
            ))));
        }
        tx.commit()
            .await
            .map_err(|e| FirstUserError::Commit(be(e)))?;
        Ok(true)
    }

    /// Create `mrsm_bootstrap` with its one row (`claim = 1`) if missing, and check the row is
    /// there. A present row is only read (an `INSERT` of a duplicate key would wait on a
    /// bootstrap holding the row). Concurrent seeders may both insert: a refused duplicate is
    /// fine, a missing row is not.
    async fn seed_bootstrap_lock(&self) -> ConnectorResult<String> {
        let lock = self.instance_table_name("mrsm_bootstrap");
        let schema = self.instance_schema().map(str::to_string);
        let check = format!("SELECT COUNT(*) AS n FROM {lock} WHERE claim = 1");
        let claims = async || -> ConnectorResult<Option<i64>> {
            let df = self.read(&check).await.map_err(be)?;
            Ok(frame_docs(&df)
                .first()
                .and_then(|d| d.get("n"))
                .and_then(user_id_of))
        };
        if self
            .table_exists_checked("mrsm_bootstrap", schema.as_deref())
            .await?
        {
            if claims().await? == Some(1) {
                return Ok(lock);
            }
        } else {
            let create = format!(
                "CREATE TABLE {lock} (claim INTEGER NOT NULL PRIMARY KEY, generation INTEGER NOT NULL)"
            );
            self.log(&create);
            let _ = self.driver.exec(&create).await;
        }
        let seed = format!("INSERT INTO {lock} (claim, generation) VALUES (1, 0)");
        self.log(&seed);
        let _ = self.driver.exec(&seed).await;
        match claims().await? {
            Some(1) => Ok(lock),
            _ => Err(be(format!(
                "Could not seed the bootstrap lock row of {self}."
            ))),
        }
    }

    /// `mrsm_user_ids` (SQLite family), created when missing.
    async fn user_ids_table(&self) -> ConnectorResult<String> {
        let ids = self.instance_table_name("mrsm_user_ids");
        let create = format!("CREATE TABLE IF NOT EXISTS {ids} (user_id INTEGER PRIMARY KEY)");
        self.log(&create);
        self.driver.exec(&create).await.map_err(be)?;
        Ok(ids)
    }

    /// Keep a user's ID reserved (SQLite family): `delete_user` calls it before deleting.
    async fn retain_user_id(&self, user_id: &Value) -> ConnectorResult<()> {
        let id = user_id
            .as_i64()
            .ok_or_else(|| be(format!("Invalid user ID: {}", user_id.py_str())))?;
        let ids = self.user_ids_table().await?;
        let keep = format!("INSERT OR IGNORE INTO {ids} (user_id) VALUES ({id})");
        self.log(&keep);
        self.driver.exec(&keep).await.map_err(be)?;
        Ok(())
    }

    /// Reserve a new user ID on a SQLite-family instance. One write statement appends it to
    /// `mrsm_user_ids`, whose rows are never deleted, above every ID reserved before and every
    /// user's and token owner's ID (rows registered before the table existed), so an ID is never
    /// handed out twice, even after its user and their keys are deleted. A user insert that then
    /// fails wastes the ID. SQLite serializes writers, so concurrent reservations differ; the
    /// largest ID is an error, never an overflow.
    async fn reserve_user_id(&self) -> ConnectorResult<i64> {
        let ids = self.user_ids_table().await?;
        let users = self.instance_table_name("mrsm_users");
        let token_owners = self.token_owner_ids_query().await?;
        let query = format!(
            "INSERT INTO {ids} (user_id)\n\
             SELECT highest + 1 FROM (\n    \
             SELECT COALESCE(MAX(id), 0) AS highest FROM (\n        \
             SELECT MAX(user_id) AS id FROM {ids}\n        \
             UNION ALL SELECT MAX(user_id) FROM {users}{token_owners}\n    \
             ) AS ids\n\
             ) AS reserved\n\
             WHERE highest < {}\n\
             RETURNING user_id",
            i64::MAX
        );
        // Drain RETURNING so a refused commit is an error before an ID is handed out.
        let reserved = self.read(&query).await.map_err(be)?;
        match frame_docs(&reserved)
            .first()
            .and_then(|doc| doc.get("user_id"))
            .and_then(user_id_of)
        {
            Some(id) => Ok(id),
            None => Err(be(format!(
                "No user ID is left on {self}: the largest has been reserved."
            ))),
        }
    }

    /// `UNION ALL SELECT MAX(...)` over the tokens table's `user_id`, or nothing while the
    /// table does not exist (a failed probe is an error).
    async fn token_owner_ids_query(&self) -> ConnectorResult<String> {
        let tokens = self.sql_pipe(&InstanceConnector::get_tokens_pipe(self))?;
        let state = self.pipe_state_checked(&tokens).await?;
        if !state.exists {
            return Ok(String::new());
        }
        Ok(format!(
            "\n        UNION ALL SELECT MAX(CAST(user_id AS INTEGER)) FROM {}",
            self.pipe_table_name(&tokens)
        ))
    }

    /// `delete_user(user)`: delete the user, their tokens and the plugins they own.
    pub async fn delete_user(&self, user: &User) -> SuccessTuple {
        let user_id = match self.resolve_user_id(user).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return SuccessTuple::fail(format!(
                    "User '{}' is not registered and cannot be deleted.",
                    user.username
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        // SQLite: the ID is kept reserved before anything is deleted (a user registered before
        // reservations existed has none), so a key written late for it never names a new user.
        if self.flavor.driver_family() == DriverFamily::Sqlite
            && let Err(e) = self.retain_user_id(&user_id).await
        {
            return SuccessTuple::fail(format!("Failed to delete user '{}': {e}", user.username));
        }
        // The user's tokens go first: none may outlive the user (SQLite reuses user IDs).
        let tokens = meerschaum_connector::pipe_ops::delete_tokens_of_user(self, &user_id).await;
        if !tokens.success {
            return tokens;
        }
        let users = self.instance_table_name("mrsm_users");
        let plugins = self.instance_table_name("mrsm_plugins");
        let f = self.flavor;
        let q1 = format!(
            "DELETE FROM {users} WHERE {users}.user_id = {}",
            placeholder(f, 1)
        );
        if self.exec_params(&q1, &[id_param(&user_id)]).await.is_none() {
            return SuccessTuple::fail(format!("Failed to delete user '{}'.", user.username));
        }
        let q2 = format!(
            "DELETE FROM {plugins} WHERE {plugins}.user_id = {}",
            placeholder(f, 1)
        );
        if self.exec_params(&q2, &[id_param(&user_id)]).await.is_none() {
            return SuccessTuple::fail(format!(
                "Failed to delete plugins of user '{}'.",
                user.username
            ));
        }
        // The user leaves every team and their pipes keep their team, without an owner (every
        // surface reaches this method: the trait, the Python façade, the API server).
        let forgotten = meerschaum_connector::teams::forget_user(self, &user.username).await;
        if !forgotten.success {
            tracing::warn!("{}", forgotten.message);
        }
        SuccessTuple::ok(format!("Successfully deleted user '{}'", user.username))
    }

    /// `set_user_attributes(user, attributes)`: replace the user's attributes and nothing else
    /// (their type, email and password stay).
    pub async fn set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        let user_id = match self.resolve_user_id(user).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return SuccessTuple::fail(format!("User '{}' does not exist.", user.username));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let table = self.instance_table_name("mrsm_users");
        let f = self.flavor;
        let query = format!(
            "UPDATE {table} SET attributes={} WHERE {table}.user_id = {}",
            placeholder(f, 1),
            placeholder(f, 2)
        );
        let params = vec![self.json_param(attributes), id_param(&user_id)];
        match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully edited user '{}'.", user.username)),
            None => SuccessTuple::fail(format!("Failed to edit user '{}'.", user.username)),
        }
    }

    /// `get_users()`: the registered usernames.
    pub async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        let _ = self.ensure_instance_tables(true).await;
        let table = self.instance_table_name("mrsm_users");
        let df = self
            .read(&format!("SELECT {table}.username \nFROM {table}"))
            .await
            .map_err(be)?;
        Ok(frame_docs(&df)
            .iter()
            .filter_map(|d| kv_str(d.get("username")))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_ids_are_exact_integers() {
        assert_eq!(user_id_of(&KValue::Int(7)), Some(7));
        assert_eq!(user_id_of(&KValue::Float(7.0)), Some(7));
        assert_eq!(user_id_of(&KValue::Float(-0.0)), Some(0));
        assert_eq!(
            user_id_of(&KValue::Float(9_007_199_254_740_992.0)),
            Some(9_007_199_254_740_992)
        );
        // Floats that are no exact identity: never saturated or rounded into one.
        for f in [
            7.5,
            1e19,
            -1e19,
            9_007_199_254_740_994.0,
            f64::NAN,
            f64::INFINITY,
        ] {
            assert_eq!(user_id_of(&KValue::Float(f)), None, "{f}");
        }
        assert_eq!(user_id_of(&KValue::Str(" 12 ".into())), Some(12));
        assert_eq!(user_id_of(&KValue::Str("12.5".into())), None);
    }
}
