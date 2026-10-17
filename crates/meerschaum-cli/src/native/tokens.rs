//! `register tokens`, `edit tokens` and `show tokens` (`meerschaum/actions/{register,edit,show}.py`)
//! over the instance connector's token methods (`meerschaum-connector`, `meerschaum-api`).

use chrono::{DateTime, Duration, Utc};
use meerschaum_connector::TokenModel;
use meerschaum_core::users::{Expiration, Token, User};
use meerschaum_core::value::Value;
use meerschaum_core::{SuccessTuple, pyfmt};
use uuid::Uuid;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::format::rich::console::Renderable;
use crate::format::rich::table::{Column, ROUNDED, Table};
use crate::format::rich::text::Text;
use crate::value::{ArgsMap, PyValue, datetime_isoformat};

use super::ask::{Ask, ask, choose};
use super::{Instance, flag, print_tuple, session, warn, words, yes_no};

/// Whether `s` is a UUID (`is_uuid`).
fn is_uuid(s: &str) -> bool {
    Uuid::parse_str(s).is_ok()
}

/// The labels and IDs the words name (`show tokens` / `edit tokens`).
fn labels_and_ids(action: &[String]) -> (Vec<String>, Vec<Uuid>) {
    let labels = action.iter().filter(|a| !is_uuid(a)).cloned().collect();
    let ids = action
        .iter()
        .filter_map(|a| Uuid::parse_str(a).ok())
        .collect();
    (labels, ids)
}

fn expiration_str(model: &TokenModel) -> Option<String> {
    model.expiration.as_ref().map(datetime_isoformat)
}

/// `str(datetime)` of a UTC timestamp (`2032-01-01 00:00:00+00:00`).
fn datetime_str(dt: &DateTime<Utc>) -> String {
    datetime_isoformat(dt).replacen('T', " ", 1)
}

/// The owner's username (`token.user.username`).
fn username(rt: &mut Runtime<'_>, inst: &Instance, model: &TokenModel) -> Option<String> {
    let user_id = model.user_id.clone()?;
    let sess = session(rt);
    sess.block_on(inst.conn().get_username(&user_id))
        .ok()
        .flatten()
        .map(|v| v.py_str())
}

/// `show tokens [labels / IDs ...]`.
pub fn show_tokens(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let inst = super::get_instance(args, rt)?;
    let nopretty = flag(args, "nopretty");
    let action = words(args, "action");
    let (labels, ids) = labels_and_ids(&action);
    let sess = session(rt);
    let tokens = sess
        .block_on(inst.conn().get_tokens(
            None,
            (!labels.is_empty()).then_some(labels.as_slice()),
            (!ids.is_empty()).then_some(ids.as_slice()),
        ))
        .map_err(|e| ActionError::new("Exception", e.to_string()))?;
    let json_of = |rt: &mut Runtime<'_>, t: &TokenModel, expiration_first: bool| -> Value {
        let user = username(rt, &inst, t);
        let mut m = meerschaum_core::value::Map::new();
        let put = |m: &mut meerschaum_core::value::Map, k: &str, v: Value| {
            m.insert(k.into(), v);
        };
        put(
            &mut m,
            "id",
            t.id.map(|i| Value::Str(i.to_string()))
                .unwrap_or(Value::Null),
        );
        put(
            &mut m,
            "label",
            t.label.clone().map(Value::Str).unwrap_or(Value::Null),
        );
        put(
            &mut m,
            "scopes",
            Value::List(t.scopes.iter().cloned().map(Value::Str).collect()),
        );
        let creation = t
            .creation
            .as_ref()
            .map(|c| Value::Str(datetime_isoformat(c)))
            .unwrap_or(Value::Null);
        let expiration = expiration_str(t).map(Value::Str).unwrap_or(Value::Null);
        // `--nopretty` lists `expiration` before `creation`.
        if expiration_first {
            put(&mut m, "expiration", expiration);
            put(&mut m, "creation", creation);
        } else {
            put(&mut m, "creation", creation);
            put(&mut m, "expiration", expiration);
        }
        put(&mut m, "user", user.map(Value::Str).unwrap_or(Value::Null));
        put(&mut m, "is_valid", Value::Bool(t.is_valid));
        Value::Map(m)
    };
    if nopretty {
        for t in &tokens {
            let v = json_of(rt, t, true);
            let line = pyfmt::json_dumps(&v, pyfmt::JsonStyle::Default, true);
            rt.print(&format!("{line}\n"));
        }
        return Ok(SuccessTuple::ok("Success"));
    }
    if tokens.len() == 1 {
        let v = json_of(rt, &tokens[0], false);
        let text = rt.with_formatter(|f| super::rich_json::print_json(f, &v));
        rt.print(&text);
        return Ok(SuccessTuple::ok("Success"));
    }
    let mut users: Vec<String> = Vec::new();
    for t in &tokens {
        users.push(username(rt, &inst, t).unwrap_or_default());
    }
    let title = format!("Registered Tokens on instance '{}'", inst.keys());
    let text = rt.with_formatter(|f| {
        let (valid, invalid) = if f.settings.unicode {
            ("\u{1f7e2}", "\u{1f534}")
        } else {
            ("[+]", "[-]")
        };
        let mut table = Table::new();
        table.box_chars = Some(ROUNDED);
        table.title = Some(Text::new(&title, "bold"));
        for header in ["ID", "Label", "User", "Expiration", "Valid"] {
            table.add_column(Column {
                header: Text::plain_text(header),
                ..Column::default()
            });
        }
        for (t, user) in tokens.iter().zip(&users) {
            let cell = |s: String| Some(Box::new(Text::plain_text(&s)) as Box<dyn Renderable>);
            table.add_row(vec![
                cell(t.id.map(|i| i.to_string()).unwrap_or_else(|| "None".into())),
                cell(t.label.clone().unwrap_or_else(|| "None".into())),
                cell(user.clone()),
                cell(expiration_str(t).unwrap_or_else(|| "Does not expire".into())),
                cell(if t.is_valid { valid } else { invalid }.to_string()),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
    Ok(SuccessTuple::ok("Success"))
}

/// `register tokens`: register a long-lived access token for the instance.
pub fn register_tokens(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let nopretty = flag(args, "nopretty");
    let expiration = match args.get("end") {
        Some(PyValue::DateTime(dt)) => Expiration::At(*dt),
        _ => match super::int(args, "ttl_days").filter(|d| *d != 0) {
            Some(days) => Expiration::At(Utc::now() + Duration::days(days)),
            None => Expiration::Default,
        },
    };
    let inst = super::get_instance(args, rt)?;
    let sess = session(rt);
    let name = super::string(args, "name");
    let scopes = {
        let s = words(args, "scopes");
        (!s.is_empty()).then_some(s)
    };
    let username = if inst.type_name() != "api" {
        let usernames = sess
            .block_on(inst.conn().get_users())
            .map_err(|e| ActionError::new("Exception", e.to_string()))?;
        if usernames.is_empty() {
            return Ok(SuccessTuple::fail(format!(
                "No users are registered to '{}'.",
                inst.keys()
            )));
        }
        choose(
            rt,
            "To which user should this token be registered? Enter the number.",
            &usernames,
            None,
            true,
        )?
        .unwrap_or_default()
    } else {
        let label = inst.keys();
        let (typ, label) = super::session::split_keys(&label);
        super::session::connector_attributes(rt.config(), &typ, &label)
            .get("username")
            .map(Value::py_str)
            .ok_or_else(|| {
                ActionError::new(
                    "AttributeError",
                    "'APIConnector' object has no attribute 'username'",
                )
            })?
    };
    if username.is_empty() {
        return Ok(SuccessTuple::fail(
            "Cannot register a token without a user.",
        ));
    }
    let mut user = User::new(&username, None);
    user.instance_keys = Some(inst.keys());
    let user_id = sess
        .block_on(inst.conn().get_user_id(&user))
        .map_err(|e| ActionError::new("Exception", e.to_string()))?;
    let Some(user_id) = user_id else {
        return Ok(SuccessTuple::fail(format!(
            "Cannot load ID for user '{username}'."
        )));
    };
    user.user_id = Some(user_id.clone());
    let default_days = rt
        .config()
        .get(&["api", "tokens", "default_expiration_days"])
        .and_then(|v| pyfmt::py_int(&v))
        .unwrap_or(366);
    let label = name.unwrap_or_else(|| {
        meerschaum_jobs::names::get_new_daemon_name(
            &meerschaum_jobs::config::JobPaths::from_config(rt.config()),
        )
    });
    let mut token = Token::new(Some(&label), expiration, scopes, default_days, Utc::now());
    token.instance_keys = Some(inst.keys());
    token.user = Some(user);
    token.user_id = Some(user_id);
    let res = sess.block_on(inst.conn().register_token(&mut token));
    if !res.success {
        return Ok(SuccessTuple::fail(format!(
            "Failed to register token '{}':\n{}",
            token.label.clone().unwrap_or_default(),
            res.message
        )));
    }
    let token_id = token.id;
    let secret = token.secret.clone().unwrap_or_default();
    // `token.to_model(refresh=True)`.
    let model = match token_id {
        Some(id) => sess
            .block_on(inst.conn().get_token_model(&id))
            .map_err(|e| ActionError::new("Exception", e.to_string()))?
            .ok_or_else(|| {
                ActionError::new(
                    "ValueError",
                    format!(
                        "{} does not exist on instance '{}'.",
                        token.label.clone().unwrap_or_default(),
                        inst.keys()
                    ),
                )
            })?,
        None => {
            return Err(ActionError::new(
                "ValueError",
                format!(
                    "ID is not set for {}.",
                    token.label.clone().unwrap_or_default()
                ),
            ));
        }
    };
    let shown_label = model.label.clone().unwrap_or_default();
    if !nopretty {
        print_tuple(
            rt,
            &SuccessTuple::ok(format!(
                "Registered token '{shown_label}'.\n    Write down the client secret, because it won't be shown again."
            )),
            TupleArgs {
                calm: true,
                ..TupleArgs::default()
            },
        );
    }
    let token_expiration = model.expiration.as_ref().map(datetime_isoformat);
    if nopretty {
        // PARITY: `json.dumps({'secret': ..., **dict(token_model)})` cannot serialize the
        // model's `UUID` (`actions/register.py`), so `--nopretty` fails after registering.
        return Err(ActionError::new(
            "TypeError",
            "Object of type UUID is not JSON serializable",
        ));
    }
    let header =
        rt.with_formatter(|f| f.make_header(&format!("Attributes for token '{shown_label}':")));
    let mut api_token = token.clone();
    api_token.id = model.id;
    api_token.secret = Some(secret.clone());
    let text = format!(
        "{header}\n    - Client ID: {}\n    - Client Secret: {secret}\n    - Expiration: {}\n    - API Key: {}\n\n",
        model
            .id
            .map(|i| i.to_string())
            .unwrap_or_else(|| "None".into()),
        token_expiration.unwrap_or_else(|| "Does not expire.".into()),
        api_token.get_api_key(),
    );
    rt.print(&text);
    Ok(SuccessTuple::ok(format!(
        "Successfully registered token '{shown_label}'."
    )))
}

/// `edit tokens <labels / IDs ...>`: edit tokens interactively.
pub fn edit_tokens(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail(
            "Provide token labels or IDs for the tokens to edit\n    (run `show tokens` to see registered tokens).",
        ));
    }
    let inst = super::get_instance(args, rt)?;
    let sess = session(rt);
    let (labels, ids) = labels_and_ids(&action);
    let tokens = sess
        .block_on(inst.conn().get_tokens(
            None,
            (!labels.is_empty()).then_some(labels.as_slice()),
            (!ids.is_empty()).then_some(ids.as_slice()),
        ))
        .map_err(|e| ActionError::new("Exception", e.to_string()))?;
    let mut num_edited = 0usize;
    for listed in tokens {
        let model = match listed.id {
            Some(id) => sess
                .block_on(inst.conn().get_token_model(&id))
                .ok()
                .flatten(),
            None => None,
        };
        let Some(model) = model else {
            warn(
                rt,
                &format!(
                    "Token '{}' does not exist.",
                    listed.id.map(|i| i.to_string()).unwrap_or_default()
                ),
            );
            continue;
        };
        let label = ask(
            rt,
            "Label:",
            &Ask::new().editable(model.label.clone().unwrap_or_default()),
        )?;
        let expiration_text = ask(
            rt,
            "Expiration (empty for no expiration):",
            &Ask::new().editable(
                model
                    .expiration
                    .as_ref()
                    .map(datetime_str)
                    .unwrap_or_default(),
            ),
        )?;
        let expiration = if expiration_text.is_empty() {
            None
        } else {
            let dates = rt.dates.clone();
            let (parsed, offset) =
                crate::dateutil::parse(&expiration_text, &dates).map_err(|_| {
                    ActionError::new(
                        "ParserError",
                        format!("Unknown string format: {expiration_text}"),
                    )
                })?;
            // A naive timestamp is UTC (`coerce_timezone`); an offset is applied.
            let utc = parsed - Duration::seconds(offset.unwrap_or(0));
            Some(DateTime::<Utc>::from_naive_utc_and_offset(utc, Utc))
        };
        let scopes_text = ask(
            rt,
            "Scope (`*` to grant all permissions):",
            &Ask::new().editable(model.scopes.join(" ")),
        )?;
        let scopes: Vec<String> = if scopes_text != "*" {
            scopes_text.split(' ').map(str::to_string).collect()
        } else {
            meerschaum_core::static_config().scope_names()
        };
        let invalidate = if model.is_valid {
            yes_no(
                rt,
                "Do you want to invalidate this token?",
                "n",
                false,
                false,
            )?
        } else {
            true
        };
        let mut token = Token::new(
            Some(&label),
            expiration.map(Expiration::At).unwrap_or(Expiration::Never),
            Some(scopes),
            366,
            Utc::now(),
        );
        token.id = model.id;
        token.creation = model.creation;
        token.instance_keys = Some(inst.keys());
        token.user_id = model.user_id.clone();
        token.secret_hash = model.secret_hash.clone();
        token.is_valid = model.is_valid && !invalidate;
        let res = sess.block_on(inst.conn().edit_token(&token));
        if !res.success {
            return Ok(SuccessTuple::fail(res.message));
        }
        if invalidate {
            token.is_valid = false;
            let res = sess.block_on(inst.conn().invalidate_token(&token));
            if !res.success {
                return Ok(SuccessTuple::fail(res.message));
            }
        }
        num_edited += 1;
    }
    Ok(SuccessTuple::ok(format!(
        "Successfully edited {num_edited} token{}.",
        super::plural(num_edited)
    )))
}
