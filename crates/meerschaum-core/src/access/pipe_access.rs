//! A pipe's `parameters['access']`: its owner, team, visibility and edit mode.

use super::*;

/// The reserved parameter key of a pipe's access.
pub const ACCESS_KEY: &str = "access";
/// The reserved parameter key of a pipe's locks.
pub const LOCKS_KEY: &str = "locks";

/// Who can see a pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    /// The members of the pipe's team (and its owner).
    #[default]
    Team,
    /// The owner, the team's managers and administrators.
    Private,
    /// Every user.
    Everyone,
}

impl Visibility {
    /// Every visibility.
    pub const ALL: [Visibility; 3] = [Visibility::Team, Visibility::Private, Visibility::Everyone];

    /// The stored name (`team`, `private`, `everyone`).
    pub fn name(&self) -> &'static str {
        match self {
            Visibility::Team => "team",
            Visibility::Private => "private",
            Visibility::Everyone => "everyone",
        }
    }

    /// Parse a visibility (any case).
    pub fn parse(text: &str) -> Option<Visibility> {
        match text.trim().to_lowercase().as_str() {
            "team" => Some(Visibility::Team),
            "private" => Some(Visibility::Private),
            "everyone" | "all" | "public" => Some(Visibility::Everyone),
            _ => None,
        }
    }
}

impl fmt::Display for Visibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Who can change a pipe (within their permissions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditMode {
    /// Anyone who can see the pipe.
    #[default]
    Team,
    /// The owner, the team's managers and administrators.
    Owner,
}

impl EditMode {
    /// Every edit mode.
    pub const ALL: [EditMode; 2] = [EditMode::Team, EditMode::Owner];

    /// The stored name (`team`, `owner`).
    pub fn name(&self) -> &'static str {
        match self {
            EditMode::Team => "team",
            EditMode::Owner => "owner",
        }
    }

    /// Parse an edit mode (any case).
    pub fn parse(text: &str) -> Option<EditMode> {
        match text.trim().to_lowercase().as_str() {
            "team" => Some(EditMode::Team),
            "owner" => Some(EditMode::Owner),
            _ => None,
        }
    }
}

impl fmt::Display for EditMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A pipe's access (`parameters['access']`). No `access` key is the default: no owner, the team
/// Everyone, visibility `team`, edit `team` (anyone may see and change the pipe).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipeAccess {
    /// The owner's username.
    #[serde(default)]
    pub owner: Option<String>,
    /// The team's name (`None` is Everyone).
    #[serde(default)]
    pub team: Option<String>,
    /// Who can see the pipe.
    #[serde(default)]
    pub visibility: Visibility,
    /// Who can change it.
    #[serde(default)]
    pub edit: EditMode,
}

impl PipeAccess {
    /// The access in a pipe's parameters (the default without an `access` key).
    pub fn from_parameters(parameters: &Map) -> PipeAccess {
        PipeAccess::from_value(parameters.get(ACCESS_KEY))
    }

    /// The access of a pipe's parameters (`pipe.parameters` resolved).
    pub fn of_pipe(pipe: &Pipe) -> PipeAccess {
        PipeAccess::from_parameters(pipe.raw_parameters())
    }

    /// Read a stored `access` value leniently: a value that is not a map is the default; an
    /// unknown visibility reads as `private` and an unknown edit mode as `owner` (a typo never
    /// opens a pipe wider than intended). Writers check with [`PipeAccess::parse`].
    pub fn from_value(value: Option<&Value>) -> PipeAccess {
        let parsed;
        let map = match value {
            Some(Value::Map(m)) => m,
            Some(Value::Str(text)) => match Value::from_json_str(text) {
                Ok(Value::Map(m)) => {
                    parsed = m;
                    &parsed
                }
                _ => return PipeAccess::default(),
            },
            _ => return PipeAccess::default(),
        };
        let text = |key: &str| {
            map.get(key)
                .filter(|v| !v.is_null())
                .map(crate::pyfmt::py_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        PipeAccess {
            owner: text("owner"),
            team: normalize_team(text("team").as_deref()),
            visibility: match text("visibility") {
                None => Visibility::Team,
                Some(v) => Visibility::parse(&v).unwrap_or(Visibility::Private),
            },
            edit: match text("edit").or_else(|| text("edit_mode")) {
                None => EditMode::Team,
                Some(v) => EditMode::parse(&v).unwrap_or(EditMode::Owner),
            },
        }
    }

    /// Parse an `access` value strictly (what a writer sets): a map whose keys are `owner`,
    /// `team`, `visibility` and `edit`, with known visibility and edit names. Missing keys take
    /// the defaults.
    pub fn parse(value: &Value) -> Result<PipeAccess, String> {
        let map = match value {
            Value::Null => return Ok(PipeAccess::default()),
            Value::Map(m) => m,
            other => {
                return Err(format!(
                    "`access` must be a dictionary with `owner`, `team`, `visibility` and `edit`, not {}.",
                    other.type_name()
                ));
            }
        };
        for key in map.keys() {
            if !["owner", "team", "visibility", "edit", "edit_mode"].contains(&key.as_str()) {
                return Err(format!(
                    "Unknown `access` key '{key}': use `owner`, `team`, `visibility` and `edit`."
                ));
            }
        }
        if let Some(v) = map.get("visibility").filter(|v| !v.is_null())
            && Visibility::parse(&crate::pyfmt::py_str(v)).is_none()
        {
            return Err(format!(
                "Invalid visibility '{}': use `team`, `private` or `everyone`.",
                crate::pyfmt::py_str(v)
            ));
        }
        if let Some(v) = map
            .get("edit")
            .or_else(|| map.get("edit_mode"))
            .filter(|v| !v.is_null())
            && EditMode::parse(&crate::pyfmt::py_str(v)).is_none()
        {
            return Err(format!(
                "Invalid edit mode '{}': use `team` or `owner`.",
                crate::pyfmt::py_str(v)
            ));
        }
        Ok(PipeAccess::from_value(Some(value)))
    }

    /// The stored form: `{"owner", "team", "visibility", "edit"}` (`team` null for Everyone).
    pub fn to_value(&self) -> Value {
        let mut out = Map::new();
        out.insert("owner".into(), self.owner.clone().into());
        out.insert("team".into(), normalize_team(self.team.as_deref()).into());
        out.insert(
            "visibility".into(),
            Value::Str(self.visibility.name().into()),
        );
        out.insert("edit".into(), Value::Str(self.edit.name().into()));
        Value::Map(out)
    }

    /// The team's name (`Everyone` for none).
    pub fn team_name(&self) -> String {
        normalize_team(self.team.as_deref()).unwrap_or_else(|| EVERYONE.to_string())
    }

    /// Whether this is the default access (no owner, Everyone, `team`, `team`).
    pub fn is_default(&self) -> bool {
        self.owner.is_none()
            && is_everyone(self.team.as_deref())
            && self.visibility == Visibility::Team
            && self.edit == EditMode::Team
    }

    /// Whether `username` owns the pipe.
    pub fn is_owner(&self, username: &str) -> bool {
        !username.is_empty() && self.owner.as_deref() == Some(username)
    }

    /// Whether the pipe belongs to `team` (names compare without case; `None` is Everyone).
    pub fn in_team(&self, team: Option<&str>) -> bool {
        same_team(self.team.as_deref(), team)
    }
}

/// `parameters` with `access` replaced (`None` removes the key).
pub fn parameters_with_access(parameters: &Map, access: Option<&PipeAccess>) -> Map {
    let mut out = parameters.clone();
    match access {
        Some(a) => {
            out.insert(ACCESS_KEY.into(), a.to_value());
        }
        None => {
            out.shift_remove(ACCESS_KEY);
        }
    }
    out
}
