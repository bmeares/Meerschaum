//! The switch's edit of `stack.yaml` as text: only the lines of the `db` image, its data volume
//! entry and that volume's declaration change; comments, order, quoting and every other line stay
//! byte for byte.
//!
//! This reads block-style YAML (what Meerschaum writes, and what people write by hand). Anything
//! else (flow style for the edited parts, anchors, tabs, multi-line scalars there) is left to the
//! caller, which writes the whole config instead; the caller also checks that the edited text
//! means exactly the original with the switch applied.

use super::stackcfg::Switch;

/// One line of the file.
#[derive(Debug, Clone)]
struct Line {
    /// The text without its line ending.
    text: String,
    /// The line ending (`\n`, `\r\n`, or nothing for a last line without one).
    ending: String,
}

impl Line {
    fn indent(&self) -> usize {
        self.text.len() - self.text.trim_start_matches(' ').len()
    }

    /// Blank, or a comment only.
    fn is_content(&self) -> bool {
        let t = self.text.trim_start();
        !t.is_empty() && !t.starts_with('#')
    }

    /// A block sequence item (`- ...`, or a lone `-`).
    fn is_item(&self) -> bool {
        let t = self.text.trim_start();
        t == "-" || t.starts_with("- ")
    }
}

fn split_lines(text: &str) -> Vec<Line> {
    let mut out = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        match rest.find('\n') {
            Some(i) => {
                let (line, ending) = if i > 0 && rest.as_bytes()[i - 1] == b'\r' {
                    (&rest[..i - 1], "\r\n")
                } else {
                    (&rest[..i], "\n")
                };
                out.push(Line {
                    text: line.to_string(),
                    ending: ending.to_string(),
                });
                rest = &rest[i + 1..];
            }
            None => {
                out.push(Line {
                    text: rest.to_string(),
                    ending: String::new(),
                });
                rest = "";
            }
        }
    }
    out
}

/// A scalar as written: its text, how it is quoted, and what follows it (a comment).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Scalar {
    value: String,
    quote: Option<char>,
    /// Everything after the scalar (spaces and a comment), kept as it is.
    trailer: String,
}

/// A quoted scalar at the start of `text`: its value, its quote and how many bytes it takes.
fn read_quoted(text: &str) -> Option<(String, char, usize)> {
    let quote = text.chars().next()?;
    let mut value = String::new();
    match quote {
        '"' => {
            let mut chars = text.char_indices().skip(1);
            while let Some((i, c)) = chars.next() {
                match c {
                    '\\' => {
                        let (_, escaped) = chars.next()?;
                        match escaped {
                            '"' | '\\' | '/' => value.push(escaped),
                            'n' => value.push('\n'),
                            't' => value.push('\t'),
                            _ => return None,
                        }
                    }
                    '"' => return Some((value, '"', i + 1)),
                    c => value.push(c),
                }
            }
            None
        }
        '\'' => {
            let mut chars = text.char_indices().skip(1).peekable();
            while let Some((i, c)) = chars.next() {
                if c == '\'' {
                    if chars.peek().map(|(_, c)| *c) == Some('\'') {
                        chars.next();
                        value.push('\'');
                        continue;
                    }
                    return Some((value, '\'', i + 1));
                }
                value.push(c);
            }
            None
        }
        _ => None,
    }
}

/// Read the scalar at the start of `text` (`None` for anything this does not edit: flow
/// collections, anchors, aliases, tags, block scalars).
fn read_scalar(text: &str) -> Option<Scalar> {
    let first = text.chars().next()?;
    match first {
        '"' | '\'' => {
            let (value, quote, used) = read_quoted(text)?;
            let trailer = &text[used..];
            trailer_ok(trailer).then(|| Scalar {
                value,
                quote: Some(quote),
                trailer: trailer.to_string(),
            })
        }
        '[' | '{' | '&' | '*' | '!' | '|' | '>' | '%' | '@' | '`' => None,
        _ => {
            // A plain scalar ends at ` #` (a comment).
            let end = text.find(" #").unwrap_or(text.len());
            let value = text[..end].trim_end();
            if value.is_empty() {
                return None;
            }
            Some(Scalar {
                value: value.to_string(),
                quote: None,
                trailer: text[value.len()..].to_string(),
            })
        }
    }
}

/// What may follow a scalar on its line: spaces, then nothing or a comment.
fn trailer_ok(trailer: &str) -> bool {
    let t = trailer.trim_start();
    t.is_empty() || (t.starts_with('#') && trailer.len() > t.len())
}

/// Whether `value` can be written as a plain scalar that reads back as the same string.
fn plain_ok(value: &str) -> bool {
    let Some(first) = value.chars().next() else {
        return false;
    };
    if "-?:,[]{}#&*!|>'\"%@`".contains(first) || first.is_whitespace() {
        return false;
    }
    if value.ends_with(' ') || value.ends_with(':') {
        return false;
    }
    if value.contains(": ") || value.contains(" #") || value.contains('\n') {
        return false;
    }
    // Words YAML reads as something else than a string.
    let lower = value.to_ascii_lowercase();
    if [
        "null", "~", "true", "false", "yes", "no", "on", "off", "y", "n",
    ]
    .contains(&lower.as_str())
    {
        return false;
    }
    !value
        .chars()
        .all(|c| c.is_ascii_digit() || ".+-_eE".contains(c))
}

/// Write `value` the way `like` was written (its quotes), keeping what followed it.
fn write_scalar(value: &str, like: &Scalar) -> String {
    let text = match like.quote {
        Some('"') => format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")),
        Some(_) => format!("'{}'", value.replace('\'', "''")),
        None if plain_ok(value) => value.to_string(),
        None => format!("'{}'", value.replace('\'', "''")),
    };
    format!("{text}{}", like.trailer)
}

/// A mapping key line: the key, where its value starts (byte offset in the line) and the
/// inline value (`None`: the value is a block below).
#[derive(Debug, Clone)]
struct KeyLine {
    key: Scalar,
    /// Byte offset of the key in the line.
    key_at: usize,
    /// Byte offset of the inline value, when there is one.
    value_at: Option<usize>,
}

/// Read `key: value` starting at byte `at` of `line`.
fn read_key(line: &str, at: usize) -> Option<KeyLine> {
    let text = &line[at..];
    let (key, after) = match text.chars().next()? {
        '"' | '\'' => {
            let (value, quote, used) = read_quoted(text)?;
            (
                Scalar {
                    value,
                    quote: Some(quote),
                    trailer: String::new(),
                },
                used,
            )
        }
        _ => {
            let colon = find_key_colon(text)?;
            (
                Scalar {
                    value: text[..colon].trim_end().to_string(),
                    quote: None,
                    trailer: String::new(),
                },
                colon,
            )
        }
    };
    let rest = &text[after..];
    let rest = rest.strip_prefix(':')?;
    if !(rest.is_empty() || rest.starts_with(' ')) {
        return None;
    }
    let value_offset = rest.len() - rest.trim_start().len();
    let inline = rest.trim_start();
    let value_at =
        (!inline.is_empty() && !inline.starts_with('#')).then(|| at + after + 1 + value_offset);
    Some(KeyLine {
        key,
        key_at: at,
        value_at,
    })
}

/// The `:` that ends a plain key (followed by a space or the end of the line).
fn find_key_colon(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'#' && i > 0 && bytes[i - 1] == b' ' {
            return None;
        }
        if *b == b':' && (i + 1 == bytes.len() || bytes[i + 1] == b' ') {
            return (i > 0).then_some(i);
        }
    }
    None
}

/// The document as lines, with the lookups the edit needs.
struct Doc {
    lines: Vec<Line>,
}

impl Doc {
    /// The end (exclusive) of the block of the key line `at` (indent `indent`): up to the next
    /// content line that is less indented, or as indented and not a sequence item of an
    /// indentless block sequence.
    fn block_end(&self, at: usize, indent: usize) -> usize {
        let mut i = at + 1;
        while i < self.lines.len() {
            let line = &self.lines[i];
            if line.is_content() {
                let n = line.indent();
                if n < indent || (n == indent && !line.is_item()) {
                    break;
                }
            }
            i += 1;
        }
        // Trailing blanks and comments belong to what follows.
        while i > at + 1 && !self.lines[i - 1].is_content() {
            i -= 1;
        }
        i
    }

    /// The direct child key `key` of the block `start..end` (whose content is indented more than
    /// `parent_indent`): its line index and key line.
    fn child(
        &self,
        start: usize,
        end: usize,
        parent_indent: Option<usize>,
        key: &str,
    ) -> Option<(usize, KeyLine)> {
        let child_indent = self.lines[start..end]
            .iter()
            .find(|l| l.is_content())
            .map(Line::indent)?;
        if parent_indent.is_some_and(|p| child_indent <= p) {
            return None;
        }
        let mut found = None;
        for (i, line) in self.lines.iter().enumerate().take(end).skip(start) {
            if !line.is_content() || line.indent() != child_indent || line.is_item() {
                continue;
            }
            let parsed = read_key(&line.text, child_indent)?;
            if parsed.key.value == key {
                if found.is_some() {
                    // A duplicate key: not something to edit by line.
                    return None;
                }
                found = Some((i, parsed));
            }
        }
        found
    }

    /// The items of the block sequence under the key line `at` (indent `indent`): for each, the
    /// line of its `-`, the byte offset of its content, and the end of the item.
    fn items(&self, at: usize, indent: usize) -> Option<Vec<(usize, usize, usize)>> {
        let end = self.block_end(at, indent);
        let starts: Vec<usize> = (at + 1..end)
            .filter(|i| self.lines[*i].is_content())
            .collect();
        let first = *starts.first()?;
        let dash_indent = self.lines[first].indent();
        if !self.lines[first].is_item() || dash_indent < indent {
            return None;
        }
        let mut items: Vec<(usize, usize, usize)> = Vec::new();
        for i in starts {
            let line = &self.lines[i];
            let n = line.indent();
            if n == dash_indent {
                if !line.is_item() {
                    return None;
                }
                let rest = &line.text[n + 1..];
                let content_at = n + 1 + (rest.len() - rest.trim_start().len());
                if let Some(last) = items.last_mut() {
                    last.2 = i;
                }
                items.push((i, content_at, end));
            } else if n < dash_indent {
                return None;
            }
        }
        if let Some(last) = items.last_mut() {
            last.2 = end;
        }
        Some(items)
    }

    fn replace_value(&mut self, at: usize, value_at: usize, value: &str) -> Option<()> {
        let line = &self.lines[at].text;
        let scalar = read_scalar(&line[value_at..])?;
        let new = format!("{}{}", &line[..value_at], write_scalar(value, &scalar));
        self.lines[at].text = new;
        Some(())
    }

    fn text(&self) -> String {
        self.lines
            .iter()
            .map(|l| format!("{}{}", l.text, l.ending))
            .collect()
    }
}

/// `source:target[:options]` of a short volume entry.
fn split_short(entry: &str) -> Option<(&str, &str, Option<&str>)> {
    let mut parts = entry.splitn(3, ':');
    let source = parts.next()?;
    let target = parts.next()?;
    Some((source, target, parts.next()))
}

fn same_target(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

/// Edit `text` (a `stack.yaml`) for `switch`: `shared` keeps the old volume's declaration (another
/// service mounts it) and adds the new one after it. `None` when the text is not in a form this
/// edits.
pub fn edit(text: &str, switch: &Switch, shared: bool) -> Option<String> {
    if text.contains('\t') {
        return None;
    }
    let mut doc = Doc {
        lines: split_lines(text),
    };
    let total = doc.lines.len();
    let (compose_at, compose) = doc.child(0, total, None, "docker-compose.yaml")?;
    if compose.value_at.is_some() {
        return None;
    }
    let compose_indent = doc.lines[compose_at].indent();
    let compose_end = doc.block_end(compose_at, compose_indent);
    let (services_at, services) = doc.child(
        compose_at + 1,
        compose_end,
        Some(compose_indent),
        "services",
    )?;
    if services.value_at.is_some() {
        return None;
    }
    let services_indent = doc.lines[services_at].indent();
    let services_end = doc.block_end(services_at, services_indent);
    let (db_at, db) = doc.child(services_at + 1, services_end, Some(services_indent), "db")?;
    if db.value_at.is_some() {
        return None;
    }
    let db_indent = doc.lines[db_at].indent();
    let db_end = doc.block_end(db_at, db_indent);

    // 1. The image.
    let (image_at, image) = doc.child(db_at + 1, db_end, Some(db_indent), "image")?;
    let image_value_at = image.value_at?;
    doc.replace_value(image_at, image_value_at, &switch.image)?;

    // 2. The data volume entry.
    let (volumes_at, volumes) = doc.child(db_at + 1, db_end, Some(db_indent), "volumes")?;
    if volumes.value_at.is_some() {
        return None;
    }
    let volumes_indent = doc.lines[volumes_at].indent();
    let mut edited_entry = false;
    for (item_at, content_at, item_end) in doc.items(volumes_at, volumes_indent)? {
        let content = doc.lines[item_at].text[content_at..].to_string();
        if let Some(key) = read_key(&doc.lines[item_at].text, content_at) {
            // A long-syntax entry: `- type: volume` with `source:` and `target:`.
            let item_indent = content_at;
            let field = |doc: &Doc, name: &str| -> Option<(usize, KeyLine)> {
                if key.key.value == name {
                    return Some((item_at, key.clone()));
                }
                doc.child(item_at + 1, item_end, Some(item_indent - 1), name)
                    .filter(|_| {
                        doc.lines[item_at + 1..item_end]
                            .iter()
                            .find(|l| l.is_content())
                            .is_some_and(|l| l.indent() == item_indent)
                    })
            };
            let (Some((source_at, source)), Some((target_at, target))) =
                (field(&doc, "source"), field(&doc, "target"))
            else {
                continue;
            };
            let source_value = read_scalar(&doc.lines[source_at].text[source.value_at?..])?.value;
            let target_value = read_scalar(&doc.lines[target_at].text[target.value_at?..])?.value;
            if source_value != switch.old_key || !same_target(&target_value, &switch.old_target) {
                continue;
            }
            if edited_entry {
                return None;
            }
            doc.replace_value(source_at, source.value_at?, &switch.new_key)?;
            doc.replace_value(target_at, target.value_at?, &switch.new_target)?;
            edited_entry = true;
        } else {
            let scalar = read_scalar(&content)?;
            let Some((source, target, options)) = split_short(&scalar.value) else {
                continue;
            };
            if source != switch.old_key || !same_target(target, &switch.old_target) {
                continue;
            }
            if edited_entry {
                return None;
            }
            let new_value = match options {
                Some(o) => format!("{}:{}:{o}", switch.new_key, switch.new_target),
                None => format!("{}:{}", switch.new_key, switch.new_target),
            };
            doc.replace_value(item_at, content_at, &new_value)?;
            edited_entry = true;
        }
    }
    if !edited_entry {
        return None;
    }

    // 3. The top-level declaration.
    let (top_at, top) = doc.child(compose_at + 1, compose_end, Some(compose_indent), "volumes")?;
    if top.value_at.is_some() {
        return None;
    }
    let top_indent = doc.lines[top_at].indent();
    let top_end = doc.block_end(top_at, top_indent);
    let (decl_at, decl) = doc.child(top_at + 1, top_end, Some(top_indent), &switch.old_key)?;
    let decl_indent = doc.lines[decl_at].indent();
    let decl_end = doc.block_end(decl_at, decl_indent).max(decl_at + 1);
    let mut block: Vec<Line> = doc.lines[decl_at..decl_end].to_vec();
    // The key, renamed (quoted as it was).
    {
        let line = &block[0].text;
        let key_text_end = {
            let rest = &line[decl.key_at..];
            match decl.key.quote {
                Some(_) => decl.key_at + read_quoted(rest)?.2,
                None => decl.key_at + find_key_colon(rest)?,
            }
        };
        let renamed = write_scalar(
            &switch.new_key,
            &Scalar {
                trailer: String::new(),
                ..decl.key.clone()
            },
        );
        block[0].text = format!("{}{renamed}{}", &line[..decl.key_at], &line[key_text_end..]);
    }
    if let Some(name) = &switch.new_declared_name {
        let mini = Doc {
            lines: block.clone(),
        };
        let (name_at, name_key) = mini.child(1, block.len(), Some(decl_indent), "name")?;
        let mut mini = mini;
        mini.replace_value(name_at, name_key.value_at?, name)?;
        block = mini.lines;
    }
    if shared {
        // The old declaration stays; the new one follows it.
        if block.last().is_some_and(|l| l.ending.is_empty()) {
            let ending = doc.lines[decl_at].ending.clone();
            let last = decl_end - 1;
            if doc.lines[last].ending.is_empty() {
                doc.lines[last].ending = if ending.is_empty() {
                    "\n".into()
                } else {
                    ending
                };
            }
        }
        doc.lines.splice(decl_end..decl_end, block);
    } else {
        doc.lines.splice(decl_at..decl_end, block);
    }
    Some(doc.text())
}

/// The lines that differ between two texts of the same length in lines (the edit replaces lines
/// in place, and adds a block only for a shared volume): `(before, after)`.
pub fn changed_lines(before: &str, after: &str) -> Vec<(String, String)> {
    let a = split_lines(before);
    let b = split_lines(after);
    if a.len() == b.len() {
        return a
            .iter()
            .zip(b.iter())
            .filter(|(x, y)| x.text != y.text)
            .map(|(x, y)| (x.text.trim().to_string(), y.text.trim().to_string()))
            .collect();
    }
    // Added lines (a shared volume's new declaration): list them as additions.
    let mut out: Vec<(String, String)> = a
        .iter()
        .zip(b.iter())
        .filter(|(x, y)| x.text != y.text)
        .map(|(x, y)| (x.text.trim().to_string(), y.text.trim().to_string()))
        .collect();
    let old: std::collections::BTreeSet<&str> = a.iter().map(|l| l.text.as_str()).collect();
    for line in &b {
        if !old.contains(line.text.as_str()) && !out.iter().any(|(_, n)| n == line.text.trim()) {
            out.push((String::new(), line.text.trim().to_string()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn switch() -> Switch {
        Switch {
            image: "timescale/timescaledb-ha:pg18".into(),
            old_key: "meerschaum_db_data".into(),
            old_target: "/home/postgres/pgdata".into(),
            new_key: "meerschaum_db_data_pg18".into(),
            new_target: "/home/postgres/pgdata".into(),
            new_declared_name: None,
        }
    }

    /// The layout `mrsm` writes (indentless sequences), with comments added by hand.
    const WRITTEN: &str = "\
# My stack.
project_name: mrsm

docker-compose.yaml:
  services:
    db:  # the database
      environment:
        POSTGRES_USER: <DOLLAR>POSTGRES_USER
      image: timescale/timescaledb-ha:pg17   # pinned on purpose
      ports:
      - 5432:5432
      volumes:
      - meerschaum_db_data:/home/postgres/pgdata
      - /root/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro
      shm_size: 1024m
    api:
      image: bmeares/meerschaum:api
      volumes:
      - api_root:/meerschaum
  volumes:
    api_root: null
    # The database.
    meerschaum_db_data: null
    grafana_storage: null

filetype: yaml
";

    #[test]
    fn only_the_three_lines_change_and_comments_stay() {
        let edited = edit(WRITTEN, &switch(), false).unwrap();
        let changed = changed_lines(WRITTEN, &edited);
        assert_eq!(
            changed,
            vec![
                (
                    "image: timescale/timescaledb-ha:pg17   # pinned on purpose".to_string(),
                    "image: timescale/timescaledb-ha:pg18   # pinned on purpose".to_string()
                ),
                (
                    "- meerschaum_db_data:/home/postgres/pgdata".to_string(),
                    "- meerschaum_db_data_pg18:/home/postgres/pgdata".to_string()
                ),
                (
                    "meerschaum_db_data: null".to_string(),
                    "meerschaum_db_data_pg18: null".to_string()
                ),
            ]
        );
        assert!(edited.starts_with("# My stack.\nproject_name: mrsm\n"));
        assert!(edited.contains("    db:  # the database\n"));
        assert!(edited.contains("    # The database.\n    meerschaum_db_data_pg18: null\n"));
    }

    #[test]
    fn indented_sequences_quotes_and_long_syntax() {
        let text = "\
docker-compose.yaml:
    services:
        db:
            image: \"postgres:17\"
            volumes:
                - type: volume
                  source: 'pg'
                  target: /var/lib/postgresql/data
                - type: bind
                  source: ./init
                  target: /docker-entrypoint-initdb.d
    volumes:
        pg:
            name: shared_pg   # keep this name
";
        let sw = Switch {
            image: "postgres:18".into(),
            old_key: "pg".into(),
            old_target: "/var/lib/postgresql/data".into(),
            new_key: "pg_pg18".into(),
            new_target: "/var/lib/postgresql".into(),
            new_declared_name: Some("shared_pg_pg18".into()),
        };
        let edited = edit(text, &sw, false).unwrap();
        assert_eq!(
            edited,
            "\
docker-compose.yaml:
    services:
        db:
            image: \"postgres:18\"
            volumes:
                - type: volume
                  source: 'pg_pg18'
                  target: /var/lib/postgresql
                - type: bind
                  source: ./init
                  target: /docker-entrypoint-initdb.d
    volumes:
        pg_pg18:
            name: shared_pg_pg18   # keep this name
"
        );
    }

    #[test]
    fn a_shared_volume_keeps_its_declaration() {
        let text = "\
docker-compose.yaml:
  services:
    db:
      image: postgres:17
      volumes:
      - pg:/var/lib/postgresql/data
    backup:
      volumes:
      - pg:/data:ro
  volumes:
    pg: {}
    other: null
";
        let sw = Switch {
            image: "postgres:18".into(),
            old_key: "pg".into(),
            old_target: "/var/lib/postgresql/data".into(),
            new_key: "pg_pg18".into(),
            new_target: "/var/lib/postgresql".into(),
            new_declared_name: None,
        };
        let edited = edit(text, &sw, true).unwrap();
        assert!(edited.contains("  volumes:\n    pg: {}\n    pg_pg18: {}\n    other: null\n"));
        assert!(edited.contains("      - pg_pg18:/var/lib/postgresql\n    backup:"));
        assert!(edited.contains("      - pg:/data:ro\n"));
    }

    #[test]
    fn forms_this_does_not_edit_are_left_to_the_caller() {
        // Flow style.
        assert!(edit("{\"docker-compose.yaml\": {}}\n", &switch(), false).is_none());
        let flow_volumes = WRITTEN.replace(
            "      volumes:\n      - meerschaum_db_data:/home/postgres/pgdata\n      - /root/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro\n",
            "      volumes: [meerschaum_db_data:/home/postgres/pgdata]\n",
        );
        assert!(edit(&flow_volumes, &switch(), false).is_none());
        // An anchor on the image.
        let anchored = WRITTEN.replace(
            "image: timescale/timescaledb-ha:pg17",
            "image: &img timescale/timescaledb-ha:pg17",
        );
        assert!(edit(&anchored, &switch(), false).is_none());
        // The volume entry is not there.
        let other = WRITTEN.replace(
            "- meerschaum_db_data:/home/postgres/pgdata",
            "- elsewhere:/home/postgres/pgdata",
        );
        assert!(edit(&other, &switch(), false).is_none());
    }

    #[test]
    fn windows_line_endings_and_a_last_line_without_one_stay() {
        let text = WRITTEN.replace('\n', "\r\n");
        let text = text.trim_end_matches("\r\n").to_string();
        let edited = edit(&text, &switch(), false).unwrap();
        assert_eq!(changed_lines(&text, &edited).len(), 3);
        assert!(edited.contains("meerschaum_db_data_pg18: null\r\n"));
        assert!(edited.ends_with("filetype: yaml"));
    }

    #[test]
    fn scalars_keep_their_quotes_and_plain_values_stay_plain_when_safe() {
        let like = |quote| Scalar {
            value: String::new(),
            quote,
            trailer: " # c".into(),
        };
        assert_eq!(write_scalar("a:b", &like(None)), "a:b # c");
        assert_eq!(write_scalar("yes", &like(None)), "'yes' # c");
        assert_eq!(write_scalar("a: b", &like(None)), "'a: b' # c");
        assert_eq!(write_scalar("it's", &like(Some('\''))), "'it''s' # c");
        assert_eq!(write_scalar("x\"y", &like(Some('"'))), "\"x\\\"y\" # c");
        assert_eq!(
            read_scalar("'it''s' # c"),
            Some(Scalar {
                value: "it's".into(),
                quote: Some('\''),
                trailer: " # c".into()
            })
        );
        assert_eq!(
            read_scalar("plain value  # c").unwrap().value,
            "plain value"
        );
        assert!(read_scalar("[a, b]").is_none());
        assert!(read_scalar("'unterminated").is_none());
    }
}
