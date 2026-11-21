//! How each SQL dialect reads a text: its comments, string literals, dollar quotes and quoted
//! identifiers, the token stream of every reading ([`Readings`]), and the word and identifier
//! helpers the policies scan those tokens with.

/// One token of a SQL text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Token {
    /// An identifier: bare (`t`) or quoted (`"t"`, `` `t` ``, `[t]`), without its quotes.
    Ident {
        /// The name.
        name: String,
        /// Whether it was quoted.
        quoted: bool,
    },
    /// A string literal's contents (`'...'`, `E'...'`, `$tag$...$tag$`).
    Str(String),
    /// Any other character that is not whitespace.
    Punct(char),
}

/// How one SQL dialect delimits comments, strings and quoted identifiers.
#[derive(Debug, Clone, Copy)]
pub(super) struct Dialect {
    /// `#` starts a line comment (MySQL, MariaDB).
    hash_comments: bool,
    /// `--` starts a comment only before whitespace, a control character or the end (MySQL).
    dash_comments_need_space: bool,
    /// `/* /* */ */` is one comment (PostgreSQL, DuckDB, SQL Server).
    nested_comments: bool,
    /// A carriage return ends a line comment as a newline does (PostgreSQL, SQL Server).
    carriage_return_ends_comments: bool,
    /// `\` escapes the next character in `'...'` and `"..."` (MySQL; PostgreSQL with
    /// `standard_conforming_strings` off).
    backslash_escapes: bool,
    /// `E'...'` reads `\` escapes (PostgreSQL, DuckDB).
    escape_strings: bool,
    /// `E'...'`, `N'...'`, `B'...'` and `X'...'` are strings, prefix included (v4's statement
    /// scanner read the prefix as a word).
    string_prefixes: bool,
    /// `$$...$$` and `$tag$...$tag$` are strings (PostgreSQL, DuckDB).
    dollar_quotes: bool,
    /// `$` is part of an identifier, so a dollar quote cannot start right after a letter or
    /// digit (v4's statement scanner opened one after any character).
    dollar_in_identifiers: bool,
}

/// The readings every SQL text must pass.
pub(super) const DIALECTS: &[Dialect] = &[
    // v4's statement scanner (`strip_sql_noise`): the read-only check refuses whatever this
    // reading cannot lex, exactly as v4 did.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: false,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: false,
        dollar_quotes: true,
        dollar_in_identifiers: false,
    },
    // v4's table-name scanner.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // PostgreSQL, TimescaleDB, DuckDB.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: false,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // PostgreSQL with `standard_conforming_strings` off.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: true,
        escape_strings: true,
        string_prefixes: true,
        dollar_quotes: true,
        dollar_in_identifiers: true,
    },
    // MySQL, MariaDB.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: true,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: true,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // MySQL, MariaDB with `NO_BACKSLASH_ESCAPES`.
    Dialect {
        hash_comments: true,
        dash_comments_need_space: true,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // SQLite, Oracle.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: false,
        carriage_return_ends_comments: false,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
    // SQL Server.
    Dialect {
        hash_comments: false,
        dash_comments_need_space: false,
        nested_comments: true,
        carriage_return_ends_comments: true,
        backslash_escapes: false,
        escape_strings: false,
        string_prefixes: true,
        dollar_quotes: false,
        dollar_in_identifiers: true,
    },
];

/// The reading whose lexing errors the read-only check refuses (v4's statement scanner).
const STATEMENT_DIALECT: usize = 0;

const EXECUTABLE_COMMENT: &str =
    "Refusing an executable comment (`/*! ... */`), which MySQL runs as SQL.";

/// One reading of a SQL text.
pub(super) struct Reading {
    pub(super) tokens: Vec<Token>,
    /// Why this reading could not lex the text (its tokens then run to the end of the text).
    error: Option<&'static str>,
    /// Whether this reading saw an executable comment (`/*!` or MariaDB's `/*M!`).
    executable_comment: bool,
}

/// Every reading of one SQL text, in [`DIALECTS`] order.
pub(super) struct Readings(Vec<Reading>);

impl Readings {
    pub(super) fn new(sql: &str) -> Self {
        let chars: Vec<char> = sql.chars().collect();
        Self(DIALECTS.iter().map(|d| lex(&chars, d)).collect())
    }

    pub(super) fn tokens(&self) -> impl Iterator<Item = &[Token]> {
        self.0.iter().map(|r| r.tokens.as_slice())
    }

    /// Refuse an executable comment under any reading and, when `statements` is set, whatever
    /// the statement reading cannot lex.
    pub(super) fn lexing_error(&self, statements: bool) -> Option<&'static str> {
        if self.0.iter().any(|r| r.executable_comment) {
            return Some(EXECUTABLE_COMMENT);
        }
        if statements {
            return self.0[STATEMENT_DIALECT].error;
        }
        None
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

pub(super) fn is_ident_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// The length (in chars) of a dollar-quote tag at `chars[i..]` (`$$` or `$tag$`).
fn dollar_tag_len(chars: &[char], i: usize) -> Option<usize> {
    if chars.get(i) != Some(&'$') {
        return None;
    }
    if chars.get(i + 1) == Some(&'$') {
        return Some(2);
    }
    let first = *chars.get(i + 1)?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    let mut j = i + 2;
    while let Some(c) = chars.get(j) {
        if *c == '$' {
            return Some(j + 1 - i);
        }
        if !(c.is_ascii_alphanumeric() || *c == '_') {
            return None;
        }
        j += 1;
    }
    None
}

/// The index after the block comment that opens at `chars[i..]`, or `None` when it is
/// unterminated.
fn block_comment_end(chars: &[char], i: usize, nested: bool) -> Option<usize> {
    let n = chars.len();
    let mut depth = 1usize;
    let mut j = i + 2;
    while j < n {
        if nested && chars[j] == '/' && chars.get(j + 1) == Some(&'*') {
            depth += 1;
            j += 2;
        } else if chars[j] == '*' && chars.get(j + 1) == Some(&'/') {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return Some(j);
            }
        } else {
            j += 1;
        }
    }
    None
}

/// Tokenize `chars` as `dialect` reads it: comments are dropped; string literals and quoted
/// identifiers are read whole, and an unterminated one runs to the end of the text (recorded in
/// [`Reading::error`]).
pub(super) fn lex(chars: &[char], dialect: &Dialect) -> Reading {
    let n = chars.len();
    let ident_char = |c: char| is_ident_char(c) && (dialect.dollar_in_identifiers || c != '$');
    let mut out = Vec::new();
    let mut error = None;
    let mut executable_comment = false;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let dash_comment = c == '-'
            && chars.get(i + 1) == Some(&'-')
            && (!dialect.dash_comments_need_space
                || chars
                    .get(i + 2)
                    .is_none_or(|c| c.is_whitespace() || c.is_control()));
        if dash_comment || (c == '#' && dialect.hash_comments) {
            while i < n
                && chars[i] != '\n'
                && !(dialect.carriage_return_ends_comments && chars[i] == '\r')
            {
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            if chars.get(i + 2) == Some(&'!')
                || (chars.get(i + 2) == Some(&'M') && chars.get(i + 3) == Some(&'!'))
            {
                executable_comment = true;
            }
            match block_comment_end(chars, i, dialect.nested_comments) {
                Some(end) => i = end,
                None => {
                    error.get_or_insert("Unterminated block comment.");
                    i = n;
                }
            }
            continue;
        }
        // String literals, with an `E` / `N` / `B` / `X` prefix read as part of them.
        let prefixed = dialect.string_prefixes
            && matches!(c, 'e' | 'E' | 'n' | 'N' | 'b' | 'B' | 'x' | 'X')
            && chars.get(i + 1) == Some(&'\'')
            && (i == 0 || !ident_char(chars[i - 1]));
        if c == '\'' || prefixed {
            let escapes = dialect.backslash_escapes
                || (dialect.escape_strings && prefixed && matches!(c, 'e' | 'E'));
            i += if prefixed { 2 } else { 1 };
            let (text, end) = quoted(chars, i, '\'', escapes);
            if end.is_none() {
                error.get_or_insert("Unterminated string literal.");
            }
            out.push(Token::Str(text));
            i = end.unwrap_or(n);
            continue;
        }
        // Dollar-quoted strings: `$$...$$` or `$tag$...$tag$`.
        if dialect.dollar_quotes
            && let Some(len) = dollar_tag_len(chars, i)
            && (!dialect.dollar_in_identifiers || i == 0 || !ident_char(chars[i - 1]))
        {
            let tag = &chars[i..i + len];
            let start = i + len;
            let end = (start..=n.saturating_sub(len))
                .find(|&j| j + len <= n && &chars[j..j + len] == tag);
            match end {
                Some(end) => {
                    out.push(Token::Str(chars[start..end].iter().collect()));
                    i = end + len;
                }
                None => {
                    error.get_or_insert("Unterminated dollar-quoted string.");
                    out.push(Token::Str(chars[start.min(n)..].iter().collect()));
                    i = n;
                }
            }
            continue;
        }
        // Quoted identifiers: "..." (`""` doubles a quote), `...` (`` `` `` doubles one), [...].
        if c == '"' || c == '`' || c == '[' {
            let (name, end) = if c == '[' {
                match chars[i + 1..].iter().position(|&ch| ch == ']') {
                    Some(p) => (chars[i + 1..i + 1 + p].iter().collect(), Some(i + 2 + p)),
                    None => (chars[i + 1..].iter().collect(), None),
                }
            } else {
                quoted(chars, i + 1, c, c == '"' && dialect.backslash_escapes)
            };
            if end.is_none() {
                error.get_or_insert("Unterminated quoted identifier.");
            }
            out.push(Token::Ident { name, quoted: true });
            i = end.unwrap_or(n);
            continue;
        }
        if is_ident_start(c) {
            let start = i;
            i += 1;
            while i < n && ident_char(chars[i]) {
                i += 1;
            }
            out.push(Token::Ident {
                name: chars[start..i].iter().collect(),
                quoted: false,
            });
            continue;
        }
        out.push(Token::Punct(c));
        i += 1;
    }
    Reading {
        tokens: out,
        error,
        executable_comment,
    }
}

/// The contents of a literal opened before `chars[start]` and closed by `close` (doubled, it
/// stands for itself; with `escapes`, `\` escapes the next character), and the index after it
/// (`None`: unterminated).
fn quoted(chars: &[char], start: usize, close: char, escapes: bool) -> (String, Option<usize>) {
    let n = chars.len();
    let mut text = String::new();
    let mut i = start;
    while i < n {
        let ch = chars[i];
        if escapes && ch == '\\' {
            if i + 1 < n {
                text.push(chars[i + 1]);
                i += 2;
                continue;
            }
            return (text, None);
        }
        if ch == close {
            if chars.get(i + 1) == Some(&close) {
                text.push(close);
                i += 2;
                continue;
            }
            return (text, Some(i + 1));
        }
        text.push(ch);
        i += 1;
    }
    (text, None)
}

pub(super) fn identifier_chains(
    toks: &[Token],
    normalize: impl Fn(&str) -> String,
) -> Vec<Vec<String>> {
    let mut chains: Vec<Vec<String>> = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        if let Token::Ident { name, .. } = &toks[i] {
            let mut chain = vec![normalize(name)];
            let mut j = i + 1;
            while j + 1 < toks.len()
                && toks[j] == Token::Punct('.')
                && let Token::Ident { name, .. } = &toks[j + 1]
            {
                chain.push(normalize(name));
                j += 2;
            }
            chains.push(chain);
            i = j;
            continue;
        }
        i += 1;
    }
    chains
}

/// The ASCII words (`[A-Za-z_][A-Za-z0-9_]*`) of `text`, as `(start, end)` byte offsets.
pub(super) fn ascii_word_spans(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            out.push((start, i));
        } else {
            i += 1;
        }
    }
    out
}

/// The ASCII words of a reading's unquoted identifiers, as v4's statement check reads them.
pub(super) fn unquoted_words(tokens: &[Token]) -> impl Iterator<Item = &str> {
    tokens.iter().flat_map(|t| match t {
        Token::Ident {
            name,
            quoted: false,
        } => ascii_word_spans(name)
            .into_iter()
            .map(|(start, end)| &name[start..end])
            .collect(),
        _ => Vec::new(),
    })
}

/// Whether `text` contains `word` with no identifier character on either side.
pub(super) fn contains_word(text: &str, word: &str) -> bool {
    if word.is_empty() {
        return false;
    }
    let mut from = 0;
    while let Some(pos) = text[from..].find(word) {
        let start = from + pos;
        let end = start + word.len();
        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        if !before.is_some_and(is_ident_char) && !after.is_some_and(is_ident_char) {
            return true;
        }
        from = start + word.chars().next().map_or(1, char::len_utf8);
    }
    false
}

/// Whether `rest`, the text after a name, calls it: `(` after the closing quote of a quoted name,
/// whitespace and comments (block comments nest, as in PostgreSQL and DuckDB).
pub(super) fn opens_call(rest: &str) -> bool {
    let mut rest = rest.strip_prefix(['"', '`', ']']).unwrap_or(rest);
    loop {
        rest = rest.trim_start();
        if let Some(after) = rest.strip_prefix("--") {
            rest = after.split_once('\n').map_or("", |(_, next)| next);
        } else if rest.starts_with("/*") {
            let bytes = rest.as_bytes();
            let (mut depth, mut j, mut end) = (0usize, 0usize, rest.len());
            while j + 1 < bytes.len() {
                match &bytes[j..j + 2] {
                    b"/*" => {
                        depth += 1;
                        j += 2;
                    }
                    b"*/" => {
                        depth -= 1;
                        j += 2;
                        if depth == 0 {
                            end = j;
                            break;
                        }
                    }
                    _ => j += 1,
                }
            }
            rest = &rest[end..];
        } else {
            return rest.starts_with('(');
        }
    }
}
