//! A port of CPython 3.14 `argparse.ArgumentParser.parse_known_args` for the argument table.
//!
//! clap cannot reproduce `argparse`'s observable behavior, which v4 sysargs (jobs, compose files,
//! shell history) depend on: positionals consumed only in the first contiguous run (`show --debug
//! pipes` drops `pipes`), unknown options silently returned as extras, greedy `nargs='+'`
//! (`-c a b show` swallows `show`), last-wins `store` for repeated list flags (`-t a -t b` keeps
//! `['b']`), prefix abbreviations (`--deb`), combined short flags (`-yf`), attached values
//! (`-p8224`, `--name=x`), `REMAINDER` (`-A`), and the `--` separator. This module follows
//! `Lib/argparse.py` (`_parse_known_args`, `consume_optional`, `consume_positionals`,
//! `_parse_optional`, `_get_option_tuples`, `_get_nargs_pattern`, `_get_values`) branch for
//! branch, with the patterns (`'A'`, `'O'`, `'-'`) matched by hand instead of with `re`.

use indexmap::IndexMap;

use crate::spec::{Action, ArgSpec, ArgType, Nargs};
use crate::types::{self, ConvError, DateContext};
use crate::value::{PyException, PyValue};

/// The namespace (`vars(args)`), in `parser._actions` order.
pub type Namespace = IndexMap<String, PyValue>;

/// A parse failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// `argparse.ArgumentError`. Meerschaum's parser turns it into `InvalidArgumentException`.
    Argument {
        /// The offending argument's display name (`-p/--port`), if any.
        argument: Option<String>,
        /// The message.
        message: String,
    },
    /// Any other exception raised by a `type=` callable.
    Raise(PyException),
}

impl ParseError {
    fn argument(spec: Option<&ArgSpec>, message: impl Into<String>) -> Self {
        ParseError::Argument {
            argument: spec.map(ArgSpec::display_name),
            message: message.into(),
        }
    }

    /// The exception `parse_arguments` stores under `_argparse_exception`.
    pub fn into_exception(self) -> PyException {
        match self {
            ParseError::Argument { argument, message } => PyException::new(
                "InvalidArgumentException",
                match argument {
                    Some(name) => format!("argument {name}: {message}"),
                    None => message,
                },
            ),
            ParseError::Raise(e) => e,
        }
    }
}

#[derive(Debug, Clone)]
struct OptTuple {
    action: Option<usize>,
    option_string: String,
    sep: Option<String>,
    explicit_arg: Option<String>,
}

/// An `argparse` parser over a static argument table.
pub struct Parser {
    specs: &'static [ArgSpec],
    /// `_option_string_actions`, in insertion order.
    option_strings: IndexMap<&'static str, usize>,
}

fn is_negative_number(s: &str) -> bool {
    // `^-\d+$|^-\d*\.\d+$`
    let Some(rest) = s.strip_prefix('-') else {
        return false;
    };
    let is_digit = |c: char| c.is_ascii_digit() || (!c.is_ascii() && c.is_numeric());
    if !rest.is_empty() && rest.chars().all(is_digit) {
        return true;
    }
    if let Some((a, b)) = rest.split_once('.') {
        return a.chars().all(is_digit) && !b.is_empty() && b.chars().all(is_digit);
    }
    false
}

impl Parser {
    /// Build a parser over a table.
    pub fn new(specs: &'static [ArgSpec]) -> Self {
        let mut option_strings = IndexMap::new();
        for (i, spec) in specs.iter().enumerate() {
            for opt in spec.option_strings {
                option_strings.insert(*opt, i);
            }
        }
        Self {
            specs,
            option_strings,
        }
    }

    /// The argument table.
    pub fn specs(&self) -> &'static [ArgSpec] {
        self.specs
    }

    /// `_parse_optional`: `None` for a positional, else the candidate interpretations.
    fn parse_optional(&self, arg: &str) -> Option<Vec<OptTuple>> {
        let first = arg.chars().next()?;
        if first != '-' {
            return None;
        }
        if let Some(&ix) = self.option_strings.get(arg) {
            return Some(vec![OptTuple {
                action: Some(ix),
                option_string: arg.to_string(),
                sep: None,
                explicit_arg: None,
            }]);
        }
        if arg.chars().count() == 1 {
            return None;
        }
        if let Some((option_string, explicit)) = arg.split_once('=')
            && let Some(&ix) = self.option_strings.get(option_string)
        {
            return Some(vec![OptTuple {
                action: Some(ix),
                option_string: option_string.to_string(),
                sep: Some("=".into()),
                explicit_arg: Some(explicit.to_string()),
            }]);
        }
        let tuples = self.get_option_tuples(arg);
        if !tuples.is_empty() {
            return Some(tuples);
        }
        // There are no negative-number-like options, so negative numbers are positionals.
        if is_negative_number(arg) {
            return None;
        }
        if arg.contains(' ') {
            return None;
        }
        Some(vec![OptTuple {
            action: None,
            option_string: arg.to_string(),
            sep: None,
            explicit_arg: None,
        }])
    }

    /// `_get_option_tuples` (with `allow_abbrev=True`).
    ///
    /// PARITY: every prefix match counts, even aliases of the same action, so `--datetime-dt`
    /// is ambiguous between `--datetime-dtypes` and `--datetime-dtype`.
    fn get_option_tuples(&self, option_string: &str) -> Vec<OptTuple> {
        let mut result = Vec::new();
        let chars: Vec<char> = option_string.chars().collect();
        if chars[0] == '-' && chars[1] == '-' {
            let (prefix, sep, explicit) = match option_string.split_once('=') {
                Some((p, e)) => (p, Some("=".to_string()), Some(e.to_string())),
                None => (option_string, None, None),
            };
            for (opt, &ix) in &self.option_strings {
                if opt.starts_with(prefix) {
                    result.push(OptTuple {
                        action: Some(ix),
                        option_string: opt.to_string(),
                        sep: sep.clone(),
                        explicit_arg: explicit.clone(),
                    });
                }
            }
        } else {
            let (prefix, sep, explicit) = match option_string.split_once('=') {
                Some((p, e)) => (p, Some("=".to_string()), Some(e.to_string())),
                None => (option_string, None, None),
            };
            let short_prefix: String = chars[..2].iter().collect();
            let short_explicit: String = chars[2..].iter().collect();
            for (opt, &ix) in &self.option_strings {
                if *opt == short_prefix {
                    result.push(OptTuple {
                        action: Some(ix),
                        option_string: opt.to_string(),
                        sep: Some(String::new()),
                        explicit_arg: Some(short_explicit.clone()),
                    });
                } else if opt.starts_with(prefix) {
                    result.push(OptTuple {
                        action: Some(ix),
                        option_string: opt.to_string(),
                        sep: sep.clone(),
                        explicit_arg: explicit.clone(),
                    });
                }
            }
        }
        result
    }

    /// `_match_argument`: how many pattern characters the action's `nargs` consumes.
    fn match_argument(&self, spec: &ArgSpec, pattern: &[char]) -> Result<usize, ParseError> {
        let count_leading = |set: &[char]| pattern.iter().take_while(|c| set.contains(c)).count();
        let matched = match spec.nargs {
            // '([A])'
            Nargs::Single => (pattern.first() == Some(&'A')).then_some(1),
            // '(A*)'
            Nargs::ZeroOrMore => Some(count_leading(&['A'])),
            // '(A+)'. PARITY: greedy, so `-c a b show` swallows `show` into the list.
            Nargs::OneOrMore => {
                let n = count_leading(&['A']);
                (n >= 1).then_some(n)
            }
            // '([AO]*)'
            Nargs::Remainder => Some(count_leading(&['A', 'O'])),
            // '([AO]{N})'
            Nargs::Exactly(n) => (count_leading(&['A', 'O']) >= n).then_some(n),
        };
        matched.ok_or_else(|| {
            let msg = match spec.nargs {
                Nargs::Single => "expected one argument".to_string(),
                Nargs::OneOrMore => "expected at least one argument".to_string(),
                Nargs::Exactly(1) => "expected 1 argument".to_string(),
                Nargs::Exactly(n) => format!("expected {n} arguments"),
                _ => "expected at least one argument".to_string(),
            };
            ParseError::argument(Some(spec), msg)
        })
    }

    /// `_get_value`: apply the `type=` callable.
    fn get_value(
        &self,
        spec: &ArgSpec,
        arg: &str,
        ctx: &DateContext,
    ) -> Result<PyValue, ParseError> {
        let result = match spec.arg_type {
            ArgType::None | ArgType::Str => Ok(PyValue::str(arg)),
            ArgType::Int => types::py_int(arg).ok_or(ConvError::Invalid),
            ArgType::Float => types::py_float(arg)
                .map(PyValue::Float)
                .ok_or(ConvError::Invalid),
            ArgType::Path => Ok(PyValue::Path(types::py_path(arg))),
            ArgType::ParseDatetime => types::parse_datetime(arg, ctx),
            ArgType::StringToDict => types::string_to_dict(arg),
            ArgType::ParseName => types::parse_name(arg),
            ArgType::ParseExecutorKeys => types::parse_executor_keys(arg),
        };
        result.map_err(|e| match e {
            ConvError::Invalid => ParseError::argument(
                Some(spec),
                format!(
                    "invalid {} value: {}",
                    spec.arg_type.name().unwrap_or("str"),
                    crate::value::repr_str(arg)
                ),
            ),
            ConvError::Raise(e) => ParseError::Raise(e),
        })
    }

    /// `_check_value`: enforce `choices`.
    fn check_value(&self, spec: &ArgSpec, value: &PyValue) -> Result<(), ParseError> {
        let Some(choices) = spec.choices else {
            return Ok(());
        };
        if let PyValue::Str(s) = value
            && choices.contains(&s.as_str())
        {
            return Ok(());
        }
        let choices_str = choices
            .iter()
            .map(|c| crate::value::repr_str(c))
            .collect::<Vec<_>>()
            .join(", ");
        Err(ParseError::argument(
            Some(spec),
            format!(
                "invalid choice: {} (choose from {choices_str})",
                crate::value::repr_str(&value.py_str())
            ),
        ))
    }

    /// `_get_values` + the action's `__call__`.
    fn take_action(
        &self,
        ns: &mut Namespace,
        ix: usize,
        args: &[String],
        ctx: &DateContext,
    ) -> Result<(), ParseError> {
        let spec = &self.specs[ix];
        match spec.action {
            Action::StoreTrue => {
                ns.insert(spec.dest.to_string(), PyValue::Bool(true));
            }
            Action::Store => {
                let value = match spec.nargs {
                    Nargs::ZeroOrMore if args.is_empty() && spec.is_positional() => {
                        PyValue::List(vec![])
                    }
                    Nargs::Single => {
                        let v = self.get_value(spec, &args[0], ctx)?;
                        self.check_value(spec, &v)?;
                        v
                    }
                    Nargs::Remainder => PyValue::List(
                        args.iter()
                            .map(|a| self.get_value(spec, a, ctx))
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    _ => {
                        let values = args
                            .iter()
                            .map(|a| self.get_value(spec, a, ctx))
                            .collect::<Result<Vec<_>, _>>()?;
                        for v in &values {
                            self.check_value(spec, v)?;
                        }
                        PyValue::List(values)
                    }
                };
                // PARITY: `store` replaces, so `-t a -t b` keeps `['b']` (argparse `_StoreAction`).
                ns.insert(spec.dest.to_string(), value);
            }
        }
        Ok(())
    }

    /// `parse_known_args(args)` → (namespace, extras).
    pub fn parse_known_args(
        &self,
        arg_strings: &[String],
        ctx: &DateContext,
    ) -> Result<(Namespace, Vec<String>), ParseError> {
        // Defaults for every action, in order.
        let mut ns: Namespace = IndexMap::new();
        for spec in self.specs {
            let default = match spec.action {
                Action::StoreTrue => PyValue::Bool(false),
                Action::Store => PyValue::None,
            };
            ns.insert(spec.dest.to_string(), default);
        }

        // Pattern: 'O' for options, 'A' for arguments, '-' for the first '--'.
        let mut option_indices: IndexMap<usize, Vec<OptTuple>> = IndexMap::new();
        let mut pattern: Vec<char> = Vec::with_capacity(arg_strings.len());
        let mut iter = arg_strings.iter().enumerate();
        while let Some((i, arg)) = iter.next() {
            if arg == "--" {
                pattern.push('-');
                pattern.extend(std::iter::repeat_n('A', iter.by_ref().count()));
                break;
            }
            match self.parse_optional(arg) {
                None => pattern.push('A'),
                Some(tuples) => {
                    option_indices.insert(i, tuples);
                    pattern.push('O');
                }
            }
        }

        let mut extras: Vec<String> = Vec::new();
        let mut positionals: Vec<usize> = self
            .specs
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_positional())
            .map(|(i, _)| i)
            .collect();

        let max_option_index: i64 = option_indices.keys().max().map(|&m| m as i64).unwrap_or(-1);
        let mut start_index: usize = 0;

        while (start_index as i64) <= max_option_index {
            let mut next_option_index = start_index;
            while (next_option_index as i64) <= max_option_index {
                if option_indices.contains_key(&next_option_index) {
                    break;
                }
                next_option_index += 1;
            }
            if start_index != next_option_index {
                let end = self.consume_positionals(
                    &mut ns,
                    &mut positionals,
                    arg_strings,
                    &pattern,
                    start_index,
                    ctx,
                )?;
                if end > start_index {
                    start_index = end;
                    continue;
                }
                start_index = end;
            }
            if !option_indices.contains_key(&start_index) {
                extras.extend(arg_strings[start_index..next_option_index].iter().cloned());
                start_index = next_option_index;
            }
            start_index = self.consume_optional(
                &mut ns,
                &mut extras,
                &option_indices,
                arg_strings,
                &pattern,
                start_index,
                ctx,
            )?;
        }
        let stop = self.consume_positionals(
            &mut ns,
            &mut positionals,
            arg_strings,
            &pattern,
            start_index,
            ctx,
        )?;
        extras.extend(arg_strings[stop..].iter().cloned());
        Ok((ns, extras))
    }

    #[allow(clippy::too_many_arguments)]
    fn consume_optional(
        &self,
        ns: &mut Namespace,
        extras: &mut Vec<String>,
        option_indices: &IndexMap<usize, Vec<OptTuple>>,
        arg_strings: &[String],
        pattern: &[char],
        start_index: usize,
        ctx: &DateContext,
    ) -> Result<usize, ParseError> {
        let tuples = &option_indices[&start_index];
        if tuples.len() > 1 {
            let options = tuples
                .iter()
                .map(|t| t.option_string.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ParseError::argument(
                None,
                format!(
                    "ambiguous option: {} could match {options}",
                    arg_strings[start_index]
                ),
            ));
        }
        let OptTuple {
            mut action,
            mut option_string,
            mut sep,
            mut explicit_arg,
        } = tuples[0].clone();

        let mut action_tuples: Vec<(usize, Vec<String>)> = Vec::new();
        let stop: usize;
        loop {
            let Some(ix) = action else {
                extras.push(arg_strings[start_index].clone());
                return Ok(start_index + 1);
            };
            let spec = &self.specs[ix];
            if let Some(explicit) = explicit_arg.clone() {
                let arg_count = self.match_argument(spec, &['A'])?;
                let second = option_string.chars().nth(1);
                if arg_count == 0 && second != Some('-') && !explicit.is_empty() {
                    if sep.as_deref().is_some_and(|s| !s.is_empty()) || explicit.starts_with('-') {
                        return Err(ParseError::argument(
                            Some(spec),
                            format!(
                                "ignored explicit argument {}",
                                crate::value::repr_str(&explicit)
                            ),
                        ));
                    }
                    action_tuples.push((ix, vec![]));
                    let first_char = option_string.chars().next().unwrap_or('-');
                    let next_char = explicit.chars().next().unwrap_or_default();
                    option_string = format!("{first_char}{next_char}");
                    if let Some(&next_ix) = self.option_strings.get(option_string.as_str()) {
                        action = Some(next_ix);
                        let rest: String = explicit.chars().skip(1).collect();
                        if rest.is_empty() {
                            sep = None;
                            explicit_arg = None;
                        } else if let Some(stripped) = rest.strip_prefix('=') {
                            sep = Some("=".into());
                            explicit_arg = Some(stripped.to_string());
                        } else {
                            sep = Some(String::new());
                            explicit_arg = Some(rest);
                        }
                    } else {
                        extras.push(format!("{first_char}{explicit}"));
                        stop = start_index + 1;
                        break;
                    }
                } else if arg_count == 1 {
                    stop = start_index + 1;
                    action_tuples.push((ix, vec![explicit]));
                    break;
                } else {
                    return Err(ParseError::argument(
                        Some(spec),
                        format!(
                            "ignored explicit argument {}",
                            crate::value::repr_str(&explicit)
                        ),
                    ));
                }
            } else {
                let start = start_index + 1;
                let arg_count = self.match_argument(spec, &pattern[start..])?;
                stop = start + arg_count;
                action_tuples.push((ix, arg_strings[start..stop].to_vec()));
                break;
            }
        }
        for (ix, args) in action_tuples {
            self.take_action(ns, ix, &args, ctx)?;
        }
        Ok(stop)
    }

    fn consume_positionals(
        &self,
        ns: &mut Namespace,
        positionals: &mut Vec<usize>,
        arg_strings: &[String],
        pattern: &[char],
        start_index: usize,
        ctx: &DateContext,
    ) -> Result<usize, ParseError> {
        // `_match_arguments_partial`: only `nargs='*'` positionals exist ('(-*[A-]*)').
        let selected = &pattern[start_index..];
        let mut arg_counts: Vec<usize> = Vec::new();
        let mut offset = 0usize;
        for &ix in positionals.iter() {
            debug_assert_eq!(self.specs[ix].nargs, Nargs::ZeroOrMore);
            let n = selected[offset..]
                .iter()
                .take_while(|c| **c == 'A' || **c == '-')
                .count();
            arg_counts.push(n);
            offset += n;
        }
        if offset < selected.len() && selected[offset] == 'O' {
            while arg_counts.last() == Some(&0) {
                arg_counts.pop();
            }
        }

        let mut start = start_index;
        for (&ix, &count) in positionals.iter().zip(arg_counts.iter()) {
            let mut args: Vec<String> = arg_strings[start..start + count].to_vec();
            if pattern[start..start + count].contains(&'-')
                && let Some(pos) = args.iter().position(|a| a == "--")
            {
                args.remove(pos);
            }
            start += count;
            self.take_action(ns, ix, &args, ctx)?;
        }
        // PARITY: the `action` positional is consumed once, by the first run of arguments, so
        // `show --debug pipes` drops `pipes` into the ignored extras (`consume_positionals`).
        let consumed = arg_counts.len();
        positionals.drain(..consumed);
        Ok(start)
    }
}
