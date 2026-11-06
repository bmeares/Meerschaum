//! The sysargs of a job written by Meerschaum 4, read from its `pickle.pkl`.
//!
//! v4 kept a job's command only in the pickled `Daemon` (`Daemon.__getstate__`:
//! `{'target_args': [sysargs], 'target_kw': {}, 'daemon_id', 'label', 'properties',
//! 'target': None, 'target_ref': {'module', 'qualname'}}`). Jobs created through the API or a
//! container left `label` empty, so without the pickle Rust saw a job with no command.
//!
//! This is a data-only reader of the pickle opcode stream: it builds strings, numbers, lists,
//! tuples and dicts, turns `GLOBAL` / `REDUCE` / `NEWOBJ` / `BUILD` into inert placeholders and
//! never imports or calls anything. Any opcode it does not know ends the read with `None`.

use std::collections::HashMap;
use std::path::Path;

/// Larger pickles are not v4 job pickles (they are a few hundred bytes).
const MAX_BYTES: u64 = 1 << 20;
/// Bound on the values one pickle may build.
const MAX_VALUES: usize = 100_000;

const ENTRY_MODULE: &str = "meerschaum._internal.entry";
const ENTRY_NAME: &str = "entry";

#[derive(Debug, Clone)]
enum P {
    Str(String),
    Bytes(Vec<u8>),
    Other,
    List(Vec<usize>),
    Tuple(Vec<usize>),
    Dict(Vec<(usize, usize)>),
    Global(String, String),
    /// An object built by `REDUCE` / `NEWOBJ`, with the state `BUILD` gave it.
    Object(usize, Option<usize>),
}

#[derive(Clone, Copy)]
enum Slot {
    Mark,
    Value(usize),
}

struct Machine<'a> {
    data: &'a [u8],
    pos: usize,
    arena: Vec<P>,
    stack: Vec<Slot>,
    memo: HashMap<u32, usize>,
}

impl<'a> Machine<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let bytes = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }

    fn uint(&mut self, n: usize) -> Option<u64> {
        let bytes = self.take(n)?;
        let mut buf = [0u8; 8];
        buf[..n].copy_from_slice(bytes);
        Some(u64::from_le_bytes(buf))
    }

    fn line(&mut self) -> Option<String> {
        let rest = self.data.get(self.pos..)?;
        let end = rest.iter().position(|b| *b == b'\n')?;
        self.pos += end + 1;
        String::from_utf8(rest[..end].to_vec()).ok()
    }

    fn alloc(&mut self, value: P) -> Option<usize> {
        if self.arena.len() >= MAX_VALUES {
            return None;
        }
        self.arena.push(value);
        Some(self.arena.len() - 1)
    }

    fn push(&mut self, value: P) -> Option<()> {
        let id = self.alloc(value)?;
        self.stack.push(Slot::Value(id));
        Some(())
    }

    fn pop(&mut self) -> Option<usize> {
        match self.stack.pop()? {
            Slot::Value(id) => Some(id),
            Slot::Mark => None,
        }
    }

    fn top(&self) -> Option<usize> {
        match self.stack.last()? {
            Slot::Value(id) => Some(*id),
            Slot::Mark => None,
        }
    }

    /// The values above the topmost mark (the mark is removed).
    fn pop_mark(&mut self) -> Option<Vec<usize>> {
        let at = self.stack.iter().rposition(|s| matches!(s, Slot::Mark))?;
        let items = self
            .stack
            .split_off(at + 1)
            .into_iter()
            .map(|s| match s {
                Slot::Value(id) => Some(id),
                Slot::Mark => None,
            })
            .collect::<Option<Vec<_>>>()?;
        self.stack.pop();
        Some(items)
    }

    fn string(&mut self, n: usize) -> Option<()> {
        let bytes = self.take(n)?;
        let text = String::from_utf8(bytes.to_vec()).ok()?;
        self.push(P::Str(text))
    }

    fn bytes(&mut self, n: usize) -> Option<()> {
        let bytes = self.take(n)?.to_vec();
        self.push(P::Bytes(bytes))
    }

    fn extend(&mut self, list: usize, items: Vec<usize>) -> Option<()> {
        match self.arena.get_mut(list)? {
            P::List(values) => {
                values.extend(items);
                Some(())
            }
            // `set.update` / unknown containers: nothing we read.
            P::Other | P::Object(..) => Some(()),
            _ => None,
        }
    }

    fn set_items(&mut self, dict: usize, items: Vec<usize>) -> Option<()> {
        if !items.len().is_multiple_of(2) {
            return None;
        }
        match self.arena.get_mut(dict)? {
            P::Dict(pairs) => {
                pairs.extend(items.chunks(2).map(|c| (c[0], c[1])));
                Some(())
            }
            P::Other | P::Object(..) => Some(()),
            _ => None,
        }
    }

    fn run(&mut self) -> Option<usize> {
        loop {
            let op = *self.take(1)?.first()?;
            match op {
                0x80 => {
                    self.take(1)?;
                } // PROTO
                0x95 => {
                    self.take(8)?;
                } // FRAME
                b'.' => return self.pop(), // STOP
                b'(' => self.stack.push(Slot::Mark),
                b'}' => self.push(P::Dict(Vec::new()))?,
                b']' => self.push(P::List(Vec::new()))?,
                b')' => self.push(P::Tuple(Vec::new()))?,
                0x8f => self.push(P::Other)?, // EMPTY_SET
                0x8c => {
                    let n = self.uint(1)? as usize;
                    self.string(n)?
                } // SHORT_BINUNICODE
                b'X' => {
                    let n = self.uint(4)? as usize;
                    self.string(n)?
                } // BINUNICODE
                0x8d => {
                    let n = usize::try_from(self.uint(8)?).ok()?;
                    self.string(n)?
                } // BINUNICODE8
                b'U' => {
                    let n = self.uint(1)? as usize;
                    self.bytes(n)?
                } // SHORT_BINSTRING
                b'T' => {
                    let n = self.uint(4)? as usize;
                    self.bytes(n)?
                } // BINSTRING
                b'C' => {
                    let n = self.uint(1)? as usize;
                    self.bytes(n)?
                } // SHORT_BINBYTES
                b'B' => {
                    let n = self.uint(4)? as usize;
                    self.bytes(n)?
                } // BINBYTES
                0x8e | 0x96 => {
                    let n = usize::try_from(self.uint(8)?).ok()?;
                    self.bytes(n)?
                } // BINBYTES8, BYTEARRAY8
                0x94 => {
                    let id = self.top()?;
                    let index = u32::try_from(self.memo.len()).ok()?;
                    self.memo.insert(index, id);
                } // MEMOIZE
                b'q' => {
                    let index = self.uint(1)? as u32;
                    let id = self.top()?;
                    self.memo.insert(index, id);
                } // BINPUT
                b'r' => {
                    let index = self.uint(4)? as u32;
                    let id = self.top()?;
                    self.memo.insert(index, id);
                } // LONG_BINPUT
                b'h' => {
                    let index = self.uint(1)? as u32;
                    let id = *self.memo.get(&index)?;
                    self.stack.push(Slot::Value(id));
                } // BINGET
                b'j' => {
                    let index = self.uint(4)? as u32;
                    let id = *self.memo.get(&index)?;
                    self.stack.push(Slot::Value(id));
                } // LONG_BINGET
                b'J' => {
                    self.take(4)?;
                    self.push(P::Other)?
                } // BININT
                b'K' => {
                    self.take(1)?;
                    self.push(P::Other)?
                } // BININT1
                b'M' => {
                    self.take(2)?;
                    self.push(P::Other)?
                } // BININT2
                0x8a => {
                    let n = self.uint(1)? as usize;
                    self.take(n)?;
                    self.push(P::Other)?
                } // LONG1
                0x8b => {
                    let n = self.uint(4)? as usize;
                    self.take(n)?;
                    self.push(P::Other)?
                } // LONG4
                b'G' => {
                    self.take(8)?;
                    self.push(P::Other)?
                } // BINFLOAT
                b'N' | 0x88 | 0x89 => self.push(P::Other)?, // NONE, NEWTRUE, NEWFALSE
                0x85..=0x87 => {
                    let n = usize::from(op - 0x84);
                    let mut items = Vec::with_capacity(n);
                    for _ in 0..n {
                        items.push(self.pop()?);
                    }
                    items.reverse();
                    self.push(P::Tuple(items))?
                } // TUPLE1..3
                b't' => {
                    let items = self.pop_mark()?;
                    self.push(P::Tuple(items))?
                } // TUPLE
                b'l' => {
                    let items = self.pop_mark()?;
                    self.push(P::List(items))?
                } // LIST
                b'd' => {
                    let items = self.pop_mark()?;
                    let dict = self.alloc(P::Dict(Vec::new()))?;
                    self.set_items(dict, items)?;
                    self.stack.push(Slot::Value(dict));
                } // DICT
                0x91 => {
                    self.pop_mark()?;
                    self.push(P::Other)?
                } // FROZENSET
                b'a' => {
                    let item = self.pop()?;
                    let list = self.top()?;
                    self.extend(list, vec![item])?
                } // APPEND
                b'e' => {
                    let items = self.pop_mark()?;
                    let list = self.top()?;
                    self.extend(list, items)?
                } // APPENDS
                0x90 => {
                    self.pop_mark()?;
                    self.top()?;
                } // ADDITEMS
                b's' => {
                    let value = self.pop()?;
                    let key = self.pop()?;
                    let dict = self.top()?;
                    self.set_items(dict, vec![key, value])?
                } // SETITEM
                b'u' => {
                    let items = self.pop_mark()?;
                    let dict = self.top()?;
                    self.set_items(dict, items)?
                } // SETITEMS
                b'c' => {
                    let module = self.line()?;
                    let name = self.line()?;
                    self.push(P::Global(module, name))?
                } // GLOBAL
                0x93 => {
                    let name = self.pop()?;
                    let module = self.pop()?;
                    let (P::Str(module), P::Str(name)) = (
                        self.arena.get(module)?.clone(),
                        self.arena.get(name)?.clone(),
                    ) else {
                        return None;
                    };
                    self.push(P::Global(module, name))?
                } // STACK_GLOBAL
                b'R' | 0x81 => {
                    let _args = self.pop()?;
                    let class = self.pop()?;
                    self.push(P::Object(class, None))?
                } // REDUCE, NEWOBJ
                0x92 => {
                    let _kwargs = self.pop()?;
                    let _args = self.pop()?;
                    let class = self.pop()?;
                    self.push(P::Object(class, None))?
                } // NEWOBJ_EX
                b'b' => {
                    let state = self.pop()?;
                    let target = self.top()?;
                    if let P::Object(_, slot) = self.arena.get_mut(target)? {
                        *slot = Some(state);
                    }
                } // BUILD
                b'0' => {
                    self.stack.pop()?;
                } // POP
                b'1' => {
                    self.pop_mark()?;
                } // POP_MARK
                b'2' => {
                    let id = self.top()?;
                    self.stack.push(Slot::Value(id));
                } // DUP
                _ => return None,
            }
        }
    }

    fn str(&self, id: usize) -> Option<&str> {
        match self.arena.get(id)? {
            P::Str(s) => Some(s),
            _ => None,
        }
    }

    fn get(&self, dict: usize, key: &str) -> Option<usize> {
        let P::Dict(pairs) = self.arena.get(dict)? else {
            return None;
        };
        pairs
            .iter()
            .rev()
            .find(|(k, _)| self.str(*k) == Some(key))
            .map(|(_, v)| *v)
    }

    fn items(&self, id: usize) -> Option<&[usize]> {
        match self.arena.get(id)? {
            P::List(items) | P::Tuple(items) => Some(items),
            _ => None,
        }
    }

    fn strings(&self, id: usize) -> Option<Vec<String>> {
        self.items(id)?
            .iter()
            .map(|item| self.str(*item).map(str::to_string))
            .collect()
    }

    /// The pickled `Daemon`'s state dict.
    fn state(&self, root: usize) -> Option<usize> {
        match self.arena.get(root)? {
            P::Object(class, Some(state)) => {
                let P::Global(module, name) = self.arena.get(*class)? else {
                    return None;
                };
                (module == "meerschaum.utils.daemon.Daemon" || module == "meerschaum.utils.daemon")
                    .then_some(())
                    .filter(|()| name == "Daemon")?;
                Some(*state)
            }
            _ => None,
        }
    }

    /// Whether the state's target is `meerschaum._internal.entry.entry` (the target of every job).
    fn targets_entry(&self, state: usize) -> bool {
        if let Some(reference) = self.get(state, "target_ref") {
            return self
                .get(reference, "module")
                .and_then(|m| self.str(m))
                .is_some_and(|m| m == ENTRY_MODULE)
                && self
                    .get(reference, "qualname")
                    .and_then(|q| self.str(q))
                    .is_some_and(|q| q == ENTRY_NAME);
        }
        // Before `target_ref`, v4 stored the target with `dill.dumps(entry, byref=True)`.
        match self.get(state, "target").and_then(|t| self.arena.get(t)) {
            Some(P::Bytes(bytes)) => {
                let module = ENTRY_MODULE.as_bytes();
                bytes.windows(module.len()).any(|w| w == module)
            }
            _ => false,
        }
    }
}

/// The sysargs in a v4 job pickle (`target_args[0]`), if the pickle is a v4 job `Daemon`.
pub fn sysargs_from_bytes(data: &[u8]) -> Option<Vec<String>> {
    let mut machine = Machine {
        data,
        pos: 0,
        arena: Vec::new(),
        stack: Vec::new(),
        memo: HashMap::new(),
    };
    let root = machine.run()?;
    let state = machine.state(root)?;
    if !machine.targets_entry(state) {
        return None;
    }
    let args = machine.get(state, "target_args")?;
    let first = *machine.items(args)?.first()?;
    let sysargs = machine.strings(first)?;
    (!sysargs.is_empty()).then_some(sysargs)
}

/// [`sysargs_from_bytes`] of the file at `path` (missing, unreadable or large files: `None`).
pub fn sysargs_from_file(path: &Path) -> Option<Vec<String>> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() || meta.len() == 0 || meta.len() > MAX_BYTES {
        return None;
    }
    sysargs_from_bytes(&std::fs::read(path).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_and_truncation_are_none() {
        assert_eq!(sysargs_from_bytes(b""), None);
        assert_eq!(sysargs_from_bytes(b"not a pickle"), None);
        assert_eq!(sysargs_from_bytes(&[0x80, 0x04, 0x95]), None);
    }
}
