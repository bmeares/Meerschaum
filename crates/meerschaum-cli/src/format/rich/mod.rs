//! A byte-compatible port of the parts of `rich` (15.0.0) that Meerschaum's formatting uses.
//!
//! Every type keeps rich's name and algorithm (`Style`, `Text`, `Segment`, `Table`, `Panel`,
//! `Tree`, `Columns`, `Pretty`, `ReprHighlighter`) so the output matches v4 byte for byte on a
//! forced truecolor console; see the module docs of each file for what is covered.

pub mod cells;
pub mod console;
pub mod panel;
pub mod pretty;
pub mod segment;
pub mod style;
pub mod table;
pub mod tables;
pub mod text;
