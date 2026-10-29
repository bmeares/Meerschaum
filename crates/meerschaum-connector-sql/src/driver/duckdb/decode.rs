//! Decoding DuckDB data-chunk vectors into driver values, and mapping DuckDB types to column kinds.
//!
//! A vector is read in place according to the physical layout of its type (`duckdb.h`, "Vectors"):
//! fixed-width values, `duckdb_string_t` for `VARCHAR` / `BLOB`, `(offset, length)` entries for
//! lists, and child vectors for structs. Types without a native cell (lists, structs, maps,
//! unions, arrays, enums, intervals, bit strings, `TIME WITH TIME ZONE`, `BIGNUM`) are rendered as
//! text the way DuckDB casts them to `VARCHAR`.
#![allow(unsafe_code)]

use std::ffi::{CStr, c_char};

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta};
use rust_decimal::Decimal;

use super::ffi::{LogicalType, Ptr, api};
use crate::driver::SqlValue;
use crate::driver::batch::ColumnKind;

// DUCKDB_TYPE ids (duckdb.h).
pub const BOOLEAN: i32 = 1;
pub const TINYINT: i32 = 2;
pub const SMALLINT: i32 = 3;
pub const INTEGER: i32 = 4;
pub const BIGINT: i32 = 5;
pub const UTINYINT: i32 = 6;
pub const USMALLINT: i32 = 7;
pub const UINTEGER: i32 = 8;
pub const UBIGINT: i32 = 9;
pub const FLOAT: i32 = 10;
pub const DOUBLE: i32 = 11;
pub const TIMESTAMP: i32 = 12;
pub const DATE: i32 = 13;
pub const TIME: i32 = 14;
pub const INTERVAL: i32 = 15;
pub const HUGEINT: i32 = 16;
pub const VARCHAR: i32 = 17;
pub const BLOB: i32 = 18;
pub const DECIMAL: i32 = 19;
pub const TIMESTAMP_S: i32 = 20;
pub const TIMESTAMP_MS: i32 = 21;
pub const TIMESTAMP_NS: i32 = 22;
pub const ENUM: i32 = 23;
pub const LIST: i32 = 24;
pub const STRUCT: i32 = 25;
pub const MAP: i32 = 26;
pub const UUID: i32 = 27;
pub const UNION: i32 = 28;
pub const BIT: i32 = 29;
pub const TIME_TZ: i32 = 30;
pub const TIMESTAMP_TZ: i32 = 31;
pub const UHUGEINT: i32 = 32;
pub const ARRAY: i32 = 33;
pub const BIGNUM: i32 = 35;
pub const TIME_NS: i32 = 39;
pub const GEOMETRY: i32 = 40;

/// Everything needed to read one top-level column.
pub struct ColumnType {
    pub id: i32,
    pub decimal: Option<(u8, u8, i32)>,
}

impl ColumnType {
    pub fn of(t: &LogicalType) -> ColumnType {
        let id = t.id();
        let decimal = (id == DECIMAL).then(|| {
            // SAFETY: valid DECIMAL logical type.
            unsafe {
                (
                    (api().duckdb_decimal_width)(t.0),
                    (api().duckdb_decimal_scale)(t.0),
                    (api().duckdb_decimal_internal_type)(t.0),
                )
            }
        });
        ColumnType { id, decimal }
    }
}

/// The column kind and database type name of a result column.
pub fn kind_for(ty: &ColumnType) -> (ColumnKind, String) {
    let name = |n: &str| n.to_string();
    match ty.id {
        BOOLEAN => (ColumnKind::Boolean, name("BOOLEAN")),
        TINYINT => (ColumnKind::Int8, name("TINYINT")),
        SMALLINT => (ColumnKind::Int16, name("SMALLINT")),
        INTEGER => (ColumnKind::Int32, name("INTEGER")),
        BIGINT => (ColumnKind::Int64, name("BIGINT")),
        UTINYINT => (ColumnKind::UInt8, name("UTINYINT")),
        USMALLINT => (ColumnKind::UInt16, name("USMALLINT")),
        UINTEGER => (ColumnKind::UInt32, name("UINTEGER")),
        UBIGINT => (ColumnKind::UInt64, name("UBIGINT")),
        FLOAT => (ColumnKind::Float32, name("FLOAT")),
        DOUBLE => (ColumnKind::Float64, name("DOUBLE")),
        // PARITY: a 128-bit integer is a numeric column of exact text (`meerschaum:dtype=numeric`):
        // 39 digits do not fit `DECIMAL(38,0)`, which the old Arrow export used and which failed
        // the whole query on the largest values.
        HUGEINT | UHUGEINT => (
            ColumnKind::NumericText,
            name(if ty.id == HUGEINT {
                "HUGEINT"
            } else {
                "UHUGEINT"
            }),
        ),
        DECIMAL => {
            let (w, s, _) = ty.decimal.unwrap_or((38, 0, HUGEINT));
            (ColumnKind::Decimal, format!("DECIMAL({w},{s})"))
        }
        VARCHAR => (ColumnKind::Utf8, name("VARCHAR")),
        BLOB => (ColumnKind::Bytes, name("BLOB")),
        GEOMETRY => (ColumnKind::Bytes, name("GEOMETRY")),
        UUID => (ColumnKind::Uuid, name("UUID")),
        DATE => (ColumnKind::Date, name("DATE")),
        TIME | TIME_NS => (ColumnKind::Time, name("TIME")),
        TIME_TZ => (ColumnKind::Utf8, name("TIME WITH TIME ZONE")),
        TIMESTAMP | TIMESTAMP_S | TIMESTAMP_MS | TIMESTAMP_NS => {
            (ColumnKind::Timestamp, name("TIMESTAMP"))
        }
        TIMESTAMP_TZ => (ColumnKind::TimestampTz, name("TIMESTAMP WITH TIME ZONE")),
        ENUM => (ColumnKind::Utf8, name("ENUM")),
        LIST => (ColumnKind::Utf8, name("LIST")),
        ARRAY => (ColumnKind::Utf8, name("ARRAY")),
        STRUCT => (ColumnKind::Utf8, name("STRUCT")),
        MAP => (ColumnKind::Utf8, name("MAP")),
        UNION => (ColumnKind::Utf8, name("UNION")),
        INTERVAL => (ColumnKind::Utf8, name("INTERVAL")),
        BIT => (ColumnKind::Utf8, name("BIT")),
        BIGNUM => (ColumnKind::Utf8, name("BIGNUM")),
        other => (ColumnKind::Utf8, format!("DUCKDB_TYPE_{other}")),
    }
}

/// The decimal text of a `BIGNUM` (`VARINT`): a 3-byte header whose top bit says "positive",
/// then the big-endian magnitude; for a negative number every byte is inverted.
pub fn bignum_text(bytes: &[u8]) -> String {
    if bytes.len() < 3 {
        return "NULL".into();
    }
    let negative = bytes[0] & 0x80 == 0;
    let mut limbs: Vec<u32> = Vec::new(); // base 10^9, least significant first
    for b in &bytes[3..] {
        let b = if negative { !*b } else { *b };
        let mut carry = b as u64;
        for limb in limbs.iter_mut() {
            let v = *limb as u64 * 256 + carry;
            *limb = (v % 1_000_000_000) as u32;
            carry = v / 1_000_000_000;
        }
        if carry > 0 {
            limbs.push(carry as u32);
        }
    }
    let mut out = String::new();
    for (i, limb) in limbs.iter().rev().enumerate() {
        if i == 0 {
            out.push_str(&limb.to_string());
        } else {
            out.push_str(&format!("{limb:09}"));
        }
    }
    if out.is_empty() {
        out.push('0');
    } else if negative {
        out.insert(0, '-');
    }
    out
}

fn is_valid(v: Ptr, row: usize) -> bool {
    // SAFETY: the validity mask is NULL (all valid) or has a bit per row of the vector.
    unsafe {
        let m = (api().duckdb_vector_get_validity)(v);
        m.is_null() || (*m.add(row / 64) >> (row % 64)) & 1 == 1
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct StringT {
    len: u32,
    rest: [u8; 12],
}

/// # Safety
/// `data` must point to at least `row + 1` values of type `T`.
unsafe fn at<T: Copy>(data: *const u8, row: usize) -> T {
    unsafe { std::ptr::read_unaligned(data.cast::<T>().add(row)) }
}

fn string_bytes(data: *const u8, row: usize) -> Vec<u8> {
    // SAFETY: a VARCHAR/BLOB vector holds `duckdb_string_t` (16 bytes): inlined up to 12
    // bytes, else a 4-byte prefix and a pointer valid while the chunk lives.
    unsafe {
        let s: StringT = at(data, row);
        let n = s.len as usize;
        if n <= 12 {
            s.rest[..n].to_vec()
        } else {
            let p = std::ptr::read_unaligned(s.rest.as_ptr().add(4).cast::<*const u8>());
            std::slice::from_raw_parts(p, n).to_vec()
        }
    }
}

fn epoch_date(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(TimeDelta::days(days as i64))
}

/// `duckdb_hugeint` (16 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
struct HugeintT {
    lower: u64,
    upper: i64,
}

/// `duckdb_uhugeint` (16 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
struct UhugeintT {
    lower: u64,
    upper: u64,
}

/// `duckdb_list_entry` (16 bytes): a list (or map) value is `length` children from `offset`.
#[repr(C)]
#[derive(Clone, Copy)]
struct ListEntryT {
    offset: u64,
    length: u64,
}

/// `duckdb_interval` (16 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
struct IntervalT {
    months: i32,
    days: i32,
    micros: i64,
}

fn hugeint(data: *const u8, row: usize) -> i128 {
    // SAFETY: the vector holds a `duckdb_hugeint` per row.
    let h: HugeintT = unsafe { at(data, row) };
    ((h.upper as i128) << 64) | h.lower as i128
}

fn uhugeint(data: *const u8, row: usize) -> u128 {
    // SAFETY: the vector holds a `duckdb_uhugeint` per row.
    let h: UhugeintT = unsafe { at(data, row) };
    ((h.upper as u128) << 64) | h.lower as u128
}

/// Exact decimal text of a mantissa and scale (values `rust_decimal` cannot hold).
pub fn decimal_text(v: i128, scale: u8) -> String {
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    let sign = if neg { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{int}")
    } else {
        format!("{sign}{int}.{frac}")
    }
}

pub fn decimal_from(v: i128, scale: u8) -> Option<Decimal> {
    Decimal::try_from_i128_with_scale(v, scale as u32).ok()
}

/// What decoding a query's cells noticed and the caller reports once.
#[derive(Debug, Default, Clone, Copy)]
pub struct Notes {
    /// An infinite timestamp or date was replaced by the type's limit.
    pub clamped: bool,
}

/// Python's `datetime.max` / `datetime.min`, which the Python `duckdb` package (v4's driver) returns
/// for `infinity` / `-infinity` (dates: `date.max` / `date.min`).
fn max_datetime() -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(9999, 12, 31)
        .and_then(|d| d.and_hms_micro_opt(23, 59, 59, 999_999))
        .expect("datetime.max")
}

fn min_datetime() -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("datetime.min")
}

/// Decode one cell of a top-level column; `Err` is a conversion reason.
pub fn cell(v: Ptr, ty: &ColumnType, row: usize, notes: &mut Notes) -> Result<SqlValue, String> {
    if !is_valid(v, row) {
        return Ok(SqlValue::Null);
    }
    // SAFETY: the data pointer of a valid vector.
    let d = unsafe { (api().duckdb_vector_get_data)(v) } as *const u8;
    // SAFETY (all `at` calls below): the vector holds `row` of the declared physical type.
    Ok(match ty.id {
        BOOLEAN => SqlValue::Bool(unsafe { at::<u8>(d, row) } != 0),
        TINYINT => SqlValue::Int(unsafe { at::<i8>(d, row) } as i64),
        SMALLINT => SqlValue::Int(unsafe { at::<i16>(d, row) } as i64),
        INTEGER => SqlValue::Int(unsafe { at::<i32>(d, row) } as i64),
        BIGINT => SqlValue::Int(unsafe { at::<i64>(d, row) }),
        UTINYINT => SqlValue::Int(unsafe { at::<u8>(d, row) } as i64),
        USMALLINT => SqlValue::Int(unsafe { at::<u16>(d, row) } as i64),
        UINTEGER => SqlValue::Int(unsafe { at::<u32>(d, row) } as i64),
        UBIGINT => SqlValue::UInt(unsafe { at::<u64>(d, row) }),
        FLOAT => SqlValue::Float(unsafe { at::<f32>(d, row) } as f64),
        DOUBLE => SqlValue::Float(unsafe { at::<f64>(d, row) }),
        HUGEINT => {
            let i = hugeint(d, row);
            i64::try_from(i).map_or_else(|_| SqlValue::Text(i.to_string()), SqlValue::Int)
        }
        UHUGEINT => {
            let i = uhugeint(d, row);
            u64::try_from(i).map_or_else(|_| SqlValue::Text(i.to_string()), SqlValue::UInt)
        }
        DECIMAL => {
            let (_, scale, internal) = ty.decimal.ok_or("decimal without a type")?;
            let raw: i128 = match internal {
                SMALLINT => (unsafe { at::<i16>(d, row) }) as i128,
                INTEGER => (unsafe { at::<i32>(d, row) }) as i128,
                BIGINT => (unsafe { at::<i64>(d, row) }) as i128,
                _ => hugeint(d, row),
            };
            decimal_from(raw, scale).map_or_else(
                || SqlValue::Text(decimal_text(raw, scale)),
                SqlValue::Decimal,
            )
        }
        TIMESTAMP | TIMESTAMP_TZ | TIMESTAMP_S | TIMESTAMP_MS | TIMESTAMP_NS => {
            let raw = unsafe { at::<i64>(d, row) };
            // PARITY: `infinity` / `-infinity` are `datetime.max` / `datetime.min`, as the Python
            // `duckdb` package returns them (DuckDB stores them as `i64::MAX` / `-i64::MAX`).
            if raw == i64::MAX || raw == -i64::MAX {
                notes.clamped = true;
                let dt = if raw > 0 {
                    max_datetime()
                } else {
                    min_datetime()
                };
                return Ok(if ty.id == TIMESTAMP_TZ {
                    SqlValue::TimestampTz(dt.and_utc())
                } else {
                    SqlValue::Timestamp(dt)
                });
            }
            let micros = match ty.id {
                TIMESTAMP_S => raw.checked_mul(1_000_000),
                TIMESTAMP_MS => raw.checked_mul(1_000),
                TIMESTAMP_NS => Some(raw.div_euclid(1_000)),
                _ => Some(raw),
            }
            .ok_or_else(|| format!("timestamp out of range: {raw}"))?;
            let dt = DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| format!("timestamp out of range: {raw}"))?;
            if ty.id == TIMESTAMP_TZ {
                SqlValue::TimestampTz(dt)
            } else {
                SqlValue::Timestamp(dt.naive_utc())
            }
        }
        DATE => {
            let days = unsafe { at::<i32>(d, row) };
            if days == i32::MAX || days == -i32::MAX {
                notes.clamped = true;
                return Ok(SqlValue::Date(if days > 0 {
                    max_datetime().date()
                } else {
                    min_datetime().date()
                }));
            }
            SqlValue::Date(epoch_date(days).ok_or_else(|| format!("date out of range: {days}"))?)
        }
        TIME => {
            let us = unsafe { at::<i64>(d, row) };
            SqlValue::Time(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (us / 1_000_000) as u32,
                    ((us % 1_000_000) * 1000) as u32,
                )
                .ok_or_else(|| format!("time out of range: {us}"))?,
            )
        }
        TIME_NS => {
            let ns = unsafe { at::<i64>(d, row) };
            SqlValue::Time(
                NaiveTime::from_num_seconds_from_midnight_opt(
                    (ns / 1_000_000_000) as u32,
                    (ns % 1_000_000_000) as u32,
                )
                .ok_or_else(|| format!("time out of range: {ns}"))?,
            )
        }
        UUID => {
            let i = hugeint(d, row) as u128 ^ (1u128 << 127);
            SqlValue::Uuid(uuid::Uuid::from_u128(i))
        }
        VARCHAR => SqlValue::Text(String::from_utf8_lossy(&string_bytes(d, row)).into_owned()),
        BIGNUM => SqlValue::Text(bignum_text(&string_bytes(d, row))),
        BLOB | GEOMETRY => SqlValue::Bytes(string_bytes(d, row)),
        _ => SqlValue::Text(render(v, row, notes)),
    })
}

fn free_cstr(p: *mut c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: DuckDB-allocated NUL-terminated string, freed once.
    unsafe {
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        (api().duckdb_free)(p.cast());
        s
    }
}

/// Whether DuckDB's cast to `VARCHAR` quotes a text found inside a list, struct or map: an empty
/// text, `NULL` in any case, a leading or trailing space, or any of `{}[],:="'`.
fn needs_quotes(s: &str) -> bool {
    s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.starts_with(char::is_whitespace)
        || s.ends_with(char::is_whitespace)
        || s.contains(['{', '}', '[', ']', ',', ':', '=', '\'', '"'])
}

/// `'text'` with `\` and `'` backslash-escaped, as DuckDB writes quoted nested elements.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// A non-nested value's text: plain at the top level, quoted when needed inside a nested value.
fn leaf(text: String, nested: bool) -> String {
    if nested && needs_quotes(&text) {
        quote(&text)
    } else {
        text
    }
}

fn fmt_time(secs: i64, micros: i64) -> String {
    let base = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    fmt_fraction(base, micros)
}

/// Append `.ffffff` without trailing zeros (DuckDB prints `00:00:01.5`).
fn fmt_fraction(mut base: String, micros: i64) -> String {
    if micros != 0 {
        let frac = format!("{micros:06}");
        base.push('.');
        base.push_str(frac.trim_end_matches('0'));
    }
    base
}

fn fmt_timestamp(t: &chrono::NaiveDateTime) -> String {
    use chrono::Timelike;
    let base = t.format("%Y-%m-%d %H:%M:%S").to_string();
    fmt_fraction(base, (t.nanosecond() / 1000) as i64)
}

/// A double the way DuckDB prints it: `1.0`, `1e+100`, `nan`, `inf`.
fn fmt_float(text: String) -> String {
    match text.split_once('e') {
        Some((mantissa, exp)) => {
            let (sign, digits) = match exp.strip_prefix('-') {
                Some(d) => ('-', d),
                None => ('+', exp),
            };
            format!("{mantissa}e{sign}{digits:0>2}")
        }
        None => text,
    }
}

/// DuckDB-style text of a value that has no cell of its own (nested types, intervals, enums, bits,
/// ...), as its cast to `VARCHAR` prints it: strings unquoted at the top level, quoted inside
/// lists, structs and maps only when they need it.
pub fn render(v: Ptr, row: usize, notes: &mut Notes) -> String {
    render_in(v, row, false, notes)
}

fn render_in(v: Ptr, row: usize, nested: bool, notes: &mut Notes) -> String {
    if !is_valid(v, row) {
        return "NULL".into();
    }
    // SAFETY: the vector's logical type (owned, destroyed by `LogicalType::drop`).
    let t = LogicalType(unsafe { (api().duckdb_vector_get_column_type)(v) });
    let id = t.id();
    // SAFETY: data pointer of a valid vector.
    let d = unsafe { (api().duckdb_vector_get_data)(v) } as *const u8;
    let a = api();
    // SAFETY (every unsafe block below): reads follow the physical layout of each type in duckdb.h.
    match id {
        LIST | MAP => unsafe {
            let entry: ListEntryT = at(d, row);
            let child = (a.duckdb_list_vector_get_child)(v);
            let range = entry.offset..entry.offset + entry.length;
            if id == MAP {
                let (keys, values) = (
                    (a.duckdb_struct_vector_get_child)(child, 0),
                    (a.duckdb_struct_vector_get_child)(child, 1),
                );
                let pairs: Vec<String> = range
                    .map(|i| {
                        format!(
                            "{}={}",
                            render_in(keys, i as usize, true, notes),
                            render_in(values, i as usize, true, notes)
                        )
                    })
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            } else {
                let items: Vec<String> = range
                    .map(|i| render_in(child, i as usize, true, notes))
                    .collect();
                format!("[{}]", items.join(", "))
            }
        },
        ARRAY => unsafe {
            let size = (a.duckdb_array_type_array_size)(t.0) as usize;
            let child = (a.duckdb_array_vector_get_child)(v);
            let items: Vec<String> = (row * size..(row + 1) * size)
                .map(|i| render_in(child, i, true, notes))
                .collect();
            format!("[{}]", items.join(", "))
        },
        STRUCT => unsafe {
            let n = (a.duckdb_struct_type_child_count)(t.0);
            let parts: Vec<String> = (0..n)
                .map(|i| {
                    let name = free_cstr((a.duckdb_struct_type_child_name)(t.0, i));
                    format!(
                        "{}: {}",
                        quote(&name),
                        render_in((a.duckdb_struct_vector_get_child)(v, i), row, true, notes)
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(", "))
        },
        UNION => unsafe {
            let tagv = (a.duckdb_struct_vector_get_child)(v, 0);
            let tag = at::<u8>((a.duckdb_vector_get_data)(tagv) as *const u8, row) as u64;
            render_in(
                (a.duckdb_struct_vector_get_child)(v, tag + 1),
                row,
                nested,
                notes,
            )
        },
        ENUM => unsafe {
            let internal = (a.duckdb_enum_internal_type)(t.0);
            let idx = match internal {
                UTINYINT => at::<u8>(d, row) as u64,
                USMALLINT => at::<u16>(d, row) as u64,
                _ => at::<u32>(d, row) as u64,
            };
            leaf(
                free_cstr((a.duckdb_enum_dictionary_value)(t.0, idx)),
                nested,
            )
        },
        INTERVAL => {
            // SAFETY: the vector holds a `duckdb_interval` per row.
            let iv: IntervalT = unsafe { at(d, row) };
            leaf(interval_text(&iv), nested)
        }
        BIT => {
            let b = string_bytes(d, row);
            let pad = b.first().copied().unwrap_or(0) as usize;
            let bits: String = b
                .iter()
                .skip(1)
                .flat_map(|byte| {
                    (0..8)
                        .rev()
                        .map(move |i| if (byte >> i) & 1 == 1 { '1' } else { '0' })
                })
                .collect();
            leaf(bits[pad.min(bits.len())..].to_string(), nested)
        }
        VARCHAR => leaf(
            String::from_utf8_lossy(&string_bytes(d, row)).into_owned(),
            nested,
        ),
        BIGNUM => leaf(bignum_text(&string_bytes(d, row)), nested),
        BLOB | GEOMETRY => leaf(blob_text(&string_bytes(d, row)), nested),
        TIME_TZ => {
            // SAFETY: a `TIME WITH TIME ZONE` is a packed u64: 40 bits of micros, 24 of offset.
            let bits = unsafe { at::<u64>(d, row) };
            let micros = (bits >> 24) as i64;
            let off = 57599 - (bits & 0xFF_FFFF) as i64;
            let sign = if off < 0 { '-' } else { '+' };
            let o = off.abs();
            leaf(
                format!(
                    "{}{sign}{:02}{}",
                    fmt_time(micros / 1_000_000, micros % 1_000_000),
                    o / 3600,
                    if o % 3600 != 0 {
                        format!(":{:02}", (o / 60) % 60)
                    } else {
                        String::new()
                    }
                ),
                nested,
            )
        }
        FLOAT => {
            // SAFETY: a FLOAT vector holds f32.
            let f = unsafe { at::<f32>(d, row) };
            leaf(fmt_float(format!("{f:?}")), nested)
        }
        DOUBLE => {
            // SAFETY: a DOUBLE vector holds f64.
            let f = unsafe { at::<f64>(d, row) };
            leaf(fmt_float(format!("{f:?}")), nested)
        }
        BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | UTINYINT | USMALLINT | UINTEGER
        | UBIGINT | TIMESTAMP | DATE | TIME | HUGEINT | DECIMAL | TIMESTAMP_S | TIMESTAMP_MS
        | TIMESTAMP_NS | UUID | UHUGEINT | TIMESTAMP_TZ | TIME_NS => {
            let text = match cell(v, &ColumnType::of(&t), row, notes) {
                Ok(SqlValue::Text(s)) => s,
                Ok(SqlValue::Bool(b)) => b.to_string(),
                Ok(SqlValue::Int(i)) => i.to_string(),
                Ok(SqlValue::UInt(i)) => i.to_string(),
                Ok(SqlValue::Decimal(x)) => x.to_string(),
                Ok(SqlValue::Uuid(u)) => u.to_string(),
                Ok(SqlValue::Date(x)) => x.format("%Y-%m-%d").to_string(),
                Ok(SqlValue::Time(x)) => {
                    use chrono::Timelike;
                    fmt_time(
                        x.num_seconds_from_midnight() as i64,
                        (x.nanosecond() / 1000) as i64,
                    )
                }
                // The session runs in UTC (see `DuckdbConnector::open`).
                Ok(SqlValue::Timestamp(x)) => fmt_timestamp(&x),
                Ok(SqlValue::TimestampTz(x)) => format!("{}+00", fmt_timestamp(&x.naive_utc())),
                Ok(SqlValue::Float(f)) => fmt_float(format!("{f:?}")),
                Ok(SqlValue::Json(s)) => s,
                Ok(SqlValue::Bytes(_)) | Ok(SqlValue::Null) => "NULL".into(),
                Err(e) => format!("<{e}>"),
            };
            leaf(text, nested)
        }
        other => format!("<DUCKDB_TYPE_{other}>"),
    }
}

/// DuckDB's text of an interval: `1 year 2 months 3 days 04:00:00.5`.
fn interval_text(iv: &IntervalT) -> String {
    let unit = |n: i32, name: &str| format!("{n} {name}{}", if n.abs() == 1 { "" } else { "s" });
    let mut parts = Vec::new();
    if iv.months / 12 != 0 {
        parts.push(unit(iv.months / 12, "year"));
    }
    if iv.months % 12 != 0 {
        parts.push(unit(iv.months % 12, "month"));
    }
    if iv.days != 0 {
        parts.push(unit(iv.days, "day"));
    }
    if iv.micros != 0 || parts.is_empty() {
        let abs = iv.micros.unsigned_abs() as i64;
        let time = fmt_time(abs / 1_000_000, abs % 1_000_000);
        parts.push(if iv.micros < 0 {
            format!("-{time}")
        } else {
            time
        });
    }
    parts.join(" ")
}

/// DuckDB's text of a blob: printable ASCII as is, every other byte (and `\`, `'`, `"`) as `\xHH`.
fn blob_text(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if (32..=126).contains(&b) && !matches!(b, b'\\' | b'\'' | b'"') {
                (b as char).to_string()
            } else {
                format!("\\x{b:02X}")
            }
        })
        .collect()
}
