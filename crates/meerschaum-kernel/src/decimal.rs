//! Python `decimal.Decimal` semantics for the `numeric` dtype.
//!
//! Python stores a decimal as `(sign, coefficient digits, exponent)` and keeps trailing zeros
//! (`Decimal('1.10') != Decimal('1.1')` as strings, though they compare equal numerically).
//! Meerschaum's diff compares numerics by their `f"{x:f}"` string, so the exponent is observable
//! and must be preserved exactly. This module reproduces parsing, `str()`, `:f` formatting and
//! `quantize(..., rounding=ROUND_HALF_UP)` from `meerschaum/utils/dtypes/__init__.py`.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A Python-compatible decimal value.
#[derive(Clone, Debug)]
pub enum Decimal {
    /// A finite value `(-1)**neg * coeff * 10**exp`. `coeff` holds ASCII digits without
    /// leading zeros (except a single `"0"`).
    Finite { neg: bool, coeff: String, exp: i64 },
    /// `Decimal('NaN')` (Meerschaum's null sentinel for numerics).
    NaN,
    /// `Decimal('Infinity')` / `Decimal('-Infinity')`.
    Inf { neg: bool },
}

/// Errors from decimal operations (mirrors `decimal.InvalidOperation`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecimalError {
    #[error("Invalid decimal literal '{0}'")]
    Invalid(String),
    #[error("Cannot quantize value '{value}' to precision={precision}, scale={scale}.")]
    Quantize {
        value: String,
        precision: i64,
        scale: i64,
    },
}

impl Decimal {
    /// Parse like `decimal.Decimal(str)`: surrounding whitespace and `_` digit separators are
    /// accepted, as are `NaN`, `sNaN`, `Inf` and `Infinity` (case-insensitive).
    pub fn parse(text: &str) -> Result<Self, DecimalError> {
        let invalid = || DecimalError::Invalid(text.to_string());
        let s = text.trim();
        if s.is_empty() {
            return Err(invalid());
        }
        let (neg, body) = match s.as_bytes()[0] {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let lower = body.to_ascii_lowercase();
        match lower.as_str() {
            "nan" | "snan" => return Ok(Decimal::NaN),
            "inf" | "infinity" => return Ok(Decimal::Inf { neg }),
            _ => {}
        }
        let body = body.replace('_', "");
        if body.starts_with('_') || body.is_empty() {
            return Err(invalid());
        }
        let (mantissa, exp_part) = match body.find(['e', 'E']) {
            Some(ix) => (&body[..ix], Some(&body[ix + 1..])),
            None => (body.as_str(), None),
        };
        let mut exp: i64 = match exp_part {
            Some(e) => {
                if e.is_empty() {
                    return Err(invalid());
                }
                let (eneg, edigits) = match e.as_bytes()[0] {
                    b'-' => (true, &e[1..]),
                    b'+' => (false, &e[1..]),
                    _ => (false, e),
                };
                if edigits.is_empty() || !edigits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid());
                }
                let v: i64 = edigits.parse().map_err(|_| invalid())?;
                if eneg { -v } else { v }
            }
            None => 0,
        };
        let (int_part, frac_part) = match mantissa.find('.') {
            Some(ix) => (&mantissa[..ix], &mantissa[ix + 1..]),
            None => (mantissa, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err(invalid());
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid());
        }
        exp -= frac_part.len() as i64;
        let digits = format!("{int_part}{frac_part}");
        Ok(Decimal::Finite {
            neg,
            coeff: strip_leading_zeros(&digits),
            exp,
        })
    }

    /// Build a decimal from an `i64` (as `Decimal(str(int))`).
    pub fn from_i64(v: i64) -> Self {
        Decimal::Finite {
            neg: v < 0,
            coeff: v.unsigned_abs().to_string(),
            exp: 0,
        }
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, Decimal::NaN)
    }

    /// Python's `str(Decimal)` (`to_sci_string`).
    pub fn to_sci_string(&self) -> String {
        match self {
            Decimal::NaN => "NaN".to_string(),
            Decimal::Inf { neg } => format!("{}Infinity", if *neg { "-" } else { "" }),
            Decimal::Finite { neg, coeff, exp } => {
                let sign = if *neg { "-" } else { "" };
                let ndigits = coeff.len() as i64;
                let adjusted = exp + ndigits - 1;
                if *exp <= 0 && adjusted >= -6 {
                    if *exp == 0 {
                        return format!("{sign}{coeff}");
                    }
                    let point = ndigits + exp;
                    if point > 0 {
                        let (a, b) = coeff.split_at(point as usize);
                        format!("{sign}{a}.{b}")
                    } else {
                        format!("{sign}0.{}{coeff}", "0".repeat((-point) as usize))
                    }
                } else {
                    let (first, rest) = coeff.split_at(1);
                    let mantissa = if rest.is_empty() {
                        first.to_string()
                    } else {
                        format!("{first}.{rest}")
                    };
                    let esign = if adjusted >= 0 { "+" } else { "-" };
                    format!("{sign}{mantissa}E{esign}{}", adjusted.abs())
                }
            }
        }
    }

    /// Python's `f"{x:f}"` (fixed-point, no exponent).
    pub fn to_plain_string(&self) -> String {
        match self {
            Decimal::NaN => "NaN".to_string(),
            Decimal::Inf { neg } => format!("{}Infinity", if *neg { "-" } else { "" }),
            Decimal::Finite { neg, coeff, exp } => {
                let sign = if *neg { "-" } else { "" };
                if *exp >= 0 {
                    if coeff == "0" {
                        return format!("{sign}0");
                    }
                    return format!("{sign}{coeff}{}", "0".repeat(*exp as usize));
                }
                let ndigits = coeff.len() as i64;
                let point = ndigits + exp;
                if point > 0 {
                    let (a, b) = coeff.split_at(point as usize);
                    format!("{sign}{a}.{b}")
                } else {
                    format!("{sign}0.{}{coeff}", "0".repeat((-point) as usize))
                }
            }
        }
    }

    /// `quantize_decimal()`: quantize to `scale` digits after the point with `ROUND_HALF_UP`,
    /// failing when the result needs more than `precision` digits.
    pub fn quantize(&self, precision: i64, scale: i64) -> Result<Self, DecimalError> {
        let err = || DecimalError::Quantize {
            value: self.to_sci_string(),
            precision,
            scale,
        };
        // PARITY: `quantize_decimal` builds `Decimal('1'*(p-s) + '.' + '1'*s)`; a negative
        // `p - s` makes the quantizer itself invalid only when `p - s < 0` repeats nothing,
        // so the target exponent is always `-scale`.
        match self {
            Decimal::NaN => Ok(Decimal::NaN),
            Decimal::Inf { .. } => Err(err()),
            Decimal::Finite { neg, coeff, exp } => {
                let target = -scale;
                let mut new_coeff;
                if target <= *exp {
                    new_coeff = coeff.clone();
                    if coeff != "0" {
                        new_coeff.push_str(&"0".repeat((*exp - target) as usize));
                    }
                } else {
                    let drop = (target - *exp) as usize;
                    if drop > coeff.len() {
                        new_coeff = "0".to_string();
                    } else {
                        let (keep, dropped) = coeff.split_at(coeff.len() - drop);
                        let round_up = dropped.as_bytes().first().is_some_and(|d| *d >= b'5');
                        new_coeff = if keep.is_empty() {
                            "0".to_string()
                        } else {
                            keep.to_string()
                        };
                        if round_up {
                            new_coeff = increment_digits(&new_coeff);
                        }
                    }
                }
                let new_coeff = strip_leading_zeros(&new_coeff);
                if new_coeff.len() as i64 > precision.max(0) {
                    return Err(err());
                }
                Ok(Decimal::Finite {
                    neg: *neg,
                    coeff: new_coeff,
                    exp: target,
                })
            }
        }
    }

    /// The `(precision, scale)` of a finite value as stored by a database (`digits`, `-exp`).
    pub fn precision_scale(&self) -> Option<(i64, i64)> {
        match self {
            Decimal::Finite { coeff, exp, .. } => {
                let scale = (-exp).max(0);
                let int_digits = (coeff.len() as i64 + exp).max(1);
                Some((int_digits + scale, scale))
            }
            _ => None,
        }
    }

    /// Unscaled `i128` at the given scale (for Arrow/Polars `Decimal128`).
    pub fn to_i128_at_scale(&self, scale: i64) -> Option<i128> {
        let Decimal::Finite { neg, coeff, exp } = self else {
            return None;
        };
        let shift = exp + scale;
        let digits = if shift >= 0 {
            format!("{coeff}{}", "0".repeat(shift as usize))
        } else {
            let drop = (-shift) as usize;
            if drop >= coeff.len() {
                "0".to_string()
            } else {
                coeff[..coeff.len() - drop].to_string()
            }
        };
        let v: i128 = digits.parse().ok()?;
        Some(if *neg { -v } else { v })
    }

    /// Build from an unscaled integer and scale (from Arrow/Polars `Decimal128`).
    pub fn from_i128_scale(v: i128, scale: i64) -> Self {
        Decimal::Finite {
            neg: v < 0,
            coeff: v.unsigned_abs().to_string(),
            exp: -scale,
        }
    }

    /// Lossy conversion to `f64`.
    pub fn to_f64(&self) -> f64 {
        match self {
            Decimal::NaN => f64::NAN,
            Decimal::Inf { neg } => {
                if *neg {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }
            }
            Decimal::Finite { .. } => self.to_sci_string().parse().unwrap_or(f64::NAN),
        }
    }

    /// Numeric comparison (`Decimal.__eq__`/`__lt__`); `None` when either is `NaN`.
    pub fn numeric_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Decimal::NaN, _) | (_, Decimal::NaN) => None,
            (Decimal::Inf { neg: a }, Decimal::Inf { neg: b }) => Some(b.cmp(a)),
            (Decimal::Inf { neg }, _) => Some(if *neg {
                Ordering::Less
            } else {
                Ordering::Greater
            }),
            (_, Decimal::Inf { neg }) => Some(if *neg {
                Ordering::Greater
            } else {
                Ordering::Less
            }),
            (
                Decimal::Finite {
                    neg: an,
                    coeff: ac,
                    exp: ae,
                },
                Decimal::Finite {
                    neg: bn,
                    coeff: bc,
                    exp: be,
                },
            ) => {
                let a_zero = ac == "0";
                let b_zero = bc == "0";
                if a_zero && b_zero {
                    return Some(Ordering::Equal);
                }
                let a_sign = if a_zero {
                    0
                } else if *an {
                    -1
                } else {
                    1
                };
                let b_sign = if b_zero {
                    0
                } else if *bn {
                    -1
                } else {
                    1
                };
                if a_sign != b_sign {
                    return Some(a_sign.cmp(&b_sign));
                }
                let min_exp = (*ae).min(*be);
                let a_digits = format!("{ac}{}", "0".repeat((ae - min_exp) as usize));
                let b_digits = format!("{bc}{}", "0".repeat((be - min_exp) as usize));
                let a_digits = strip_leading_zeros(&a_digits);
                let b_digits = strip_leading_zeros(&b_digits);
                let mag = a_digits
                    .len()
                    .cmp(&b_digits.len())
                    .then_with(|| a_digits.cmp(&b_digits));
                Some(if a_sign < 0 { mag.reverse() } else { mag })
            }
        }
    }
}

impl PartialEq for Decimal {
    /// Structural equality (same digits and exponent), matching how the diff compares
    /// serialized numerics. Use [`Decimal::numeric_cmp`] for numeric equality.
    fn eq(&self, other: &Self) -> bool {
        self.to_sci_string() == other.to_sci_string()
    }
}
impl Eq for Decimal {}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_sci_string().hash(state);
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_sci_string())
    }
}

fn strip_leading_zeros(digits: &str) -> String {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

fn increment_digits(digits: &str) -> String {
    let mut bytes = digits.as_bytes().to_vec();
    let mut i = bytes.len();
    loop {
        if i == 0 {
            bytes.insert(0, b'1');
            break;
        }
        i -= 1;
        if bytes[i] == b'9' {
            bytes[i] = b'0';
        } else {
            bytes[i] += 1;
            break;
        }
    }
    String::from_utf8(bytes).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_and_plain() {
        let cases = [
            ("1.10", "1.10", "1.10"),
            ("1e3", "1E+3", "1000"),
            ("1E-7", "1E-7", "0.0000001"),
            ("0.000001", "0.000001", "0.000001"),
            ("100", "100", "100"),
            ("-0", "-0", "-0"),
            ("0.00", "0.00", "0.00"),
            (" 2 ", "2", "2"),
            ("1_000", "1000", "1000"),
            (
                "123456789.123456789",
                "123456789.123456789",
                "123456789.123456789",
            ),
            ("1e+16", "1E+16", "10000000000000000"),
        ];
        for (input, sci, plain) in cases {
            let d = Decimal::parse(input).unwrap();
            assert_eq!(d.to_sci_string(), sci, "{input}");
            assert_eq!(d.to_plain_string(), plain, "{input}");
        }
    }

    #[test]
    fn quantize_half_up() {
        let q = |s: &str, p, sc| Decimal::parse(s).unwrap().quantize(p, sc);
        assert_eq!(q("1.005", 10, 2).unwrap().to_sci_string(), "1.01");
        assert_eq!(q("-1.005", 10, 2).unwrap().to_sci_string(), "-1.01");
        assert_eq!(q("12.3", 5, 2).unwrap().to_sci_string(), "12.30");
        assert!(q("99.995", 4, 2).is_err());
        assert!(q("123456.789", 5, 2).is_err());
    }
}
