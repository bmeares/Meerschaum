//! Geometry values: WKB / EWKB / GeoPackage WKB / WKT / GeoJSON, as shapely + GEOS produce them.
//!
//! v4 holds geometries as shapely objects inside pandas object columns and serializes them with
//! `shapely.to_wkb(..., include_srid=True)` (EWKB flags), `shapely.to_wkt` (GEOS writer, 6-digit
//! rounding with trimming) and `shapely.to_geojson`. The kernel stores a parsed [`Geometry`]
//! (coordinates in `f64`, optional Z/M, optional SRID) and reproduces those byte and text formats.
//! The column CRS is carried separately (GeoArrow `geoarrow.wkb` metadata, D10).

use std::fmt::Write as _;

/// Coordinate dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Dims {
    #[default]
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

impl Dims {
    pub fn has_z(self) -> bool {
        matches!(self, Dims::Xyz | Dims::Xyzm)
    }
    pub fn has_m(self) -> bool {
        matches!(self, Dims::Xym | Dims::Xyzm)
    }
    fn width(self) -> usize {
        match self {
            Dims::Xy => 2,
            Dims::Xyz | Dims::Xym => 3,
            Dims::Xyzm => 4,
        }
    }
    fn from_flags(z: bool, m: bool) -> Self {
        match (z, m) {
            (false, false) => Dims::Xy,
            (true, false) => Dims::Xyz,
            (false, true) => Dims::Xym,
            (true, true) => Dims::Xyzm,
        }
    }
}

/// A coordinate (`z`/`m` are present according to the geometry's [`Dims`]).
pub type Coord = Vec<f64>;

/// The shape of a geometry.
#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    /// `None` is `POINT EMPTY`.
    Point(Option<Coord>),
    LineString(Vec<Coord>),
    Polygon(Vec<Vec<Coord>>),
    MultiPoint(Vec<Option<Coord>>),
    MultiLineString(Vec<Vec<Coord>>),
    MultiPolygon(Vec<Vec<Vec<Coord>>>),
    GeometryCollection(Vec<Geometry>),
}

/// A geometry with dimensions and an optional SRID (shapely's `get_srid`).
#[derive(Clone, Debug, PartialEq)]
pub struct Geometry {
    pub shape: Shape,
    pub dims: Dims,
    pub srid: Option<i32>,
}

/// Geometry parsing errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeometryError {
    #[error("Invalid WKB: {0}")]
    Wkb(String),
    #[error("Invalid WKT: {0}")]
    Wkt(String),
    #[error("Invalid GeoJSON: {0}")]
    GeoJson(String),
    #[error("Invalid GeoPackage WKB header: {0}")]
    Gpkg(String),
}

impl Geometry {
    pub fn new(shape: Shape, dims: Dims) -> Self {
        Self {
            shape,
            dims,
            srid: None,
        }
    }

    /// The shapely `geom_type` (`Point`, `LineString`, ...).
    pub fn geom_type(&self) -> &'static str {
        match self.shape {
            Shape::Point(_) => "Point",
            Shape::LineString(_) => "LineString",
            Shape::Polygon(_) => "Polygon",
            Shape::MultiPoint(_) => "MultiPoint",
            Shape::MultiLineString(_) => "MultiLineString",
            Shape::MultiPolygon(_) => "MultiPolygon",
            Shape::GeometryCollection(_) => "GeometryCollection",
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.shape {
            Shape::Point(c) => c.is_none(),
            Shape::LineString(v) => v.is_empty(),
            Shape::Polygon(v) => v.is_empty(),
            Shape::MultiPoint(v) => v.is_empty(),
            Shape::MultiLineString(v) => v.is_empty(),
            Shape::MultiPolygon(v) => v.is_empty(),
            Shape::GeometryCollection(v) => v.is_empty(),
        }
    }

    fn type_code(&self) -> u32 {
        match self.shape {
            Shape::Point(_) => 1,
            Shape::LineString(_) => 2,
            Shape::Polygon(_) => 3,
            Shape::MultiPoint(_) => 4,
            Shape::MultiLineString(_) => 5,
            Shape::MultiPolygon(_) => 6,
            Shape::GeometryCollection(_) => 7,
        }
    }

    // ---------------------------------------------------------------- WKB

    /// `shapely.to_wkb(geom, hex=False, include_srid=include_srid)`: little-endian extended WKB
    /// (Z flag `0x80000000`, M flag `0x40000000`, SRID flag `0x20000000` when the geometry has a
    /// non-zero SRID and `include_srid`).
    pub fn to_wkb(&self, include_srid: bool) -> Vec<u8> {
        let mut out = Vec::new();
        self.write_wkb(&mut out, include_srid);
        out
    }

    fn write_wkb(&self, out: &mut Vec<u8>, include_srid: bool) {
        out.push(1);
        let mut code = self.type_code();
        if self.dims.has_z() {
            code |= 0x8000_0000;
        }
        if self.dims.has_m() {
            code |= 0x4000_0000;
        }
        let srid = if include_srid {
            self.srid.filter(|s| *s != 0)
        } else {
            None
        };
        if srid.is_some() {
            code |= 0x2000_0000;
        }
        out.extend_from_slice(&code.to_le_bytes());
        if let Some(s) = srid {
            out.extend_from_slice(&s.to_le_bytes());
        }
        let width = self.dims.width();
        let write_coord = |out: &mut Vec<u8>, c: &Coord| {
            for i in 0..width {
                out.extend_from_slice(&c.get(i).copied().unwrap_or(f64::NAN).to_le_bytes());
            }
        };
        let write_seq = |out: &mut Vec<u8>, seq: &Vec<Coord>| {
            out.extend_from_slice(&(seq.len() as u32).to_le_bytes());
            for c in seq {
                write_coord(out, c);
            }
        };
        let sub = |shape: Shape| Geometry {
            shape,
            dims: self.dims,
            srid: None,
        };
        match &self.shape {
            Shape::Point(c) => match c {
                Some(c) => write_coord(out, c),
                None => {
                    for _ in 0..width {
                        out.extend_from_slice(&f64::NAN.to_le_bytes());
                    }
                }
            },
            Shape::LineString(seq) => write_seq(out, seq),
            Shape::Polygon(rings) => {
                out.extend_from_slice(&(rings.len() as u32).to_le_bytes());
                for ring in rings {
                    write_seq(out, ring);
                }
            }
            Shape::MultiPoint(points) => {
                out.extend_from_slice(&(points.len() as u32).to_le_bytes());
                for p in points {
                    sub(Shape::Point(p.clone())).write_wkb(out, false);
                }
            }
            Shape::MultiLineString(lines) => {
                out.extend_from_slice(&(lines.len() as u32).to_le_bytes());
                for l in lines {
                    sub(Shape::LineString(l.clone())).write_wkb(out, false);
                }
            }
            Shape::MultiPolygon(polys) => {
                out.extend_from_slice(&(polys.len() as u32).to_le_bytes());
                for p in polys {
                    sub(Shape::Polygon(p.clone())).write_wkb(out, false);
                }
            }
            Shape::GeometryCollection(geoms) => {
                out.extend_from_slice(&(geoms.len() as u32).to_le_bytes());
                for g in geoms {
                    g.write_wkb(out, false);
                }
            }
        }
    }

    /// `shapely.to_wkb(geom, hex=True, include_srid=...)` (uppercase hex).
    pub fn to_wkb_hex(&self, include_srid: bool) -> String {
        hex::encode_upper(self.to_wkb(include_srid))
    }

    /// Parse WKB, EWKB (SRID/Z/M flags) or ISO WKB (`1000`/`2000`/`3000` type offsets),
    /// in either byte order (`shapely.wkb.loads`).
    pub fn from_wkb(bytes: &[u8]) -> Result<Self, GeometryError> {
        let mut reader = WkbReader { buf: bytes, pos: 0 };
        let geom = reader.read_geometry()?;
        if reader.pos != bytes.len() {
            return Err(GeometryError::Wkb("trailing bytes".into()));
        }
        Ok(geom)
    }

    /// Parse hex-encoded WKB (`shapely.wkb.loads(hex_str)`).
    pub fn from_wkb_hex(text: &str) -> Result<Self, GeometryError> {
        let bytes = hex::decode(text.trim()).map_err(|e| GeometryError::Wkb(e.to_string()))?;
        Self::from_wkb(&bytes)
    }

    // ---------------------------------------------------------------- WKT

    /// `shapely.to_wkt(geom)` with GEOS defaults (`rounding_precision=6`, `trim=True`).
    pub fn to_wkt(&self) -> String {
        self.wkt_with_precision(Some(6))
    }

    /// Full-precision WKT (`shapely.to_wkt(geom, rounding_precision=-1)`, also `str(geom)`).
    pub fn to_wkt_full(&self) -> String {
        self.wkt_with_precision(None)
    }

    fn wkt_with_precision(&self, precision: Option<usize>) -> String {
        let mut out = String::new();
        self.write_wkt(&mut out, precision, true);
        out
    }

    fn write_wkt(&self, out: &mut String, precision: Option<usize>, with_tag: bool) {
        let dims_tag = match self.dims {
            Dims::Xy => "",
            Dims::Xyz => " Z",
            Dims::Xym => " M",
            Dims::Xyzm => " ZM",
        };
        if with_tag {
            out.push_str(&self.geom_type().to_ascii_uppercase());
            out.push_str(dims_tag);
        }
        if self.is_empty() {
            out.push_str(if with_tag { " EMPTY" } else { "EMPTY" });
            return;
        }
        if with_tag {
            out.push(' ');
        }
        let fmt_coord = |c: &Coord| {
            c.iter()
                .map(|v| format_geos_number(*v, precision))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let fmt_seq = |seq: &Vec<Coord>| {
            format!(
                "({})",
                seq.iter().map(fmt_coord).collect::<Vec<_>>().join(", ")
            )
        };
        let fmt_poly = |rings: &Vec<Vec<Coord>>| {
            format!(
                "({})",
                rings.iter().map(fmt_seq).collect::<Vec<_>>().join(", ")
            )
        };
        match &self.shape {
            Shape::Point(Some(c)) => {
                let _ = write!(out, "({})", fmt_coord(c));
            }
            Shape::Point(None) => {}
            Shape::LineString(seq) => out.push_str(&fmt_seq(seq)),
            Shape::Polygon(rings) => out.push_str(&fmt_poly(rings)),
            Shape::MultiPoint(points) => {
                let inner = points
                    .iter()
                    .map(|p| match p {
                        Some(c) => format!("({})", fmt_coord(c)),
                        None => "EMPTY".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = write!(out, "({inner})");
            }
            Shape::MultiLineString(lines) => {
                let inner = lines.iter().map(fmt_seq).collect::<Vec<_>>().join(", ");
                let _ = write!(out, "({inner})");
            }
            Shape::MultiPolygon(polys) => {
                let inner = polys.iter().map(fmt_poly).collect::<Vec<_>>().join(", ");
                let _ = write!(out, "({inner})");
            }
            Shape::GeometryCollection(geoms) => {
                let inner = geoms
                    .iter()
                    .map(|g| g.wkt_with_precision(precision))
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = write!(out, "({inner})");
            }
        }
    }

    /// Parse WKT (`shapely.wkt.loads`), case-insensitive, with optional `Z`/`M`/`ZM` and `EMPTY`.
    /// An optional `SRID=n;` prefix (EWKT) sets the SRID.
    pub fn from_wkt(text: &str) -> Result<Self, GeometryError> {
        let mut parser = WktParser {
            chars: text.as_bytes(),
            pos: 0,
        };
        parser.skip_ws();
        let mut srid = None;
        if parser.peek_word().eq_ignore_ascii_case("SRID") {
            parser.read_word();
            parser.expect(b'=')?;
            let n = parser.read_number()?;
            srid = Some(n as i32);
            parser.expect(b';')?;
        }
        let mut geom = parser.read_geometry(None)?;
        parser.skip_ws();
        if parser.pos != parser.chars.len() {
            return Err(GeometryError::Wkt("trailing characters".into()));
        }
        geom.srid = srid;
        Ok(geom)
    }

    // ---------------------------------------------------------------- GeoJSON

    /// `json.loads(shapely.to_geojson(geom))`.
    pub fn to_geojson(&self) -> serde_json::Value {
        use serde_json::{Value as J, json};
        let coord = |c: &Coord| J::Array(c.iter().map(|v| json_f64(*v)).collect());
        let seq = |s: &Vec<Coord>| J::Array(s.iter().map(coord).collect());
        let poly = |p: &Vec<Vec<Coord>>| J::Array(p.iter().map(seq).collect());
        match &self.shape {
            Shape::Point(c) => json!({
                "type": "Point",
                "coordinates": c.as_ref().map(coord).unwrap_or(J::Array(vec![])),
            }),
            Shape::LineString(s) => json!({"type": "LineString", "coordinates": seq(s)}),
            Shape::Polygon(p) => json!({"type": "Polygon", "coordinates": poly(p)}),
            Shape::MultiPoint(points) => json!({
                "type": "MultiPoint",
                "coordinates": J::Array(points.iter().map(|p| p.as_ref().map(coord).unwrap_or(J::Array(vec![]))).collect()),
            }),
            Shape::MultiLineString(lines) => json!({
                "type": "MultiLineString",
                "coordinates": J::Array(lines.iter().map(seq).collect()),
            }),
            Shape::MultiPolygon(polys) => json!({
                "type": "MultiPolygon",
                "coordinates": J::Array(polys.iter().map(poly).collect()),
            }),
            Shape::GeometryCollection(geoms) => json!({
                "type": "GeometryCollection",
                "geometries": J::Array(geoms.iter().map(|g| g.to_geojson()).collect()),
            }),
        }
    }

    /// `shapely.from_geojson(json.dumps(value))` for a geometry object.
    pub fn from_geojson(value: &serde_json::Value) -> Result<Self, GeometryError> {
        let err = |m: &str| GeometryError::GeoJson(m.to_string());
        let obj = value.as_object().ok_or_else(|| err("not an object"))?;
        let typ = obj
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| err("missing type"))?;
        if typ == "Feature" {
            return Self::from_geojson(obj.get("geometry").ok_or_else(|| err("missing geometry"))?);
        }
        if typ == "GeometryCollection" {
            let geoms = obj
                .get("geometries")
                .and_then(|g| g.as_array())
                .ok_or_else(|| err("missing geometries"))?
                .iter()
                .map(Self::from_geojson)
                .collect::<Result<Vec<_>, _>>()?;
            let dims = geoms.first().map(|g| g.dims).unwrap_or_default();
            return Ok(Geometry::new(Shape::GeometryCollection(geoms), dims));
        }
        let coords = obj
            .get("coordinates")
            .ok_or_else(|| err("missing coordinates"))?;
        let mut width = 2usize;
        let parse_coord =
            |v: &serde_json::Value, width: &mut usize| -> Result<Coord, GeometryError> {
                let arr = v.as_array().ok_or_else(|| err("bad coordinate"))?;
                let c: Vec<f64> = arr
                    .iter()
                    .map(|x| x.as_f64().ok_or_else(|| err("bad number")))
                    .collect::<Result<_, _>>()?;
                if c.len() < 2 {
                    return Err(err("coordinate needs two values"));
                }
                *width = (*width).max(c.len().min(3));
                Ok(c.into_iter().take(3).collect())
            };
        let parse_seq =
            |v: &serde_json::Value, width: &mut usize| -> Result<Vec<Coord>, GeometryError> {
                v.as_array()
                    .ok_or_else(|| err("bad sequence"))?
                    .iter()
                    .map(|c| parse_coord(c, width))
                    .collect()
            };
        let parse_poly =
            |v: &serde_json::Value, width: &mut usize| -> Result<Vec<Vec<Coord>>, GeometryError> {
                v.as_array()
                    .ok_or_else(|| err("bad polygon"))?
                    .iter()
                    .map(|r| parse_seq(r, width))
                    .collect()
            };
        let shape = match typ {
            "Point" => {
                let empty = coords.as_array().is_some_and(|a| a.is_empty());
                Shape::Point(if empty {
                    None
                } else {
                    Some(parse_coord(coords, &mut width)?)
                })
            }
            "LineString" => Shape::LineString(parse_seq(coords, &mut width)?),
            "Polygon" => Shape::Polygon(parse_poly(coords, &mut width)?),
            "MultiPoint" => Shape::MultiPoint(
                coords
                    .as_array()
                    .ok_or_else(|| err("bad multipoint"))?
                    .iter()
                    .map(|c| parse_coord(c, &mut width).map(Some))
                    .collect::<Result<_, _>>()?,
            ),
            "MultiLineString" => Shape::MultiLineString(
                coords
                    .as_array()
                    .ok_or_else(|| err("bad multilinestring"))?
                    .iter()
                    .map(|s| parse_seq(s, &mut width))
                    .collect::<Result<_, _>>()?,
            ),
            "MultiPolygon" => Shape::MultiPolygon(
                coords
                    .as_array()
                    .ok_or_else(|| err("bad multipolygon"))?
                    .iter()
                    .map(|p| parse_poly(p, &mut width))
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(err("unknown type")),
        };
        let dims = if width >= 3 { Dims::Xyz } else { Dims::Xy };
        let mut geom = Geometry::new(shape, dims);
        if dims == Dims::Xyz {
            geom.pad_z();
        }
        Ok(geom)
    }

    fn pad_z(&mut self) {
        fn pad(c: &mut Coord) {
            while c.len() < 3 {
                c.push(f64::NAN);
            }
        }
        match &mut self.shape {
            Shape::Point(Some(c)) => pad(c),
            Shape::LineString(s) => s.iter_mut().for_each(pad),
            Shape::Polygon(p) => p.iter_mut().flatten().for_each(pad),
            Shape::MultiPoint(ps) => ps.iter_mut().flatten().for_each(pad),
            Shape::MultiLineString(ls) => ls.iter_mut().flatten().for_each(pad),
            Shape::MultiPolygon(ps) => ps.iter_mut().flatten().flatten().for_each(pad),
            _ => {}
        }
    }

    // ---------------------------------------------------------------- GeoPackage

    /// `serialize_geometry(geom, 'gpkg_wkb', srid)`: an 8-byte `GP` header (no envelope,
    /// little-endian, empty flag `0x20` for empty geometries) followed by plain WKB.
    pub fn to_gpkg_wkb(&self, srid: Option<i32>) -> Vec<u8> {
        let byte_order: u8 = 1;
        let flags = if self.is_empty() {
            (byte_order & 0x01) | 0x20
        } else {
            byte_order & 0x01
        };
        // PARITY: `srid or -1` in `serialize_geometry` (meerschaum/utils/dtypes/__init__.py).
        let srid_val = srid.filter(|s| *s != 0).unwrap_or(-1);
        let mut out = vec![b'G', b'P', 0, flags];
        out.extend_from_slice(&srid_val.to_le_bytes());
        out.extend_from_slice(&self.to_wkb(false));
        out
    }
}

/// `geometry_is_gpkg()`: bytes starting with `GP`.
pub fn geometry_is_gpkg(value: &[u8]) -> bool {
    value.len() >= 2 && &value[0..2] == b"GP"
}

/// `gpkg_wkb_to_wkb()`: strip a GeoPackage header, returning `(wkb, srid, flags)`.
pub fn gpkg_wkb_to_wkb(bytes: &[u8]) -> Result<(Vec<u8>, i32, u8), GeometryError> {
    if !geometry_is_gpkg(bytes) {
        return Err(GeometryError::Gpkg("missing magic number.".into()));
    }
    let (flags, srid) = if bytes.len() >= 8 {
        (
            bytes[3],
            i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
        )
    } else if bytes.len() >= 6 {
        (bytes[3], i16::from_le_bytes([bytes[4], bytes[5]]) as i32)
    } else {
        return Err(GeometryError::Gpkg(
            "unpack requires a buffer of 6 bytes".into(),
        ));
    };
    let envelope_type = (flags >> 1) & 0x07;
    let envelope_size = match envelope_type {
        1 => 32,
        2 | 3 => 48,
        4 => 64,
        _ => 0,
    };
    let header_length = (8 + envelope_size).min(bytes.len());
    Ok((bytes[header_length..].to_vec(), srid, flags))
}

/// `geometry_is_wkt()` for strings: `Some(true)` for WKT matching v4's regex, `Some(false)` for
/// even-length hex, else `None`.
///
/// PARITY: the v4 regex only accepts `TYPE (` (no `Z`/`M` tag and no `EMPTY`), so
/// `POINT Z (1 2 3)` and `POINT EMPTY` are *not* recognized as WKT and are left unparsed by
/// `attempt_cast_to_geometry` (meerschaum/utils/dtypes/__init__.py, `geometry_is_wkt`).
pub fn geometry_is_wkt_str(value: &str) -> Option<bool> {
    static WKT_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(
            r"(?is)^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)\s*$",
        )
        .expect("valid regex")
    });
    // Python's `re.match(..., re.IGNORECASE)` without DOTALL: `.` does not match newlines.
    if !value.contains('\n') && WKT_RE.is_match(value) {
        return Some(true);
    }
    if value.bytes().all(|b| b.is_ascii_hexdigit()) && value.len().is_multiple_of(2) {
        return Some(false);
    }
    None
}

/// Format a number like GEOS's `WKTWriter` with `trim=True` (GEOS 3.13).
///
/// GEOS rounds the *shortest round-trip* decimal digits half-to-even (not the exact binary
/// value): `2.5000005` → `2.5`, `2.5000015` → `2.500002`. Numbers with `1e-4 <= |x| < 1e17` use
/// fixed notation, others scientific (`1e-7`, `1e+20`); trailing zeros are trimmed and `-0`
/// prints as `0`. `precision=None` keeps every digit (`rounding_precision=-1`).
pub fn format_geos_number(v: f64, precision: Option<usize>) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Inf" } else { "-Inf" }.to_string();
    }
    if v == 0.0 {
        return "0".to_string();
    }
    let sign = if v < 0.0 { "-" } else { "" };
    let sci = format!("{:e}", v.abs());
    let (mant, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let mut exp: i64 = exp.parse().unwrap_or(0);
    let mut digits: Vec<u8> = mant
        .bytes()
        .filter(u8::is_ascii_digit)
        .map(|b| b - b'0')
        .collect();
    let a = v.abs();
    let scientific = !(1e-4..1e17).contains(&a);
    if let Some(p) = precision {
        // Number of significant digits to keep.
        let keep: i64 = if scientific {
            1 + p as i64
        } else {
            exp + 1 + p as i64
        };
        if keep < digits.len() as i64 {
            if keep < 0 {
                digits = vec![0];
                exp = 0;
            } else {
                let keep = keep as usize;
                let rest = &digits[keep..];
                let round_up = match rest.first() {
                    Some(d) if *d > 5 => true,
                    Some(5) => {
                        rest[1..].iter().any(|d| *d != 0)
                            || keep
                                .checked_sub(1)
                                .map(|i| digits[i] % 2 == 1)
                                .unwrap_or(false)
                    }
                    _ => false,
                };
                digits.truncate(keep);
                if round_up {
                    let mut i = digits.len();
                    loop {
                        if i == 0 {
                            digits.insert(0, 1);
                            exp += 1;
                            break;
                        }
                        i -= 1;
                        if digits[i] == 9 {
                            digits[i] = 0;
                        } else {
                            digits[i] += 1;
                            break;
                        }
                    }
                }
                if digits.is_empty() {
                    digits = vec![0];
                }
            }
        }
    }
    while digits.len() > 1 && digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.iter().all(|d| *d == 0) {
        return "0".to_string();
    }
    let ds: String = digits.iter().map(|d| (d + b'0') as char).collect();
    if scientific {
        let (first, rest) = ds.split_at(1);
        let mant = if rest.is_empty() {
            first.to_string()
        } else {
            format!("{first}.{rest}")
        };
        return format!(
            "{sign}{mant}e{}{}",
            if exp < 0 { "-" } else { "+" },
            exp.abs()
        );
    }
    let decpt = exp + 1;
    let n = ds.len() as i64;
    let body = if decpt <= 0 {
        format!("0.{}{}", "0".repeat((-decpt) as usize), ds)
    } else if decpt >= n {
        format!("{}{}", ds, "0".repeat((decpt - n) as usize))
    } else {
        let (x, y) = ds.split_at(decpt as usize);
        format!("{x}.{y}")
    };
    format!("{sign}{body}")
}

fn json_f64(v: f64) -> serde_json::Value {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

struct WkbReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl WkbReader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], GeometryError> {
        if self.pos + n > self.buf.len() {
            return Err(GeometryError::Wkb("unexpected end of buffer".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u32(&mut self, le: bool) -> Result<u32, GeometryError> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("4 bytes");
        Ok(if le {
            u32::from_le_bytes(b)
        } else {
            u32::from_be_bytes(b)
        })
    }

    fn f64(&mut self, le: bool) -> Result<f64, GeometryError> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("8 bytes");
        Ok(if le {
            f64::from_le_bytes(b)
        } else {
            f64::from_be_bytes(b)
        })
    }

    fn read_geometry(&mut self) -> Result<Geometry, GeometryError> {
        let order = self.take(1)?[0];
        let le = match order {
            0 => false,
            1 => true,
            _ => return Err(GeometryError::Wkb(format!("bad byte order {order}"))),
        };
        let raw = self.u32(le)?;
        let mut has_z = raw & 0x8000_0000 != 0;
        let mut has_m = raw & 0x4000_0000 != 0;
        let has_srid = raw & 0x2000_0000 != 0;
        let mut code = raw & 0x0fff_ffff;
        if code >= 1000 {
            match code / 1000 {
                1 => has_z = true,
                2 => has_m = true,
                3 => {
                    has_z = true;
                    has_m = true
                }
                _ => return Err(GeometryError::Wkb(format!("bad type {raw}"))),
            }
            code %= 1000;
        }
        let srid = if has_srid {
            Some(self.u32(le)? as i32)
        } else {
            None
        };
        let dims = Dims::from_flags(has_z, has_m);
        let width = dims.width();
        let read_coord = |r: &mut Self| -> Result<Coord, GeometryError> {
            (0..width).map(|_| r.f64(le)).collect()
        };
        let shape = match code {
            1 => {
                let c = read_coord(self)?;
                Shape::Point(if c.iter().all(|v| v.is_nan()) {
                    None
                } else {
                    Some(c)
                })
            }
            2 => {
                let n = self.u32(le)?;
                Shape::LineString((0..n).map(|_| read_coord(self)).collect::<Result<_, _>>()?)
            }
            3 => {
                let nrings = self.u32(le)?;
                let mut rings = Vec::new();
                for _ in 0..nrings {
                    let n = self.u32(le)?;
                    rings.push((0..n).map(|_| read_coord(self)).collect::<Result<_, _>>()?);
                }
                Shape::Polygon(rings)
            }
            4..=7 => {
                let n = self.u32(le)?;
                let parts: Vec<Geometry> = (0..n)
                    .map(|_| self.read_geometry())
                    .collect::<Result<_, _>>()?;
                match code {
                    4 => Shape::MultiPoint(
                        parts
                            .into_iter()
                            .map(|g| match g.shape {
                                Shape::Point(c) => Ok(c),
                                _ => Err(GeometryError::Wkb("bad multipoint member".into())),
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    5 => Shape::MultiLineString(
                        parts
                            .into_iter()
                            .map(|g| match g.shape {
                                Shape::LineString(s) => Ok(s),
                                _ => Err(GeometryError::Wkb("bad multilinestring member".into())),
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    6 => Shape::MultiPolygon(
                        parts
                            .into_iter()
                            .map(|g| match g.shape {
                                Shape::Polygon(p) => Ok(p),
                                _ => Err(GeometryError::Wkb("bad multipolygon member".into())),
                            })
                            .collect::<Result<_, _>>()?,
                    ),
                    _ => Shape::GeometryCollection(parts),
                }
            }
            _ => return Err(GeometryError::Wkb(format!("unknown geometry type {code}"))),
        };
        Ok(Geometry { shape, dims, srid })
    }
}

struct WktParser<'a> {
    chars: &'a [u8],
    pos: usize,
}

impl WktParser<'_> {
    fn skip_ws(&mut self) {
        while self.pos < self.chars.len() && self.chars[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.chars.get(self.pos).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), GeometryError> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(GeometryError::Wkt(format!(
                "expected '{}' at position {}",
                c as char, self.pos
            )))
        }
    }

    fn peek_word(&mut self) -> String {
        self.skip_ws();
        let start = self.pos;
        let mut end = start;
        while end < self.chars.len() && self.chars[end].is_ascii_alphabetic() {
            end += 1;
        }
        String::from_utf8_lossy(&self.chars[start..end]).to_string()
    }

    fn read_word(&mut self) -> String {
        let w = self.peek_word();
        self.pos += w.len();
        w
    }

    fn read_number(&mut self) -> Result<f64, GeometryError> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.chars.len() {
            let c = self.chars[self.pos];
            if c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+' | b'e' | b'E') {
                self.pos += 1;
            } else {
                break;
            }
        }
        let text = std::str::from_utf8(&self.chars[start..self.pos]).unwrap_or("");
        if text.is_empty() {
            let word = self.peek_word();
            if word.eq_ignore_ascii_case("nan") {
                self.pos += word.len();
                return Ok(f64::NAN);
            }
            if word.eq_ignore_ascii_case("inf") {
                self.pos += word.len();
                return Ok(f64::INFINITY);
            }
        }
        text.parse()
            .map_err(|_| GeometryError::Wkt(format!("bad number '{text}'")))
    }

    fn read_coord(&mut self, dims: &mut Option<Dims>) -> Result<Coord, GeometryError> {
        let mut c = vec![self.read_number()?, self.read_number()?];
        while matches!(self.peek(), Some(b) if b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'n' | b'N' | b'i' | b'I'))
        {
            c.push(self.read_number()?);
        }
        let inferred = match c.len() {
            2 => Dims::Xy,
            3 => Dims::Xyz,
            _ => Dims::Xyzm,
        };
        match dims {
            Some(d) => {
                if d.width() != c.len() {
                    return Err(GeometryError::Wkt("mixed coordinate dimensions".into()));
                }
            }
            None => *dims = Some(inferred),
        }
        Ok(c)
    }

    fn read_seq(&mut self, dims: &mut Option<Dims>) -> Result<Vec<Coord>, GeometryError> {
        if self.peek_word().eq_ignore_ascii_case("EMPTY") {
            self.read_word();
            return Ok(vec![]);
        }
        self.expect(b'(')?;
        let mut out = vec![self.read_coord(dims)?];
        while self.peek() == Some(b',') {
            self.pos += 1;
            out.push(self.read_coord(dims)?);
        }
        self.expect(b')')?;
        Ok(out)
    }

    fn read_list<T>(
        &mut self,
        dims: &mut Option<Dims>,
        mut f: impl FnMut(&mut Self, &mut Option<Dims>) -> Result<T, GeometryError>,
    ) -> Result<Vec<T>, GeometryError> {
        if self.peek_word().eq_ignore_ascii_case("EMPTY") {
            self.read_word();
            return Ok(vec![]);
        }
        self.expect(b'(')?;
        let mut out = vec![f(self, dims)?];
        while self.peek() == Some(b',') {
            self.pos += 1;
            out.push(f(self, dims)?);
        }
        self.expect(b')')?;
        Ok(out)
    }

    fn read_geometry(&mut self, parent_dims: Option<Dims>) -> Result<Geometry, GeometryError> {
        let typ = self.read_word().to_ascii_uppercase();
        let tag = self.peek_word().to_ascii_uppercase();
        let mut dims: Option<Dims> = parent_dims;
        match tag.as_str() {
            "Z" => {
                self.read_word();
                dims = Some(Dims::Xyz)
            }
            "M" => {
                self.read_word();
                dims = Some(Dims::Xym)
            }
            "ZM" => {
                self.read_word();
                dims = Some(Dims::Xyzm)
            }
            _ => {}
        }
        let shape = match typ.as_str() {
            "POINT" => {
                if self.peek_word().eq_ignore_ascii_case("EMPTY") {
                    self.read_word();
                    Shape::Point(None)
                } else {
                    self.expect(b'(')?;
                    let c = self.read_coord(&mut dims)?;
                    self.expect(b')')?;
                    Shape::Point(Some(c))
                }
            }
            "LINESTRING" => Shape::LineString(self.read_seq(&mut dims)?),
            "POLYGON" => Shape::Polygon(self.read_list(&mut dims, |p, d| p.read_seq(d))?),
            "MULTIPOINT" => Shape::MultiPoint(self.read_list(&mut dims, |p, d| {
                if p.peek_word().eq_ignore_ascii_case("EMPTY") {
                    p.read_word();
                    return Ok(None);
                }
                if p.peek() == Some(b'(') {
                    p.pos += 1;
                    let c = p.read_coord(d)?;
                    p.expect(b')')?;
                    Ok(Some(c))
                } else {
                    p.read_coord(d).map(Some)
                }
            })?),
            "MULTILINESTRING" => {
                Shape::MultiLineString(self.read_list(&mut dims, |p, d| p.read_seq(d))?)
            }
            "MULTIPOLYGON" => Shape::MultiPolygon(
                self.read_list(&mut dims, |p, d| p.read_list(d, |p2, d2| p2.read_seq(d2)))?,
            ),
            "GEOMETRYCOLLECTION" => {
                let geoms = self.read_list(&mut dims, |p, _| p.read_geometry(None))?;
                if dims.is_none() {
                    dims = geoms.first().map(|g| g.dims);
                }
                Shape::GeometryCollection(geoms)
            }
            other => {
                return Err(GeometryError::Wkt(format!(
                    "unknown geometry type '{other}'"
                )));
            }
        };
        Ok(Geometry {
            shape,
            dims: dims.unwrap_or_default(),
            srid: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wkt_roundtrip() {
        for (input, out) in [
            ("POINT (1.123456789 2)", "POINT (1.123457 2)"),
            ("POINT Z (1 2 3)", "POINT Z (1 2 3)"),
            ("MULTIPOINT ((0 0), (1 1))", "MULTIPOINT ((0 0), (1 1))"),
            ("MULTIPOINT (0 0, 1 1)", "MULTIPOINT ((0 0), (1 1))"),
            ("POINT EMPTY", "POINT EMPTY"),
            (
                "POINT (100000000000 0.0000001)",
                "POINT (100000000000 1e-7)",
            ),
            ("POINT (-1.5 1e20)", "POINT (-1.5 1e+20)"),
            (
                "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
                "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
            ),
        ] {
            let g = Geometry::from_wkt(input).unwrap();
            assert_eq!(g.to_wkt(), out);
            let back = Geometry::from_wkb(&g.to_wkb(true)).unwrap();
            assert_eq!(back.to_wkt(), out);
        }
    }

    #[test]
    fn geos_rounding() {
        for (v, out) in [
            (2.5000005, "2.5"),
            (2.5000015, "2.500002"),
            (2.5000025, "2.500002"),
            (3.0000045, "3.000004"),
            (123456.1234575, "123456.123458"),
            (99999.9999995, "100000"),
            (1.5e-6, "1.5e-6"),
            (1e16 + 2.0, "10000000000000002"),
            (9.999e-5, "9.999e-5"),
            (-2.5000025, "-2.500002"),
            (1.00000049999, "1"),
            (-0.0, "0"),
        ] {
            assert_eq!(format_geos_number(v, Some(6)), out, "{v}");
        }
    }

    #[test]
    fn wkb_hex() {
        let g = Geometry::from_wkt("POINT Z (1 2 3)").unwrap();
        assert_eq!(
            g.to_wkb_hex(true),
            "0101000080000000000000F03F00000000000000400000000000000840"
        );
        let mut g = Geometry::from_wkt("POINT (1 2)").unwrap();
        g.srid = Some(4326);
        assert_eq!(
            g.to_wkb_hex(true),
            "0101000020E6100000000000000000F03F0000000000000040"
        );
    }
}
