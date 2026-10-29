//! The vendored xterm.js bundle (and the terminal's own script and style), embedded
//! gzip-compressed (`build.rs`) and served from `/webterm/assets/<name>`. (`/dash/assets/` is the
//! Dash console's.)
//!
//! A client that accepts gzip gets the embedded bytes as they are; any other client gets them
//! inflated. Every response carries `Vary: Accept-Encoding`, an exact `Content-Length`, and a
//! strong `ETag` per representation (`"<hash>"` identity, `"<hash>-gzip"` gzip) with
//! `Cache-Control: no-cache`, so a browser revalidates (a cheap 304) and a new build is never
//! hidden behind a stale cache.
use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use std::io::Read;

pub(crate) struct Asset {
    pub name: &'static str,
    pub mime: &'static str,
    /// Hex prefix of the SHA-256 of the uncompressed file.
    pub hash: &'static str,
    pub raw_len: usize,
    pub gzip: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/assets_gen.rs"));

pub(crate) fn lookup(name: &str) -> Option<&'static Asset> {
    ASSETS.iter().find(|asset| asset.name == name)
}

/// Whether `Accept-Encoding` allows a gzip response (`gzip`/`x-gzip`, or `*`, with a non-zero q).
pub(crate) fn accepts_gzip(headers: &HeaderMap) -> bool {
    let (mut gzip, mut star): (Option<f32>, Option<f32>) = (None, None);
    for value in headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|value| value.to_str().ok())
    {
        for part in value.split(',') {
            let mut pieces = part.split(';');
            let coding = pieces.next().unwrap_or("").trim().to_ascii_lowercase();
            let q = pieces
                .find_map(|piece| {
                    let piece = piece.trim();
                    piece
                        .strip_prefix("q=")
                        .or_else(|| piece.strip_prefix("Q="))
                        .and_then(|q| q.trim().parse::<f32>().ok())
                })
                .unwrap_or(1.0);
            match coding.as_str() {
                "gzip" | "x-gzip" => gzip = Some(q),
                "*" => star = Some(q),
                _ => {}
            }
        }
    }
    gzip.or(star).is_some_and(|q| q > 0.0)
}

/// Whether `If-None-Match` names `etag` (a list, `*`, and weak `W/` prefixes are all handled).
fn not_modified(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate.trim_start_matches("W/") == etag)
}

pub(crate) fn inflate(asset: &Asset) -> Vec<u8> {
    let mut raw = Vec::with_capacity(asset.raw_len);
    flate2::read::GzDecoder::new(asset.gzip)
        .read_to_end(&mut raw)
        .expect("embedded asset is valid gzip");
    raw
}

/// `GET /webterm/assets/{name}`.
pub(crate) async fn serve(req: Request) -> Response {
    let name = req.uri().path().rsplit('/').next().unwrap_or("");
    let Some(asset) = lookup(name) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let gzip = accepts_gzip(req.headers());
    let etag = if gzip {
        format!("\"{}-gzip\"", asset.hash)
    } else {
        format!("\"{}\"", asset.hash)
    };
    let mut response = if not_modified(req.headers(), &etag) {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response
    } else {
        let body: Body = if gzip {
            Body::from(asset.gzip)
        } else {
            Body::from(inflate(asset))
        };
        let length = if gzip {
            asset.gzip.len()
        } else {
            asset.raw_len
        };
        let mut response = Response::new(body);
        let headers = response.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(asset.mime));
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
        if gzip {
            headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }
        response
    };
    let headers = response.headers_mut();
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&etag).expect("ASCII ETag"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_str(value).unwrap(),
        );
        map
    }

    #[test]
    fn accept_encoding_is_parsed_with_q_values() {
        assert!(accepts_gzip(&headers("gzip")));
        assert!(accepts_gzip(&headers("deflate, gzip;q=0.5, br")));
        assert!(accepts_gzip(&headers("x-gzip")));
        assert!(accepts_gzip(&headers("*")));
        assert!(accepts_gzip(&headers("br, *;q=0.1")));
        assert!(!accepts_gzip(&headers("gzip;q=0")));
        assert!(!accepts_gzip(&headers("gzip;q=0, *")));
        assert!(!accepts_gzip(&headers("identity")));
        assert!(!accepts_gzip(&headers("br")));
        assert!(!accepts_gzip(&HeaderMap::new()));
    }

    #[test]
    fn if_none_match_handles_lists_weak_tags_and_star() {
        let mut map = HeaderMap::new();
        map.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"a\", W/\"b-gzip\""),
        );
        assert!(not_modified(&map, "\"a\""));
        assert!(not_modified(&map, "\"b-gzip\""));
        assert!(!not_modified(&map, "\"b\""));
        map.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        assert!(not_modified(&map, "\"anything\""));
        assert!(!not_modified(&HeaderMap::new(), "\"a\""));
    }

    #[test]
    fn embedded_assets_round_trip_to_the_files_in_git() {
        assert_eq!(ASSETS.len(), 6, "{} assets", ASSETS.len());
        for asset in ASSETS {
            let path = concat!(env!("CARGO_MANIFEST_DIR"), "/assets");
            let on_disk = std::fs::read(format!("{path}/{}", asset.name)).unwrap();
            assert_eq!(inflate(asset), on_disk, "{}", asset.name);
            assert_eq!(asset.raw_len, on_disk.len(), "{}", asset.name);
            assert!(
                asset.gzip.len() < on_disk.len() || on_disk.len() < 512,
                "{}",
                asset.name
            );
        }
    }
}
