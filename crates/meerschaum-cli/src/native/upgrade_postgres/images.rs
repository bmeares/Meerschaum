//! Image references: which image of the same family runs another PostgreSQL major.
//!
//! The stack's `db` image names its PostgreSQL major in the tag for the images Meerschaum has
//! shipped in its stack (`timescale/timescaledb-ha:pg17`, `timescale/timescaledb:latest-pg16`)
//! and the common upstream images (`postgres:17`, `postgis/postgis:17-3.5`). Only tags that float
//! with their major are mapped: a pinned build (`pg17.5-ts2.20.3`, `postgres:17.6`, a digest)
//! cannot be moved to another major without choosing a build, so the user names the image.

use std::sync::LazyLock;

use regex::Regex;

/// An image family whose floating tags carry the PostgreSQL major.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// `timescale/timescaledb-ha:pg<N>[-oss|-all|-all-oss]` (the stack's default since v3).
    TimescaleHa,
    /// `timescale/timescaledb:latest-pg<N>[-oss]` (the stack's default before v3).
    Timescale,
    /// `postgres:<N>[-<variant>]`.
    Postgres,
    /// `postgis/postgis:<N>-<x.y>[-<variant>]`.
    Postgis,
}

/// An image reference split into its repository (with any registry), tag and digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    pub repository: String,
    pub tag: Option<String>,
    pub digest: Option<String>,
}

impl ImageRef {
    /// Split `[registry[:port]/]path[:tag][@digest]`.
    pub fn parse(image: &str) -> ImageRef {
        let image = image.trim();
        let (rest, digest) = match image.split_once('@') {
            Some((rest, digest)) => (rest, Some(digest.to_string())),
            None => (image, None),
        };
        // A `:` after the last `/` is the tag; one before it is a registry port.
        let last_slash = rest.rfind('/').map(|i| i + 1).unwrap_or(0);
        match rest[last_slash..].rfind(':') {
            Some(i) => ImageRef {
                repository: rest[..last_slash + i].to_string(),
                tag: Some(rest[last_slash + i + 1..].to_string()),
                digest,
            },
            None => ImageRef {
                repository: rest.to_string(),
                tag: None,
                digest,
            },
        }
    }

    /// The repository without Docker Hub's implicit prefixes (`docker.io/`, `library/`).
    pub fn short_repository(&self) -> String {
        let mut repo = self.repository.as_str();
        for prefix in ["docker.io/", "index.docker.io/", "registry-1.docker.io/"] {
            if let Some(rest) = repo.strip_prefix(prefix) {
                repo = rest;
                break;
            }
        }
        repo.strip_prefix("library/").unwrap_or(repo).to_string()
    }

    /// The family of the repository, if it is one whose tags are mapped.
    pub fn family(&self) -> Option<Family> {
        match self.short_repository().as_str() {
            "timescale/timescaledb-ha" => Some(Family::TimescaleHa),
            "timescale/timescaledb" => Some(Family::Timescale),
            "postgres" => Some(Family::Postgres),
            "postgis/postgis" => Some(Family::Postgis),
            _ => None,
        }
    }
}

static HA_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^pg(\d+)(-oss|-all|-all-oss)?$").expect("regex"));
static TIMESCALE_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^latest-pg(\d+)(-oss)?$").expect("regex"));
static POSTGRES_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d+)((?:-[a-z][a-z0-9.]*)?)$").expect("regex"));
static POSTGIS_TAG: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(\d+)-(\d+\.\d+)((?:-[a-z][a-z0-9.]*)?)$").expect("regex"));

/// The PostgreSQL major a floating tag of a known family names (`pg17` -> 17).
pub fn tag_major(image: &str) -> Option<u32> {
    let parsed = ImageRef::parse(image);
    let tag = parsed.tag.as_deref()?;
    let re: &Regex = match parsed.family()? {
        Family::TimescaleHa => &HA_TAG,
        Family::Timescale => &TIMESCALE_TAG,
        Family::Postgres => &POSTGRES_TAG,
        Family::Postgis => &POSTGIS_TAG,
    };
    re.captures(tag)?.get(1)?.as_str().parse().ok()
}

/// The image of the same family and variant for PostgreSQL `major`, or why there is none.
pub fn derive_target(image: &str, major: u32) -> Result<String, String> {
    let parsed = ImageRef::parse(image);
    let unknown = || {
        format!(
            "Cannot tell which image runs PostgreSQL {major} from '{image}': only the floating \
             tags of timescale/timescaledb-ha (pg<N>), timescale/timescaledb (latest-pg<N>), \
             postgres (<N>) and postgis/postgis (<N>-<x.y>) are mapped."
        )
    };
    if parsed.digest.is_some() {
        return Err(unknown());
    }
    let Some(tag) = parsed.tag.as_deref() else {
        return Err(unknown());
    };
    let new_tag = match parsed.family().ok_or_else(unknown)? {
        Family::TimescaleHa => {
            let c = HA_TAG.captures(tag).ok_or_else(unknown)?;
            format!("pg{major}{}", c.get(2).map(|m| m.as_str()).unwrap_or(""))
        }
        Family::Timescale => {
            let c = TIMESCALE_TAG.captures(tag).ok_or_else(unknown)?;
            format!(
                "latest-pg{major}{}",
                c.get(2).map(|m| m.as_str()).unwrap_or("")
            )
        }
        Family::Postgres => {
            let c = POSTGRES_TAG.captures(tag).ok_or_else(unknown)?;
            format!("{major}{}", c.get(2).map(|m| m.as_str()).unwrap_or(""))
        }
        Family::Postgis => {
            let c = POSTGIS_TAG.captures(tag).ok_or_else(unknown)?;
            format!(
                "{major}-{}{}",
                &c[2],
                c.get(3).map(|m| m.as_str()).unwrap_or("")
            )
        }
    };
    Ok(format!("{}:{new_tag}", parsed.repository))
}

/// What the action word names: a PostgreSQL major (`18`, `pg18`) or an image reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetSpec {
    /// The default: the major of the default stack image.
    Default,
    /// A major, mapped to an image of the current image's family.
    Major(u32),
    /// An image named explicitly.
    Image(String),
}

/// Parse the action word after `upgrade postgres`.
pub fn parse_target(word: Option<&str>) -> Result<TargetSpec, String> {
    let Some(word) = word.map(str::trim).filter(|w| !w.is_empty()) else {
        return Ok(TargetSpec::Default);
    };
    let digits = word
        .strip_prefix("pg")
        .or_else(|| word.strip_prefix("PG"))
        .unwrap_or(word);
    if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
        return digits
            .parse::<u32>()
            .ok()
            .filter(|m| (10..1000).contains(m))
            .map(TargetSpec::Major)
            .ok_or_else(|| format!("'{word}' is not a PostgreSQL major version."));
    }
    if word.contains(':') || word.contains('/') || word.contains('@') {
        return Ok(TargetSpec::Image(word.to_string()));
    }
    Err(format!(
        "'{word}' is neither a PostgreSQL major version (e.g. `18`) nor an image (e.g. \
         `timescale/timescaledb-ha:pg18`)."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_split_registry_ports_tags_and_digests() {
        let r = ImageRef::parse("timescale/timescaledb-ha:pg17");
        assert_eq!(r.repository, "timescale/timescaledb-ha");
        assert_eq!(r.tag.as_deref(), Some("pg17"));
        let r = ImageRef::parse("registry.local:5000/team/postgres:17-alpine");
        assert_eq!(r.repository, "registry.local:5000/team/postgres");
        assert_eq!(r.tag.as_deref(), Some("17-alpine"));
        assert_eq!(r.family(), None);
        let r = ImageRef::parse("docker.io/library/postgres:18");
        assert_eq!(r.family(), Some(Family::Postgres));
        let r = ImageRef::parse("postgres@sha256:abc");
        assert_eq!(r.tag, None);
        assert_eq!(r.digest.as_deref(), Some("sha256:abc"));
        let r = ImageRef::parse("registry.local:5000/postgres");
        assert_eq!(r.repository, "registry.local:5000/postgres");
        assert_eq!(r.tag, None);
    }

    #[test]
    fn floating_tags_map_to_the_target_major() {
        let cases = [
            (
                "timescale/timescaledb-ha:pg17",
                "timescale/timescaledb-ha:pg18",
            ),
            (
                "timescale/timescaledb-ha:pg17-all",
                "timescale/timescaledb-ha:pg18-all",
            ),
            (
                "timescale/timescaledb-ha:pg16-oss",
                "timescale/timescaledb-ha:pg18-oss",
            ),
            (
                "timescale/timescaledb:latest-pg16-oss",
                "timescale/timescaledb:latest-pg18-oss",
            ),
            (
                "timescale/timescaledb:latest-pg17",
                "timescale/timescaledb:latest-pg18",
            ),
            ("postgres:17", "postgres:18"),
            ("postgres:16-alpine", "postgres:18-alpine"),
            (
                "docker.io/library/postgres:17",
                "docker.io/library/postgres:18",
            ),
            ("postgis/postgis:17-3.5", "postgis/postgis:18-3.5"),
            (
                "postgis/postgis:17-3.5-alpine",
                "postgis/postgis:18-3.5-alpine",
            ),
        ];
        for (from, to) in cases {
            assert_eq!(derive_target(from, 18).as_deref(), Ok(to), "{from}");
        }
    }

    #[test]
    fn pinned_and_unknown_images_are_refused() {
        for image in [
            "timescale/timescaledb-ha:pg17.5-ts2.20.3",
            "timescale/timescaledb:2.17.2-pg17",
            "postgres:17.6",
            "postgres",
            "postgres:latest",
            "postgres@sha256:0123",
            "bitnami/postgresql:17",
            "postgis/postgis:17",
        ] {
            assert!(derive_target(image, 18).is_err(), "{image}");
        }
    }

    #[test]
    fn majors_come_from_floating_tags() {
        assert_eq!(tag_major("timescale/timescaledb-ha:pg18"), Some(18));
        assert_eq!(tag_major("timescale/timescaledb:latest-pg16-oss"), Some(16));
        assert_eq!(tag_major("postgres:17-alpine"), Some(17));
        assert_eq!(tag_major("postgis/postgis:17-3.5"), Some(17));
        assert_eq!(tag_major("postgres:17.6"), None);
        assert_eq!(tag_major("example/db:pg17"), None);
    }

    #[test]
    fn action_words_name_a_major_or_an_image() {
        assert_eq!(parse_target(None), Ok(TargetSpec::Default));
        assert_eq!(parse_target(Some("18")), Ok(TargetSpec::Major(18)));
        assert_eq!(parse_target(Some("pg18")), Ok(TargetSpec::Major(18)));
        assert_eq!(
            parse_target(Some("postgis/postgis:18-3.6")),
            Ok(TargetSpec::Image("postgis/postgis:18-3.6".into()))
        );
        assert!(parse_target(Some("eighteen")).is_err());
        assert!(parse_target(Some("pg")).is_err());
        assert!(parse_target(Some("9")).is_err());
    }
}
