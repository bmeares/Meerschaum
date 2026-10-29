//! Port of `meerschaum/utils/daemon/_names.py`: random `adjective-noun` job names.

use std::path::Path;

use crate::config::JobPaths;

/// Why `name` cannot be a job name, if it cannot: a job name becomes a directory under `jobs/`
/// (and a unit and log name), so it must be exactly one ordinary path component. `/`, `\\`,
/// NUL, the empty name, `.` and `..` would let a name (which arrives unfiltered from `DELETE
/// /jobs/{name}`) address a path outside `jobs/`, e.g. the whole root.
///
/// v4 has no such check (`BANNED_CHARS` is only `,;'"$#=*&!`~`); this is a v5 hardening.
pub fn invalid_job_name_reason(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("the name is empty".into());
    }
    if name == "." || name == ".." {
        return Some(format!("'{name}' is a reserved path name"));
    }
    if let Some(c) = name.chars().find(|c| matches!(c, '/' | '\\' | '\0')) {
        let shown = if c == '\0' {
            "NUL".to_string()
        } else {
            c.to_string()
        };
        return Some(format!("({shown}) is not allowed"));
    }
    None
}

/// Whether `dir` is `base/<name>` for the one-component job `name`, resolving symlinks in the
/// parent: the check before any recursive removal.
pub fn is_direct_child(base: &Path, dir: &Path, name: &str) -> bool {
    if invalid_job_name_reason(name).is_some()
        || dir.file_name().and_then(|n| n.to_str()) != Some(name)
    {
        return false;
    }
    match (dir.parent().map(Path::canonicalize), base.canonicalize()) {
        (Some(Ok(parent)), Ok(base)) => parent == base,
        _ => false,
    }
}

/// The adjective categories, in Python's dict order.
pub const ADJECTIVES: &[(&str, &[&str])] = &[
    (
        "colors",
        &[
            "red", "blue", "green", "orange", "pink", "amber", "bright", "dark", "neon",
        ],
    ),
    (
        "sizes",
        &[
            "big", "small", "large", "huge", "tiny", "long", "short", "average", "mini", "micro",
            "maximum", "minimum", "median",
        ],
    ),
    (
        "personalities",
        &[
            "groovy",
            "cool",
            "awesome",
            "nice",
            "fantastic",
            "sweet",
            "great",
            "amazing",
            "terrific",
            "funky",
            "fancy",
            "sneaky",
            "elegant",
            "dreamy",
        ],
    ),
    (
        "emotions",
        &[
            "angry",
            "happy",
            "excited",
            "suspicious",
            "sad",
            "thankful",
            "grateful",
            "satisfied",
            "peaceful",
            "ferocious",
            "content",
        ],
    ),
    (
        "sensations",
        &[
            "sleepy", "awake", "alert", "thirsty", "comfy", "warm", "cold", "chilly", "soft",
            "smooth", "chunky", "hungry",
        ],
    ),
    (
        "materials",
        &[
            "golden", "silver", "metal", "plastic", "wool", "wooden", "nylon", "fuzzy", "silky",
            "suede", "vinyl",
        ],
    ),
    (
        "qualities",
        &[
            "expensive",
            "cheap",
            "premier",
            "best",
            "favorite",
            "better",
            "good",
            "affordable",
            "organic",
            "electric",
        ],
    ),
];

/// The noun categories, in Python's dict order.
pub const NOUNS: &[(&str, &[&str])] = &[
    (
        "animals",
        &[
            "mouse",
            "fox",
            "horse",
            "pig",
            "hippo",
            "elephant",
            "tiger",
            "deer",
            "salmon",
            "gerbil",
            "snake",
            "turtle",
            "rhino",
            "dog",
            "cat",
            "giraffe",
            "rabbit",
            "squirrel",
            "unicorn",
            "lizard",
            "lion",
            "bear",
            "gazelle",
            "whale",
            "dolphin",
            "fish",
            "butterfly",
            "ladybug",
            "fly",
            "shrimp",
            "flamingo",
            "parrot",
            "tuna",
            "panda",
            "lemur",
            "duck",
            "seal",
            "walrus",
            "seagull",
            "iguana",
            "salamander",
            "kitten",
            "puppy",
            "octopus",
        ],
    ),
    (
        "weather",
        &[
            "rain",
            "sun",
            "snow",
            "wind",
            "tornado",
            "hurricane",
            "blizzard",
            "monsoon",
            "storm",
            "shower",
            "hail",
        ],
    ),
    (
        "plants",
        &[
            "tree",
            "flower",
            "vine",
            "fern",
            "palm",
            "palmetto",
            "oak",
            "pine",
            "rose",
            "lily",
            "ivy",
            "leaf",
            "shrubbery",
            "acorn",
            "fruit",
        ],
    ),
    (
        "foods",
        &[
            "pizza",
            "sushi",
            "apple",
            "banana",
            "sandwich",
            "burger",
            "taco",
            "bratwurst",
            "grape",
            "coconut",
            "bread",
            "toast",
        ],
    ),
    (
        "geographies",
        &[
            "ocean",
            "mountain",
            "desert",
            "forest",
            "tundra",
            "savanna",
            "grassland",
            "prairie",
            "lake",
            "city",
            "river",
        ],
    ),
    (
        "vehicles",
        &[
            "car",
            "bike",
            "boat",
            "bus",
            "trolley",
            "tram",
            "plane",
            "skates",
            "skateboard",
            "kayak",
            "canoe",
            "paddleboard",
            "skis",
            "snowboard",
            "truck",
            "bicycle",
            "unicycle",
            "tricycle",
        ],
    ),
    (
        "clothing",
        &[
            "socks", "shirt", "dress", "shoes", "hat", "glasses", "pocket", "shorts", "pants",
            "skirt", "capris", "helmet",
        ],
    ),
    (
        "instruments",
        &[
            "guitar",
            "piano",
            "trombone",
            "drums",
            "viola",
            "violin",
            "trumpet",
            "bass",
            "harmonica",
            "banjo",
        ],
    ),
];

fn choice<T>(items: &[T]) -> &T {
    let mut buf = [0u8; 8];
    let _ = getrandom::fill(&mut buf);
    &items[(u64::from_le_bytes(buf) % items.len() as u64) as usize]
}

/// `generate_random_name(separator)`: a random category, then a random word in it (so words in
/// small categories are likelier, as in Python).
pub fn generate_random_name(separator: &str) -> String {
    let adjective = choice(choice(ADJECTIVES).1);
    let noun = choice(choice(NOUNS).1);
    format!("{adjective}{separator}{noun}")
}

/// `get_new_daemon_name()`: a random name not already used under `jobs/`.
pub fn get_new_daemon_name(paths: &JobPaths) -> String {
    let existing: Vec<String> = std::fs::read_dir(&paths.jobs_dir)
        .map(|d| {
            d.flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    loop {
        let name = generate_random_name("-");
        if !existing.contains(&name) {
            return name;
        }
    }
}
