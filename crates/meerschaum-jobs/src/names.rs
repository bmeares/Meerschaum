//! Port of `meerschaum/utils/daemon/_names.py`: random `adjective-noun` job names.

use crate::config::JobPaths;

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
