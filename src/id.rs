//! Human-readable agent IDs, generated the same way as nchat nicks:
//! `<adjective>-<noun>-<number>`, e.g. `gentle-otter-72`.

use rand::seq::SliceRandom;
use rand::Rng;

pub const ADJECTIVES: &[&str] = &[
    "swift", "brave", "calm", "clever", "eager", "gentle", "jolly", "lucky", "mellow", "nimble",
    "quiet", "sunny", "witty", "zesty",
];

pub const NOUNS: &[&str] = &[
    "otter", "fox", "hawk", "lynx", "badger", "heron", "ibex", "raven", "stoat", "tapir", "wombat",
    "gecko", "puffin", "marmot",
];

/// Generates a random agent ID.
pub fn generate() -> String {
    generate_with(&mut rand::thread_rng())
}

pub fn generate_with(rng: &mut impl Rng) -> String {
    format!(
        "{}-{}-{}",
        ADJECTIVES.choose(rng).unwrap(),
        NOUNS.choose(rng).unwrap(),
        rng.gen_range(10..100)
    )
}

/// Whether `id` is acceptable as an agent ID: non-empty, at most 64 bytes, and
/// made only of ASCII letters, digits, `-`, `_` and `.`.
pub fn is_valid(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_have_nchat_shape() {
        for _ in 0..500 {
            let id = generate();
            let parts: Vec<&str> = id.split('-').collect();
            assert_eq!(parts.len(), 3, "{id}");
            assert!(ADJECTIVES.contains(&parts[0]), "{id}");
            assert!(NOUNS.contains(&parts[1]), "{id}");
            let n: u32 = parts[2].parse().unwrap();
            assert!((10..100).contains(&n), "{id}");
            assert!(is_valid(&id));
        }
    }

    #[test]
    fn validates_ids() {
        assert!(is_valid("gentle-otter-72"));
        assert!(is_valid("reviewer_1.local"));
        assert!(!is_valid(""));
        assert!(!is_valid("has space"));
        assert!(!is_valid("slash/y"));
        assert!(!is_valid(&"a".repeat(65)));
    }
}
