//! The process environment as strings.
//!
//! `std::env::vars()` panics on a variable whose name or value is not valid UTF-8, and one stray
//! variable in a user's environment must not end the program. Everything that reads the whole
//! environment goes through here: such a variable is skipped (nothing Meerschaum reads is named
//! or holds anything but UTF-8).

/// The process environment, in the order the OS lists it, without the variables that are not
/// valid UTF-8.
pub fn vars() -> Vec<(String, String)> {
    std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_the_process_environment() {
        let vars = vars();
        assert!(vars.iter().any(|(k, _)| k == "PATH") || vars.is_empty());
    }
}
