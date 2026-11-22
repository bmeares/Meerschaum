//! Sanitizing of untrusted executor log chunks before they reach a terminal.

/// Sanitize untrusted log chunks across message boundaries. Preserve plain text and bounded
/// SGR colors; discard OSC clipboard/hyperlink sequences, device control and cursor commands.
#[derive(Default)]
pub struct LogSanitizer {
    state: Escape,
}

#[derive(Default)]
enum Escape {
    #[default]
    Ground,
    Start,
    Csi {
        sequence: String,
        valid: bool,
    },
    Osc,
    OscEnd,
    Discard,
    DiscardEnd,
}

impl LogSanitizer {
    /// Feed a chunk, retaining parser state for an escape split across websocket messages.
    pub fn push(&mut self, input: &str) -> String {
        let mut output = String::new();
        for character in input.chars() {
            let state = std::mem::take(&mut self.state);
            self.state = match (state, character) {
                (_, '\u{9c}') => Escape::Ground,
                (Escape::Ground, '\u{1b}') => Escape::Start,
                (Escape::Ground, '\u{9b}') | (Escape::Start, '[') => Escape::Csi {
                    sequence: String::new(),
                    valid: true,
                },
                (Escape::Ground, '\u{9d}') | (Escape::Start, ']') => Escape::Osc,
                (Escape::Start, 'P' | '^' | '_' | 'X') => Escape::Discard,
                (Escape::Start, _) => Escape::Ground,
                (Escape::Csi { sequence, valid }, final_character)
                    if ('@'..='~').contains(&final_character) =>
                {
                    if valid && final_character == 'm' {
                        output.push_str(&format!("\u{1b}[{sequence}m"));
                    }
                    Escape::Ground
                }
                (
                    Escape::Csi {
                        mut sequence,
                        valid,
                    },
                    character,
                ) => {
                    let valid = valid
                        && sequence.len() < 96
                        && (character.is_ascii_digit() || matches!(character, ';' | ':'));
                    if valid {
                        sequence.push(character);
                    }
                    Escape::Csi { sequence, valid }
                }
                (Escape::Osc | Escape::OscEnd, '\u{7}')
                | (Escape::OscEnd | Escape::DiscardEnd, '\\') => Escape::Ground,
                (Escape::Osc | Escape::OscEnd, '\u{1b}') => Escape::OscEnd,
                (Escape::Osc | Escape::OscEnd, _) => Escape::Osc,
                (Escape::Discard | Escape::DiscardEnd, '\u{1b}') => Escape::DiscardEnd,
                (Escape::Discard | Escape::DiscardEnd, _) => Escape::Discard,
                (Escape::Ground, character) => {
                    if !character.is_control() || matches!(character, '\n' | '\r' | '\t') {
                        output.push(character);
                    }
                    Escape::Ground
                }
            };
        }
        output
    }
}
