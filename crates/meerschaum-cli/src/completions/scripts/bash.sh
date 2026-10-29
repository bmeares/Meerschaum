# mrsm shell completions (managed by `mrsm install completions`; version {{VERSION}})
#
# Bash completion for `mrsm` and `meerschaum`. The candidates come from the program itself
# (`_MRSM_COMPLETE=bash`): nothing is run and nothing is written. Works with bash 3.2 and later,
# with or without the bash-completion package.
_mrsm_completion() {
    local line cur out kind cand raw tail ch skip quote first sq sqrep dqbang
    line="${COMP_LINE:0:COMP_POINT}"
    cur="${COMP_WORDS[COMP_CWORD]}"
    COMPREPLY=()
    # No stdin: a program that does not know this protocol (an older mrsm) must not start an
    # interactive shell on the terminal and swallow the keystrokes.
    out="$(_MRSM_COMPLETE=bash _MRSM_COMPLETE_LINE="$line" "${COMP_WORDS[0]}" 2>/dev/null </dev/null)" || return 0
    kind="${out%%$'\n'*}"
    case "$kind" in
        files)
            if declare -F _filedir >/dev/null 2>&1; then
                _filedir
            else
                type compopt >/dev/null 2>&1 && compopt -o filenames 2>/dev/null
                local IFS=$'\n'
                COMPREPLY=($(compgen -f -- "$cur"))
            fi
            return 0
            ;;
        candidates*)
            # The header carries the quote left open in the word being completed, if any.
            quote="${kind#candidates}"
            quote="${quote#$'\t'}"
            ;;
        *) return 0 ;;
    esac
    # `:` and `=` split words in COMP_WORDS (COMP_WORDBREAKS): the shell replaces only what
    # follows the last one, so drop the part of each candidate that is already typed.
    raw="${line##*[[:space:]]}"
    skip=0
    case "$raw" in
        *[\'\"\\]*)
            # An open quote starts what the shell replaces: drop what precedes it (`--flag=`).
            if [ -n "$quote" ]; then
                tail="${raw%"$quote"*}"
                skip=${#tail}
            fi
            ;;
        *)
            tail="$raw"
            for ch in : =; do
                case "$COMP_WORDBREAKS" in
                    *"$ch"*) tail="${tail##*$ch}" ;;
                esac
            done
            skip=$(( ${#raw} - ${#tail} ))
            ;;
    esac
    sq="'"
    sqrep="'\\''"
    dqbang="\"'!'\""
    first=1
    while IFS= read -r cand; do
        if [ -n "$first" ]; then
            first=
            continue
        fi
        cand="${cand%%$'\t'*}"
        [ -n "$cand" ] || continue
        cand="${cand:$skip}"
        [ -n "$cand" ] || continue
        # Escape for the context the candidate lands in: a single-quoted word, a double-quoted
        # word, or bare (where `printf %q` quotes every character the shell treats specially).
        case "$quote" in
            "'") cand="${cand//$sq/$sqrep}" ;;
            '"')
                cand="${cand//\\/\\\\}"
                cand="${cand//\"/\\\"}"
                cand="${cand//\$/\\\$}"
                cand="${cand//\`/\\\`}"
                # History expansion still sees a `!` inside double quotes: close the quote, put the
                # `!` in single quotes, and reopen (readline closes the last quote itself).
                cand="${cand//!/$dqbang}"
                # History expansion still sees a `!` inside double quotes: close the quote, put the
                # `!` in single quotes, and reopen (readline closes the last quote itself).
                ;;
            *) printf -v cand '%q' "$cand" ;;
        esac
        COMPREPLY+=("$cand")
    done <<<"$out"
    return 0
}
complete -F _mrsm_completion mrsm meerschaum
