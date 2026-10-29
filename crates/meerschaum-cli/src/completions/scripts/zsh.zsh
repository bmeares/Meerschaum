#compdef mrsm meerschaum
# mrsm shell completions (managed by `mrsm install completions`; version {{VERSION}})
#
# Zsh completion for `mrsm` and `meerschaum`. The candidates come from the program itself
# (`_MRSM_COMPLETE=zsh`): nothing is run and nothing is written. Installed as `_mrsm` in a
# directory on $fpath, or loaded with `source <(mrsm show completions zsh)` after `compinit`.
_mrsm() {
    local -a out items
    local joined="" w kind line cand desc
    for w in "${(@Q)words[1,CURRENT-1]}"; do
        joined+="${w}"$'\x1f'
    done
    joined+="${(Q)PREFIX}"
    out=("${(@f)$(_MRSM_COMPLETE=zsh _MRSM_COMPLETE_WORDS="$joined" "${(Q)words[1]}" 2>/dev/null </dev/null)}")
    kind="${out[1]}"
    case "$kind" in
        files)
            _files
            return
            ;;
        candidates) ;;
        *) return 1 ;;
    esac
    items=()
    for line in "${(@)out[2,-1]}"; do
        [[ -n "$line" ]] || continue
        cand="${line%%$'\t'*}"
        desc=""
        if [[ "$line" == *$'\t'* ]]; then
            desc="${line#*$'\t'}"
        fi
        [[ -n "$cand" ]] || continue
        cand="${cand//\\/\\\\}"
        cand="${cand//:/\\:}"
        if [[ -n "$desc" ]]; then
            items+=("${cand}:${desc}")
        else
            items+=("${cand}")
        fi
    done
    (( ${#items} )) || return 1
    _describe -t mrsm-candidates 'mrsm' items
}

if [ "$funcstack[1]" = "_mrsm" ]; then
    _mrsm "$@"
else
    compdef _mrsm mrsm meerschaum
fi
