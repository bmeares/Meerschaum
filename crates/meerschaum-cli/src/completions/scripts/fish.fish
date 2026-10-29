# mrsm shell completions (managed by `mrsm install completions`; version {{VERSION}})
#
# Fish completion for `mrsm` and `meerschaum`. The candidates come from the program itself
# (`_MRSM_COMPLETE=fish`): nothing is run and nothing is written.
function __mrsm_complete
    set -l words (commandline -opc)
    set -l current (commandline -ct)
    set -l joined (string join \x1f -- $words "$current")
    set -l out (env _MRSM_COMPLETE=fish "_MRSM_COMPLETE_WORDS=$joined" $words[1] 2>/dev/null </dev/null)
    test (count $out) -ge 1; or return
    switch $out[1]
        case files
            __fish_complete_path "$current"
        case candidates
            if test (count $out) -gt 1
                printf '%s\n' $out[2..-1]
            end
    end
end

complete -c mrsm -f -a '(__mrsm_complete)'
complete -c meerschaum -f -a '(__mrsm_complete)'
