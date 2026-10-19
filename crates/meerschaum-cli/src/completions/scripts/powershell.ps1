# mrsm shell completions (managed by `mrsm install completions`; version {{VERSION}})
#
# PowerShell completion for `mrsm` and `meerschaum` (Windows PowerShell 5.1 and PowerShell 7+).
# The candidates come from the program itself (`_MRSM_COMPLETE=powershell`): nothing is run and
# nothing is written.
$__mrsmCompleter = {
    param($wordToComplete, $commandAst, $cursorPosition)
    $words = @()
    foreach ($element in $commandAst.CommandElements) {
        if ($element.Extent.StartOffset -ge $cursorPosition) { break }
        if ($element -is [System.Management.Automation.Language.StringConstantExpressionAst]) {
            $words += $element.Value
        } else {
            $words += $element.Extent.Text
        }
    }
    if ($wordToComplete -eq '') {
        $words += ''
    } else {
        $words[$words.Count - 1] = $wordToComplete.Trim("'", '"')
    }
    $program = $words[0]
    $lines = @()
    $previous = $env:_MRSM_COMPLETE_WORDS
    try {
        $env:_MRSM_COMPLETE = 'powershell'
        $env:_MRSM_COMPLETE_WORDS = ($words -join [string][char]0x1f)
        $lines = @('' | & $program 2>$null)
    } catch {
        return
    } finally {
        Remove-Item Env:\_MRSM_COMPLETE -ErrorAction SilentlyContinue
        if ($null -eq $previous) {
            Remove-Item Env:\_MRSM_COMPLETE_WORDS -ErrorAction SilentlyContinue
        } else {
            $env:_MRSM_COMPLETE_WORDS = $previous
        }
    }
    # `files` (or no answer) leaves the completion to PowerShell's own file names.
    if ($lines.Count -lt 2 -or $lines[0] -ne 'candidates') { return }
    foreach ($line in $lines[1..($lines.Count - 1)]) {
        if (-not $line) { continue }
        $parts = $line -split "`t", 2
        $text = $parts[0]
        $tip = $text
        if ($parts.Count -gt 1 -and $parts[1]) { $tip = $parts[1] }
        # Bare only when every character is harmless; otherwise single-quoted, doubling the quote
        # characters (PowerShell also reads the typographic single quotes as quotes).
        $insert = $text
        if ($text -notmatch '^[A-Za-z0-9_:./\\=+%~-]+$') {
            $insert = "'" + [regex]::Replace($text, "['\u2018\u2019\u201a\u201b]", '$0$0') + "'"
        }
        [System.Management.Automation.CompletionResult]::new($insert, $text, 'ParameterValue', $tip)
    }
}
Register-ArgumentCompleter -Native -CommandName 'mrsm', 'meerschaum', 'mrsm.exe', 'meerschaum.exe' -ScriptBlock $__mrsmCompleter
