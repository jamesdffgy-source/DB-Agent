param(
    [switch]$Record,
    [string]$Summary = ""
)

$runner = Join-Path $PSScriptRoot "run_python.cmd"
$arguments = @((Join-Path $PSScriptRoot "project_gate.py"))
if ($Record) {
    $arguments += "--record"
    $arguments += "--summary"
    $arguments += $Summary
}

& $runner @arguments
exit $LASTEXITCODE
