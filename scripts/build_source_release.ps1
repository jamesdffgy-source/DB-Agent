param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$')]
    [string]$Version
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$normalizedVersion = $Version.TrimStart('v')
$outputDirectory = Join-Path $projectRoot 'dist'
$archiveName = "DBQuill-$normalizedVersion-source.zip"
$archivePath = Join-Path $outputDirectory $archiveName
$checksumPath = "$archivePath.sha256"

New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
if (Test-Path -LiteralPath $archivePath) {
    Remove-Item -LiteralPath $archivePath -Force
}
if (Test-Path -LiteralPath $checksumPath) {
    Remove-Item -LiteralPath $checksumPath -Force
}

git -C $projectRoot archive --format=zip --prefix="DBQuill-$normalizedVersion/" --output=$archivePath HEAD
if ($LASTEXITCODE -ne 0) {
    throw 'git archive failed'
}

$hash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
Set-Content -LiteralPath $checksumPath -Value "$hash  $archiveName" -Encoding ascii

[pscustomobject]@{
    archive = $archivePath
    sha256 = $hash
    bytes = (Get-Item -LiteralPath $archivePath).Length
} | ConvertTo-Json
