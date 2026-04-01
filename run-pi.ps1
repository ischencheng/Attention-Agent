param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PromptArgs
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $repoRoot

$nodePath = "C:\Program Files\nodejs"
if (Test-Path $nodePath) {
    $env:Path = "$nodePath;$env:Path"
}

$envFile = Join-Path $repoRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) { return }

        $parts = $line.Split("=", 2)
        if ($parts.Length -ne 2) { return }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim().Trim("'`"")
        if ($name) {
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

$piCmd = Join-Path $repoRoot "node_modules\.bin\pi.cmd"
if (-not (Test-Path $piCmd)) {
    Write-Error "pi is not installed locally. Run: `"`"C:\Program Files\nodejs\npm.cmd`" install @mariozechner/pi-coding-agent"
}

if ($PromptArgs -and $PromptArgs.Length -gt 0) {
    & $piCmd @PromptArgs
} else {
    & $piCmd
}
