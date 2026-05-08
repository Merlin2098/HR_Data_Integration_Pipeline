[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$MakePath,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$MakeArguments
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Step {
    param(
        [string]$Message,
        [ConsoleColor]$Color = [ConsoleColor]::Yellow
    )

    Write-Host $Message -ForegroundColor $Color
}

function Write-Phase {
    param(
        [string]$Title
    )

    Write-Host ""
    Write-Host "=== $Title ===" -ForegroundColor Cyan
}

function New-MakeCommand {
    param(
        [string]$Command,
        [string]$Description,
        [bool]$RequiresPathValidation
    )

    return [pscustomobject]@{
        Command = $Command
        Description = $Description
        RequiresPathValidation = $RequiresPathValidation
    }
}

function Assert-MakePath {
    param(
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "make.exe not found at '$Path'."
    }
}

function Test-MakeCommand {
    param(
        [pscustomobject]$MakeCommand
    )

    if ($MakeCommand.RequiresPathValidation) {
        Assert-MakePath -Path $MakeCommand.Command
    }

    & $MakeCommand.Command --version *> $null
    return $LASTEXITCODE -eq 0
}

function Find-MakeFromPath {
    $command = Get-Command make -ErrorAction SilentlyContinue
    if ($null -ne $command -and $command.CommandType -eq "Application") {
        return New-MakeCommand -Command $command.Source -Description "make from PATH" -RequiresPathValidation $true
    }

    return $null
}

function Find-MakeWithWhere {
    $whereResults = & where.exe make.exe 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $whereResults) {
        return $null
    }

    foreach ($result in $whereResults) {
        if ([string]::IsNullOrWhiteSpace($result)) {
            continue
        }

        $trimmedResult = $result.Trim()
        if (Test-Path -LiteralPath $trimmedResult) {
            return New-MakeCommand -Command $trimmedResult -Description "make.exe discovered with where.exe" -RequiresPathValidation $true
        }
    }

    return $null
}

function Find-MakeExecutable {
    $searchRoots = @(
        (Join-Path $HOME "tools"),
        $env:LOCALAPPDATA,
        $env:ProgramFiles,
        ${env:ProgramFiles(x86)}
    ) | Where-Object { $_ -and (Test-Path -LiteralPath $_) }

    foreach ($root in $searchRoots) {
        $match = Get-ChildItem -Path $root -Filter "make.exe" -File -Recurse -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -ne $match) {
            return New-MakeCommand -Command $match.FullName -Description "make.exe discovered under '$root'" -RequiresPathValidation $true
        }
    }

    return $null
}

function Resolve-MakeCommand {
    param(
        [string]$ExplicitMakePath
    )

    $attemptedResolvers = @()

    if ($ExplicitMakePath) {
        $attemptedResolvers += "explicit path '$ExplicitMakePath'"
        $makeCommand = New-MakeCommand -Command $ExplicitMakePath -Description "explicit path '$ExplicitMakePath'" -RequiresPathValidation $true
        if (-not (Test-MakeCommand -MakeCommand $makeCommand)) {
            throw "make.exe at '$ExplicitMakePath' did not respond correctly."
        }
        return $makeCommand
    }

    $resolvers = @(
        @{ Name = "make from PATH"; Action = { Find-MakeFromPath } },
        @{ Name = "where.exe make.exe"; Action = { Find-MakeWithWhere } },
        @{ Name = "filesystem search for make.exe"; Action = { Find-MakeExecutable } }
    )

    foreach ($resolver in $resolvers) {
        $attemptedResolvers += $resolver.Name
        $candidate = & $resolver.Action
        if ($null -eq $candidate) {
            continue
        }

        if (Test-MakeCommand -MakeCommand $candidate) {
            return $candidate
        }
    }

    $attemptedText = $attemptedResolvers -join ", "
    throw "Unable to resolve a working make executable. Tried: $attemptedText. Install/configure make or pass -MakePath."
}

Write-Step "Starting Windows make wrapper." ([ConsoleColor]::Cyan)

Write-Phase "Phase 1: Resolve make"
$makeCommand = Resolve-MakeCommand -ExplicitMakePath $MakePath
Write-Step "[make] Using executable resolved via $($makeCommand.Description)." ([ConsoleColor]::DarkCyan)

if (-not $MakeArguments) {
    Write-Phase "Phase 2: Summary"
    Write-Host "Resolved make executable: $($makeCommand.Command)"
    Write-Host "No make target was provided. Pass any make arguments to execute a target."
    exit 0
}

Write-Phase "Phase 2: Execute"
Write-Step "[make] Running target or arguments: $($MakeArguments -join ' ')" ([ConsoleColor]::Yellow)
& $makeCommand.Command @MakeArguments
if ($LASTEXITCODE -ne 0) {
    $joinedArguments = $MakeArguments -join " "
    throw "Command failed: $($makeCommand.Command) $joinedArguments"
}

Write-Phase "Phase 3: Summary"
Write-Step "make command completed successfully." ([ConsoleColor]::Green)
