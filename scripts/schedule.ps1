# One-step install and run on Windows: deps, .env, schema, doctor, then a Task Scheduler task that
# keeps `usfin serve` (scheduler + web UI on http://127.0.0.1:8660) running from logon. Windows
# counterpart of install.sh (launchd, macOS). Safe to re-run: re-installing replaces the task.
#
#   powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1 [install|uninstall|status|run] [options]
#
# serve catches up at startup: every scheduled job whose last due fire is later than its last
# ok run is run once, so a first install (or a PC that was off for days) fills in by itself.
# Keep this file ASCII-only: Windows PowerShell 5.1 reads BOM-less scripts as ANSI.
param(
    [Parameter(Position = 0)] [string] $Command = 'install',
    [switch] $NoAgent,
    [switch] $DryRun,
    [switch] $Help,
    [Parameter(ValueFromRemainingArguments = $true)] [string[]] $Rest
)
$ErrorActionPreference = 'Stop'

$TaskName = 'usfin-serve'
$Root = Split-Path -Parent $PSScriptRoot
$Log = Join-Path $Root 'data\usfin-serve.log'
$Launcher = Join-Path $PSScriptRoot 'schedule-hidden.js'
$Port = 8660

function Show-Usage {
    @"
Usage: powershell -ExecutionPolicy Bypass -File scripts\schedule.ps1 [install|uninstall|status|run] [options]

  install     (default) uv sync, check .env, usfin db migrate, usfin doctor, then register (or
              replace) the task '$TaskName' and start it
  uninstall   stop serve and remove the task (data\, raw\ and .env are kept)
  status      show the task state, last run, last result, the uv it calls and whether
              http://127.0.0.1:$Port answers
  run         start the task now (if serve is not already running)

Options (install):
  -NoAgent    everything but the task: start serve by hand with 'uv run usfin serve'
  -DryRun     print the task action that install would register, and register nothing
  -Help       show this help

The task runs ``uv run usfin serve`` in $Root at logon, with no window (through
schedule-hidden.js), appends to $Log and restarts serve if it exits. The job times
(config\schedule.yaml, America/New_York) are kept by serve itself, not by Task Scheduler.
The task calls uv by its full path: re-run install if uv moves.
"@
}

function Say([string] $msg) { Write-Output "==> $msg" }
function Warn([string] $msg) { [Console]::Error.WriteLine("warning: $msg") }
function Fail([string] $msg, [int] $code = 1) {
    [Console]::Error.WriteLine("error: $msg")
    exit $code
}
function Usage-Error([string] $msg) {
    [Console]::Error.WriteLine("error: $msg")
    [Console]::Error.WriteLine((Show-Usage | Out-String))
    exit 2
}

function Test-Serve {
    try {
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 "http://127.0.0.1:$Port/" | Out-Null
        return $true
    } catch {
        return $false
    }
}

# Stop the task and the serve process tree under it (Stop-ScheduledTask ends only wscript.exe,
# and serve must be gone before migrate takes the DB lock).
function Stop-Serve {
    $procs = @(Get-CimInstance Win32_Process)
    $roots = @($procs | Where-Object { $_.Name -eq 'wscript.exe' -and $_.CommandLine -like "*$Launcher*" })
    $tree = @()
    $queue = [System.Collections.Queue]::new()
    foreach ($p in $roots) { $queue.Enqueue($p) }
    while ($queue.Count) {
        $p = $queue.Dequeue()
        $tree += $p
        foreach ($c in $procs | Where-Object { $_.ParentProcessId -eq $p.ProcessId }) { $queue.Enqueue($c) }
    }
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Stop-ScheduledTask -TaskName $TaskName
    }
    foreach ($p in $tree) { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
    for ($i = 0; $i -lt 20 -and (Test-Serve); $i++) { Start-Sleep -Milliseconds 500 }
}

# Required keys must be non-empty in .env or the environment (real env wins, as in usfin).
function Get-Key([string] $key) {
    $val = [Environment]::GetEnvironmentVariable($key)
    if (-not $val) {
        $line = Get-Content -LiteralPath (Join-Path $Root '.env') | Where-Object { $_ -like "$key=*" } |
            Select-Object -Last 1
        if ($line) { $val = $line.Substring($key.Length + 1) }
    }
    return $val
}

if ($Help) { Show-Usage; exit 0 }
if ($Rest) {
    $what = if ($Rest[0] -like '-*') { 'option' } else { 'argument' }
    Usage-Error "unknown ${what}: $($Rest[0])"
}
if ($Command -notin 'install', 'uninstall', 'status', 'run') { Usage-Error "unknown command: $Command" }

$Task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue

switch ($Command) {
    'uninstall' {
        Say "stopping $TaskName"
        Stop-Serve
        if ($Task) { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false }
        Say "removed $TaskName (data\, raw\ and .env kept)"
        exit 0
    }
    'status' {
        if (-not $Task) { Write-Output "$TaskName is not registered" }
        else {
            $info = Get-ScheduledTaskInfo -TaskName $TaskName
            $action = $Task.Actions[0]
            $taskUv = if ($action.Arguments -match '"([^"]*uv\.exe)"') { $Matches[1] } else { $null }
            Write-Output "state: $($Task.State)"
            Write-Output "last run: $($info.LastRunTime)    last result: $($info.LastTaskResult)"
            Write-Output "trigger: at logon of $($Task.Triggers[0].UserId)"
            Write-Output "action: $($action.Execute) $($action.Arguments)"
            Write-Output "uv: $taskUv"
            if (-not $taskUv -or -not (Test-Path -LiteralPath $taskUv)) {
                Warn "the task's uv ($taskUv) no longer exists; re-run: scripts\schedule.ps1 install"
            }
        }
        $up = if (Test-Serve) { 'answering' } else { 'not answering' }
        Write-Output "serve: http://127.0.0.1:$Port $up"
        Write-Output "log: $Log"
        exit 0
    }
    'run' {
        if (-not $Task) { Fail "$TaskName is not registered; run scripts\schedule.ps1 install" }
        if (Test-Serve) { Say "serve already answers on http://127.0.0.1:$Port"; exit 0 }
        Start-ScheduledTask -TaskName $TaskName
        Say "started; follow with: Get-Content -Wait '$Log'"
        exit 0
    }
}

$Uv = (Get-Command uv -ErrorAction SilentlyContinue).Source
if (-not $Uv) { Fail 'uv not found on PATH: install it (https://docs.astral.sh/uv/), then rerun' }

$ActionArgs = "//E:JScript //B //Nologo `"$Launcher`" `"$Log`" `"$Uv`" run usfin serve"
if ($DryRun) {
    Write-Output "action: wscript.exe $ActionArgs"
    Write-Output "    in: $Root"
    exit 0
}

Set-Location -LiteralPath $Root

Say 'uv sync'
& $Uv sync --frozen
if ($LASTEXITCODE) { exit $LASTEXITCODE }

if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item '.env.example' '.env'
    Fail 'created .env from .env.example: fill in SEC_USER_AGENT and BLS_CONTACT_EMAIL, then rerun'
}
$missing = @('SEC_USER_AGENT', 'BLS_CONTACT_EMAIL') | Where-Object { -not (Get-Key $_) }
if ($missing) { Fail "fill in $($missing -join ' ') in $Root\.env, then rerun" }
if (-not (Get-Key 'FRED_API_KEY')) {
    Say 'warning: FRED_API_KEY is empty - job fred (layer-0 series) will be skipped'
}

# A running serve holds the DB between jobs only briefly, but migrate must not race it.
if (-not $NoAgent) { Stop-Serve }

Say 'usfin db migrate'
& $Uv run usfin db migrate
if ($LASTEXITCODE) { exit $LASTEXITCODE }
Say 'usfin doctor'
& $Uv run usfin doctor
if ($LASTEXITCODE) { exit $LASTEXITCODE }

if ($NoAgent) {
    Say "done (no task): start it by hand with 'uv run usfin serve'"
    exit 0
}

Say "registering $TaskName"
New-Item -ItemType Directory -Force -Path (Join-Path $Root 'data') | Out-Null
$Action = New-ScheduledTaskAction -Execute 'wscript.exe' -WorkingDirectory $Root -Argument $ActionArgs
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
# no time limit, restart if serve exits (launchd KeepAlive), one instance at a time
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
    -Description "usfin serve (scheduler + web UI on http://127.0.0.1:$Port) from $Root" -Force |
    Out-Null
Start-ScheduledTask -TaskName $TaskName

Say "waiting for http://127.0.0.1:$Port"
for ($i = 0; $i -lt 60; $i++) {
    if (Test-Serve) {
        Say "running: http://127.0.0.1:$Port (log: $Log)"
        Say 'startup catch-up runs any job that missed its due fire; watch it on the jobs page'
        exit 0
    }
    Start-Sleep 1
}
Fail "serve did not answer on port $Port within 60 s; see $Log"
