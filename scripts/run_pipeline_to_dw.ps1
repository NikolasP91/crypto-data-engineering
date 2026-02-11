Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$envFile = Join-Path $projectRoot ".env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }

        $name, $value = $line -split "=", 2
        if (-not $name -or $null -eq $value) {
            return
        }

        $name = $name.Trim()
        $value = $value.Trim().Trim('"')

        if (-not [string]::IsNullOrWhiteSpace($name) -and [string]::IsNullOrEmpty([Environment]::GetEnvironmentVariable($name, "Process"))) {
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

$requiredFiles = @(
    "ddl/01_create_tables.sql",
    "ddl/02_create_staging.sql",
    "ddl/03_populate_dw.sql"
)

foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        throw "Required file not found: $file"
    }
}

$psqlCandidates = @()

if ($env:PSQL_PATH) {
    $psqlCandidates += $env:PSQL_PATH
}

$psqlCmd = Get-Command psql -ErrorAction SilentlyContinue
if ($psqlCmd) {
    $psqlCandidates += $psqlCmd.Source
}

$pgSearchRoots = @("C:\Program Files\PostgreSQL", "D:\Program Files\PostgreSQL", "E:\Program Files\PostgreSQL")
foreach ($root in $pgSearchRoots) {
    if (Test-Path $root) {
        $found = Get-ChildItem -Path $root -Recurse -Filter "psql.exe" -ErrorAction SilentlyContinue |
            Sort-Object FullName -Descending |
            Select-Object -First 1
        if ($found) {
            $psqlCandidates += $found.FullName
        }
    }
}

$psqlExe = $psqlCandidates |
    Where-Object { $_ -and (Test-Path $_) } |
    Select-Object -First 1

if (-not $psqlExe) {
    throw "psql was not found. Set PSQL_PATH to your psql.exe location or add psql to PATH."
}

$pythonExe = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    $pythonExe = "python"
}

Write-Host "Using psql: $psqlExe"
Write-Host "[1/5] Running ETL (extract -> transform -> processed CSV)..."
& $pythonExe run_etl.py
if ($LASTEXITCODE -ne 0) {
    throw "ETL run failed."
}

$latestCsv = Get-ChildItem -Path "data\processed" -Filter "crypto_market_tidy_*.csv" |
    Sort-Object LastWriteTimeUtc -Descending |
    Select-Object -First 1

if (-not $latestCsv) {
    throw "No processed CSV found under data/processed."
}

$databaseTarget = if ($env:DATABASE_URL) { $env:DATABASE_URL } elseif ($env:PGDATABASE) { $env:PGDATABASE } else { "postgres" }

Write-Host "[2/5] Ensuring DW tables exist..."
& $psqlExe -v ON_ERROR_STOP=1 -d $databaseTarget -f "ddl/01_create_tables.sql"

Write-Host "[3/5] Recreating staging table..."
& $psqlExe -v ON_ERROR_STOP=1 -d $databaseTarget -f "ddl/02_create_staging.sql"

$csvPath = $latestCsv.FullName.Replace('\\', '/')

Write-Host "[4/5] Loading latest CSV into staging: $($latestCsv.Name)"
& $psqlExe -v ON_ERROR_STOP=1 -d $databaseTarget -c "\\copy stg_crypto_market_tidy (extracted_at_utc, coin_id, symbol, name, vs_currency, open_time, close_time, open_price, high_price, low_price, close_price, volume, quote_asset_volume, number_of_trades) FROM '$csvPath' WITH (FORMAT csv, HEADER true)"

Write-Host "[5/5] Populating DW dimensions and fact..."
& $psqlExe -v ON_ERROR_STOP=1 -d $databaseTarget -f "ddl/03_populate_dw.sql"

Write-Host "Pipeline completed through DW population."
Write-Host "Processed file: $csvPath"
