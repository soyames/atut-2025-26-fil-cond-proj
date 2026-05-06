$ErrorActionPreference = "Stop"

function Run-Step([string]$Command) {
    Write-Output "Running: $Command"
    Invoke-Expression $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Command"
    }
}

Run-Step "uv run python scripts\init_sql_source.py"
Run-Step "uv run python -m src.etl.jobs.run_extract"
Run-Step "uv run python -m src.etl.jobs.run_transform"
Run-Step "uv run python -m src.etl.jobs.run_load"

Write-Output "Pipeline ETL local termine."

