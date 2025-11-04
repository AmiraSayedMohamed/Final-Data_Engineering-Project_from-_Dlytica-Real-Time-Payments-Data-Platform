# Helper script to run dbt seed, run, and test (PowerShell)
param(
    [string]$profilePath = "./profiles.yml"
)

Set-Location -LiteralPath "D:\projects\Dlytica Final Project\dbt_project"

Write-Host "Ensure you have installed dbt-core and dbt-postgres in this environment."

Write-Host "Running dbt seed..."
dbt seed --profiles-dir . --profiles-file $profilePath

Write-Host "Running dbt run..."
dbt run --profiles-dir . --profiles-file $profilePath

Write-Host "Running dbt test..."
dbt test --profiles-dir . --profiles-file $profilePath

Write-Host "dbt complete."
