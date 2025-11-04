<#
Run the full pipeline end-to-end from PowerShell.

What this script does:
- Builds Docker images (producer, spark, airflow)
- Starts core services via docker compose
- Waits for Postgres to become available
- Creates Kafka topics (payments.raw, payments.deadletter)
- Runs a finite producer job to inject test events
- Starts the streaming job in a detached container for a short period
- Runs Bronze->Silver and Silver->Gold batch jobs
- Runs dbt seed/run/test inside the Airflow container (Airflow image includes dbt)
- Runs pytest (unit + integration tests) locally
- Exports reports to outputs/
- Tears down the compose stack (on success or failure)

Notes:
- This script assumes Docker Desktop is installed and `docker compose` is available.
- It attempts best-effort retries and prints progress. If anything fails, check the printed logs.
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Run-Command($cmd) {
    Write-Host "> $cmd"
    & cmd /c $cmd
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $cmd (exit $LASTEXITCODE)"
    }
}

Push-Location -LiteralPath (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)\..\

try {
    Write-Host "[1/12] Checking Docker availability..."
    & docker version | Out-Null
    & docker compose version | Out-Null

    Write-Host "[2/12] Building Docker images (producer, spark, airflow)"
    Run-Command "docker compose build producer spark airflow"

    Write-Host "[3/12] Starting core services"
    # start services in detached mode
    Run-Command "docker compose up -d postgres zookeeper kafka airflow spark"

    Write-Host "[4/12] Waiting for Postgres to be ready (timeout: 60s)"
    $pgReady = $false
    for ($i=0; $i -lt 60; $i++) {
        try {
            docker compose exec -T postgres pg_isready -U payments -d payments_dw | Out-Null
            $pgReady = $true; break
        } catch { Start-Sleep -Seconds 1 }
    }
    if (-not $pgReady) { throw "Postgres did not become ready in time" }

    Write-Host "[5/12] Creating Kafka topics"
    # Bitnami kafka path inside container
    $kafkaCmd = "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic payments.raw --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 || true"
    Run-Command "docker compose exec kafka bash -lc \"$kafkaCmd\""
    $kafkaCmd2 = "/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic payments.deadletter --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || true"
    Run-Command "docker compose exec kafka bash -lc \"$kafkaCmd2\""

    Write-Host "[6/12] Running producer to inject events (one-off)"
    Run-Command "docker compose run --rm producer python generate_payments.py --bootstrap-server kafka:9092 --rate 50 --total 500"

    Write-Host "[7/12] Starting streaming job (detached)"
    $streamCid = (& docker compose run -d spark spark-submit --master local[2] src/spark_stream/stream_payments.py).Trim()
    Write-Host "Started streaming container id: $streamCid"

    # Let streaming process events for a short time
    $streamSeconds = 30
    Write-Host "Allowing streaming job to process for $streamSeconds seconds..."
    Start-Sleep -Seconds $streamSeconds

    Write-Host "[8/12] Running Bronze -> Silver batch"
    Run-Command "docker compose run --rm spark spark-submit --master local[2] src/spark_batch/bronze_to_silver.py"

    Write-Host "[9/12] Running Silver -> Gold loader"
    Run-Command "docker compose run --rm spark spark-submit --master local[2] src/spark_batch/silver_to_gold.py"

    Write-Host "[10/12] Running dbt seed/run/test inside Airflow container"
    Run-Command "docker compose exec airflow bash -lc \"cd /opt/airflow/dbt_project && cp profiles.yml.example profiles.yml || true && dbt seed --profiles-dir . --profiles-file profiles.yml && dbt run --profiles-dir . --profiles-file profiles.yml && dbt test --profiles-dir . --profiles-file profiles.yml\""

    Write-Host "[11/12] Running pytest (unit + integration) locally"
    if (-not (Test-Path -Path .venv)) {
        python -m venv .venv
    }
    . .venv\Scripts\Activate.ps1
    pip install --upgrade pip
    pip install -r src/producer/requirements.txt
    pip install pytest sqlalchemy psycopg2-binary pandas
    pytest -q || Write-Host "pytest returned non-zero (check failures)"

    Write-Host "[12/12] Exporting reports"
    python scripts/export_reports.py || Write-Host "Report export failed (check logs)"

    Write-Host "Demo run completed. Reports (if any) are in outputs/"

} catch {
    Write-Error "An error occurred: $_"
} finally {
    Write-Host "Cleaning up: stopping streaming job and bringing down compose stack"
    try {
        if ($streamCid) { Run-Command "docker rm -f $streamCid" }
    } catch { Write-Warning "Could not remove streaming container: $_" }

    try { Run-Command "docker compose down --volumes --remove-orphans" } catch { Write-Warning "docker compose down failed: $_" }

    Pop-Location
}
