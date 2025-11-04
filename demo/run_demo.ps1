# Demo script to bring up the stack and run a short pipeline test.
param(
    [int]$producerRate = 20,
    [int]$producerCount = 200
)

Set-Location -LiteralPath "D:\projects\Dlytica Final Project"

Write-Host "Starting Docker Compose..."
docker compose up -d

Write-Host "Creating Kafka topics (sleep 5s to let services start)"
Start-Sleep -s 5
bash infra/kafka/create_topics.sh kafka

Write-Host "Starting producer in background (will send $producerCount events at $producerRate eps)"
Start-Process -NoNewWindow -FilePath pwsh -ArgumentList "-Command cd src/producer; . .venv/Scripts/Activate.ps1; python generate_payments.py --rate $producerRate --total $producerCount"

Write-Host "Waiting for producer to finish (sleep 30s)"
Start-Sleep -s 30

Write-Host "Trigger Bronze->Silver DAG via Airflow CLI (if Airflow running)"
docker exec -it $(docker ps --filter "ancestor=apache/airflow" -q | Select-Object -First 1) airflow dags trigger bronze_to_silver_hourly

Write-Host "Run export reports"
python scripts/export_reports.py

Write-Host "Demo finished. Check outputs/ for CSV reports."
