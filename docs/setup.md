# Local development environment checks and quick setup (Windows / PowerShell)

This file lists the essential tools and quick PowerShell commands to verify or install them. Adjust versions as needed.

Required tools
- Docker (Engine + Compose)
- Python 3.10+
- Java JDK 11+ (for Spark)
- dbt-core and dbt-postgres (pip)

Quick checks (PowerShell)

```powershell
# Docker
docker --version
docker-compose --version

# Python
python --version
pip --version

# Java
java -version

# Optional: verify pip packages in a venv
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt

# dbt (install in venv)
pip install dbt-core dbt-postgres
dbt --version
```

Notes
- On Windows, ensure Docker Desktop is installed and WSL2 backend available for best performance.
- Java is required by PySpark; set JAVA_HOME and add to PATH.
- The project uses Docker Compose to run Kafka, Postgres, Airflow and Spark. Use `docker compose up --build` from the project root to start services.

If you need, I can add a PowerShell script that validates these tools and provides guided install links.
