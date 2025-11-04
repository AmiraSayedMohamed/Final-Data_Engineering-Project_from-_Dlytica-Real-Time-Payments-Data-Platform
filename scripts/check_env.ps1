<#
PowerShell helper to validate local environment for the project.
Usage: .\scripts\check_env.ps1
#>

Write-Host "Checking environment..."

Write-Host "Docker:" 
try { & docker --version } catch { Write-Host '  Docker not found' }
Write-Host "Docker Compose:" 
try { & docker compose version } catch { Write-Host '  Docker Compose not found' }
Write-Host "Python:" 
try { & python --version } catch { Write-Host '  Python not found' }
Write-Host "Pip:" 
try { & pip --version } catch { Write-Host '  pip not found' }
Write-Host "Java:" 
try { & java -version } catch { Write-Host '  Java not found' }

# Suggest creating venv
if (-not (Test-Path -Path ".venv")) {
    Write-Host "No .venv found. Create one with:`npython -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt"
}

Write-Host "Check complete. If running on Windows, ensure Docker Desktop is installed and WSL2 is enabled for best results."
