$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$backendDirectory = Join-Path $projectRoot "backend"
$desktopDirectory = Join-Path $projectRoot "desktop"
$datasetPath = Join-Path $projectRoot "access_preprocessed.csv.gz"

& (Join-Path $PSScriptRoot "start_dependencies.ps1")

$python = (Get-Command python.exe -ErrorAction Stop).Source
$env:DATASET_PATH = $datasetPath
$backendReady = $false
$existingBackend = $null
try {
    $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8000/api/health" -TimeoutSec 3
    if ($response.StatusCode -eq 200) {
        $existingBackend = $response.Content | ConvertFrom-Json
    }
}
catch {
    Write-Host "No running backend was detected; starting FastAPI."
}
if ($existingBackend) {
    if ($existingBackend.version -ne "2.0.0") {
        throw "An older or different backend is already using port 8000. Stop that process, then run this launcher again."
    }
    $backendReady = $true
}
else {
    $backendProcess = Start-Process -FilePath $python -ArgumentList "-m", "app" `
        -WorkingDirectory $backendDirectory -PassThru
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        if ($backendProcess.HasExited) {
            throw "The FastAPI backend exited before becoming ready."
        }
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:8000/api/health" -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                $backendReady = $true
                break
            }
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }
}
if (-not $backendReady) {
    throw "The FastAPI backend did not become ready at http://127.0.0.1:8000."
}

$maven = (Get-Command mvn.cmd -ErrorAction Stop).Source
Start-Process -FilePath $maven -ArgumentList "javafx:run" -WorkingDirectory $desktopDirectory | Out-Null
Write-Host "Application started. Dashboard: JavaFX desktop; API: http://127.0.0.1:8000/docs"
