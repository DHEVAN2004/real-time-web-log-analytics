# Real-Time Web Log Analytics and Monitoring System

A desktop observability dashboard for the preprocessed Kaggle web-server access log in this lab workspace.

## Current Implementation

- Streaming preprocessing script: `preprocess_access_log.py`
- Compressed normalized dataset: `access_preprocessed.csv.gz`
- FastAPI analytics backend under `backend/app`
- JavaFX desktop dashboard under `desktop`
- MySQL schema and analytics queries under `database`
- Kafka replay producer and Spark Structured Streaming consumer under `backend/app/streaming`

The raw `access.log` and generated `access_preprocessed.csv.gz` are not included in this repository; the compressed dataset exceeds GitHub's 100 MB per-file limit. To run dataset-dependent features, place an access log at the project root and run `python preprocess_access_log.py` to generate the compressed dataset locally. Local databases, virtual environments, and build output are also excluded.

The application replays the real preprocessed dataset, publishes events to Kafka, processes the Kafka topic with Spark, and reads indexed logs and alerts from MySQL. Service status in the desktop sidebar is read from live backend health checks.

## Prerequisites

- Python 3.11+
- Java 17+
- Maven 3.9+
- MySQL 8+
- Apache Spark 4.2.0 (set `SPARK_HOME` if it is not installed at `C:\spark`)
- Internet access on first launch to download the Apache Kafka 4.1.1 distribution and Spark's Kafka connector

## Run The Application

```powershell
.\scripts\start_application.bat
```

The launcher verifies the existing MySQL service, starts a local single-node Kafka KRaft broker if needed, starts the FastAPI backend, and opens the JavaFX desktop dashboard. On first launch, Maven downloads Kafka's runtime dependencies under `%LOCALAPPDATA%\WebLogAnalytics`; Kafka data, service logs, Spark checkpoints, and Spark status are stored outside the source tree. The Kafka launcher preserves existing broker metadata and refuses to format an unrecognized non-empty data directory. Install backend dependencies once with `pip install -r backend\requirements.txt`.

The API and interactive docs are available at `http://127.0.0.1:8000` and `http://127.0.0.1:8000/docs`.

## Run Services Individually

```powershell
.\scripts\start_dependencies.ps1
cd backend
$env:DATASET_PATH = "..\access_preprocessed.csv.gz"
python -m app
```

In another terminal:

```powershell
cd desktop
mvn javafx:run
```

MySQL connection settings, Kafka bootstrap servers/topic, Spark installation/version, and backend settings can be overridden with environment variables or `backend\.env` (copy `.env.example` and enter the local database password). The desktop status view shows live backend, dataset, Kafka, Spark, and MySQL health. Spark is optional; when `SPARK_HOME` is configured, the backend starts a Spark Structured Streaming consumer and reports startup or processing failures rather than marking it healthy just because its configuration exists.

The Dashboard and Analytics views use the exact full-dataset aggregate file, while log search reads the existing indexed database records. The Live Monitoring view is explicitly a paced replay of historical data; it is not a live production access-log tail. New live events can be validated and persisted through `POST /api/ingest/log`. Incoming events are fingerprinted for duplicate protection and include source, request, and ingestion metadata. Latency analytics remain unavailable unless the source contains real response-time measurements.

## API Endpoints

- `GET /api/health`
- `GET /api/ready`
- `GET /api/dashboard`
- `GET /api/logs?limit=100&offset=0`
- `GET /api/logs/search?keyword=...&ip=...&method=...&status=...&source_type=...`
- `GET /api/logs/{id}`
- `GET /api/logs/export`
- `POST /api/ingest/log`
- `GET /api/analytics/traffic`
- `GET /api/analytics/status`
- `GET /api/analytics/top-urls`
- `GET /api/analytics/top-ips`
- `GET /api/analytics/methods`
- `GET /api/analytics/bandwidth`
- `GET /api/analytics/latency`
- `GET /api/alerts`
- `POST /api/alerts`
- `PUT /api/alerts/{id}/status`
- `GET /api/stream/status`
- `GET /api/stream/events`
- `GET /api/system/status`

## Tests

```powershell
cd backend
python -m pytest -q tests
cd ..\desktop
mvn -q -DskipTests package
```

Database startup migrations are additive and do not truncate or reload existing tables. The current checked-in dataset index contains 10,365,077 historical requests in aggregates and 100,000 indexed records for log search. Do not run `scripts\build_full_dataset_index.py` against an existing production database without first reviewing its import behavior and taking a backup.

## Project Structure

```text
backend/     FastAPI service, configuration, analytics, Kafka producer
 database/   MySQL schema and queries
desktop/     Maven JavaFX desktop client
data/        Place for future dataset metadata and mappings
scripts/     Local service launch scripts
```

## Troubleshooting

- `503 Dataset not found`: start the backend from `backend` and confirm `DATASET_PATH=../access_preprocessed.csv.gz`.
- `Backend unavailable` in JavaFX: verify the API is running on port 8000 and `BACKEND_URL` matches it.
- Maven cannot resolve JavaFX: verify Java 17+ and internet access for the first Maven dependency download.
- Kafka startup logs are stored at `%LOCALAPPDATA%\WebLogAnalytics\kafka.log`.
- Spark connector download and Structured Streaming logs are stored in `%TEMP%\web_log_spark.log`.
- Spark's Kafka connector version must match the installed Spark version; set `SPARK_VERSION` accordingly.
