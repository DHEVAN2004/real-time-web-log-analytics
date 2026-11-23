"""
FastAPI Analytics Backend for Real-Time Web Log Analytics & Monitoring System.
Provides RESTful endpoints for executive dashboard, high-throughput indexed log search,
deep Big Data analytics, proactive SRE alerting, continuous live streaming, and system telemetry.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from contextlib import asynccontextmanager
from functools import lru_cache
from html.parser import HTMLParser
from pathlib import Path
from time import perf_counter
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone

from .config import settings
from .database import (
    add_system_event,
    check_mysql_status,
    create_alert,
    get_alerts,
    get_log_by_id,
    get_system_events,
    persist_stream_events,
    query_logs,
    search_logs,
    update_alert_status,
)
from .database_migrations import initialize_database
from .streaming.spark_processor import get_status as get_spark_status
from .streaming.spark_processor import start_spark_processor, stop_spark_processor
from .streaming.stream_engine import stream_engine

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(application: FastAPI):
    database_status = initialize_database()
    logger.info("Database startup status: %s", database_status)
    stream_engine.start()
    start_spark_processor()
    add_system_event(
        "STARTUP",
        "FastAPI Analytics Backend",
        f"System initialized with {database_status['engine']} persistence",
        database_status["status"],
    )
    try:
        yield
    finally:
        stop_spark_processor()
        stream_engine.stop()


app = FastAPI(
    title="Real-Time Web Log Analytics & Monitoring System",
    description="Big Data analytics and live observability engine for Kaggle Apache access logs.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in settings.cors_origins.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------
# Models
# ---------------------------------------------------------
class PageAnalyzerRequest(BaseModel):
    urls: list[str] = Field(min_length=1, max_length=20)

    @field_validator("urls")
    @classmethod
    def validate_urls(cls, urls: list[str]) -> list[str]:
        for url in urls:
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ValueError(f"Only absolute HTTP(S) URLs are supported: {url}")
        return urls


class AlertStatusUpdate(BaseModel):
    status: str = Field(pattern="^(OPEN|ACKNOWLEDGED|RESOLVED)$")


class CreateAlertRequest(BaseModel):
    alert_type: str = Field(min_length=1, max_length=100)
    severity: str = "WARNING"
    description: str = Field(min_length=1, max_length=2000)
    source: str = Field(default="Manual Rule", max_length=100)


class LogEventRequest(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ip_address: str = Field(min_length=1, max_length=45)
    http_method: str = Field(min_length=1, max_length=30)
    url: str = Field(min_length=1, max_length=8192)
    protocol: str = Field(default="HTTP/1.1", max_length=100)
    status_code: int = Field(ge=100, le=599)
    response_bytes: int = Field(default=0, ge=0)
    response_time: float | None = Field(default=None, ge=0)
    request_id: str | None = Field(default=None, max_length=128)
    user_agent: str | None = Field(default=None, max_length=4096)
    source_identifier: str | None = Field(default=None, max_length=255)


# ---------------------------------------------------------
# Aggregates Cache
# ---------------------------------------------------------
@lru_cache(maxsize=1)
def get_aggregates() -> dict[str, Any]:
    agg_path = settings.aggregates_path
    if not agg_path.is_absolute():
        base_dir = Path(__file__).resolve().parents[2]
        agg_path = base_dir / "data" / "dataset_aggregates.json"

    if not agg_path.is_file():
        raise HTTPException(status_code=503, detail=f"Dataset aggregates not found: {agg_path}")
    try:
        with agg_path.open("r", encoding="utf-8") as aggregate_file:
            data = json.load(aggregate_file)
    except (OSError, json.JSONDecodeError) as exc:
        logger.exception("Could not load dataset aggregates from %s", agg_path)
        raise HTTPException(status_code=503, detail="Dataset aggregate file is unreadable") from exc
    if not isinstance(data, dict) or not isinstance(data.get("dashboard"), dict):
        raise HTTPException(status_code=503, detail="Dataset aggregate file has an invalid structure")
    return data


# ---------------------------------------------------------
# Health & Status
# ---------------------------------------------------------
@app.get("/api/health")
def health() -> dict[str, Any]:
    mysql_info = check_mysql_status()
    stream_info = stream_engine.get_status()
    dataset_status = dataset_health()
    return {
        "status": "online" if dataset_status["status"] == "ONLINE" else "degraded",
        "service": "analytics-backend",
        "version": "2.0.0",
        "mysql_persistence": mysql_info["status"],
        "kafka": stream_info["kafka_status"],
        "spark": get_spark_status()["status"],
        "dataset_pipeline": dataset_status["status"],
        "live_stream": "historical_replay" if stream_info["is_active"] else "paused",
    }


@app.get("/api/analytics/latency")
def latency_metrics() -> dict[str, Any]:
    return {
        "available": False,
        "average_ms": None,
        "median_ms": None,
        "p95_ms": None,
        "detail": "The historical access-log dataset contains no measured response-time field.",
    }


def dataset_health() -> dict[str, str]:
    dataset_path = settings.dataset_path
    if not dataset_path.is_absolute():
        dataset_path = Path.cwd() / dataset_path
    if not dataset_path.is_file():
        dataset_path = Path(__file__).resolve().parents[2] / settings.dataset_path
    aggregate_path = settings.aggregates_path
    if not aggregate_path.is_absolute():
        aggregate_path = Path(__file__).resolve().parents[2] / "data" / "dataset_aggregates.json"
    if not dataset_path.is_file():
        return {"status": "UNAVAILABLE", "detail": f"Dataset not found: {dataset_path}"}
    if not aggregate_path.is_file():
        return {"status": "DEGRADED", "detail": f"Dataset aggregates not found: {aggregate_path}"}
    return {"status": "ONLINE", "detail": str(dataset_path)}


@app.get("/api/ready")
def readiness() -> dict[str, Any]:
    health_status = health()
    if health_status["dataset_pipeline"] != "ONLINE":
        raise HTTPException(status_code=503, detail=health_status)
    return health_status


# ---------------------------------------------------------
# Dashboard Metrics
# ---------------------------------------------------------
@app.get("/api/dashboard")
def dashboard() -> dict[str, Any]:
    data = get_aggregates()
    dash = dict(data.get("dashboard", {}))
    # Augment with live stream telemetry
    stream_status = stream_engine.get_status()
    dash["live_events_per_second"] = stream_status["current_throughput_eps"]
    dash["live_total_streamed"] = stream_status["total_streamed"]
    dash["live_stream_active"] = stream_status["is_active"]
    return dash


# ---------------------------------------------------------
# Logs & Indexed Search
# ---------------------------------------------------------
@app.get("/api/logs")
def logs(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    return query_logs(limit=limit, offset=offset)


@app.get("/api/logs/search")
def search(
    keyword: Optional[str] = None,
    ip: Optional[str] = None,
    method: Optional[str] = None,
    status: Optional[int] = Query(None, ge=100, le=599),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    source_type: Optional[str] = Query(None, pattern="^(historical|replay|live)$"),
    request_id: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    sort_by: str = Query("timestamp", pattern="^(timestamp|status_code|response_bytes|response_time)$"),
    sort_order: str = Query("DESC", pattern="(?i)^(ASC|DESC)$"),
) -> dict[str, Any]:
    if start_time and end_time and start_time > end_time:
        raise HTTPException(status_code=422, detail="start_time must be earlier than end_time")
    return search_logs(
        keyword=keyword,
        ip=ip,
        method=method,
        status=status,
        limit=limit,
        offset=offset,
        source_type=source_type,
        request_id=request_id,
        start_time=start_time.isoformat() if start_time else None,
        end_time=end_time.isoformat() if end_time else None,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@app.get("/api/logs/export")
def export_logs(
    keyword: Optional[str] = None,
    ip: Optional[str] = None,
    method: Optional[str] = None,
    status: Optional[int] = None,
    source_type: Optional[str] = Query(None, pattern="^(historical|replay|live)$"),
    limit: int = Query(1000, ge=1, le=10000),
):
    results = search_logs(
        keyword=keyword, ip=ip, method=method, status=status, source_type=source_type, limit=limit, offset=0
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Timestamp", "IP Address", "Method", "URL", "Protocol", "Status", "Bytes", "User Agent"])
    for item in results.get("items", []):
        writer.writerow([
            item.get("id"),
            item.get("timestamp"),
            item.get("ip_address"),
            item.get("http_method"),
            item.get("url"),
            item.get("protocol"),
            item.get("status_code"),
            item.get("response_bytes"),
            item.get("user_agent"),
        ])
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="web_logs_export.csv"'},
    )


@app.get("/api/logs/{log_id}")
def log_detail(log_id: int) -> dict[str, Any]:
    result = get_log_by_id(log_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Log record {log_id} was not found")
    return result


@app.post("/api/ingest/log", status_code=202)
def ingest_log_event(request: LogEventRequest) -> dict[str, Any]:
    event = request.model_dump()
    event["timestamp"] = request.timestamp.isoformat()
    event["http_method"] = request.http_method.upper()
    event["source_type"] = "live"
    event["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    try:
        inserted = persist_stream_events([event])
    except Exception as exc:
        logger.exception("Could not persist incoming log event")
        raise HTTPException(status_code=503, detail="Log event could not be persisted") from exc
    recorded_event = stream_engine.record_live_event(event)
    return {
        "accepted": True,
        "duplicate": inserted == 0,
        "stream_id": recorded_event["stream_id"],
        "source_type": "live",
    }


# ---------------------------------------------------------
# Big Data Analytics Endpoints
# ---------------------------------------------------------
@app.get("/api/analytics/traffic")
def traffic() -> list[dict[str, Any]]:
    data = get_aggregates()
    series = data.get("traffic_time_series", [])
    if series:
        return series[-60:]
    return []


@app.get("/api/analytics/status")
def status_metrics() -> list[dict[str, Any]]:
    data = get_aggregates()
    return data.get("status_metrics", [])


@app.get("/api/analytics/status-breakdown")
def status_breakdown() -> list[dict[str, Any]]:
    data = get_aggregates()
    return data.get("status_breakdown", [])


@app.get("/api/analytics/top-urls")
def top_urls(limit: int = Query(10, ge=1, le=50)) -> list[dict[str, Any]]:
    data = get_aggregates()
    urls = data.get("top_urls", [])
    return urls[:limit]


@app.get("/api/analytics/top-ips")
def top_ips(limit: int = Query(10, ge=1, le=50)) -> list[dict[str, Any]]:
    data = get_aggregates()
    ips = data.get("top_ips", [])
    return ips[:limit]


@app.get("/api/analytics/methods")
def methods() -> list[dict[str, Any]]:
    data = get_aggregates()
    return data.get("methods_breakdown", [])


@app.get("/api/analytics/bandwidth")
def bandwidth_metrics() -> dict[str, Any]:
    data = get_aggregates()
    dash = data.get("dashboard", {})
    total_bytes = int(dash["total_bytes"])
    total_requests = max(1, int(dash["total_requests"]))
    avg_bytes = round(total_bytes / total_requests, 1)
    return {
        "total_bytes": total_bytes,
        "total_megabytes": round(total_bytes / (1024 * 1024), 2),
        "total_gigabytes": round(total_bytes / (1024 * 1024 * 1024), 2),
        "avg_bytes_per_request": avg_bytes,
        "avg_kb_per_request": round(avg_bytes / 1024, 2),
    }


# ---------------------------------------------------------
# SRE Alerts & Incident Management
# ---------------------------------------------------------
@app.get("/api/alerts")
def alerts(severity: Optional[str] = None, status: Optional[str] = None) -> list[dict[str, Any]]:
    db_alerts = get_alerts(severity=severity, status=status)
    if db_alerts:
        return db_alerts
    # Fallback to cached alerts in aggregates
    data = get_aggregates()
    raw = data.get("alerts", [])
    if severity and severity.upper() != "ALL":
        raw = [a for a in raw if a.get("severity", "").upper() == severity.upper()]
    if status and status.upper() != "ALL":
        raw = [a for a in raw if a.get("status", "").upper() == status.upper()]
    return raw


@app.post("/api/alerts", status_code=201)
def create_manual_alert(request: CreateAlertRequest) -> dict[str, Any]:
    if request.severity.upper() not in {"INFO", "WARNING", "CRITICAL"}:
        raise HTTPException(status_code=422, detail="severity must be INFO, WARNING, or CRITICAL")
    return create_alert(
        alert_type=request.alert_type,
        severity=request.severity,
        description=request.description,
        source=request.source,
    )


@app.put("/api/alerts/{alert_id}/status")
def update_alert(alert_id: int, req: AlertStatusUpdate) -> dict[str, Any]:
    success = update_alert_status(alert_id, req.status)
    if not success:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} was not found")
    add_system_event("ALERT_STATUS_UPDATE", "Incident Manager", f"Alert #{alert_id} marked as {req.status}")
    return {"id": alert_id, "status": req.status, "updated": True}


# ---------------------------------------------------------
# Continuous Live Streaming Endpoints
# ---------------------------------------------------------
@app.get("/api/stream/status")
def stream_status() -> dict[str, Any]:
    return stream_engine.get_status()


@app.post("/api/stream/start")
def stream_start() -> dict[str, Any]:
    stream_engine.resume()
    add_system_event("STREAM_CONTROL", "Historical Replay", "Historical dataset replay resumed", "ONLINE")
    return {"action": "started", "mode": "historical_replay", "status": stream_engine.get_status()}


@app.post("/api/stream/pause")
def stream_pause() -> dict[str, Any]:
    stream_engine.pause()
    add_system_event("STREAM_CONTROL", "Historical Replay", "Historical dataset replay paused", "PAUSED")
    return {"action": "paused", "status": stream_engine.get_status()}


@app.post("/api/stream/speed")
def stream_speed(eps: int = Query(..., ge=1, le=500)) -> dict[str, Any]:
    stream_engine.set_speed(eps)
    return {"action": "speed_updated", "speed_eps": eps, "status": stream_engine.get_status()}


@app.get("/api/stream/events")
def stream_events(since_id: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    return stream_engine.get_events(since_id=since_id, limit=limit)


# ---------------------------------------------------------
# System Telemetry & Multi-Service Health
# ---------------------------------------------------------
@app.get("/api/system/status")
def system_status() -> list[dict[str, Any]]:
    mysql_info = check_mysql_status()
    stream_info = stream_engine.get_status()
    spark_info = get_spark_status()
    aggregates = get_aggregates()
    dataset_info = dataset_health()
    metadata = aggregates.get("metadata", {})
    checked_at = datetime.now(timezone.utc).isoformat()

    return [
        {
            "service_name": "Python Backend API",
            "status": "ONLINE",
            "host": f"{settings.backend_host}:{settings.backend_port}",
            "last_checked_at": checked_at,
            "description": "FastAPI backend is responding to this health check",
        },
        {
            "service_name": "Dataset Pipeline",
            "status": dataset_info["status"],
            "details": (
                f"{metadata.get('total_records', 0):,} source records; "
                f"{metadata.get('indexed_records', 0):,} records indexed"
            ),
            "description": dataset_info["detail"],
            "last_checked_at": checked_at,
        },
        {
            "service_name": "Kafka",
            "status": stream_info["kafka_status"],
            "details": f"{settings.kafka_bootstrap_servers} / topic {settings.kafka_topic}",
            "description": stream_info.get("kafka_error") or "Kafka producer bootstrap connection is healthy",
            "last_checked_at": checked_at,
        },
        {
            "service_name": "Spark",
            "status": spark_info["status"],
            "details": f"{spark_info.get('records_processed', 0):,} streamed records processed",
            "description": spark_info.get("detail", "Structured Streaming consumer and error aggregation"),
            "last_checked_at": checked_at,
        },
        {
            "service_name": "MySQL",
            "status": mysql_info["status"],
            "details": (
                f"{mysql_info.get('engine', 'MySQL')} @ {settings.mysql_host}:{settings.mysql_port} "
                f"({mysql_info.get('indexed_rows', 0):,} rows)"
            ),
            "description": mysql_info.get("detail", mysql_info.get("error", "MySQL query health check succeeded")),
            "last_checked_at": checked_at,
        },
    ]


@app.get("/api/system/events")
def system_events_endpoint(limit: int = Query(50, ge=1, le=200)) -> list[dict[str, Any]]:
    return get_system_events(limit=limit)


@app.post("/api/system/test-connection")
def test_all_connections() -> dict[str, Any]:
    mysql_status = check_mysql_status()
    stream_status = stream_engine.get_status()
    spark_status = get_spark_status()
    add_system_event("DIAGNOSTIC", "System Diagnostics", "Health check probe executed across all services", "ONLINE")
    return {
        "timestamp": perf_counter(),
        "backend": "ONLINE",
        "mysql": mysql_status["status"],
        "stream_engine": "ONLINE" if stream_status["is_active"] else "PAUSED",
        "kafka": stream_status["kafka_status"],
        "spark": spark_status["status"],
    }


# ---------------------------------------------------------
# SEO / Page Analyzer (Original Feature)
# ---------------------------------------------------------
class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self._in_title = False
        self.headings: list[str] = []
        self.links: list[str] = []
        self.images = 0
        self.images_without_alt = 0
        self.meta: dict[str, str] = {}
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "title":
            self._in_title = True
        elif tag in {"h1", "h2", "h3"}:
            self.headings.append(tag)
        elif tag == "a" and attributes.get("href"):
            self.links.append(attributes["href"] or "")
        elif tag == "img":
            self.images += 1
            if not attributes.get("alt", "").strip():
                self.images_without_alt += 1
        elif tag == "meta":
            key = (attributes.get("name") or attributes.get("property") or "").lower()
            if key and attributes.get("content"):
                self.meta[key] = attributes["content"] or ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title += data
        self._text.append(data)


def analyze_page(url: str) -> dict[str, Any]:
    started = perf_counter()
    request = Request(url, headers={"User-Agent": "WebLogMonitorPageAnalyzer/1.0"})
    try:
        with urlopen(request, timeout=10) as response:
            content_type = response.headers.get_content_type()
            body = response.read(2_000_001)
            status_code = response.status
            final_url = response.geturl()
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        return {"url": url, "error": str(exc), "load_time_ms": round((perf_counter() - started) * 1000, 2)}

    if content_type != "text/html":
        return {
            "url": url,
            "final_url": final_url,
            "status_code": status_code,
            "content_type": content_type,
            "content_bytes": len(body),
            "load_time_ms": round((perf_counter() - started) * 1000, 2),
            "error": "The URL did not return an HTML page",
        }

    parser = PageParser()
    parser.feed(body.decode("utf-8", errors="replace"))
    description = parser.meta.get("description", "")
    canonical = parser.meta.get("canonical", "")
    absolute_links = [urljoin(final_url, link) for link in parser.links]
    return {
        "url": url,
        "final_url": final_url,
        "status_code": status_code,
        "content_type": content_type,
        "content_bytes": len(body),
        "load_time_ms": round((perf_counter() - started) * 1000, 2),
        "title": parser.title.strip(),
        "description": description,
        "canonical_url": canonical,
        "headings": {"h1": parser.headings.count("h1"), "h2": parser.headings.count("h2"), "h3": parser.headings.count("h3")},
        "word_count": len(" ".join(parser._text).split()),
        "link_count": len(absolute_links),
        "image_count": parser.images,
        "images_without_alt": parser.images_without_alt,
        "seo_checks": {
            "has_title": bool(parser.title.strip()),
            "has_description": bool(description),
            "has_canonical": bool(canonical),
            "has_single_h1": parser.headings.count("h1") == 1,
            "images_have_alt": parser.images_without_alt == 0,
        },
    }


@app.post("/api/page-analyzer")
def page_analyzer(request: PageAnalyzerRequest) -> dict[str, Any]:
    return {"total": len(request.urls), "results": [analyze_page(url) for url in request.urls]}
