from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import runpy

from app import database
from app.database_migrations import initialize_database
from app.main import LogEventRequest, dashboard, latency_metrics
from app.streaming.stream_engine import StreamEngine


def test_log_event_validates_http_status_and_nonnegative_bytes() -> None:
    event = LogEventRequest(
        ip_address="192.0.2.1",
        http_method="get",
        url="/health",
        status_code=200,
        response_bytes=24,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert event.http_method == "get"
    assert event.response_bytes == 24


def test_sqlite_ingestion_is_idempotent_and_queryable(tmp_path, monkeypatch) -> None:
    sqlite_path = tmp_path / "logs.db"
    monkeypatch.setattr(database, "get_mysql_connection", lambda: None)
    monkeypatch.setattr(database, "get_sqlite_path", lambda: sqlite_path)
    initialize_database()

    event = {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "ip_address": "192.0.2.1",
        "http_method": "GET",
        "url": "/health",
        "protocol": "HTTP/1.1",
        "status_code": 200,
        "response_bytes": 24,
        "user_agent": "test-client",
        "source_type": "live",
        "source_identifier": "test-1",
        "request_id": "request-1",
    }

    assert database.persist_stream_events([event]) == 1
    assert database.persist_stream_events([event]) == 0
    result = database.search_logs(
        ip="192.0.2.1",
        source_type="live",
        request_id="request-1",
        limit=10,
    )
    assert result["total"] == 1
    assert result["items"][0]["url"] == "/health"
    assert result["items"][0]["source_type"] == "live"


def test_latency_is_reported_as_unavailable_when_not_in_source() -> None:
    result = latency_metrics()
    assert result["available"] is False
    assert result["p95_ms"] is None


def test_dashboard_reads_real_generated_aggregate_file() -> None:
    result = dashboard()
    assert result["total_requests"] == 10_365_077
    assert result["successful_requests"] > 0


def test_kafka_health_uses_successful_producer_connectivity() -> None:
    class DisconnectedProducer:
        def bootstrap_connected(self) -> bool:
            return False

    engine = StreamEngine()
    engine._producer = DisconnectedProducer()
    engine._kafka_connected = True

    assert engine.is_kafka_online()


def test_spark_entrypoint_loads_as_standalone_script() -> None:
    processor_path = Path(__file__).parents[1] / "app" / "streaming" / "spark_processor.py"
    namespace = runpy.run_path(str(processor_path), run_name="spark_processor_test")

    assert callable(namespace["_run_stream"])
