"""
Real-Time Continuous Stream Engine and Replay Simulator.
Streams events continuously in the background, maintains ring buffer,
calculates streaming telemetry, and integrates with Kafka if available.
"""

from __future__ import annotations

import collections
import csv
import gzip
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from ..database import persist_stream_events

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

from ..config import settings

logger = logging.getLogger(__name__)


class StreamEngine:
    def __init__(self):
        self._is_active = True
        self._speed_eps = settings.stream_default_rate
        self._lock = threading.Lock()
        self._buffer: collections.deque[dict[str, Any]] = collections.deque(maxlen=1000)
        self._event_counter = 0
        self._total_streamed = 0
        self._error_count = 0
        self._persisted_events = 0
        self._recent_events_window: collections.deque[tuple[float, int]] = collections.deque()  # (time, status)
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._producer = None
        self._kafka_connected = False
        self._kafka_error: Optional[str] = None
        self._last_kafka_attempt = 0.0

    def _init_kafka(self):
        if not HAS_KAFKA:
            self._kafka_error = "kafka-python is not installed"
            return
        self._last_kafka_attempt = time.monotonic()
        try:
            if self._producer is not None:
                self._producer.close(timeout=1)
            self._producer = KafkaProducer(
                bootstrap_servers=[
                    server.strip() for server in settings.kafka_bootstrap_servers.split(",")
                ],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                request_timeout_ms=3000,
                max_block_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            self._kafka_connected = self._producer.bootstrap_connected()
            self._kafka_error = None if self._kafka_connected else "Kafka bootstrap broker is unreachable"
            if not self._kafka_connected:
                logger.warning("Kafka bootstrap broker is unreachable: %s", settings.kafka_bootstrap_servers)
        except Exception as exc:
            self._producer = None
            self._kafka_connected = False
            self._kafka_error = str(exc)
            logger.warning("Kafka producer is unavailable: %s", exc)

    def is_kafka_online(self) -> bool:
        return bool(self._producer and self._kafka_connected)

    def start(self):
        with self._lock:
            self._is_active = True
        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="StreamEngineThread")
            self._thread.start()

    def pause(self):
        with self._lock:
            self._is_active = False

    def resume(self):
        with self._lock:
            self._is_active = True
        if self._thread is None or not self._thread.is_alive():
            self.start()

    def stop(self):
        self._stop_event.set()
        with self._lock:
            self._is_active = False
            producer = self._producer
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        if producer is not None:
            try:
                producer.flush(timeout=5)
                producer.close(timeout=5)
            except Exception:
                logger.exception("Could not close the Kafka producer cleanly")
        self._producer = None
        self._kafka_connected = False

    def set_speed(self, eps: int):
        with self._lock:
            self._speed_eps = max(1, min(eps, 500))

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            now = time.time()
            # Clean window older than 10 seconds
            while self._recent_events_window and self._recent_events_window[0][0] < now - 10:
                self._recent_events_window.popleft()

            window_len = len(self._recent_events_window)
            current_eps = round(window_len / 10.0, 1) if window_len > 0 else (self._speed_eps if self._is_active else 0.0)
            window_errors = sum(1 for _, st in self._recent_events_window if st >= 400)
            error_rate = round((window_errors / max(1, window_len)) * 100, 1) if window_len > 0 else 0.0

            return {
                "is_active": self._is_active,
                "speed_eps": self._speed_eps,
                "current_throughput_eps": current_eps,
                "total_streamed": self._total_streamed,
                "persisted_events": self._persisted_events,
                "buffer_size": len(self._buffer),
                "error_rate_pct": error_rate,
                "kafka_enabled": self.is_kafka_online(),
                "kafka_status": "ONLINE" if self.is_kafka_online() else "OFFLINE",
                "kafka_error": self._kafka_error,
                "streaming_mode": "Historical dataset replay",
            }

    def get_events(self, since_id: int = 0, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            if since_id == 0:
                # Return newest events
                items = list(self._buffer)[-limit:]
                items.reverse()
                return items
            else:
                # Return items with stream_id > since_id
                results = [e for e in self._buffer if e["stream_id"] > since_id]
                return results[-limit:]

    def record_live_event(self, event: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._event_counter += 1
            recorded_event = {**event, "stream_id": self._event_counter}
            self._buffer.append(recorded_event)
            self._total_streamed += 1
            self._recent_events_window.append((time.time(), int(event["status_code"])))
            producer = self._producer
        if producer is not None:
            try:
                producer.send(settings.kafka_topic, recorded_event).add_callback(
                    self._on_kafka_success
                ).add_errback(self._on_kafka_error)
            except Exception as exc:
                self._on_kafka_error(exc)
        return recorded_event

    def _run_loop(self):
        # Locate preprocessed dataset
        dataset_path = settings.dataset_path
        if not dataset_path.is_absolute():
            dataset_path = Path.cwd() / dataset_path
        if not dataset_path.exists():
            dataset_path = Path(__file__).resolve().parents[3] / settings.dataset_path
        if not dataset_path.exists():
            logger.error("Stream dataset does not exist: %s", dataset_path)
            return

        if self._producer is None or not self.is_kafka_online():
            self._init_kafka()
        database_batch: list[dict[str, Any]] = []
        last_database_flush = time.monotonic()
        rows_since_kafka_attempt = 0
        while not self._stop_event.is_set():
            try:
                opener = gzip.open if dataset_path.suffix == ".gz" else open
                with opener(dataset_path, "rt", encoding="utf-8", errors="replace") as f:
                    reader = csv.reader(f)
                    try:
                        next(reader)  # Header
                    except StopIteration:
                        break

                    for row_number, row in enumerate(reader, start=2):
                        if self._stop_event.is_set():
                            break

                        while not self._is_active and not self._stop_event.is_set():
                            time.sleep(0.2)

                        if self._stop_event.is_set():
                            break

                        if len(row) < 8:
                            with self._lock:
                                self._error_count += 1
                            continue

                        try:
                            status_code = int(row[6])
                            bytes_sent = int(row[7])
                            event_time = datetime.fromisoformat(row[1]).isoformat()
                            if not 100 <= status_code <= 599 or bytes_sent < 0:
                                raise ValueError("Invalid status code or response size")
                        except (ValueError, IndexError):
                            with self._lock:
                                self._error_count += 1
                            logger.warning("Skipping malformed dataset record on line %s", row_number)
                            continue

                        self._event_counter += 1
                        now = time.time()

                        event = {
                            "stream_id": self._event_counter,
                            "timestamp": event_time,
                            "ip_address": row[0],
                            "http_method": row[2],
                            "url": row[3] + (f"?{row[4]}" if len(row) > 4 and row[4] else ""),
                            "protocol": row[5] if len(row) > 5 else "HTTP/1.1",
                            "status_code": status_code,
                            "response_bytes": bytes_sent,
                            "user_agent": row[9] if len(row) > 9 else "",
                            "source_type": "replay",
                            "source_identifier": f"{dataset_path.resolve()}:{row_number}",
                        }

                        with self._lock:
                            self._buffer.append(event)
                            self._total_streamed += 1
                            self._recent_events_window.append((now, status_code))
                            target_eps = self._speed_eps

                        # Publish to Kafka if connected
                        if self._producer is not None:
                            try:
                                self._producer.send(settings.kafka_topic, event).add_callback(
                                    self._on_kafka_success
                                ).add_errback(self._on_kafka_error)
                            except Exception as exc:
                                self._on_kafka_error(exc)
                        rows_since_kafka_attempt += 1
                        if (
                            not self.is_kafka_online()
                            and rows_since_kafka_attempt >= 100
                            and time.monotonic() - self._last_kafka_attempt >= 30
                        ):
                            self._init_kafka()
                            rows_since_kafka_attempt = 0

                        database_batch.append(event)
                        if len(database_batch) >= 100 or time.monotonic() - last_database_flush >= 1:
                            try:
                                persisted = persist_stream_events(database_batch)
                                with self._lock:
                                    self._persisted_events += persisted
                                database_batch.clear()
                            except Exception:
                                logger.exception("Could not persist streamed log events")
                                with self._lock:
                                    self._error_count += 1
                            last_database_flush = time.monotonic()

                        # Sleep to throttle to target events/sec
                        delay = 1.0 / max(1, target_eps)
                        time.sleep(delay)

                    if database_batch:
                        persisted = persist_stream_events(database_batch)
                        with self._lock:
                            self._persisted_events += persisted
                        database_batch.clear()

            except Exception:
                logger.exception("Dataset stream iteration failed")
                with self._lock:
                    self._error_count += 1
                time.sleep(1.0)

    def _on_kafka_error(self, error: Exception) -> None:
        with self._lock:
            self._kafka_connected = False
            self._kafka_error = str(error)
        logger.error("Kafka publish failed: %s", error)

    def _on_kafka_success(self, metadata: Any) -> None:
        with self._lock:
            self._kafka_connected = True
            self._kafka_error = None


stream_engine = StreamEngine()
