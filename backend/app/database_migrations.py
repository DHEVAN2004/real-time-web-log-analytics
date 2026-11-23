"""Idempotent, non-destructive schema upgrades for existing log databases."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from .config import settings
from . import database

logger = logging.getLogger(__name__)


def _ensure_sqlite_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            ip_address TEXT,
            http_method TEXT,
            url TEXT,
            protocol TEXT,
            status_code INTEGER,
            response_bytes INTEGER,
            user_agent TEXT,
            response_time REAL
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            description TEXT,
            source TEXT,
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS system_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            service_name TEXT,
            message TEXT,
            status TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS traffic_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_time TEXT NOT NULL,
            request_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0,
            client_error_count INTEGER DEFAULT 0,
            server_error_count INTEGER DEFAULT 0
        );
        """
    )
    existing_columns = {row[1] for row in connection.execute("PRAGMA table_info(logs)")}
    columns = {
        "source_type": "TEXT NOT NULL DEFAULT 'historical'",
        "source_identifier": "TEXT",
        "request_id": "TEXT",
        "ingestion_timestamp": "TEXT",
        "event_fingerprint": "TEXT",
    }
    for column, definition in columns.items():
        if column not in existing_columns:
            connection.execute(f"ALTER TABLE logs ADD COLUMN {column} {definition}")
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_logs_event_fingerprint ON logs(event_fingerprint)"
    )
    alert_columns = {row[1] for row in connection.execute("PRAGMA table_info(alerts)")}
    for column in ("acknowledged_at", "resolved_at"):
        if column not in alert_columns:
            connection.execute(f"ALTER TABLE alerts ADD COLUMN {column} TEXT")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_logs_source_type ON logs(source_type)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_logs_ingestion_timestamp ON logs(ingestion_timestamp)"
    )
    connection.commit()


def _ensure_mysql_schema(connection: Any) -> None:
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            timestamp DATETIME NOT NULL,
            ip_address VARCHAR(45),
            http_method VARCHAR(30),
            url TEXT,
            protocol VARCHAR(100),
            status_code INT,
            response_bytes BIGINT,
            user_agent TEXT,
            response_time DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            alert_type VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            description TEXT,
            source VARCHAR(100),
            status VARCHAR(20) DEFAULT 'OPEN',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            acknowledged_at DATETIME NULL,
            resolved_at DATETIME NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS system_events (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            event_type VARCHAR(100),
            service_name VARCHAR(100),
            message TEXT,
            status VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic_metrics (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            metric_time DATETIME NOT NULL,
            request_count BIGINT DEFAULT 0,
            requests_per_second DOUBLE DEFAULT 0,
            success_count BIGINT DEFAULT 0,
            redirect_count BIGINT DEFAULT 0,
            client_error_count BIGINT DEFAULT 0,
            server_error_count BIGINT DEFAULT 0
        )
        """
    )
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'logs'
        """,
        (settings.mysql_database,),
    )
    existing_columns = {row[0] for row in cursor.fetchall()}
    columns = {
        "source_type": "VARCHAR(16) NOT NULL DEFAULT 'historical'",
        "source_identifier": "VARCHAR(255) NULL",
        "request_id": "VARCHAR(128) NULL",
        "ingestion_timestamp": "DATETIME NULL",
        "event_fingerprint": "CHAR(64) NULL",
    }
    for column, definition in columns.items():
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE logs ADD COLUMN {column} {definition}")
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'alerts'
        """,
        (settings.mysql_database,),
    )
    alert_columns = {row[0] for row in cursor.fetchall()}
    for column in ("acknowledged_at", "resolved_at"):
        if column not in alert_columns:
            cursor.execute(f"ALTER TABLE alerts ADD COLUMN {column} DATETIME NULL")

    cursor.execute(
        """
        SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'logs'
        """,
        (settings.mysql_database,),
    )
    indexes = {row[0] for row in cursor.fetchall()}
    if "uq_logs_event_fingerprint" not in indexes:
        cursor.execute("CREATE UNIQUE INDEX uq_logs_event_fingerprint ON logs(event_fingerprint)")
    if "idx_logs_source_type" not in indexes:
        cursor.execute("CREATE INDEX idx_logs_source_type ON logs(source_type)")
    if "idx_logs_ingestion_timestamp" not in indexes:
        cursor.execute("CREATE INDEX idx_logs_ingestion_timestamp ON logs(ingestion_timestamp)")
    connection.commit()
    cursor.close()


def initialize_database() -> dict[str, Any]:
    mysql_connection = database.get_mysql_connection()
    if mysql_connection is not None:
        try:
            _ensure_mysql_schema(mysql_connection)
            logger.info("MySQL schema is ready; existing records were preserved")
            return {"status": "ONLINE", "engine": "MySQL"}
        finally:
            mysql_connection.close()

    sqlite_path = database.get_sqlite_path()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(sqlite_path) as connection:
        _ensure_sqlite_schema(connection)
    logger.warning("MySQL is unavailable; SQLite fallback is ready at %s", sqlite_path)
    return {"status": "DEGRADED", "engine": "SQLite"}
