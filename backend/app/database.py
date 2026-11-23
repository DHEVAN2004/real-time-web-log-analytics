"""
Database persistence layer supporting MySQL with automatic SQLite fallback.
Provides high-performance B-tree indexed queries for logs, alerts, metrics, and system events.
"""

from __future__ import annotations

import sqlite3
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    import mysql.connector
    from mysql.connector import pooling
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

from .config import settings

_mysql_pool: Optional[Any] = None
_mysql_error: Optional[str] = None
logger = logging.getLogger(__name__)


def get_sqlite_path() -> Path:
    p = settings.sqlite_db_path
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[2] / "data" / "web_log_monitoring.db"
    return p


def get_mysql_connection():
    if not HAS_MYSQL:
        return None
    try:
        return mysql.connector.connect(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_database,
            connection_timeout=2
        )
    except Exception:
        return None


def check_mysql_status() -> dict[str, Any]:
    conn = get_mysql_connection()
    if conn is not None:
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM logs")
            row_count = cur.fetchone()[0]
            conn.close()
            return {
                "status": "ONLINE",
                "engine": "MySQL 8.0 (InnoDB)",
                "database": settings.mysql_database,
                "host": f"{settings.mysql_host}:{settings.mysql_port}",
                "indexed_rows": row_count
            }
        except Exception as exc:
            return {"status": "DEGRADED", "engine": "MySQL", "error": str(exc)}
    return {"status": "OFFLINE", "engine": "MySQL", "detail": "Cannot connect to MySQL server"}


def get_db_cursor():
    """Returns (connection, cursor, is_mysql)"""
    mysql_conn = get_mysql_connection()
    if mysql_conn is not None:
        return mysql_conn, mysql_conn.cursor(dictionary=True), True

    sqlite_path = get_sqlite_path()
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    return sqlite_conn, sqlite_conn.cursor(), False


def persist_stream_events(events: list[dict[str, Any]]) -> int:
    if not events:
        return 0

    conn, cur, is_mysql = get_db_cursor()
    placeholder = "%s" if is_mysql else "?"
    timestamp_values = [
        datetime.fromisoformat(event["timestamp"]).replace(tzinfo=None)
        if is_mysql
        else event["timestamp"]
        for event in events
    ]
    values = []
    for event, timestamp in zip(events, timestamp_values):
        fingerprint = event.get("event_fingerprint") or hashlib.sha256(
            json.dumps(
                [
                    event.get("source_type", "live"),
                    event.get("source_identifier"),
                    event.get("request_id"),
                    event.get("timestamp"),
                    event.get("ip_address"),
                    event.get("http_method"),
                    event.get("url"),
                    event.get("status_code"),
                    event.get("response_bytes"),
                    event.get("user_agent"),
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        ingested_at = (
            datetime.now().replace(microsecond=0)
            if is_mysql
            else datetime.now().isoformat(timespec="seconds")
        )
        values.append(
            (
                timestamp,
                event.get("ip_address"),
                event.get("http_method"),
                event.get("url"),
                event.get("protocol"),
                event.get("status_code"),
                event.get("response_bytes"),
                event.get("user_agent"),
                event.get("response_time"),
                event.get("source_type", "live"),
                event.get("source_identifier"),
                event.get("request_id"),
                ingested_at,
                fingerprint,
            )
        )
    try:
        query = f"""
            INSERT INTO logs
                (timestamp, ip_address, http_method, url, protocol, status_code, response_bytes, user_agent, response_time,
                 source_type, source_identifier, request_id, ingestion_timestamp, event_fingerprint)
            VALUES ({", ".join([placeholder] * 14)})
        """
        if is_mysql:
            query = query.replace("INSERT INTO logs", "INSERT IGNORE INTO logs")
        else:
            query += " ON CONFLICT(event_fingerprint) DO NOTHING"
        cur.executemany(query, values)
        conn.commit()
        return max(0, cur.rowcount)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_logs(limit: int = 100, offset: int = 0) -> dict[str, Any]:
    conn, cur, is_mysql = get_db_cursor()
    try:
        if is_mysql:
            cur.execute("SELECT COUNT(*) as total FROM logs")
            total = cur.fetchone()["total"]
            cur.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT %s OFFSET %s", (limit, offset))
            rows = cur.fetchall()
        else:
            cur.execute("SELECT COUNT(*) as total FROM logs")
            total = cur.fetchone()["total"]
            cur.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))
            rows = [dict(r) for r in cur.fetchall()]

        items = []
        for r in rows:
            items.append({
                "id": r.get("id"),
                "timestamp": str(r.get("timestamp")),
                "ip_address": r.get("ip_address"),
                "http_method": r.get("http_method"),
                "url": r.get("url"),
                "protocol": r.get("protocol"),
                "status_code": int(r.get("status_code", 0)),
                "response_bytes": int(r.get("response_bytes", 0)),
                "user_agent": r.get("user_agent", ""),
                "response_time": r.get("response_time"),
                "source_type": r.get("source_type", "historical"),
                "source_identifier": r.get("source_identifier"),
                "request_id": r.get("request_id"),
                "ingestion_timestamp": str(r.get("ingestion_timestamp") or ""),
            })
        return {"total": total, "items": items, "limit": limit, "offset": offset}
    finally:
        conn.close()


def search_logs(
    keyword: Optional[str] = None,
    ip: Optional[str] = None,
    method: Optional[str] = None,
    status: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    source_type: Optional[str] = None,
    request_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    sort_by: str = "timestamp",
    sort_order: str = "DESC",
) -> dict[str, Any]:
    conn, cur, is_mysql = get_db_cursor()
    try:
        conditions = []
        params = []
        ph = "%s" if is_mysql else "?"

        if keyword:
            conditions.append(f"(url LIKE {ph} OR user_agent LIKE {ph})")
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if ip:
            conditions.append(f"ip_address = {ph}")
            params.append(ip)
        if method:
            conditions.append(f"UPPER(http_method) = {ph}")
            params.append(method.upper())
        if status is not None:
            conditions.append(f"status_code = {ph}")
            params.append(status)
        if source_type:
            conditions.append(f"source_type = {ph}")
            params.append(source_type)
        if request_id:
            conditions.append(f"request_id = {ph}")
            params.append(request_id)
        if start_time:
            conditions.append(f"timestamp >= {ph}")
            params.append(start_time)
        if end_time:
            conditions.append(f"timestamp <= {ph}")
            params.append(end_time)

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        safe_sort_by = sort_by if sort_by in {"timestamp", "status_code", "response_bytes", "response_time"} else "timestamp"
        safe_sort_order = "ASC" if sort_order.upper() == "ASC" else "DESC"

        # Count matching
        count_sql = f"SELECT COUNT(*) as total FROM logs{where_clause}"
        cur.execute(count_sql, tuple(params))
        total_row = cur.fetchone()
        total = total_row["total"] if isinstance(total_row, dict) else total_row[0]

        # Fetch page
        query_sql = f"SELECT * FROM logs{where_clause} ORDER BY {safe_sort_by} {safe_sort_order} LIMIT {ph} OFFSET {ph}"
        page_params = list(params) + [limit, offset]
        cur.execute(query_sql, tuple(page_params))
        rows = cur.fetchall()

        items = []
        for r in rows:
            rd = dict(r)
            items.append({
                "id": rd.get("id"),
                "timestamp": str(rd.get("timestamp")),
                "ip_address": rd.get("ip_address"),
                "http_method": rd.get("http_method"),
                "url": rd.get("url"),
                "protocol": rd.get("protocol"),
                "status_code": int(rd.get("status_code", 0)),
                "response_bytes": int(rd.get("response_bytes", 0)),
                "user_agent": rd.get("user_agent", ""),
                "response_time": rd.get("response_time"),
                "source_type": rd.get("source_type", "historical"),
                "source_identifier": rd.get("source_identifier"),
                "request_id": rd.get("request_id"),
                "ingestion_timestamp": str(rd.get("ingestion_timestamp") or ""),
            })
        return {"total": total, "items": items, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_alerts(severity: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    conn, cur, is_mysql = get_db_cursor()
    try:
        conditions = []
        params = []
        ph = "%s" if is_mysql else "?"

        if severity and severity.upper() != "ALL":
            conditions.append(f"severity = {ph}")
            params.append(severity.upper())
        if status and status.upper() != "ALL":
            conditions.append(f"status = {ph}")
            params.append(status.upper())

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""
        sql = f"SELECT * FROM alerts{where_clause} ORDER BY created_at DESC LIMIT {ph}"
        params.append(limit)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_alert_status(alert_id: int, new_status: str) -> bool:
    conn, cur, is_mysql = get_db_cursor()
    try:
        ph = "%s" if is_mysql else "?"
        cur.execute(
            f"""
            UPDATE alerts SET status = {ph},
                acknowledged_at = CASE WHEN {ph} = 'ACKNOWLEDGED' THEN CURRENT_TIMESTAMP ELSE acknowledged_at END,
                resolved_at = CASE WHEN {ph} = 'RESOLVED' THEN CURRENT_TIMESTAMP ELSE resolved_at END
            WHERE id = {ph}
            """,
            (new_status.upper(), new_status.upper(), new_status.upper(), alert_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_system_events(limit: int = 50) -> list[dict[str, Any]]:
    conn, cur, is_mysql = get_db_cursor()
    try:
        ph = "%s" if is_mysql else "?"
        cur.execute(f"SELECT * FROM system_events ORDER BY created_at DESC LIMIT {ph}", (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_system_event(event_type: str, service_name: str, message: str, status: str = "ONLINE"):
    conn, cur, is_mysql = get_db_cursor()
    try:
        ph = "%s" if is_mysql else "?"
        cur.execute(
            f"INSERT INTO system_events (event_type, service_name, message, status) VALUES ({ph}, {ph}, {ph}, {ph})",
            (event_type, service_name, message, status)
        )
        conn.commit()
    finally:
        conn.close()


def get_mysql_connection():
    global _mysql_pool, _mysql_error
    if not HAS_MYSQL:
        _mysql_error = "mysql-connector-python is not installed"
        return None
    try:
        if _mysql_pool is None:
            _mysql_pool = pooling.MySQLConnectionPool(
                pool_name="web_log_monitor",
                pool_size=8,
                pool_reset_session=True,
                host=settings.mysql_host,
                port=settings.mysql_port,
                user=settings.mysql_user,
                password=settings.mysql_password,
                database=settings.mysql_database,
                connection_timeout=3,
            )
        _mysql_error = None
        return _mysql_pool.get_connection()
    except mysql.connector.Error as exc:
        _mysql_error = str(exc)
        logger.error("MySQL connection failed: %s", exc)
        return None


def check_mysql_status() -> dict[str, Any]:
    conn = get_mysql_connection()
    if conn is None:
        return {
            "status": "OFFLINE",
            "engine": "MySQL",
            "host": f"{settings.mysql_host}:{settings.mysql_port}",
            "detail": _mysql_error or "MySQL connection could not be established",
        }
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM logs")
        row_count = int(cur.fetchone()[0])
        cur.close()
        return {
            "status": "ONLINE",
            "engine": "MySQL 8.0 (InnoDB)",
            "database": settings.mysql_database,
            "host": f"{settings.mysql_host}:{settings.mysql_port}",
            "indexed_rows": row_count,
        }
    except mysql.connector.Error as exc:
        logger.error("MySQL health check failed: %s", exc)
        return {"status": "DEGRADED", "engine": "MySQL", "error": str(exc)}
    finally:
        conn.close()


def get_log_by_id(log_id: int) -> Optional[dict[str, Any]]:
    conn, cur, is_mysql = get_db_cursor()
    placeholder = "%s" if is_mysql else "?"
    try:
        cur.execute(f"SELECT * FROM logs WHERE id = {placeholder}", (log_id,))
        row = cur.fetchone()
        if row is None:
            return None
        record = dict(row)
        record["timestamp"] = str(record.get("timestamp"))
        record["status_code"] = int(record.get("status_code") or 0)
        record["response_bytes"] = int(record.get("response_bytes") or 0)
        return record
    finally:
        conn.close()


def create_alert(
    alert_type: str,
    severity: str,
    description: str,
    source: str = "Rule Engine",
) -> dict[str, Any]:
    conn, cur, is_mysql = get_db_cursor()
    placeholder = "%s" if is_mysql else "?"
    try:
        cur.execute(
            f"""
            INSERT INTO alerts (alert_type, severity, description, source, status)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, 'OPEN')
            """,
            (alert_type, severity.upper(), description, source),
        )
        conn.commit()
        alert_id = cur.lastrowid
        return {
            "id": alert_id,
            "alert_type": alert_type,
            "severity": severity.upper(),
            "description": description,
            "source": source,
            "status": "OPEN",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
