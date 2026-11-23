"""Spark Structured Streaming consumer for the Kafka web-log topic."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
_process: subprocess.Popen[bytes] | None = None
_status_path = Path(tempfile.gettempdir()) / "web_log_spark_status.json"


def _read_status() -> dict[str, Any]:
    try:
        with _status_path.open("r", encoding="utf-8") as status_file:
            return json.load(status_file)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError):
        logger.exception("Could not read Spark status file: %s", _status_path)
        return {}


def get_status() -> dict[str, Any]:
    if _process is None:
        return {"status": "NOT_CONFIGURED", "detail": "Spark streaming process has not been started"}

    return_code = _process.poll()
    if return_code is not None:
        return {
            "status": "FAILED",
            "detail": f"Spark streaming process exited with code {return_code}",
            "log_file": str(Path(tempfile.gettempdir()) / "web_log_spark.log"),
        }

    status = _read_status()
    if not status:
        return {"status": "STARTING", "detail": "Waiting for Spark to initialize"}

    return {**status, "status": "ONLINE"}


def start_spark_processor() -> None:
    global _process
    from ..config import settings

    if _process is not None and _process.poll() is None:
        return

    spark_home = os.getenv("SPARK_HOME")
    if not spark_home:
        logger.warning("SPARK_HOME is not set; Spark streaming was not started")
        return

    spark_submit = Path(spark_home) / "bin" / "spark-submit.cmd"
    if not spark_submit.exists():
        spark_submit = Path(spark_home) / "bin" / "spark-submit"
    if not spark_submit.exists():
        logger.error("Spark submit executable was not found under %s", spark_home)
        return

    try:
        _status_path.unlink(missing_ok=True)
    except OSError:
        logger.exception("Could not clear previous Spark status file: %s", _status_path)
        return

    version = os.getenv("SPARK_VERSION", "4.2.0")
    connector = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{version}"
    processor = Path(__file__).resolve()
    env = os.environ.copy()
    env["WEB_LOG_SPARK_STATUS"] = str(_status_path)
    env["SPARK_CHECKPOINT_PATH"] = str(Path(tempfile.gettempdir()) / "web_log_spark_checkpoint")
    env["KAFKA_BOOTSTRAP_SERVERS"] = ",".join(
        server.strip() for server in settings.kafka_bootstrap_servers.split(",")
    )
    env["KAFKA_TOPIC"] = settings.kafka_topic
    env["PYSPARK_PYTHON"] = sys.executable

    java = shutil.which("java")
    if java is None:
        logger.error("Java executable was not found; Spark streaming was not started")
        return

    command = [
        java,
        "-cp",
        str(Path(spark_home) / "jars" / "*"),
        "org.apache.spark.deploy.SparkSubmit",
        "--master",
        "local[2]",
        "--driver-memory",
        "512m",
        "--packages",
        connector,
        "--conf",
        "spark.sql.shuffle.partitions=2",
        str(processor),
    ]
    log_path = Path(tempfile.gettempdir()) / "web_log_spark.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            _process = subprocess.Popen(
                command,
                cwd=str(processor.parents[3]),
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
    except OSError:
        logger.exception("Could not launch Spark streaming")
        return

    logger.info("Started Spark streaming process with PID %s", _process.pid)


def stop_spark_processor() -> None:
    if _process is None or _process.poll() is not None:
        return

    _process.terminate()
    try:
        _process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _process.kill()
        _process.wait(timeout=5)


def _write_status(status: dict[str, Any]) -> None:
    _status_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _status_path.with_suffix(".tmp")
    temporary_path.write_text(json.dumps(status), encoding="utf-8")
    temporary_path.replace(_status_path)


def build_stream():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("stream_id", LongType()),
            StructField("timestamp", StringType()),
            StructField("ip_address", StringType()),
            StructField("http_method", StringType()),
            StructField("url", StringType()),
            StructField("protocol", StringType()),
            StructField("status_code", IntegerType()),
            StructField("response_bytes", LongType()),
            StructField("user_agent", StringType()),
        ]
    )
    spark = SparkSession.builder.appName("WebLogMonitoringKafkaConsumer").getOrCreate()
    messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
        .option("subscribe", os.getenv("KAFKA_TOPIC", "web-logs"))
        .option("startingOffsets", "latest")
        .load()
    )
    records = messages.select(from_json(col("value").cast("string"), schema).alias("record")).select("record.*")
    return spark, records.filter(col("timestamp").isNotNull())


def _run_stream() -> None:
    from pyspark.sql.functions import col, count, sum as spark_sum, when

    spark, records = build_stream()
    _write_status({"status": "ONLINE", "started_at": datetime.now(timezone.utc).isoformat(), "batches_processed": 0, "records_processed": 0})

    def process_batch(batch, batch_id: int) -> None:
        summary = batch.agg(
            count("*").alias("records"),
            spark_sum(when(col("status_code") >= 400, 1).otherwise(0)).alias("errors"),
            spark_sum(when(col("status_code") >= 500, 1).otherwise(0)).alias("server_errors"),
        ).first()
        previous = _read_status()
        record_count = int(summary["records"] or 0)
        _write_status(
            {
                "status": "ONLINE",
                "started_at": previous.get("started_at"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "batches_processed": int(previous.get("batches_processed", 0)) + 1,
                "records_processed": int(previous.get("records_processed", 0)) + record_count,
                "last_batch_id": batch_id,
                "last_batch_records": record_count,
                "last_batch_errors": int(summary["errors"] or 0),
                "last_batch_server_errors": int(summary["server_errors"] or 0),
            }
        )

    query = (
        records.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT_PATH", "data/spark-checkpoint"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    _run_stream()
