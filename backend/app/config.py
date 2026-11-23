from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    dataset_path: Path = Path("../access_preprocessed.csv.gz")
    aggregates_path: Path = Path("../data/dataset_aggregates.json")
    sqlite_db_path: Path = Path("../data/web_log_monitoring.db")
    backend_host: str = "127.0.0.1"
    backend_port: int = 8000
    cors_origins: str = "*"

    # MySQL Persistence
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "root"
    mysql_database: str = "web_log_monitoring"

    # Streaming / Kafka
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "web-logs"
    stream_default_rate: int = 25

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
