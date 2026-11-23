CREATE DATABASE IF NOT EXISTS web_log_monitoring;
USE web_log_monitoring;

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
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_logs_timestamp (timestamp),
  INDEX idx_logs_ip (ip_address),
  INDEX idx_logs_status (status_code),
  INDEX idx_logs_method (http_method)
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  alert_type VARCHAR(100) NOT NULL,
  severity VARCHAR(20) NOT NULL,
  description TEXT,
  source VARCHAR(100),
  status VARCHAR(20) DEFAULT 'OPEN',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  acknowledged_at DATETIME NULL,
  resolved_at DATETIME NULL,
  INDEX idx_alert_created (created_at),
  INDEX idx_alert_severity (severity),
  INDEX idx_alert_status (status),
  INDEX idx_alert_type (alert_type)
);

CREATE TABLE IF NOT EXISTS traffic_metrics (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  metric_time DATETIME NOT NULL,
  request_count BIGINT DEFAULT 0,
  requests_per_second DOUBLE DEFAULT 0,
  success_count BIGINT DEFAULT 0,
  redirect_count BIGINT DEFAULT 0,
  client_error_count BIGINT DEFAULT 0,
  server_error_count BIGINT DEFAULT 0,
  INDEX idx_metric_time (metric_time)
);

CREATE TABLE IF NOT EXISTS system_events (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  event_type VARCHAR(100),
  service_name VARCHAR(100),
  message TEXT,
  status VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
