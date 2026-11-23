USE web_log_monitoring;

SELECT COUNT(*) AS total_requests FROM logs;

SELECT CASE WHEN status_code BETWEEN 200 AND 299 THEN '2xx' WHEN status_code BETWEEN 300 AND 399 THEN '3xx' WHEN status_code BETWEEN 400 AND 499 THEN '4xx' WHEN status_code BETWEEN 500 AND 599 THEN '5xx' ELSE 'Other' END AS status_category, COUNT(*) AS request_count FROM logs GROUP BY status_category ORDER BY request_count DESC;

SELECT url, COUNT(*) AS request_count FROM logs GROUP BY url ORDER BY request_count DESC LIMIT 10;
SELECT ip_address, COUNT(*) AS request_count FROM logs GROUP BY ip_address ORDER BY request_count DESC LIMIT 10;
SELECT http_method, COUNT(*) AS request_count FROM logs GROUP BY http_method ORDER BY request_count DESC;
SELECT COUNT(*) AS client_errors FROM logs WHERE status_code BETWEEN 400 AND 499;
SELECT COUNT(*) AS server_errors FROM logs WHERE status_code BETWEEN 500 AND 599;
SELECT DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') AS minute, COUNT(*) AS request_count FROM logs GROUP BY minute ORDER BY minute;
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100;
SELECT * FROM alerts ORDER BY created_at DESC LIMIT 50;
