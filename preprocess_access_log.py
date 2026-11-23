"""Stream an Apache access log into a clean, typed, compressed CSV file."""

import argparse
import csv
import gzip
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit


LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^]]+)\] '
    r'"(?P<request>[^"]*)" (?P<status>\d{3}) (?P<bytes>\S+) '
    r'"(?P<referrer>[^"]*)" "(?P<user_agent>[^"]*)"(?: "(?P<extra>[^"]*)")?$'
)
DATE_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
FIELDS = [
    "ip",
    "timestamp",
    "method",
    "path",
    "query",
    "protocol",
    "status",
    "response_bytes",
    "referrer",
    "user_agent",
]


def parse_line(line: str) -> dict[str, object] | None:
    match = LOG_PATTERN.match(line.rstrip("\r\n"))
    if match is None:
        return None

    request_parts = match["request"].split(" ", 2)
    if len(request_parts) != 3:
        return None

    method, target, protocol = request_parts
    url = urlsplit(target)
    try:
        timestamp = datetime.strptime(match["timestamp"], DATE_FORMAT).isoformat()
        status = int(match["status"])
        response_bytes = 0 if match["bytes"] == "-" else int(match["bytes"])
    except ValueError:
        return None

    return {
        "ip": match["ip"],
        "timestamp": timestamp,
        "method": method,
        "path": url.path or "/",
        "query": url.query,
        "protocol": protocol,
        "status": status,
        "response_bytes": response_bytes,
        "referrer": "" if match["referrer"] == "-" else match["referrer"],
        "user_agent": "" if match["user_agent"] == "-" else match["user_agent"],
    }


def preprocess(input_path: Path, output_path: Path) -> tuple[int, int]:
    valid_rows = 0
    skipped_rows = 0
    with input_path.open("r", encoding="utf-8", errors="replace", buffering=1024 * 1024) as source:
        with gzip.open(output_path, "wt", encoding="utf-8", newline="", compresslevel=6) as target:
            writer = csv.DictWriter(target, fieldnames=FIELDS)
            writer.writeheader()
            for line in source:
                row = parse_line(line)
                if row is None:
                    skipped_rows += 1
                    continue
                writer.writerow(row)
                valid_rows += 1
    return valid_rows, skipped_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, nargs="?", default=Path("access.log"))
    parser.add_argument(
        "output", type=Path, nargs="?", default=Path("access_preprocessed.csv.gz")
    )
    args = parser.parse_args()
    valid_rows, skipped_rows = preprocess(args.input, args.output)
    print(f"Wrote {valid_rows:,} valid rows to {args.output}")
    print(f"Skipped {skipped_rows:,} malformed rows")


if __name__ == "__main__":
    main()