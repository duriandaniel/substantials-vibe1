"""
output.py — Write parsed records to output.csv and needs_review.csv.
"""
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_CSV = Path("output.csv")
NEEDS_REVIEW_CSV = Path("needs_review.csv")

CSV_HEADERS = [
    "announcement_id",
    "asx_code",
    "company_name",
    "form_type",
    "action_type",
    "lodgement_date",
    "lodgement_time",
    "date_of_change",
    "investment_manager",
    "manager_acn",
    "previous_shares",
    "previous_percent",
    "new_shares",
    "new_percent",
    "consideration",
    "parse_method",
    "confidence",
    "pdf_url",
]

NEEDS_REVIEW_HEADERS = [
    "announcement_id",
    "asx_code",
    "pdf_url",
    "reason",
    "timestamp",
]


def _load_existing_ids(csv_path: Path, id_field: str = "announcement_id") -> set:
    """Read existing announcement_ids from a CSV file."""
    ids = set()
    try:
        if csv_path.exists():
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    val = row.get(id_field, "").strip()
                    if val:
                        ids.add(val)
    except Exception as e:
        logger.error(f"Failed to load existing IDs from {csv_path}: {e}")
    return ids


def append_result(record: dict) -> bool:
    """
    Append a parsed record to output.csv.
    Skips if announcement_id already exists.
    Creates file with headers if it doesn't exist.
    Returns True if written, False if skipped or error.
    """
    try:
        ann_id = str(record.get("announcement_id", "")).strip()
        existing_ids = _load_existing_ids(OUTPUT_CSV)

        if ann_id and ann_id in existing_ids:
            logger.info(f"Skipping duplicate announcement_id: {ann_id}")
            return False

        file_exists = OUTPUT_CSV.exists()
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
            if not file_exists or OUTPUT_CSV.stat().st_size == 0:
                writer.writeheader()
            sanitized = {h: str(record.get(h, "") or "").replace(",", "") for h in CSV_HEADERS}
            writer.writerow(sanitized)

        logger.info(f"Wrote record {ann_id} to {OUTPUT_CSV}")
        return True

    except Exception as e:
        logger.error(f"Failed to write record to {OUTPUT_CSV}: {e}")
        return False


def log_needs_review(announcement_id: str, asx_code: str, pdf_url: str, reason: str) -> None:
    """Append a failed/needs-review entry to needs_review.csv."""
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        timestamp = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%Y-%m-%dT%H:%M:%S+11:00")

        file_exists = NEEDS_REVIEW_CSV.exists()
        with open(NEEDS_REVIEW_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=NEEDS_REVIEW_HEADERS, extrasaction="ignore")
            if not file_exists or NEEDS_REVIEW_CSV.stat().st_size == 0:
                writer.writeheader()
            writer.writerow({
                "announcement_id": announcement_id,
                "asx_code": asx_code,
                "pdf_url": pdf_url,
                "reason": reason,
                "timestamp": timestamp,
            })
        logger.info(f"Logged needs_review: {announcement_id}")
    except Exception as e:
        logger.error(f"Failed to write needs_review entry: {e}")
