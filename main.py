"""
main.py — Orchestrate the ASX substantial holder scraper pipeline.
"""
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

from dotenv import load_dotenv

load_dotenv()

# Configure logging before importing other modules — timestamps in Sydney time
class _SydneyFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        from zoneinfo import ZoneInfo
        from datetime import datetime
        dt = datetime.fromtimestamp(record.created, tz=ZoneInfo("Australia/Sydney"))
        return dt.strftime(datefmt or "%Y-%m-%dT%H:%M:%S%z")

_fmt = "%(asctime)s %(levelname)s %(message)s"
_file_handler = logging.FileHandler("scraper.log")
_file_handler.setFormatter(_SydneyFormatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_file_handler])

_console = logging.StreamHandler(sys.stdout)
_console.setLevel(logging.INFO)
_console.setFormatter(_SydneyFormatter(_fmt))
logging.getLogger().addHandler(_console)

logger = logging.getLogger(__name__)

import notifier
import output
import parser
import scraper

AU_PUBLIC_HOLIDAYS = {
    "2024-01-01", "2024-01-26", "2024-03-29", "2024-04-01", "2024-04-25",
    "2024-06-10", "2024-12-25", "2024-12-26",
    "2025-01-01", "2025-01-27", "2025-04-18", "2025-04-21", "2025-04-25",
    "2025-06-09", "2025-12-25", "2025-12-26",
    "2026-01-01", "2026-01-26", "2026-04-03", "2026-04-06", "2026-04-25",
    "2026-06-08", "2026-12-25", "2026-12-28",
}

PDFS_DIR = Path("pdfs")
# Alert only when current position data is missing — previous position is nice-to-have.
# 603 forms (initial holder) never have previous_percent by design.
CURRENT_POSITION_FIELDS = ["investment_manager", "new_percent", "date_of_change"]


def sydney_today() -> date:
    """Return today's date in Sydney time."""
    return datetime.now(SYDNEY_TZ).date()


def is_trading_day(today: date | None = None) -> bool:
    """Return True if today is an ASX trading day (weekday, not AU public holiday)."""
    if today is None:
        today = sydney_today()
    date_str = today.strftime("%Y-%m-%d")
    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        logger.info(f"{date_str} is a weekend — not a trading day")
        return False
    if date_str in AU_PUBLIC_HOLIDAYS:
        logger.info(f"{date_str} is an AU public holiday — not a trading day")
        return False
    return True


def run() -> None:
    today = sydney_today()
    logger.info(f"=== ASX Scraper run started: {today} ===")

    if not is_trading_day(today):
        logger.info("Not a trading day — exiting cleanly")
        print(f"Not a trading day ({today}) — skipping.")
        return

    # Load already-processed IDs
    processed_ids = scraper.load_processed_ids()
    logger.info(f"Loaded {len(processed_ids)} previously processed IDs")

    # Fetch announcements
    announcements = scraper.get_announcements()
    if not announcements:
        logger.info("No substantial holder announcements found today")
        print("No substantial holder announcements found today.")
        return

    logger.info(f"Processing {len(announcements)} announcements")
    new_count = 0
    skipped_count = 0

    for ann in announcements:
        ids_id = ann["announcement_id"]

        if ids_id in processed_ids:
            logger.info(f"Skipping already-processed: {ids_id}")
            skipped_count += 1
            continue

        try:
            # Download PDF
            pdf_path = PDFS_DIR / f"{ids_id}.pdf"
            ok = scraper.download_pdf(ann, pdf_path)
            if not ok:
                reason = f"PDF download failed for {ann.get('pdf_url', 'unknown url')}"
                logger.error(reason)
                output.log_needs_review(ids_id, ann.get("asx_code", ""), ann.get("pdf_url", ""), reason)
                scraper.save_processed_id(ids_id)
                processed_ids.add(ids_id)
                continue

            # Parse PDF
            parsed = parser.parse_pdf(pdf_path, ann)

            # Write to output.csv
            output.append_result(parsed)

            confidence = parsed.get("confidence", "needs_review")

            # Missing current position fields — these trigger alerts and needs_review logging
            missing_current = [f for f in CURRENT_POSITION_FIELDS if not parsed.get(f)]

            # Log to needs_review.csv only when current position data is incomplete
            if missing_current:
                output.log_needs_review(
                    ids_id,
                    ann.get("asx_code", ""),
                    parsed.get("pdf_url", ""),
                    f"missing current position fields: {missing_current}",
                )

            # Send alert only when current position data is missing
            if missing_current:
                notifier.send_alert(parsed, missing_current)

            # Mark as processed
            scraper.save_processed_id(ids_id)
            processed_ids.add(ids_id)
            new_count += 1

            logger.info(
                f"Processed {ids_id} ({ann.get('asx_code')}) "
                f"confidence={confidence} method={parsed.get('parse_method')}"
            )

        except Exception as e:
            logger.error(f"Unexpected error processing {ids_id}: {e}", exc_info=True)
            try:
                output.log_needs_review(ids_id, ann.get("asx_code", ""), ann.get("pdf_url", ""), str(e))
                scraper.save_processed_id(ids_id)
                processed_ids.add(ids_id)
            except Exception:
                pass  # Never crash

    logger.info(
        f"=== Run complete: {new_count} processed, {skipped_count} skipped ==="
    )
    print(f"Done: {new_count} new announcements processed, {skipped_count} skipped.")


if __name__ == "__main__":
    run()
