"""
notifier.py — Send Gmail SMTP alerts for low/needs_review confidence records.
"""
import logging
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


# Current position fields — alert is triggered only when these are missing.
CURRENT_POSITION_FIELDS = ["investment_manager", "new_percent", "date_of_change"]


def send_alert(record: dict, missing_fields: list[str] | None = None) -> bool:
    """
    Send an email alert when current position data could not be extracted.

    Includes all detected data in the body so the recipient can compare
    against the source PDF without opening a separate tool.

    Returns True if sent (or skipped due to missing config), False on error.
    """
    email_from = os.environ.get("ALERT_EMAIL_FROM", "")
    email_to = os.environ.get("ALERT_EMAIL_TO", "")
    smtp_password = os.environ.get("SMTP_PASSWORD", "")

    if not all([email_from, email_to, smtp_password]):
        logger.warning(
            "Email alert skipped — ALERT_EMAIL_FROM, ALERT_EMAIL_TO, or SMTP_PASSWORD not set"
        )
        return True  # Not an error — just not configured

    confidence = record.get("confidence", "unknown")
    asx_code = record.get("asx_code", "")
    announcement_id = record.get("announcement_id", "")
    pdf_url = record.get("pdf_url", "")

    subject = f"ASX Scraper Alert: {confidence} -- {asx_code} {announcement_id}"

    missing = missing_fields or []

    def _val(field: str) -> str:
        v = record.get(field)
        return str(v) if v not in (None, "") else "(not found)"

    body_lines = [
        "ASX Scraper Alert — current position data missing or incomplete",
        "",
        "--- Announcement ---",
        f"announcement_id  : {announcement_id}",
        f"asx_code         : {asx_code}",
        f"form_type        : {_val('form_type')}",
        f"lodgement_date   : {_val('lodgement_date')}",
        f"pdf_url          : {pdf_url}",
        "",
        "--- Extracted Data ---",
        f"investment_manager : {_val('investment_manager')}",
        f"manager_acn        : {_val('manager_acn')}",
        f"date_of_change     : {_val('date_of_change')}",
        f"previous_shares    : {_val('previous_shares')}",
        f"previous_percent   : {_val('previous_percent')}",
        f"new_shares         : {_val('new_shares')}",
        f"new_percent        : {_val('new_percent')}",
        f"consideration      : {_val('consideration')}",
        "",
        "--- Parse Result ---",
        f"confidence         : {confidence}",
        f"parse_method       : {_val('parse_method')}",
        f"missing_fields     : {', '.join(missing) if missing else 'none'}",
        "",
        f"Open PDF to verify: {pdf_url}",
    ]
    body = "\n".join(body_lines)

    try:
        msg = MIMEMultipart()
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(email_from, smtp_password)
            server.sendmail(email_from, email_to, msg.as_string())

        logger.info(f"Alert email sent for {announcement_id} ({confidence})")
        return True

    except Exception as e:
        logger.error(f"Failed to send alert email for {announcement_id}: {e}")
        return False
