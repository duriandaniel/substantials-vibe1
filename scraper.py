"""
scraper.py — Fetch ASX substantial holder announcements and download PDFs.
"""
import os
import re
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

ASX_ANNS_URL = "https://www.asx.com.au/asx/v2/statistics/todayAnns.do"
ASX_DISPLAY_URL = "https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId={ids_id}"
PROCESSED_IDS_FILE = "processed_ids.txt"
PDFS_DIR = Path("pdfs")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def load_processed_ids() -> set:
    try:
        p = Path(PROCESSED_IDS_FILE)
        if p.exists():
            return set(p.read_text().splitlines())
        return set()
    except Exception as e:
        logger.error(f"Failed to load processed_ids.txt: {e}")
        return set()


def save_processed_id(ids_id: str) -> None:
    try:
        with open(PROCESSED_IDS_FILE, "a") as f:
            f.write(ids_id + "\n")
    except Exception as e:
        logger.error(f"Failed to save processed id {ids_id}: {e}")


def get_announcements() -> list[dict]:
    """Fetch today's ASX announcements and return substantial holder notices."""
    try:
        resp = requests.get(ASX_ANNS_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        logger.info(f"Fetched announcements page: HTTP {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to fetch announcements: {e}")
        return []

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
        announcements = _parse_announcements(soup)
        logger.info(f"Found {len(announcements)} substantial holder announcements")
        return announcements
    except Exception as e:
        logger.error(f"Failed to parse announcements HTML: {e}")
        return []


def _parse_announcements(soup: BeautifulSoup) -> list[dict]:
    """Parse the HTML table and return substantial holder rows."""
    today = datetime.now(SYDNEY_TZ).strftime("%Y%m%d")
    results = []

    # Find the announcements table — rows have td elements
    rows = soup.find_all("tr")
    for row in rows:
        try:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Extract text from cells — typical layout: time | asx_code | headline(link)
            row_text = " ".join(c.get_text(strip=True) for c in cells)
            if "substantial" not in row_text.lower():
                continue

            # Find the PDF link — contains idsId parameter
            link = row.find("a", href=True)
            if not link:
                continue

            href = link["href"]
            ids_id = _extract_ids_id(href)
            if not ids_id:
                continue

            headline = link.get_text(strip=True)
            asx_code = _extract_asx_code(cells, href)
            form_type = _infer_form_type(headline)
            # pdf_url will be resolved at download time from the display page
            display_url = ASX_DISPLAY_URL.format(ids_id=ids_id)

            # Find the time cell — looks like "10:15 AM" or "10:15"
            lodgement_time = ""
            for cell in cells:
                t = cell.get_text(strip=True)
                if re.match(r"^\d{1,2}:\d{2}", t):
                    lodgement_time = t
                    break

            results.append({
                "announcement_id": ids_id,
                "asx_code": asx_code,
                "headline": headline,
                "form_type": form_type,
                "lodgement_date": datetime.now(SYDNEY_TZ).strftime("%Y-%m-%d"),
                "lodgement_time": lodgement_time,
                "display_url": display_url,
                "pdf_url": "",  # resolved during download
            })
            logger.info(f"Found announcement: {asx_code} {ids_id} — {headline[:60]}")

        except Exception as e:
            logger.error(f"Error parsing row: {e}")
            continue

    return results


def _extract_ids_id(href: str) -> str | None:
    """Extract idsId from href like /asx/statistics/displayAnnouncement.do?idsId=XXXXX"""
    # Try query param
    try:
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        if "idsId" in params:
            return params["idsId"][0]
    except Exception:
        pass

    # Try regex fallback
    m = re.search(r"idsId=([a-zA-Z0-9]+)", href)
    if m:
        return m.group(1)

    # The href itself might be the idsId path component
    m = re.search(r"/pdf/([a-zA-Z0-9]+)\.pdf", href)
    if m:
        return m.group(1)

    return None


def _extract_asx_code(cells, href: str) -> str:
    """Extract ASX code from table cells or href."""
    for cell in cells:
        text = cell.get_text(strip=True)
        if re.match(r"^[A-Z0-9]{2,5}$", text):
            return text

    # Try href param
    m = re.search(r"issuerCode=([A-Z0-9]{2,5})", href)
    if m:
        return m.group(1)

    return ""


def _infer_form_type(headline: str) -> str:
    headline_lower = headline.lower()
    if "603" in headline or "initial" in headline_lower or "becom" in headline_lower:
        return "603"
    if "604" in headline or "change" in headline_lower:
        return "604"
    if "605" in headline or "ceas" in headline_lower:
        return "605"
    return ""


def resolve_pdf_url(announcement: dict) -> str | None:
    """Fetch the display page and extract the real PDF URL."""
    display_url = announcement.get("display_url", "")
    if not display_url:
        return None
    try:
        resp = requests.get(display_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        # Find the PDF URL embedded in the page
        m = re.search(r'(https?://announcements\.asx\.com\.au/asxpdf/[^\s"\'<>]+\.pdf)', resp.text)
        if m:
            return m.group(1)
        # Fallback: relative path
        m = re.search(r'(asxpdf/\d{8}/pdf/[a-zA-Z0-9]+\.pdf)', resp.text)
        if m:
            return "https://announcements.asx.com.au/" + m.group(1)
        logger.warning(f"Could not find PDF URL in display page for {announcement['announcement_id']}")
        return None
    except Exception as e:
        logger.error(f"Failed to resolve PDF URL for {announcement['announcement_id']}: {e}")
        return None


def download_pdf(announcement: dict, pdf_path: Path) -> bool:
    """Resolve the real PDF URL, download to pdf_path. Returns True on success."""
    pdf_url = announcement.get("pdf_url") or resolve_pdf_url(announcement)
    if not pdf_url:
        logger.error(f"No PDF URL for announcement {announcement['announcement_id']}")
        return False
    announcement["pdf_url"] = pdf_url  # store for later use
    try:
        resp = requests.get(pdf_url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_path.write_bytes(resp.content)
        logger.info(f"Downloaded PDF: {pdf_path} ({len(resp.content)} bytes)")
        return True
    except Exception as e:
        logger.error(f"Failed to download PDF {pdf_url}: {e}")
        return False


if __name__ == "__main__":
    print("Fetching ASX substantial holder announcements...")
    processed = load_processed_ids()
    announcements = get_announcements()

    if not announcements:
        print("No substantial holder announcements found today.")
    else:
        for ann in announcements:
            ids_id = ann["announcement_id"]
            if ids_id in processed:
                print(f"  SKIP (already processed): {ids_id}")
                continue

            pdf_path = PDFS_DIR / f"{ids_id}.pdf"
            print(f"  Downloading {ids_id} ({ann['asx_code']}) -> {pdf_path}")
            ok = download_pdf(ann, pdf_path)
            if ok:
                print(f"    OK — {pdf_path}")
            else:
                print(f"    FAILED — check scraper.log")

    print("Done.")
