"""
parser.py — Two-tier PDF parser for ASX substantial holder notices.

Tier 1: pdfplumber + regex (fast, deterministic)
Tier 2: Claude API fallback (for documents that Tier 1 cannot fully parse)
"""
import json
import logging
import os
import re
from pathlib import Path

import anthropic
import pdfplumber
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Fields required to consider a parse successful.
# previous_percent is excluded — 603 forms (initial holder) never have it,
# and 605 forms (ceasing) don't reliably carry it either.
# We only require current position fields.
REQUIRED_FIELDS = ["investment_manager", "new_percent", "date_of_change"]

# Fields that are nice-to-have but not required for confidence scoring.
OPTIONAL_FIELDS = ["previous_percent", "previous_shares"]

CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_SYSTEM_PROMPT = (
    "You are a financial document parser. Extract data from ASX substantial holder notices. "
    "Return ONLY valid JSON with these exact keys: "
    "investment_manager, manager_acn, date_of_change, previous_shares, "
    "previous_percent, new_shares, new_percent, consideration. "
    "Use null for any field you cannot find. No preamble, no markdown."
)


# ---------------------------------------------------------------------------
# Tier 1 — pdfplumber + regex
# ---------------------------------------------------------------------------

def extract_text(pdf_path: str | Path) -> str:
    """Extract all text from a PDF file."""
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
            return "\n".join(pages)
    except Exception as e:
        logger.error(f"pdfplumber failed to open {pdf_path}: {e}")
        return ""


def _clean(text: str) -> str:
    """Collapse multiple spaces/newlines for easier regex matching."""
    return re.sub(r"\s+", " ", text)


def tier1_parse(text: str) -> dict:
    """Run regex extraction against PDF text. Returns partial or full result dict."""
    result = {}
    clean = _clean(text)

    # investment_manager — grab everything between "Details of substantial holder"
    # and first "ACN/ARSN", then strip the "Name" label.
    # This handles two-column PDFs where the name wraps around the "Name" label.
    m = re.search(
        r"Details of substantial holder[^)]*\)\s*(.*?)\s*ACN/ARSN",
        clean,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        name_block = m.group(1)
        # Remove the "Name" label word and any trailing noise
        name = re.sub(r"\bName\b", " ", name_block, flags=re.IGNORECASE)
        name = re.sub(r"\s+", " ", name).strip()
        # Reject empty or pure-label captures
        if name and not re.match(r"^(ACN|ARSN|NFPFRN|Not Applicable)", name, re.IGNORECASE):
            result["investment_manager"] = name
    if not result.get("investment_manager"):
        # Fallback for blank-template PDFs: extract from cover letter "from Foo in respect of"
        m = re.search(r"from\s+(.+?)\s+in respect of", text, re.IGNORECASE | re.DOTALL)
        if m:
            name = re.sub(r"\s+", " ", m.group(1)).strip()
            result["investment_manager"] = name

    # manager_acn — digits after "ACN/ARSN (if applicable)"
    m = re.search(
        r"ACN/ARSN\s+\(if applicable\)\s*([0-9\s]+?)(?:\n|NFPFRN|The holder)",
        clean,
        re.IGNORECASE,
    )
    if m:
        acn = re.sub(r"\s+", " ", m.group(1)).strip()
        # Strip leading "Not Applicable" noise
        if acn and not re.match(r"not\s+applicable", acn, re.IGNORECASE):
            result["manager_acn"] = acn

    # date_of_change — covers 603 (became), 604 (change), 605 (ceased)
    # Numeric date: DD/MM/YY or DD/MM/YYYY
    _d = r"\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}"
    # Textual date: DD Month YYYY or DD/Month/YYYY or DD-Mon-YY
    _dt = r"\d{1,2}[\s/\-][A-Za-z]{3,9}[\s/\-]\d{2,4}"
    _any_date = f"(?:{_d}|{_dt})"
    date_patterns = [
        rf"became a substantial holder on\s+({_any_date})",
        rf"ceased to be a substantial holder on\s+({_any_date})",
        rf"change in the interests of the substantial holder on\s+({_any_date})",
        rf"interests of the\s+({_d})\s+substantial holder on",
        rf"substantial holder on\s+({_any_date})",
    ]
    for pat in date_patterns:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            raw_date = re.sub(r"\s+", " ", m.group(1)).strip()
            result["date_of_change"] = _normalise_date(raw_date)
            break

    # Voting power percentages
    # For Form 604 — extract both Previous notice and Present notice voting power
    # Pattern: table row with "Ordinary ... 31.90% ... 34.93%"
    m604 = re.search(
        r"Ordinary\s+[\d,]+\s+([\d.]+%)\s+[\d,]+\s+([\d.]+%)",
        clean,
        re.IGNORECASE,
    )
    if m604:
        result["previous_percent"] = m604.group(1)
        result["new_percent"] = m604.group(2)
    else:
        # Form 603/605 — single "Voting power" column
        # Look for a percentage in "Voting power" section
        vp_matches = re.findall(r"(\d+\.\d+%)", clean)
        if vp_matches:
            # For 603: only new_percent (no previous)
            result["new_percent"] = vp_matches[-1]

    # Shares
    # previous_shares: "Person's votes" in previous notice for 604
    # new_shares: Number of securities in section 2 for 603/605
    shares_matches = re.findall(r"\b(\d{1,3}(?:,\d{3})+)\b", clean)
    if shares_matches:
        # Largest number is usually the share count
        share_counts = [int(s.replace(",", "")) for s in shares_matches]
        share_counts.sort(reverse=True)
        if share_counts:
            result["new_shares"] = str(share_counts[0])

    # For 604, look for "Previous notice ... Person's votes"
    m_prev = re.search(
        r"Previous notice\s+Present notice.*?(\d{1,3}(?:,\d{3})+)\s+[\d.]+%\s+(\d{1,3}(?:,\d{3})+)\s+[\d.]+%",
        clean,
        re.IGNORECASE | re.DOTALL,
    )
    if m_prev:
        result["previous_shares"] = m_prev.group(1).replace(",", "")
        result["new_shares"] = m_prev.group(2).replace(",", "")

    # consideration — dollar amounts
    m_con = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", clean)
    if m_con:
        result["consideration"] = "$" + m_con.group(1)

    return result


_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _normalise_date(raw: str) -> str:
    """Normalise various date formats to YYYY-MM-DD.

    Handles: DD/MM/YY, DD/MM/YYYY, DD-Mon-YY, DD/Month/YYYY, YYYY-MM-DD
    """
    raw = raw.strip()

    # Already ISO format
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    # DD Month YYYY or DD-Mon-YY (space, dash, or slash separators)
    m = re.match(r"^(\d{1,2})[\s\-/]([A-Za-z]+)[\s\-/](\d{2,4})$", raw)
    if m:
        day, mon_str, year = m.group(1), m.group(2).lower()[:3], m.group(3)
        month = _MONTH_MAP.get(mon_str)
        if month:
            if len(year) == 2:
                year = "20" + year
            try:
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass

    # DD/Month/YYYY (e.g. 24/March/2026)
    m = re.match(r"^(\d{1,2})/([A-Za-z]+)/(\d{2,4})$", raw)
    if m:
        day, mon_str, year = m.group(1), m.group(2).lower()[:3], m.group(3)
        month = _MONTH_MAP.get(mon_str)
        if month:
            if len(year) == 2:
                year = "20" + year
            try:
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass

    # DD/MM/YY or DD/MM/YYYY
    parts = raw.split("/")
    if len(parts) == 3:
        day, month, year = parts
        if len(year) == 2:
            year = "20" + year
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            pass

    return raw


# ---------------------------------------------------------------------------
# Tier 2 — Claude API
# ---------------------------------------------------------------------------

def tier2_parse(text: str) -> dict:
    """Send PDF text to Claude API and return parsed fields."""
    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning("ANTHROPIC_API_KEY not set — skipping Tier 2")
            return {}

        client = anthropic.Anthropic(api_key=api_key)
        # Truncate text to avoid token limits
        truncated = text[:15000]
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=CLAUDE_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": truncated}],
            timeout=30,
        )
        raw = message.content[0].text.strip()
        # Strip markdown code fences if present
        raw = re.sub(r"^```json\s*|^```\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        logger.info("Tier 2 Claude API parse succeeded")
        return parsed
    except json.JSONDecodeError as e:
        logger.error(f"Tier 2 JSON parse error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Tier 2 Claude API error: {e}")
        return {}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def parse_pdf(pdf_path: str | Path, announcement: dict | None = None) -> dict:
    """
    Parse a PDF file using two-tier approach.

    Returns a dict with all CSV fields filled in as best as possible.
    Sets 'confidence' and 'parse_method' fields.
    """
    ann = announcement or {}
    result = {
        "announcement_id": ann.get("announcement_id", ""),
        "asx_code": ann.get("asx_code", ""),
        "company_name": ann.get("company_name", ""),
        "form_type": ann.get("form_type", ""),
        "lodgement_date": ann.get("lodgement_date", ""),
        "pdf_url": ann.get("pdf_url", ""),
        "investment_manager": None,
        "manager_acn": None,
        "date_of_change": None,
        "previous_shares": None,
        "previous_percent": None,
        "new_shares": None,
        "new_percent": None,
        "consideration": None,
        "parse_method": None,
        "confidence": None,
    }

    try:
        text = extract_text(pdf_path)
        if not text:
            logger.error(f"No text extracted from {pdf_path}")
            result["confidence"] = "needs_review"
            result["parse_method"] = "none"
            return result

        # Tier 1
        tier1 = tier1_parse(text)
        result.update({k: v for k, v in tier1.items() if v is not None})

        missing = [f for f in REQUIRED_FIELDS if not result.get(f)]
        if not missing:
            result["confidence"] = "high"
            result["parse_method"] = "rule-based"
            logger.info(f"Tier 1 success for {pdf_path}: confidence=high")
            return result

        logger.info(f"Tier 1 missing fields {missing} for {pdf_path} — escalating to Tier 2")

        # Tier 2
        tier2 = tier2_parse(text)
        if tier2:
            for k, v in tier2.items():
                if v is not None and not result.get(k):
                    result[k] = v

        # Normalise date in case Tier 2 returned a non-ISO format
        if result.get("date_of_change"):
            result["date_of_change"] = _normalise_date(result["date_of_change"])

        missing_after = [f for f in REQUIRED_FIELDS if not result.get(f)]
        if missing_after:
            result["confidence"] = "needs_review"
            result["parse_method"] = "ai"
            logger.warning(f"Still missing after Tier 2: {missing_after} for {pdf_path}")
        else:
            result["confidence"] = "low"
            result["parse_method"] = "ai"
            logger.info(f"Tier 2 success for {pdf_path}: confidence=low")

    except Exception as e:
        logger.error(f"parse_pdf failed for {pdf_path}: {e}")
        result["confidence"] = "needs_review"
        result["parse_method"] = "none"

    return result


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from pathlib import Path

    test_dir = Path("test_pdfs")
    pdfs = sorted(test_dir.glob("*.pdf"))
    if not pdfs:
        print("No PDFs found in test_pdfs/")
        sys.exit(1)

    print(f"{'PDF':<30} {'Confidence':<14} {'Method':<12} {'Manager':<40} {'Prev%':<8} {'New%':<8} {'Date':<12}")
    print("-" * 130)

    for pdf_path in pdfs:
        result = parse_pdf(pdf_path)
        manager = (result.get("investment_manager") or "")[:38]
        print(
            f"{pdf_path.name:<30} "
            f"{result.get('confidence') or '':<14} "
            f"{result.get('parse_method') or '':<12} "
            f"{manager:<40} "
            f"{result.get('previous_percent') or '':<8} "
            f"{result.get('new_percent') or '':<8} "
            f"{result.get('date_of_change') or '':<12}"
        )
