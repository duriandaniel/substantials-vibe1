"""
parser.py — Three-tier PDF parser for ASX substantial holder notices.

Tier 1 : pdfplumber + regex          (fast, free, handles clean digital PDFs)
Tier 2A: Claude Vision API           (for scanned/image-based pages)
Tier 2B: Claude text API             (for digital PDFs that Tier 1 can't fully parse)

Many ASX PDFs are scanned documents — the form pages are images, while the
appendix pages (trade tables) contain extractable text.  We detect image pages
by checking how much real text pdfplumber can extract.  If the key pages are
images we render them with PyMuPDF and send to Claude Vision instead of the
text API.
"""
import base64
import json
import logging
import os
import re
from pathlib import Path

import anthropic
import fitz          # PyMuPDF
import pdfplumber
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Required fields vary by form type:
#   603 (initial holder)  — new_percent is the current position; no previous
#   604 (change)          — new_percent required; previous is nice-to-have
#   605 (cease)           — holder dropped below 5%; new_percent not applicable
REQUIRED_FIELDS_BY_FORM = {
    "603": ["investment_manager", "new_percent", "date_of_change"],
    "604": ["investment_manager", "new_percent", "date_of_change"],
    "605": ["investment_manager", "date_of_change"],   # no new_percent for cease
    "":    ["investment_manager", "new_percent", "date_of_change"],  # unknown form
}
# Default (used externally)
REQUIRED_FIELDS = ["investment_manager", "new_percent", "date_of_change"]
OPTIONAL_FIELDS = ["previous_percent", "previous_shares"]

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Shared extraction prompt used for both text and vision calls
_EXTRACTION_PROMPT = (
    "You are a financial document parser. Extract data from this ASX substantial "
    "holder notice. Return ONLY valid JSON with these exact keys: "
    "investment_manager, manager_acn, date_of_change, previous_shares, "
    "previous_percent, new_shares, new_percent, consideration. "
    "Use null for any field you cannot find. No preamble, no markdown."
)

# A page is considered image-based if it yields fewer than this many real chars
# after stripping the boilerplate ASX watermark.
IMAGE_PAGE_THRESHOLD = 100


# ---------------------------------------------------------------------------
# Page classification helpers
# ---------------------------------------------------------------------------

def _page_real_text(page_text: str) -> str:
    """Strip the 'For personal use only' watermark that appears on every ASX page."""
    return re.sub(r"for\s+personal\s+use\s+only", "", page_text, flags=re.IGNORECASE).strip()


def _classify_pages(pdf_path: str | Path) -> dict:
    """
    Return per-page info: text content and whether the page is image-based.

    Returns:
        {
          "pages": [{"text": str, "is_image": bool}, ...],
          "has_image_pages": bool,
          "image_page_indices": [int, ...],   # 0-based
          "full_text": str,                   # concatenated text from all pages
        }
    """
    result = {"pages": [], "has_image_pages": False, "image_page_indices": [], "full_text": ""}
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for i, page in enumerate(pdf.pages):
                raw = page.extract_text() or ""
                real = _page_real_text(raw)
                is_image = len(real) < IMAGE_PAGE_THRESHOLD
                result["pages"].append({"text": raw, "is_image": is_image})
                if is_image:
                    result["image_page_indices"].append(i)
                    result["has_image_pages"] = True
    except Exception as e:
        logger.error(f"Page classification failed for {pdf_path}: {e}")
    result["full_text"] = "\n".join(p["text"] for p in result["pages"])
    return result


# ---------------------------------------------------------------------------
# Tier 1 — pdfplumber + regex
# ---------------------------------------------------------------------------

def extract_text(pdf_path: str | Path) -> str:
    """Extract all text from a PDF (used externally and in tests)."""
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
            return "\n".join(parts)
    except Exception as e:
        logger.error(f"pdfplumber failed to open {pdf_path}: {e}")
        return ""


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text)


_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _normalise_date(raw: str) -> str:
    """Normalise various date formats to YYYY-MM-DD."""
    raw = raw.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    # DD Month YYYY / DD-Mon-YY / DD/Month/YYYY (any separator)
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


def tier1_parse(text: str) -> dict:
    """Regex extraction from PDF text. Returns whatever fields can be found."""
    result = {}
    clean = _clean(text)

    # investment_manager — grab block between "Details of substantial holder" and "ACN/ARSN",
    # remove the "Name" label. Handles two-column layouts where name wraps around label.
    m = re.search(
        r"Details of substantial holder[^)]*\)\s*(.*?)\s*ACN/ARSN",
        clean,
        re.IGNORECASE | re.DOTALL,
    )
    if m:
        name_block = m.group(1)
        name = re.sub(r"\bName\b", " ", name_block, flags=re.IGNORECASE)
        name = re.sub(r"\s+", " ", name).strip()
        if name and not re.match(r"^(ACN|ARSN|NFPFRN|Not Applicable)", name, re.IGNORECASE):
            result["investment_manager"] = name
    if not result.get("investment_manager"):
        # Cover letter fallback: "from Foo in respect of"
        m = re.search(r"from\s+(.+?)\s+in respect of", text, re.IGNORECASE | re.DOTALL)
        if m:
            result["investment_manager"] = re.sub(r"\s+", " ", m.group(1)).strip()

    # manager_acn
    m = re.search(
        r"ACN/ARSN\s+\(if applicable\)\s*([0-9\s]+?)(?:\n|NFPFRN|The holder)",
        clean, re.IGNORECASE,
    )
    if m:
        acn = re.sub(r"\s+", " ", m.group(1)).strip()
        if acn and not re.match(r"not\s+applicable", acn, re.IGNORECASE):
            result["manager_acn"] = acn

    # date_of_change — all known formats
    _d   = r"\d{1,2}\s*/\s*\d{1,2}\s*/\s*\d{2,4}"
    _dt  = r"\d{1,2}[\s/\-][A-Za-z]{3,9}[\s/\-]\d{2,4}"
    _any = f"(?:{_d}|{_dt})"
    for pat in [
        rf"became a substantial holder on\s+({_any})",
        rf"ceased to be a substantial holder on\s+({_any})",
        rf"change in the interests of the substantial holder on\s+({_any})",
        rf"interests of the\s+({_d})\s+substantial holder on",
        rf"substantial holder on\s+({_any})",
    ]:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            result["date_of_change"] = _normalise_date(re.sub(r"\s+", " ", m.group(1)).strip())
            break

    # Voting power — Form 604 has previous + present columns
    m604 = re.search(
        r"Ordinary\s+[\d,]+\s+([\d.]+%)\s+[\d,]+\s+([\d.]+%)", clean, re.IGNORECASE
    )
    if m604:
        result["previous_percent"] = m604.group(1)
        result["new_percent"]      = m604.group(2)
    else:
        vp = re.findall(r"(\d+\.\d+%)", clean)
        if vp:
            result["new_percent"] = vp[-1]

    # Share counts
    shares = re.findall(r"\b(\d{1,3}(?:,\d{3})+)\b", clean)
    if shares:
        counts = sorted([int(s.replace(",", "")) for s in shares], reverse=True)
        result["new_shares"] = str(counts[0])

    m_prev = re.search(
        r"Previous notice\s+Present notice.*?(\d{1,3}(?:,\d{3})+)\s+[\d.]+%"
        r"\s+(\d{1,3}(?:,\d{3})+)\s+[\d.]+%",
        clean, re.IGNORECASE | re.DOTALL,
    )
    if m_prev:
        result["previous_shares"] = m_prev.group(1).replace(",", "")
        result["new_shares"]      = m_prev.group(2).replace(",", "")

    # Consideration
    m_con = re.search(r"\$\s*([\d,]+(?:\.\d{2})?)", clean)
    if m_con:
        result["consideration"] = "$" + m_con.group(1)

    return result


# ---------------------------------------------------------------------------
# Tier 2A — Claude Vision (for image-based pages)
# ---------------------------------------------------------------------------

def _render_pages(pdf_path: str | Path, page_indices: list[int], zoom: float = 2.5) -> list[bytes]:
    """Render PDF pages to PNG bytes using PyMuPDF at given zoom level."""
    images = []
    try:
        doc = fitz.open(str(pdf_path))
        mat = fitz.Matrix(zoom, zoom)
        for i in page_indices:
            if i >= len(doc):
                continue
            pix = doc[i].get_pixmap(matrix=mat)
            images.append(pix.tobytes("png"))
    except Exception as e:
        logger.error(f"Page render failed for {pdf_path}: {e}")
    return images


def tier2a_vision_parse(pdf_path: str | Path, image_page_indices: list[int]) -> dict:
    """
    Send image-based PDF pages to Claude Vision and extract structured data.
    Only sends pages 0 and 1 (the form pages with the key data).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping vision parse")
        return {}

    # Only look at the first 2 image pages — the form data lives there
    target_pages = [i for i in image_page_indices if i < 2]
    if not target_pages:
        target_pages = image_page_indices[:2]

    try:
        images = _render_pages(pdf_path, target_pages)
        if not images:
            logger.warning(f"No pages rendered for {pdf_path}")
            return {}

        client = anthropic.Anthropic(api_key=api_key)
        content: list = []
        for img_bytes in images:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": base64.standard_b64encode(img_bytes).decode(),
                },
            })
        content.append({"type": "text", "text": _EXTRACTION_PROMPT})

        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": content}],
            timeout=60,
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```json\s*|^```\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        logger.info(f"Tier 2A vision parse succeeded for {pdf_path} (pages {target_pages})")
        return parsed

    except json.JSONDecodeError as e:
        logger.error(f"Tier 2A JSON parse error for {pdf_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Tier 2A vision parse failed for {pdf_path}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Tier 2B — Claude text API
# ---------------------------------------------------------------------------

def tier2b_text_parse(text: str) -> dict:
    """Send PDF text to Claude text API and return parsed fields."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — skipping text parse")
        return {}
    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1024,
            system=_EXTRACTION_PROMPT,
            messages=[{"role": "user", "content": text[:15000]}],
            timeout=30,
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```json\s*|^```\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        logger.info("Tier 2B text parse succeeded")
        return parsed
    except json.JSONDecodeError as e:
        logger.error(f"Tier 2B JSON parse error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Tier 2B text parse failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Action type derivation
# ---------------------------------------------------------------------------

def _derive_action_type(form_type: str, previous_percent: str | None, new_percent: str | None) -> str:
    """
    Derive a human-readable action type from form type and percent values.

    603 → initial        (became substantial holder, crossed 5% threshold)
    605 → cease          (dropped below 5%, no longer substantial holder)
    604 → increase / decrease / change  (based on comparing percents)
    """
    ft = (form_type or "").strip()
    if ft == "603":
        return "initial"
    if ft == "605":
        return "cease"
    if ft == "604":
        try:
            prev = float(str(previous_percent or "").replace("%", "").strip())
            curr = float(str(new_percent or "").replace("%", "").strip())
            if curr > prev:
                return "increase"
            if curr < prev:
                return "decrease"
            return "change"
        except (ValueError, TypeError):
            return "change"
    # Unknown form type — infer from headline keywords stored in form_type field
    return "change"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def parse_pdf(pdf_path: str | Path, announcement: dict | None = None) -> dict:
    """
    Parse a PDF using a three-tier approach:
      Tier 1  : pdfplumber regex
      Tier 2A : Claude Vision  (when page 1 is a scanned image)
      Tier 2B : Claude text    (when page 1 has text but Tier 1 incomplete)
    """
    ann = announcement or {}
    result = {
        "announcement_id":  ann.get("announcement_id", ""),
        "asx_code":         ann.get("asx_code", ""),
        "company_name":     ann.get("company_name", ""),
        "form_type":        ann.get("form_type", ""),
        "action_type":      None,
        "lodgement_date":   ann.get("lodgement_date", ""),
        "pdf_url":          ann.get("pdf_url", ""),
        "investment_manager": None,
        "manager_acn":        None,
        "date_of_change":     None,
        "previous_shares":    None,
        "previous_percent":   None,
        "new_shares":         None,
        "new_percent":        None,
        "consideration":      None,
        "parse_method":       None,
        "confidence":         None,
    }

    try:
        # Classify pages — detect which are image-based
        page_info = _classify_pages(pdf_path)
        text = page_info["full_text"]

        if not text and not page_info["pages"]:
            logger.error(f"Could not open {pdf_path}")
            result.update(confidence="needs_review", parse_method="none")
            return result

        if page_info["has_image_pages"]:
            logger.info(
                f"{pdf_path}: image pages detected at indices "
                f"{page_info['image_page_indices']}"
            )

        # Use form-type-aware required fields
        form_type = result.get("form_type") or ann.get("form_type") or ""
        required = REQUIRED_FIELDS_BY_FORM.get(form_type, REQUIRED_FIELDS_BY_FORM[""])

        # ── Tier 1 ──────────────────────────────────────────────────────────
        t1 = tier1_parse(text)
        result.update({k: v for k, v in t1.items() if v is not None})

        missing = [f for f in required if not result.get(f)]
        if not missing:
            result.update(confidence="high", parse_method="rule-based")
            result["action_type"] = _derive_action_type(
                form_type, result.get("previous_percent"), result.get("new_percent")
            )
            logger.info(f"Tier 1 success for {pdf_path}")
            return result

        logger.info(f"Tier 1 missing {missing} for {pdf_path} — escalating")

        # ── Tier 2 ──────────────────────────────────────────────────────────
        if page_info["has_image_pages"]:
            t2 = tier2a_vision_parse(pdf_path, page_info["image_page_indices"])
            parse_method = "ai-vision"
        else:
            t2 = tier2b_text_parse(text)
            parse_method = "ai"

        if t2:
            for k, v in t2.items():
                if v is not None and not result.get(k):
                    result[k] = v

        # Normalise date regardless of which tier produced it
        if result.get("date_of_change"):
            result["date_of_change"] = _normalise_date(result["date_of_change"])

        # Normalise percent fields — add % sign if missing (Vision API sometimes omits it)
        for pct_field in ("new_percent", "previous_percent"):
            v = result.get(pct_field)
            if v and str(v) != "null" and "%" not in str(v):
                try:
                    float(str(v))
                    result[pct_field] = f"{v}%"
                except ValueError:
                    pass

        missing_after = [f for f in required if not result.get(f)]
        if missing_after:
            result.update(confidence="needs_review", parse_method=parse_method)
            logger.warning(f"Still missing {missing_after} after Tier 2 for {pdf_path}")
        else:
            result.update(confidence="low", parse_method=parse_method)
            logger.info(f"Tier 2 success for {pdf_path}: method={parse_method}")

        result["action_type"] = _derive_action_type(
            form_type, result.get("previous_percent"), result.get("new_percent")
        )

    except Exception as e:
        logger.error(f"parse_pdf failed for {pdf_path}: {e}", exc_info=True)
        result.update(confidence="needs_review", parse_method="none")

    return result


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    test_dir = Path("test_pdfs")
    pdfs = sorted(test_dir.glob("*.pdf"))
    if not pdfs:
        print("No PDFs in test_pdfs/")
        sys.exit(1)

    print(f"{'PDF':<30} {'Conf':<14} {'Method':<12} {'Manager':<38} {'Prev%':<7} {'New%':<7} {'Date'}")
    print("-" * 120)
    for pdf_path in pdfs:
        r = parse_pdf(pdf_path)
        print(
            f"{pdf_path.name:<30} "
            f"{r.get('confidence') or '':<14} "
            f"{r.get('parse_method') or '':<12} "
            f"{(r.get('investment_manager') or '')[:36]:<38} "
            f"{r.get('previous_percent') or '':<7} "
            f"{r.get('new_percent') or '':<7} "
            f"{r.get('date_of_change') or ''}"
        )
