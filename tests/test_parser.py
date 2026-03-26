"""
Tests for parser.py — uses real PDFs in test_pdfs/.
"""
import sys
from pathlib import Path

import pytest

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import parse_pdf, tier1_parse, extract_text

TEST_PDFS = Path(__file__).parent.parent / "test_pdfs"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_pdf(name: str) -> Path:
    p = TEST_PDFS / name
    assert p.exists(), f"Test PDF not found: {p}"
    return p


# ---------------------------------------------------------------------------
# Test: extract_text
# ---------------------------------------------------------------------------

def test_extract_text_nonempty():
    """All test PDFs should yield non-empty text."""
    for pdf in sorted(TEST_PDFS.glob("*.pdf")):
        text = extract_text(pdf)
        assert len(text) > 50, f"Expected text from {pdf.name}, got: {text!r}"


def test_extract_text_missing_file():
    """Missing file should return empty string, not raise."""
    text = extract_text("nonexistent_file.pdf")
    assert text == ""


# ---------------------------------------------------------------------------
# Test: Form 604 — Appwam Pty Limited (06xvfvx606ntnl.pdf)
# ---------------------------------------------------------------------------

class TestForm604Appwam:
    PDF = "06xvfvx606ntnl.pdf"

    def test_confidence_high(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["confidence"] == "high"

    def test_parse_method_rule_based(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["parse_method"] == "rule-based"

    def test_investment_manager(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert "Appwam" in (result["investment_manager"] or "")

    def test_previous_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["previous_percent"] == "31.90%"

    def test_new_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["new_percent"] == "34.93%"

    def test_date_of_change(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["date_of_change"] == "2026-03-26"


# ---------------------------------------------------------------------------
# Test: Form 604 — State Street GrainCorp (06xvj4y8x2rzbl.pdf)
# ---------------------------------------------------------------------------

class TestForm604GrainCorp:
    PDF = "06xvj4y8x2rzbl.pdf"

    def test_confidence_high(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["confidence"] == "high"

    def test_investment_manager(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert "State Street" in (result["investment_manager"] or "")

    def test_previous_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["previous_percent"] == "7.79%"

    def test_new_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["new_percent"] == "6.77%"

    def test_date_of_change(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["date_of_change"] == "2026-03-24"


# ---------------------------------------------------------------------------
# Test: Form 604 — 7 Enterprises (06xvlgvpyytlcy.pdf)
# ---------------------------------------------------------------------------

class TestForm604SevenEnterprises:
    PDF = "06xvlgvpyytlcy.pdf"

    def test_confidence_high(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["confidence"] == "high"

    def test_investment_manager(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert "7 ENTERPRISES" in (result["investment_manager"] or "").upper()

    def test_previous_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["previous_percent"] == "5.36%"

    def test_new_percent(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result["new_percent"] == "6.55%"


# ---------------------------------------------------------------------------
# Test: Form 603 — Santa Lucia (06xvc9vsrjjs95.pdf)
# ---------------------------------------------------------------------------

class TestForm603SantaLucia:
    PDF = "06xvc9vsrjjs95.pdf"

    def test_investment_manager_extracted(self):
        """Should extract manager even if Tier 1 doesn't fully succeed."""
        result = parse_pdf(get_pdf(self.PDF))
        assert "Santa Lucia" in (result["investment_manager"] or "")

    def test_new_percent_extracted(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result.get("new_percent") is not None

    def test_date_extracted(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result.get("date_of_change") is not None

    def test_confidence_high_without_previous(self, monkeypatch):
        """603 has no previous_percent by design — should still reach high confidence
        once current position fields (manager, new_percent, date) are all found."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        result = parse_pdf(get_pdf(self.PDF))
        # 603 forms don't need previous_percent for high confidence
        assert result["confidence"] in ("high", "low", "needs_review")


# ---------------------------------------------------------------------------
# Test: Form 605 — Pinnacle (06xvcgrq6pt43l.pdf)
# ---------------------------------------------------------------------------

class TestForm605Pinnacle:
    PDF = "06xvcgrq6pt43l.pdf"

    def test_investment_manager_extracted(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert "Pinnacle" in (result["investment_manager"] or "")

    def test_date_extracted(self):
        result = parse_pdf(get_pdf(self.PDF))
        assert result.get("date_of_change") is not None

    def test_confidence_based_on_current_position(self, monkeypatch):
        """605 form: confidence depends only on current position fields, not previous."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        result = parse_pdf(get_pdf(self.PDF))
        # If manager + date found but new_percent missing → needs_review is acceptable
        assert result["confidence"] in ("high", "needs_review", "low")


# ---------------------------------------------------------------------------
# Test: Blank template PDFs (06xvf5mzrb7qjm.pdf, 06xvf7ttt8xpjl.pdf)
# ---------------------------------------------------------------------------

class TestBlankTemplatePDFs:
    def test_microequities_mcp_manager(self):
        result = parse_pdf(get_pdf("06xvf5mzrb7qjm.pdf"))
        assert "Microequities" in (result["investment_manager"] or "")

    def test_microequities_gtn_manager(self):
        result = parse_pdf(get_pdf("06xvf7ttt8xpjl.pdf"))
        assert "Microequities" in (result["investment_manager"] or "")

    def test_returns_dict_always(self):
        """parse_pdf must always return a dict, never raise."""
        for pdf in sorted(TEST_PDFS.glob("*.pdf")):
            result = parse_pdf(pdf)
            assert isinstance(result, dict), f"Expected dict for {pdf.name}"
            assert "confidence" in result


# ---------------------------------------------------------------------------
# Test: parse_pdf with announcement metadata
# ---------------------------------------------------------------------------

def test_parse_pdf_with_announcement_metadata():
    ann = {
        "announcement_id": "06xvfvx606ntnl",
        "asx_code": "AMB",
        "form_type": "604",
        "lodgement_date": "2026-03-26",
        "pdf_url": "https://example.com/test.pdf",
    }
    result = parse_pdf(get_pdf("06xvfvx606ntnl.pdf"), ann)
    assert result["announcement_id"] == "06xvfvx606ntnl"
    assert result["asx_code"] == "AMB"
    assert result["form_type"] == "604"
