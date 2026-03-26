"""
Tests for output.py — deduplication and CSV writing.
"""
import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import output


@pytest.fixture(autouse=True)
def tmp_csv(tmp_path, monkeypatch):
    """Redirect CSV paths to a temp directory for each test."""
    monkeypatch.setattr(output, "OUTPUT_CSV", tmp_path / "output.csv")
    monkeypatch.setattr(output, "NEEDS_REVIEW_CSV", tmp_path / "needs_review.csv")
    return tmp_path


# ---------------------------------------------------------------------------
# append_result
# ---------------------------------------------------------------------------

def test_append_creates_file_with_headers():
    record = {"announcement_id": "AA001", "asx_code": "XYZ", "confidence": "high"}
    output.append_result(record)
    assert output.OUTPUT_CSV.exists()
    with open(output.OUTPUT_CSV) as f:
        headers = f.readline().strip().split(",")
    assert "announcement_id" in headers
    assert "asx_code" in headers


def test_append_writes_record():
    record = {
        "announcement_id": "AA001",
        "asx_code": "XYZ",
        "confidence": "high",
        "parse_method": "rule-based",
        "investment_manager": "Test Corp",
    }
    output.append_result(record)
    with open(output.OUTPUT_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["announcement_id"] == "AA001"
    assert rows[0]["investment_manager"] == "Test Corp"


def test_deduplication_skips_existing_id():
    record = {"announcement_id": "AA001", "asx_code": "XYZ"}
    result1 = output.append_result(record)
    result2 = output.append_result(record)
    assert result1 is True
    assert result2 is False  # duplicate → skipped

    with open(output.OUTPUT_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1  # only one row written


def test_multiple_different_records():
    records = [
        {"announcement_id": f"ID{i:03d}", "asx_code": f"T{i:02d}"}
        for i in range(5)
    ]
    for r in records:
        output.append_result(r)

    with open(output.OUTPUT_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 5


def test_append_handles_none_values():
    record = {
        "announcement_id": "AA001",
        "asx_code": None,
        "previous_percent": None,
    }
    # Should not raise
    result = output.append_result(record)
    assert result is True


# ---------------------------------------------------------------------------
# log_needs_review
# ---------------------------------------------------------------------------

def test_log_needs_review_creates_file():
    output.log_needs_review("ID001", "XYZ", "http://x.com", "missing fields")
    assert output.NEEDS_REVIEW_CSV.exists()


def test_log_needs_review_writes_entry():
    output.log_needs_review("ID001", "XYZ", "http://x.com", "missing: previous_percent")
    with open(output.NEEDS_REVIEW_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["announcement_id"] == "ID001"
    assert rows[0]["asx_code"] == "XYZ"
    assert "previous_percent" in rows[0]["reason"]
