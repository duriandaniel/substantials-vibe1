"""
Tests for main.py — trading day check.
"""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from main import is_trading_day, AU_PUBLIC_HOLIDAYS


# ---------------------------------------------------------------------------
# Trading day logic
# ---------------------------------------------------------------------------

def test_weekday_is_trading_day():
    # 2026-03-26 is a Thursday
    assert is_trading_day(date(2026, 3, 26)) is True


def test_saturday_not_trading():
    # 2026-03-28 is a Saturday
    assert is_trading_day(date(2026, 3, 28)) is False


def test_sunday_not_trading():
    # 2026-03-29 is a Sunday
    assert is_trading_day(date(2026, 3, 29)) is False


def test_au_holiday_not_trading():
    # 2026-04-25 is ANZAC Day
    assert is_trading_day(date(2026, 4, 25)) is False


def test_good_friday_not_trading():
    # 2026-04-03 is Good Friday
    assert is_trading_day(date(2026, 4, 3)) is False


def test_regular_weekday_in_holiday_set_false():
    # Verify every date in AU_PUBLIC_HOLIDAYS returns False
    for h in AU_PUBLIC_HOLIDAYS:
        d = date.fromisoformat(h)
        assert is_trading_day(d) is False, f"Expected non-trading for holiday {h}"


def test_known_trading_days():
    known_trading = [
        date(2026, 1, 2),   # Friday after New Year
        date(2026, 3, 26),  # Thursday
        date(2025, 3, 3),   # Monday
    ]
    for d in known_trading:
        assert is_trading_day(d) is True, f"Expected trading day for {d}"
