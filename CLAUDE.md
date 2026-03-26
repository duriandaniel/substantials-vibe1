# ASX Substantial Holder Scraper

## Project Goal
Scrape ASX substantial holder notices (Forms 603, 604, 605), parse PDFs,
output structured CSV for dashboard ingestion. Runs via GitHub Actions hourly
on ASX trading days (weekdays AEST, excluding AU public holidays).

## Scraping Approach — Simple HTTP (no browser needed)

Endpoint: `GET https://www.asx.com.au/asx/v2/statistics/todayAnns.do`
- Returns an HTML page with a table of today's announcements
- No special headers, cookies, or authentication required — public endpoint
- Parse the HTML table with BeautifulSoup
- Filter rows where headline contains "substantial" (case-insensitive)
- Extract from each matching row: ASX code, time, headline, idsId param
- PDF URL pattern: `https://www.asx.com.au/asxpdf/{date}/pdf/{idsId}.pdf`
- Do NOT use pyasx, Playwright, or any headless browser

## Architecture

| Module | Responsibility |
|--------|----------------|
| scraper.py | GET todayAnns.do, parse HTML, filter substantial notices, download PDFs |
| parser.py | Tier 1 pdfplumber regex, Tier 2 Claude API fallback |
| output.py | Append to output.csv, deduplicate on announcement_id (idsId) |
| notifier.py | Gmail SMTP alert on low/needs_review confidence |
| main.py | Orchestrate all modules; never crash on single PDF failure |

## CSV Schema
```
announcement_id, asx_code, company_name, form_type, lodgement_date,
date_of_change, investment_manager, manager_acn, previous_shares,
previous_percent, new_shares, new_percent, consideration,
parse_method, confidence, pdf_url
```
- announcement_id = idsId value from the HTML
- form_type = infer from headline: "603", "604", or "605"
- company_name = from HTML table (may need separate lookup or leave blank)

## PDF Parsing — Two-Tier

### Tier 1: pdfplumber + regex (always run first)
- Required fields: investment_manager, previous_percent, new_percent, date_of_change
- All found → confidence="high", parse_method="rule-based"
- Any missing → escalate to Tier 2

### Tier 2: Claude API fallback
- Model: claude-sonnet-4-20250514
- System prompt: "You are a financial document parser. Extract data from ASX
  substantial holder notices. Return ONLY valid JSON with these exact keys:
  investment_manager, manager_acn, date_of_change, previous_shares,
  previous_percent, new_shares, new_percent, consideration.
  Use null for any field you cannot find. No preamble, no markdown."
- Fields returned → confidence="low", parse_method="ai"
- Fields still missing → confidence="needs_review" → trigger email alert
- 30s timeout, wrapped in try/except

## Deduplication
- processed_ids.txt stores every idsId ever processed (one per line)
- Check before downloading — skip if already seen
- Commit processed_ids.txt back to repo after each run

## Email Alerts
- Trigger when confidence == "low" OR confidence == "needs_review"
- Env vars: ALERT_EMAIL_FROM, ALERT_EMAIL_TO, SMTP_PASSWORD
- Subject: "ASX Scraper Alert: [confidence] — [asx_code] [announcement_id]"
- Body: announcement_id, asx_code, pdf_url, missing fields, confidence
- Port 587, STARTTLS

## Environment Variables (never hardcode)
```
ANTHROPIC_API_KEY
ALERT_EMAIL_FROM
ALERT_EMAIL_TO
SMTP_PASSWORD
```

## Error Handling — Non-Negotiable
- Every module: try/except that logs and continues — never crash full pipeline
- Partial output.csv is always better than a failed run
- Log to scraper.log with ISO timestamps
- Failed PDFs → log to needs_review.csv with reason

## ASX Trading Days
- Weekdays only (GitHub cron: `0 * * * 1-5`)
- Check hardcoded AU public holiday list before processing
```python
AU_PUBLIC_HOLIDAYS = {
    "2024-01-01","2024-01-26","2024-03-29","2024-04-01","2024-04-25",
    "2024-06-10","2024-12-25","2024-12-26",
    "2025-01-01","2025-01-27","2025-04-18","2025-04-21","2025-04-25",
    "2025-06-09","2025-12-25","2025-12-26",
    "2026-01-01","2026-01-26","2026-04-03","2026-04-06","2026-04-25",
    "2026-06-08","2026-12-25","2026-12-28",
}
```

## GitHub Actions
- Schedule: `0 * * * 1-5`
- OS: ubuntu-latest, Python 3.11
- Secrets: ANTHROPIC_API_KEY, ALERT_EMAIL_FROM, ALERT_EMAIL_TO, SMTP_PASSWORD
- After run: commit processed_ids.txt, output.csv, needs_review.csv back to repo
  (use GITHUB_TOKEN, set permissions: contents: write)
- Upload output.csv as artifact "asx-output", 30-day retention
- Cache pip with actions/cache

## File Structure
```
/
├── CLAUDE.md
├── .env                  # never commit
├── .gitignore
├── requirements.txt
├── main.py
├── scraper.py
├── parser.py
├── output.py
├── notifier.py
├── processed_ids.txt     # commit this
├── output.csv            # commit this
├── needs_review.csv      # commit this
├── test_pdfs/            # commit this — real ASX PDFs for testing
├── pdfs/                 # never commit
└── .github/workflows/scraper.yml
```

## .gitignore
```
.env
pdfs/
scraper.log
__pycache__/
*.pyc
.pytest_cache/
```
