# Search Pattern Discovery Engine

This repository contains a production‑grade asynchronous search pattern
intelligence engine.  It crawls domains with Playwright, detects how search is
implemented, and stores reusable patterns for later scraping.

## Features

* layered detection (DOM, heuristics, network, CMS-specific)
* confidence scoring
* built in-memory cache and database upsert
* concurrency with semaphore (20 domains)
* retry with exponential backoff
* configurable timeouts and selectors
* structured results and logging
* simple CLI for discovery and re‑verification
* asynchronous Postgres (Supabase) support

## Quickstart

1. create a virtual environment:
   ```powershell
   python -m venv .venv
   . .venv/Scripts/Activate.ps1
   ```
2. install dependencies:
   ```powershell
   pip install -r requirements.txt
   playwright install
   ```
3. set `DATABASE_URL` to your Supabase/Postgres connection string.
4. run discovery on a list of domains:
   ```powershell
   python main.py https://example.com https://bbc.com
   ```
5. re‑verify existing patterns:
   ```powershell
   python main.py --verify https://example.com
   ```

## Extending

* detection logic lives in `detector.py`; scores are in `scorers.py`;
  concurrency is handled by `worker.py` and database interactions by `db.py`.
* all timeouts and selectors are configurable via environment variables or
  module constants.

## Testing

Straightforward unit tests exist in `tests/` and require `pytest`.

```powershell
pip install pytest
pytest -q
```

## Roadmap

* add distributed queue (Redis/Cloud Tasks)
* proxy rotation and CAPTCHA handling
* consistent verification of stored patterns
* metrics export (Prometheus)

The code is written to be easily pluggable; feel free to fork!  
