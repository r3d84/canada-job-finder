"""
data_pipeline.py
=================
Core data-processing module for a personal Canadian entry-level /
visa-sponsored job search tool.

Data source
-----------
Rather than scraping the rendered HTML of jobbank.gc.ca/jobsearch (whose
CSS class names are unstable and partly JS-templated), this module drives
Job Bank's own live Atom feed, which accepts the same search parameters as
the public search box (dkw = keyword, sort = D for newest-first):

    https://www.jobbank.gc.ca/jobsearch/feed/jobSearchRSSfeed?dkw=<keyword>&sort=D&rows=<n>

Each <entry> contains a title, a link, and a summary formatted as:
    "Job number: <id> Location: <city, prov> Employer: <name> Salary: <text>"

This is fetched with `requests` and parsed with `BeautifulSoup` (xml parser),
same tools you asked for, just pointed at Job Bank's structured feed rather
than guessing at HTML markup that can silently change under you.

Pipeline behaviour
-------------------
1. For every keyword in SEARCH_KEYWORDS, fetch the live feed (with retries
   and timeout handling).
2. Parse each entry into title / employer / location / salary / URL.
3. Apply a strict INCLUSION filter (title must contain one of the target
   entry-level terms) and a strict EXCLUSION filter (title or summary must
   NOT contain any disqualifying phrase).
4. Upsert surviving listings into a local SQLite cache (daily_jobs.db),
   recording the date each job was first found and last seen.

Public functions meant to be called from a Streamlit front-end:
    run_pipeline()          -> triggers a live fetch + cache refresh
    get_jobs_for_date(date) -> read cached jobs found on a given date
    get_all_cached_jobs()   -> read the full local cache
    get_available_dates()   -> list of dates with cached results
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

logger = logging.getLogger("jobbank_pipeline")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

DB_PATH = Path(__file__).resolve().parent / "daily_jobs.db"

FEED_ENDPOINT = "https://www.jobbank.gc.ca/jobsearch/feed/jobSearchRSSfeed"

# Keywords used to query Job Bank AND as the strict inclusion whitelist
# applied to the returned job titles.
SEARCH_KEYWORDS = [
    "Warehouse",
    "Production",
    "Farm",
    "Greenhouse",
    "General Labourer",
    "Harvest",
    "Packer",
]
INCLUDE_KEYWORDS = [k.lower() for k in SEARCH_KEYWORDS]

# Any of these phrases appearing in the title OR summary drops the listing,
# no exceptions.
EXCLUDE_PHRASES = [
    "forklift",
    "machine operator",
    "driver license",
    "driver's license",
    "drivers license",
    "heavy machinery",
    "licensed",
    "experience required",
]

ROWS_PER_KEYWORD = 100          # feed page size per keyword
REQUEST_TIMEOUT = 15            # seconds per HTTP request
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0        # seconds; multiplied by attempt number
DELAY_BETWEEN_KEYWORDS = 1.5    # seconds; be a polite, non-hammering client

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PersonalJobSearchTool/1.0; "
        "personal-use entry-level job alert script)"
    ),
    "Accept": "application/atom+xml, application/xml, text/xml, */*",
}


@dataclass
class JobListing:
    job_id: str
    title: str
    employer: str
    location: str
    salary: str
    url: str
    matched_keyword: str
    date_posted: Optional[str]
    date_found: str


# --------------------------------------------------------------------------
# SQLite cache layer
# --------------------------------------------------------------------------

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    with closing(get_connection()) as conn, conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id          TEXT PRIMARY KEY,
                title           TEXT NOT NULL,
                employer        TEXT,
                location        TEXT,
                salary          TEXT,
                url             TEXT,
                matched_keyword TEXT,
                date_posted     TEXT,
                date_found      TEXT NOT NULL,
                last_seen       TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scrape_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword       TEXT NOT NULL,
                run_date      TEXT NOT NULL,
                run_timestamp TEXT NOT NULL,
                jobs_found    INTEGER NOT NULL,
                status        TEXT NOT NULL,
                detail        TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_date_found ON jobs(date_found)")
    logger.info("SQLite schema ready at %s", DB_PATH)


def upsert_job(conn: sqlite3.Connection, job: JobListing) -> bool:
    """Insert a new job, or just refresh last_seen if we've already cached it.
    Returns True only when a brand-new row was inserted."""
    today = date.today().isoformat()
    existing = conn.execute(
        "SELECT job_id FROM jobs WHERE job_id = ?", (job.job_id,)
    ).fetchone()

    if existing:
        conn.execute("UPDATE jobs SET last_seen = ? WHERE job_id = ?", (today, job.job_id))
        return False

    conn.execute("""
        INSERT INTO jobs (job_id, title, employer, location, salary, url,
                           matched_keyword, date_posted, date_found, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job.job_id, job.title, job.employer, job.location, job.salary, job.url,
        job.matched_keyword, job.date_posted, job.date_found, today,
    ))
    return True


def log_scrape(conn: sqlite3.Connection, keyword: str, jobs_found: int, status: str, detail: str = "") -> None:
    now = datetime.now()
    conn.execute("""
        INSERT INTO scrape_log (keyword, run_date, run_timestamp, jobs_found, status, detail)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (keyword, now.date().isoformat(), now.isoformat(timespec="seconds"), jobs_found, status, detail))


def get_jobs_for_date(target_date: Optional[str] = None) -> list[dict]:
    """Return cached listings first *found* on a given ISO date (defaults to today)."""
    target_date = target_date or date.today().isoformat()
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE date_found = ? ORDER BY date_posted DESC",
            (target_date,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_all_cached_jobs() -> list[dict]:
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY date_found DESC, date_posted DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get_available_dates() -> list[str]:
    """All distinct dates for which we have cached results, newest first."""
    with closing(get_connection()) as conn:
        rows = conn.execute(
            "SELECT DISTINCT date_found FROM jobs ORDER BY date_found DESC"
        ).fetchall()
        return [r["date_found"] for r in rows]


# --------------------------------------------------------------------------
# Filtering
# --------------------------------------------------------------------------

def passes_inclusion_filter(title: str) -> bool:
    """Keep only titles matching one of the entry-level target terms."""
    title_l = title.lower()
    return any(keyword in title_l for keyword in INCLUDE_KEYWORDS)


def passes_exclusion_filter(title: str, summary: str) -> bool:
    """Returns True if the listing should be KEPT, i.e. neither the title
    nor the summary contains any disqualifying phrase."""
    haystack = f"{title} {summary}".lower()
    return not any(phrase in haystack for phrase in EXCLUDE_PHRASES)


# --------------------------------------------------------------------------
# Networking
# --------------------------------------------------------------------------

def _build_feed_url(keyword: str, rows: int) -> str:
    params = {"dkw": keyword, "sort": "D", "rows": rows}
    return f"{FEED_ENDPOINT}?{urlencode(params)}"


def fetch_feed(keyword: str, rows: int = ROWS_PER_KEYWORD) -> Optional[bytes]:
    """Fetch the raw Atom feed for one search keyword.
    Retries on timeouts/connection errors with exponential backoff.
    Returns None (never raises) if every attempt fails, so the pipeline
    can skip that keyword and keep going."""
    url = _build_feed_url(keyword, rows)
    last_error: Optional[Exception] = None
    response = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.content

        except requests.exceptions.Timeout as exc:
            last_error = exc
            logger.warning("Timeout fetching '%s' (attempt %d/%d)", keyword, attempt, MAX_RETRIES)

        except requests.exceptions.ConnectionError as exc:
            last_error = exc
            logger.warning("Connection error fetching '%s' (attempt %d/%d)", keyword, attempt, MAX_RETRIES)

        except requests.exceptions.HTTPError as exc:
            last_error = exc
            status = response.status_code if response is not None else "?"
            logger.warning("HTTP %s error fetching '%s': %s", status, keyword, exc)
            if response is not None and response.status_code in (400, 404):
                break  # client-side error, retrying identically won't help

        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.warning("Request error fetching '%s': %s", keyword, exc)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_BASE * attempt)

    logger.error("Giving up on keyword '%s' after %d attempt(s): %s", keyword, MAX_RETRIES, last_error)
    return None


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------

# Job Bank feed summaries look like:
# "Job number: 3600743 Location: Shaunavon (SK) Employer: Cypress Hills
#  Ability Centres, Inc. Salary: $20.00 to $21.50 hourly (to be negotiated)"
SUMMARY_PATTERN = re.compile(
    r"Job number:\s*(?P<job_number>[\w\-]+)?\s*"
    r"Location:\s*(?P<location>.*?)\s*"
    r"Employer:\s*(?P<employer>.*?)\s*"
    r"(?:Salary:\s*(?P<salary>.*))?$",
    re.IGNORECASE | re.DOTALL,
)


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _extract_job_id(link: str, fallback_seed: str) -> str:
    match = re.search(r"[?&]id=(\d+)", link or "")
    if match:
        return match.group(1)
    # No numeric id found in the link (feed format changed?) - fall back to
    # a stable hash of title+link+employer so dedup/caching still works.
    return hashlib.sha1(fallback_seed.encode("utf-8")).hexdigest()[:16]


def parse_feed(xml_content: bytes, keyword: str) -> tuple[list[JobListing], int]:
    """Parse a raw Atom feed response into filtered JobListing objects.

    Filtering happens here (not later) because the exclusion check needs
    the raw summary text, which we don't keep around after parsing.

    Returns (kept_listings, raw_entry_count).
    """
    soup = BeautifulSoup(xml_content, "xml")
    entries = soup.find_all("entry") or soup.find_all("item")  # Atom, RSS 2.0 fallback

    today = date.today().isoformat()
    listings: list[JobListing] = []
    raw_count = 0

    for entry in entries:
        raw_count += 1

        title_tag = entry.find("title")
        link_tag = entry.find("link")
        summary_tag = entry.find("summary") or entry.find("description")
        updated_tag = entry.find("updated") or entry.find("pubDate") or entry.find("published")

        title = _clean(title_tag.get_text() if title_tag else "")
        if not title:
            continue

        # Atom uses <link href="..."/>, RSS 2.0 uses <link>text</link>
        link = ""
        if link_tag is not None:
            link = link_tag.get("href") or _clean(link_tag.get_text())

        summary_raw = _clean(summary_tag.get_text()) if summary_tag else ""
        date_posted = _clean(updated_tag.get_text()) if updated_tag else None

        # --- Strict filters applied to title + full summary text ---
        if not passes_inclusion_filter(title):
            continue
        if not passes_exclusion_filter(title, summary_raw):
            continue

        match = SUMMARY_PATTERN.search(summary_raw)
        location = _clean(match.group("location")) if match else ""
        employer = _clean(match.group("employer")) if match else ""
        salary = _clean(match.group("salary")) if match and match.group("salary") else ""

        job_id = _extract_job_id(link, fallback_seed=f"{title}|{link}|{employer}")

        listings.append(JobListing(
            job_id=job_id,
            title=title,
            employer=employer,
            location=location,
            salary=salary,
            url=link,
            matched_keyword=keyword,
            date_posted=date_posted,
            date_found=today,
        ))

    return listings, raw_count


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_pipeline(rows_per_keyword: int = ROWS_PER_KEYWORD) -> dict:
    """
    Fetch every configured keyword from the live Job Bank feed, apply the
    inclusion/exclusion filters, cache new results in SQLite, and return a
    run summary dict. Safe to call repeatedly (e.g. on every Streamlit
    refresh) - already-cached jobs are not re-inserted, only their
    'last_seen' date is bumped.
    """
    init_db()
    summary = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "keywords_processed": 0,
        "keywords_failed": [],
        "raw_results": 0,
        "kept_after_filters": 0,
        "new_jobs_saved": 0,
    }

    with closing(get_connection()) as conn:
        for keyword in SEARCH_KEYWORDS:
            raw = fetch_feed(keyword, rows=rows_per_keyword)

            if raw is None:
                summary["keywords_failed"].append(keyword)
                with conn:
                    log_scrape(conn, keyword, 0, "error", "fetch failed after retries")
                time.sleep(DELAY_BETWEEN_KEYWORDS)
                continue

            try:
                kept, raw_count = parse_feed(raw, keyword)
            except Exception as exc:  # a malformed feed shouldn't crash the whole run
                logger.exception("Failed to parse feed for '%s'", keyword)
                summary["keywords_failed"].append(keyword)
                with conn:
                    log_scrape(conn, keyword, 0, "error", f"parse failed: {exc}")
                time.sleep(DELAY_BETWEEN_KEYWORDS)
                continue

            summary["raw_results"] += raw_count
            summary["kept_after_filters"] += len(kept)

            new_count = 0
            with conn:
                for job in kept:
                    if upsert_job(conn, job):
                        new_count += 1
                log_scrape(conn, keyword, len(kept), "success")

            summary["new_jobs_saved"] += new_count
            summary["keywords_processed"] += 1

            logger.info(
                "'%s': %d raw entries -> %d passed filters -> %d newly cached",
                keyword, raw_count, len(kept), new_count,
            )

            time.sleep(DELAY_BETWEEN_KEYWORDS)

    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    return summary


if __name__ == "__main__":
    result = run_pipeline()
    print("\n--- Job Bank pipeline run summary ---")
    for key, value in result.items():
        print(f"{key}: {value}")

    todays_jobs = get_jobs_for_date()
    print(f"\n{len(todays_jobs)} matching job(s) cached for today ({date.today().isoformat()}).")
