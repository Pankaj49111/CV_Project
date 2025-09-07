# indeed_crawler.py
import re
import sqlite3
import datetime
import time
import math
import argparse
from urllib.parse import urlencode, urlparse, parse_qs, quote

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

DB_PATH = "career_assistant.db"

def clear_jobs_table():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS indeed_jobs""")  # or DROP TABLE if you want to recreate later
    conn.commit()
    conn.close()

# -----------------------------
# DB helpers
# -----------------------------
def ensure_jobs_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS indeed_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            company TEXT,
            url TEXT,
            date_posted TEXT,
            city TEXT,
            salary_text TEXT,
            salary_min_inr INTEGER,
            is_senior INTEGER
        )
    """)
    # Ensure columns exist (for older DBs)
    c.execute("PRAGMA table_info(indeed_jobs)")
    cols = {row[1] for row in c.fetchall()}
    cols_needed = {
        "salary_text": "TEXT",
        "salary_min_inr": "INTEGER",
        "city": "TEXT",
        "is_senior": "INTEGER"
    }
    for col, typ in cols_needed.items():
        if col not in cols:
            try:
                c.execute(f"ALTER TABLE indeed_jobs ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    conn.commit()
    conn.close()

def save_jobs_to_db(jobs):
    ensure_jobs_table()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for job in jobs:
        c.execute("""
            INSERT INTO indeed_jobs (title, company, url, date_posted, city, salary_text, salary_min_inr, is_senior)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job.get("title", "N/A"),
            job.get("company", "N/A"),
            job.get("url", ""),
            job.get("date_posted", str(datetime.date.today())),
            job.get("city", "India"),
            job.get("salary_text", None),
            job.get("salary_min_inr", None),
            job.get("is_senior", 0),
        ))
    conn.commit()
    conn.close()

# -----------------------------
# Salary parsing
# -----------------------------
NUM_RE = r"(?:\d{1,3}(?:,\d{2}){1,}|(?:\d{1,3}(?:,\d{3})+)|\d+(?:\.\d+)?)"
RANGE_RE = re.compile(rf"(₹?\s*{NUM_RE})\s*[-–—]\s*(₹?\s*{NUM_RE})", re.I)
VALUE_RE = re.compile(rf"(₹?\s*{NUM_RE})", re.I)

def _to_float_rupees(num_str: str) -> float:
    """
    Convert strings like '₹32,20,000' or '3.5' (when paired with LPA) to rupees (float).
    """
    s = num_str.strip().replace("₹", "").replace(" ", "")
    # Handle Indian grouping (lakhs format) and western commas
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0

def parse_salary_to_annual_min_inr(text: str) -> int | None:
    """
    Parse salary text like:
      - "₹12,00,000 - ₹18,00,000 a year"
      - "₹90,000 - ₹1,10,000 a month"
      - "₹2,000 a day"
      - "₹1,000 an hour"
      - "32 LPA"
      - "12-20 LPA"
    Convert to ANNUAL minimum INR.
    Assumptions: 22 workdays/month, 8 hours/day.
    """
    if not text:
        return None

    t = text.lower()

    # LPA / lakh / crore explicit forms
    # e.g., "32 LPA", "12-20 lpa", "15 lakh", "0.5 crore"
    if "lpa" in t or "lakh" in t or "lac" in t or "crore" in t:
        def lakh_to_inr(x): return int(round(x * 100_000))
        def crore_to_inr(x): return int(round(x * 10_000_000))

        # range like "12-20 LPA"
        m_range = re.search(rf"({NUM_RE})\s*[-–—]\s*({NUM_RE})\s*(lpa|lakh|lac|crore)", t, re.I)
        if m_range:
            a, b, unit = m_range.groups()
            a, b = _to_float_rupees(a), _to_float_rupees(b)
            if unit.lower() == "crore":
                return min(crore_to_inr(a), crore_to_inr(b))
            elif unit.lower() in ("lpa", "lakh", "lac"):
                return min(lakh_to_inr(a), lakh_to_inr(b))

        # single like "32 LPA" / "15 lakh" / "0.5 crore"
        m_single = re.search(rf"({NUM_RE})\s*(lpa|lakh|lac|crore)", t, re.I)
        if m_single:
            v, unit = m_single.groups()
            v = _to_float_rupees(v)
            if unit.lower() == "crore":
                return crore_to_inr(v)
            elif unit.lower() in ("lpa", "lakh", "lac"):
                return lakh_to_inr(v)

    # Period detection
    per_year = "year" in t or "yr" in t or "annum" in t
    per_month = "month" in t
    per_day = "day" in t
    per_hour = "hour" in t or "hr" in t

    # Range with currency, e.g., ₹X - ₹Y a month/year
    m = RANGE_RE.search(text)
    if m:
        a, b = m.groups()
        low = _to_float_rupees(a)
        # Convert to annual
        if per_year:
            annual_min = low
        elif per_month:
            annual_min = low * 12
        elif per_day:
            annual_min = low * 22 * 12
        elif per_hour:
            annual_min = low * 8 * 22 * 12
        else:
            # default assume annual if no period
            annual_min = low
        return int(round(annual_min))

    # Single value with currency
    m2 = VALUE_RE.search(text)
    if m2:
        v = _to_float_rupees(m2.group(1))
        if v > 0:
            if per_year:
                annual_min = v
            elif per_month:
                annual_min = v * 12
            elif per_day:
                annual_min = v * 22 * 12
            elif per_hour:
                annual_min = v * 8 * 22 * 12
            else:
                annual_min = v  # assume annual
            return int(round(annual_min))

    return None

def parse_salary_threshold(s: str | int | None) -> int | None:
    """
    Accepts:
      - int rupees: 3200000
      - string rupees: "₹32,20,000"
      - LPA string: "32 LPA" / "32.2 LPA"
    Returns annual INR integer (min) or None.
    """
    if s is None:
        return None
    if isinstance(s, int):
        return s
    s_str = str(s)
    # Try LPA/crore/lakh forms first
    v = parse_salary_to_annual_min_inr(s_str)
    if v:
        return v
    # Fallback: strip non-digits and parse as rupees
    digits = re.sub(r"[^\d.]", "", s_str)
    if digits:
        try:
            return int(round(float(digits)))
        except ValueError:
            return None
    return None

# -----------------------------
# Helpers
# -----------------------------
def extract_jobkey_from_href(href: str | None) -> str | None:
    if not href:
        return None
    # Most Indeed links contain ?jk=xxxxxxxx
    try:
        q = urlparse(href)
        jk = parse_qs(q.query).get("jk")
        if jk:
            return jk[0]
    except Exception:
        pass
    # Fallback: regex
    m = re.search(r"[?&]jk=([0-9a-f]+)", href, re.I)
    return m.group(1) if m else None

def extract_experience(text: str) -> int:
    matches = re.findall(r'(\d+)[+]*\s*(?:years|yrs)', (text or "").lower())
    return max(map(int, matches)) if matches else 0

def looks_like_captcha(page) -> bool:
    content = page.content()
    needles = [
        "verify you are human",
        "unusual traffic",
        "hcaptcha",
        "recaptcha",
        "/captcha/",
        "detected unusual"
    ]
    return any(n in content.lower() for n in needles)

# -----------------------------
# Core: Playwright Indeed crawler with pagination + pay parsing
# -----------------------------
def crawl_indeed(query="Senior Software Engineer",
                 location="India",
                 limit=50,
                 salary_min=None):
    """
    Scrape Indeed India results for `query` across India with pagination until `limit`.
    If salary_min is provided (e.g., "₹32,20,000" or "32 LPA"), only keep jobs whose
    parsed annual_min >= salary_min.
    """
    threshold = parse_salary_threshold(salary_min)
    print(f"🔄 Starting Indeed crawl | query='{query}' | location='{location}' | limit={limit} | salary_min={salary_min if salary_min else 'None'}")

    collected = []
    seen_jks = set()
    results_per_page = 15  # Indeed typically shows ~15 per page
    max_pages = math.ceil(limit / results_per_page) + 5  # some slack

    with sync_playwright() as p:
        # Headful to reduce bot detection
        browser = p.chromium.launch(headless=False, slow_mo=200)
        context = browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            java_script_enabled=True,
        )
        page = context.new_page()
        # Simple stealth tweak
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        # salary_encoded = quote(salary_min, safe="₹")
        # salary_encoded = salary_min.replace(",", "%2C")

        base = "https://in.indeed.com/jobs"
        params = {"q": query, "l": location, "salaryType":salary_min, "start": 0}

        for page_idx in range(max_pages):
            params["start"] = page_idx * results_per_page
            encoded_query = urlencode(params, quote_via=lambda s, safe, encoding, errors: quote(s, safe="₹"))
            url = f"{base}?{encoded_query}"
            print(f"\n🔎 Visiting: {url}")
            page.goto(url, timeout=60000)

            # Basic wait + scroll to trigger lazy content
            page.wait_for_timeout(3000)
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)

            if looks_like_captcha(page):
                print("🛑 Captcha/human check detected. Please solve it in the opened window.")
                try:
                    input("↪️  Press ENTER here after you solve it to continue...")
                except KeyboardInterrupt:
                    pass
                # wait and re-check
                page.wait_for_timeout(4000)
                if looks_like_captcha(page):
                    print("❌ Still blocked by captcha. Stopping this run.")
                    break

            try:
                # Newer DOMs may use <li data-testid="result">; fallback to .job_seen_beacon
                page.wait_for_selector("li[data-testid='result'], div.job_seen_beacon", timeout=20000)
            except PWTimeout:
                print("⚠️ No job cards detected on this page.")
                continue

            cards = page.query_selector_all("li[data-testid='result']")
            if not cards:
                cards = page.query_selector_all("div.job_seen_beacon")

            print(f"ℹ️ Found {len(cards)} cards on page {page_idx + 1}")

            for card in cards:
                if len(collected) >= limit:
                    break

                # Title
                title_el = card.query_selector("h2.jobTitle span") or card.query_selector("[data-testid='jobTitle']")
                title = title_el.inner_text().strip() if title_el else "N/A"

                # Company
                company_el = (
                        card.query_selector("span.companyName")
                        or card.query_selector("span[data-testid='company-name']")
                        or card.query_selector("[data-testid='company-name']")
                )
                company = company_el.inner_text().strip() if company_el else "N/A"

                # Link
                link_el = (
                        card.query_selector("a.jcs-JobTitle")
                        or card.query_selector("a[data-jk]")
                        or card.query_selector("a")
                )
                href = link_el.get_attribute("href") if link_el else None
                url = ("https://in.indeed.com" + href) if (href and href.startswith("/")) else (href or "")

                jk = extract_jobkey_from_href(href) if href else None
                if jk and jk in seen_jks:
                    continue
                if jk:
                    seen_jks.add(jk)

                # Location
                loc_el = card.query_selector("div.companyLocation") or card.query_selector("[data-testid='text-location']")
                city = loc_el.inner_text().strip() if loc_el else location

                # Posted date (relative)
                date_el = card.query_selector("span.date") or card.query_selector("[data-testid='myJobsStateDate']")
                date_posted = date_el.inner_text().strip() if date_el else str(datetime.date.today())

                # Salary text
                sal_el = (
                        card.query_selector("div.metadata div.salary-snippet-container")
                        or card.query_selector("div.salary-snippet-container")
                        or card.query_selector("div.salary-snippet")
                        or card.query_selector("span.salary-snippet-container")
                        or card.query_selector("[data-testid='attribute_snippet_testid']")
                )
                salary_text = sal_el.inner_text().strip() if sal_el else None
                salary_min_inr = parse_salary_to_annual_min_inr(salary_text) if salary_text else None

                # Experience & senior flag (from card snippet if present)
                snippet_el = card.query_selector("div.job-snippet")
                snippet_text = snippet_el.inner_text().strip() if snippet_el else ""
                years = extract_experience(snippet_text)
                is_senior = 1 if (years >= 6 or "senior" in title.lower() or "lead" in title.lower()) else 0

                # Salary filter (if threshold set)
                # if threshold is not None:
                #     if salary_min_inr is None or salary_min_inr < threshold:
                #         # Skip entries without salary or below threshold
                #         continue

                collected.append({
                    "title": title,
                    "company": company,
                    "url": url,
                    "date_posted": date_posted,
                    "city": city if city else "India",
                    "salary_text": salary_text,
                    "salary_min_inr": salary_min_inr,
                    "is_senior": is_senior
                })

            # polite pacing
            page.wait_for_timeout(2500)

            if len(collected) >= limit:
                break

        # Cleanup
        page.close()
        context.close()
        browser.close()

    save_jobs_to_db(collected)
    print(f"\n✅ Saved {len(collected)} jobs to SQLite (filtered by salary_min={threshold}).")
    return collected


def main(arg_list=None):
    clear_jobs_table()

    parser = argparse.ArgumentParser(description="Indeed India crawler with pagination and salary filter.")
    parser.add_argument("--query", default="Java Developer", help="Search keywords")
    parser.add_argument("--location", default="India", help="Location (keep 'India' for all-India)")
    parser.add_argument("--limit", type=int, default=50, help="Target number of jobs to fetch")
    parser.add_argument("--salary-min", default='₹32,50,000',
                        help="Minimum starting salary (e.g., 3200000, '₹32,50,000', '32 LPA')")

    if arg_list is not None:
        args = parser.parse_args(arg_list)
    else:
        args = parser.parse_args()

    results = crawl_indeed(
        query=args.query,
        location=args.location,
        limit=args.limit,
        salary_min=args.salary_min
    )

    return results


if __name__ == "__main__":
    main()
