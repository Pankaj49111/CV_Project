from playwright.sync_api import sync_playwright
import sqlite3
import datetime
import re
import time
import argparse
from typing import Optional
from urllib.parse import urlencode, quote

DB_PATH = "career_assistant.db"

# -------------------------
# DB helpers
# -------------------------

def clear_jobs_table():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS naukri_jobs""")  # or DROP TABLE if you want to recreate later
    conn.commit()
    conn.close()


def ensure_jobs_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS naukri_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            company TEXT,
            url TEXT UNIQUE,
            date_posted TEXT,
            city TEXT,
            salary_text TEXT,
            salary_min_inr INTEGER,
            experience_years INTEGER,
            is_senior INTEGER,
            source TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_jobs_to_db(jobs):
    ensure_jobs_table()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for job in jobs:
        try:
            c.execute("""
                INSERT OR IGNORE INTO naukri_jobs
                (title, company, url, date_posted, city, salary_text, salary_min_inr, experience_years, is_senior, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.get("title", "Unknown"),
                job.get("company", "Unknown"),
                job.get("link", ""),
                job.get("date_posted", datetime.date.today().isoformat()),
                job.get("location", "Not specified"),
                job.get("salary_text", "Not specified"),
                job.get("salary_min_inr") or 0,
                job.get("experience_years") if job.get("experience_years") is not None else -1,
                job.get("is_senior", 0),
                "naukri"
            ))
        except Exception as e:
            print(f"⚠️ Failed saving job {job.get('title')}: {e}")
    conn.commit()
    conn.close()

# -------------------------
# Salary parsing utilities
# -------------------------
NUM_RE = r"(?:\d{1,3}(?:,\d{2}){1,}|(?:\d{1,3}(?:,\d{3})+)|\d+(?:\.\d+)?)"
RANGE_RE = re.compile(rf"(₹?\s*{NUM_RE})\s*[-–—]\s*(₹?\s*{NUM_RE})", re.I)
VALUE_RE = re.compile(rf"(₹?\s*{NUM_RE})", re.I)

def _to_float_rupees(num_str: str) -> float:
    s = num_str.strip().replace("₹", "").replace(" ", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_salary_to_annual_min_inr(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = text.lower()

    # LPA, lakh, crore
    if "lpa" in t or "lakh" in t or "lac" in t or "crore" in t:
        def lakh_to_inr(x): return int(round(x * 100_000))
        def crore_to_inr(x): return int(round(x * 10_000_000))

        m_range = re.search(rf"({NUM_RE})\s*[-–—]\s*({NUM_RE})\s*(lpa|lakh|lac|crore)", t, re.I)
        if m_range:
            a, b, unit = m_range.groups()
            a_val, b_val = _to_float_rupees(a), _to_float_rupees(b)
            if unit.lower() == "crore":
                return min(crore_to_inr(a_val), crore_to_inr(b_val))
            else:
                return min(lakh_to_inr(a_val), lakh_to_inr(b_val))

        m_single = re.search(rf"({NUM_RE})\s*(lpa|lakh|lac|crore)", t, re.I)
        if m_single:
            v, unit = m_single.groups()
            v_val = _to_float_rupees(v)
            if unit.lower() == "crore":
                return crore_to_inr(v_val)
            else:
                return lakh_to_inr(v_val)

    m = RANGE_RE.search(text)
    if m:
        a, b = m.groups()
        low = _to_float_rupees(a)
        per_year = "year" in t or "yr" in t or "annum" in t
        per_month = "month" in t
        per_day = "day" in t
        per_hour = "hour" in t or "hr" in t

        if per_year:
            return int(round(low))
        elif per_month:
            return int(round(low * 12))
        elif per_day:
            return int(round(low * 22 * 12))
        elif per_hour:
            return int(round(low * 8 * 22 * 12))
        else:
            return int(round(low))

    m2 = VALUE_RE.search(text)
    if m2:
        v = _to_float_rupees(m2.group(1))
        if v > 0:
            if "year" in t or "yr" in t or "annum" in t:
                return int(round(v))
            if "month" in t:
                return int(round(v * 12))
            if "day" in t:
                return int(round(v * 22 * 12))
            if "hour" in t or "hr" in t:
                return int(round(v * 8 * 22 * 12))
            return int(round(v))
    return None

# -------------------------
# Helpers
# -------------------------
def parse_experience_years(exp_text: Optional[str]) -> Optional[int]:
    if not exp_text:
        return None
    t = exp_text.lower()
    if "fresher" in t:
        return 0
    m = re.search(r"(\d+)\s*[-–—]?\s*(\d+)?", t)
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

# -------------------------
# Naukri crawler
# -------------------------
def crawl_naukri(keyword="java developer, spring boot",
                 location="india",
                 salary_range="25to50",
                 experience=6,
                 job_age=15,
                 limit=50,
                 headless=False):
    jobs = []
    seen_links = set()
    page_num = 1
    base_url = "https://www.naukri.com/java-developer-spring-boot-jobs-in-india"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, slow_mo=120)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )
        page = context.new_page()

        while len(jobs) < limit:
            params = {
                "k": keyword,
                "l": location,
                "experience": str(experience),
                "ctcFilter": salary_range,
                "jobAge": str(job_age),
                "page": str(page_num)
            }
            url = f"{base_url}?{urlencode(params, quote_via=quote)}"
            print(f"🔎 Crawling Naukri page {page_num}: {url}")

            try:
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                # Human-like scrolling
                for _ in range(5):
                    page.mouse.wheel(0, 500)
                    page.wait_for_timeout(1000)
            except Exception as e:
                print(f"⚠️ Failed to load page {page_num}: {e}")
                break

            # Replace wait + selector part with this
            try:
                page.wait_for_selector("article.jobTuple, div.jobTuple, div.row1", timeout=15000)
            except Exception:
                print("⚠️ Still no job cards. Dumping part of HTML for debug...")
                print(page.content()[:2000])   # print first 2000 chars of HTML
                break

            # Now fetch cards
            job_cards = page.query_selector_all("article.jobTuple") \
                        or page.query_selector_all("div.jobTuple") \
                        or page.query_selector_all("div.row1")

            print(f"🔎 Found {len(job_cards)} job cards on page {page_num}")

            for card in job_cards:
                # Title
                title_el = card.query_selector("a.title") or card.query_selector("a.jobTitle") or card.query_selector("a[href*='/job-listings']")
                title = title_el.inner_text().strip() if title_el else "Unknown"
                link = title_el.get_attribute("href") if title_el else None

                # Company
                company_el = card.query_selector("a.subTitle, div.comp-name, span.comp-name") or card.query_selector("div.companyInfo span") or card.query_selector(".comp-name")
                company = company_el.inner_text().strip() if company_el else "Unknown"

                # Location
                loc_el = card.query_selector("span.loc") or card.query_selector("span.locWdth") or card.query_selector(".loc") or card.query_selector(".locWdth") or card.query_selector(".location")
                location_text = loc_el.inner_text().strip() if loc_el else "Not specified"

                # Experience
                exp_el = card.query_selector("span.exp") or card.query_selector("span.expwdth") or card.query_selector(".exp") or card.query_selector(".expwdth") or card.query_selector(".experience")
                exp_text = exp_el.inner_text().strip() if exp_el else None
                exp_years = parse_experience_years(exp_text)

                # Salary
                sal_el = card.query_selector("span.salary") or card.query_selector("span.sal") or card.query_selector(".salaryRange") or card.query_selector(".sal")
                salary_text = sal_el.inner_text().strip() if sal_el else "Not specified"
                salary_min_inr = parse_salary_to_annual_min_inr(salary_text) or 0

                date_posted = datetime.date.today().isoformat()

                is_senior = 1 if ((exp_years is not None and exp_years >= 6) or ("senior" in title.lower())) else 0

                job = {
                    "title": title,
                    "company": company,
                    "link": link,
                    "date_posted": date_posted,
                    "location": location_text,
                    "salary_text": salary_text,
                    "salary_min_inr": salary_min_inr,
                    "experience_years": exp_years,
                    "is_senior": is_senior
                }
                jobs.append(job)

            print(f"✅ Collected {len(jobs)} jobs so far...")
            page_num += 1
            time.sleep(1.5)

        page.close()
        context.close()
        browser.close()

    save_jobs_to_db(jobs)
    print(f"\n🎯 Finished. Saved {len(jobs)} jobs to DB ({DB_PATH}).")
    return jobs


def main(arg_list=None):
    clear_jobs_table()
    parser = argparse.ArgumentParser(description="Naukri crawler (standalone).")
    parser.add_argument("--keyword", default="java developer, spring boot", help="Job keyword")
    parser.add_argument("--location", default="india", help="Location")
    parser.add_argument("--salary-range", default="25to50", help="Salary range filter")
    parser.add_argument("--experience", type=int, default=6, help="Minimum experience (years)")
    parser.add_argument("--job-age", type=int, default=15, help="Job posting age (days)")
    parser.add_argument("--limit", type=int, default=50, help="Number of jobs to fetch")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")

    if arg_list is not None:
        args = parser.parse_args(arg_list)
    else:
        args = parser.parse_args()

    results = crawl_naukri(
        keyword=args.keyword,
        location=args.location,
        salary_range=args.salary_range,
        experience=args.experience,
        job_age=args.job_age,
        limit=args.limit,
        headless=args.headless
    )

    return results

if __name__ == "__main__":
    main()
