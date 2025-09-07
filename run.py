import indeed_crawler
import naukri_crawler

if __name__ == "__main__":
    print("🔄 Crawling jobs from job boards before starting UI...")
    indeed_crawler.main()
    naukri_crawler.main()
    print("✅ Job data updated in SQLite")
