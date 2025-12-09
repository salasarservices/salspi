# salspi
A humble web crawler
# Web Crawler + SEO Analysis (Streamlit)

This repository contains a Streamlit app that crawls a website, stores crawl results in MongoDB, performs basic SEO checks, visualizes site structure, and provides a site-wide search.

Quick features
- Crawl a whole site (pages, images, H tags, meta tags, canonical, etc.)
- Compute metrics (total pages, duplicate pages, duplicate titles/descriptions, canonical issues, image alt issues, broken links, HTTP response categories, indexable vs non-indexable)
- Clickable metric tiles to view the list of affected URLs
- Visualize structure with an interactive network graph (pyvis)
- Search site content for words/phrases
- Store and retrieve crawled data from MongoDB
- Buttons: Refresh App / Clear Cache, Delete MongoDB database
- Minimalistic UI with pastel colors

Requirements
- Python 3.10+
- MongoDB URI (set as environment variable MONGO_URI)
- (Optional) Google Search Console / URL Inspection API credentials — see README section below

Install
1. Create virtualenv and install:
   pip install -r requirements.txt

2. Set environment variables:
   export MONGO_URI="mongodb://user:pass@host:port/dbname"
   (on Windows use set)

Run
   streamlit run streamlit_app.py

Project structure
- streamlit_app.py — main Streamlit app & UI
- crawler.py — site crawler (requests + ThreadPoolExecutor)
- seo_checks.py — metric calculation helpers
- db.py — MongoDB storage helpers (pymongo)
- utils.py — utility functions
- requirements.txt — dependencies
- .gitignore

Notes & limitations
- This is a starter implementation. The crawler is polite but does not yet implement advanced rate-limiting, robots.txt crawling policies, or JS rendering (no headless browser).
- Duplicate detection currently uses content hashing (sha256) of cleaned text. For large sites or fuzzy duplicates, consider shingling or MinHash.
- URL Inspection (Google API) is included as a placeholder integration — you will need to add credentials and code per API.
- For production use, add queueing, distributed crawling, robust error handling, tests, and authentication.

What's next
- If you want, I can:
  - Add robots.txt parsing & obey crawl-delay
  - Add support for rendering JavaScript pages (Playwright/Playwright-async)
  - Add scheduled/recurring crawls and webhooks
  - Harden duplicate detection (shingles/minhash)
  - Add CI/CD and GitHub Actions
