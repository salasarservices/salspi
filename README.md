# salspi
A humble web crawler
# Website Crawler & Search (Streamlit)

A Streamlit app that crawls a website (limited by pages, default max 2000), extracts page content (body text, titles, meta descriptions, image alt tags), builds a simple index, and allows searching words or phrases across those fields.

Features
- Obeys robots.txt (basic handling).
- Limits crawl to the same domain (configurable).
- Extracts page title, meta description, visible text, image alt tags and image URLs.
- Simple inverted-index + substring phrase search.
- Streamlit UI:
  - Crawl settings (start URL, page limit, concurrency, delay).
  - Field selectors (search in body/title/meta/alt).
  - Keyword / phrase search, results table.
  - Per-page viewer with highlights and CSV export.

How to run
1. Clone the repo.
2. Create a virtualenv and install requirements:
   pip install -r requirements.txt
3. Run:
   streamlit run streamlit_app.py

Notes & limitations
- This is a starting implementation. For large-scale or production usage, consider:
  - Using asynchronous crawling (aiohttp) with better session handling.
  - Persistent storage (SQLite/Elastic/Lucene) for indexing.
  - More robust robots.txt, sitemap parsing, canonical URL handling and duplicate detection.
  - Respect crawling politeness and site owner wishes.

Ethics and legal
- Only crawl sites you have permission to crawl. Respect robots.txt and site usage policies. Rate-limit your requests to avoid burdening servers.

License
- MIT
