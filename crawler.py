import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import time

from utils import normalize_url, is_same_domain, extract_text, hash_text

DEFAULT_HEADERS = {
    "User-Agent": "SeoCrawler/1.0 (+https://github.com/yourname)"
}

def crawl_site(start_url, max_workers=10, max_pages=1000, timeout=10):
    """
    Crawl pages within the same domain starting from start_url. Returns a dict:
    {
      "pages": {url: page_data, ...},
      "links": {from_url: [to_url,...], ...},
      "start_url": start_url,
      "timestamp": iso8601...
    }

    This implementation uses a ThreadPoolExecutor and submits new tasks as pages complete,
    so it will discover and crawl same-domain links until max_pages is reached.
    """
    start_url = normalize_url(start_url, start_url)  # ensure normalized
    parsed = urlparse(start_url)
    base_netloc = parsed.netloc

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    pages = {}   # url -> page data
    links = {}   # url -> [outs]
    seen = set() # URLs already submitted/fetched
    start_time = time.time()

    def fetch(url):
        try:
            t0 = time.time()
            r = session.get(url, timeout=timeout, allow_redirects=True)
            rt = time.time() - t0
            status = r.status_code
            content_type = r.headers.get("Content-Type", "")
            text = r.text if "html" in content_type else ""
            soup = BeautifulSoup(text, "lxml") if text else BeautifulSoup("", "lxml")
            title_tag = soup.title.string.strip() if soup.title and soup.title.string else ""
            meta_desc = ""
            canonical = ""
            # meta description & canonical
            for m in soup.find_all("meta"):
                nm = (m.get("name") or "").lower()
                prop = (m.get("property") or "").lower()
                if nm == "description" or prop == "og:description":
                    meta_desc = meta_desc or m.get("content", "") or ""
            link_tag = soup.find("link", rel="canonical")
            if link_tag and link_tag.get("href"):
                canonical = link_tag.get("href")
            # headings
            h_tags = {}
            for i in range(1,7):
                tag = f"h{i}"
                h_tags[tag] = [t.get_text(strip=True) for t in soup.find_all(tag)]
            # images
            images = []
            for img in soup.find_all("img"):
                images.append({"src": normalize_url(url, img.get("src")), "alt": (img.get("alt") or "").strip()})
            # out links (normalize and remove fragments)
            out_links = []
            for a in soup.find_all("a", href=True):
                out = normalize_url(url, a.get("href"))
                if out:
                    out_links.append(out)
            content_text = extract_text(soup)
            content_hash = hash_text(content_text) if content_text else ""
            return {
                "url": url,
                "status_code": status,
                "headers": dict(r.headers),
                "title": title_tag,
                "meta_description": meta_desc,
                "canonical": canonical,
                "h_tags": h_tags,
                "images": images,
                "out_links": out_links,
                "content_text": content_text,
                "content_hash": content_hash,
                "response_time": rt,
            }
        except Exception as e:
            return {
                "url": url,
                "status_code": None,
                "error": str(e),
                "headers": {},
                "title": "",
                "meta_description": "",
                "canonical": "",
                "h_tags": {},
                "images": [],
                "out_links": [],
                "content_text": "",
                "content_hash": "",
                "response_time": 0.0
            }

    # Start crawling: submit the start_url first
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}  # future -> url
        # normalize and submit start_url
        seen.add(start_url)
        futures[executor.submit(fetch, start_url)] = start_url

        # Process futures as they complete; when we see new same-domain links, submit them
        while futures and len(pages) < max_pages:
            # as_completed yields futures as they finish
            for f in as_completed(list(futures.keys())):
                origin = futures.pop(f)
                page = f.result()
                pages[origin] = page
                links[origin] = page.get("out_links", [])

                # Submit discovered same-domain links
                for out in page.get("out_links", []):
                    if not out:
                        continue
                    # only follow same domain
                    if not is_same_domain(base_netloc, out):
                        continue
                    norm = normalize_url(out, out)
                    if not norm:
                        continue
                    if norm in seen:
                        continue
                    # if we're at capacity, stop adding more
                    if len(seen) >= max_pages:
                        break
                    seen.add(norm)
                    # submit new fetch task
                    futures[executor.submit(fetch, norm)] = norm

                # Stop early if we've reached the page limit
                if len(pages) >= max_pages:
                    break

        # In case any remaining futures completed after loop, ensure we store them (but do not add new links)
        for f in as_completed(list(futures.keys())):
            origin = futures.pop(f)
            if origin in pages:
                continue
            page = f.result()
            pages[origin] = page
            links[origin] = page.get("out_links", [])

    result = {
        "pages": pages,
        "links": links,
        "start_url": start_url,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time)),
    }
    return result
