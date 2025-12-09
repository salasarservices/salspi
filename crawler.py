import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from collections import deque
import time

from utils import normalize_url, is_same_domain, extract_text, hash_text

DEFAULT_HEADERS = {
    "User-Agent": "SeoCrawler/1.0 (+https://github.com/yourname)"
}

def crawl_site(start_url, max_workers=10, max_pages=1000, timeout=10):
    """
    Crawl pages within the same domain (start_url). Return dict:
    {
      "pages": {url: page_data, ...},
      "links": {from_url: [to_url,...], ...},
      "start_url": start_url,
      "timestamp": iso8601...
    }
    page_data:
      {
        url, status_code, headers, title, meta_description, canonical,
        h_tags: {"h1": [...], "h2": [...]}, images: [{"src":..., "alt":...}], content_text, content_hash, response_time
      }
    """
    parsed = urlparse(start_url)
    base_netloc = parsed.netloc

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    seen = set()
    pages = {}
    links = {}
    q = deque([normalize_url(start_url, start_url)])
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
            for m in soup.find_all("meta"):
                nm = (m.get("name") or "").lower()
                prop = (m.get("property") or "").lower()
                if nm == "description" or prop == "og:description":
                    meta_desc = m.get("content", "") or meta_desc
                if (m.get("rel") and "canonical" in m.get("rel")) or m.get("property") == "canonical":
                    canonical = m.get("href") or canonical
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
            # out links
            out_links = []
            for a in soup.find_all("a", href=True):
                out = normalize_url(url, a.get("href"))
                if out:
                    out_links.append(out)
            content_text = extract_text(soup)
            content_hash = hash_text(content_text)
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
            return {"url": url, "status_code": None, "error": str(e), "out_links": [], "images": [], "h_tags": {}, "title": "", "meta_description": "", "canonical": "", "content_text": "", "content_hash": "", "response_time": 0.0}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        while q and len(seen) < max_pages:
            url = q.popleft()
            if url in seen:
                continue
            seen.add(url)
            futures[executor.submit(fetch, url)] = url

            # collect completed ones to push new links & maintain queue size
            done_futures = [f for f in futures if f.done()]
            for f in done_futures:
                url_origin = futures.pop(f)
                page = f.result()
                pages[url_origin] = page
                links[url_origin] = []
                for out in page.get("out_links", []):
                    links[url_origin].append(out)
                    if is_same_domain(base_netloc, out) and out not in seen and out not in q and len(seen) + len(q) < max_pages:
                        q.append(out)
        # wait for remaining futures
        for f in as_completed(list(futures.keys())):
            url_origin = futures[f]
            page = f.result()
            pages[url_origin] = page
            links[url_origin] = page.get("out_links", [])

    result = {
        "pages": pages,
        "links": links,
        "start_url": start_url,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time)),
    }
    return result
