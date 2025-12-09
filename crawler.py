import re
import time
import logging
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from urllib import robotparser
from collections import deque

logger = logging.getLogger("site_crawler")
logging.basicConfig(level=logging.INFO)

DEFAULT_HEADERS = {
    "User-Agent": "site-crawler-bot/1.0 (+https://example.com)"
}

class Crawler:
    def __init__(self, start_url, max_pages=2000, delay=0.5, same_domain=True, headers=None, timeout=10):
        self.start_url = start_url.rstrip("/")
        self.max_pages = max_pages
        self.delay = delay
        self.same_domain = same_domain
        self.headers = headers or DEFAULT_HEADERS
        self.timeout = timeout

        parsed = urlparse(self.start_url)
        self.root_netloc = parsed.netloc
        self.scheme = parsed.scheme

        self.visited = set()
        self.results = []  # list of dicts: url, title, meta, text, images (list of (src, alt))
        self.rp = robotparser.RobotFileParser()
        self._init_robots()

    def _init_robots(self):
        robots_url = urljoin(f"{self.scheme}://{self.root_netloc}", "/robots.txt")
        try:
            self.rp.set_url(robots_url)
            self.rp.read()
        except Exception:
            # ignore robots failures and assume allowed
            logger.info("Could not read robots.txt, proceeding without it.")

    def _allowed(self, url):
        try:
            return self.rp.can_fetch(self.headers.get("User-Agent", "*"), url)
        except Exception:
            return True

    def _same_domain(self, url):
        if not self.same_domain:
            return True
        parsed = urlparse(url)
        return parsed.netloc == self.root_netloc

    def _normalize(self, link, base):
        if not link:
            return None
        link = link.strip()
        if link.startswith("javascript:") or link.startswith("mailto:"):
            return None
        return urljoin(base, link.split("#")[0])

    def _extract(self, html, url):
        soup = BeautifulSoup(html, "lxml")

        title_tag = soup.title.string.strip() if soup.title and soup.title.string else ""
        meta_desc = ""
        desc_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
        if desc_tag and desc_tag.get("content"):
            meta_desc = desc_tag["content"].strip()

        # visible text: naive extraction
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        text = soup.get_text(separator=" ", strip=True)

        images = []
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            alt = img.get("alt") or ""
            src = urljoin(url, src)
            images.append({"src": src, "alt": alt})

        links = []
        for a in soup.find_all("a", href=True):
            href = self._normalize(a["href"], url)
            if href:
                links.append(href)

        return {
            "url": url,
            "title": title_tag,
            "meta": meta_desc,
            "text": text,
            "images": images,
            "links": links,
        }

    def crawl(self, progress_callback=None):
        """
        Breadth-first crawl from start_url until max_pages or queue exhausted.
        progress_callback(current_count, max_pages, last_url) can be provided.
        """
        q = deque([self.start_url])
        self.visited = set()
        self.results = []

        while q and len(self.results) < self.max_pages:
            url = q.popleft()
            if url in self.visited:
                continue
            if not url.startswith("http"):
                continue
            if not self._same_domain(url):
                continue
            if not self._allowed(url):
                logger.debug(f"Blocked by robots: {url}")
                self.visited.add(url)
                continue

            try:
                resp = requests.get(url, headers=self.headers, timeout=self.timeout)
                content_type = resp.headers.get("Content-Type", "")
                if resp.status_code != 200 or "html" not in content_type:
                    logger.debug(f"Skipping non-HTML or non-200: {url} ({resp.status_code})")
                    self.visited.add(url)
                    if progress_callback:
                        progress_callback(len(self.results), self.max_pages, url)
                    continue
                page = self._extract(resp.text, url)
                self.results.append(page)
                self.visited.add(url)

                # enqueue discovered links
                for link in page["links"]:
                    if link not in self.visited and link not in q:
                        if self._same_domain(link):
                            q.append(link)

                if progress_callback:
                    progress_callback(len(self.results), self.max_pages, url)

                time.sleep(self.delay)
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
                self.visited.add(url)
                if progress_callback:
                    progress_callback(len(self.results), self.max_pages, url)
                time.sleep(self.delay)
        return self.results
