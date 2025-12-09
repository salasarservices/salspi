import threading
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from utils import normalize_url, is_same_domain, extract_text, hash_text

DEFAULT_HEADERS = {
    "User-Agent": "SeoCrawler/1.0 (+https://github.com/yourname)"
}

class CrawlManager:
    """
    Manages a crawl in a background thread. Supports start, pause, resume, stop.
    Use get_progress() to retrieve progress (pages_crawled, discovered, max_pages).
    When finished, result is available as `self.result` (same shape as crawl_site returned dict).
    """
    def __init__(self, start_url, max_workers=10, max_pages=1000, timeout=10):
        self.start_url = normalize_url(start_url, start_url)
        parsed = urlparse(self.start_url)
        self.base_netloc = parsed.netloc
        self.max_workers = max_workers
        self.max_pages = max_pages
        self.timeout = timeout

        # runtime data
        self.pages = {}   # url -> page_data
        self.links = {}   # url -> [outs]
        self.seen = set() # urls submitted
        self.discovered = 0
        self.pages_crawled = 0

        # control
        self._thread = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()  # when set => paused
        self._lock = threading.Lock()

        # result container & status
        self.result = None
        self.error = None
        self.finished = False

    def start(self):
        if self._thread and self._thread.is_alive():
            return False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def pause(self):
        self._pause_event.set()

    def resume(self):
        self._pause_event.clear()

    def stop(self):
        self._stop_event.set()
        # if paused, unpause so thread can notice stop
        self._pause_event.clear()
        if self._thread:
            self._thread.join(timeout=5)

    def is_running(self):
        return self._thread is not None and self._thread.is_alive() and not self.finished

    def is_paused(self):
        return self._pause_event.is_set()

    def is_stopped(self):
        return self._stop_event.is_set()

    def get_progress(self):
        with self._lock:
            return {
                "pages_crawled": self.pages_crawled,
                "discovered": self.discovered,
                "max_pages": self.max_pages,
                "finished": self.finished,
                "error": self.error,
            }

    def get_result(self):
        return self.result

    def _fetch(self, session, url):
        try:
            t0 = time.time()
            r = session.get(url, timeout=self.timeout, allow_redirects=True)
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
            # out links
            out_links = []
            for a in soup.find_all("a", href=True):
                out = normalize_url(url, a.get("href"))
                if out:
                    out_links.append(out)
           

