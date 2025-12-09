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
            for i in range(1, 7):
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

    def _run(self):
        """
        Background crawl runner. Uses ThreadPoolExecutor and submits discovered same-domain links.
        Honors pause and stop events.
        """
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}  # future -> url

            # submit start url
            start = self.start_url
            if not start:
                self.error = "Invalid start URL"
                self.finished = True
                return
            with self._lock:
                self.seen.add(start)
                self.discovered = 1
            futures[executor.submit(self._fetch, session, start)] = start

            try:
                while futures and not self._stop_event.is_set():
                    # Pause handling
                    if self._pause_event.is_set():
                        # Sleep in short increments while paused, allowing stop to be noticed
                        while self._pause_event.is_set() and not self._stop_event.is_set():
                            time.sleep(0.2)
                        if self._stop_event.is_set():
                            break

                    # Collect completed futures as they finish
                    done_any = False
                    for f in as_completed(list(futures.keys()), timeout=1):
                        done_any = True
                        origin = futures.pop(f)
                        page = f.result()

                        with self._lock:
                            if origin not in self.pages:
                                self.pages[origin] = page
                                self.links[origin] = page.get("out_links", [])
                                self.pages_crawled += 1

                        # If stop requested, break out
                        if self._stop_event.is_set():
                            break

                        # submit discovered same-domain links
                        for out in page.get("out_links", []):
                            if not out:
                                continue
                            if not is_same_domain(self.base_netloc, out):
                                continue
                            norm = normalize_url(out, out)
                            if not norm:
                                continue
                            with self._lock:
                                if norm in self.seen or len(self.seen) >= self.max_pages:
                                    continue
                                self.seen.add(norm)
                                self.discovered = len(self.seen)
                            # submit
                            futures[executor.submit(self._fetch, session, norm)] = norm

                        # Stop if we've crawled enough pages
                        if self.pages_crawled >= self.max_pages:
                            self._stop_event.set()
                            break

                    # If as_completed timed out without any done futures, check loop conditions
                    if not done_any:
                        # no futures completed within timeout; if there are still futures, continue and check pause/stop
                        if self._stop_event.is_set():
                            break
                        # small sleep - allow event changes
                        time.sleep(0.1)

                    # Stop condition - no more futures (all tasks done)
                    if not futures:
                        break

                # If stop_event set, try to cancel outstanding futures (best effort)
                if self._stop_event.is_set():
                    for f in list(futures.keys()):
                        f.cancel()

                # Save result snapshot
                with self._lock:
                    self.result = {
                        "pages": dict(self.pages),
                        "links": dict(self.links),
                        "start_url": self.start_url,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())),
                    }
                    self.finished = True
            except Exception as e:
                self.error = str(e)
                self.finished = True
                # Save partial results if any
                with self._lock:
                    self.result = {
                        "pages": dict(self.pages),
                        "links": dict(self.links),
                        "start_url": self.start_url,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time())),
                        "error": self.error
                    }


def crawl_site(start_url, max_workers=10, max_pages=1000, timeout=10):
    """
    Backwards-compatible synchronous crawl function.

    Many parts of the app (or older code) import crawl_site directly. The new crawler
    primarily exposes CrawlManager for background crawling, but this helper lets you
    run a blocking/synchronous crawl that returns the final crawl snapshot (same shape
    as CrawlManager.result).

    Usage: crawl = crawl_site("https://example.com", max_workers=8, max_pages=500)
    """
    mgr = CrawlManager(start_url, max_workers=max_workers, max_pages=max_pages, timeout=timeout)
    # call the runner synchronously (blocking) so callers expecting a return value get the result
    mgr._run()
    return mgr.get_result()
