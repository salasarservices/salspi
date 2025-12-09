import re
import time
import logging
import os
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from urllib import robotparser
from collections import deque

# NLTK VADER fallback
try:
    from nltk.sentiment import SentimentIntensityAnalyzer
    import nltk
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except Exception:
        try:
            nltk.download("vader_lexicon")
        except Exception:
            pass
    _SIA = SentimentIntensityAnalyzer()
except Exception:
    _SIA = None

# Google Cloud client (optional)
try:
    from google.cloud import language_v1
    _HAS_GOOGLE = True
except Exception:
    _HAS_GOOGLE = False

logger = logging.getLogger("site_crawler")
logging.basicConfig(level=logging.INFO)

DEFAULT_HEADERS = {
    "User-Agent": "site-crawler-bot/1.0 (+https://example.com)"
}

class Crawler:
    """
    Crawler with optional sentiment backend. To persist pages during crawl, pass an on_page callback:
      def on_page(page): ...
    The on_page callback is called immediately after a page is successfully extracted.
    """
    def __init__(self, start_url, max_pages=2000, delay=0.5, same_domain=True, headers=None, timeout=10,
                 sentiment_backend="nltk", google_credentials_env_var=None):
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
        self.results = []  # list of dicts per page
        self.rp = robotparser.RobotFileParser()
        self._init_robots()

        # sentiment backend: "nltk" or "google"
        self.sentiment_backend = sentiment_backend.lower() if sentiment_backend else "nltk"

        # optionally set credentials env var for google client (if provided)
        if google_credentials_env_var:
            # caller should set os.environ[google_credentials_env_var] to a path or JSON string prior to initialization.
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", google_credentials_env_var)

        # lazy init google client only if requested
        self._google_client = None

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

    def _ensure_google_client(self):
        if not _HAS_GOOGLE:
            raise RuntimeError("google-cloud-language library not available (not installed).")
        if not self._google_client:
            self._google_client = language_v1.LanguageServiceClient()
        return self._google_client

    def _analyze_sentiment_nltk(self, text: str):
        if not text or _SIA is None:
            return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0, "backend": "nltk"}
        try:
            scores = _SIA.polarity_scores(text)
            scores["backend"] = "nltk"
            return scores
        except Exception:
            return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0, "backend": "nltk"}

    def _analyze_sentiment_google(self, text: str):
        """
        Returns: {"score": float, "magnitude": float, "backend": "google"}.
        Also include "compound" key mapped to score for compatibility.
        """
        if not text:
            return {"score": 0.0, "magnitude": 0.0, "compound": 0.0, "backend": "google"}
        try:
            client = self._ensure_google_client()
            document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
            response = client.analyze_sentiment(request={'document': document, 'encoding_type': language_v1.EncodingType.UTF8})
            # overall document sentiment
            score = response.document_sentiment.score
            magnitude = response.document_sentiment.magnitude
            return {"score": float(score), "magnitude": float(magnitude), "compound": float(score), "backend": "google"}
        except Exception as e:
            logger.exception("Google NLP sentiment failed: %s", e)
            return {"score": 0.0, "magnitude": 0.0, "compound": 0.0, "backend": "google", "error": str(e)}

    def _analyze_sentiment(self, text: str):
        if self.sentiment_backend == "google":
            return self._analyze_sentiment_google(text)
        else:
            return self._analyze_sentiment_nltk(text)

    def _extract(self, html, url):
        soup = BeautifulSoup(html, "lxml")

        title_tag = soup.title.string.strip() if soup.title and soup.title.string else ""
        meta_desc = ""
        desc_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
        if desc_tag and desc_tag.get("content"):
            meta_desc = desc_tag["content"].strip()

        # remove script/style
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        text = soup.get_text(separator=" ", strip=True)

        images = []
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            alt = img.get("alt") or ""
            src = urljoin(url, src)
            images.append({"src": src, "alt": alt})

        # headings
        h_counts = {}
        headings_text_parts = []
        for i in range(1, 7):
            tag = f"h{i}"
            found = soup.find_all(tag)
            h_counts[tag] = len(found)
            for fh in found:
                headings_text_parts.append(fh.get_text(" ", strip=True))

        headings_text = " ".join([p for p in headings_text_parts if p])

        links = []
        for a in soup.find_all("a", href=True):
            href = self._normalize(a["href"], url)
            if href:
                links.append(href)

        title_len = len(title_tag)
        meta_len = len(meta_desc)
        content_len = len(text)

        sentiment_source = " ".join([title_tag, meta_desc, headings_text, text])
        sentiment = self._analyze_sentiment(sentiment_source)

        return {
            "url": url,
            "title": title_tag,
            "title_len": title_len,
            "meta": meta_desc,
            "meta_len": meta_len,
            "text": text,
            "content_len": content_len,
            "images": images,
            "image_alts": [img.get("alt", "") for img in images],
            "h_counts": h_counts,
            "headings": headings_text,
            "links": links,
            "sentiment": sentiment,
        }

    def crawl(self, progress_callback=None, on_page=None):
        """
        Breadth-first crawl from start_url until max_pages or queue exhausted.
        progress_callback(current_count, max_pages, last_url) can be provided.
        on_page(page_dict) will be called for each extracted page (useful for incremental persistence).
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

                # call per-page callback for persistence
                if on_page:
                    try:
                        on_page(page)
                    except Exception:
                        logger.exception("on_page callback raised an exception")

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
