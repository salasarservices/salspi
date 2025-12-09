import re
import time
import logging
import os
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from urllib import robotparser
from collections import deque

# Lazy imports for OCR
try:
    from PIL import Image
    from io import BytesIO
    import pytesseract
    import warnings
    _HAS_OCR = True
except Exception:
    Image = None
    BytesIO = None
    pytesseract = None
    warnings = None
    _HAS_OCR = False

# Do not perform heavy initialization at import time.
_SIA = None
try:
    import nltk
except Exception:
    nltk = None

# detect google client availability without instantiating credentials
try:
    from google.cloud import language_v1  # may not be installed
    _HAS_GOOGLE = True
except Exception:
    language_v1 = None
    _HAS_GOOGLE = False

logger = logging.getLogger("site_crawler")
logging.basicConfig(level=logging.INFO)

DEFAULT_HEADERS = {
    "User-Agent": "site-crawler-bot/1.0 (+https://example.com)"
}


class Crawler:
    """
    Crawler that supports extracting page text, meta, headings, images and performing OCR on images.
    Sentiment via 'nltk' (VADER) or 'google' (Cloud Natural Language).
    """

    def __init__(
        self,
        start_url,
        max_pages=2000,
        delay=0.5,
        same_domain=True,
        headers=None,
        timeout=10,
        sentiment_backend="nltk",
        google_credentials_env_var=None,
        ocr_enabled=True,
    ):
        self.start_url = start_url.rstrip("/")
        self.max_pages = int(max_pages)
        self.delay = float(delay)
        self.same_domain = bool(same_domain)
        self.headers = headers or DEFAULT_HEADERS
        self.timeout = int(timeout)

        parsed = urlparse(self.start_url)
        self.root_netloc = parsed.netloc
        self.scheme = parsed.scheme

        self.visited = set()
        self.results = []
        self.rp = robotparser.RobotFileParser()
        self._init_robots()

        # choose sentiment backend
        self.sentiment_backend = (sentiment_backend or "nltk").lower()

        # if caller provides a path via google_credentials_env_var, set it as GOOGLE_APPLICATION_CREDENTIALS
        if google_credentials_env_var:
            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", google_credentials_env_var)

        # lazy init
        self._sia = None
        self._google_client = None

        # OCR
        self.ocr_enabled = bool(ocr_enabled) and _HAS_OCR
        if ocr_enabled and not _HAS_OCR:
            logger.warning("OCR requested but pytesseract/Pillow not available; OCR disabled.")

    def _init_robots(self):
        try:
            robots_url = urljoin(f"{self.scheme}://{self.root_netloc}", "/robots.txt")
            self.rp.set_url(robots_url)
            self.rp.read()
        except Exception:
            logger.info("Could not read robots.txt; proceeding without it.")

    def _allowed(self, url):
        try:
            ua = self.headers.get("User-Agent", "*")
            return self.rp.can_fetch(ua, url)
        except Exception:
            return True

    def _same_domain(self, url):
        if not self.same_domain:
            return True
        try:
            parsed = urlparse(url)
            return parsed.netloc == self.root_netloc
        except Exception:
            return False

    def _normalize(self, link, base):
        if not link:
            return None
        link = link.strip()
        if link.startswith("javascript:") or link.startswith("mailto:"):
            return None
        return urljoin(base, link.split("#")[0])

    def _ensure_nltk(self):
        global _SIA
        if _SIA is not None:
            self._sia = _SIA
            return
        if nltk is None:
            self._sia = None
            return
        try:
            from nltk.sentiment import SentimentIntensityAnalyzer

            # assume data is preinstalled in environment; do NOT call nltk.download here
            self._sia = SentimentIntensityAnalyzer()
            _SIA = self._sia
        except Exception:
            self._sia = None

    def _ensure_google_client(self):
        if not _HAS_GOOGLE:
            raise RuntimeError("google-cloud-language is not installed in the environment.")
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not creds_path:
            raise RuntimeError(
                "Google NLP requested but GOOGLE_APPLICATION_CREDENTIALS is not set."
            )
        if self._google_client is None:
            try:
                from google.cloud import language_v1
                self._google_client = language_v1.LanguageServiceClient()
            except Exception as e:
                logger.exception("Failed to instantiate Google Language client: %s", e)
                raise
        return self._google_client

    def _analyze_sentiment_nltk(self, text: str):
        if self._sia is None:
            self._ensure_nltk()
        if not text or self._sia is None:
            return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0, "backend": "nltk"}
        try:
            scores = self._sia.polarity_scores(text)
            scores["backend"] = "nltk"
            return scores
        except Exception:
            return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0, "backend": "nltk"}

    def _analyze_sentiment_google(self, text: str):
        if not text:
            return {"score": 0.0, "magnitude": 0.0, "compound": 0.0, "backend": "google"}
        try:
            client = self._ensure_google_client()
            from google.cloud import language_v1
            document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
            response = client.analyze_sentiment(request={"document": document, "encoding_type": language_v1.EncodingType.UTF8})
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

    def _perform_ocr_on_image(self, img_url: str):
        """
        Download an image and run OCR returning the extracted text.
        Returns {'ocr_text': str, 'error': str or None}
        """
        if not self.ocr_enabled:
            return {"ocr_text": "", "error": "ocr-disabled"}
        try:
            # small safety: do not download extremely large images
            resp = requests.get(img_url, headers=self.headers, timeout=self.timeout, stream=True)
            content_type = resp.headers.get("Content-Type", "") or ""
            if resp.status_code != 200 or not content_type.startswith("image"):
                return {"ocr_text": "", "error": f"non-image or status {resp.status_code}"}
            # limit read to e.g. 5MB
            max_bytes = 5 * 1024 * 1024
            content = resp.raw.read(max_bytes + 1)
            if len(content) > max_bytes:
                return {"ocr_text": "", "error": "image-too-large"}

            # Use BytesIO and PIL open. Handle palette/transparency by explicit conversion.
            try:
                img = Image.open(BytesIO(content))
            except Exception as e:
                logger.debug("PIL failed to open image %s: %s", img_url, e)
                return {"ocr_text": "", "error": f"pil-open-failed: {e}"}

            # Convert palette images (mode 'P') to RGBA first to avoid PIL warning,
            # then convert to RGB for OCR (pytesseract expects RGB).
            try:
                if img.mode == "P":
                    # avoid warning: convert to RGBA then to RGB
                    try:
                        img = img.convert("RGBA")
                    except Exception:
                        # fallback: convert directly to RGB
                        img = img.convert("RGB")
                elif img.mode in ("LA", "L", "RGBA", "CMYK", "P"):
                    # normalize common modes to RGB
                    try:
                        img = img.convert("RGB")
                    except Exception:
                        pass
            except Exception:
                # if conversion fails, continue with original img
                pass

            # optionally pre-process: resize if huge
            try:
                w, h = img.size
                max_dim = 2500
                if max(w, h) > max_dim:
                    ratio = max_dim / float(max(w, h))
                    new_size = (int(w * ratio), int(h * ratio))
                    img = img.resize(new_size)
            except Exception:
                pass

            # run OCR
            try:
                text = pytesseract.image_to_string(img)
                return {"ocr_text": text.strip(), "error": None}
            except Exception as e:
                logger.debug("pytesseract failed on %s: %s", img_url, e)
                return {"ocr_text": "", "error": f"ocr-failed: {e}"}
        except Exception as e:
            logger.exception("OCR failed for %s: %s", img_url, e)
            return {"ocr_text": "", "error": str(e)}

    def _extract(self, html, url):
        soup = BeautifulSoup(html, "lxml")

        title_tag = soup.title.string.strip() if soup.title and soup.title.string else ""
        meta_desc = ""
        desc_tag = soup.find("meta", attrs={"name": re.compile("description", re.I)})
        if desc_tag and desc_tag.get("content"):
            meta_desc = desc_tag["content"].strip()

        # remove scripts/styles
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        text = soup.get_text(separator=" ", strip=True)

        images = []
        ocr_texts = []
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            alt = img.get("alt") or ""
            src = urljoin(url, src)
            img_entry = {"src": src, "alt": alt}
            # attempt OCR for each image (may be empty if disabled or failed)
            try:
                ocr_res = self._perform_ocr_on_image(src) if self.ocr_enabled else {"ocr_text": "", "error": "ocr-disabled"}
            except Exception as e:
                ocr_res = {"ocr_text": "", "error": str(e)}
            img_entry["ocr_text"] = ocr_res.get("ocr_text", "")
            img_entry["ocr_error"] = ocr_res.get("error")
            if img_entry["ocr_text"]:
                ocr_texts.append(img_entry["ocr_text"])
            images.append(img_entry)

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

        # sentiment source
        sentiment_source = " ".join([title_tag, meta_desc, headings_text, text])
        sentiment = self._analyze_sentiment(sentiment_source)

        # aggregate OCR text for the page
        ocr_aggregate = " ".join([t for t in ocr_texts if t])

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
            # OCR
            "ocr_text": ocr_aggregate,
            "ocr_details": images,  # per-image src/alt/ocr_text/ocr_error
        }

    def crawl(self, progress_callback=None, on_page=None):
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
                logger.debug("Blocked by robots: %s", url)
                self.visited.add(url)
                continue

            try:
                resp = requests.get(url, headers=self.headers, timeout=self.timeout)
                content_type = resp.headers.get("Content-Type", "") or ""
                if resp.status_code != 200 or "html" not in content_type:
                    logger.debug("Skipping non-HTML or non-200: %s (%s)", url, resp.status_code)
                    self.visited.add(url)
                    if progress_callback:
                        progress_callback(len(self.results), self.max_pages, url)
                    continue

                page = self._extract(resp.text, url)
                self.results.append(page)
                self.visited.add(url)

                if on_page:
                    try:
                        on_page(page)
                    except Exception:
                        logger.exception("on_page callback raised an exception")

                for link in page.get("links", []):
                    if link not in self.visited and link not in q:
                        if self._same_domain(link):
                            q.append(link)

                if progress_callback:
                    progress_callback(len(self.results), self.max_pages, url)

                time.sleep(self.delay)
            except Exception as e:
                logger.exception("Error fetching %s: %s", url, e)
                self.visited.add(url)
                if progress_callback:
                    progress_callback(len(self.results), self.max_pages, url)
                time.sleep(self.delay)

        return self.results
