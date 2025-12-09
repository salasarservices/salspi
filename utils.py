import hashlib
import re
from urllib.parse import urljoin, urlparse

def normalize_url(base, link):
    if not link:
        return None
    # remove fragments
    joined = urljoin(base, link)
    parsed = urlparse(joined)
    normalized = parsed._replace(fragment="").geturl().rstrip('/')
    return normalized

def is_same_domain(start_netloc, url):
    try:
        return urlparse(url).netloc == start_netloc
    except Exception:
        return False

def extract_text(soup):
    # remove script/style
    for s in soup(["script", "style", "noscript"]):
        s.decompose()
    text = soup.get_text(separator=" ", strip=True)
    # collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text

def hash_text(text):
    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()
