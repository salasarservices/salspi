```python
import asyncio
import aiohttp
import async_timeout
import re
import threading
import time
import logging
from urllib.parse import urljoin, urldefrag, urlparse
from urllib.robotparser import RobotFileParser
from typing import Optional, Callable

logger = logging.getLogger("salspi.crawler")
logger.setLevel(logging.INFO)


HREF_RE = re.compile(r'href=[\'"]?([^\'" >]+)', re.IGNORECASE)


async def _fetch_robots_txt(session: aiohttp.ClientSession, base_url: str) -> RobotFileParser:
    """
    Fetch robots.txt and return a RobotFileParser instance.
    If robots.txt cannot be fetched, returns a parser that allows everything.
    """
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    parser = RobotFileParser()
    try:
        async with async_timeout.timeout(8):
            async with session.get(robots_url, allow_redirects=True) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    parser.parse(text.splitlines())
                    return parser
    except Exception as e:
        logger.debug("Could not fetch robots.txt (%s): %s", robots_url, e)
    # fallback: allow everything
    parser.parse(["User-agent: *", "Disallow:"])
    return parser


def _normalize_url(base: str, link: str) -> Optional[str]:
    """
    Normalize and join a discovered link against a base. Remove fragments.
    Return None if link is javascript: or mailto: or empty, etc.
    """
    if not link:
        return None
    link = link.strip()
    if link.startswith("javascript:") or link.startswith("mailto:") or link.startswith("tel:"):
        return None
    # Join relative URLs
    joined = urljoin(base, link)
    # Remove fragment
    clean, _ = urldefrag(joined)
    return clean


def _extract_links(base_url: str, html: str):
    """
    Lightweight HTML anchor extractor using regex (fast, dependency-free).
    This will miss JS-generated links; use a renderer (Playwright/Puppeteer) if needed.
    """
    for match in HREF_RE.findall(html):
        normalized = _normalize_url(base_url, match)
        if normalized:
            yield normalized


async def _fetch_url(session: aiohttp.ClientSession, url: str, timeout: int = 15):
    """
    Fetch a URL and return (status, text, headers). Raises on network errors.
    """
    async with async_timeout.timeout(timeout):
        async with session.get(url, allow_redirects=True) as resp:
            content_type = resp.headers.get("Content-Type", "")
            text = await resp.text(errors="ignore")
            return resp.status, text, content_type


async def crawl_site(
    start_url: str,
    max_pages: int = 0,
    workers: int = 8,
    timeout: int = 15,
    max_retries: int = 2,
    same_host_only: bool = True,
    progress_cb: Optional[Callable[[dict], None]] = None,
    db_writer: Optional[Callable[[dict], None]] = None,
    stop_event: Optional[threading.Event] = None,
):
    """
    Asynchronous crawler that starts from start_url and crawls up to max_pages (0 = unlimited).
    - same_host_only: only follow links on the same netloc as start_url.
    - progress_cb: optional callback called with {"crawled":int, "discovered":int, "current":str}
    - db_writer: optional callable to persist documents (gets dict)
    - stop_event: optional threading.Event to request cancellation from outside
    Returns a summary dict.
    """
    parsed_start = urlparse(start_url)
    base_netloc = parsed_start.netloc

    seen = set()
    discovered = set()
    queue = asyncio.Queue()
    await queue.put(start_url)
    discovered.add(start_url)

    # counters
    crawled = 0
    discovered_count = 1

    # For robust retries, we'll keep a small per-URL retry counter
    retries = {}

    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=30)

    async with aiohttp.ClientSession(timeout=session_timeout, headers={"User-Agent": "salspi-crawler/1.0"}) as session:
        robots = await _fetch_robots_txt(session, start_url)

        semaphore = asyncio.Semaphore(workers)

        async def worker(worker_id: int):
            nonlocal crawled, discovered_count
            while True:
                if stop_event and stop_event.is_set():
                    logger.info("Worker %s stopping due to stop_event", worker_id)
                    break
                try:
                    url = await asyncio.wait_for(queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    # check for completion condition: queue empty and no more work
                    if queue.empty():
                        break
                    else:
                        continue

                try:
                    # Respect robots
                    try:
                        can_fetch = robots.can_fetch("*", url)
                    except Exception:
                        can_fetch = True

                    if not can_fetch:
                        logger.debug("Blocked by robots.txt: %s", url)
                        queue.task_done()
                        continue

                    parsed = urlparse(url)
                    if same_host_only and parsed.netloc != base_netloc:
                        logger.debug("Skipping external host: %s", url)
                        queue.task_done()
                        continue

                    if max_pages and crawled >= max_pages:
                        queue.task_done()
                        break

                    async with semaphore:
                        attempt = retries.get(url, 0)
                        try:
                            status, text, content_type = await _fetch_url(session, url, timeout=timeout)
                        except Exception as e:
                            logger.debug("Fetch error for %s: %s (attempt %d)", url, e, attempt)
                            if attempt < max_retries:
                                retries[url] = attempt + 1
                                await queue.put(url)  # retry
                            else:
                                logger.info("Giving up on %s after %d attempts", url, attempt)
                            queue.task_done()
                            continue

                        # Only process HTML
                        if "text/html" in content_type.lower():
                            # Extract links
                            for link in _extract_links(url, text):
                                if link not in discovered:
                                    discovered.add(link)
                                    discovered_count += 1
                                    await queue.put(link)

                        # Optionally write to DB (non-blocking if db_writer is quick)
                        if db_writer:
                            try:
                                db_writer({"url": url, "status": status, "fetched_at": time.time()})
                            except Exception as e:
                                logger.debug("db_writer failed for %s: %s", url, e)

                        crawled += 1
                        if progress_cb:
                            try:
                                progress_cb({"crawled": crawled, "discovered": discovered_count, "current": url})
                            except Exception:
                                logger.debug("progress_cb raised an exception", exc_info=True)

                        logger.info("Crawled %s (%d) content-type=%s", url, crawled, content_type)
                finally:
                    queue.task_done()

                # stop if we've reached the limit
                if max_pages and crawled >= max_pages:
                    break

        # start workers
        tasks = [asyncio.create_task(worker(i)) for i in range(max(1, workers))]
        # Wait until queue is fully processed or stop_event is set or max_pages reached
        try:
            # Wait for all tasks; workers will exit when queue is empty or stop_event set
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            logger.exception("Unexpected error while crawling")

    summary = {
        "start_url": start_url,
        "crawled": crawled,
        "discovered": discovered_count,
        "timestamp": time.time(),
    }

    # Final DB write if desired
    if db_writer:
        try:
            db_writer({"type": "summary", **summary})
        except Exception:
            logger.debug("db_writer failed for summary", exc_info=True)

    return summary


def start_crawl_thread(
    start_url: str,
    max_pages: int = 0,
    workers: int = 8,
    timeout: int = 15,
    max_retries: int = 2,
    same_host_only: bool = True,
    progress_cb: Optional[Callable[[dict], None]] = None,
    db_writer: Optional[Callable[[dict], None]] = None,
):
    """
    Launch crawl_site(...) in a background daemon thread. Returns (stop_event, thread).
    Call stop_event.set() to request cancellation.
    """

    stop_event = threading.Event()

    def _runner():
        try:
            # Run the async crawler to completion in this thread
            asyncio.run(
                crawl_site(
                    start_url=start_url,
                    max_pages=max_pages,
                    workers=workers,
                    timeout=timeout,
                    max_retries=max_retries,
                    same_host_only=same_host_only,
                    progress_cb=progress_cb,
                    db_writer=db_writer,
                    stop_event=stop_event,
                )
            )
        except Exception:
            logger.exception("Crawler thread crashed")

    thread = threading.Thread(target=_runner, daemon=True, name="salspi-crawler-thread")
    thread.start()
    return stop_event, thread
