from collections import defaultdict, Counter
from urllib.parse import urlparse

def compute_metrics(crawl):
    """
    Input: crawl dict from crawler.crawl_site
    Output: metrics dict and mapping metric->list(urls)
    """
    pages = crawl["pages"]
    metrics = {}
    lists = defaultdict(list)

    # 1. Total pages
    metrics["total_pages"] = len(pages)
    lists["total_pages"] = list(pages.keys())

    # Duplicate pages by content_hash
    hash_map = defaultdict(list)
    for url, p in pages.items():
        h = p.get("content_hash", "")
        if h:
            hash_map[h].append(url)
    duplicate_pages = [urls for urls in hash_map.values() if len(urls) > 1]
    dup_count = sum(len(u) for u in duplicate_pages)
    metrics["duplicate_pages"] = dup_count
    lists["duplicate_pages"] = [u for grp in duplicate_pages for u in grp]

    # Duplicate titles
    title_map = defaultdict(list)
    for url, p in pages.items():
        title_map[(p.get("title") or "").strip().lower()].append(url)
    dup_titles = [grp for grp in title_map.values() if len(grp) > 1 and grp[0] != ""]
    metrics["duplicate_meta_titles"] = sum(len(g) for g in dup_titles)
    lists["duplicate_meta_titles"] = [u for grp in dup_titles for u in grp]

    # Duplicate descriptions
    desc_map = defaultdict(list)
    for url, p in pages.items():
        desc_map[(p.get("meta_description") or "").strip().lower()].append(url)
    dup_descs = [grp for grp in desc_map.values() if len(grp) > 1 and grp[0] != ""]
    metrics["duplicate_meta_descriptions"] = sum(len(g) for g in dup_descs)
    lists["duplicate_meta_descriptions"] = [u for grp in dup_descs for u in grp]

    # Canonical issues (missing or pointing outside domain or duplicate canonicals)
    canon_issues = []
    parsed_start = urlparse(crawl["start_url"]).netloc
    for url, p in pages.items():
        canon = p.get("canonical", "") or ""
        if not canon:
            canon_issues.append(url)
        else:
            try:
                if urlparse(canon).netloc and urlparse(canon).netloc != parsed_start:
                    canon_issues.append(url)
            except Exception:
                canon_issues.append(url)
    metrics["canonical_issues"] = len(canon_issues)
    lists["canonical_issues"] = canon_issues

    # Images missing or duplicate alt tags
    missing_alt = []
    alt_map = defaultdict(list)
    for url, p in pages.items():
        for img in p.get("images", []):
            alt = (img.get("alt") or "").strip()
            src = img.get("src")
            if not alt:
                missing_alt.append({"page": url, "src": src})
            else:
                alt_map[alt].append({"page": url, "src": src})
    duplicate_alt = [v for v in alt_map.values() if len(v) > 1]
    metrics["images_missing_alt"] = len(missing_alt)
    metrics["images_duplicate_alt"] = sum(len(g) for g in duplicate_alt)
    lists["images_missing_alt"] = [x["page"] + " -> " + (x["src"] or "") for x in missing_alt]
    lists["images_duplicate_alt"] = [x["page"] + " -> " + (x["src"] or "") for grp in duplicate_alt for x in grp]

    # Broken links: pages with any out_link that returned >=400 or None as fetch status
    broken_pages = []
    pages_with_300 = []
    pages_with_400 = []
    pages_with_500 = []
    for url, p in pages.items():
        status = p.get("status_code") or 0
        if 300 <= status < 400:
            pages_with_300.append(url)
        if 400 <= status < 500:
            pages_with_400.append(url)
        if 500 <= status < 600:
            pages_with_500.append(url)
        # check out_links status if we stored them (crawler stores only URLs, not statuses for each outgoing)
        # simple heuristic: if page itself returned 4xx/5xx, mark broken
        if status is None or (400 <= status < 600):
            broken_pages.append(url)
    metrics["pages_with_300_responses"] = len(set(pages_with_300))
    metrics["pages_with_400_responses"] = len(set(pages_with_400))
    metrics["pages_with_500_responses"] = len(set(pages_with_500))
    metrics["pages_with_broken_links"] = len(set(broken_pages))
    lists["pages_with_300_responses"] = list(set(pages_with_300))
    lists["pages_with_400_responses"] = list(set(pages_with_400))
    lists["pages_with_500_responses"] = list(set(pages_with_500))
    lists["pages_with_broken_links"] = list(set(broken_pages))

    # Indexable vs non-indexable (simple: meta robots noindex)
    indexable = []
    non_indexable = []
    for url, p in pages.items():
        # detect noindex in headers or meta
        # (crawler didn't save meta robots separately; search in content_text)
        txt = (p.get("content_text") or "").lower()
        if "noindex" in txt:
            non_indexable.append(url)
        else:
            indexable.append(url)
    metrics["indexable_pages"] = len(indexable)
    metrics["non_indexable_pages"] = len(non_indexable)
    lists["indexable_pages"] = indexable
    lists["non_indexable_pages"] = non_indexable

    # On-Page / Content Issues / PageSpeed / URL Inspection -> placeholders
    # These require previous crawl data and external APIs (PageSpeed Insights, Google URL Inspection)
    metrics["onpage_changes"] = 0
    metrics["content_issues"] = 0
    metrics["pagespeed_issues"] = 0
    metrics["url_inspection_issues"] = 0
    lists["onpage_changes"] = []
    lists["content_issues"] = []
    lists["pagespeed_issues"] = []
    lists["url_inspection_issues"] = []

    return metrics, lists
