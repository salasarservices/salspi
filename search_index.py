import re
from collections import defaultdict
from typing import List, Dict, Any

WORD_RE = re.compile(r"\b\w[\w'-]*\b", re.UNICODE)

class SearchIndex:
    def __init__(self):
        # word -> list of (url, field, snippet)
        self.inverted = defaultdict(list)
        # url -> page dict
        self.pages = {}

    def build(self, pages: List[Dict[str, Any]]):
        self.inverted.clear()
        self.pages = {}
        for p in pages:
            url = p["url"]
            self.pages[url] = p
            # index title, meta, text, image alts
            fields = {
                "title": p.get("title", "") or "",
                "meta": p.get("meta", "") or "",
                "text": p.get("text", "") or "",
                "alt": " ".join(img.get("alt", "") for img in p.get("images", []))
            }
            for field_name, content in fields.items():
                # store snippets per word
                for m in WORD_RE.finditer(content):
                    w = m.group(0).lower()
                    snippet = self._make_snippet(content, m.start(), m.end())
                    self.inverted[w].append({"url": url, "field": field_name, "snippet": snippet})
            # also store content for phrase search
            p["_fields"] = fields

    def _make_snippet(self, content, start, end, radius=40):
        s = max(0, start - radius)
        e = min(len(content), end + radius)
        snippet = content[s:e].strip()
        return snippet.replace("\n", " ")

    def search(self, query: str, fields: List[str]=None, phrase: bool=False, max_results=500):
        """
        fields: list of field names to search: title, meta, text, alt
        phrase: if True, perform substring search over selected fields; else token search
        """
        fields = fields or ["title", "meta", "text", "alt"]
        q = query.strip().lower()
        results = []
        seen = set()

        if phrase:
            for url, p in self.pages.items():
                for f in fields:
                    content = p.get("_fields", {}).get(f, "")
                    if q in content.lower():
                        snippet = self._make_snippet(content, content.lower().index(q), content.lower().index(q)+len(q))
                        key = (url, f)
                        if key not in seen:
                            results.append({"url": url, "field": f, "snippet": snippet, "title": p.get("title", "")})
                            seen.add(key)
                            if len(results) >= max_results:
                                return results
            return results

        # token search: split query into words and find pages containing all words (AND)
        tokens = [t for t in re.findall(WORD_RE, q)]
        if not tokens:
            return []

        # gather candidates for first token
        candidates = [entry for entry in self.inverted.get(tokens[0], []) if entry["field"] in fields]
        # use a mapping url->matches
        url_hits = {}
        for c in candidates:
            url = c["url"]
            url_hits.setdefault(url, []).append(c)

        for t in tokens[1:]:
            next_entries = [entry for entry in self.inverted.get(t, []) if entry["field"] in fields]
            next_urls = {}
            for e in next_entries:
                next_urls.setdefault(e["url"], []).append(e)
            # keep intersection
            url_hits = {url: url_hits[url] + next_urls[url] for url in list(url_hits.keys()) if url in next_urls}

        # convert to result list
        for url, hits in url_hits.items():
            page = self.pages.get(url, {})
            snippets = "; ".join(h["snippet"] for h in hits[:3])
            results.append({"url": url, "title": page.get("title", ""), "snippets": snippets, "count": len(hits)})
            if len(results) >= max_results:
                break
        # sort by count desc
        results.sort(key=lambda x: x.get("count", 0), reverse=True)
        return results
