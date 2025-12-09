import streamlit as st
from datetime import datetime
import streamlit.components.v1 as components
import networkx as nx
from pyvis.network import Network
import os

from crawler import crawl_site
from seo_checks import compute_metrics
from db import save_crawl, latest_crawl, delete_database, list_crawls

# --- UI styling ---
st.set_page_config(page_title="SEO Crawler", layout="wide")
PASTEL_CARDS = {
    "total_pages": "#E9F7EF",
    "duplicate_pages": "#FFF3E0",
    "duplicate_meta_titles": "#E8F0FF",
    "duplicate_meta_descriptions": "#F3E8FF",
    "canonical_issues": "#FFEFF0",
    "images_missing_alt": "#E8FFF4",
    "images_duplicate_alt": "#FFF8E8",
    "pages_with_broken_links": "#FDEFEF",
    "pages_with_300_responses": "#F0F4FF",
    "pages_with_400_responses": "#FFF0F3",
    "pages_with_500_responses": "#FFF7E6",
    "indexable_pages": "#F7FFF0",
    "non_indexable_pages": "#FFF0F8",
}

# --- Helpers ---
def metric_tile(key, value, color, label):
    st.markdown(
        f"""
        <div style="background:{color}; padding:14px; border-radius:8px; text-align:center">
          <div style="font-size:20px; color:#333; margin-bottom:4px">{label}</div>
          <div style="font-weight:700; font-size:28px; color:#111">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def draw_network_graph(crawl):
    g = nx.DiGraph()
    pages = crawl["pages"]
    links = crawl["links"]
    for u in pages.keys():
        g.add_node(u, label=u)
    for u, outs in links.items():
        for v in outs:
            # Add if same domain only for readability
            g.add_edge(u, v)
    net = Network(height="600px", width="100%", directed=True)
    net.from_nx(g)
    net.repulsion(node_distance=200, central_gravity=0.1)
    path = "html_reports/site_graph.html"
    net.show(path)
    html = open(path, "r", encoding="utf-8").read()
    components.html(html, height=600, scrolling=True)

# --- Main App ---
st.title("SEO Crawler & Analyzer")
col1, col2, col3 = st.columns([1,2,1])
with col1:
    site = st.text_input("Site to crawl (include scheme)", value="https://example.com")
    max_pages = st.number_input("Max pages", min_value=10, max_value=10000, value=500, step=10)
    max_workers = st.number_input("Workers", min_value=2, max_value=40, value=8)
with col2:
    if st.button("Start Crawl"):
        with st.spinner("Crawling site... this may take a while"):
            crawl = crawl_site(site, max_workers=max_workers, max_pages=max_pages)
            save_crawl(site, crawl)
            st.success("Crawl complete and saved.")
            st.session_state["last_crawl"] = crawl
    if st.button("Load Latest Crawl"):
        doc = latest_crawl(site)
        if doc:
            st.session_state["last_crawl"] = doc["crawl"]
            st.success("Loaded latest crawl from DB.")
        else:
            st.warning("No crawl saved for this site.")
with col3:
    if st.button("Clear Cache / Refresh App"):
        # minimal: clear session state
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.experimental_rerun()
    if st.button("Delete Mongo DB Database"):
        if st.button("Confirm Delete DB"):
            delete_database()
            st.success("Database deleted. Restart app.")

# check session
crawl = st.session_state.get("last_crawl")
if not crawl:
    st.info("No crawl loaded. Start a crawl or load latest from DB.")

if crawl:
    metrics, lists = compute_metrics(crawl)
    # Show metrics as columns (grouped)
    metric_keys = [
        ("total_pages", "Total Pages"),
        ("duplicate_pages", "Duplicate Pages"),
        ("duplicate_meta_titles", "Duplicate Meta Titles"),
        ("duplicate_meta_descriptions", "Duplicate Meta Descriptions"),
        ("canonical_issues", "Canonical Issues"),
        ("images_missing_alt", "Images Missing Alt"),
        ("images_duplicate_alt", "Images Duplicate Alt"),
        ("pages_with_broken_links", "Pages with Broken Links"),
        ("pages_with_300_responses", "Pages with 300 Responses"),
        ("pages_with_400_responses", "Pages with 400 Responses"),
        ("pages_with_500_responses", "Pages with 500 Responses"),
        ("indexable_pages", "Indexable Pages"),
        ("non_indexable_pages", "Non-indexable Pages"),
    ]
    cols = st.columns(3)
    i = 0
    for key, label in metric_keys:
        c = cols[i % 3]
        with c:
            color = PASTEL_CARDS.get(key, "#F0F0F0")
            val = metrics.get(key, 0)
            if st.button(f"{label} — {val}", key=f"btn_{key}"):
                st.session_state["selected_metric"] = key
            metric_tile(key, val, color, label)
        i += 1

    # Show selected list
    sel = st.session_state.get("selected_metric")
    if sel:
        st.subheader(f"URLs for metric: {sel}")
        items = lists.get(sel) or []
        st.write(f"Total: {len(items)}")
        for u in items:
            st.markdown(f"- {u}")

    # Site structure visualization
    st.header("Site Structure")
    try:
        draw_network_graph(crawl)
    except Exception as e:
        st.error(f"Could not render graph: {e}")

    # Search Site
    st.header("Search Site Content")
    query = st.text_input("Search for a word or phrase")
    if st.button("Search"):
        results = []
        for url, p in crawl["pages"].items():
            txt = (p.get("content_text") or "").lower()
            if query.strip().lower() in txt:
                results.append(url)
        st.write(f"Found {len(results)} pages containing '{query}'")
        for r in results[:200]:
            st.markdown(f"- {r}")

    st.header("Crawl meta")
    st.write(f"Start URL: {crawl.get('start_url')}")
    st.write(f"Pages crawled: {len(crawl.get('pages', {}))}")
    st.write(f"Timestamp: {crawl.get('timestamp')}")
