import streamlit as st
import time
import logging
import pandas as pd
from crawler import CrawlManager, crawl_site
from seo_checks import compute_metrics
from db import save_crawl, latest_crawl, delete_database, list_crawls, get_config, is_connected
from graph_utils import draw_network_graph

st.set_page_config(page_title="SEO Crawler", layout="wide")

# --- Helper: build DataFrame for metric display ---
def build_metric_dataframe(crawl, metric_key, lists):
    """
    Build a pandas DataFrame for the selected metric_key using crawl + lists mapping.
    DataFrame columns: URL, Title, Status, Notes (where applicable).
    """
    pages = crawl.get("pages", {}) if crawl else {}
    rows = []

    entries = lists.get(metric_key, [])
    # entries might be strings like "page -> src" for image lists, or just URLs.
    for e in entries:
        if isinstance(e, dict):
            # some lists might return dicts
            page = e.get("page") or e.get("url") or ""
            src = e.get("src") or ""
            status = pages.get(page, {}).get("status_code") if page in pages else ""
            title = pages.get(page, {}).get("title") if page in pages else ""
            notes = src
            rows.append({"URL": page, "Title": title, "Status": status, "Notes": notes})
            continue

        if isinstance(e, str) and "->" in e:
            # pattern "page -> src"
            try:
                page_part, src_part = [p.strip() for p in e.split("->", 1)]
            except Exception:
                page_part = e
                src_part = ""
            page = page_part
            status = pages.get(page, {}).get("status_code") if page in pages else ""
            title = pages.get(page, {}).get("title") if page in pages else ""
            rows.append({"URL": page, "Title": title, "Status": status, "Notes": src_part})
            continue

        # otherwise treat e as URL
        page = e
        status = pages.get(page, {}).get("status_code") if page in pages else ""
        title = pages.get(page, {}).get("title") if page in pages else ""
        rows.append({"URL": page, "Title": title, "Status": status, "Notes": ""})

    if not rows:
        return pd.DataFrame(columns=["URL", "Title", "Status", "Notes"])
    df = pd.DataFrame(rows)
    # Deduplicate rows by URL while preserving first occurrence
    df = df.drop_duplicates(subset=["URL"], keep="first").reset_index(drop=True)
    return df

# --- UI: controls ---
st.title("SEO Crawler & Analyzer")

col_left, col_mid, col_right = st.columns([1, 1, 1])
with col_left:
    site = st.text_input("Site to crawl (include scheme)", value="https://example.com")
    max_pages = st.number_input("Max pages", min_value=10, max_value=20000, value=500, step=10)
    max_workers = st.number_input("Workers", min_value=1, max_value=50, value=8)
with col_mid:
    # Import the crawler runner (adjust import path if your package layout differs)
try:
    from salspi.crawler import start_crawl_thread  # preferred if app is a package
except Exception:
    from crawler import start_crawl_thread  # fallback for local import

logger = logging.getLogger("salspi.app")

# Ensure session_state keys for the form fields are present (adapt keys if yours differ)
# If your UI uses different field names, replace these keys accordingly.
start_url = st.session_state.get("start_url") or st.session_state.get("site_to_crawl")
max_pages = int(st.session_state.get("max_pages", 500))
workers = int(st.session_state.get("workers", 8))

# Start Crawl
if st.button("Start Crawl"):
    # Read current values from session_state (or fallback to text inputs if you use them)
    start_url = st.session_state.get("start_url") or st.session_state.get("site_to_crawl") or start_url
    if not start_url:
        st.error("Please enter the site to crawl (include scheme).")
    else:
        # Prevent launching multiple threads
        thread_alive = False
        thread_obj = st.session_state.get("crawl_thread")
        if thread_obj is not None and getattr(thread_obj, "is_alive", lambda: False)():
            thread_alive = True

        if thread_alive:
            st.info("Crawl already running")
        else:
            stop_event, thread = start_crawl_thread(
                start_url=start_url,
                max_pages=max_pages,
                workers=workers,
                progress_cb=None,   # supply a progress callback or db_writer if you have one
                db_writer=None,     # or pass a lightweight db writer function
            )
            st.session_state.crawl_stop_event = stop_event
            st.session_state.crawl_thread = thread
            st.success("Crawl started in background. Use Refresh Progress to update status.")

# Pause Crawl (optional behaviour)
if st.button("Pause Crawl"):
    # Simple behaviour: request stop; you can implement a dedicated pause flag instead
    if st.session_state.get("crawl_stop_event"):
        st.session_state.crawl_stop_event.set()
        st.info("Pause/stop requested; workers will exit. Restart to resume.")
    else:
        st.info("No crawl in progress to pause.")

# Stop Crawl
if st.button("Stop Crawl"):
    if st.session_state.get("crawl_stop_event"):
        st.session_state.crawl_stop_event.set()
        st.info("Stop requested; workers will exit soon.")
    else:
        st.info("No crawl in progress.")

# Refresh Progress (safe fallback if experimental_rerun is missing)
if st.button("Refresh Progress"):
    rerun = getattr(st, "experimental_rerun", None)
    if callable(rerun):
        try:
            rerun()
        except Exception:
            # fallback: update query params to force a rerun without crashing
            st.experimental_set_query_params(_refresh=int(time.time()))
    else:
        st.experimental_set_query_params(_refresh=int(time.time()))
# ---- END REPLACE BLOCK ----
with col_right:
    # DB status
    cfg = get_config()
    if is_connected():
        st.success("Connected to MongoDB")
        host = (cfg.get("uri") or "").split("@")[-1] if cfg.get("uri") else ""
        st.write("Host:", host)
        st.write("DB:", cfg.get("db"))
    else:
        st.warning("MongoDB unreachable — using local fallback.")

st.markdown("---")

# --- Progress area (visible and updateable) ---
manager = st.session_state.get("crawler_manager")
if manager:
    prog = manager.get_progress()
    pages_crawled = prog["pages_crawled"]
    discovered = prog["discovered"]
    max_pages_cfg = prog["max_pages"]
    finished = prog["finished"]
    error = prog["error"]

    st.subheader("Crawl Progress")
    progress_placeholder = st.empty()
    # Display a numeric + progress bar area
    with progress_placeholder.container():
        denom = max(1, max_pages_cfg)
        fraction = min(1.0, pages_crawled / denom)
        pct = int(fraction * 100)
        st.progress(pct)
        st.markdown(f"Pages crawled: **{pages_crawled}** / **{max_pages_cfg}** — Discovered URLs: **{discovered}**")
        if manager.is_paused():
            st.info("Status: Paused")
        elif manager.is_running():
            st.info("Status: Running")
        elif finished:
            st.success("Status: Finished")
        if error:
            st.error(f"Error: {error}")

    # Option: auto-refresh while running
    auto_refresh = st.checkbox("Auto-refresh progress (every 2s)", value=False)
    if auto_refresh and manager.is_running():
        # Try to use streamlit_autorefresh if available; otherwise simple rerun loop
        try:
            from streamlit_autorefresh import st_autorefresh
            # st_autorefresh returns an integer count; requesting reruns at interval_ms
            st_autorefresh(interval=2000, key="auto_refresh")
        except Exception:
            # Best-effort fallback: short sleep then rerun (non-blocking user interactions will still be limited)
            time.sleep(2)
            st.experimental_rerun()

    # When finished, allow saving results to DB and view metrics
    if finished and manager.get_result():
        crawl = manager.get_result()
        st.write("Crawl completed. You can save it to DB or view metrics.")
        if st.button("Save Crawl to DB"):
            saved_id = save_crawl(site, crawl)
            st.success(f"Crawl saved: {saved_id}")
        if st.button("View Metrics for Last Crawl"):
            metrics, lists = compute_metrics(crawl)
            st.experimental_set_query_params()  # small no-op to trigger UI stability
            st.json(metrics)

else:
    st.info("No crawl in progress. Click 'Start Crawl' to begin.")

st.markdown("---")

# --- Metrics tiles & selectable listing ---
# Load latest crawl snapshot either from session manager result or from stored last crawl
crawl = None
if manager and manager.get_result():
    crawl = manager.get_result()
else:
    crawl = st.session_state.get("last_crawl")

if crawl:
    metrics, lists = compute_metrics(crawl)

    # Metric keys and labels
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

    st.subheader("Metrics")
    cols = st.columns(3)
    i = 0
    for key, label in metric_keys:
        c = cols[i % 3]
        with c:
            val = metrics.get(key, 0)
            # Button to select metric; will set selected_metric in session_state
            if st.button(f"{label} — {val}", key=f"btn_{key}"):
                st.session_state["selected_metric"] = key
            # Display as a subtle tile
            st.markdown(f"<div style='background:#f5f7fb; padding:12px; border-radius:8px; text-align:center'>"
                        f"<div style='font-size:14px; color:#333;'>{label}</div>"
                        f"<div style='font-weight:700; font-size:22px; color:#111'>{val}</div></div>",
                        unsafe_allow_html=True)
        i += 1

    # Show selected list in an excel-like scrollable table
    sel = st.session_state.get("selected_metric")
    if sel:
        st.subheader(f"URLs for metric: {sel}")
        items_df = build_metric_dataframe(crawl, sel, lists)
        st.write(f"Total rows: {len(items_df)}")
        # Excel-like, scrollable display using st.dataframe with a fixed height
        st.dataframe(items_df, height=360)  # user can scroll inside the frame

    # Option to render site structure graph
    st.header("Site Structure")
    try:
        draw_network_graph(crawl)
    except Exception as e:
        st.error(f"Could not render graph: {e}")

else:
    st.info("No crawl data available. Start a crawl or load the latest crawl from DB.")

st.markdown("---")
# --- Search Site ---
st.header("Search Site (from last crawl)")
if st.button("Load Latest Crawl From DB"):
    doc = latest_crawl(site)
    if doc:
        # doc may be a dict with "crawl" key (from Mongo) or the raw crawl (from local fallback)
        if isinstance(doc, dict) and "crawl" in doc:
            st.session_state["last_crawl"] = doc["crawl"]
        else:
            st.session_state["last_crawl"] = doc
        st.success("Loaded latest crawl from DB (or local fallback).")
    else:
        st.warning("No saved crawl found for this site.")

crawl_loaded = st.session_state.get("last_crawl")
if crawl_loaded:
    query = st.text_input("Search for a word or phrase")
    if st.button("Search"):
        results = []
        for url, p in crawl_loaded["pages"].items():
            txt = (p.get("content_text") or "").lower()
            if query.strip().lower() in txt:
                results.append({"URL": url, "Title": p.get("title", ""), "Status": p.get("status_code", "")})
        df_res = pd.DataFrame(results)
        st.dataframe(df_res, height=360)

st.markdown("""
Notes:
- Click a metric tile to view the affected URLs in an excel-like scrollable table.
- Use 'Auto-refresh progress' to keep the progress bar updating automatically (requires the optional streamlit-autorefresh package; otherwise use 'Refresh Progress').
- When a crawl is running, use Pause / Resume / Stop controls to manage it.
""")
