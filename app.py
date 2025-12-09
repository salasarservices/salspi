# streamlit_app.py (modified to add MongoDB save and batch search textbox)
import streamlit as st
import pandas as pd
from crawler import Crawler
from search_index import SearchIndex
from db import save_pages_to_mongo
import time
from io import StringIO

st.set_page_config(page_title="Website Crawler & Search", layout="wide")

st.title("Website Crawler & Search")
st.markdown("Crawl a site (limited pages) and search words/phrases in titles, descriptions, body text and image alt tags.")

# Sidebar: crawl settings
st.sidebar.header("Crawl settings")
start_url = st.sidebar.text_input("Start URL (including http:// or https://)", value="https://example.com")
max_pages = st.sidebar.number_input("Max pages to crawl", min_value=1, max_value=2000, value=500, step=50)
delay = st.sidebar.slider("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
same_domain = st.sidebar.checkbox("Restrict to same domain", True)

st.sidebar.markdown("Advanced")
user_agent = st.sidebar.text_input("User-Agent header", value="site-crawler-bot/1.0")
timeout = st.sidebar.number_input("Request timeout (s)", min_value=1, value=10)

# Sidebar: MongoDB settings
st.sidebar.header("MongoDB (optional)")
mongo_uri = st.sidebar.text_input("MongoDB URI", value="", help="e.g. mongodb://user:pass@host:27017 or mongodb://localhost:27017")
mongo_db = st.sidebar.text_input("DB name", value="sitecrawler")
mongo_collection = st.sidebar.text_input("Collection name", value="pages")
auto_save_mongo = st.sidebar.checkbox("Auto-save crawl to MongoDB after finishing", value=False)

# Initialize session state
if "pages" not in st.session_state:
    st.session_state.pages = []
if "index" not in st.session_state:
    st.session_state.index = None

# Main controls
col1, col2 = st.columns([2,1])

with col1:
    run = st.button("Start crawl")
    stop = st.button("Stop (not implemented)")  # placeholder
with col2:
    st.write("Index status")
    if st.session_state.pages:
        st.write(f"Pages indexed: {len(st.session_state.pages)}")
    else:
        st.write("No pages indexed")

progress_bar = st.progress(0)
status_text = st.empty()
log_area = st.empty()

if run:
    if not start_url.startswith("http"):
        st.error("Please enter a valid http/https URL.")
    else:
        st.info("Starting crawl — this will run synchronously in the Streamlit session. For large crawls you may want to run separately.")
        crawler = Crawler(start_url=start_url, max_pages=int(max_pages), delay=float(delay), same_domain=bool(same_domain),
                          headers={"User-Agent": user_agent}, timeout=int(timeout))
        pages = []
        def progress_cb(current, maximum, last_url):
            try:
                progress = int((current / maximum) * 100)
            except Exception:
                progress = 0
            progress_bar.progress(min(progress, 100))
            status_text.markdown(f"Crawled {current}/{maximum}: {last_url}")
            log_area.text(f"Crawled {current}/{maximum}: {last_url}")

        pages = crawler.crawl(progress_callback=progress_cb)
        st.session_state.pages = pages
        st.success(f"Finished crawling: {len(pages)} pages collected.")
        progress_bar.progress(100)
        # build index
        idx = SearchIndex()
        idx.build(pages)
        st.session_state.index = idx

        # Optionally save to MongoDB automatically
        if auto_save_mongo and mongo_uri:
            with st.spinner("Saving pages to MongoDB..."):
                try:
                    summary = save_pages_to_mongo(pages, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                    if summary.get("errors"):
                        st.warning(f"Completed with errors: {len(summary['errors'])} (first: {summary['errors'][0]})")
                    st.success(f"Saved to MongoDB — inserted: {summary.get('inserted',0)}, updated: {summary.get('updated',0)}")
                except Exception as e:
                    st.error(f"Error saving to MongoDB: {e}")

# Allow manual save to Mongo after crawl
if st.session_state.pages:
    st.markdown("### Save crawl results")
    col_a, col_b = st.columns([3,1])
    with col_a:
        st.write("MongoDB destination:")
        st.write(f"URI: {'(not set)' if not mongo_uri else mongo_uri}")
        st.write(f"DB: {mongo_db}, Collection: {mongo_collection}")
    with col_b:
        if st.button("Save to MongoDB") and mongo_uri:
            with st.spinner("Saving pages to MongoDB..."):
                try:
                    summary = save_pages_to_mongo(st.session_state.pages, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                    if summary.get("errors"):
                        st.warning(f"Completed with errors: {len(summary['errors'])} (first: {summary['errors'][0]})")
                    st.success(f"Saved to MongoDB — inserted: {summary.get('inserted',0)}, updated: {summary.get('updated',0)}")
                except Exception as e:
                    st.error(f"Error saving to MongoDB: {e}")
        elif st.button("Save to MongoDB") and not mongo_uri:
            st.error("Please enter a MongoDB URI in the sidebar before saving.")

st.markdown("---")

# Batch queries textbox (main UI)
st.subheader("Batch search queries")
st.markdown("Enter one word or phrase per line. Press 'Run batch search' to execute searches for each line.")
batch_input = st.text_area("Queries (one per line)", height=120, placeholder="e.g.\ncontact\nabout us\nprivacy policy\nproduct features")

col_run, col_space = st.columns([1,4])
with col_run:
    run_batch = st.button("Run batch search")

if run_batch:
    if not batch_input.strip():
        st.warning("Please enter one or more queries (one per line).")
    elif not st.session_state.index:
        st.error("Index not built. Run a crawl first to build the index (or load from DB / build index).")
    else:
        queries = [line.strip() for line in batch_input.splitlines() if line.strip()]
        st.info(f"Running {len(queries)} queries...")
        all_results = {}
        for q in queries:
            # If the query contains spaces, treat as phrase search by default, otherwise token
            is_phrase = " " in q.strip()
            res = st.session_state.index.search(q, fields=["title","meta","text","alt"], phrase=is_phrase, max_results=200)
            all_results[q] = res
            st.markdown(f"#### Query: `{q}` — {len(res)} result(s)")
            if res:
                # show top 10 results
                df = pd.DataFrame(res[:10])
                st.dataframe(df)
            else:
                st.write("No results found.")

# Searching UI (single-query form)
st.subheader("Single query search")
if not st.session_state.pages:
    st.info("No pages indexed yet. Start a crawl to index pages.")
else:
    with st.form("search_form"):
        q = st.text_input("Search query (word or phrase)")
        phrase = st.checkbox("Treat query as phrase (substring)", value=False)
        cols = st.multiselect("Fields to search", options=["title","meta","text","alt"], default=["title","meta","text"])
        max_results = st.slider("Max results", min_value=10, max_value=1000, value=200, step=10)
        submitted = st.form_submit_button("Search")
    if submitted and q.strip():
        idx = st.session_state.index
        if not idx:
            st.error("Index not built yet.")
        else:
            with st.spinner("Searching..."):
                results = idx.search(q, fields=cols, phrase=phrase, max_results=max_results)
            st.success(f"Found {len(results)} result rows")
            # present results
            if results:
                df = pd.DataFrame(results)
                st.dataframe(df)
                # CSV export
                csv_buf = StringIO()
                df.to_csv(csv_buf, index=False)
                st.download_button("Download results CSV", data=csv_buf.getvalue(), file_name="search_results.csv", mime="text/csv")

                # per-page viewer: pick a result
                st.markdown("### Inspect a result")
                pick = st.selectbox("Choose a URL", options=[r["url"] for r in results])
                page = st.session_state.index.pages.get(pick)
                if page:
                    st.markdown(f"**Title:** {page.get('title')}")
                    st.markdown(f"**Meta description:** {page.get('meta')}")
                    st.markdown("---")
                    st.markdown("**Images (src / alt)**")
                    for img in page.get("images", []):
                        st.markdown(f"- {img.get('src')} — alt: {img.get('alt')}")
                    st.markdown("---")
                    # highlight matches in text (simple)
                    content = page.get("text", "") or ""
                    lowq = q.lower()
                    if phrase:
                        highlighted = content.replace(q, f"**{q}**")
                        st.markdown(highlighted[:5000] + ("..." if len(highlighted) > 5000 else ""))
                    else:
                        # highlight tokens
                        import re
                        tokens = [t.lower() for t in re.findall(r"\w[\w'-]*", q)]
                        display = content
                        for tkn in set(tokens):
                            display = re.sub(f"(?i)({re.escape(tkn)})", r"**\1**", display)
                        st.markdown(display[:5000] + ("..." if len(display) > 5000 else ""))

                    st.markdown(f"[Open original page]({page.get('url')})")
            else:
                st.warning("No matches found.")
