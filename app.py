import streamlit as st
import traceback
import sys

# Configure page early
st.set_page_config(page_title="Website Crawler & Search", layout="wide")

try:
    import os
    import pandas as pd
    from io import StringIO
    from crawler import Crawler
    from search_index import SearchIndex
    from db import save_pages_to_mongo, load_pages_from_mongo

    # --- Helper: get secrets ---
    mongo_secrets = st.secrets.get("mongo", {}) if hasattr(st, "secrets") else {}
    mongo_uri = mongo_secrets.get("uri") or os.getenv("MONGO_URI")
    mongo_db = mongo_secrets.get("db", "sitecrawler")
    mongo_collection = mongo_secrets.get("collection", "pages")

    google_secrets = st.secrets.get("google", {}) if hasattr(st, "secrets") else {}
    google_creds_json = google_secrets.get("credentials")  # optional JSON string

    # If Google creds are present in secrets and not already in env, write to temp file
    if google_creds_json and not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            creds_path = "/tmp/streamlit_google_creds.json"
            with open(creds_path, "w") as f:
                f.write(google_creds_json)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        except Exception:
            # don't call st.* here (we're still in setup); we'll show a warning later if needed
            pass

    # --- UI layout and state initialization ---
    st.title("Website Crawler & Search")
    st.markdown("Crawl a site (limited pages) and search titles, descriptions, body text, image alt tags and headings. Sentiment backend selectable (NLTK or Google NLP).")

    # Sidebar: Crawl settings
    st.sidebar.header("Crawl settings")
    start_url = st.sidebar.text_input("Start URL (including http:// or https://)", value="https://example.com")
    max_pages = st.sidebar.number_input("Max pages to crawl", min_value=1, max_value=2000, value=500, step=50)
    delay = st.sidebar.slider("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
    same_domain = st.sidebar.checkbox("Restrict to same domain", True)
    st.sidebar.markdown("Advanced")
    user_agent = st.sidebar.text_input("User-Agent header", value="site-crawler-bot/1.0")
    timeout = st.sidebar.number_input("Request timeout (s)", min_value=1, value=10)

    # Persistence / batch save
    st.sidebar.header("Persistence")
    batch_size = st.sidebar.number_input("Save to Mongo every N pages (batch size)", min_value=1, max_value=500, value=20, step=1)
    auto_save_mongo = st.sidebar.checkbox("Auto-save crawl to MongoDB after finishing", value=False)

    # Sentiment backend
    st.sidebar.header("Sentiment")
    sentiment_backend = st.sidebar.selectbox("Sentiment backend", options=["nltk", "google"], index=0)
    st.sidebar.caption("Google requires GOOGLE_APPLICATION_CREDENTIALS to be set or credentials provided via secrets.")

    # Initialize session state
    if "pages" not in st.session_state:
        st.session_state.pages = []
    if "index" not in st.session_state:
        st.session_state.index = None

    # Main controls
    col1, col2 = st.columns([2, 1])
    with col1:
        start_crawl = st.button("Start crawl")
        # note: stop button is still a placeholder in this synchronous version
        stop_crawl = st.button("Stop (not implemented)")
    with col2:
        st.write("Index status")
        if st.session_state.pages:
            st.write(f"Pages indexed: {len(st.session_state.pages)}")
        else:
            st.write("No pages indexed")

    progress_bar = st.progress(0)
    status_text = st.empty()
    log_area = st.empty()

    # Load from MongoDB button (in sidebar)
    st.sidebar.markdown("---")
    if st.sidebar.button("Load from MongoDB and build index"):
        if not mongo_uri:
            st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
        else:
            with st.spinner("Loading pages from MongoDB..."):
                try:
                    pages = load_pages_from_mongo(uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, limit=max_pages)
                    st.session_state.pages = pages
                    idx = SearchIndex()
                    idx.build(pages)
                    st.session_state.index = idx
                    st.sidebar.success(f"Loaded {len(pages)} pages and rebuilt in-memory index.")
                except Exception as e:
                    st.sidebar.error(f"Failed to load from MongoDB: {e}")

    # Crawl logic
    if start_crawl:
        # Validate start_url
        if not start_url or not start_url.startswith("http"):
            st.error("Please enter a valid http/https Start URL.")
        else:
            st.info("Starting crawl — this runs synchronously in the Streamlit session.")
            crawler = Crawler(
                start_url=start_url,
                max_pages=int(max_pages),
                delay=float(delay),
                same_domain=bool(same_domain),
                headers={"User-Agent": user_agent},
                timeout=int(timeout),
                sentiment_backend=sentiment_backend,
            )

            # incremental save buffer and stats (mutated by callback)
            save_buffer = []
            stats = {"inserted": 0, "updated": 0, "errors": []}

            # on_page callback MUST NOT call streamlit UI functions directly (avoid ScriptRunContext warnings)
            def on_page_callback(page):
                # Append page to session_state list and to save buffer
                st.session_state.pages.append(page)
                save_buffer.append(page)

                # Only mutate save_buffer and stats here (no st.* calls)
                if len(save_buffer) >= int(batch_size):
                    if mongo_uri:
                        try:
                            summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                            stats["inserted"] += summary.get("inserted", 0)
                            stats["updated"] += summary.get("updated", 0)
                            if summary.get("errors"):
                                stats["errors"].extend(summary.get("errors"))
                        except Exception as e:
                            stats["errors"].append({"error": str(e)})
                    # clear buffer regardless to avoid infinite growth
                    save_buffer.clear()

            def progress_cb(current, maximum, last_url):
                try:
                    progress = int((current / maximum) * 100)
                except Exception:
                    progress = 0
                progress_bar.progress(min(progress, 100))
                status_text.markdown(f"Crawled {current}/{maximum}: {last_url}")
                log_area.text(f"Crawled {current}/{maximum}: {last_url}")

            # reset session pages before crawl (unless you intentionally want to append)
            st.session_state.pages = []
            try:
                pages = crawler.crawl(progress_callback=progress_cb, on_page=on_page_callback)
            except Exception as e:
                st.error(f"Crawl failed with exception: {e}")
                # still attempt to use whatever was appended
                pages = st.session_state.pages

            # flush remaining buffer
            if save_buffer:
                if mongo_uri:
                    try:
                        summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                        stats["inserted"] += summary.get("inserted", 0)
                        stats["updated"] += summary.get("updated", 0)
                        if summary.get("errors"):
                            stats["errors"].extend(summary.get("errors"))
                    except Exception as e:
                        stats["errors"].append({"error": str(e)})
                save_buffer.clear()

            # ensure session_state.pages is set
            st.session_state.pages = pages or st.session_state.pages
            st.success(f"Finished crawling: {len(st.session_state.pages)} pages collected.")
            progress_bar.progress(100)

            # build index from pages (pages already include analysis fields)
            idx = SearchIndex()
            idx.build(st.session_state.pages)
            st.session_state.index = idx
            st.info("Built in-memory search index (includes headings and image alt tags).")

            # optional final save of all pages
            if auto_save_mongo:
                if not mongo_uri:
                    st.error("Auto-save requested but Mongo secrets not found. Set st.secrets['mongo']['uri'] or MONGO_URI.")
                else:
                    with st.spinner("Saving all pages to MongoDB (final save)..."):
                        try:
                            summary = save_pages_to_mongo(st.session_state.pages, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                            if summary.get("errors"):
                                st.warning(f"Completed with errors: {len(summary['errors'])} (first: {summary['errors'][0]})")
                            st.success(f"Saved to MongoDB — inserted: {summary.get('inserted',0)}, updated: {summary.get('updated',0)}")
                        except Exception as e:
                            st.error(f"Error saving to MongoDB: {e}")

            # show incremental save stats (from callback)
            st.write("Incremental save summary during crawl:")
            st.write(f"Total inserted (batches): {stats['inserted']}, total updated (batches): {stats['updated']}, batch errors: {len(stats['errors'])}")
            if stats["errors"]:
                st.write(stats["errors"][:10])

    # --- Batch queries textbox (main UI) ---
    st.markdown("---")
    st.subheader("Batch search queries")
    st.markdown("Enter one word or phrase per line. Press 'Run batch search' to execute searches for each line.")
    batch_input = st.text_area("Queries (one per line)", height=120, placeholder="e.g.\ncontact\nabout us\nprivacy policy\nproduct features")
    run_batch = st.button("Run batch search")

    if run_batch:
        if not batch_input.strip():
            st.warning("Please enter one or more queries (one per line).")
        elif not st.session_state.index:
            st.error("Index not built. Run a crawl first to build the index (or load from DB / build index).")
        else:
            queries = [line.strip() for line in batch_input.splitlines() if line.strip()]
            st.info(f"Running {len(queries)} queries...")
            for q in queries:
                is_phrase = " " in q.strip()
                res = st.session_state.index.search(q, fields=["title","meta","text","alt","headings"], phrase=is_phrase, max_results=200)
                st.markdown(f"#### Query: `{q}` — {len(res)} result(s)")
                if res:
                    df = pd.DataFrame(res[:20])
                    st.dataframe(df)
                else:
                    st.write("No results found.")

    # --- Single-query search UI ---
    st.markdown("---")
    st.subheader("Single query search")
    if not st.session_state.pages:
        st.info("No pages indexed yet. Start a crawl to index pages.")
    else:
        with st.form("search_form"):
            q = st.text_input("Search query (word or phrase)")
            phrase = st.checkbox("Treat query as phrase (substring)", value=False)
            cols = st.multiselect("Fields to search", options=["title","meta","text","alt","headings"], default=["title","meta","text"])
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
                if results:
                    df = pd.DataFrame(results)
                    st.dataframe(df)
                    csv_buf = StringIO()
                    df.to_csv(csv_buf, index=False)
                    st.download_button("Download results CSV", data=csv_buf.getvalue(), file_name="search_results.csv", mime="text/csv")

                    st.markdown("### Inspect a result")
                    pick = st.selectbox("Choose a URL", options=[r["url"] for r in results])
                    page = st.session_state.index.pages.get(pick)
                    if page:
                        st.markdown(f"**Title:** {page.get('title')} (length: {page.get('title_len')})")
                        st.markdown(f"**Meta description:** {page.get('meta')} (length: {page.get('meta_len')})")
                        st.markdown(f"**Main content length:** {page.get('content_len')}")
                        st.markdown("**Heading counts:**")
                        hcounts = page.get("h_counts", {})
                        for k in sorted(hcounts.keys()):
                            st.write(f"- {k}: {hcounts[k]}")
                        st.markdown("---")
                        st.markdown("**Images (src / alt)**")
                        for img in page.get("images", []):
                            st.write(f"- {img.get('src')} — alt: {img.get('alt')}")
                        st.markdown("---")
                        st.markdown("**Sentiment (details)**")
                        sent = page.get("sentiment", {})
                        st.write(sent)
                        st.markdown("---")
                        content = page.get("text", "") or ""
                        if phrase:
                            highlighted = content.replace(q, f"**{q}**")
                            st.markdown(highlighted[:5000] + ("..." if len(highlighted) > 5000 else ""))
                        else:
                            import re
                            tokens = [t.lower() for t in re.findall(r"\w[\w'-]*", q)]
                            display = content
                            for tkn in set(tokens):
                                display = re.sub(f"(?i)({re.escape(tkn)})", r"**\1**", display)
                            st.markdown(display[:5000] + ("..." if len(display) > 5000 else ""))
                        st.markdown(f"[Open original page]({page.get('url')})")
                else:
                    st.warning("No matches found.")

except Exception:
    tb = traceback.format_exc()
    try:
        st.error("An exception occurred during app startup. See traceback below:")
        st.text(tb)
    except Exception:
        # If Streamlit UI calls fail, print to stderr (check logs)
        print("Exception during app startup:\n", tb, file=sys.stderr)
