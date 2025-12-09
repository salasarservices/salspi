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

    import threading
    import queue
    import time

    # --- Secrets and credentials detection ---
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
            pass

    # Detect whether Google credentials are present (explicit file)
    google_creds_present = bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    # --- UI ---
    st.title("Website Crawler & Search")
    st.markdown("Crawl a site and search titles, descriptions, body text, image alt tags and headings. Select sentiment backend.")

    # Sidebar controls
    st.sidebar.header("Crawl settings")
    start_url = st.sidebar.text_input("Start URL (including http:// or https://)", value="https://example.com")
    max_pages = st.sidebar.number_input("Max pages to crawl", min_value=1, max_value=2000, value=50, step=10)
    delay = st.sidebar.slider("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
    same_domain = st.sidebar.checkbox("Restrict to same domain", True)
    st.sidebar.markdown("Advanced")
    user_agent = st.sidebar.text_input("User-Agent header", value="site-crawler-bot/1.0")
    timeout = st.sidebar.number_input("Request timeout (s)", min_value=1, value=10)

    st.sidebar.header("Persistence")
    batch_size = st.sidebar.number_input("Save to Mongo every N pages (batch size)", min_value=1, max_value=500, value=20, step=1)
    auto_save_mongo = st.sidebar.checkbox("Auto-save crawl to MongoDB after finishing", value=False)

    st.sidebar.header("Sentiment")
    sentiment_backend = st.sidebar.selectbox("Sentiment backend", options=["nltk", "google"], index=0)
    if sentiment_backend == "google" and not google_creds_present:
        st.sidebar.warning("Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or st.secrets['google']['credentials'] to use Google NLP.")
    st.sidebar.caption("Google requires GOOGLE_APPLICATION_CREDENTIALS to be set or credentials provided via secrets.")

    # Session state init
    if "pages" not in st.session_state:
        st.session_state.pages = []
    if "index" not in st.session_state:
        st.session_state.index = None

    # Buttons and status widgets
    col1, col2 = st.columns([2, 1])
    with col1:
        start_crawl = st.button("Start crawl")
        stop_crawl = st.button("Stop (not implemented)")
    with col2:
        st.write("Index status")
        if st.session_state.pages:
            st.write(f"Pages indexed: {len(st.session_state.pages)}")
        else:
            st.write("No pages indexed")

    # Use dedicated placeholders for real-time updates
    progress_placeholder = st.empty()
    progress_bar = progress_placeholder.progress(0)
    status_text = st.empty()
    log_area = st.empty()
    # A small area for a running indicator/ETA
    running_info = st.empty()

    # Load from Mongo
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

    # Helper for ETA calculation (simple)
    def eta_text(start_ts, processed, total):
        if processed <= 0:
            return "ETA: calculating..."
        elapsed = time.time() - start_ts
        per_page = elapsed / processed
        remaining = max(0, total - processed)
        eta_secs = per_page * remaining
        # human readable
        if eta_secs < 60:
            return f"ETA: {int(eta_secs)}s"
        else:
            mins = int(eta_secs // 60)
            secs = int(eta_secs % 60)
            return f"ETA: {mins}m {secs}s"

    # Start crawl: validate Google credential case and prevent metadata calls
    if start_crawl:
        if sentiment_backend == "google" and not google_creds_present:
            st.error(
                "Google NLP selected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
                "Please set credentials in the environment or st.secrets['google']['credentials']."
            )
        elif not start_url or not start_url.startswith("http"):
            st.error("Please enter a valid http/https Start URL.")
        else:
            st.info("Starting crawl — runs in a background thread; UI will update in real time.")

            # Setup queue and thread
            q = queue.Queue()
            crawler = Crawler(
                start_url=start_url,
                max_pages=int(max_pages),
                delay=float(delay),
                same_domain=bool(same_domain),
                headers={"User-Agent": user_agent},
                timeout=int(timeout),
                sentiment_backend=sentiment_backend,
            )

            def on_page_threadsafe(page):
                # the crawler thread places page events on the queue
                q.put({"type": "page", "page": page})

            def crawl_worker():
                try:
                    crawler.crawl(progress_callback=None, on_page=on_page_threadsafe)
                    q.put({"type": "done"})
                except Exception as e:
                    q.put({"type": "error", "error": str(e)})
                    q.put({"type": "done"})

            t = threading.Thread(target=crawl_worker, daemon=True)
            # reset pages in session_state, start time for ETA
            st.session_state.pages = []
            start_ts = time.time()
            t.start()

            inserted_total = 0
            updated_total = 0
            save_errors = []
            save_buffer = []

            # Poll queue loop: updates placed widgets on main thread (real-time)
            while t.is_alive() or not q.empty():
                # Drain queue
                while not q.empty():
                    item = q.get_nowait()
                    if item["type"] == "page":
                        page = item["page"]
                        st.session_state.pages.append(page)
                        current = len(st.session_state.pages)
                        # update progress bar and status
                        try:
                            percent = int((current / float(max_pages)) * 100)
                        except Exception:
                            percent = 0
                        progress_bar.progress(min(percent, 100))
                        status_text.markdown(f"Crawled {current}/{max_pages}: {page.get('url')}")
                        log_area.text(f"Last: {page.get('url')}  |  Title: {page.get('title','')}")
                        running_info.text(eta_text(start_ts, current, max_pages))

                        # batch save logic on main thread
                        save_buffer.append(page)
                        if len(save_buffer) >= int(batch_size):
                            if mongo_uri:
                                try:
                                    summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                                    inserted_total += summary.get("inserted", 0)
                                    updated_total += summary.get("updated", 0)
                                    if summary.get("errors"):
                                        save_errors.extend(summary.get("errors"))
                                except Exception as e:
                                    save_errors.append({"error": str(e)})
                            save_buffer.clear()

                    elif item["type"] == "error":
                        st.error(f"Crawl thread error: {item.get('error')}")
                    elif item["type"] == "done":
                        # final marker; we'll break out after draining queue
                        pass

                # Yield to Streamlit so widget updates are rendered in browser
                # short sleep ensures UI remains responsive and updates display
                time.sleep(0.15)

            # flush any remaining buffer after crawl finishes
            if save_buffer:
                if mongo_uri:
                    try:
                        summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                        inserted_total += summary.get("inserted", 0)
                        updated_total += summary.get("updated", 0)
                        if summary.get("errors"):
                            save_errors.extend(summary.get("errors"))
                    except Exception as e:
                        save_errors.append({"error": str(e)})
                save_buffer.clear()

            # Build index from the collected pages
            idx = SearchIndex()
            idx.build(st.session_state.pages)
            st.session_state.index = idx

            progress_bar.progress(100)
            running_info.empty()
            st.success(f"Finished crawling: {len(st.session_state.pages)} pages collected.")
            st.write("Incremental save summary during crawl:")
            st.write(f"Total inserted (batches): {inserted_total}, total updated (batches): {updated_total}, batch errors: {len(save_errors)}")
            if save_errors:
                st.write(save_errors[:10])

            # Optional final full save
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

    # --- Batch queries UI ---
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
            for q_line in queries:
                is_phrase = " " in q_line.strip()
                res = st.session_state.index.search(q_line, fields=["title","meta","text","alt","headings"], phrase=is_phrase, max_results=200)
                st.markdown(f"#### Query: `{q_line}` — {len(res)} result(s)")
                if res:
                    df = pd.DataFrame(res[:20])
                    st.dataframe(df)
                else:
                    st.write("No results found.")

    # --- Single query UI ---
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

except Exception:
    tb = traceback.format_exc()
    try:
        st.error("An exception occurred during app startup. See traceback below:")
        st.text(tb)
    except Exception:
        print("Exception during app startup:\n", tb, file=sys.stderr)
