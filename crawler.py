import streamlit as st
import traceback
import sys

# Configure page early
st.set_page_config(page_title="Website Crawler & Search", layout="wide")

try:
    import os
    import threading
    import queue
    import time
    import re
    import pandas as pd
    from io import StringIO

    # App modules
    from crawler import Crawler
    from search_index import SearchIndex
    from db import (
        save_pages_to_mongo,
        save_ocr_to_mongo,
        load_pages_from_mongo,
        get_mongo_client,
    )

    # --- Config ---
    MAX_PAGES = 5000
    BATCH_SAVE_SIZE = 50
    DEFAULT_SEARCH_FIELDS = ["title", "meta", "text", "alt", "headings", "ocr"]

    # --- Helpers ---
    def eta_text(start_ts, processed, total):
        if processed <= 0 or not start_ts:
            return "ETA: calculating..."
        elapsed = time.time() - start_ts
        per_page = elapsed / processed
        remaining = max(0, total - processed)
        eta_secs = per_page * remaining
        if eta_secs < 60:
            return f"ETA: {int(eta_secs)}s"
        else:
            mins = int(eta_secs // 60)
            secs = int(eta_secs % 60)
            return f"ETA: {mins}m {secs}s"

    # --- Secrets / env ---
    mongo_secrets = st.secrets.get("mongo", {}) if hasattr(st, "secrets") else {}
    mongo_uri = mongo_secrets.get("uri") or os.getenv("MONGO_URI")
    mongo_db = mongo_secrets.get("db", "sitecrawler")
    mongo_collection = mongo_secrets.get("collection", "pages")

    google_secrets = st.secrets.get("google", {}) if hasattr(st, "secrets") else {}
    google_creds_json = google_secrets.get("credentials")
    if google_creds_json and not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        try:
            creds_path = "/tmp/streamlit_google_creds.json"
            with open(creds_path, "w") as f:
                f.write(google_creds_json)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        except Exception:
            pass
    google_creds_present = bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    # --- session_state defaults (must run before any access) ---
    defaults = {
        "pages": [],
        "index": None,
        "crawl_thread": None,
        "crawl_queue": None,
        "crawl_running": False,
        "crawl_start_ts": None,
        "crawl_stats": {"inserted": 0, "updated": 0, "ocr_inserted": 0, "ocr_updated": 0, "errors": []},
        "last_thread_error": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

    # If previous thread object present but dead, reset running flag
    stored_thread = st.session_state.get("crawl_thread")
    if stored_thread is None or not getattr(stored_thread, "is_alive", lambda: False)():
        st.session_state["crawl_running"] = False

    # --- UI header ---
    st.title("Website Crawler & Search")
    st.markdown(
        "Crawl a site (up to 5000 pages) and search titles, descriptions, body text, image alt tags, OCR text and headings. "
        "Single-query search only (search runs across all indexed fields)."
    )

    # --- Sidebar: settings ---
    st.sidebar.header("Crawl settings")
    start_url = st.sidebar.text_input("Start URL (including http:// or https://)", value="https://example.com")
    delay = st.sidebar.slider("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
    same_domain = st.sidebar.checkbox("Restrict to same domain", True)
    st.sidebar.markdown("Advanced")
    user_agent = st.sidebar.text_input("User-Agent header", value="site-crawler-bot/1.0")
    timeout = st.sidebar.number_input("Request timeout (s)", min_value=1, value=10)

    st.sidebar.header("Sentiment")
    sentiment_backend = st.sidebar.selectbox("Sentiment backend", options=["nltk", "google"], index=0)
    if sentiment_backend == "google" and not google_creds_present:
        st.sidebar.warning("Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or st.secrets['google']['credentials'] to use Google NLP.")
    st.sidebar.caption("Google requires GOOGLE_APPLICATION_CREDENTIALS or secrets.")

    # Mongo actions
    st.sidebar.markdown("---")
    st.sidebar.header("MongoDB actions")
    if st.sidebar.button("Refresh MongoDB data"):
        if not mongo_uri:
            st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
        else:
            with st.spinner("Loading pages from MongoDB..."):
                try:
                    pages = load_pages_from_mongo(uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, limit=MAX_PAGES)
                    st.session_state["pages"] = pages
                    idx = SearchIndex()
                    idx.build(pages)
                    st.session_state["index"] = idx
                    st.sidebar.success(f"Loaded {len(pages)} pages and rebuilt in-memory index.")
                except Exception as e:
                    st.sidebar.error(f"Failed to refresh from MongoDB: {e}")

    st.sidebar.markdown("**Delete MongoDB data (pages + ocr-data)**")
    confirm_delete_text = st.sidebar.text_input("Type DELETE to enable delete button", key="confirm_delete_text")
    if confirm_delete_text == "DELETE":
        if st.sidebar.button("Confirm DELETE collections"):
            if not mongo_uri:
                st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
            else:
                try:
                    client = get_mongo_client(mongo_uri)
                    db = client[mongo_db]
                    dropped = []
                    for coll_name in [mongo_collection, "ocr-data"]:
                        if coll_name in db.list_collection_names():
                            db.drop_collection(coll_name)
                            dropped.append(coll_name)
                    client.close()
                    st.sidebar.success(f"Dropped collections: {', '.join(dropped) if dropped else 'none found'}")
                    st.session_state["pages"] = []
                    st.session_state["index"] = None
                    st.session_state["crawl_stats"] = defaults["crawl_stats"].copy()
                except Exception as e:
                    st.sidebar.error(f"Failed to delete collections: {e}")
    else:
        st.sidebar.info("Enter DELETE to enable the collection delete button.")

    if st.sidebar.button("Refresh app & clear cache"):
        try:
            if hasattr(st, "cache_data") and hasattr(st.cache_data, "clear"):
                st.cache_data.clear()
        except Exception:
            pass
        try:
            if hasattr(st, "cache_resource") and hasattr(st.cache_resource, "clear"):
                st.cache_resource.clear()
        except Exception:
            pass
        # clear session state except secrets
        keys = list(st.session_state.keys())
        for k in keys:
            try:
                del st.session_state[k]
            except Exception:
                pass
        st.experimental_rerun() if hasattr(st, "experimental_rerun") else None

    # --- Main controls ---
    col1, col2 = st.columns([2, 1])
    with col1:
        start_btn = st.button("Start crawl", disabled=st.session_state.get("crawl_running", False))
        stop_btn = st.button("Stop (not implemented)")
    with col2:
        st.write("Index status")
        if st.session_state.get("pages"):
            st.write(f"Pages indexed: {len(st.session_state.get('pages', []))} (in-memory)")
        else:
            st.write("No pages indexed")

    # Real-time progress placeholders
    st.markdown("### Crawl progress (real time)")
    pa_col1, pa_col2 = st.columns([3, 2])
    with pa_col1:
        progress_ph = st.empty()
        progress_bar = progress_ph.progress(0)
        percent_ph = st.empty()
        percent_ph.markdown("Completion: 0%")
    with pa_col2:
        m1, m2 = st.columns(2)
        pages_crawled_ph = m1.empty()
        pages_saved_ph = m2.empty()
        m3, m4 = st.columns(2)
        ocr_saved_ph = m3.empty()
        errors_ph = m4.empty()

    status_text = st.empty()
    log_area = st.empty()
    running_info = st.empty()

    # --- Start crawl: create worker that writes only to a local queue ---
    def start_crawl_background():
        local_q = queue.Queue()
        st.session_state["crawl_queue"] = local_q
        st.session_state["crawl_start_ts"] = time.time()
        st.session_state["crawl_stats"] = {"inserted": 0, "updated": 0, "ocr_inserted": 0, "ocr_updated": 0, "errors": []}
        st.session_state["last_thread_error"] = None

        crawler = Crawler(
            start_url=start_url,
            max_pages=MAX_PAGES,
            delay=float(delay),
            same_domain=bool(same_domain),
            headers={"User-Agent": user_agent},
            timeout=int(timeout),
            sentiment_backend=sentiment_backend,
            ocr_enabled=True,
        )

        def on_page_local(page):
            try:
                local_q.put({"type": "page", "page": page})
            except Exception:
                pass

        def worker():
            try:
                crawler.crawl(progress_callback=None, on_page=on_page_local)
                try:
                    local_q.put({"type": "done"})
                except Exception:
                    pass
            except Exception:
                tb = traceback.format_exc()
                try:
                    local_q.put({"type": "error", "error": tb})
                    local_q.put({"type": "done"})
                except Exception:
                    pass

        t = threading.Thread(target=worker, daemon=True)
        st.session_state["crawl_thread"] = t
        st.session_state["crawl_running"] = True
        t.start()

    # If user pressed Start
    if start_btn:
        if not st.session_state.get("crawl_running", False):
            if not start_url or not start_url.startswith("http"):
                st.error("Please enter a valid http/https Start URL.")
            else:
                start_crawl_background()
                st.info("Starting crawl — runs in background; indexing up to %d pages." % MAX_PAGES)

    # --- Blocking main-thread polling loop (keeps UI updating in real time) ---
    # This loop runs in the current Streamlit script execution and keeps updating the UI
    q = st.session_state.get("crawl_queue")
    t = st.session_state.get("crawl_thread")
    stats = st.session_state.get("crawl_stats", {"inserted": 0, "updated": 0, "ocr_inserted": 0, "ocr_updated": 0, "errors": []})
    save_buffer = []

    # Only run poll loop if a thread was started (or queue exists from earlier)
    if q is not None and st.session_state.get("crawl_running", False):
        # Poll until worker finishes and queue is drained
        while True:
            drained = False
            while not q.empty():
                drained = True
                try:
                    item = q.get_nowait()
                except Exception:
                    break

                if item.get("type") == "page":
                    page = item.get("page")
                    st.session_state["pages"].append(page)
                    current = len(st.session_state["pages"])

                    percent = int((current / float(MAX_PAGES)) * 100) if MAX_PAGES > 0 else 100
                    percent = max(0, min(100, percent))
                    progress_bar.progress(percent)
                    percent_ph.markdown(f"Completion: {percent}%")

                    pages_crawled_ph.metric("Pages crawled", f"{current}")
                    pages_saved_ph.metric("Pages saved", f"{stats.get('inserted',0) + stats.get('updated',0)}")
                    ocr_saved_ph.metric("OCR docs saved", f"{stats.get('ocr_inserted',0) + stats.get('ocr_updated',0)}")
                    errors_ph.metric("Batch errors", f"{len(stats.get('errors', []))}")

                    status_text.markdown(f"Crawled {current}/{MAX_PAGES}: {page.get('url')}")
                    log_area.text(f"Last: {page.get('url')}  |  Title: {page.get('title','')}")
                    running_info.text(eta_text(st.session_state.get("crawl_start_ts"), current, MAX_PAGES))

                    # Batch save
                    save_buffer.append(page)
                    if len(save_buffer) >= BATCH_SAVE_SIZE:
                        if mongo_uri:
                            try:
                                summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                                stats["inserted"] += summary.get("inserted", 0)
                                stats["updated"] += summary.get("updated", 0)
                                if summary.get("errors"):
                                    stats["errors"].extend(summary.get("errors"))
                            except Exception as e:
                                stats["errors"].append({"error": str(e)})
                            try:
                                ocr_summary = save_ocr_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name="ocr-data", upsert=True)
                                stats["ocr_inserted"] += ocr_summary.get("inserted", 0)
                                stats["ocr_updated"] += ocr_summary.get("updated", 0)
                                if ocr_summary.get("errors"):
                                    stats["errors"].extend(ocr_summary.get("errors"))
                            except Exception as e:
                                stats["errors"].append({"ocr_error": str(e)})
                        save_buffer.clear()
                        st.session_state["crawl_stats"] = stats

                elif item.get("type") == "error":
                    errtxt = item.get("error")
                    st.error("Crawl thread error (see traceback):")
                    st.code(errtxt)
                    st.session_state["last_thread_error"] = errtxt
                    stats["errors"].append({"thread_error": errtxt})
                    st.session_state["crawl_stats"] = stats
                elif item.get("type") == "done":
                    # worker signaled completion; we still need to flush queue items then break
                    pass

            # If worker thread alive, continue polling. If not alive and queue empty, break.
            thread_alive = st.session_state.get("crawl_thread") is not None and st.session_state.get("crawl_thread").is_alive()
            if not thread_alive and q.empty():
                break

            # Small sleep to allow the browser to receive updates and avoid tight-loop CPU usage
            time.sleep(0.12)

        # flush any remaining save_buffer after loop completes
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
                try:
                    ocr_summary = save_ocr_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name="ocr-data", upsert=True)
                    stats["ocr_inserted"] += ocr_summary.get("inserted", 0)
                    stats["ocr_updated"] += ocr_summary.get("updated", 0)
                    if ocr_summary.get("errors"):
                        stats["errors"].extend(ocr_summary.get("errors"))
                except Exception as e:
                    stats["errors"].append({"ocr_error": str(e)})
            save_buffer.clear()
            st.session_state["crawl_stats"] = stats

        # Build index, final UI update and cleanup
        if st.session_state.get("pages"):
            idx = SearchIndex()
            idx.build(st.session_state.get("pages"))
            st.session_state["index"] = idx

        total = len(st.session_state.get("pages", []))
        final_pct = int((total / float(MAX_PAGES)) * 100) if MAX_PAGES > 0 else 100
        final_pct = max(0, min(100, final_pct))
        progress_bar.progress(final_pct)
        percent_ph.markdown(f"Completion: {final_pct}%")
        pages_crawled_ph.metric("Pages crawled", f"{total}")
        pages_saved_ph.metric("Pages saved", f"{stats.get('inserted',0) + stats.get('updated',0)}")
        ocr_saved_ph.metric("OCR docs saved", f"{stats.get('ocr_inserted',0) + stats.get('ocr_updated',0)}")
        errors_ph.metric("Batch errors", f"{len(stats.get('errors', []))}")

        st.session_state["crawl_running"] = False
        st.session_state["crawl_thread"] = None
        st.session_state["crawl_queue"] = None
        st.session_state["crawl_start_ts"] = None
        st.session_state["crawl_stats"] = stats

    # --- Single-query search UI ---
    st.markdown("---")
    st.subheader("Single query search")
    if not st.session_state.get("pages"):
        st.info("No pages indexed yet. Start a crawl to index pages.")
        st.text_input("Search query (word or phrase)", value="", disabled=True)
    else:
        with st.form("search_form"):
            search_q = st.text_input("Search query (word or phrase)")
            submitted = st.form_submit_button("Search")
        if submitted and search_q and search_q.strip():
            if not st.session_state.get("index"):
                st.error("Index not built yet.")
            else:
                with st.spinner("Searching..."):
                    results = st.session_state["index"].search(search_q, fields=DEFAULT_SEARCH_FIELDS, phrase=(" " in search_q.strip()), max_results=500)
                st.success(f"Found {len(results)} result rows")
                if results:
                    df = pd.DataFrame(results)
                    st.dataframe(df)
                    csv_buf = StringIO()
                    df.to_csv(csv_buf, index=False)
                    st.download_button("Download results CSV", data=csv_buf.getvalue(), file_name="search_results.csv", mime="text/csv")

                    st.markdown("### Inspect a result")
                    pick = st.selectbox("Choose a URL", options=[r["url"] for r in results])
                    page = st.session_state["index"].pages.get(pick)
                    if page:
                        st.markdown(f"**Title:** {page.get('title')} (length: {page.get('title_len')})")
                        st.markdown(f"**Meta description:** {page.get('meta')} (length: {page.get('meta_len')})")
                        st.markdown(f"**Main content length:** {page.get('content_len')}")
                        st.markdown("**Heading counts:**")
                        hcounts = page.get("h_counts", {})
                        for k in sorted(hcounts.keys()):
                            st.write(f"- {k}: {hcounts[k]}")
                        st.markdown("---")
                        st.markdown("**Images (src / alt / ocr_text)**")
                        for img in page.get("ocr_details", []):
                            st.write(f"- {img.get('src')} — alt: {img.get('alt')}")
                            if img.get("ocr_text"):
                                st.write("  OCR text (trimmed):", (img.get("ocr_text")[:300] + "...") if len(img.get("ocr_text"))>300 else img.get("ocr_text"))
                            if img.get("ocr_error"):
                                st.write("  OCR error:", img.get("ocr_error"))
                        st.markdown("---")
                        st.markdown("**Aggregated OCR text (page-level)**")
                        st.write((page.get("ocr_text")[:1000] + "...") if len(page.get("ocr_text",""))>1000 else page.get("ocr_text",""))
                        st.markdown("---")
                        st.markdown("**Sentiment (details)**")
                        sent = page.get("sentiment", {})
                        st.write(sent)
                        st.markdown("---")
                        content = page.get("text", "") or ""
                        if " " in search_q.strip():
                            highlighted = content.replace(search_q, f"**{search_q}**")
                            st.markdown(highlighted[:5000] + ("..." if len(highlighted) > 5000 else ""))
                        else:
                            tokens = [t.lower() for t in re.findall(r"\w[\w'-]*", search_q)]
                            display = content
                            for tkn in set(tokens):
                                display = re.sub(f"(?i)({re.escape(tkn)})", r"**\1**", display)
                            st.markdown(display[:5000] + ("..." if len(display) > 5000 else ""))

except Exception:
    tb = traceback.format_exc()
    try:
        st.error("An exception occurred during app startup. See traceback below:")
        st.text(tb)
    except Exception:
        print("Exception during app startup:\n", tb, file=sys.stderr)
