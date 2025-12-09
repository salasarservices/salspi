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
    from db import (
        save_pages_to_mongo,
        save_ocr_to_mongo,
        load_pages_from_mongo,
        get_mongo_client,
    )

    import threading
    import queue
    import time
    import re

    # --- Constants / Defaults ---
    MAX_PAGES = 5000  # hard limit per your request
    BATCH_SAVE_SIZE = 50  # internal batch save size (no UI exposed)
    DEFAULT_SEARCH_FIELDS = ["title", "meta", "text", "alt", "headings", "ocr"]

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
    st.markdown(
        "Crawl a site (up to 5000 pages) and search titles, descriptions, body text, image alt tags, OCR text and headings. "
        "Single-query search only (search runs across all indexed fields)."
    )

    # Sidebar controls (only crawl/settings & sentiment)
    st.sidebar.header("Crawl settings")
    start_url = st.sidebar.text_input(
        "Start URL (including http:// or https://)", value="https://example.com"
    )
    delay = st.sidebar.slider(
        "Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1
    )
    same_domain = st.sidebar.checkbox("Restrict to same domain", True)
    st.sidebar.markdown("Advanced")
    user_agent = st.sidebar.text_input("User-Agent header", value="site-crawler-bot/1.0")
    timeout = st.sidebar.number_input("Request timeout (s)", min_value=1, value=10)

    st.sidebar.header("Sentiment")
    sentiment_backend = st.sidebar.selectbox(
        "Sentiment backend", options=["nltk", "google"], index=0
    )
    if sentiment_backend == "google" and not google_creds_present:
        st.sidebar.warning(
            "Google credentials not found. Set GOOGLE_APPLICATION_CREDENTIALS or st.secrets['google']['credentials'] to use Google NLP."
        )
    st.sidebar.caption(
        "Google requires GOOGLE_APPLICATION_CREDENTIALS to be set or credentials provided via secrets."
    )

    # --- New: MongoDB actions & app controls (left sidebar) ---
    st.sidebar.markdown("---")
    st.sidebar.header("MongoDB actions")

    # Refresh MongoDB data (load pages from mongo and rebuild index)
    if st.sidebar.button("Refresh MongoDB data"):
        if not mongo_uri:
            st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
        else:
            with st.spinner("Refreshing pages from MongoDB and rebuilding index..."):
                try:
                    pages = load_pages_from_mongo(
                        uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, limit=MAX_PAGES
                    )
                    st.session_state.pages = pages
                    idx = SearchIndex()
                    idx.build(pages)
                    st.session_state.index = idx
                    st.sidebar.success(f"Loaded {len(pages)} pages and rebuilt in-memory index.")
                except Exception as e:
                    st.sidebar.error(f"Failed to refresh from MongoDB: {e}")

    # Delete MongoDB data (two-step confirmation)
    st.sidebar.markdown("**Delete MongoDB data (pages + ocr-data)**")
    delete_clicked = st.sidebar.button("Delete MongoDB data")
    if delete_clicked:
        st.sidebar.warning("This will permanently delete the 'pages' and 'ocr-data' collections.")
        confirm_delete = st.sidebar.text_input(
            "Type DELETE to confirm permanent removal of those collections", value=""
        )
        if confirm_delete == "DELETE":
            if not mongo_uri:
                st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
            else:
                try:
                    client = get_mongo_client(mongo_uri)
                    db = client[mongo_db]
                    # drop collections if they exist
                    dropped = []
                    for coll_name in [mongo_collection, "ocr-data"]:
                        if coll_name in db.list_collection_names():
                            db.drop_collection(coll_name)
                            dropped.append(coll_name)
                    client.close()
                    st.sidebar.success(f"Dropped collections: {', '.join(dropped) if dropped else 'none found'}")
                    # clear in-memory index as well
                    st.session_state.pages = []
                    st.session_state.index = None
                except Exception as e:
                    st.sidebar.error(f"Failed to delete collections: {e}")
        else:
            st.sidebar.info("Type DELETE in the box above and press Enter to confirm deletion.")

    st.sidebar.markdown("---")
    # Refresh app & clear cache button
    if st.sidebar.button("Refresh app & clear cache"):
        # clear Streamlit caches if available
        try:
            # new API
            if hasattr(st, "cache_data") and hasattr(st.cache_data, "clear"):
                st.cache_data.clear()
        except Exception:
            pass
        try:
            if hasattr(st, "cache_resource") and hasattr(st.cache_resource, "clear"):
                st.cache_resource.clear()
        except Exception:
            pass
        # clear session state
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        # re-run to refresh UI
        st.experimental_rerun()

    # --- Session state init (ensure keys exist) ---
    if "pages" not in st.session_state:
        st.session_state.pages = []
    if "index" not in st.session_state:
        st.session_state.index = None

    # Buttons and status widgets on main area
    col1, col2 = st.columns([2, 1])
    with col1:
        start_crawl = st.button("Start crawl")
        stop_crawl = st.button("Stop (not implemented)")
    with col2:
        st.write("Index status")
        if st.session_state.pages:
            st.write(f"Pages indexed: {len(st.session_state.pages)} (showing in-memory index)")
        else:
            st.write("No pages indexed")

    # Real-time progress area: progress bar, percent text, numeric metrics
    st.markdown("### Crawl progress (real time)")
    progress_area_col1, progress_area_col2 = st.columns([3, 2])

    with progress_area_col1:
        progress_bar_ph = st.empty()
        progress_bar = progress_bar_ph.progress(0)
        percent_text_ph = st.empty()
        percent_text_ph.markdown("Completion: 0%")

    with progress_area_col2:
        metrics_cols = st.columns(2)
        pages_crawled_ph = metrics_cols[0].empty()
        pages_saved_ph = metrics_cols[1].empty()
        # Another row for OCR and errors
        metrics_cols2 = st.columns(2)
        ocr_saved_ph = metrics_cols2[0].empty()
        errors_ph = metrics_cols2[1].empty()

    # Additional status logs and ETA
    status_text = st.empty()
    log_area = st.empty()
    running_info = st.empty()

    # Load from MongoDB and build index (utility) - kept as alternative
    st.sidebar.markdown("---")
    if st.sidebar.button("Load from MongoDB and build index"):
        if not mongo_uri:
            st.sidebar.error("Mongo URI not found in secrets or environment (MONGO_URI).")
        else:
            with st.spinner("Loading pages from MongoDB..."):
                try:
                    pages = load_pages_from_mongo(uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, limit=MAX_PAGES)
                    st.session_state.pages = pages
                    idx = SearchIndex()
                    idx.build(pages)
                    st.session_state.index = idx
                    st.sidebar.success(f"Loaded {len(pages)} pages and rebuilt in-memory index.")
                except Exception as e:
                    st.sidebar.error(f"Failed to load from MongoDB: {e}")

    # Helper for ETA calculation
    def eta_text(start_ts, processed, total):
        if processed <= 0:
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

    # Start crawl: validate Google credential case
    if start_crawl:
        if sentiment_backend == "google" and not google_creds_present:
            st.error(
                "Google NLP selected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
                "Please set credentials in the environment or st.secrets['google']['credentials']."
            )
        elif not start_url or not start_url.startswith("http"):
            st.error("Please enter a valid http/https Start URL.")
        else:
            st.info(f"Starting crawl — runs in a background thread; indexing up to {MAX_PAGES} pages and saving OCR to Mongo if configured.")

            # Setup queue and thread
            q = queue.Queue()
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

            def on_page_threadsafe(page):
                # background thread places page events on the queue
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
            ocr_inserted = 0
            ocr_updated = 0
            save_errors = []
            save_buffer = []

            # --- Replace your current poll loop with this block ---
# t is the background thread; q is queue.Queue populated by the crawler thread
inserted_total = 0
updated_total = 0
ocr_inserted = 0
ocr_updated = 0
save_errors = []
save_buffer = []
start_ts = time.time()

# Short helper to force safe widget updates (keeps UI responsive)
def update_ui_counts(current, inserted_total, updated_total, ocr_inserted, ocr_updated, save_errors, start_ts):
    percent = int((current / float(MAX_PAGES)) * 100)
    percent = max(0, min(100, percent))
    progress_bar.progress(percent)
    percent_text_ph.markdown(f"Completion: {percent}%")
    pages_crawled_ph.metric("Pages crawled", f"{current}")
    pages_saved_ph.metric("Pages saved", f"{inserted_total + updated_total}")
    ocr_saved_ph.metric("OCR docs saved", f"{ocr_inserted + ocr_updated}")
    errors_ph.metric("Batch errors", f"{len(save_errors)}")
    running_info.text(eta_text(start_ts, current, MAX_PAGES))

# Poll the queue and update UI in small steps.
# This loop yields frequently so Streamlit can send updates to the browser.
while t.is_alive() or not q.empty():
    # Drain queue completely before sleeping (fast)
    drained = False
    while not q.empty():
        drained = True
        item = q.get_nowait()
        if item["type"] == "page":
            page = item["page"]
            st.session_state.pages.append(page)
            current = len(st.session_state.pages)

            # immediate UI update
            status_text.markdown(f"Crawled {current}/{MAX_PAGES}: {page.get('url')}")
            log_area.text(f"Last: {page.get('url')}  |  Title: {page.get('title','')}")
            update_ui_counts(current, inserted_total, updated_total, ocr_inserted, ocr_updated, save_errors, start_ts)

            # append to buffer for DB writes
            save_buffer.append(page)
            if len(save_buffer) >= BATCH_SAVE_SIZE:
                if mongo_uri:
                    try:
                        summary = save_pages_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name=mongo_collection, upsert=True)
                        inserted_total += summary.get("inserted", 0)
                        updated_total += summary.get("updated", 0)
                        if summary.get("errors"):
                            save_errors.extend(summary.get("errors"))
                    except Exception as e:
                        save_errors.append({"error": str(e)})
                    try:
                        ocr_summary = save_ocr_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name="ocr-data", upsert=True)
                        ocr_inserted += ocr_summary.get("inserted", 0)
                        ocr_updated += ocr_summary.get("updated", 0)
                        if ocr_summary.get("errors"):
                            save_errors.extend(ocr_summary.get("errors"))
                    except Exception as e:
                        save_errors.append({"ocr_error": str(e)})
                save_buffer.clear()
                # update counts after write
                update_ui_counts(len(st.session_state.pages), inserted_total, updated_total, ocr_inserted, ocr_updated, save_errors, start_ts)

        elif item["type"] == "error":
            st.error(f"Crawl thread error: {item.get('error')}")
        elif item["type"] == "done":
            # nothing special — loop will exit when t.is_alive() is False and queue empty
            pass

    # Important: small sleep so Streamlit can push updates to browser and keep animation fluid.
    # This is the critical bit — keep this value small (0.1..0.25 sec).
    if not drained:
        # if nothing new, sleep a tiny bit
        time.sleep(0.12)
    else:
        # if we drained items, yield briefly to make UI draw changes
        time.sleep(0.08)

# After thread completes, flush any remaining save_buffer (same as before)
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
        try:
            ocr_summary = save_ocr_to_mongo(save_buffer, uri=mongo_uri, db_name=mongo_db, collection_name="ocr-data", upsert=True)
            ocr_inserted += ocr_summary.get("inserted", 0)
            ocr_updated += ocr_summary.get("updated", 0)
            if ocr_summary.get("errors"):
                save_errors.extend(ocr_summary.get("errors"))
        except Exception as e:
            save_errors.append({"ocr_error": str(e)})
    save_buffer.clear()

# Final UI update
total = len(st.session_state.pages)
update_ui_counts(total, inserted_total, updated_total, ocr_inserted, ocr_updated, save_errors, start_ts)
progress_bar.progress(min(int((total/float(MAX_PAGES))*100), 100))
percent_text_ph.markdown(f"Completion: {int((total/float(MAX_PAGES))*100)}%")

                        # update progress percentage and bar
                        percent = int((current / float(MAX_PAGES)) * 100)
                        percent = max(0, min(100, percent))
                        progress_bar.progress(percent)
                        percent_text_ph.markdown(f"Completion: {percent}%")

                        # update metrics
                        pages_crawled_ph.metric("Pages crawled", f"{current}")
                        # pages saved (inserted + updated)
                        pages_saved_ph.metric("Pages saved", f"{inserted_total + updated_total}")
                        ocr_saved_ph.metric("OCR docs saved", f"{ocr_inserted + ocr_updated}")
                        errors_ph.metric("Batch errors", f"{len(save_errors)}")

                        # status/log
                        status_text.markdown(f"Crawled {current}/{MAX_PAGES}: {page.get('url')}")
                        log_area.text(f"Last: {page.get('url')}  |  Title: {page.get('title','')}")
                        running_info.text(eta_text(start_ts, current, MAX_PAGES))

                        # Add to buffer and persist in batches
                        save_buffer.append(page)
                        if len(save_buffer) >= BATCH_SAVE_SIZE:
                            if mongo_uri:
                                try:
                                    summary = save_pages_to_mongo(
                                        save_buffer,
                                        uri=mongo_uri,
                                        db_name=mongo_db,
                                        collection_name=mongo_collection,
                                        upsert=True,
                                    )
                                    inserted_total += summary.get("inserted", 0)
                                    updated_total += summary.get("updated", 0)
                                    if summary.get("errors"):
                                        save_errors.extend(summary.get("errors"))
                                except Exception as e:
                                    save_errors.append({"error": str(e)})
                                # save OCR data for this batch
                                try:
                                    ocr_summary = save_ocr_to_mongo(
                                        save_buffer,
                                        uri=mongo_uri,
                                        db_name=mongo_db,
                                        collection_name="ocr-data",
                                        upsert=True,
                                    )
                                    ocr_inserted += ocr_summary.get("inserted", 0)
                                    ocr_updated += ocr_summary.get("updated", 0)
                                    if ocr_summary.get("errors"):
                                        save_errors.extend(ocr_summary.get("errors"))
                                except Exception as e:
                                    save_errors.append({"ocr_error": str(e)})
                            save_buffer.clear()

                    elif item["type"] == "error":
                        st.error(f"Crawl thread error: {item.get('error')}")
                    elif item["type"] == "done":
                        pass

                # Yield to Streamlit so widget updates are rendered in browser
                time.sleep(0.15)

            # flush remaining buffer after crawl completes
            if save_buffer:
                if mongo_uri:
                    try:
                        summary = save_pages_to_mongo(
                            save_buffer,
                            uri=mongo_uri,
                            db_name=mongo_db,
                            collection_name=mongo_collection,
                            upsert=True,
                        )
                        inserted_total += summary.get("inserted", 0)
                        updated_total += summary.get("updated", 0)
                        if summary.get("errors"):
                            save_errors.extend(summary.get("errors"))
                    except Exception as e:
                        save_errors.append({"error": str(e)})
                    # OCR save for remaining buffer
                    try:
                        ocr_summary = save_ocr_to_mongo(
                            save_buffer,
                            uri=mongo_uri,
                            db_name=mongo_db,
                            collection_name="ocr-data",
                            upsert=True,
                        )
                        ocr_inserted += ocr_summary.get("inserted", 0)
                        ocr_updated += ocr_summary.get("updated", 0)
                        if ocr_summary.get("errors"):
                            save_errors.extend(ocr_summary.get("errors"))
                    except Exception as e:
                        save_errors.append({"ocr_error": str(e)})
                save_buffer.clear()

            # Build index from collected pages
            idx = SearchIndex()
            idx.build(st.session_state.pages)
            st.session_state.index = idx

            # final metric updates
            total = len(st.session_state.pages)
            final_percent = int((total / float(MAX_PAGES)) * 100)
            final_percent = max(0, min(100, final_percent))
            progress_bar.progress(final_percent)
            percent_text_ph.markdown(f"Completion: {final_percent}%")
            pages_crawled_ph.metric("Pages crawled", f"{total}")
            pages_saved_ph.metric("Pages saved", f"{inserted_total + updated_total}")
            ocr_saved_ph.metric("OCR docs saved", f"{ocr_inserted + ocr_updated}")
            errors_ph.metric("Batch errors", f"{len(save_errors)}")

            running_info.empty()
            st.success(f"Finished crawling: {len(st.session_state.pages)} pages collected (max {MAX_PAGES}).")
            if mongo_uri:
                st.write("Saved to Mongo in incremental batches during crawl (summary):")
                st.write(f"Pages inserted: {inserted_total}, pages updated: {updated_total}")
                st.write(f"OCR docs inserted: {ocr_inserted}, OCR docs updated: {ocr_updated}")
                if save_errors:
                    st.write(save_errors[:10])
            else:
                st.warning("Mongo URI not configured; pages (and OCR) indexed in-memory only. To persist set st.secrets['mongo']['uri'] or MONGO_URI.")

    # --- Single-query search UI (kept) ---
    st.markdown("---")
    st.subheader("Single query search")
    if not st.session_state.pages:
        st.info("No pages indexed yet. Start a crawl to index pages.")
        search_q = st.text_input("Search query (word or phrase)", value="", disabled=True)
    else:
        with st.form("search_form"):
            search_q = st.text_input("Search query (word or phrase)")
            submitted = st.form_submit_button("Search")
        if submitted and search_q.strip():
            idx = st.session_state.index
            if not idx:
                st.error("Index not built yet.")
            else:
                with st.spinner("Searching..."):
                    results = idx.search(
                        search_q, fields=DEFAULT_SEARCH_FIELDS, phrase=(" " in search_q.strip()), max_results=500
                    )
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
                        st.markdown(f"[Open original page]({page.get('url')})")

except Exception:
    tb = traceback.format_exc()
    try:
        st.error("An exception occurred during app startup. See traceback below:")
        st.text(tb)
    except Exception:
        print("Exception during app startup:\n", tb, file=sys.stderr)
