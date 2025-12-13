import streamlit as st
import pandas as pd
from helpers import (
    setup_google_auth, setup_textrazor_auth, init_mongo_connection, 
    get_db_collection, crawl_site, get_metrics_df, analyze_google, 
    analyze_textrazor, scrape_external_page, fetch_bing_backlinks,
    run_technical_audit, 
    NLP_AVAILABLE, TEXTRAZOR_AVAILABLE
)

# --- CONFIG ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")
st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff; border-radius: 4px; padding: 10px 20px; border: 1px solid #f0f0f0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #E3F2FD !important; color: #000000 !important; border-color: #90CDF4 !important;
    }
    .metric-card {
        padding: 20px; border-radius: 12px; margin-bottom: 10px;
        border: 1px solid rgba(0,0,0,0.05); color: #333;
    }
    .metric-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 5px; opacity: 0.8; }
    .metric-value { font-size: 2.5rem; font-weight: 800; margin-bottom: 0; }
    .metric-desc { font-size: 0.9rem; opacity: 0.7; margin-bottom: 10px; }
    div[data-testid="stDataFrame"] { width: 100%; }
</style>
""", unsafe_allow_html=True)

# --- SETUP ---
google_auth_status = setup_google_auth()
textrazor_auth_status = setup_textrazor_auth()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Control Panel")
    c1, c2 = st.columns(2)
    with c1:
        if init_mongo_connection(): st.success("DB: Online")
        else: st.error("DB: Offline")
    with c2:
        if google_auth_status: st.success("G-NLP: Ready")
        else: st.warning("G-NLP: Inactive")
    
    if textrazor_auth_status: st.success("TextRazor: Ready")
    else: st.warning("TextRazor: Inactive")

    st.markdown("---")
    target_url = st.text_input("Target URL", "https://example.com")
    st.caption("Default Crawl Limit: 1000 Pages")
    
    if st.button("Start Crawl", type="primary"):
        crawl_site(target_url)
        st.rerun()

# --- HELPER UI ---
def display_metric_block(title, count, df_data, color_hex, display_cols):
    st.markdown(f"""
    <div class="metric-card" style="background-color: {color_hex};">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{count}</div>
        <div class="metric-desc">Click dropdown to view details</div>
    </div>""", unsafe_allow_html=True)
    
    if count > 0:
        with st.expander(f"Show Details for {title}"):
            if isinstance(df_data, list):
                df_display = pd.DataFrame(df_data)
            elif isinstance(df_data, pd.DataFrame):
                valid_cols = [c for c in display_cols if c in df_data.columns]
                df_display = df_data[valid_cols]
            else:
                return

            column_config = {}
            if 'url' in df_display.columns:
                column_config['url'] = st.column_config.LinkColumn("URL")
            if 'Page' in df_display.columns:
                column_config['Page'] = st.column_config.LinkColumn("Page")

            st.dataframe(
                df_display, 
                width="stretch", 
                column_config=column_config,
                hide_index=True
            )

# --- MAIN UI ---
# Added Tab 6: Deep Tech Audit
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 SEO Report", 
    "🧠 NLP Analysis", 
    "🔍 Search", 
    "📄 Content Analysis", 
    "🔗 Backlinks", 
    "🛠️ Deep Tech Audit"
])
df = get_metrics_df()

# TAB 1: SEO REPORT
with tab1:
    if df is not None:
        st.subheader("Site Health Overview")
        
        dup_content = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
        display_metric_block("Duplicate Content Pages", len(dup_content), dup_content, "#FFB3BA", ['url', 'title'])

        col1, col2 = st.columns(2)
        with col1:
            dup_title = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
            display_metric_block("Duplicate Meta Titles", len(dup_title), dup_title, "#FFDFBA", ['url', 'title'])
        with col2:
            dup_desc = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
            display_metric_block("Duplicate Meta Desc", len(dup_desc), dup_desc, "#FFFFBA", ['url', 'meta_desc'])

        col3, col4 = st.columns(2)
        with col3:
            def check_canonical(row):
                if not row['canonical']: return False
                return row['canonical'] != row['url']
            canon_issues = df[df.apply(check_canonical, axis=1)]
            display_metric_block("Canonical Issues", len(canon_issues), canon_issues, "#BAFFC9", ['url', 'canonical'])
        with col4:
            missing_alt_data = []
            for _, row in df.iterrows():
                if isinstance(row['images'], list):
                    for img in row['images']:
                        if not img.get('alt'): missing_alt_data.append({'Page': row['url'], 'Image Src': img.get('src')})
            display_metric_block("Missing Alt Tags", len(missing_alt_data), missing_alt_data, "#BAE1FF", ['Page', 'Image Src'])

        col5, col6, col7, col8 = st.columns(4)
        with col5:
             broken = df[df['status_code'] == 404]
             display_metric_block("Broken Pages (404)", len(broken), broken, "#FFCCE5", ['url', 'status_code'])
        with col6:
            r300 = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
            display_metric_block("3xx Redirects", len(r300), r300, "#E2B3FF", ['url', 'status_code'])
        with col7:
            r400 = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
            display_metric_block("4xx Errors", len(r400), r400, "#FF9AA2", ['url', 'status_code'])
        with col8:
             r500 = df[df['status_code'] >= 500]
             display_metric_block("5xx Errors", len(r500), r500, "#C7CEEA", ['url', 'status_code'])

        col9, col10 = st.columns(2)
        with col9:
            indexable = df[df['indexable'] == True]
            display_metric_block("Indexable Pages", len(indexable), indexable, "#B5EAD7", ['url', 'title'])
        with col10:
            non_indexable = df[df['indexable'] == False]
            display_metric_block("Non-Indexable Pages", len(non_indexable), non_indexable, "#FFDAC1", ['url', 'title'])

        h1_issues = df[(df['h1_count'] == 0) | (df['h1_count'] > 1)]
        display_metric_block("On-Page Heading Issues", len(h1_issues), h1_issues, "#E2F0CB", ['url', 'h1_count'])

        thin_content = df[df['word_count'] < 200]
        display_metric_block("Thin Content (<200 words)", len(thin_content), thin_content, "#F7D9C4", ['url', 'word_count'])

        slow_pages = df[df['latency_ms'] > 1500]
        display_metric_block("Slow Pages (> 1.5s)", len(slow_pages), slow_pages, "#D7E3FC", ['url', 'latency_ms'])
    else:
        st.info("No crawl data available. Please start a crawl from the sidebar.")

# TAB 2: GOOGLE NLP
with tab2:
    st.subheader("Google NLP Analysis")
    if df is not None and google_auth_status and NLP_AVAILABLE:
        from google.cloud import language_v1 
        url_sel = st.selectbox("Select Page for G-NLP:", df['url'].unique(), key="gnlp_sel")
        if st.button("Analyze with Google"):
            doc = get_db_collection().find_one({"url": url_sel})
            res, err = analyze_google(doc.get('page_text', ''))
            if res:
                s = res['sentiment']
                c1, c2 = st.columns(2)
                c1.metric("Sentiment", f"{s.score:.2f}")
                c2.metric("Magnitude", f"{s.magnitude:.2f}")
                ents = [{"Name": e.name, "Type": language_v1.Entity.Type(e.type_).name, "Salience": f"{e.salience:.1%}"} for e in res['entities'][:10]]
                st.dataframe(pd.DataFrame(ents), width="stretch")
            else: st.error(err)
    elif not google_auth_status:
        st.warning("Google NLP is not active. Check credentials.")

# TAB 3: SEARCH
with tab3:
    q = st.text_input("Deep Search:")
    if q and get_db_collection() is not None:
        res = list(get_db_collection().find({"page_text": {"$regex": q, "$options": "i"}}).limit(20))
        if res:
            data = [{"URL": r['url'], "Match": "..." + r['page_text'][r['page_text'].lower().find(q.lower()):][:100] + "..."} for r in res]
            st.dataframe(pd.DataFrame(data), width="stretch")

# TAB 4: CONTENT INTELLIGENCE
with tab4:
    st.subheader("Content Intelligence")
    if df is not None:
        tr_url_sel = st.selectbox("Select Page for Analysis:", df['url'].unique(), key="tr_sel")
        
        # --- SECTION: IMAGE OCR RESULTS ---
        st.markdown("#### 🖼️ Image Text Extraction (OCR)")
        doc = get_db_collection().find_one({"url": tr_url_sel})
        
        if doc and 'images' in doc and doc['images']:
            ocr_images = [img for img in doc['images'] if img.get('ocr_text')]
            if ocr_images:
                st.info(f"Found {len(ocr_images)} images with readable text.")
                ocr_df = pd.DataFrame(ocr_images)[['src', 'alt', 'ocr_text']]
                st.dataframe(
                    ocr_df,
                    width="stretch",
                    column_config={
                        "src": st.column_config.LinkColumn("Image Link"),
                        "alt": "Alt Text",
                        "ocr_text": "Extracted Text (OCR)"
                    }
                )
            else:
                st.caption("No text detected in images (or OCR limit reached).")
        else:
            st.caption("No images found on this page.")
        st.markdown("---")

    st.markdown("#### 📝 TextRazor Text Analysis")
    if not TEXTRAZOR_AVAILABLE: st.error("Please install TextRazor.")
    elif not textrazor_auth_status: st.error("Please add TextRazor API key.")
    elif df is not None:
        if st.button("Analyze Page Text (TextRazor)"):
            doc = get_db_collection().find_one({"url": tr_url_sel})
            with st.spinner("Processing..."):
                resp, err = analyze_textrazor(doc.get('page_text', ''), textrazor_auth_status)
                if resp:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("**Top Entities**")
                        ents = [{"ID": e.id, "Relevance": f"{e.relevance_score:.2f}"} for e in sorted(resp.entities(), key=lambda x: x.relevance_score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(ents), width="stretch")
                    with c2:
                        st.markdown("**Top Topics**")
                        tops = [{"Label": t.label, "Score": f"{t.score:.2f}"} for t in sorted(resp.topics(), key=lambda x: x.score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(tops), width="stretch")
                else: st.error(err)

# TAB 5: BACKLINKS
with tab5:
    st.subheader("Inbound Link Checker")
    st.info("Check official backlinks from Bing Webmaster Tools.")
    
    if "bing_api_key" not in st.session_state:
        st.session_state["bing_api_key"] = ""
        
    bing_key = st.text_input("Enter Bing API Key:", value=st.session_state["bing_api_key"], type="password")
    
    if st.button("Fetch Bing Backlinks"):
        if not bing_key:
            st.warning("Please enter a Bing API Key.")
        else:
            st.session_state["bing_api_key"] = bing_key
            if not target_url:
                st.error("Please enter a Target URL in the sidebar first.")
            else:
                with st.spinner(f"Fetching backlinks for {target_url}..."):
                    data, err = fetch_bing_backlinks(target_url, bing_key)
                    if data:
                        st.success(f"Found {len(data)} backlinks")
                        df_bing = pd.DataFrame(data)
                        st.dataframe(df_bing, width="stretch", column_config={
                            "Url": st.column_config.LinkColumn("Target Page"),
                            "SourceUrl": st.column_config.LinkColumn("Backlink Source")
                        })
                    else:
                        st.error(f"Error fetching data: {err}")

# TAB 6: DEEP TECH AUDIT
with tab6:
    st.subheader("Deep Technical SEO Audit")
    st.info("Powered by python-seo-analyzer. This runs a separate, rigorous scan of your target URL.")
    
    if st.button("Run Deep Audit"):
        if not target_url:
            st.error("Please enter a Target URL in the sidebar.")
        else:
            with st.spinner("Running Deep Scan (this may take a minute)..."):
                audit_data, err = run_technical_audit(target_url)
                
                if audit_data:
                    # Process Results
                    pages = audit_data.get('pages', [])
                    
                    all_warnings = []
                    all_errors = []
                    
                    for p in pages:
                        p_url = p.get('url', '')
                        # Collect Warnings
                        if 'warnings' in p and p['warnings']:
                            for w in p['warnings']:
                                all_warnings.append({"Page": p_url, "Warning": w})
                        # Collect Errors
                        if 'errors' in p and p['errors']:
                            for e in p['errors']:
                                all_errors.append({"Page": p_url, "Error": e})
                    
                    # Display Tabs for results
                    t1, t2 = st.tabs(["⚠️ Warnings", "❌ Errors"])
                    
                    with t1:
                        if all_warnings:
                            st.dataframe(pd.DataFrame(all_warnings), width="stretch", column_config={
                                "Page": st.column_config.LinkColumn("Page URL")
                            })
                        else:
                            st.success("No Warnings Found!")
                            
                    with t2:
                        if all_errors:
                            st.dataframe(pd.DataFrame(all_errors), width="stretch", column_config={
                                "Page": st.column_config.LinkColumn("Page URL")
                            })
                        else:
                            st.success("No Critical Errors Found!")
                            
                else:
                    st.error(f"Audit Failed: {err}")
