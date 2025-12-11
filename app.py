import streamlit as st
import pandas as pd
from helpers import (
    setup_google_auth, setup_textrazor_auth, init_mongo_connection, 
    get_db_collection, crawl_site, get_metrics_df, analyze_google, 
    analyze_textrazor, scrape_external_page, NLP_AVAILABLE, TEXTRAZOR_AVAILABLE
)
# Note: Since helpers.py is in the same folder, simple import works best.

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
    max_pages = st.number_input("Max Pages", 10, 500, 50)
    if st.button("Start Crawl", type="primary"):
        crawl_site(target_url, max_pages)
        st.rerun()
    if st.button("Clear DB"):
        get_db_collection().delete_many({})
        st.rerun()

# --- HELPER UI ---
def display_metric_block(title, count, df_data, color_hex, display_cols):
    st.markdown(f"""
    <div class="metric-card" style="background-color: {color_hex};">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{count}</div>
        <div class="metric-desc">Showing top results below</div>
    </div>""", unsafe_allow_html=True)
    if count > 0:
        with st.expander(f"Show Top 10 {title}"):
            if isinstance(df_data, pd.DataFrame):
                valid_cols = [c for c in display_cols if c in df_data.columns]
                st.dataframe(df_data[valid_cols].head(10), use_container_width=True)
            elif isinstance(df_data, list):
                st.dataframe(pd.DataFrame(df_data).head(10), use_container_width=True)

# --- MAIN UI ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 SEO Report", "🧠 NLP Analysis", "🔍 Search", "📄 Content Analysis"])
df = get_metrics_df()

# TAB 1: SEO REPORT
with tab1:
    if df is not None:
        st.subheader("Site Health Overview")
        
        # 1.2 Duplicate Content
        dup_content = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
        display_metric_block("1.2 Duplicate Content Pages", len(dup_content), dup_content, "#FFB3BA", ['url', 'title'])

        col1, col2 = st.columns(2)
        with col1:
            dup_title = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
            display_metric_block("1.3 Duplicate Meta Titles", len(dup_title), dup_title, "#FFDFBA", ['url', 'title'])
        with col2:
            dup_desc = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
            display_metric_block("1.4 Duplicate Meta Desc", len(dup_desc), dup_desc, "#FFFFBA", ['url', 'meta_desc'])

        col3, col4 = st.columns(2)
        with col3:
            def check_canonical(row):
                if not row['canonical']: return False
                return row['canonical'] != row['url']
            canon_issues = df[df.apply(check_canonical, axis=1)]
            display_metric_block("1.5 Canonical Issues", len(canon_issues), canon_issues, "#BAFFC9", ['url', 'canonical'])
        with col4:
            missing_alt_data = []
            for _, row in df.iterrows():
                if isinstance(row['images'], list):
                    for img in row['images']:
                        if not img.get('alt'): missing_alt_data.append({'Page': row['url'], 'Image Src': img.get('src')})
            display_metric_block("1.6 Missing Alt Tags", len(missing_alt_data), missing_alt_data, "#BAE1FF", ['Page', 'Image Src'])

        col5, col6, col7, col8 = st.columns(4)
        with col5:
             broken = df[df['status_code'] == 404]
             display_metric_block("1.7 Broken Pages (404)", len(broken), broken, "#FFCCE5", ['url', 'status_code'])
        with col6:
            r300 = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
            display_metric_block("1.8 3xx Redirects", len(r300), r300, "#E2B3FF", ['url', 'status_code'])
        with col7:
            r400 = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
            display_metric_block("1.9 4xx Errors", len(r400), r400, "#FF9AA2", ['url', 'status_code'])
        with col8:
             r500 = df[df['status_code'] >= 500]
             display_metric_block("1.10 5xx Errors", len(r500), r500, "#C7CEEA", ['url', 'status_code'])

        col9, col10 = st.columns(2)
        with col9:
            indexable = df[df['indexable'] == True]
            display_metric_block("1.11 Indexable Pages", len(indexable), indexable, "#B5EAD7", ['url', 'title'])
        with col10:
            non_indexable = df[df['indexable'] == False]
            display_metric_block("1.12 Non-Indexable Pages", len(non_indexable), non_indexable, "#FFDAC1", ['url', 'title'])

        h1_issues = df[(df['h1_count'] == 0) | (df['h1_count'] > 1)]
        display_metric_block("1.13 On-Page Heading Issues", len(h1_issues), h1_issues, "#E2F0CB", ['url', 'h1_count'])

        thin_content = df[df['word_count'] < 200]
        display_metric_block("1.14 Thin Content (<200 words)", len(thin_content), thin_content, "#F7D9C4", ['url', 'word_count'])

        slow_pages = df[df['latency_ms'] > 1500]
        display_metric_block("1.15 Slow Pages (> 1.5s)", len(slow_pages), slow_pages, "#D7E3FC", ['url', 'latency_ms'])
    else:
        st.info("No crawl data available. Please start a crawl from the sidebar.")

# TAB 2: GOOGLE NLP
with tab2:
    if df is not None and google_auth_status and NLP_AVAILABLE:
        from google.cloud import language_v1 # Import here for Type hinting
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
                st.dataframe(pd.DataFrame(ents), use_container_width=True)
            else: st.error(err)
        
        st.markdown("---")
        comp_url_g = st.text_input("Competitor URL (Google):")
        if st.button("Compare (Google)"):
            doc = get_db_collection().find_one({"url": url_sel})
            comp_txt, c_err = scrape_external_page(comp_url_g)
            if doc and comp_txt:
                res_in, _ = analyze_google(doc.get('page_text', ''))
                res_ex, _ = analyze_google(comp_txt)
                if res_in and res_ex:
                    c1, c2 = st.columns(2)
                    with c1: 
                        st.subheader("Our Page")
                        st.metric("Sentiment", f"{res_in['sentiment'].score:.2f}")
                        st.dataframe(pd.DataFrame([{"Name": e.name, "Sal": f"{e.salience:.1%}"} for e in res_in['entities'][:5]]), use_container_width=True)
                    with c2:
                        st.subheader("Competitor")
                        st.metric("Sentiment", f"{res_ex['sentiment'].score:.2f}")
                        st.dataframe(pd.DataFrame([{"Name": e.name, "Sal": f"{e.salience:.1%}"} for e in res_ex['entities'][:5]]), use_container_width=True)

# TAB 3: SEARCH
with tab3:
    q = st.text_input("Deep Search:")
    if q and get_db_collection():
        res = list(get_db_collection().find({"page_text": {"$regex": q, "$options": "i"}}).limit(20))
        if res:
            data = [{"URL": r['url'], "Match": "..." + r['page_text'][r['page_text'].lower().find(q.lower()):][:100] + "..."} for r in res]
            st.dataframe(pd.DataFrame(data), use_container_width=True)

# TAB 4: TEXTRAZOR
with tab4:
    if not TEXTRAZOR_AVAILABLE: st.error("Please install TextRazor.")
    elif not textrazor_auth_status: st.error("Please add TextRazor API key.")
    elif df is not None:
        tr_url_sel = st.selectbox("Select Page for Analysis:", df['url'].unique(), key="tr_sel")
        if st.button("Analyze Current Page (TextRazor)"):
            doc = get_db_collection().find_one({"url": tr_url_sel})
            with st.spinner("Processing..."):
                resp, err = analyze_textrazor(doc.get('page_text', ''), textrazor_auth_status)
                if resp:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("#### Top Entities")
                        ents = [{"ID": e.id, "Relevance": f"{e.relevance_score:.2f}"} for e in sorted(resp.entities(), key=lambda x: x.relevance_score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(ents), use_container_width=True)
                    with c2:
                        st.markdown("#### Top Topics")
                        tops = [{"Label": t.label, "Score": f"{t.score:.2f}"} for t in sorted(resp.topics(), key=lambda x: x.score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(tops), use_container_width=True)
                else: st.error(err)

        st.markdown("---")
        comp_url_tr = st.text_input("Enter Competitor URL:", key="tr_comp_input")
        if st.button("Compare Pages (TextRazor)"):
            doc = get_db_collection().find_one({"url": tr_url_sel})
            with st.spinner("Analyzing..."):
                text_a = doc.get('page_text', '')
                text_b, err_b = scrape_external_page(comp_url_tr)
                if text_a and text_b:
                    resp_a, _ = analyze_textrazor(text_a, textrazor_auth_status)
                    resp_b, _ = analyze_textrazor(text_b, textrazor_auth_status)
                    if resp_a and resp_b:
                        ents_a = {e.id for e in resp_a.entities()}
                        ents_b = {e.id for e in resp_b.entities()}
                        common = list(ents_a.intersection(ents_b))
                        missing = list(ents_b - ents_a) 
                        unique = list(ents_a - ents_b)
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.success(f"Common ({len(common)})")
                            st.dataframe(pd.DataFrame(common, columns=["Entity"]), height=400, use_container_width=True)
                        with c2:
                            st.error(f"Missing ({len(missing)})")
                            st.dataframe(pd.DataFrame(missing, columns=["Entity"]), height=400, use_container_width=True)
                        with c3:
                            st.info(f"Unique ({len(unique)})")
                            st.dataframe(pd.DataFrame(unique, columns=["Entity"]), height=400, use_container_width=True)
                        else: st.error(f"Analysis Failed.")
