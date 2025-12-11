import streamlit as st
import pandas as pd
from helpers import (
    setup_google_auth, setup_textrazor_auth, init_mongo_connection, 
    get_db_collection, crawl_site, get_metrics_df, analyze_google, 
    analyze_textrazor, scrape_external_page, NLP_AVAILABLE, TEXTRAZOR_AVAILABLE
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
        display_metric_block("1.15 Slow Pages (> 1.5s)", len(slow_pages), slow_
