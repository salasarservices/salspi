import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pymongo
import tempfile
import time
import hashlib
import os
import json
import urllib3
from datetime import datetime

# --- SAFE IMPORTS ---
try:
    from google.cloud import language_v1
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

try:
    import textrazor
    TEXTRAZOR_AVAILABLE = True
except ImportError:
    TEXTRAZOR_AVAILABLE = False

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

st.markdown("""
<style>
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff; border-radius: 4px; padding: 10px 20px; border: 1px solid #f0f0f0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #E3F2FD !important; color: #000000 !important; border-color: #90CDF4 !important;
    }
    
    /* Custom Pastel Metric Card Styling */
    .metric-card {
        padding: 20px;
        border-radius: 12px;
        margin-bottom: 10px;
        border: 1px solid rgba(0,0,0,0.05);
        color: #333;
    }
    .metric-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 5px; opacity: 0.8; }
    .metric-value { font-size: 2.5rem; font-weight: 800; margin-bottom: 0; }
    .metric-desc { font-size: 0.9rem; opacity: 0.7; margin-bottom: 10px; }
    
    /* Table Styling for 'Top 10' */
    div[data-testid="stDataFrame"] { width: 100%; }
</style>
""", unsafe_allow_html=True)

# --- AUTHENTICATION ---
def setup_google_auth():
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            creds = st.secrets["google"]["credentials"]
            if isinstance(creds, str):
                try: creds = json.loads(creds)
                except json.JSONDecodeError: return False
            
            creds_dict = dict(creds)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                json.dump(creds_dict, f)
                temp_cred_path = f.name
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
            return True
        except Exception: return False
    return False

google_auth_status = setup_google_auth()

def setup_textrazor_auth():
    if "textrazor" in st.secrets and "api_key" in st.secrets["textrazor"]:
        textrazor.api_key = st.secrets["textrazor"]["api_key"]
        return True
    return False

textrazor_auth_status = setup_textrazor_auth() if TEXTRAZOR_AVAILABLE else False

# --- MONGODB CONNECTION ---
@st.cache_resource(show_spinner=False)
def init_mongo_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception: return None

def get_db_collection():
    client = init_mongo_connection()
    if client:
        try: return client[st.secrets["mongo"]["db"]][st.secrets["mongo"]["collection"]]
        except KeyError: return None
    return None

# --- CRAWLER ENGINE ---
def get_page_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def normalize_url(url):
    try:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="").geturl()
        return clean.rstrip('/')
    except: return url

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None:
        st.error("Database unavailable.")
        return
    
    collection.delete_many({})
    start_url = normalize_url(start_url)
    base_domain = urlparse(start_url).netloc.replace('www.', '')
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    while queue and count < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        count += 1
        
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling {count}/{max_pages}: {url}")
        
        try:
            time.sleep(0.1)
            start_time = time.time()
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            latency = (time.time() - start_time) * 1000
            final_url = normalize_url(response.url)
            
            page_data = {
                "url": final_url, "domain": base_domain, "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''), "crawl_time": datetime.now(),
                "latency_ms": latency, "links": [], "images": [], "title": "", "meta_desc": "",
                "canonical": "", "page_text": "", "content_hash": "", "indexable": True, "h1_count": 0, "word_count": 0
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta['content'].strip() if meta and meta.get('content') else ""
                canon = soup.find('link', rel='canonical')
                page_data['canonical'] = canon['href'] if canon else ""
                
                # H1 Count
                page_data['h1_count'] = len(soup.find_all('h1'))
                
                for s in soup(["script", "style"]): s.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['word_count'] = len(text_content.split())
                
                for img in soup.find_all('img'):
                    if img.get('src'):
                        page_data['images'].append({'src': urljoin(url, img.get('src')), 'alt': img.get('alt', '')})
                
                robots = soup.find('meta', attrs={'name': 'robots'})
                if robots and 'noindex' in robots.get('content', '').lower(): page_data['indexable'] = False
                
                for link in soup.find_all('a', href=True):
                    raw = link['href'].strip()
                    if not raw or raw.startswith(('mailto:', 'tel:', 'javascript:', '#')): continue
                    abs_link = normalize_url(urljoin(url, raw))
                    if base_domain in urlparse(abs_link).netloc:
                        page_data['links'].append(abs_link)
                        if abs_link not in visited and abs_link not in queue: queue.append(abs_link)

            collection.update_one({"url": final_url}, {"$set": page_data}, upsert=True)
        except Exception as e:
            collection.update_one({"url": url}, {"$set": {"url": url, "status_code": 0, "error": str(e)}}, upsert=True)

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- METRICS CALCULATOR ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    data = list(col.find({}, {'page_text': 0, '_id': 0}))
    df = pd.DataFrame(data)
    if df.empty: return None, None
    
    cols = ['url', 'title', 'meta_desc', 'canonical', 'images', 'status_code', 'content_hash', 'latency_ms', 'indexable', 'h1_count', 'word_count']
    for c in cols: 
        if c not in df.columns: df[c] = None
        
    # Fill NaNs
    df['title'] = df['title'].fillna("")
    df['meta_desc'] = df['meta_desc'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    df['canonical'] = df['canonical'].fillna("")
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    df['h1_count'] = pd.to_numeric(df['h1_count'], errors='coerce').fillna(0)
    df['word_count'] = pd.to_numeric(df['word_count'], errors='coerce').fillna(0)
    
    return df

# --- NLP ENGINE ---
def analyze_content(text):
    if not NLP_AVAILABLE: return None, "Library missing."
    try:
        client = language_v1.LanguageServiceClient()
        if not text or len(text.split()) < 20: return None, "Text too short (<20 words)."
        doc = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(request={'document': doc}).document_sentiment
        entities = client.analyze_entities(request={'document': doc}).entities
        return {"sentiment": sentiment, "entities": entities}, None
    except Exception as e: return None, str(e)

# --- TEXTRAZOR ENGINE ---
def analyze_textrazor(text):
    if not TEXTRAZOR_AVAILABLE: return None, "TextRazor Library missing."
    if not textrazor_auth_status: return None, "TextRazor API Key missing."
    try:
        client = textrazor.TextRazor(extractors=["entities", "topics"])
        if not text or len(text.strip()) < 50: return None, "Text too short for TextRazor."
        response = client.analyze(text)
        return response, None
    except Exception as e: return None, str(e)

# --- SCRAPER ---
def scrape_external_page(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=10, verify=False)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            for s in soup(["script", "style"]): s.extract()
            return soup.get_text(separator=' ', strip=True), None
        return None, f"Status {resp.status_code}"
    except Exception as e: return None, str(e)

# --- HELPER: DISPLAY METRIC CARD ---
def display_metric_block(title, count, df_data, color_hex, display_cols):
    """
    Renders a custom HTML block with pastel background and a dataframe expansion below.
    """
    st.markdown(f"""
    <div class="metric-card" style="background-color: {color_hex};">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{count}</div>
        <div class="metric-desc">Showing top results below</div>
    </div>
    """, unsafe_allow_html=True)
    
    if count > 0:
        with st.expander(f"Show Top 10 {title}"):
            if isinstance(df_data, pd.DataFrame):
                # Filter to specific columns if they exist
                valid_cols = [c for c in display_cols if c in df_data.columns]
                st.dataframe(df_data[valid_cols].head(10), use_container_width=True)
            elif isinstance(df_data, list):
                st.dataframe(pd.DataFrame(df_data).head(10), use_container_width=True)

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

# --- MAIN APP ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 SEO Report", "🧠 NLP Analysis", "🔍 Search", "📄 Content Analysis"])
df = get_metrics()

# TAB 1: SEO REPORT (REDESIGNED)
with tab1:
    if df is not None:
        st.subheader("Site Health Overview")
        
        # 1.2 Duplicate Content
        dup_content = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
        display_metric_block("1.2 Duplicate Content Pages", len(dup_content), dup_content, "#FFB3BA", ['url', 'title'])

        col1, col2 = st.columns(2)
        with col1:
            # 1.3 Duplicate Titles
            dup_title = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
            display_metric_block("1.3 Duplicate Meta Titles", len(dup_title), dup_title, "#FFDFBA", ['url', 'title'])
        with col2:
            # 1.4 Duplicate Desc
            dup_desc = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
            display_metric_block("1.4 Duplicate Meta Desc", len(dup_desc), dup_desc, "#FFFFBA", ['url', 'meta_desc'])

        col3, col4 = st.columns(2)
        with col3:
            # 1.5 Canonical Issues
            def check_canonical(row):
                if not row['canonical']: return False
                return row['canonical'] != row['url']
            canon_issues = df[df.apply(check_canonical, axis=1)]
            display_metric_block("1.5 Canonical Issues", len(canon_issues), canon_issues, "#BAFFC9", ['url', 'canonical'])
        with col4:
            # 1.6 Missing Alt Tags
            missing_alt_data = []
            for _, row in df.iterrows():
                if isinstance(row['images'], list):
                    for img in row['images']:
                        if not img.get('alt'):
                            missing_alt_data.append({'Page': row['url'], 'Image Src': img.get('src')})
            display_metric_block("1.6 Missing Alt Tags", len(missing_alt_data), missing_alt_data, "#BAE1FF", ['Page', 'Image Src'])

        # 1.7 Broken Links (Using 404 as broken for simplicity in crawl report)
        # Note: Crawlers usually report broken links based on response code 404
        
        col5, col6, col7, col8 = st.columns(4)
        with col5:
             # 1.7 Broken Links (Proxy: 404 pages found during crawl)
             broken = df[df['status_code'] == 404]
             display_metric_block("1.7 Broken Pages (404)", len(broken), broken, "#FFCCE5", ['url', 'status_code'])
        with col6:
            # 1.8 300 Responses
            r300 = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
            display_metric_block("1.8 3xx Redirects", len(r300), r300, "#E2B3FF", ['url', 'status_code'])
        with col7:
            # 1.9 400 Responses (General 4xx excluding 404 if needed, or all 4xx)
            r400 = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
            display_metric_block("1.9 4xx Errors", len(r400), r400, "#FF9AA2", ['url', 'status_code'])
        with col8:
             # 1.10 500 Responses
             r500 = df[df['status_code'] >= 500]
             display_metric_block("1.10 5xx Errors", len(r500), r500, "#C7CEEA", ['url', 'status_code'])

        col9, col10 = st.columns(2)
        with col9:
            # 1.11 Indexable
            indexable = df[df['indexable'] == True]
            display_metric_block("1.11 Indexable Pages", len(indexable), indexable, "#B5EAD7", ['url', 'title'])
        with col10:
             # 1.12 Non-Indexable
            non_indexable = df[df['indexable'] == False]
            display_metric_block("1.12 Non-Indexable Pages", len(non_indexable), non_indexable, "#FFDAC1", ['url', 'title'])

        # 1.13 On-Page Changes (Proxy: Heading Analysis)
        # Identify changes requires history. For now, identifying "Issues" with metadata/headings.
        # e.g., Missing H1 or Multiple H1
        h1_issues = df[(df['h1_count'] == 0) | (df['h1_count'] > 1)]
        display_metric_block("1.13 On-Page Heading Issues (0 or >1 H1)", len(h1_issues), h1_issues, "#E2F0CB", ['url', 'h1_count'])

        # 1.14 Content Issues (Proxy: Thin Content / Low Word Count)
        # "Changes" require history. "Spelling" requires heavy library. Using Word Count < 200 as issue.
        thin_content = df[df['word_count'] < 200]
        display_metric_block("1.14 Content Issues (Thin Content <200 words)", len(thin_content), thin_content, "#F7D9C4", ['url', 'word_count'])

        # 1.15 PageSpeed (Proxy: Latency > 1.5s)
        slow_pages = df[df['latency_ms'] > 1500]
        display_metric_block("1.15 PageSpeed Opportunities (Slow > 1.5s)", len(slow_pages), slow_pages, "#D7E3FC", ['url', 'latency_ms'])

    else:
        st.info("No crawl data available. Please start a crawl from the sidebar.")

# TAB 2: GOOGLE NLP
with tab2:
    st.subheader("Google NLP Analysis")
    if df is not None and google_auth_status and NLP_AVAILABLE:
        url_sel = st.selectbox("Select Page for G-NLP:", df['url'].unique(), key="gnlp_sel")
        if st.button("Analyze with Google"):
            doc = get_db_collection().find_one({"url": url_sel})
            res, err = analyze_content(doc.get('page_text', ''))
            if res:
                s = res['sentiment']
                c1, c2 = st.columns(2)
                c1.metric("Sentiment", f"{s.score:.2f}")
                c2.metric("Magnitude", f"{s.magnitude:.2f}")
                ents = [{"Name": e.name, "Type": language_v1.Entity.Type(e.type_).name, "Salience": f"{e.salience:.1%}"} for e in res['entities'][:10]]
                st.dataframe(pd.DataFrame(ents), use_container_width=True)
            else: st.error(err)
        
        st.markdown("---")
        st.markdown("### Competitor Comparison (Google)")
        comp_url_g = st.text_input("Competitor URL (Google):")
        if st.button("Compare (Google)"):
            doc = get_db_collection().find_one({"url": url_sel})
            comp_txt, c_err = scrape_external_page(comp_url_g)
            if doc and comp_txt:
                res_in, _ = analyze_content(doc.get('page_text', ''))
                res_ex, _ = analyze_content(comp_txt)
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

# TAB 4: TEXTRAZOR ANALYSIS
with tab4:
    st.subheader("TextRazor Content Intelligence")
    if not TEXTRAZOR_AVAILABLE:
        st.error("Please install TextRazor: `pip install textrazor`")
    elif not textrazor_auth_status:
        st.error("Please add [textrazor] api_key to secrets.toml")
    elif df is not None:
        tr_url_sel = st.selectbox("Select Page for Analysis:", df['url'].unique(), key="tr_sel")
        
        # Section A: Single Page Analysis
        if st.button("Analyze Current Page (TextRazor)"):
            doc = get_db_collection().find_one({"url": tr_url_sel})
            with st.spinner("Processing with TextRazor..."):
                resp, err = analyze_textrazor(doc.get('page_text', ''))
                if resp:
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("#### Top Entities")
                        ents = [{"ID": e.id, "Relevance": f"{e.relevance_score:.2f}", "Confidence": f"{e.confidence_score:.2f}"} for e in sorted(resp.entities(), key=lambda x: x.relevance_score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(ents), use_container_width=True)
                    with c2:
                        st.markdown("#### Top Topics")
                        tops = [{"Label": t.label, "Score": f"{t.score:.2f}"} for t in sorted(resp.topics(), key=lambda x: x.score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(tops), use_container_width=True)
                else:
                    st.error(err)

        # Section B: Comparison
        st.markdown("---")
        st.markdown("### ⚔️ Competitor Content Gap Analysis")
        comp_url_tr = st.text_input("Enter Competitor URL:", key="tr_comp_input")
        
        if st.button("Compare Pages (TextRazor)"):
            if not comp_url_tr:
                st.warning("Enter a URL first.")
            else:
                doc = get_db_collection().find_one({"url": tr_url_sel})
                with st.spinner("Scraping & Analyzing both pages..."):
                    text_a = doc.get('page_text', '')
                    text_b, err_b = scrape_external_page(comp_url_tr)
                    
                    if text_a and text_b:
                        resp_a, err_a = analyze_textrazor(text_a)
                        resp_b, err_b = analyze_textrazor(text_b)
                        
                        if resp_a and resp_b:
                            ents_a = {e.id for e in resp_a.entities()}
                            ents_b = {e.id for e in resp_b.entities()}
                            
                            common = list(ents_a.intersection(ents_b))
                            missing = list(ents_b - ents_a) 
                            unique = list(ents_a - ents_b)
                            
                            c1, c2, c3 = st.columns(3)
                            with c1:
                                st.success(f"✅ Common ({len(common)})")
                                st.dataframe(pd.DataFrame(common, columns=["Entity ID"]), height=400, use_container_width=True)
                            with c2:
                                st.error(f"⚠️ Missing ({len(missing)})")
                                st.dataframe(pd.DataFrame(missing, columns=["Entity ID"]), height=400, use_container_width=True)
                            with c3:
                                st.info(f"🔹 Unique ({len(unique)})")
                                st.dataframe(pd.DataFrame(unique, columns=["Entity ID"]), height=400, use_container_width=True)
                        else:
                            st.error(f"Analysis Failed. Internal: {err_a}, External: {err_b}")
                    else:
                        st.error("Failed to retrieve text from one of the pages.")
