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
    /* Metric Cards */
    div[data-testid="metric-container"] {
        background-color: #F0F4F8; 
        border: 1px solid #D9E2EC;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        color: #102A43;
        transition: transform 0.2s;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-2px);
    }
    div[data-testid="metric-container"] label {
        color: #486581; 
        font-size: 1rem !important;
        font-weight: 600 !important; 
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #102A43;
        font-size: 2.2rem !important; 
        font-weight: 800 !important; 
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #ffffff;
        border-radius: 4px;
        padding: 10px 20px;
        border: 1px solid #f0f0f0;
    }
    .stTabs [aria-selected="true"] {
        background-color: #E3F2FD !important;
        color: #000000 !important;
        border-color: #90CDF4 !important;
    }
    
    /* Table Headers */
    th {
        font-weight: 800 !important;
        color: #102A43 !important;
        font-size: 1.05rem !important;
    }
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
        try:
            return client[st.secrets["mongo"]["db"]][st.secrets["mongo"]["collection"]]
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
                "canonical": "", "page_text": "", "content_hash": "", "indexable": True
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta['content'].strip() if meta and meta.get('content') else ""
                canon = soup.find('link', rel='canonical')
                page_data['canonical'] = canon['href'] if canon else ""
                
                for s in soup(["script", "style"]): s.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                page_data['content_hash'] = get_page_hash(text_content)
                
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
    
    cols = ['url', 'title', 'meta_desc', 'canonical', 'images', 'status_code', 'content_hash', 'latency_ms', 'indexable']
    for c in cols: 
        if c not in df.columns: df[c] = None
    
    # Fill NaNs safely
    df['title'] = df['title'].fillna("")
    df['meta_desc'] = df['meta_desc'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    df['canonical'] = df['canonical'].fillna("")
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    
    metrics = {
        'total_pages': len(df),
        'dup_content_count': len(df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]),
        'dup_title_count': len(df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]),
        'dup_desc_count': len(df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]),
        'broken_count': len(df[df['status_code'] == 404]),
        '3xx_count': len(df[(df['status_code'] >= 300) & (df['status_code'] < 400)]),
        '4xx_count': len(df[(df['status_code'] >= 400) & (df['status_code'] < 500)]),
        '5xx_count': len(df[df['status_code'] >= 500]),
        'indexable_count': len(df[df['indexable'] == True]),
        'non_indexable_count': len(df[df['indexable'] == False]),
        'slow_pages_count': len(df[df['latency_ms'] > 1500])
    }
    
    # FIXED: Check canonical strictly ensuring return is boolean
    def check_canonical(row):
        if not row['canonical']: 
            return False
        return row['canonical'] != row['url']
        
    metrics['canon_issues_count'] = len(df[df.apply(check_canonical, axis=1)])
    
    missing_alt = 0
    for _, row in df.iterrows():
        if isinstance(row['images'], list):
            for img in row['images']:
                if not img.get('alt'):
                    missing_alt += 1
                    break
    metrics['missing_alt_count'] = missing_alt
    return metrics, df

# --- NLP ENGINE (GOOGLE) ---
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
        # Clean text slightly to avoid API errors on empty input
        if not text or len(text.strip()) < 50: return None, "Text too short for TextRazor."
        response = client.analyze(text)
        return response, None
    except Exception as e:
        return None, str(e)

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
metrics, df = get_metrics()

# TAB 1: SEO REPORT
with tab1:
    if metrics:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Pages", metrics['total_pages'])
        c2.metric("Indexable", metrics['indexable_count'])
        c3.metric("Broken (404)", metrics['broken_count'])
        c4.metric("Slow Pages", metrics['slow_pages_count'])
        
        # Display DataFrame
        st.dataframe(df, width=2000)
    else: st.info("Start a crawl first.")

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
                st.dataframe(pd.DataFrame(ents), width=2000)
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
                        st.dataframe(pd.DataFrame([{"Name": e.name, "Sal": f"{e.salience:.1%}"} for e in res_in['entities'][:5]]), width=500)
                    with c2:
                        st.subheader("Competitor")
                        st.metric("Sentiment", f"{res_ex['sentiment'].score:.2f}")
                        st.dataframe(pd.DataFrame([{"Name": e.name, "Sal": f"{e.salience:.1%}"} for e in res_ex['entities'][:5]]), width=500)

# TAB 3: SEARCH
with tab3:
    q = st.text_input("Deep Search:")
    if q and get_db_collection():
        res = list(get_db_collection().find({"page_text": {"$regex": q, "$options": "i"}}).limit(20))
        if res:
            data = [{"URL": r['url'], "Match": "..." + r['page_text'][r['page_text'].lower().find(q.lower()):][:100] + "..."} for r in res]
            st.dataframe(pd.DataFrame(data), width=2000)

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
                        st.dataframe(pd.DataFrame(ents), width=600)
                    with c2:
                        st.markdown("#### Top Topics")
                        tops = [{"Label": t.label, "Score": f"{t.score:.2f}"} for t in sorted(resp.topics(), key=lambda x: x.score, reverse=True)[:10]]
                        st.dataframe(pd.DataFrame(tops), width=600)
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
                    # 1. Get Texts
                    text_a = doc.get('page_text', '')
                    text_b, err_b = scrape_external_page(comp_url_tr)
                    
                    if text_a and text_b:
                        # 2. Analyze Both
                        resp_a, err_a = analyze_textrazor(text_a)
                        resp_b, err_b = analyze_textrazor(text_b)
                        
                        if resp_a and resp_b:
                            # 3. Extract Entity IDs
                            ents_a = {e.id for e in resp_a.entities()}
                            ents_b = {e.id for e in resp_b.entities()}
                            
                            common = list(ents_a.intersection(ents_b))
                            missing = list(ents_b - ents_a) # They have, we don't
                            unique = list(ents_a - ents_b)  # We have, they don't
                            
                            # 4. Display
                            c1, c2, c3 = st.columns(3)
                            
                            with c1:
                                st.success(f"✅ Common Entities ({len(common)})")
                                st.dataframe(pd.DataFrame(common, columns=["Entity ID"]), height=400, width=400)
                                
                            with c2:
                                st.error(f"⚠️ Content Gap (Missing) ({len(missing)})")
                                st.caption("Entities found in competitor but NOT in your page.")
                                st.dataframe(pd.DataFrame(missing, columns=["Entity ID"]), height=400, width=400)
                                
                            with c3:
                                st.info(f"🔹 Your Unique Edge ({len(unique)})")
                                st.caption("Entities found in your page but NOT in competitor.")
                                st.dataframe(pd.DataFrame(unique, columns=["Entity ID"]), height=400, width=400)
                        else:
                            st.error(f"Analysis Failed. Internal: {err_a}, External: {err_b}")
                    else:
                        st.error("Failed to retrieve text from one of the pages.")
