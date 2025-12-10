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

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

# PASTEL THEME CSS
st.markdown("""
<style>
    /* Metric Cards */
    div[data-testid="metric-container"] {
        background-color: #F0F4F8; /* Pastel Blue-Grey */
        border: 1px solid #D9E2EC;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        color: #102A43;
    }
    div[data-testid="metric-container"] label {
        color: #486581; /* Muted Blue text */
        font-size: 0.9rem;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #334E68;
        font-size: 1.8rem;
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
</style>
""", unsafe_allow_html=True)

# --- AUTHENTICATION (ROBUST) ---
def setup_google_auth():
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            creds = st.secrets["google"]["credentials"]
            
            # Handle if it's a JSON string (Old format)
            if isinstance(creds, str):
                try:
                    creds = json.loads(creds)
                except json.JSONDecodeError:
                    return False
            
            # Handle if it's a Streamlit AttrDict (New, correct format)
            # We convert to a standard dict to ensure json.dump works
            creds_dict = dict(creds)

            # Dump to a temporary file for the Google Library to read
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                json.dump(creds_dict, f)
                temp_cred_path = f.name
            
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
            return True
        except Exception:
            return False
    return False

google_auth_status = setup_google_auth()

# --- MONGODB CONNECTION ---
@st.cache_resource(show_spinner=False)
def init_mongo_connection():
    try:
        uri = st.secrets["mongo"]["uri"]
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception:
        return None

def get_db_collection():
    client = init_mongo_connection()
    if client:
        try:
            db_name = st.secrets["mongo"]["db"]
            coll_name = st.secrets["mongo"]["collection"]
            db = client[db_name]
            return db[coll_name]
        except KeyError:
            return None
    return None

# --- CRAWLER ENGINE ---
def get_page_hash(content):
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def normalize_url(url):
    try:
        parsed = urlparse(url)
        clean = parsed._replace(fragment="").geturl()
        return clean.rstrip('/')
    except:
        return url

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None:
        st.error("Database unavailable.")
        return
    
    collection.delete_many({})
    
    start_url = normalize_url(start_url)
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc.replace('www.', '') 
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    debug_log = st.sidebar.empty()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
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
                "url": final_url,
                "domain": base_domain,
                "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''),
                "crawl_time": datetime.now(),
                "latency_ms": latency,
                "links": [],
                "images": [],
                "title": "",
                "meta_desc": "",
                "canonical": "",
                "page_text": "",
                "content_hash": "",
                "indexable": True
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                for script in soup(["script", "style"]):
                    script.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                page_data['content_hash'] = get_page_hash(text_content)
                
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({
                            'src': urljoin(url, src),
                            'alt': img.get('alt', '')
                        })

                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                all_links = soup.find_all('a', href=True)
                internal_links_found = 0
                for link in all_links:
                    raw = link['href'].strip()
                    if not raw or raw.startswith(('mailto:', 'tel:', 'javascript:', '#')): continue
                    abs_link = normalize_url(urljoin(url, raw))
                    
                    if base_domain in urlparse(abs_link).netloc:
                        page_data['links'].append(abs_link)
                        internal_links_found += 1
                        if abs_link not in visited and abs_link not in queue:
                            queue.append(abs_link)
                
                if count == 1:
                    debug_log.info(f"Root: Found {internal_links_found} internal links.")

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

    df['title'] = df['title'].fillna("")
    df['meta_desc'] = df['meta_desc'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    
    metrics = {}
    metrics['total_pages'] = len(df)
    
    content_dupes = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    metrics['dup_content_count'] = len(content_dupes)
    
    title_dupes = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    metrics['dup_title_count'] = len(title_dupes)
    
    desc_dupes = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    metrics['dup_desc_count'] = len(desc_dupes)
    
    def check_canonical(row):
        if not row['canonical']: return False 
        return row['canonical'] != row['url']
    canon_issues = df[df.apply(check_canonical, axis=1)]
    metrics['canon_issues_count'] = len(canon_issues)
    
    missing_alt_urls = []
    for _, row in df.iterrows():
        if isinstance(row['images'], list):
            for img in row['images']:
                if not img.get('alt'):
                    missing_alt_urls.append(row['url'])
                    break
    metrics['missing_alt_count'] = len(missing_alt_urls)
    
    metrics['broken_count'] = len(df[df['status_code'] == 404])
    metrics['3xx_count'] = len(df[(df['status_code'] >= 300) & (df['status_code'] < 400)])
    metrics['4xx_count'] = len(df[(df['status_code'] >= 400) & (df['status_code'] < 500)])
    metrics['5xx_count'] = len(df[df['status_code'] >= 500])
    
    metrics['indexable_count'] = len(df[df['indexable'] == True])
    metrics['non_indexable_count'] = len(df[df['indexable'] == False])
    
    metrics['slow_pages_count'] = len(df[df['latency_ms'] > 1500])

    return metrics, df

# --- NLP ENGINE ---
def analyze_content(text):
    if not NLP_AVAILABLE: return None, "Library missing."
    try:
        client = language_v1.LanguageServiceClient()
        if not text or len(text.split()) < 20: return None, "Text too short."
        document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(request={'document': document}).document_sentiment
        entities = client.analyze_entities(request={'document': document}).entities
        return {"sentiment": sentiment, "entities": entities}, None
    except Exception as e:
        return None, str(e)

# --- SIDEBAR ---
with st.sidebar:
    st.header("Control Panel")
    c1, c2 = st.columns(2)
    with c1:
        if init_mongo_connection(): st.success("DB: Online")
        else: st.error("DB: Offline")
    with c2:
        if google_auth_status: st.success("NLP: Ready")
        else: st.warning("NLP: Inactive")

    st.markdown("---")
    target_url = st.text_input("Target URL", "https://example.com")
    max_pages = st.number_input("Max Pages", 10, 500, 50)
    
    if st.button("Start Crawl", type="primary"):
        crawl_site(target_url, max_pages)
        st.rerun()
    
    if st.button("Clear DB"):
        col = get_db_collection()
        if col is not None: 
            col.delete_many({})
            st.success("Cleared!")
            time.sleep(1)
            st.rerun()

# --- MAIN APP ---
tab1, tab2, tab3 = st.tabs(["📊 SEO Report", "🧠 NLP Analysis", "🔍 Search"])

metrics, df = get_metrics()

# TAB 1: SEO REPORT
with tab1:
    if metrics:
        st.subheader("1. Crawl Overview")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Pages", metrics['total_pages'])
        c2.metric("Indexable", metrics['indexable_count'])
        c3.metric("Non-Indexable", metrics['non_indexable_count'])
        c4.metric("Slow Pages", metrics['slow_pages_count'])

        st.subheader("2. Content Issues")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Dup. Content", metrics['dup_content_count'])
        c2.metric("Dup. Titles", metrics['dup_title_count'])
        c3.metric("Dup. Desc", metrics['dup_desc_count'])
        c4.metric("Canonical Err", metrics['canon_issues_count'])

        st.subheader("3. Technical Issues")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Missing Alt", metrics['missing_alt_count'])
        c2.metric("Broken (404)", metrics['broken_count'])
        c3.metric("Redirects (3xx)", metrics['3xx_count'])
        c4.metric("Errors (5xx)", metrics['5xx_count'])
        
        with st.expander("View Full Data Table"):
            st.dataframe(df, width=1500) # Changed to fixed int width or use "stretch" if valid in your version
    else:
        st.info("No data found. Start a crawl.")

# TAB 2: NLP
with tab2:
    st.subheader("Content Intelligence")
    if df is not None and google_auth_status and NLP_AVAILABLE:
        url_sel = st.selectbox("Select Page:", df['url'].unique())
        if st.button("Analyze Content"):
            col = get_db_collection()
            doc = col.find_one({"url": url_sel}, {"page_text": 1})
            res, err = analyze_content(doc.get('page_text', ''))
            
            if res:
                s = res['sentiment']
                c1, c2 = st.columns(2)
                c1.metric("Sentiment Score", f"{s.score:.2f}")
                c2.metric("Magnitude", f"{s.magnitude:.2f}")
                
                st.write("#### Top Entities")
                ents = [{"Name": e.name, "Type": language_v1.Entity.Type(e.type_).name, "Salience": f"{e.salience:.2%}"} for e in res['entities'][:10]]
                # Updated dataframe width
                st.dataframe(pd.DataFrame(ents), use_container_width=True) 
            else:
                st.error(err)
    elif not NLP_AVAILABLE:
        st.warning("NLP Library missing. Install google-cloud-language")
    elif not google_auth_status:
        st.warning("Google Auth missing. Check secrets.")

# TAB 3: SEARCH
with tab3:
    st.subheader("Deep Content Search")
    q = st.text_input("Query:")
    if q and get_db_collection():
        res = list(get_db_collection().find({"page_text": {"$regex": q, "$options": "i"}}, {"url": 1, "page_text": 1}).limit(20))
        if res:
            data = []
            for r in res:
                txt = r.get('page_text', '')
                idx = txt.lower().find(q.lower())
                snip = txt[max(0, idx-40):min(len(txt), idx+len(q)+40)]
                data.append({"URL": r['url'], "Context": f"...{snip}..."})
            st.dataframe(pd.DataFrame(data), use_container_width=True)
        else:
            st.warning("No matches.")
