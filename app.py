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
from datetime import datetime
import urllib3

# --- SAFE IMPORTS ---
# This prevents the app from crashing instantly if a library is missing
try:
    from google.cloud import language_v1
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro + NLP", page_icon="🧠", layout="wide")

st.markdown("""
<style>
    .metric-card {
        background-color: #F0F2F6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        margin-bottom: 10px;
        border: 1px solid #E0E0E0;
    }
    .metric-value { font-size: 32px; font-weight: bold; color: #4A90E2; }
    .metric-label { font-size: 14px; color: #666; }
    .stButton>button { width: 100%; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# --- AUTHENTICATION ---
def setup_google_auth():
    if "google" in st.secrets and "credentials" in st.secrets["google"]:
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(st.secrets["google"]["credentials"])
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

# --- NLP ENGINE ---
def analyze_content(text):
    """Calls Google Cloud Natural Language API"""
    if not NLP_AVAILABLE:
        return None, "Library 'google-cloud-language' is missing."

    try:
        client = language_v1.LanguageServiceClient()
        
        # Google NLP requires text to be non-empty
        if not text or len(text.split()) < 20:
            return None, "Text too short for analysis."

        document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
        
        # 1. Analyze Sentiment
        sentiment = client.analyze_sentiment(request={'document': document}).document_sentiment
        
        # 2. Analyze Entities
        entities = client.analyze_entities(request={'document': document}).entities
        
        # 3. Classify Content
        try:
            categories = client.classify_text(request={'document': document}).categories
        except:
            categories = [] 

        return {
            "sentiment_score": sentiment.score,
            "sentiment_magnitude": sentiment.magnitude,
            "entities": entities,
            "categories": categories
        }, None

    except Exception as e:
        return None, str(e)

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
    
    start_url = normalize_url(start_url)
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc.replace('www.', '')
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
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
        status_text.text(f"Crawling {count}: {url}")
        
        try:
            time.sleep(0.1)
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            final_url = normalize_url(response.url)
            
            page_data = {
                "url": final_url,
                "domain": base_domain,
                "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''),
                "crawl_time": datetime.now(),
                "latency_ms": response.elapsed.total_seconds() * 1000,
                "links": [],
                "images": [],
                "title": "",
                "meta_desc": "",
                "canonical": "",
                "page_text": ""
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                # Clean text for NLP
                for script in soup(["script", "style"]):
                    script.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({'src': urljoin(url, src), 'alt': img.get('alt', '')})

                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    raw = link['href'].strip()
                    if not raw or raw.startswith(('mailto:', 'tel:', 'javascript:', '#')): continue
                    abs_link = normalize_url(urljoin(url, raw))
                    if base_domain in urlparse(abs_link).netloc:
                        page_data['links'].append(abs_link)
                        if abs_link not in visited and abs_link not in queue:
                            queue.append(abs_link)
                            
            collection.update_one({"url": final_url}, {"$set": page_data}, upsert=True)

        except Exception as e:
            collection.update_one({"url": url}, {"$set": {"url": url, "status_code": 0, "error": str(e)}}, upsert=True)

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- ANALYZER ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    
    # Exclude page_text to allow large DBs to load fast
    data = list(col.find({}, {'page_text': 0, '_id': 0}))
    df = pd.DataFrame(data)
    if df.empty: return None, None

    cols = ['url', 'title', 'meta_desc', 'canonical', 'images', 'status_code']
    for c in cols: 
        if c not in df.columns: df[c] = None
        
    metrics = {
        'total_pages': len(df),
        'status_200': len(df[df['status_code'] == 200]),
        'status_4xx': len(df[(df['status_code'] >= 400) & (df['status_code'] < 500)]),
        'status_5xx': len(df[df['status_code'] >= 500]),
        'missing_title': len(df[df['title'].isna() | (df['title'] == "")]),
        'missing_desc': len(df[df['meta_desc'].isna() | (df['meta_desc'] == "")])
    }
    return metrics, df

# --- UI COMPONENTS ---
def render_metric_card(label, value):
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{value}</div>
        <div class="metric-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

# --- SIDEBAR ---
with st.sidebar:
    st.header("Control Panel")
    
    c1, c2 = st.columns(2)
    with c1:
        if init_mongo_connection(): st.success("DB: Online")
        else: st.error("DB: Offline")
    with c2:
        if google_auth_status: st.success("NLP: Ready")
        else: st.warning("NLP: No Auth")

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
tab1, tab2, tab3 = st.tabs(["📊 Crawl Report", "🧠 NLP Analysis", "🔍 Deep Search"])

metrics, df = get_metrics()

if not NLP_AVAILABLE:
    st.error("⚠️ Library `google-cloud-language` not found. Please check requirements.txt")

# TAB 1: CRAWL REPORT
with tab1:
    if metrics:
        st.subheader("Site Health")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Total Pages", metrics['total_pages'])
        with c2: render_metric_card("Healthy (200)", metrics['status_200'])
        with c3: render_metric_card("Client Errors", metrics['status_4xx'])
        with c4: render_metric_card("Server Errors", metrics['status_5xx'])
        
        st.subheader("Metadata Issues")
        c1, c2 = st.columns(2)
        with c1: render_metric_card("Missing Titles", metrics['missing_title'])
        with c2: render_metric_card("Missing Desc", metrics['missing_desc'])
        
        with st.expander("View Full Crawl Data"):
            st.dataframe(df, width=1000)
    else:
        st.info("No data. Start a crawl first.")

# TAB 2: GOOGLE NLP ANALYSIS
with tab2:
    st.subheader("Google Cloud Natural Language Analysis")
    
    if metrics and google_auth_status and NLP_AVAILABLE:
        selected_url = st.selectbox("Select a page to analyze:", df['url'].unique())
        
        if st.button("🚀 Analyze Page Content"):
            with st.spinner("Calling Google Cloud NLP API..."):
                col = get_db_collection()
                doc = col.find_one({"url": selected_url}, {"page_text": 1})
                text_content = doc.get('page_text', "") if doc else ""
                
                results, error = analyze_content(text_content)
                
                if error:
                    st.error(f"Analysis Failed: {error}")
                elif results:
                    st.markdown("#### 1. Sentiment Analysis")
                    score = results['sentiment_score']
                    mag = results['sentiment_magnitude']
                    
                    c1, c2, c3 = st.columns(3)
                    with c1: st.metric("Sentiment Score", f"{score:.2f}")
                    with c2: st.metric("Magnitude", f"{mag:.2f}")
                    with c3:
                        if score > 0.25: st.success("Positive 😊")
                        elif score < -0.25: st.error("Negative 😠")
                        else: st.info("Neutral 😐")

                    st.divider()
                    st.markdown("#### 2. Content Classification")
                    if results['categories']:
                        for cat in results['categories']:
                            st.info(f"📂 **{cat.name}** (Confidence: {cat.confidence:.2f})")
                    else:
                        st.caption("No categories detected.")
                    
                    st.divider()
                    st.markdown("#### 3. Key Entities Detected")
                    
                    entity_data = []
                    for entity in results['entities']:
                        entity_data.append({
                            "Name": entity.name,
                            "Type": language_v1.Entity.Type(entity.type_).name,
                            "Salience": f"{entity.salience:.2%}"
                        })
                    
                    if entity_data:
                        entity_df = pd.DataFrame(entity_data).sort_values("Salience", ascending=False).head(10)
                        st.table(entity_df)
                    else:
                        st.caption("No entities found.")
                        
    elif not google_auth_status:
        st.error("⚠️ Google Cloud Credentials not found. Please check your secrets.toml file.")
    elif not NLP_AVAILABLE:
        st.error("⚠️ NLP Library missing.")
    else:
        st.warning("Please crawl the site first.")

# TAB 3: DEEP SEARCH
with tab3:
    st.subheader("Deep Content Search")
    query = st.text_input("Search phrase:", placeholder="e.g. shipping policy")
    
    if query:
        col = get_db_collection()
        if col:
            results = list(col.find(
                {"page_text": {"$regex": query, "$options": "i"}},
                {"url": 1, "page_text": 1, "_id": 0}
            ).limit(20))
            
            if results:
                st.success(f"Found {len(results)} matches.")
                search_res = []
                for res in results:
                    text = res.get('page_text', '')
                    idx = text.lower().find(query.lower())
                    snippet = text[max(0, idx-40):min(len(text), idx+len(query)+40)]
                    search_res.append({"URL": res['url'], "Snippet": f"...{snippet}..."})
                st.dataframe(pd.DataFrame(search_res), width=1000)
            else:
                st.warning("No matches found.")
