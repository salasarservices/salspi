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
import graphviz

# --- SAFE IMPORTS ---
try:
    from google.cloud import language_v1
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro Ultimate", page_icon="🕸️", layout="wide")

st.markdown("""
<style>
    .metric-card {
        background-color: #eaf2f8; /* Applied pastel color here */
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        margin-bottom: 10px;
        border: 1px solid #dce4ec; /* Softer border to match */
        height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .metric-value { font-size: 28px; font-weight: bold; color: #4A90E2; }
    .metric-label { font-size: 14px; color: #666; margin-top: 5px;}
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
    
    # Reset DB for fresh crawl
    collection.delete_many({})
    
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
                
                # Metadata extraction
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                # Text Content & Hash
                for script in soup(["script", "style"]):
                    script.extract()
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['page_text'] = text_content
                page_data['content_hash'] = get_page_hash(text_content)
                
                # Images
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({
                            'src': urljoin(url, src),
                            'alt': img.get('alt', '') # Empty string if missing
                        })

                # Robots / Indexable check
                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                # Links
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

# --- ANALYZER (RESTORED METRICS) ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    
    # Exclude heavy page_text for the report
    data = list(col.find({}, {'page_text': 0, '_id': 0}))
    df = pd.DataFrame(data)
    if df.empty: return None, None

    # Ensure columns exist
    expected_cols = ['url', 'title', 'meta_desc', 'canonical', 'images', 'status_code', 'content_hash', 'latency_ms', 'indexable']
    for c in expected_cols:
        if c not in df.columns: df[c] = None

    # Pre-processing
    df['title'] = df['title'].fillna("")
    df['meta_desc'] = df['meta_desc'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    
    metrics = {}
    
    # 1.1 Total Pages
    metrics['total_pages'] = len(df)
    
    # 1.2 Duplicate Content
    # We ignore empty content pages (like errors)
    content_dupes = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    metrics['dup_content_count'] = len(content_dupes)
    metrics['dup_content_df'] = content_dupes
    
    # 1.3 Duplicate Titles
    title_dupes = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    metrics['dup_title_count'] = len(title_dupes)
    metrics['dup_title_df'] = title_dupes
    
    # 1.4 Duplicate Descriptions
    desc_dupes = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    metrics['dup_desc_count'] = len(desc_dupes)
    metrics['dup_desc_df'] = desc_dupes
    
    # 1.5 Canonical Issues (Self-referencing mismatch)
    def check_canonical(row):
        if not row['canonical']: return False # Missing canonical is an issue too, but we check mismatch here
        return row['canonical'] != row['url']
    canon_issues = df[df.apply(check_canonical, axis=1)]
    metrics['canon_issues_count'] = len(canon_issues)
    metrics['canon_issues_df'] = canon_issues
    
    # 1.6 Missing Alt Tags
    missing_alt_urls = []
    for _, row in df.iterrows():
        if isinstance(row['images'], list):
            for img in row['images']:
                if not img.get('alt'):
                    missing_alt_urls.append(row['url'])
                    break
    metrics['missing_alt_count'] = len(missing_alt_urls)
    metrics['missing_alt_df'] = df[df['url'].isin(missing_alt_urls)]
    
    # 1.7 Broken Pages (404)
    broken = df[df['status_code'] == 404]
    metrics['broken_count'] = len(broken)
    
    # 1.8 - 1.10 Status Codes
    metrics['3xx_count'] = len(df[(df['status_code'] >= 300) & (df['status_code'] < 400)])
    metrics['4xx_count'] = len(df[(df['status_code'] >= 400) & (df['status_code'] < 500)])
    metrics['5xx_count'] = len(df[df['status_code'] >= 500])
    
    # 1.11 - 1.12 Indexability
    metrics['indexable_count'] = len(df[df['indexable'] == True])
    metrics['non_indexable_count'] = len(df[df['indexable'] == False])
    
    # 1.15 PageSpeed (Proxy via Latency)
    slow_pages = df[df['latency_ms'] > 1500] # Threshold 1.5s
    metrics['slow_pages_count'] = len(slow_pages)
    metrics['slow_pages_df'] = slow_pages

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

# --- UI COMPONENTS ---
def render_metric_card(label, value, df_subset=None):
    with st.container():
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)
        if df_subset is not None and not df_subset.empty:
            with st.expander("Details"):
                st.dataframe(df_subset[['url', 'title']].head(20), width=1000)

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
tab1, tab2, tab3, tab4 = st.tabs(["📊 SEO Report", "🏗️ Architecture", "🧠 NLP Analysis", "🔍 Search"])

metrics, df = get_metrics()

# TAB 1: SEO REPORT
with tab1:
    if metrics:
        st.subheader("1. Crawl Overview")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Total Pages", metrics['total_pages'])
        with c2: render_metric_card("Indexable", metrics['indexable_count'])
        with c3: render_metric_card("Non-Indexable", metrics['non_indexable_count'])
        with c4: render_metric_card("Slow Pages (>1.5s)", metrics['slow_pages_count'], metrics['slow_pages_df'])

        st.subheader("2. Content Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Duplicate Content", metrics['dup_content_count'], metrics['dup_content_df'])
        with c2: render_metric_card("Duplicate Titles", metrics['dup_title_count'], metrics['dup_title_df'])
        with c3: render_metric_card("Duplicate Desc", metrics['dup_desc_count'], metrics['dup_desc_df'])
        with c4: render_metric_card("Canonical Issues", metrics['canon_issues_count'], metrics['canon_issues_df'])

        st.subheader("3. Technical Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Missing Alt Tags", metrics['missing_alt_count'], metrics['missing_alt_df'])
        with c2: render_metric_card("Broken Pages (404)", metrics['broken_count'])
        with c3: render_metric_card("3xx Redirects", metrics['3xx_count'])
        with c4: render_metric_card("5xx Errors", metrics['5xx_count'])
    else:
        st.info("No data. Start a crawl first.")

# TAB 2: SITE STRUCTURE (GRAPHVIZ)
with tab2:
    st.subheader("Site Hierarchy Tree")
    if df is not None:
        graph = graphviz.Digraph(engine='dot')
        graph.attr(rankdir='TB', splines='ortho')
        graph.attr('node', shape='rect', style='filled, rounded', fontname='Arial', fontsize='10', height='0.4')
        graph.attr('edge', color='#888888')

        root_url = None
        if target_url and normalize_url(target_url) in df['url'].values:
            root_url = normalize_url(target_url)
        elif not df.empty:
            root_url = df['url'].iloc[0]

        if root_url:
            queue = [root_url]
            visited = {root_url}
            limit = 200
            
            graph.node(root_url, label='Home', fillcolor='#0047AB', fontcolor='white')
            
            count = 1
            while queue and count < limit:
                curr = queue.pop(0)
                row = df[df['url'] == curr]
                if row.empty: continue
                links = row.iloc[0]['links']
                if isinstance(links, list):
                    for link in links:
                        if link in df['url'].values and link not in visited:
                            visited.add(link)
                            queue.append(link)
                            count += 1
                            
                            # Node Logic
                            l_row = df[df['url'] == link].iloc[0]
                            color = '#E8F4FA'
                            if l_row['status_code'] >= 400: color = '#FFD2D2'
                            
                            path = urlparse(link).path.strip('/')
                            label = path[:15] + '...' if len(path) > 15 else path
                            if not label: label = "Page"
                            
                            graph.node(link, label=label, fillcolor=color)
                            graph.edge(curr, link)
            st.graphviz_chart(graph)
    else:
        st.warning("Crawl data needed.")

# TAB 3: NLP
with tab3:
    st.subheader("Content Analysis")
    if df is not None and google_auth_status and NLP_AVAILABLE:
        url_sel = st.selectbox("Select Page:", df['url'].unique())
        if st.button("Analyze"):
            col = get_db_collection()
            doc = col.find_one({"url": url_sel}, {"page_text": 1})
            res, err = analyze_content(doc.get('page_text', ''))
            
            if res:
                s = res['sentiment']
                c1, c2 = st.columns(2)
                c1.metric("Sentiment", f"{s.score:.2f}")
                c2.metric("Magnitude", f"{s.magnitude:.2f}")
                
                st.write("**Top Entities:**")
                e_data = [{"Name": e.name, "Type": language_v1.Entity.Type(e.type_).name, "Salience": f"{e.salience:.2%}"} for e in res['entities'][:10]]
                st.table(pd.DataFrame(e_data))
            else:
                st.error(err)

# TAB 4: SEARCH
with tab4:
    st.subheader("Deep Search")
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
            st.dataframe(pd.DataFrame(data), width=1000)
        else:
            st.warning("No matches.")
