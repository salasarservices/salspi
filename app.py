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
import json
import urllib3
import graphviz 

# Suppress SSL warnings for robust crawling
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

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
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #4A90E2; 
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
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
    except Exception as e:
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
    """
    Standardizes URLs to ensure 'site.com/about' and 'site.com/about/' 
    are treated as the same page.
    """
    try:
        parsed = urlparse(url)
        # Reconstruct without fragment (#)
        clean = parsed._replace(fragment="").geturl()
        return clean.rstrip('/')
    except:
        return url

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None:
        st.error("Database unavailable. Check secrets.")
        return
    
    start_url = normalize_url(start_url)
    parsed_start = urlparse(start_url)
    # Allow both www and non-www variations of the domain
    base_domain = parsed_start.netloc.replace('www.', '')
    
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    debug_box = st.sidebar.empty() # For real-time feedback
    
    # Robust Headers to mimic a real Chrome browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive'
    }
    
    while queue and count < max_pages:
        url = queue.pop(0)
        
        if url in visited: continue
        visited.add(url)
        count += 1
        
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling {count}/{max_pages}: {url}")
        
        try:
            time.sleep(0.1) # Be polite
            start_time = time.time()
            
            # verify=False is crucial for some server configurations
            response = requests.get(url, headers=headers, timeout=20, verify=False)
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
                "word_count": 0,
                "content_hash": "",
                "indexable": True,
                "page_text": ""
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Metadata
                page_data['title'] = soup.title.string.strip() if soup.title and soup.title.string else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'].strip() if meta_desc and meta_desc.get('content') else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                # Content
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['word_count'] = len(text_content.split())
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['page_text'] = text_content
                
                # Images
                imgs = soup.find_all('img')
                for img in imgs:
                    src = img.get('src')
                    if src:
                        page_data['images'].append({
                            'src': urljoin(url, src),
                            'alt': img.get('alt', '')
                        })

                # Links
                all_links = soup.find_all('a', href=True)
                links_found = 0
                
                for link in all_links:
                    raw_link = link['href'].strip()
                    if not raw_link or raw_link.startswith(('mailto:', 'tel:', 'javascript:', '#')):
                        continue
                    
                    abs_link = urljoin(url, raw_link)
                    clean_link = normalize_url(abs_link)
                    
                    # Domain Check (Loose matching to catch subdomains/www)
                    link_parsed = urlparse(clean_link)
                    if base_domain in link_parsed.netloc:
                        page_data['links'].append(clean_link)
                        links_found += 1
                        
                        if clean_link not in visited and clean_link not in queue:
                            queue.append(clean_link)
                
                if count == 1 and links_found == 0:
                    debug_box.warning("⚠️ Warning: No internal links found on homepage. Site might be JavaScript-rendered.")

            collection.update_one({"url": final_url}, {"$set": page_data}, upsert=True)

        except Exception as e:
            # Log failure
            collection.update_one(
                {"url": url},
                {"$set": {"url": url, "status_code": 0, "error": str(e), "crawl_time": datetime.now()}},
                upsert=True
            )

    progress_bar.progress(100)
    status_text.success(f"Crawl Complete! Visited {count} pages.")

# --- ANALYZER ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None, None
    
    # Exclude heavy text field for performance
    data = list(col.find({}, {'_id': 0, 'page_text': 0}))
    df = pd.DataFrame(data)
    
    if df.empty: return None, None

    expected_cols = [
        'url', 'title', 'meta_desc', 'canonical', 'images', 
        'status_code', 'content_hash', 'indexable', 'latency_ms'
    ]
    for c in expected_cols:
        if c not in df.columns: df[c] = None

    df['meta_desc'] = df['meta_desc'].fillna("")
    df['title'] = df['title'].fillna("")
    df['canonical'] = df['canonical'].fillna("")
    df['content_hash'] = df['content_hash'].fillna("")
    
    metrics = {}
    metrics['total_pages'] = len(df)
    
    if 'content_hash' in df.columns:
        metrics['duplicate_content'] = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    else:
        metrics['duplicate_content'] = pd.DataFrame()

    metrics['duplicate_titles'] = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    metrics['duplicate_desc'] = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    
    def check_canonical(row):
        if not row['canonical']: return False
        return row['canonical'] != row['url']
    metrics['canonical_issues'] = df[df.apply(check_canonical, axis=1)]
    
    missing_alt_urls = []
    if 'images' in df.columns:
        for idx, row in df.iterrows():
            if isinstance(row['images'], list):
                for img in row['images']:
                    if isinstance(img, dict) and not img.get('alt'):
                        missing_alt_urls.append(row['url'])
                        break
    metrics['missing_alt'] = df[df['url'].isin(missing_alt_urls)]

    df['status_code'] = pd.to_numeric(df['status_code'], errors='coerce').fillna(0)
    metrics['status_3xx'] = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
    metrics['status_4xx'] = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
    metrics['status_5xx'] = df[df['status_code'] >= 500]
    
    metrics['indexable'] = df[df['indexable'] == True]
    metrics['non_indexable'] = df[df['indexable'] == False]
    
    df['latency_ms'] = pd.to_numeric(df['latency_ms'], errors='coerce').fillna(0)
    metrics['slow_pages'] = df[df['latency_ms'] > 2000]

    return metrics, df

# --- UI HELPER ---
def render_metric_card(label, value, df_subset, key_suffix):
    with st.container():
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)
        if value > 0:
            with st.expander(f"View Details"):
                limit = 50
                display_df = df_subset[['url', 'status_code']].head(limit)
                # UPDATED: Replaced use_container_width with width='stretch'
                st.dataframe(display_df, width=1000) 
                if len(df_subset) > limit:
                    st.caption(f"Showing top {limit} of {len(df_subset)} records.")

# --- SIDEBAR ---
with st.sidebar:
    st.header("Control Panel")
    
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if init_mongo_connection():
            st.success("DB: Online")
        else:
            st.error("DB: Offline")
    with col_s2:
        if google_auth_status:
            st.success("Auth: Active")
        else:
            st.warning("Auth: Inactive")

    st.markdown("---")
    
    target_url = st.text_input("Target URL", "https://example.com")
    max_pages_limit = st.number_input("Max Pages", 10, 2000, 50)
    
    if st.button("Start Crawl", type="primary"):
        crawl_site(target_url, max_pages_limit)
        st.rerun()
    
    if st.button("Clear Database"):
        coll = get_db_collection()
        if coll is not None:
            coll.delete_many({})
            st.success("Database Cleared")
            time.sleep(1)
            st.rerun()

# --- MAIN LOGIC ---
tab1, tab2, tab3 = st.tabs(["Crawl Report", "Site Structure", "Deep Search"])

col = get_db_collection()
has_data = False
if col is not None:
    try:
        if col.count_documents({}, limit=1) > 0:
            has_data = True
    except Exception:
        has_data = False

if has_data:
    metrics_data, full_df = get_metrics()
else:
    metrics_data, full_df = None, None

with tab1:
    if metrics_data:
        st.subheader("Site Health Overview")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Total Pages", metrics_data['total_pages'], full_df, "tot")
        with c2: render_metric_card("Indexable", len(metrics_data['indexable']), metrics_data['indexable'], "idx")
        with c3: render_metric_card("Non-Indexable", len(metrics_data['non_indexable']), metrics_data['non_indexable'], "nidx")
        with c4: render_metric_card("Slow (>2s)", len(metrics_data['slow_pages']), metrics_data['slow_pages'], "slow")

        st.subheader("Content Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Dup. Content", len(metrics_data['duplicate_content']), metrics_data['duplicate_content'], "dup_c")
        with c2: render_metric_card("Dup. Titles", len(metrics_data['duplicate_titles']), metrics_data['duplicate_titles'], "dup_t")
        with c3: render_metric_card("Dup. Desc", len(metrics_data['duplicate_desc']), metrics_data['duplicate_desc'], "dup_d")
        with c4: render_metric_card("Canonical Err", len(metrics_data['canonical_issues']), metrics_data['canonical_issues'], "canon")

        st.subheader("Technical Issues")
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Missing Alt", len(metrics_data['missing_alt']), metrics_data['missing_alt'], "alt")
        with c2: render_metric_card("3xx Redirects", len(metrics_data['status_3xx']), metrics_data['status_3xx'], "3xx")
        with c3: render_metric_card("4xx Errors", len(metrics_data['status_4xx']), metrics_data['status_4xx'], "4xx")
        with c4: render_metric_card("5xx Errors", len(metrics_data['status_5xx']), metrics_data['status_5xx'], "5xx")
    else:
        st.info("No crawl data found. Enter a URL in the sidebar and click 'Start Crawl'.")

# --- SITE STRUCTURE (GRAPHVIZ TREE) ---
with tab2:
    if full_df is not None:
        st.subheader("Site Architecture")
        st.info("Visualizing strict parent-child hierarchy using Graphviz.")
        
        # 1. IDENTIFY ROOT
        root_url = None
        if target_url:
            clean_target = normalize_url(target_url)
            if clean_target in full_df['url'].values:
                root_url = clean_target
        if not root_url and not full_df.empty:
            root_url = full_df['url'].iloc[0]

        # 2. INITIALIZE GRAPHVIZ
        graph = graphviz.Digraph(engine='dot')
        graph.attr(rankdir='TB') # Top to Bottom
        graph.attr(splines='ortho') # Squared lines like organization chart
        # Clean Node Styling
        graph.attr('node', shape='rect', style='filled, rounded', fontname='Arial', fontsize='10', height='0.4')
        graph.attr('edge', color='#888888')

        if root_url:
            queue = [root_url]
            visited_in_tree = {root_url}
            limit_nodes = 200 # Safety Limit
            
            # Root Styling
            graph.node(root_url, label='Home', fillcolor='#0047AB', fontcolor='white')
            
            nodes_count = 1
            
            # BFS Traversal to build unique Tree
            while queue and nodes_count < limit_nodes:
                curr_url = queue.pop(0)
                
                # Get DB Data
                row = full_df[full_df['url'] == curr_url]
                if row.empty: continue
                links = row.iloc[0]['links']
                
                if isinstance(links, list):
                    for link in links:
                        # CORE LOGIC: Add child only if not already in tree
                        if link in full_df['url'].values and link not in visited_in_tree:
                            visited_in_tree.add(link)
                            queue.append(link)
                            nodes_count += 1
                            
                            # Child Styling
                            link_row = full_df[full_df['url'] == link].iloc[0]
                            code = link_row['status_code']
                            
                            color = '#E8F4FA' # Default
                            if code >= 400: color = '#FFD2D2' # Red
                            elif 300 <= code < 400: color = '#FFF4D2' # Orange
                                
                            # Clean Label
                            path = urlparse(link).path.strip('/')
                            label = path[:20] + '...' if len(path) > 20 else path
                            if not label: label = "Page"
                            
                            graph.node(link, label=label, fillcolor=color)
                            graph.edge(curr_url, link)

            if nodes_count >= limit_nodes:
                st.warning(f"Graph limited to top {limit_nodes} pages.")

            # 3. RENDER
            st.graphviz_chart(graph)
        
        else:
            st.warning("Could not identify a root page.")
    else:
        st.warning("Data needed for visualization.")

# --- DEEP SEARCH ---
with tab3:
    st.subheader("Search Website Content")
    st.markdown("Search for words inside the main content of crawled pages.")
    
    query = st.text_input("Enter search phrase:", placeholder="e.g. contact us")
    
    search_col = get_db_collection()
    if query and search_col is not None:
        try:
            results = list(search_col.find(
                {"page_text": {"$regex": query, "$options": "i"}},
                {"url": 1, "page_text": 1, "_id": 0}
            ).limit(50))
            
            if results:
                st.success(f"✅ Found **{len(results)}** pages.")
                search_data = []
                for res in results:
                    text = res.get('page_text', '')
                    idx = text.lower().find(query.lower())
                    if idx != -1:
                        start = max(0, idx - 50)
                        end = min(len(text), idx + len(query) + 50)
                        snippet = "..." + text[start:end] + "..."
                    else:
                        snippet = "Match found (text hidden)"
                    search_data.append({"URL": res['url'], "Context": snippet})
                # UPDATED: Width fix
                st.dataframe(pd.DataFrame(search_data), width=1000)
            else:
                st.warning(f"No pages found containing '{query}'.")
        except Exception as e:
            st.error(f"Search failed: {e}")
