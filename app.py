import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pymongo
import networkx as nx
from pyvis.network import Network
import tempfile
import time
import hashlib
from datetime import datetime

# --- CONFIGURATION & STYLING ---
st.set_page_config(page_title="SeoSpider Pro", page_icon="🕸️", layout="wide")

# Custom CSS for Pastel Colors and Minimalist UI
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
        color: #4A90E2; /* Pastel Blue */
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
    /* Status Code Colors */
    .status-200 { color: #77DD77; font-weight: bold; } /* Pastel Green */
    .status-300 { color: #FFB347; font-weight: bold; } /* Pastel Orange */
    .status-400 { color: #FF6961; font-weight: bold; } /* Pastel Red */
    .status-500 { color: #B19CD9; font-weight: bold; } /* Pastel Purple */
</style>
""", unsafe_allow_html=True)

# --- MONGODB CONNECTION ---
# Replace with your actual connection string or use st.secrets
# Example: "mongodb+srv://<user>:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority"
MONGO_URI = st.sidebar.text_input("MongoDB Connection URI", value="mongodb://localhost:27017/", type="password")
DB_NAME = "seo_crawler_db"
COLLECTION_NAME = "crawled_pages"

def get_db_collection():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db[COLLECTION_NAME]
    except Exception as e:
        st.sidebar.error(f"DB Connection Error: {e}")
        return None

# --- CRAWLER ENGINE ---
def get_page_hash(content):
    """Generate MD5 hash of content to check for duplicates"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def crawl_site(start_url, max_pages):
    collection = get_db_collection()
    if collection is None: return
    
    # Reset DB for new crawl
    collection.delete_many({})
    
    domain = urlparse(start_url).netloc
    queue = [start_url]
    visited = set()
    count = 0
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while queue and count < max_pages:
        url = queue.pop(0)
        if url in visited: continue
        
        visited.add(url)
        count += 1
        
        # UI Update
        progress_bar.progress(count / max_pages)
        status_text.text(f"Crawling: {url}")
        
        try:
            start_time = time.time()
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            latency = (time.time() - start_time) * 1000 # ms
            
            page_data = {
                "url": url,
                "status_code": response.status_code,
                "content_type": response.headers.get('Content-Type', ''),
                "crawl_time": datetime.now(),
                "latency_ms": latency,
                "links": [],
                "images": [],
                "h1": [],
                "h2": [],
                "title": "",
                "meta_desc": "",
                "canonical": "",
                "word_count": 0,
                "content_hash": "",
                "indexable": True
            }

            if response.status_code == 200 and 'text/html' in page_data['content_type']:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Metadata
                page_data['title'] = soup.title.string if soup.title else ""
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_desc'] = meta_desc['content'] if meta_desc else ""
                canonical = soup.find('link', rel='canonical')
                page_data['canonical'] = canonical['href'] if canonical else ""
                
                # Headings
                page_data['h1'] = [h.get_text(strip=True) for h in soup.find_all('h1')]
                page_data['h2'] = [h.get_text(strip=True) for h in soup.find_all('h2')]
                
                # Content & Duplicate Check
                text_content = soup.get_text(separator=' ', strip=True)
                page_data['word_count'] = len(text_content.split())
                page_data['content_hash'] = get_page_hash(text_content)
                page_data['page_text'] = text_content # Storing full text for search

                # Images
                imgs = soup.find_all('img')
                for img in imgs:
                    page_data['images'].append({
                        'src': img.get('src'),
                        'alt': img.get('alt', '')
                    })

                # Robots / Indexability (Simplified)
                robots_meta = soup.find('meta', attrs={'name': 'robots'})
                if robots_meta and 'noindex' in robots_meta.get('content', '').lower():
                    page_data['indexable'] = False

                # Extract Links for recursion and structure
                for link in soup.find_all('a', href=True):
                    abs_link = urljoin(url, link['href'])
                    # Normalize
                    abs_link = abs_link.split('#')[0].rstrip('/')
                    
                    if urlparse(abs_link).netloc == domain:
                        page_data['links'].append(abs_link)
                        if abs_link not in visited and abs_link not in queue:
                            queue.append(abs_link)
                            
            # Insert into Mongo
            collection.insert_one(page_data)

        except Exception as e:
            # Log failed pages
            collection.insert_one({
                "url": url,
                "status_code": 0, # Error
                "error": str(e)
            })

    progress_bar.progress(100)
    status_text.success("Crawl Complete!")

# --- ANALYZER ---
def get_metrics():
    col = get_db_collection()
    if col is None: return None
    
    df = pd.DataFrame(list(col.find({}, {'_id': 0})))
    if df.empty: return None
    
    metrics = {}
    
    # 1.1 Total Pages
    metrics['total_pages'] = len(df)
    
    # 1.2 Duplicate Content Pages
    if 'content_hash' in df.columns:
        metrics['duplicate_content'] = df[df.duplicated(subset=['content_hash'], keep=False) & (df['content_hash'] != "")]
    else:
        metrics['duplicate_content'] = pd.DataFrame()

    # 1.3 Duplicate Titles
    metrics['duplicate_titles'] = df[df.duplicated(subset=['title'], keep=False) & (df['title'] != "")]
    
    # 1.4 Duplicate Descriptions
    metrics['duplicate_desc'] = df[df.duplicated(subset=['meta_desc'], keep=False) & (df['meta_desc'] != "")]
    
    # 1.5 Canonical Issues (Self-referencing mismatch or missing)
    # Simplified logic: If canonical exists but doesn't match URL
    def check_canonical(row):
        return row['canonical'] and row['canonical'] != row['url']
    metrics['canonical_issues'] = df[df.apply(check_canonical, axis=1)] if 'canonical' in df.columns else pd.DataFrame()
    
    # 1.6 Images Missing Alt
    missing_alt_urls = []
    if 'images' in df.columns:
        for idx, row in df.iterrows():
            if isinstance(row['images'], list):
                for img in row['images']:
                    if not img.get('alt'):
                        missing_alt_urls.append(row['url'])
                        break
    metrics['missing_alt'] = df[df['url'].isin(missing_alt_urls)]

    # 1.7 Broken Links (Based on 404s found during crawl)
    # Note: To find broken OUTBOUND links requires deeper parsing, here we count broken INTERNAL pages found.
    metrics['broken_pages'] = df[df['status_code'] == 404]

    # 1.8 - 1.10 Status Codes
    metrics['status_3xx'] = df[(df['status_code'] >= 300) & (df['status_code'] < 400)]
    metrics['status_4xx'] = df[(df['status_code'] >= 400) & (df['status_code'] < 500)]
    metrics['status_5xx'] = df[df['status_code'] >= 500]

    # 1.11 - 1.12 Indexability
    metrics['indexable'] = df[df['indexable'] == True]
    metrics['non_indexable'] = df[df['indexable'] == False]

    # 1.15 PageSpeed (Mocked for Demo - requires API)
    # We use latency as a proxy
    metrics['slow_pages'] = df[df['latency_ms'] > 2000] # Pages taking > 2s

    return metrics, df

# --- VISUALIZER ---
def generate_network_graph(df):
    G = nx.DiGraph()
    
    for _, row in df.iterrows():
        G.add_node(row['url'], title=row['title'], group=row['status_code'])
        if isinstance(row['links'], list):
            for link in row['links']:
                # Only add edge if target also in df (internal linking structure)
                if link in df['url'].values:
                    G.add_edge(row['url'], link)
    
    return G

# --- UI COMPONENTS ---
def render_metric_card(label, value, df_subset, key_suffix):
    """Renders a metric that can be expanded to show details"""
    with st.container():
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{value}</div>
            <div class="metric-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)
        
        if value > 0:
            with st.expander(f"View {label} Details"):
                st.dataframe(df_subset[['url', 'title', 'status_code']], use_container_width=True)

# --- MAIN APP FLOW ---

# Sidebar Controls
with st.sidebar:
    st.header("🎮 Control Panel")
    target_url = st.text_input("Website URL", "https://example.com")
    max_pages_limit = st.number_input("Max Pages", 10, 2000, 50)
    
    col1, col2 = st.columns(2)
    if col1.button("Start Crawl", type="primary"):
        crawl_site(target_url, max_pages_limit)
        st.rerun()
    
    if col2.button("Delete DB"):
        coll = get_db_collection()
        if coll is not None:
            coll.drop()
            st.success("Database Cleared")
            st.rerun()

# Main Tabs
tab1, tab2, tab3 = st.tabs(["📊 Crawl Report", "🕸️ Site Structure", "🔍 Search Site"])

# Calculate Metrics if DB has data
metrics_data, full_df = get_metrics() if get_db_collection().count_documents({}) > 0 else (None, None)

with tab1:
    if metrics_data:
        st.subheader("Site Health Overview")
        
        # Row 1: High Level
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Total Pages", metrics_data['total_pages'], full_df, "tot")
        with c2: render_metric_card("Indexable Pages", len(metrics_data['indexable']), metrics_data['indexable'], "idx")
        with c3: render_metric_card("Non-Indexable", len(metrics_data['non_indexable']), metrics_data['non_indexable'], "nidx")
        with c4: render_metric_card("Slow Pages (>2s)", len(metrics_data['slow_pages']), metrics_data['slow_pages'], "slow")

        st.markdown("---")
        st.subheader("Content & Metadata Issues")
        
        # Row 2: Content Issues
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Duplicate Content", len(metrics_data['duplicate_content']), metrics_data['duplicate_content'], "dup_cont")
        with c2: render_metric_card("Duplicate Titles", len(metrics_data['duplicate_titles']), metrics_data['duplicate_titles'], "dup_tit")
        with c3: render_metric_card("Duplicate Desc", len(metrics_data['duplicate_desc']), metrics_data['duplicate_desc'], "dup_desc")
        with c4: render_metric_card("Canonical Issues", len(metrics_data['canonical_issues']), metrics_data['canonical_issues'], "canon")

        # Row 3: Technical & Images
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_metric_card("Missing Alt Tags", len(metrics_data['missing_alt']), metrics_data['missing_alt'], "alt")
        with c2: render_metric_card("3xx Redirection", len(metrics_data['status_3xx']), metrics_data['status_3xx'], "3xx")
        with c3: render_metric_card("4xx Client Errors", len(metrics_data['status_4xx']), metrics_data['status_4xx'], "4xx")
        with c4: render_metric_card("5xx Server Errors", len(metrics_data['status_5xx']), metrics_data['status_5xx'], "5xx")
        
        st.markdown("---")
        st.info("Note: 'URL Inspection' and full 'PageSpeed' metrics require Google API keys. Basic latency and structural checks are included above.")

    else:
        st.info("No data found. Please enter a URL and start crawling.")

with tab2:
    if full_df is not None:
        st.subheader("Interactive Site Architecture")
        st.caption("Visualizing internal link structure. Zoom and drag nodes.")
        
        # Create Graph
        G = generate_network_graph(full_df)
        
        # Initiate PyVis
        net = Network(height='600px', width='100%', bgcolor='#222222', font_color='white')
        net.from_nx(G)
        
        # Physics options for stability
        net.toggle_physics(True)
        
        # Save and display
        try:
            path = tempfile.gettempdir() + "/network.html"
            net.save_graph(path)
            with open(path, 'r', encoding='utf-8') as f:
                source_code = f.read()
            st.components.v1.html(source_code, height=600)
        except Exception as e:
            st.error(f"Error generating graph: {e}")
    else:
        st.warning("Crawl the site first to visualize structure.")

with tab3:
    st.subheader("Global Search")
    search_query = st.text_input("Search for words or phrases across the entire site")
    
    if search_query and get_db_collection() is not None:
        col = get_db_collection()
        # MongoDB Text Search (requires text index, but we will use Regex for simplicity in this demo)
        # For production: col.create_index([("page_text", "text")])
        
        results = list(col.find(
            {"page_text": {"$regex": search_query, "$options": "i"}},
            {"url": 1, "title": 1, "page_text": 1}
        ))
        
        st.markdown(f"**Found {len(results)} pages matching '{search_query}'**")
        
        for res in results:
            with st.expander(f"📄 {res.get('title', 'No Title')} ({res['url']})"):
                # snippet extraction
                text = res['page_text']
                idx = text.lower().find(search_query.lower())
                start = max(0, idx - 50)
                end = min(len(text), idx + len(search_query) + 50)
                snippet = text[start:end]
                st.markdown(f"...**{snippet}**...")
                st.write(f"[Open Link]({res['url']})")
