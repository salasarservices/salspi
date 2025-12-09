import streamlit as st
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
import time
import re
from collections import deque

# --- CONFIGURATION ---
MAX_PAGES_DEFAULT = 50
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# --- HELPER FUNCTIONS ---

def is_valid_url(url, base_domain):
    """
    Checks if a URL is valid and belongs to the same domain to prevent external crawling.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme) and base_domain in parsed.netloc

def get_page_content(url):
    """
    Fetches page content with a timeout and user-agent.
    """
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=5)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None

def search_text_in_soup(soup, query, include_alt, include_meta):
    """
    Searches for the query in visible text, alt tags, and meta tags.
    Returns a list of finding dictionaries.
    """
    findings = []
    query_lower = query.lower()

    # 1. Search Visible Text
    # We remove scripts and styles to only search visible text
    for script in soup(["script", "style"]):
        script.extract()
    
    text = soup.get_text(separator=' ', strip=True)
    if query_lower in text.lower():
        # Simple snippet extraction
        start_idx = text.lower().find(query_lower)
        snippet = text[max(0, start_idx - 30): min(len(text), start_idx + len(query) + 30)]
        findings.append({
            "Type": "Text Content",
            "Context": f"...{snippet}...",
            "Match": query
        })

    # 2. Search Image Alt Tags
    if include_alt:
        images = soup.find_all('img', alt=True)
        for img in images:
            if query_lower in img['alt'].lower():
                findings.append({
                    "Type": "Image Alt Tag",
                    "Context": f"Alt text: {img['alt']}",
                    "Match": query
                })

    # 3. Search Meta Titles & Descriptions
    if include_meta:
        # Title
        if soup.title and query_lower in soup.title.string.lower():
            findings.append({
                "Type": "Page Title",
                "Context": soup.title.string,
                "Match": query
            })
        
        # Meta Description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content') and query_lower in meta_desc['content'].lower():
            findings.append({
                "Type": "Meta Description",
                "Context": meta_desc['content'],
                "Match": query
            })

    return findings

def crawl_and_search(start_url, max_pages, query, include_alt, include_meta, progress_bar, status_text):
    """
    The main crawling logic using BFS (Breadth-First Search).
    """
    domain = urlparse(start_url).netloc
    visited = set()
    queue = deque([start_url])
    results = []
    
    pages_crawled = 0
    
    # Initialize UI
    status_text.text(f"Starting crawl on {start_url}...")
    
    while queue and pages_crawled < max_pages:
        url = queue.popleft()
        
        if url in visited:
            continue
        
        visited.add(url)
        pages_crawled += 1
        
        # Update UI
        progress_val = pages_crawled / max_pages
        progress_bar.progress(progress_val)
        status_text.text(f"Crawling ({pages_crawled}/{max_pages}): {url}")
        
        html_content = get_page_content(url)
        if not html_content:
            continue
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # A. Search content
        page_findings = search_text_in_soup(soup, query, include_alt, include_meta)
        for finding in page_findings:
            finding['URL'] = url
            results.append(finding)
            
        # B. Find new links
        for link in soup.find_all('a', href=True):
            absolute_link = urljoin(url, link['href'])
            # Remove fragments (#) to avoid duplicates
            absolute_link = absolute_link.split('#')[0]
            
            if is_valid_url(absolute_link, domain) and absolute_link not in visited:
                queue.append(absolute_link)
                
        time.sleep(0.1) # Be polite to the server

    return results, pages_crawled

# --- STREAMLIT UI ---

st.set_page_config(page_title="SiteCrawler Pro", page_icon="🕷️", layout="wide")

st.title("🕷️ Website Deep Search Crawler")
st.markdown("""
This tool meticulously crawls a website to find specific words, phrases, or technical tags.
**Note:** Please respect website terms of service and robots.txt.
""")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    target_url = st.text_input("Target URL", placeholder="https://example.com")
    search_query = st.text_input("Search Query", placeholder="e.g., sustainability")
    
    st.subheader("Search Scope")
    include_alt = st.checkbox("Include Image Alt Tags", value=True)
    include_meta = st.checkbox("Include Titles & Descriptions", value=True)
    
    st.subheader("Limits")
    max_pages = st.slider("Max Pages to Crawl", min_value=10, max_value=2000, value=50)
    
    start_btn = st.button("🚀 Start Crawling", type="primary")

# Main Area
if start_btn:
    if not target_url or not search_query:
        st.error("Please provide both a URL and a Search Query.")
    else:
        # Layout for results
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Target: {target_url}")
        with col2:
            st.info(f"Query: '{search_query}'")
            
        # Placeholders for progress
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Run Crawler
        try:
            results_data, total_pages = crawl_and_search(
                target_url, max_pages, search_query, include_alt, include_meta, progress_bar, status_text
            )
            
            # Post-processing
            status_text.success(f"✅ Crawl Complete! Scanned {total_pages} pages.")
            progress_bar.progress(100)
            
            if results_data:
                df = pd.DataFrame(results_data)
                
                # Reorder columns
                df = df[['Match', 'Type', 'Context', 'URL']]
                
                st.subheader(f"📊 Results Found ({len(df)})")
                st.dataframe(df, use_container_width=True)
                
                # Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Results CSV",
                    data=csv,
                    file_name='crawl_results.csv',
                    mime='text/csv',
                )
            else:
                st.warning("No matches found matching your query.")
                
        except Exception as e:
            st.error(f"An error occurred: {e}")

else:
    st.info("Enter details in the sidebar and click 'Start Crawling' to begin.")
