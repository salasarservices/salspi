import os
import streamlit as st
import streamlit.components.v1 as components
import networkx as nx

# optional imports for pyvis/plotly - import inside functions to avoid hard failures
from typing import Dict, Any

def _ensure_dir(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def draw_network_graph(crawl: Dict[str, Any], height: int = 600):
    """
    Render the site graph for the given `crawl` dict.
    Strategy:
     1) Try to render with pyvis and embed HTML.
     2) If pyvis fails (templates missing or other errors), render an interactive Plotly graph instead.
    This function writes to the Streamlit page directly.
    """
    pages = crawl.get("pages", {}) or {}
    links = crawl.get("links", {}) or {}

    # Build directed graph using only discovered pages as nodes (for readability)
    G = nx.DiGraph()
    for u in pages.keys():
        G.add_node(u)
    for u, outs in links.items():
        if u not in pages:
            # only include edges from nodes we crawled (optional)
            G.add_node(u)
        for v in outs:
            # include edge, but prefer nodes we discovered
            G.add_edge(u, v)

    if G.number_of_nodes() == 0:
        st.info("No pages found to render the site structure.")
        return

    # Try pyvis first (most featureful)
    try:
        from pyvis.network import Network

        net = Network(height=f"{height}px", width="100%", directed=True, notebook=False)
        # Add nodes/edges - pyvis handles layout on its own
        net.from_nx(G)

        # adjust physics for readability
        try:
            net.repulsion(node_distance=200, central_gravity=0.05)
        except Exception:
            pass

        # ensure output dir exists
        out_path = "html_reports/site_graph.html"
        _ensure_dir(out_path)
        # write html - use write_html to avoid trying to open a browser
        net.write_html(out_path)

        # read & embed
        with open(out_path, "r", encoding="utf-8") as f:
            html = f.read()
        components.html(html, height=height, scrolling=True)
        return
    except Exception as e:
        # fallback to plotly (safer in many environments)
        st.warning("Pyvis rendering failed, falling back to Plotly rendering. " +
                   "Reason: " + repr(e))

    # FALLBACK: Plotly + networkx
    try:
        import plotly.graph_objects as go
    except Exception as e:
        st.error("Plotly is not installed (required for fallback). Install plotly in your environment.")
        return

    # Use spring layout for positions
    try:
        pos = nx.spring_layout(G, k=0.5, iterations=50, seed=42)
    except Exception:
        pos = nx.spring_layout(G)

    # Build edge traces
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos.get(edge[0], (0, 0))
        x1, y1 = pos.get(edge[1], (0, 0))
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines'
    )

    # Build node traces
    node_x = []
    node_y = []
    node_text = []
    node_size = []
    for node in G.nodes():
        x, y = pos.get(node, (0, 0))
        node_x.append(x)
        node_y.append(y)
        # Show URL and degree info as hovertext (truncate URL length shown)
        deg_in = G.in_degree(node)
        deg_out = G.out_degree(node)
        txt = f"{node}<br>in: {deg_in} out: {deg_out}"
        node_text.append(txt)
        # Node size proportional to degree (clamped)
        size = min(40, 8 + (deg_in + deg_out) * 3)
        node_size.append(size)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        text=node_text,
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            color=[G.degree(n) for n in G.nodes()],
            size=node_size,
            colorbar=dict(
                thickness=10,
                title='Node Degree',
                xanchor='left',
                titleside='right'
            ),
            line_width=1
        )
    )

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title_text='Site Structure',
                        title_x=0.5,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20,l=5,r=5,t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        height=height
                    ))
    st.plotly_chart(fig, use_container_width=True)
