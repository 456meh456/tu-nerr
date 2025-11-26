import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
import pandas as pd
import numpy as np

def render_graph(disp_df, center, source):
    """
    Renders the interactive AgGraph network view, distinguishing between 
    Solar System (Search) and Galaxy (Global) views.
    """
    
    nodes = []
    edges = []
    added_node_ids = set() 

    # Determine if we are in a focused search mode or global view
    is_search_mode = source in ["Social", "AI (Audio)"]
    real_center_id = None
    
    if center:
        # 1. ESTABLISH THE CENTER (THE SUN)
        center_row = disp_df[disp_df['Artist'].astype(str).str.lower() == str(center).lower()]
        
        if not center_row.empty:
            r = center_row.iloc[0]
            real_center_id = r['Artist']
            
            # --- IMAGE FIX: Robustly get image URL ---
            center_image_url = r.get('Image URL', "https://placehold.co/80x80/000/FFF?text=TARGET")
            # Fallback for old/corrupt placeholder data
            if str(center_image_url).startswith("https://commons.wikimedia"): 
                 center_image_url = "https://placehold.co/80x80/000/FFF?text=TARGET"
            
            # VISUALS FOR CENTER NODE
            energy_val = float(r.get('Audio_Brightness', 0) or r.get('Tag_Energy', 0.5))
            bpm = int(r.get('Audio_BPM', 0))
            
            if energy_val > 0.75: border_color = "#E74C3C" # Red
            elif energy_val < 0.4: border_color = "#2ECC71" # Green
            else: border_color = "#F1C40F" # Yellow
            
            nodes.append(Node(
                id=real_center_id,
                label=real_center_id,
                size=80,
                shape="circularImage",
                image=center_image_url,
                title=f"CENTER\nGenre: {r['Genre']}\nBPM: {bpm}",
                borderWidth=6,
                color={'border': border_color}
            ))
            added_node_ids.add(real_center_id)
        else:
            # Ghost Sun (Center not found in this specific dataframe slice)
            real_center_id = center
            nodes.append(Node(id=real_center_id, label=real_center_id, size=80, shape="circularImage", image="https://placehold.co/80x80/000/FFF?text=SCANNING", title="Target Artist (Data Loading...)", borderWidth=4, color={'border': '#FFFFFF'}))
            added_node_ids.add(real_center_id)


    # 2. CREATE NEIGHBOR NODES (THE PLANETS)
    for i, r in disp_df.iterrows():
        artist_name = r['Artist']
        if artist_name in added_node_ids: continue
        
        # Sizing based on Listeners
        listeners = r.get('Monthly Listeners', 0)
        size = 30
        if listeners > 10_000_000: size = 60
        elif listeners > 1_000_000: size = 45

        # Vibe Coloring
        audio_bright = r.get('Audio_Brightness', 0)
        tag_e = r.get('Tag_Energy', 0.5)
        energy_val = float(audio_bright or tag_e)
        
        if energy_val > 0.75: border_color = "#E74C3C"
        elif energy_val < 0.4: border_color = "#2ECC71"
        else: border_color = "#F1C40F"

        # Image Lookup for Neighbors
        neighbor_image_url = r.get('Image URL', "https://placehold.co/80x80/000/FFF?text=NODE")

        nodes.append(Node(
            id=artist_name,
            label=artist_name,
            size=size,
            shape="circularImage",
            image=neighbor_image_url,
            title=f"Genre: {r['Genre']}\nBPM: {int(r.get('Audio_BPM', 0))}\nEnergy: {energy_val:.2f}",
            borderWidth=4,
            color={"border": border_color}
        ))
        added_node_ids.add(artist_name)
    
    # 3. DRAW EDGES (GRAVITY)
    if is_search_mode and real_center_id:
        # A. SEARCH MODE: Star Topology (Everything connects to Center)
        for i, r in disp_df.iterrows():
            target_id = r['Artist']
            if target_id != real_center_id and target_id in added_node_ids:
                # Differentiate edge color for AI results vs Social results
                edge_color = "#FF4B4B" if source == "AI (Audio)" else "#555555"
                edges.append(Edge(source=real_center_id, target=target_id, color=edge_color))

    else: 
        # B. GLOBAL MODE: Cluster Topology (Connect to Genres/Floating Galaxy)
        genres = disp_df['Genre'].unique()
        for g in genres:
            nodes.append(Node(id=f"g_{g}", label=g, size=20, color="#f1c40f", shape="star", physics=False))
            
        for i, r in disp_df.iterrows():
            edges.append(Edge(source=r['Artist'], target=f"g_{r['Genre']}", color="#333333", length=150))

    # 4. RENDER CONFIG
    config = Config(
        width="100%", 
        height=700, 
        directed=False, 
        physics=True, 
        hierarchical=False, 
        collapsible=True,
        physicsOptions={
            "barnesHut": {"gravitationalConstant": -10000, "centralGravity": 0.05, "springLength": 100, "damping": 0.5},
            "stabilization": {"iterations": 50}
        }
    )
    
    return agraph(nodes=nodes, edges=edges, config=config)