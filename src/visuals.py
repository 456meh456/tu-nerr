import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config
import pandas as pd
import numpy as np

def render_graph(disp_df, center, source):
    """
    Renders the interactive AgGraph network view.
    Handles both Solar System (Physics) and Territory Map (Fixed X/Y).
    """
    
    nodes = []
    edges = []
    added_node_ids = set() 

    is_search_mode = source in ["Social", "AI (Audio)"]
    real_center_id = None
    
    # 1. ESTABLISH CENTER (For Solar System View)
    if center:
        center_row = disp_df[disp_df['Artist'].astype(str).str.lower() == str(center).lower()]
        if not center_row.empty:
            r = center_row.iloc[0]
            real_center_id = r['Artist']
            
            # Visuals for Center
            bpm = int(r.get('Audio_BPM', 0))
            e_val = float(r.get('Audio_Brightness', 0) or r.get('Tag_Energy', 0.5))
            border = "#E74C3C" if e_val > 0.7 else "#2ECC71" if e_val < 0.4 else "#F1C40F"
            
            # Fix Image
            img = r.get('Image URL')
            if not img or "placeholder" in str(img): img = "https://placehold.co/80x80/000/FFF?text=ARTIST"

            nodes.append(Node(
                id=real_center_id,
                label=real_center_id,
                size=80,
                shape="circularImage",
                image=img,
                title=f"CENTER\nGenre: {r['Genre']}\nBPM: {bpm}",
                borderWidth=6,
                color={'border': border}
            ))
            added_node_ids.add(real_center_id)

    # 2. CREATE NODES
    for i, r in disp_df.iterrows():
        artist_name = r['Artist']
        if artist_name in added_node_ids: continue
        
        # Sizing
        listeners = r.get('Monthly Listeners', 0)
        size = 25
        if listeners > 10_000_000: size = 50
        elif listeners > 1_000_000: size = 35

        # Coloring
        e_val = float(r.get('Audio_Brightness', 0) or r.get('Tag_Energy', 0.5))
        border = "#E74C3C" if e_val > 0.7 else "#2ECC71" if e_val < 0.4 else "#F1C40F"

        # Image
        img = r.get('Image URL')
        if not img or "placeholder" in str(img): img = "https://placehold.co/80x80/000/FFF?text=NODE"

        # COORDINATES (The UMAP Upgrade)
        # If UMAP data exists, we scale it up (UMAP is usually -1 to 1, AgGraph needs pixels)
        x_pos = None
        y_pos = None
        if 'UMAP_X' in r and not pd.isna(r['UMAP_X']):
            x_pos = float(r['UMAP_X']) * 500 # Scale factor
            y_pos = float(r['UMAP_Y']) * 500

        nodes.append(Node(
            id=artist_name,
            label=artist_name,
            size=size,
            shape="circularImage",
            image=img,
            title=f"Genre: {r['Genre']}\nBPM: {int(r.get('Audio_BPM', 0))}",
            borderWidth=3,
            color={"border": border},
            x=x_pos, # Apply UMAP coordinates
            y=y_pos
        ))
        added_node_ids.add(artist_name)
    
    # 3. EDGES & PHYSICS CONFIG
    # If we have UMAP coordinates, we turn OFF physics so nodes stay in their smart clusters.
    # If we are searching, we turn ON physics so they float around the center.
    
    use_physics = True
    
    if is_search_mode and real_center_id:
        # SOLAR SYSTEM (Physics ON)
        for i, r in disp_df.iterrows():
            if r['Artist'] != real_center_id and r['Artist'] in added_node_ids:
                edge_color = "#FF4B4B" if source == "AI (Audio)" else "#555555"
                edges.append(Edge(source=real_center_id, target=r['Artist'], color=edge_color))
    else:
        # GLOBAL TERRITORY (Physics OFF - use UMAP positions)
        if 'UMAP_X' in disp_df.columns:
            use_physics = False 
            # We don't draw edges in Territory view to avoid clutter
        else:
            # Fallback to Genre clusters if UMAP failed
            genres = disp_df['Genre'].unique()
            for g in genres:
                nodes.append(Node(id=f"g_{g}", label=g, size=15, color="#f1c40f", shape="star"))
            for i, r in disp_df.iterrows():
                edges.append(Edge(source=r['Artist'], target=f"g_{r['Genre']}", color="#333333"))

    # 4. RENDER CONFIG
    config = Config(
        width="100%", 
        height=700, 
        directed=False, 
        physics=use_physics, # Dynamic Physics toggle
        hierarchical=False, 
        collapsible=True,
        physicsOptions={
            "barnesHut": {"gravitationalConstant": -4000, "centralGravity": 0.1, "springLength": 120, "damping": 0.4},
            "stabilization": {"iterations": 50}
        }
    )
    
    return agraph(nodes=nodes, edges=edges, config=config)