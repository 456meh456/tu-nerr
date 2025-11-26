import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import streamlit as st

# Try importing UMAP, handle case where it's missing (graceful degradation)
try:
    import umap
    HAS_UMAP = True
except ImportError:
    HAS_UMAP = False

@st.cache_data(ttl=600)
def get_ai_neighbors(center_artist, df_db, n_neighbors=5):
    """Finds mathematically similar artists using Audio Physics and Mood."""
    
    if len(df_db) < 5: 
        # Not enough data for meaningful neighbors
        return pd.DataFrame()
    
    # 1. HYBRID FEATURE CONSTRUCTION
    df_calc = df_db.copy()
    
    # Normalize names for lookup
    df_calc['lookup_name'] = df_calc['Artist'].astype(str).str.strip().str.lower()
    target = str(center_artist).strip().lower()

    # Create the primary Energy metric: prefer Audio Brightness, fall back to Tag Energy
    df_calc['Energy_Feature'] = df_calc.apply(
        lambda x: x.get('Audio_Brightness', 0) if x.get('Audio_Brightness', 0) > 0 else x.get('Tag_Energy', 0.5), axis=1
    )
    
    # 2. FEATURE MATRIX
    # Use Energy (Timbre/Intensity), Valence (Mood), and Tempo (BPM)
    features = df_calc[['Energy_Feature', 'Valence', 'Audio_BPM']].fillna(0).values
    
    # 3. SCALE DATA
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # 4. FIT MODEL
    knn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(df_db)), metric='euclidean')
    knn.fit(features_scaled)
    
    # 5. FIND NEIGHBORS
    center_idx = df_calc[df_calc['lookup_name'] == target].index
    
    if center_idx.empty: 
        return pd.DataFrame()
        
    target_index = center_idx[0]
    distances, indices = knn.kneighbors([features_scaled[target_index]])
    
    neighbor_indices = indices[0][1:] 
    return df_db.iloc[neighbor_indices]

@st.cache_data(ttl=3600)
def generate_territory_map(df_db):
    """
    Generates 2D coordinates for the global map using UMAP.
    If UMAP is missing or data is sparse, returns the original DF (Visuals will handle fallback).
    """
    if len(df_db) < 15 or not HAS_UMAP:
        # Not enough data for UMAP or library missing -> Return as is
        # Visuals.py will default to random/grid layout
        return df_db
        
    # 1. Prepare Features
    df_calc = df_db.copy()
    df_calc['Energy_Feature'] = df_calc.apply(
        lambda x: x.get('Audio_Brightness', 0) if x.get('Audio_Brightness', 0) > 0 else x.get('Tag_Energy', 0.5), axis=1
    )
    
    # Use 5 Dimensions for the map
    features = df_calc[['Energy_Feature', 'Valence', 'Audio_BPM', 'Monthly Listeners']].fillna(0).values
    
    # 2. Scale
    scaled_data = StandardScaler().fit_transform(features)
    
    # 3. Run UMAP
    # n_neighbors=15 balances local vs global structure
    try:
        reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, metric='euclidean', random_state=42)
        embedding = reducer.fit_transform(scaled_data)
        
        # 4. Assign Coordinates back to DF
        # We repurpose Valence/Energy columns in the view_df OR add new ones.
        # For now, let's add explicit UMAP columns
        df_db['UMAP_X'] = embedding[:, 0]
        df_db['UMAP_Y'] = embedding[:, 1]
        return df_db
    except Exception as e:
        print(f"UMAP Failed: {e}")
        return df_db