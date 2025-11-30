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

# --- KNN MODEL FOR ARTIST COMPOSITE SCORES ---
@st.cache_data(ttl=600)
def get_ai_neighbors(center_artist, df_db, n_neighbors=5):
    """Finds mathematically similar artists using Composite Audio Physics (Artist Table)."""
    
    if len(df_db) < 5: 
        return pd.DataFrame()
    
    df_calc = df_db.copy()
    
    # Use only the composite audio features for KNN training
    features = df_calc[['Audio_Brightness', 'Valence', 'Audio_BPM', 'Audio_Noisiness', 'Audio_Warmth', 'Audio_Complexity']].fillna(0).values
    
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    knn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(df_db)), metric='euclidean')
    knn.fit(features_scaled)
    
    target_idx = df_db[df_db['Artist'] == center_artist].index
    if target_idx.empty: return pd.DataFrame()
        
    target_index = target_idx[0]
    
    target_vector_scaled = features_scaled[target_index].reshape(1, -1)
    distances, indices = knn.kneighbors(target_vector_scaled, n_neighbors=min(n_neighbors + 1, len(df_db)))
    
    neighbor_indices = indices.flatten()[1:] 
    return df_db.iloc[neighbor_indices]


# --- NEW FEATURE: TRACK-LEVEL KNN ---

@st.cache_data(ttl=600)
def get_track_neighbors(artist_name, track_title, n_neighbors=5):
    """
    Finds songs (rows in the tracks table) mathematically similar to the single target track.
    Uses native Supabase join syntax instead of raw SQL RPC.
    """
    from src.db_model import get_supabase_client
    supabase = get_supabase_client()
    if not supabase: return pd.DataFrame()

    try:
        # 1. Fetch joined data using Supabase syntax
        # We select all track columns and the specific artist columns we need
        response = supabase.table("tracks").select(
            "*, artists!inner(name, valence, tag_energy, image_url)"
        ).execute()
        
        raw_data = response.data
        if not raw_data: return pd.DataFrame()

        # 2. Flatten the response (artists data comes nested in a dict)
        flat_data = []
        for row in raw_data:
            artist_info = row.pop('artists', {})
            # Merge artist info into the flat row
            row['artist_name'] = artist_info.get('name')
            row['valence'] = artist_info.get('valence')
            row['tag_energy'] = artist_info.get('tag_energy')
            row['image_url'] = artist_info.get('image_url')
            flat_data.append(row)
            
        df_tracks = pd.DataFrame(flat_data)
        
    except Exception as e:
        print(f"Error fetching tracks: {e}")
        return pd.DataFrame()

    if df_tracks.empty: return pd.DataFrame()

    # 3. Define and scale features
    # Use raw track physics + artist-level valence
    feature_cols = ['bpm', 'brightness', 'noisiness', 'warmth', 'complexity', 'valence']
    
    # Ensure columns exist and are numeric
    for col in feature_cols:
        if col not in df_tracks.columns: df_tracks[col] = 0.0
        df_tracks[col] = pd.to_numeric(df_tracks[col], errors='coerce').fillna(0)

    features = df_tracks[feature_cols].values
    
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    knn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(df_tracks)), metric='cosine') 
    knn.fit(features_scaled)

    # 4. Find the target track's vector
    # Case-insensitive match for robustness
    target_idx = df_tracks[
        (df_tracks['artist_name'].str.lower() == str(artist_name).lower()) & 
        (df_tracks['title'].str.lower() == str(track_title).lower())
    ].index
    
    if target_idx.empty: 
        print(f"Target track '{track_title}' by '{artist_name}' not found in track DB.")
        return pd.DataFrame()

    target_vector_scaled = features_scaled[target_idx[0]].reshape(1, -1)
    
    # 5. Get Neighbors
    distances, indices = knn.kneighbors(target_vector_scaled, n_neighbors=min(n_neighbors + 1, len(df_tracks)))
    
    neighbor_indices = indices.flatten()[1:] # Skip the first one (it's the target itself)
    
    # 6. Return the resulting rows
    return df_tracks.iloc[neighbor_indices]


# --- UMAP Logic (for Global View, unchanged) ---
@st.cache_data(ttl=3600)
def generate_territory_map(df_db):
    if len(df_db) < 15 or not HAS_UMAP: return df_db
    
    df_calc = df_db.copy()
    df_calc['Energy_Feature'] = df_calc.apply(
        lambda x: x.get('Audio_Brightness', 0) if x.get('Audio_Brightness', 0) > 0 else x.get('Tag_Energy', 0.5), axis=1
    )
    
    features = df_calc[['Energy_Feature', 'Valence', 'Audio_BPM', 'Monthly Listeners']].fillna(0).values
    scaled_data = StandardScaler().fit_transform(features)
    
    reducer = umap.UMAP(n_neighbors=15, min_dist=0.1, metric='euclidean', random_state=42)
    embedding = reducer.fit_transform(scaled_data)
    
    df_db['UMAP_X'] = embedding[:, 0]
    df_db['UMAP_Y'] = embedding[:, 1]
    
    return df_db