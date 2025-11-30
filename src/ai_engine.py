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
    
    This requires a fresh database query to join artists and tracks for the vector comparison.
    """
    from src.db_model import get_supabase_client
    supabase = get_supabase_client()
    if not supabase: return pd.DataFrame()

    # 1. Fetch full relational data (Artist + all Track data)
    # This query pulls all track records and their parent artist names
    query = """
    SELECT
        t.title, 
        t.bpm, t.brightness, t.noisiness, t.warmth, t.complexity,
        a.name AS artist_name, a.valence, a.tag_energy, a.image_url
    FROM
        tracks t
    JOIN
        artists a ON t.artist_id = a.id;
    """
    
    try:
        data = supabase.rpc('execute_sql', {'query_string': query}).execute()
        df_tracks = pd.DataFrame(data.data)
    except Exception as e:
        # NOTE: If Supabase doesn't allow stored procedure (rpc), this needs raw client connection
        print(f"SQL Error fetching tracks for KNN: {e}")
        return pd.DataFrame()


    if df_tracks.empty: return pd.DataFrame()

    # 2. Define and scale features (using raw track data + Valence from artist)
    features = df_tracks[['bpm', 'brightness', 'noisiness', 'warmth', 'complexity', 'valence']].fillna(0).values
    
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    knn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(df_tracks)), metric='cosine') # Cosine better for vectors
    knn.fit(features_scaled)

    # 3. Find the target track's vector
    target_idx = df_tracks[(df_tracks['artist_name'] == artist_name) & (df_tracks['title'] == track_title)].index
    if target_idx.empty: return pd.DataFrame()

    target_vector_scaled = features_scaled[target_idx[0]].reshape(1, -1)
    
    # 4. Get Neighbors
    distances, indices = knn.kneighbors(target_vector_scaled, n_neighbors=n_neighbors + 1)
    
    neighbor_indices = indices.flatten()[1:]
    
    # 5. Return the resulting rows
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