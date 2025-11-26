import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import streamlit as st

@st.cache_data(ttl=600)
def get_ai_neighbors(center_artist, df_db, n_neighbors=5):
    """Finds mathematically similar artists using Audio Physics and Mood."""
    
    if len(df_db) < 5: 
        st.info("The AI needs at least 5 bands to calculate similarity.")
        return pd.DataFrame()
    
    # 1. HYBRID FEATURE CONSTRUCTION
    df_calc = df_db.copy()
    
    # Create the primary Energy metric: prefer Audio Brightness, fall back to Tag Energy
    df_calc['Energy_Feature'] = df_calc.apply(
        lambda x: x.get('Audio_Brightness', 0) if x.get('Audio_Brightness', 0) > 0 else x.get('Tag_Energy', 0.5), axis=1
    )
    
    # 2. FEATURE MATRIX: Use Energy (Timbre/Intensity), Valence (Mood), and Tempo (BPM)
    # CRITICAL: Fill NaNs to avoid crashing the scaler
    features = df_calc[['Energy_Feature', 'Valence', 'Audio_BPM']].fillna(0).values
    
    # 3. SCALE DATA (The Fix): Normalizing all features to the same range (0-1).
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # 4. FIT KNN MODEL (Euclidean distance is standard for normalized feature vectors)
    knn = NearestNeighbors(n_neighbors=min(n_neighbors + 1, len(df_db)), metric='euclidean')
    knn.fit(features_scaled)
    
    # 5. FIND CENTER INDEX
    target_index = df_db[df_db['Artist'] == center_artist].index
    if target_index.empty: 
        st.error(f"Error: Could not find '{center_artist}' in the database for analysis.")
        return pd.DataFrame()
    target_index = target_index[0]
    
    # 6. GET NEIGHBORS
    # The center artist's vector must be scaled before prediction
    target_vector_scaled = features_scaled[target_index].reshape(1, -1)
    
    distances, indices = knn.kneighbors(target_vector_scaled, n_neighbors=min(n_neighbors + 1, len(df_db)))
    
    # 7. RETURN SLICE (excluding the target itself)
    neighbor_indices = indices.flatten()[1:] 
    return df_db.iloc[neighbor_indices]