import os
import toml
import pandas as pd
from supabase import create_client, Client
import streamlit as st
import numpy as np
import ssl

# --- SSL MONKEY PATCH (Standard for Httpx/Supabase) ---
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# --- CONNECTION FACTORY ---
# (Omitted - Assumed Correct)

def get_supabase_client():
    # ... (Connection logic remains the same) ...
    try:
        # 1. Try Streamlit Cloud Secrets (Production)
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]
        else:
            # 2. Try Local Secrets (Development/Scripts)
            secrets_path = os.path.join(os.getcwd(), ".streamlit", "secrets.toml")
            if os.path.exists(secrets_path):
                secrets = toml.load(secrets_path)
                url = secrets["supabase"]["url"]
                key = secrets["supabase"]["key"]
            else:
                return None
            
        return create_client(url, key)
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return None

# --- CORE OPERATIONS (SQL) ---
def add_artist(data):
    """Inserts a new artist (if not exists) and returns their ID."""
    supabase = get_supabase_client()
    if not supabase: raise ConnectionError("Supabase client is not available.")

    # 1. Check if artist exists
    existing = supabase.table("artists").select("id").eq("name", data['Artist']).execute()
    
    if existing.data:
        # If artist exists, we update the missing field
        update_payload = {
            "first_release_year": data.get('First Release Year'),
            "listeners": int(data.get('Monthly Listeners', 0)),
            "image_url": data.get('Image URL', ''),
            "tag_energy": float(data.get('Tag_Energy', 0.5)),
            "valence": float(data.get('Valence', 0.5)),
        }
        
        # Use primary key for update
        supabase.table("artists").update(update_payload).eq("id", existing.data[0]['id']).execute()
        return existing.data[0]['id']

    # 2. Prepare Payload for Insertion (NEW ARTIST)
    payload = {
        "name": data['Artist'],
        "genre": data.get('Genre', 'Unknown'),
        "listeners": int(data.get('Monthly Listeners', 0)),
        "image_url": data.get('Image URL', ''),
        "first_release_year": data.get('First Release Year'), # <-- NEW
        "valence": float(data.get('Valence', 0.5)),
        "tag_energy": float(data.get('Tag_Energy', 0.5))
    }
    
    # 3. Insert and return ID
    response = supabase.table("artists").insert(payload).execute()
    return response.data[0]['id'] if response.data else None

def add_track(artist_id, track_data):
    # ... (Code unchanged, omitted for space)
    supabase = get_supabase_client()
    if not supabase: return

    payload = {
        "artist_id": artist_id,
        "title": track_data.get('title', 'Unknown'),
        "preview_url": track_data.get('preview', ''),
        "bpm": float(track_data.get('bpm', 0)),
        "brightness": float(track_data.get('brightness', 0)),
        "noisiness": float(track_data.get('noisiness', 0)),
        "warmth": float(track_data.get('warmth', 0)),
        "complexity": float(track_data.get('complexity', 0))
    }
    supabase.table("tracks").insert(payload).execute()

def synthesize_scores(artist_id):
    # ... (Code unchanged, omitted for space)
    supabase = get_supabase_client()
    if not supabase: return

    response = supabase.table("tracks").select("*").eq("artist_id", artist_id).execute()
    tracks = response.data
    
    if not tracks: return
    
    df = pd.DataFrame(tracks)
    
    update_payload = {
        "avg_bpm": float(df['bpm'].mean()),
        "avg_brightness": float(df['brightness'].mean()),
        "avg_noisiness": float(df['noisiness'].mean()),
        "avg_warmth": float(df['warmth'].mean()),
        "avg_complexity": float(df['complexity'].mean())
    }
    
    supabase.table("artists").update(update_payload).eq("id", artist_id).execute()

def delete_artist(artist_name):
    # ... (Code unchanged, omitted for space)
    supabase = get_supabase_client()
    if not supabase: return False

    try:
        supabase.table("artists").delete().eq("name", artist_name).execute()
        return True
    except Exception:
        return False

def fetch_all_artists_df():
    # ... (Code unchanged, omitted for space)
    supabase = get_supabase_client()
    if not supabase: raise ConnectionError("Supabase client is not available.")
    
    # Select columns needed for visualization
    response = supabase.table("artists").select(
        "name, genre, listeners, avg_brightness, valence, avg_bpm, image_url, tag_energy, first_release_year"
    ).execute()
    
    df = pd.DataFrame(response.data)
    if df.empty: return pd.DataFrame()
    
    # Map SQL columns back to app.py expectations
    df = df.rename(columns={
        "name": "Artist", "genre": "Genre", "listeners": "Monthly Listeners",
        "avg_brightness": "Audio_Brightness", "valence": "Valence",
        "avg_bpm": "Audio_BPM", "image_url": "Image URL", "tag_energy": "Tag_Energy",
        "first_release_year": "First Release Year"
    })
    
    df['Artist_Lower'] = df['Artist'].str.lower()
    return df