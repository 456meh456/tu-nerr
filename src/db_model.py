import os
import toml
import pandas as pd
from supabase import create_client, Client
import streamlit as st
import numpy as np
import ssl

# --- SSL MONKEY PATCH ---
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# --- CONNECTION FACTORY ---
def get_supabase_client():
    """Initializes the Supabase client from secrets."""
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]
        else:
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

    # Check if artist exists
    existing = supabase.table("artists").select("id").eq("name", data['Artist']).execute()
    
    if existing.data:
        # Update existing (Upsert logic)
        update_payload = {
            "first_release_year": data.get('First Release Year'),
            "listeners": int(data.get('Monthly Listeners', 0)),
            "image_url": data.get('Image URL', ''),
            "tag_energy": float(data.get('Tag_Energy', 0.5)),
            "valence": float(data.get('Valence', 0.5)),
        }
        supabase.table("artists").update(update_payload).eq("id", existing.data[0]['id']).execute()
        return existing.data[0]['id']

    # Insert new
    payload = {
        "name": data['Artist'],
        "genre": data.get('Genre', 'Unknown'),
        "listeners": int(data.get('Monthly Listeners', 0)),
        "image_url": data.get('Image URL', ''),
        "first_release_year": data.get('First Release Year'), 
        "valence": float(data.get('Valence', 0.5)),
        "tag_energy": float(data.get('Tag_Energy', 0.5))
    }
    
    response = supabase.table("artists").insert(payload).execute()
    return response.data[0]['id'] if response.data else None

def add_track(artist_id, track_data):
    supabase = get_supabase_client()
    if not supabase: return

    payload = {
        "artist_id": artist_id,
        "title": track_data.get('title', 'Unknown'),
        "preview_url": track_data.get('preview_url', ''),
        "bpm": float(track_data.get('bpm', 0)),
        "brightness": float(track_data.get('brightness', 0)),
        "noisiness": float(track_data.get('noisiness', 0)),
        "warmth": float(track_data.get('warmth', 0)),
        "complexity": float(track_data.get('complexity', 0))
    }
    supabase.table("tracks").insert(payload).execute()

def synthesize_scores(artist_id):
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
    supabase = get_supabase_client()
    if not supabase: return False
    try:
        supabase.table("artists").delete().eq("name", artist_name).execute()
        return True
    except Exception:
        return False

def fetch_all_artists_df():
    """Returns the main dataframe for the App Visualization."""
    supabase = get_supabase_client()
    if not supabase: raise ConnectionError("Supabase client is not available.")
    
    # FIX: Added avg_noisiness, avg_warmth, avg_complexity to selection
    response = supabase.table("artists").select(
        "name, genre, listeners, avg_brightness, valence, avg_bpm, image_url, tag_energy, first_release_year, avg_noisiness, avg_warmth, avg_complexity"
    ).execute()
    
    df = pd.DataFrame(response.data)
    if df.empty: return pd.DataFrame()
    
    # Map SQL columns back to app.py expectations
    df = df.rename(columns={
        "name": "Artist", "genre": "Genre", "listeners": "Monthly Listeners",
        "avg_brightness": "Audio_Brightness", "valence": "Valence",
        "avg_bpm": "Audio_BPM", "image_url": "Image URL", "tag_energy": "Tag_Energy",
        "first_release_year": "First Release Year",
        "avg_noisiness": "Audio_Noisiness",   # NEW
        "avg_warmth": "Audio_Warmth",         # NEW
        "avg_complexity": "Audio_Complexity"  # NEW
    })
    
    df['Artist_Lower'] = df['Artist'].str.lower()
    return df