import os
import toml
import pandas as pd
from supabase import create_client, Client
import streamlit as st

# --- CONNECTION FACTORY ---
def get_supabase_client():
    try:
        if hasattr(st, "secrets") and "supabase" in st.secrets:
            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]
        else:
            secrets_path = os.path.join(os.getcwd(), ".streamlit", "secrets.toml")
            secrets = toml.load(secrets_path)
            url = secrets["supabase"]["url"]
            key = secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        print(f"❌ DB Connection Error: {e}")
        return None

# --- CORE OPERATIONS ---
def add_artist(data):
    supabase = get_supabase_client()
    existing = supabase.table("artists").select("id").eq("name", data['Artist']).execute()
    if existing.data: return existing.data[0]['id']

    payload = {
        "name": data['Artist'],
        "genre": data.get('Genre', 'Unknown'),
        "listeners": int(data.get('Monthly Listeners', 0)),
        "image_url": data.get('Image URL', ''),
        "valence": float(data.get('Valence', 0.5)),
        "tag_energy": float(data.get('Tag_Energy', 0.5))
    }
    response = supabase.table("artists").insert(payload).execute()
    return response.data[0]['id']

def add_track(artist_id, track_data):
    supabase = get_supabase_client()
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
    supabase = get_supabase_client()
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
    """Deletes an artist and cascades to tracks (handled by SQL)."""
    supabase = get_supabase_client()
    try:
        # Postgres CASCADE will handle the tracks automatically
        supabase.table("artists").delete().eq("name", artist_name).execute()
        return True
    except Exception as e:
        print(f"Delete Error: {e}")
        return False

def fetch_all_artists_df():
    supabase = get_supabase_client()
    # Map SQL columns to App columns
    response = supabase.table("artists").select(
        "name, genre, listeners, avg_brightness, valence, avg_bpm, image_url, tag_energy"
    ).execute()
    
    df = pd.DataFrame(response.data)
    if df.empty: return pd.DataFrame()
    
    df = df.rename(columns={
        "name": "Artist", "genre": "Genre", "listeners": "Monthly Listeners",
        "avg_brightness": "Audio_Brightness", "valence": "Valence",
        "avg_bpm": "Audio_BPM", "image_url": "Image URL", "tag_energy": "Tag_Energy"
    })
    df['Artist_Lower'] = df['Artist'].str.lower()
    return df