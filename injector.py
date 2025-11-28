import sys
import time
import os
import random
import argparse
import toml
import pandas as pd
import requests
import numpy as np
import librosa
import tempfile
import urllib3
from supabase import create_client, Client

# --- Import Core Processing Logic and Data ---
from new_seeds import GENRE_SEEDS
from src.db_model import fetch_all_artists_df, add_artist, add_track, synthesize_scores

# --- CONFIGURATION ---
SECRETS_PATH = ".streamlit/secrets.toml"
TRACKS_TO_ANALYZE = 5
AUDIODB_API_KEY = "2"
COMPLEXITY_DIVISOR = 0.2860 # Derived from raw data audit
MAX_API_RETRIES = 3 # New: Max attempts for critical API calls

try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
    SUPABASE_URL = secrets["supabase"]["url"]
    SUPABASE_KEY = secrets["supabase"]["key"]
except Exception as e:
    print(f"❌ ERROR: Could not load secrets. Details: {e}")
    sys.exit(1)

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- DB FUNCTIONS (Minimal copies for local logic) ---

def get_supabase_client_standalone():
    """Initializes the Supabase client for scripting environment."""
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        return None

# --- NEW: API RETRY HELPER ---

def api_request_with_retry(url, headers=None, verify=True, timeout=5, attempts=MAX_API_RETRIES):
    """Handles network requests with retries and exception mapping."""
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=headers, verify=verify, timeout=timeout)
            
            # Success check (200 OK)
            if response.status_code == 200:
                return response
            
            # Soft Failures (404, 429, 403, 500) - Wait and retry
            elif response.status_code in [403, 429, 500]:
                time.sleep(attempt + 1)
            
            # Hard Fail (404 Not Found)
            elif response.status_code == 404:
                return None
            
        except requests.exceptions.RequestException:
            # Catch Connection Errors, Timeouts, SSL Failures
            time.sleep(attempt + 1)
            
    return None # Return None if all attempts fail

# --- API/ANALYSIS HELPERS (Updated to use Retry Helper) ---

def get_deezer_data(artist_name):
    """Fetches Deezer ID, Listeners, Image, and Preview URL for processing."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    url = f"https://api.deezer.com/search/artist?q={artist_name}"
    
    # Use the retry mechanism
    response = api_request_with_retry(url, headers=headers, verify=False)
    
    if not response: return None

    try:
        data = response.json()
        if data.get('error'): return None
        if not data.get('data'): return None
        artist = data['data'][0]
        
        # Get Preview URL & Track ID
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_data_resp = api_request_with_retry(track_url, headers=headers, verify=False)
        
        if not t_data_resp: return None

        t_data = t_data_resp.json()
        preview = t_data['data'][0]['preview'] if t_data.get('data') else None
        top_track_id = t_data['data'][0]['id'] if t_data.get('data') else None

        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "preview": preview, "top_track_id": top_track_id
        }
    except Exception: return None

def get_release_year(artist_id):
    """Fetches the absolute earliest release year via discography scan."""
    earliest_date_str = None
    offset = 0
    limit = 50 
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    while True:
        url = f"https://api.deezer.com/artist/{artist_id}/albums?limit={limit}&index={offset}"
        response = api_request_with_retry(url, headers=headers, verify=False)
        
        if not response: break
        
        try:
            resp = response.json()
            if 'data' not in resp or not resp['data']: break
                
            dates = [album.get('release_date') for album in resp['data'] if album.get('release_date')]
            
            if dates:
                current_min_date = min(dates)
                if earliest_date_str is None or current_min_date < earliest_date_str:
                    earliest_date_str = current_min_date
            
            if resp.get('total') is not None and resp['total'] <= offset + limit: break
            if 'next' in resp: offset += limit
            else: break
            
        except Exception: break 

    return int(earliest_date_str[:4]) if earliest_date_str else 0

def get_audiodb_mood(artist_name):
    """Fetches a mood/valence score proxy from the AudioDB API."""
    try:
        url = f"http://www.theaudiodb.com/api/v1/json/{AUDIODB_API_KEY}/search.php?s={artist_name}"
        response = api_request_with_retry(url)
        
        if not response: return 0.5
        
        resp = response.json()
        if resp.get('artists') and resp['artists']:
            data = resp['artists'][0]
            mood_raw = data.get('strMood')
            
            if mood_raw:
                mood_raw = mood_raw.lower()
                if 'happy' in mood_raw or 'party' in mood_raw: return 0.8
                if 'sad' in mood_raw or 'melancholy' in mood_raw: return 0.2
                if 'aggressive' in mood_raw or 'angry' in mood_raw: return 0.3
                if 'dark' in mood_raw or 'gothic' in mood_raw: return 0.1
        return 0.5 
    except Exception: return 0.5 

def get_lastfm_tags(artist_name):
    """Fetches tags and calculates Tag_Energy."""
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        response = api_request_with_retry(url, verify=False)
        
        if not response: return [], 0.5

        resp = response.json()
        if 'error' in resp: return [], 0.5
        
        tags = [t['name'].lower() for t in resp['artist']['tags']['tag']]
        
        VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
        
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
            
        return tags, score(ENERGY_SCORES)
    except Exception: 
        return [], 0.5

def get_top_tracks_previews(deezer_id, limit=5):
    """Fetches top tracks for analysis."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={limit}"
        response = api_request_with_retry(url, headers=headers, verify=False)
        
        if not response: return []
        
        resp = response.json()
        tracks = []
        if resp.get('data'):
            for t in resp['data']:
                if 'preview' in t and t['preview']:
                    tracks.append({"title": t['title'], "preview": t['preview']})
        return tracks
    except: return []

def analyze_audio(preview_url):
    """Downloads MP3 to temp file and extracts 5-dimensional physics."""
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        if not preview_url: return None
        response = api_request_with_retry(preview_url, headers=headers, verify=False, timeout=15, attempts=5)
        
        if not response or response.status_code != 200: return None 

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        bpm = round(float(tempo[0])) if isinstance(tempo, np.ndarray) else round(float(tempo))
            
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        
        zcr = np.mean(librosa.feature.zero_crossing_rate(y))
        rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0]
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        complexity = np.mean(np.std(chroma, axis=1))
        
        # FINAL NORMALIZATION
        norm_brightness = min(brightness / 3000.0, 1.0)
        norm_noise = min(np.mean(zcr) / 0.5, 1.0) 
        norm_warmth = min(np.mean(rolloff) / 5000.0, 1.0)
        norm_complexity = min(complexity / COMPLEXITY_DIVISOR, 1.0) 
        
        return {
            "bpm": bpm, "brightness": norm_brightness, "noisiness": norm_noise, 
            "warmth": norm_warmth, "complexity": norm_complexity
        }
    except Exception as e:
        print(f" (Analysis Crash: {e})", end="", flush=True)
        return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass


# --- CORE INGESTION LOGIC ---

def process_and_commit_artist(artist_name, existing_artists):
    """Processes one artist record (fetching data and committing to SQL)."""
    
    # Imports must be local to function in bare python 
    from src.db_model import add_artist, add_track, synthesize_scores
    
    # 1. Fetch Metadata (Deezer/LastFM)
    d_info = get_deezer_data(artist_name)
    if not d_info:
        print(f" [Skip: Deezer metadata unavailable]")
        return None
    
    clean_name = d_info['name']
    
    # Check DB (Avoid re-processing known bands)
    if clean_name.lower() in existing_artists:
        print(f"   ⏭️ Skip: {clean_name} (Already in DB).")
        return clean_name
        
    tags, tag_energy = get_lastfm_tags(clean_name)
    if not tags: 
        print(f" [Skip: No LastFM tags]")
        return None
        
    main_genre = tags[0].title() if tags else "Unknown"
    valence = get_audiodb_mood(clean_name)
    release_year = get_release_year(d_info['id'])

    # 2. Insert Parent Artist Record
    artist_data = {
        "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": d_info['listeners'],
        "Image URL": d_info['image'], "Valence": valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year
    }
    artist_id = add_artist(artist_data)

    # 3. Process Tracks (Child Records)
    tracks = get_top_tracks_previews(d_info['id']) 
    analyzed_count = 0
    
    for t in tracks:
        phys = analyze_audio(t['preview'])
        if phys:
            track_record = {**phys, "title": t['title'], "preview_url": t['preview']}
            add_track(artist_id, track_record)
            analyzed_count += 1
            time.sleep(0.5)
    
    # 4. Synthesize Scores
    if analyzed_count > 0:
        synthesize_scores(artist_id)
    
    return clean_name

# --- MAIN EXECUTION LOOP ---

def run_injector():
    print("\n--- 💉 STARTING GENRE INJECTION ---")
    
    # 1. Load Current DB State
    try:
        from new_seeds import GENRE_SEEDS
        
        df = fetch_all_artists_df()
        
        # CRITICAL FIX: Robust check for column existence in DB
        if df.empty or 'Artist' not in df.columns:
            existing_artists = set()
            print("🚨 COLD START: Database appears empty/uninitialized. Running full seed.")
        else:
            existing_artists = set(df['Artist'].str.lower().tolist())
            print(f"📚 Database contains {len(existing_artists)} artists.")
            
    except Exception as e:
        print(f"❌ ERROR: Failed to connect or load DB/Seeds. Details: {e}")
        return

    total_added = 0
    
    for genre, artists in GENRE_SEEDS.items():
        print(f"\n[SECTION] Injecting Anchor Genre: {genre}")
        
        for artist_name in artists:
            
            if artist_name.lower() in existing_artists:
                print(f"   ⏭️ Skip: {artist_name} (Already in DB).")
                continue
                
            # 2. Process and Commit
            print(f"   🔎 Processing {artist_name}...", end="", flush=True)
            
            # The logic is simplified: attempt to process and commit.
            # add_artist will handle existing records (UPSERT).
            result_name = process_and_commit_artist(artist_name, existing_artists) 
            
            if result_name:
                total_added += 1
                existing_artists.add(result_name.lower()) # Update local set
                print(f" ✅ COMMITTED.")
            else:
                print(f" ❌ FAILED (API/Data issue).")
                
            time.sleep(1.0) # Politeness delay between major artist lookups

    print(f"\n---------------------------------------------")
    print(f"🎉 INJECTION COMPLETE. Total new anchors added: {total_added}.")

if __name__ == "__main__":
    run_injector()