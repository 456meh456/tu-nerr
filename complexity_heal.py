import requests
import time
import urllib3
import os
import tempfile
import numpy as np
import librosa
import pandas as pd
import random
import argparse
import sys
import warnings
import contextlib
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# Suppress Python-level warnings
warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
SLEEP_TIME = 0.5  
MAX_FAILURES_ALLOWED = 10 
TRACKS_TO_ANALYZE = 5 
MAX_API_RETRIES = 3 
COMPLEXITY_DIVISOR = 0.2860 
AUDIODB_API_KEY = "2" 
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# --- AUTH SETUP ---
import toml
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
    SUPABASE_URL = secrets["supabase"]["url"]
    SUPABASE_KEY = secrets["supabase"]["key"]
except Exception as e:
    print(f"❌ Error loading secrets: {e}")
    exit()

# --- HELPER: C-Level Warning Suppressor ---
@contextlib.contextmanager
def ignore_stderr():
    """Redirects stderr to null to hide C-library warnings (like ID3v2)."""
    try:
        # Save original stderr
        original_stderr = sys.stderr
        # Open a null file
        null_file = open(os.devnull, 'w')
        # Replace stderr with null file
        sys.stderr = null_file
        yield
    finally:
        # Restore stderr
        sys.stderr = original_stderr
        if 'null_file' in locals():
            null_file.close()

# --- DB FUNCTIONS (Embedded) ---
from supabase import create_client

def get_supabase_client_standalone():
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        return None

# --- API HELPERS (Embedded) ---

def api_request_with_retry(url, headers=None, verify=True, timeout=5, attempts=MAX_API_RETRIES):
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=headers, verify=verify, timeout=timeout)
            if response.status_code == 200: return response
            elif response.status_code in [404, 429, 403, 500]:
                time.sleep(attempt + 1)
            else: return None
        except requests.exceptions.RequestException:
            time.sleep(attempt + 1)
    return None

def get_release_year(artist_id):
    # ... (Standard logic omitted for brevity, identical to previous)
    # Returns 0 if fail
    try:
        url = f"https://api.deezer.com/artist/{artist_id}/albums?limit=50"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = api_request_with_retry(url, headers=headers, verify=False)
        if not resp: return 0
        data = resp.json()
        if 'data' in data:
            dates = [a.get('release_date') for a in data['data'] if a.get('release_date')]
            if dates: return int(min(dates)[:4])
    except: pass
    return 0

def get_audiodb_mood(artist_name):
    # ... (Standard logic omitted for brevity)
    try:
        url = f"http://www.theaudiodb.com/api/v1/json/{AUDIODB_API_KEY}/search.php?s={artist_name}"
        resp = api_request_with_retry(url)
        if resp:
            d = resp.json()
            if d.get('artists'):
                m = d['artists'][0].get('strMood', '').lower()
                if 'happy' in m or 'party' in m: return 0.8
                if 'sad' in m or 'melancholy' in m: return 0.2
                if 'aggressive' in m: return 0.3
                if 'dark' in m: return 0.1
        return 0.5
    except: return 0.5

def get_lastfm_tags(artist_name):
    # ... (Standard logic omitted for brevity)
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        r = api_request_with_retry(url, verify=False)
        if not r: return [], 0.5
        tags = [t['name'].lower() for t in r.json()['artist']['tags']['tag']]
        ENERGY = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
        def score(d):
            h = [v for k,v in d.items() for t in tags if k in t]
            return sum(h)/len(h) if h else 0.5
        return tags, score(ENERGY)
    except: return [], 0.5

def get_deezer_data(artist_name):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        r = api_request_with_retry(url, headers=headers, verify=False)
        if not r: return None
        d = r.json()
        if not d.get('data'): return None
        artist = d['data'][0]
        
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_r = api_request_with_retry(track_url, headers=headers, verify=False)
        preview = None
        top_track_id = None
        if t_r:
            td = t_r.json()
            preview = td['data'][0]['preview'] if td.get('data') else None
            top_track_id = td['data'][0]['id'] if td.get('data') else None

        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "preview": preview, "top_track_id": top_track_id
        }
    except: return None

def get_top_tracks_previews(deezer_id):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={TRACKS_TO_ANALYZE}"
        r = api_request_with_retry(url, headers=headers, verify=False)
        if not r: return []
        tracks = []
        for t in r.json().get('data', []):
            if 'preview' in t and t['preview']:
                tracks.append({"title": t['title'], "preview": t['preview']})
        return tracks
    except: return []

# --- 1. AUDIO ANALYSIS ENGINE ---
def analyze_audio(preview_url):
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        if not preview_url: return None
        
        response = api_request_with_retry(preview_url, headers=headers, verify=False, timeout=15, attempts=3)
        if not response: return None 

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        # --- SILENCE C-LEVEL WARNINGS ---
        with ignore_stderr():
            y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        bpm = round(float(tempo[0])) if isinstance(tempo, np.ndarray) else round(float(tempo))
            
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        norm_brightness = min(brightness / 3000, 1.0)
        zcr = np.mean(librosa.feature.zero_crossing_rate(y))
        norm_noise = min(zcr * 10, 1.0) 
        rolloff = np.mean(librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0])
        norm_warmth = min(rolloff / 5000, 1.0)

        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        complexity = np.mean(np.std(chroma, axis=1))
        
        # FIX: Apply the statistically derived normalization divisor
        norm_complexity = min(complexity / COMPLEXITY_DIVISOR, 1.0) 
        
        return {
            "bpm": bpm, "brightness": norm_brightness, "noisiness": norm_noise, 
            "warmth": norm_warmth, "complexity": norm_complexity
        }
    except Exception: return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

# --- 2. CORE PROCESSOR (Healing Mode) ---
def process_artist_sql(name):
    from src.db_model import add_artist, add_track, synthesize_scores
    
    # 1. Fetch Metadata
    d_info = get_deezer_data(name)
    if not d_info: 
        print(f" [Skip: Deezer metadata unavailable]")
        return None
    
    clean_name = d_info['name']
    
    # Check DB
    df_check = fetch_all_artists_df()
    match = df_check[df_check['Artist'].str.lower() == clean_name.lower()]
    is_healing = not match.empty
    
    if is_healing:
        print(f"      🩺 HEALING: {clean_name}")
        old_complexity = match.iloc[0].get('avg_complexity', 0.0)
    
    tags, tag_energy = get_lastfm_tags(clean_name)
    if not tags: return None
    main_genre = tags[0].title() if tags else "Unknown"
    valence = get_audiodb_mood(clean_name)
    release_year = get_release_year(d_info['id'])

    # 2. Update Artist Record
    artist_data = {
        "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": d_info['listeners'],
        "Image URL": d_info['image'], "Valence": valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year
    }
    artist_id = add_artist(artist_data)
    
    # 3. Clean and Repopulate Tracks
    if is_healing:
        supabase = get_supabase_client_standalone()
        supabase.table("tracks").delete().eq("artist_id", artist_id).execute()
        
    tracks = get_top_tracks_previews(d_info['id'])
    analyzed_count = 0
    
    for t in tracks:
        audio_features = analyze_audio(t['preview'])
        if audio_features:
            track_record = {**t, **audio_features, "title": t['title'], "preview": t['preview']}
            add_track(artist_id, track_record)
            analyzed_count += 1
            # print(".", end="", flush=True) # Progress dots
            time.sleep(0.1)
    
    # 4. Synthesize Scores & Report
    if analyzed_count > 0:
        synthesize_scores(artist_id)
        if is_healing:
            # CRITICAL FIX: Query Supabase directly for the single updated record
            # Do NOT fetch the whole dataframe again
            supabase = get_supabase_client_standalone()
            resp = supabase.table("artists").select("avg_complexity").eq("id", artist_id).execute()
            if resp.data:
                new_complexity = resp.data[0]['avg_complexity']
                print(f"      ✅ Record healed. Old Comp: {old_complexity:.4f} -> New Comp: {new_complexity:.4f}")
    else:
        print("      ⚠️ Track analysis failed.")
    
    return clean_name

# --- 4. MAIN EXECUTION ---
def run_bulk_harvest():
    print(f"🚀 Starting SQL-Powered Full Audit & Heal...")
    print("------------------------------------------------")
    
    try:
        df = fetch_all_artists_df()
    except Exception as e:
        print(f"❌ Error connecting to DB: {e}")
        exit()
    
    source_artists = df['Artist'].tolist() if not df.empty else SEED_ARTISTS
    existing_artists_set = set(df['Artist'].str.lower().tolist()) if not df.empty else set()
    
    print(f"📚 Database contains {len(existing_artists_set)} artists.")
    print("------------------------------------------------")
    
    total_processed = 0
    consecutive_failures = 0
    
    for artist_name in source_artists:
        if consecutive_failures >= MAX_FAILURES_ALLOWED:
            print(f"\n🛑 {MAX_FAILURES_ALLOWED} consecutive failures reached. Exiting.")
            break
            
        print(f"\n🔍 Auditing: {artist_name}...", end="", flush=True)

        if artist_name.lower() in existing_artists_set:
            result_name = process_artist_sql(artist_name)
        else:
            # If new, logic is same just without 'Old Comp' print
            result_name = process_artist_sql(artist_name)
        
        if result_name:
            total_processed += 1
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            print(f" ❌ FAILED.")
        
        time.sleep(SLEEP_TIME)
        
    print(f"\n\n🎉 Job complete! Total records processed: {total_processed}.")

if __name__ == "__main__":
    run_bulk_harvest()