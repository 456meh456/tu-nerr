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

# Import the SQL Database Manager
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# --- CONFIGURATION ---
SEARCH_LIMIT = 50 
MAX_PAGES = 3     
TRACKS_TO_ANALYZE = 5 
MAX_SEEDS_PER_RUN = 20 # Use a small number for testing stability
SLEEP_TIME = 0.5 

# Seed list for cold start
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# Disable SSL warnings for the requests library
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load Secrets for API Keys
import toml
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
except Exception as e:
    print(f"❌ Error loading secrets: {e}")
    sys.exit(1)

# --- 1. AUDIO ANALYSIS ENGINE (LIBROSA) ---
def analyze_audio(preview_url):
    """Downloads MP3 to temp file and extracts 5-dimensional physics."""
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        if not preview_url: return None
        
        response = requests.get(preview_url, headers=headers, verify=False, timeout=10)
        if response.status_code != 200: return None

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
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
        norm_complexity = min(complexity * 5, 1.0)
        
        return {
            "bpm": bpm, "brightness": norm_brightness, "noisiness": norm_noise, 
            "warmth": norm_warmth, "complexity": norm_complexity
        }

    except Exception: return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

# --- 2. API HELPERS ---
def get_neighbors(artist, limit=50, page=1):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={API_KEY}&limit={limit}&page={page}&format=json"
    try:
        resp = requests.get(url, verify=False, timeout=5).json()
        return [a['name'] for a in resp['similarartists']['artist']] if 'similarartists' in resp else []
    except: return []

def get_release_year(artist_id):
    """Fetches the full discography and finds the absolute earliest release year."""
    earliest_date_str = None
    offset = 0
    limit = 50 
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    while True:
        try:
            url = f"https://api.deezer.com/artist/{artist_id}/albums?limit={limit}&index={offset}"
            resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
            
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

def get_deezer_data(artist_name):
    """Fetches Deezer ID, Listeners, Image, and Preview URL."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = requests.get(url, headers=headers, verify=False, timeout=5).json()
        if response.status_code != 200 or not response.json().get('data'): return None
        artist = response.json()['data'][0]
        
        # Get Top Track Preview URL
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_data = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
        preview = t_data['data'][0]['preview'] if t_data.get('data') else None
        top_track_id = t_data['data'][0]['id'] if t_data.get('data') else None


        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "link": artist['link'], "preview": preview,
            "top_track_id": top_track_id
        }
    except: return None

def get_top_tracks_previews(deezer_id):
    """Fetches top tracks for analysis."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={TRACKS_TO_ANALYZE}"
        resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
        
        tracks = []
        if resp.get('data'):
            for t in resp['data']:
                if 'preview' in t and t['preview']:
                    tracks.append({"title": t['title'], "preview": t['preview']})
        return tracks
    except: return []

def get_lastfm_tags(artist_name):
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'error' in resp: return [], 0.5, 0.5
        
        tags = [t['name'].lower() for t in resp['artist']['tags']['tag']]
        
        VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
        
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
            
        return tags, score(ENERGY_SCORES), score(VALENCE_SCORES)
    except: return [], 0.5, 0.5


# --- 3. CORE PROCESSOR (SQL EDITION) ---
def process_artist_sql(name):
    # Ensure this script has access to db_model functions
    from src.db_model import add_artist, add_track, synthesize_scores
    
    # 1. Fetch Metadata
    d_info = get_deezer_data(name)
    if not d_info: return None
    
    clean_name = d_info['name']
    
    # Check DB (Avoid re-processing known bands)
    df_check = fetch_all_artists_df()
    if not df_check.empty and clean_name.lower() in df_check['Artist_Lower'].tolist():
        return clean_name
        
    tags, tag_energy, valence = get_lastfm_tags(clean_name)
    main_genre = tags[0].title() if tags else "Unknown"

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
        audio_features = analyze_audio(t['preview'])
        if audio_features:
            track_record = {**t, **audio_features, "title": t['title']}
            add_track(artist_id, track_record)
            analyzed_count += 1
            time.sleep(0.5)
    
    # 4. Synthesize Scores
    if analyzed_count > 0:
        synthesize_scores(artist_id)
    
    return clean_name

# --- 4. MAIN LOOP HANDLER (Scheduler Logic) ---

def run_automated_harvest(max_seeds, time_limit_minutes):
    """
    Core function that samples the frontier and processes the artists within 
    the given time constraints.
    """
    
    from src.db_model import fetch_all_artists_df # Get the database fetcher

    time_limit_seconds = time_limit_minutes * 60
    start_time = time.time()
    total_added = 0
    
    # 1. Load Data and determine seeds
    try:
        df = fetch_all_artists_df()
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        return

    if df.empty:
        source_artists = SEED_ARTISTS
        existing_artists = set()
    else:
        full_list = df['Artist'].tolist()
        # Sample the existing list to prevent excessive runtimes
        source_artists = random.sample(full_list, min(len(full_list), max_seeds))
        existing_artists = set(df['Artist'].str.lower().tolist())
    
    print(f"📚 Loaded {len(existing_artists)} existing artists. Sampling {len(source_artists)} seeds.")
    
    
    for seed_artist in source_artists:
        if time.time() - start_time > time_limit_seconds:
            print(f"\n🛑 Time limit of {time_limit_minutes} minutes reached. Stopping harvest.")
            break
            
        print(f"\n🔍 Scanning neighbors of: {seed_artist}...")
        
        added_for_this_seed = 0
        page = 1
        
        while added_for_this_seed < 2 and page <= MAX_PAGES:
            candidates = get_neighbors(seed_artist, limit=SEARCH_LIMIT, page=page)
            
            if not candidates: break

            for cand in candidates:
                if time.time() - start_time > time_limit_seconds: break
                if added_for_this_seed >= 2: break
                if cand.lower() in existing_artists: continue
                
                # Process and commit new artist
                result_name = process_artist_sql(cand)
                
                if result_name:
                    existing_artists.add(result_name.lower())
                    added_for_this_seed += 1
                    total_added += 1
                    print(f"   ✅ COMMITTED: {result_name}")
                
                time.sleep(SLEEP_TIME)
            
            page += 1
        
    print(f"\n🎉 Job segment complete! Total new artists added: {total_added}.")
    return total_added


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs a time-limited, sampled data harvest job.")
    parser.add_argument("--limit", type=int, default=10, help="Time limit in minutes for the job.")
    parser.add_argument("--seeds", type=int, default=20, help="Maximum number of artists to select for scanning.")
    
    args = parser.parse_args()
    
    # Execute the core function
    run_automated_harvest(max_seeds=args.seeds, time_limit_minutes=args.limit)