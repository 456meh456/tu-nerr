import requests
import time
import urllib3
import os
import tempfile
import numpy as np
import librosa
import pandas as pd
import random # Needed for sampling

# Import the SQL Database Manager
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# --- CONFIGURATION ---
SLEEP_TIME = 1.0  # Seconds between API calls to be polite
SEARCH_LIMIT = 50 # Neighbors to scan
MAX_PAGES = 3     
TRACKS_TO_ANALYZE = 5 # How many songs to listen to per band
MAX_SEEDS_PER_RUN = 20 # Prevents the script from taking forever as DB grows

# Seed list for cold start
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# Disable SSL warnings for the requests library
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# We need to load secrets just for the Last.fm API Key
import toml
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
except Exception as e:
    print(f"❌ Error loading secrets: {e}")
    exit()

# --- 1. AUDIO ANALYSIS ENGINE (LIBROSA) ---
def analyze_audio(preview_url):
    """Downloads MP3 to temp file and extracts 5-dimensional physics."""
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    try:
        if not preview_url: return None
        
        # 1. Download
        response = requests.get(preview_url, headers=headers, verify=False, timeout=10)
        if response.status_code != 200: return None

        # 2. Save to Temp (Windows fix)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        # 3. Load Audio
        # sr=22050 is standard for analysis, mono=True mixes to single channel
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        # 4. Extract Physics
        # BPM
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        bpm = round(float(tempo[0])) if isinstance(tempo, np.ndarray) else round(float(tempo))
            
        # Brightness (Spectral Centroid)
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        norm_brightness = min(brightness / 3000, 1.0)

        # Noisiness (Zero Crossing Rate) - Filters Rap/Percussive Cadence
        zcr = np.mean(librosa.feature.zero_crossing_rate(y))
        norm_noise = min(zcr * 10, 1.0) 

        # Warmth (Spectral Rolloff)
        rolloff = np.mean(librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0])
        norm_warmth = min(rolloff / 5000, 1.0)

        # Complexity (Chroma Variance)
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        complexity = np.mean(np.std(chroma, axis=1))
        norm_complexity = min(complexity * 5, 1.0)
        
        return {
            "bpm": bpm,
            "brightness": norm_brightness,
            "noisiness": norm_noise,
            "warmth": norm_warmth,
            "complexity": norm_complexity
        }

    except Exception as e:
        # print(f"Audio Error: {e}")
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

# --- 2. API HELPERS ---
def get_neighbors(artist, limit=50, page=1):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={API_KEY}&limit={limit}&page={page}&format=json"
    try:
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'similarartists' not in resp: return []
        return [a['name'] for a in resp['similarartists']['artist']]
    except: return []

def get_deezer_data(artist_name):
    """Fetches Artist ID, Fans, Image."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
        
        if not resp.get('data'): return None
        artist = resp['data'][0]
        
        return {
            "name": artist['name'], 
            "id": artist['id'], 
            "listeners": artist['nb_fan'], 
            "image": artist['picture_medium']
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
                    tracks.append({
                        "title": t['title'],
                        "preview": t['preview']
                    })
        return tracks
    except: return []

def get_lastfm_tags(artist_name):
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'error' in resp: return [], 0.5, 0.5
        
        tags = [t['name'].lower() for t in resp['artist']['tags']['tag']]
        
        # Scoring Logic for Vibe
        VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8, 'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5, 'progressive': 0.5, 'rock': 0.45, 'sad': 0.2, 'dark': 0.15, 'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.3, 'metal': 0.3, 'heavy': 0.3, 'thrash': 0.2, 'death': 0.1}
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6, 'acoustic': 0.2}
        
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
            
        return tags, score(ENERGY_SCORES), score(VALENCE_SCORES)
    except: return [], 0.5, 0.5

# --- 3. CORE PROCESSOR (SQL EDITION) ---
def process_artist_sql(name):
    # 1. Fetch Metadata
    d_info = get_deezer_data(name)
    if not d_info: return None
    
    clean_name = d_info['name']
    tags, tag_energy, valence = get_lastfm_tags(clean_name)
    main_genre = tags[0].title() if tags else "Unknown"

    print(f"   ✨ Found: {clean_name}")

    # 2. Create Artist in SQL (Parent Record)
    artist_data = {
        "Artist": clean_name,
        "Genre": main_genre,
        "Monthly Listeners": d_info['listeners'],
        "Image URL": d_info['image'],
        "Valence": valence,
        "Tag_Energy": tag_energy,
        "First Release Year": 0 # Placeholder until we add discography logic
    }
    
    # This inserts the artist and gets the Primary Key ID
    artist_id = add_artist(artist_data)
    
    # 3. Process Tracks (Child Records)
    tracks = get_top_tracks_previews(d_info['id'])
    analyzed_count = 0
    
    print(f"      🎧 Analyzing {len(tracks)} tracks for physics...")
    
    for t in tracks:
        audio_features = analyze_audio(t['preview'])
        if audio_features:
            # Merge title/url with physics data
            track_record = {**t, **audio_features}
            
            # Insert Track into SQL
            add_track(artist_id, track_record)
            
            analyzed_count += 1
            print(f"         - {t['title']} (BPM: {audio_features['bpm']})")
            time.sleep(0.5)
    
    # 4. Synthesize (Trigger SQL Average Calculation)
    if analyzed_count > 0:
        synthesize_scores(artist_id)
        print(f"      ⚗️ Scores synthesized and profile updated.")
    
    return clean_name

# --- 4. MAIN LOOP ---
def run_bulk_harvest():
    print(f"🚀 Starting SQL-Powered Harvest...")
    
    # Get current list from SQL to avoid duplicates
    try:
        df = fetch_all_artists_df()
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        df = pd.DataFrame()
    
    if df.empty:
        print("🚨 Database empty. Initializing with SEED ARTISTS.")
        source_artists = SEED_ARTISTS
        existing_artists = set()
    else:
        # SAMPLING LOGIC: Grab a random subset of existing artists to be the "seed" for this run
        # This prevents the script from trying to iterate through 5,000 bands every time.
        full_list = df['Artist'].tolist()
        if len(full_list) > MAX_SEEDS_PER_RUN:
            print(f"🎲 Database large. Sampling {MAX_SEEDS_PER_RUN} random artists to expand frontier.")
            source_artists = random.sample(full_list, MAX_SEEDS_PER_RUN)
        else:
            source_artists = full_list
            
        existing_artists = set(df['Artist'].str.lower().tolist())
    
    print(f"📚 Loaded {len(existing_artists)} existing artists. Expanding from {len(source_artists)} seeds.")
    
    total_added = 0
    
    for seed_artist in source_artists:
        print(f"🔍 Scanning neighbors of: {seed_artist}...")
        
        added_for_this_seed = 0
        page = 1
        
        while added_for_this_seed < 2 and page <= MAX_PAGES:
            candidates = get_neighbors(seed_artist, limit=SEARCH_LIMIT, page=page)
            
            if not candidates: break

            for cand in candidates:
                if added_for_this_seed >= 2: break
                
                if cand.lower() in existing_artists: continue
                
                # Run the SQL Processor
                result_name = process_artist_sql(cand)
                
                if result_name:
                    existing_artists.add(result_name.lower())
                    added_for_this_seed += 1
                    total_added += 1
                    print(f"   ✅ COMMITTED: {result_name}")
                
                time.sleep(SLEEP_TIME)
            
            page += 1
        
    print(f"🎉 Job Complete! Added {total_added} new artists.")

if __name__ == "__main__":
    run_bulk_harvest()