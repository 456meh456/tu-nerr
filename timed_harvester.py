import requests
import time
import subprocess
import sys
import argparse
import random
import os
import tempfile
import numpy as np
import librosa
import pandas as pd
import toml
import urllib3
from supabase import create_client, Client

# --- CONFIGURATION ---
SEARCH_LIMIT = 50 
MAX_PAGES = 3     
TRACKS_TO_ANALYZE = 5 
SLEEP_TIME = 0.5 
MAX_SEEDS_PER_RUN = 50 

# FINAL CALIBRATION CONSTANTS
COMPLEXITY_DIVISOR = 0.3115 
NOISINESS_DIVISOR = 0.1771 
BRIGHTNESS_DIVISOR = 3569.1107 
WARMTH_DIVISOR = 7967.8935 

# Seed list for cold start
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]
AUDIODB_API_KEY = "2" 

# --- AUTH SETUP ---
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
    SUPABASE_URL = secrets["supabase"]["url"]
    SUPABASE_KEY = secrets["supabase"]["key"]
except Exception as e:
    print(f"❌ CRITICAL ERROR: Could not load secrets. Details: {e}")
    sys.exit(1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- DB FUNCTIONS ---

def get_supabase_client_standalone():
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception: return None

def fetch_all_artists_df():
    client = get_supabase_client_standalone()
    if not client: raise ConnectionError("Supabase client is not available.")
    
    response = client.table("artists").select(
        "id, name, listeners, avg_brightness, valence, avg_bpm, image_url, tag_energy"
    ).execute()
    
    df = pd.DataFrame(response.data)
    if df.empty: return pd.DataFrame()
    
    df = df.rename(columns={"name": "Artist", "listeners": "Monthly Listeners"})
    df['Artist_Lower'] = df['Artist'].str.lower()
    return df

def add_artist(data):
    supabase = get_supabase_client_standalone()
    if not supabase: return None

    payload = {
        "name": data['Artist'],
        "genre": data.get('Genre', 'Unknown'),
        "listeners": int(data.get('Monthly Listeners', 0)),
        "image_url": data.get('Image URL', ''),
        "first_release_year": data.get('First Release Year'),
        "valence": float(data.get('Valence', 0.5)),
        "tag_energy": float(data.get('Tag_Energy', 0.5))
    }
    
    response = supabase.table("artists").upsert(payload, on_conflict="name").execute()
    return response.data[0]['id'] if response.data else None

def add_track(artist_id, track_data):
    supabase = get_supabase_client_standalone()
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
    supabase = get_supabase_client_standalone()
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

# --- API/ANALYSIS LOGIC ---

def api_request_with_retry(url, headers=None, verify=True, timeout=5, attempts=3):
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=headers, verify=verify, timeout=timeout)
            if response.status_code == 200: return response
            elif response.status_code in [403, 429, 500]: time.sleep(attempt + 1)
            elif response.status_code == 404: return None
        except requests.exceptions.RequestException: time.sleep(attempt + 1)
    return None

def get_audiodb_mood(artist_name):
    try:
        url = f"http://www.theaudiodb.com/api/v1/json/{AUDIODB_API_KEY}/search.php?s={artist_name}"
        resp = api_request_with_retry(url)
        if not resp: return 0.5
        
        data = resp.json()
        if data.get('artists') and data['artists']:
            mood_raw = data['artists'][0].get('strMood')
            if mood_raw:
                mood_raw = mood_raw.lower()
                if 'happy' in mood_raw or 'party' in mood_raw: return 0.8
                if 'sad' in mood_raw or 'melancholy' in mood_raw: return 0.2
                if 'aggressive' in mood_raw or 'angry' in mood_raw: return 0.3
                if 'dark' in mood_raw or 'gothic' in mood_raw: return 0.1
        return 0.5 
    except Exception: return 0.5 

def get_neighbors(artist, limit=50, page=1):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={API_KEY}&limit={limit}&page={page}&format=json"
    try:
        resp = requests.get(url, verify=False, timeout=5).json()
        return [a['name'] for a in resp['similarartists']['artist']] if 'similarartists' in resp else []
    except: return []

def analyze_audio(preview_url):
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        if not preview_url: return None
        response = api_request_with_retry(preview_url, headers=headers, verify=False, timeout=10, attempts=3)
        if not response: return None 

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        bpm = round(float(tempo[0])) if isinstance(tempo, np.ndarray) else round(float(tempo))
        
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        zcr = librosa.feature.zero_crossing_rate(y)
        rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0]
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        complexity = np.mean(np.std(chroma, axis=1))
        
        norm_brightness = min(brightness / BRIGHTNESS_DIVISOR, 1.0)
        norm_noise = min(np.mean(zcr) / NOISINESS_DIVISOR, 1.0) 
        norm_warmth = min(np.mean(rolloff) / WARMTH_DIVISOR, 1.0)
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

def get_deezer_data(artist_name):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = api_request_with_retry(url, headers=headers, verify=False)
        if not response: return None
        data = response.json()
        if not data.get('data'): return None
        artist = data['data'][0]
        
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_resp = api_request_with_retry(track_url, headers=headers, verify=False)
        preview = None
        top_track_id = None
        if t_resp:
            td = t_resp.json()
            preview = td['data'][0]['preview'] if td.get('data') else None
            top_track_id = td['data'][0]['id'] if td.get('data') else None

        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "preview": preview, "top_track_id": top_track_id
        }
    except Exception: return None

def get_lastfm_tags(artist_name):
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        response = api_request_with_retry(url, verify=False)
        if not response: return [], 0.5
        data = response.json()
        if data.get('error'): return [], 0.5
        tags = [t['name'].lower() for t in data['artist']['tags']['tag']]
        VALENCE = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
        ENERGY = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
        return tags, score(ENERGY)
    except: return [], 0.5

def get_release_year(artist_id):
    earliest_date_str = None
    offset = 0
    limit = 50 
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    while True:
        url = f"https://api.deezer.com/artist/{artist_id}/albums?limit={limit}&index={offset}"
        response = api_request_with_retry(url, headers=headers, verify=False)
        if not response: break
        try:
            data = response.json()
            if 'data' not in data or not data['data']: break
            dates = [album.get('release_date') for album in data['data'] if album.get('release_date')]
            if dates:
                current_min_date = min(dates)
                if earliest_date_str is None or current_min_date < earliest_date_str:
                    earliest_date_str = current_min_date
            if data.get('total') is not None and data['total'] <= offset + limit: break
            if 'next' in data: offset += limit
            else: break
        except Exception: break 
    return int(earliest_date_str[:4]) if earliest_date_str else 0

def get_top_tracks_previews(deezer_id):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={TRACKS_TO_ANALYZE}"
        response = api_request_with_retry(url, headers=headers, verify=False)
        if not response: return []
        data = response.json()
        tracks = []
        if data.get('data'):
            for t in data['data']:
                if 'preview' in t and t['preview']:
                    tracks.append({"title": t['title'], "preview": t['preview']})
        return tracks
    except: return []


# --- CORE PROCESSOR ---

def process_artist_and_commit(name, existing_artists):
    """Processes one artist record (fetching data and committing to SQL)."""
    
    print(f"   🔎 Inspecting: {name}")

    # 1. Metadata
    d_info = get_deezer_data(name)
    if not d_info:
        print(f"      ❌ Skip (No Deezer)")
        return None
    
    clean_name = d_info['name']
    print(f"      ✅ Match: {clean_name} (ID: {d_info['id']})")
    
    if clean_name.lower() in existing_artists:
        print(f"      ⏭️  Skip (Exists)")
        return clean_name
        
    tags, tag_energy = get_lastfm_tags(clean_name)
    if not tags: 
        print(f"      ❌ Skip (No Tags)")
        return None
    
    print(f"      🏷️  Tags: {', '.join(tags[:3])}...")
        
    main_genre = tags[0].title() if tags else "Unknown"
    valence = get_audiodb_mood(clean_name)
    release_year = get_release_year(d_info['id'])
    
    print(f"      📅 Year: {release_year} | Mood: {valence:.2f} | Energy: {tag_energy:.2f}")

    # 2. Insert Parent
    artist_data = {
        "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": d_info['listeners'],
        "Image URL": d_info['image'], "Valence": valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year
    }
    artist_id = add_artist(artist_data)
    print(f"      💾 Created Artist Record (ID: {artist_id})")

    # 3. Tracks
    tracks = get_top_tracks_previews(d_info['id'])
    analyzed_count = 0
    
    if tracks:
        print(f"      🎧 Analyzing {len(tracks)} tracks...")
        for t in tracks:
            print(f"         - {t['title']}...", end="", flush=True)
            phys = analyze_audio(t['preview'])
            if phys:
                track_record = {**phys, "title": t['title'], "preview": t['preview']}
                add_track(artist_id, track_record)
                analyzed_count += 1
                print(f" OK (BPM: {phys['bpm']})")
            else:
                print(" Failed")
            time.sleep(0.1)
    
    # 4. Synthesize
    if analyzed_count > 0:
        synthesize_scores(artist_id)
        print(f"      ⚗️  Scores synthesized.")
    else:
        print(f"      ⚠️ Added (No Audio)")
    
    return clean_name

# --- MAIN SCHEDULER ---

def run_automated_harvest_scheduler(time_limit_minutes, max_seeds):
    
    time_limit_seconds = time_limit_minutes * 60
    start_time = time.time()
    total_added = 0
    
    print(f"\n--- ⏳ STARTING AUTONOMOUS HARVEST (MATRIX MODE) ---")
    
    try:
        df = fetch_all_artists_df()
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return

    if df.empty:
        source_artists = SEED_ARTISTS
        print("🚨 COLD START: Running full seed list.")
    else:
        full_list = df['Artist'].tolist()
        sample_size = min(len(full_list), max_seeds)
        source_artists = random.sample(full_list, sample_size)
        print(f"🎲 Sampling {sample_size} artists from {len(full_list)} total.")

    existing_artists = set(df['Artist'].str.lower().tolist()) if not df.empty else set()
    
    for seed_artist in source_artists:
        if time.time() - start_time > time_limit_seconds:
            print(f"\n🛑 Time limit reached.")
            break
            
        print(f"\n📡 Scanning neighbors of: {seed_artist}...")
        
        added_for_this_seed = 0
        page = 1
        
        while added_for_this_seed < 2 and page <= MAX_PAGES:
            candidates = get_neighbors(seed_artist, limit=SEARCH_LIMIT, page=page)
            if not candidates: break

            for cand in candidates:
                if time.time() - start_time > time_limit_seconds: break
                if added_for_this_seed >= 2: break
                
                # NOTE: removed duplicate check here, it's handled verbosely inside process_artist_and_commit
                
                result_name = process_artist_and_commit(cand, existing_artists)
                
                if result_name:
                    existing_artists.add(result_name.lower())
                    added_for_this_seed += 1
                    total_added += 1
                
                time.sleep(SLEEP_TIME)
            page += 1
        
    print(f"\n🎉 JOB FINISHED. Total new artists added: {total_added}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20, help="Time limit in minutes.")
    parser.add_argument("--seeds", type=int, default=50, help="Max seeds.")
    args = parser.parse_args()
    run_automated_harvest_scheduler(time_limit_minutes=args.limit, max_seeds=args.seeds)