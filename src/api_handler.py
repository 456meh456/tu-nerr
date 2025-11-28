import pandas as pd
import requests
import time
import urllib3
import toml
import os
import tempfile
import numpy as np
import librosa
import json
import streamlit as st
import sys # For error printing
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION (Shared Constants) ---
AUDIODB_API_KEY = "2" # Public API key for AudioDB
LIVE_TRACK_LIMIT = 5

# FINAL CALIBRATION CONSTANTS (Derived from Audit)
# These constants ensure the raw Librosa values (Centroid, ZCR, etc.) are scaled between 0 and 1.
COMPLEXITY_DIVISOR = 0.3115 
NOISINESS_DIVISOR = 0.1771 
BRIGHTNESS_DIVISOR = 3569.1107 
WARMTH_DIVISOR = 7967.8935 

# Load API Key from secrets (required for local script context)
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    LASTFM_API_KEY = secrets["lastfm_key"]
except:
    LASTFM_API_KEY = "" # Fallback to empty string if secrets fails

# --- API HELPERS ---

def get_similar_artists(artist_name, api_key, limit=20):
    """Fetches similar artists from Last.fm (Social recommendation)."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist_name}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return [a['name'] for a in response.json().get('similarartists', {}).get('artist', [])]
    except: pass
    return []

def get_top_artists_by_genre(genre, api_key, limit=20):
    """Fetches top artists by genre/tag from Last.fm."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag={genre}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return [a['name'] for a in response.json().get('topartists', {}).get('artist', [])]
    except: pass
    return []

def get_artist_details(artist_name, api_key):
    """Fetches Last.fm bio and raw stats."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={api_key}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return response.json().get('artist')
    except: pass
    return None

def get_top_tracks(artist_name, api_key):
    """Fetches top tracks list for dashboard display."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={artist_name}&api_key={api_key}&limit=5&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return response.json().get('toptracks', {}).get('track', [])
    except: pass
    return None

def get_deezer_preview(artist_id):
    """Fetches the top track preview URL and title."""
    try:
        url = f"https://api.deezer.com/artist/{artist_id}/top"
        response = requests.get(url, verify=False, timeout=5)
        data = response.json()
        if data.get('data') and len(data['data']) > 0:
            track = data['data'][0]
            return { "title": track['title'], "preview": track['preview'] }
    except: pass
    return None

def get_release_year(artist_id):
    """Fetches the absolute earliest release year via discography scan."""
    earliest_date_str = None
    offset = 0
    limit = 50 
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    while True:
        try:
            url = f"https://api.deezer.com/artist/{artist_id}/albums?limit={limit}&index={offset}"
            resp = requests.get(url, headers=headers, verify=False, timeout=5)
            
            if resp.status_code != 200: break
            data = resp.json()

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

def get_audiodb_mood(artist_name):
    """Fetches a mood/valence score proxy from the AudioDB API."""
    try:
        url = f"http://www.theaudiodb.com/api/v1/json/{AUDIODB_API_KEY}/search.php?s={artist_name}"
        resp = requests.get(url, timeout=5)
        
        if resp.status_code != 200: return 0.5
        
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

def get_deezer_data(artist_name):
    """Fetches Deezer ID, Listeners, Image, and Preview URL for processing."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = requests.get(url, headers=headers, verify=False, timeout=5)
        
        if response.status_code != 200: return None
        data = response.json()

        if not data.get('data'): return None
        artist = data['data'][0]
        
        # Get Preview URL & Track ID
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_data = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
        preview = t_data['data'][0]['preview'] if t_data.get('data') else None
        top_track_id = t_data['data'][0]['id'] if t_data.get('data') else None

        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "preview": preview, "top_track_id": top_track_id
        }
    except Exception: return None

def get_top_tracks_previews(deezer_id, limit=LIVE_TRACK_LIMIT):
    """Fetches top tracks for analysis."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={limit}"
        resp = requests.get(url, headers=headers, verify=False, timeout=5)
        
        if resp.status_code != 200: return []
        
        tracks = []
        if resp.get('data'):
            for t in resp['data']:
                if 'preview' in t and t['preview']:
                    tracks.append({"title": t['title'], "preview": t['preview']})
        return tracks
    except: return []

def get_lastfm_tags(artist_name):
    """Fetches tags and calculates Tag_Energy."""
    try:
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={LASTFM_API_KEY}&format=json"
        resp = requests.get(url, verify=False, timeout=5)
        
        if resp.status_code != 200: return [], 0.5
        
        data = resp.json()
        if data.get('error'): return [], 0.5
        
        tags = [t['name'].lower() for t in data['artist']['tags']['tag']]
        
        VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
        
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
            
        return tags, score(ENERGY_SCORES)
    except Exception: 
        return [], 0.5


# --- AUDIO ANALYSIS & DATA PROCESSING ---

def analyze_audio(preview_url):
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
        
        zcr = librosa.feature.zero_crossing_rate(y)
        rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0]
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        complexity = np.mean(np.std(chroma, axis=1))
        
        # --- FINAL NORMALIZATION ---
        norm_brightness = min(brightness / BRIGHTNESS_DIVISOR, 1.0)
        norm_noise = min(np.mean(zcr) / NOISINESS_DIVISOR, 1.0) 
        norm_warmth = min(np.mean(rolloff) / WARMTH_DIVISOR, 1.0)
        norm_complexity = min(complexity / COMPLEXITY_DIVISOR, 1.0) 
        
        return {
            "bpm": bpm, "brightness": norm_brightness, "noisiness": norm_noise, 
            "warmth": norm_warmth, "complexity": norm_complexity
        }
    except Exception as e:
        print(f"Librosa Analysis Failed: {e}", file=sys.stderr, flush=True) 
        return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass


def process_artist(name, df_db, api_key, session_added_set):
    """Checks DB, fetches API data, analyzes audio, and saves artist to SQL."""
    
    from src.db_model import add_artist, add_track, synthesize_scores
    
    # 1. Check Local Session (Duplicate Prevention)
    if name.strip().lower() in session_added_set: return None
    # Check Database (Return existing data if found)
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty: return match.iloc[0].to_dict()

    # 2. Fetch Metadata (Deezer/LastFM)
    d_info = get_deezer_data(name)
    if not d_info: return None
    clean_name = d_info['name']
    if clean_name.strip().lower() in session_added_set: return None

    tags, tag_energy = get_lastfm_tags(clean_name)
    if not tags: return None
    
    valence = get_audiodb_mood(clean_name)
    main_genre = tags[0].title() if tags else "Unknown"
    release_year = get_release_year(d_info['id'])

    # 3. INSERT/UPDATE Parent Artist (SQL)
    artist_data = {
        "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": d_info['listeners'],
        "Image URL": d_info['image'], "Valence": valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year
    }
    artist_id = add_artist(artist_data)

    # 4. LIVE AUDIO ANALYSIS (MULTI-TRACK LOOP)
    tracks = get_top_tracks_previews(d_info['id']) 
    analyzed_count = 0
    
    for t in tracks:
        phys = analyze_audio(t['preview'])
        if phys:
            track_record = {**phys, "title": t['title'], "preview_url": t['preview']}
            add_track(artist_id, track_record)
            analyzed_count += 1
            time.sleep(0.5)

    # 5. Synthesize Scores
    if analyzed_count > 0:
        synthesize_scores(artist_id)
    
    # 6. Return Data for UI 
    final_data = artist_data.copy()
    if 'phys' in locals() and phys:
        final_data['Audio_BPM'] = phys['bpm']
        final_data['Audio_Brightness'] = phys['brightness']
    else:
        final_data['Audio_BPM'] = 0
        final_data['Audio_Brightness'] = tag_energy

    session_added_set.add(clean_name.strip().lower())
    return final_data