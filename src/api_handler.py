import pandas as pd
import requests
import time
import urllib3
import os
import tempfile
import numpy as np
import librosa
import json
import streamlit as st
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
LIVE_TRACK_LIMIT = 5 # UPDATED: Analyze 5 tracks for better data quality (slower but accurate)

# --- API HELPERS ---

def get_similar_artists(artist_name, api_key, limit=10):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist_name}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return [a['name'] for a in response.json().get('similarartists', {}).get('artist', [])]
    except: pass
    return []

def get_top_artists_by_genre(genre, api_key, limit=12):
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag={genre}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return [a['name'] for a in response.json().get('topartists', {}).get('artist', [])]
    except: pass
    return []

def get_artist_details(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={api_key}&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return response.json().get('artist')
    except: pass
    return None

def get_top_tracks(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={artist_name}&api_key={api_key}&limit=5&format=json"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200: return response.json().get('toptracks', {}).get('track', [])
    except: pass
    return []

def get_deezer_preview(artist_id):
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

def get_audiodb_mood(artist_name):
    AUDIODB_API_KEY = "2" 
    try:
        url = f"http://www.theaudiodb.com/api/v1/json/{AUDIODB_API_KEY}/search.php?s={artist_name}"
        resp = requests.get(url, timeout=5).json()
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

def get_deezer_data(artist_name):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = requests.get(url, headers=headers, verify=False, timeout=5)
        if response.status_code != 200 or not response.json().get('data'): return None
        artist = response.json()['data'][0]
        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "link": artist['link']
        }
    except: return None

def get_top_tracks_previews(deezer_id, limit=5):
    """Fetches top tracks (Title + Preview URL) for analysis."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://api.deezer.com/artist/{deezer_id}/top?limit={limit}"
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
        # HACK: Retrieve API_KEY from secrets if running in the original bare-metal context
        import toml
        SECRETS_PATH = ".streamlit/secrets.toml"
        API_KEY = toml.load(SECRETS_PATH).get("lastfm_key")
        
        url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={API_KEY}&format=json"
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'error' in resp: return [], 0.5
        tags = [t['name'].lower() for t in resp['artist']['tags']['tag']]
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6, 'acoustic': 0.2}
        def score(d):
            hits = [v for k,v in d.items() for t in tags if k in t]
            return sum(hits)/len(hits) if hits else 0.5
        return tags, score(ENERGY_SCORES)
    except: return [], 0.5


# --- AUDIO ANALYSIS ---

def analyze_audio(preview_url):
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        if not preview_url: return None
        response = requests.get(preview_url, headers=headers, verify=False, timeout=5) # Short timeout for live app
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

def process_artist(name, df_db, api_key, session_added_set):
    """Checks DB, fetches API data, analyzes audio (MULTI-TRACK), and saves artist to SQL."""
    
    # 1. Check Local Session (Duplicate Prevention)
    if name.strip().lower() in session_added_set: return None
    # Check Database (Return existing data if found)
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty: return match.iloc[0].to_dict()

    # 2. Fetch Metadata (Deezer/LastFM)
    deezer_info = get_deezer_data(name)
    if not deezer_info: return None
    
    clean_name = deezer_info['name']
    if clean_name.strip().lower() in session_added_set: return None

    lastfm_info = get_artist_details(clean_name, api_key)
    if not lastfm_info: return None

    # 3. Calculate Scores
    tags = [t['name'].lower() for t in lastfm_info['tags']['tag']]
    VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'pop': 0.8, 'sad': 0.2, 'dark': 0.15, 'doom': 0.1, 'metal': 0.3}
    
    def score(d):
        h = [v for k,v in d.items() for t in tags if k in t]
        return sum(h)/len(h) if h else 0.5

    tag_energy = get_lastfm_tags(clean_name)[1] # Re-use existing function for energy logic
    tag_valence = score(VALENCE_SCORES)
    main_genre = tags[0].title() if tags else "Unknown"

    # 4. GET RELEASE YEAR
    release_year = 0
    if deezer_info.get('id'):
        release_year = get_release_year(deezer_info['id'])


    # 5. INSERT PARENT ARTIST (SQL)
    artist_data = {
        "Artist": clean_name, "Genre": main_genre,
        "Monthly Listeners": deezer_info['listeners'], "Image URL": deezer_info['image'],
        "Valence": tag_valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year
    }
    artist_id = add_artist(artist_data)

    # 6. LIVE AUDIO ANALYSIS (MULTI-TRACK LOOP)
    # Fetch top tracks for this artist
    tracks = get_top_tracks_previews(deezer_info['id'], limit=LIVE_TRACK_LIMIT)
    analyzed_count = 0
    
    # Container for the first valid track to return for immediate UI display
    first_valid_phys = None

    if tracks:
        for t in tracks:
            phys = analyze_audio(t['preview'])
            if phys:
                if not first_valid_phys: first_valid_phys = phys
                
                # Save track with REAL TITLE
                track_rec = {**phys, "title": t['title'], "preview": t['preview']}
                add_track(artist_id, track_rec)
                analyzed_count += 1

    # 7. Synthesize Scores (Update Parent)
    if analyzed_count > 0:
        synthesize_scores(artist_id)

    # 8. Return Data for UI 
    final_data = artist_data.copy()
    if first_valid_phys:
        final_data['Audio_BPM'] = first_valid_phys['bpm']
        final_data['Audio_Brightness'] = first_valid_phys['brightness']
        final_data['Audio_Noisiness'] = first_valid_phys['noisiness']
        final_data['Audio_Warmth'] = first_valid_phys['warmth']
        final_data['Audio_Complexity'] = first_valid_phys['complexity']
    else:
        # Fallback to defaults if no audio could be analyzed live
        final_data['Audio_BPM'] = 0
        final_data['Audio_Brightness'] = tag_energy 

    session_added_set.add(clean_name.strip().lower())
    return final_data