import pandas as pd
import requests
import time
import urllib3
import os
import tempfile
import numpy as np
import librosa
import json
from src.db_model import add_artist, add_track, synthesize_scores, fetch_all_artists_df

# Disable SSL warnings for API calls
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    """
    FIX: Fetches the full discography and finds the absolute earliest release year
    by iterating through the album list and finding the minimum date.
    """
    earliest_date_str = None
    offset = 0
    limit = 50 # Max items per page
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    while True:
        try:
            url = f"https://api.deezer.com/artist/{artist_id}/albums?limit={limit}&index={offset}"
            resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
            
            if 'data' not in resp or not resp['data']:
                break
                
            dates = [album.get('release_date') for album in resp['data'] if album.get('release_date')]
            
            if dates:
                current_min_date = min(dates)
                if earliest_date_str is None or current_min_date < earliest_date_str:
                    earliest_date_str = current_min_date

            # If total count is less than the current index + limit, we are done
            if resp.get('total') is not None and resp['total'] <= offset + limit:
                break
                
            # If there is a 'next' link, continue to the next page
            if 'next' in resp:
                offset += limit
            else:
                break
            
        except Exception:
            break # Stop on any network error

    if earliest_date_str:
        return int(earliest_date_str[:4])
    
    return 0


# --- AUDIO ANALYSIS & DATA PROCESSING ---

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
        
        # Load Audio and extract features
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_env, sr=sr)
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
            "bpm": bpm,
            "brightness": norm_brightness,
            "noisiness": norm_noise,
            "warmth": norm_warmth,
            "complexity": norm_complexity
        }
    except Exception as e: 
        print(f"Librosa Analysis Failed: {e}")
        return None 
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

def get_deezer_data(artist_name):
    """Fetches Deezer ID, Listener count, Image, and Preview URL for processing."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = requests.get(url, headers=headers, verify=False, timeout=5)
        if response.status_code != 200 or not response.json().get('data'): return None
        artist = response.json()['data'][0]
        
        # We need the top track ID for analysis and year lookup
        track_url = f"https://api.deezer.com/artist/{artist['id']}/top?limit=1"
        t_data = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
        preview = t_data['data'][0]['preview'] if t_data.get('data') else None
        top_track_id = t_data['data'][0]['id'] if t_data.get('data') else None


        return {
            "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
            "image": artist['picture_medium'], "link": artist['link'], "preview": preview,
            "top_track_id": top_track_id # Export the ID needed for year lookup
        }
    except: return None


def process_artist(name, df_db, api_key, session_added_set):
    """Checks DB, fetches API data, analyzes audio, and saves artist to SQL."""
    
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
    VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
    ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
    
    def score(d):
        h = [v for k,v in d.items() for t in tags if k in t]
        return sum(h)/len(h) if h else 0.5

    tag_energy = score(ENERGY_SCORES)
    tag_valence = score(VALENCE_SCORES)
    main_genre = tags[0].title() if tags else "Unknown"

    # 4. GET RELEASE YEAR (Time Travel)
    release_year = 0
    if deezer_info.get('id'):
        release_year = get_release_year(deezer_info['id'])


    # 5. INSERT PARENT ARTIST (SQL)
    artist_data = {
        "Artist": clean_name, "Genre": main_genre,
        "Monthly Listeners": deezer_info['listeners'], "Image URL": deezer_info['image'],
        "Valence": tag_valence, "Tag_Energy": tag_energy,
        "First Release Year": release_year # <-- NOW CORRECTLY FETCHED
    }
    artist_id = add_artist(artist_data)

    # 6. LIVE AUDIO ANALYSIS (1 Track for Speed)
    phys = analyze_audio(deezer_info['preview'])
    if phys:
        # Add Track to SQL
        track_rec = {
            "title": "Top Track (Live Scan)", # Use placeholder title for live search
            "preview": deezer_info['preview'],
            **phys
        }
        add_track(artist_id, track_rec)
        synthesize_scores(artist_id) # Update averages

    # 7. Return Data for UI 
    final_data = artist_data.copy()
    final_data['Audio_BPM'] = phys['bpm'] if phys else 0
    final_data['Audio_Brightness'] = phys['brightness'] if phys else 0
    final_data['Audio_Noisiness'] = phys['noisiness'] if phys else 0
    final_data['Audio_Warmth'] = phys['warmth'] if phys else 0
    final_data['Audio_Complexity'] = phys['complexity'] if phys else 0

    session_added_set.add(clean_name.strip().lower())
    return final_data