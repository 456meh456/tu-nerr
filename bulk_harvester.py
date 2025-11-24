import pandas as pd
import requests
import gspread
import time
import toml
import urllib3
import io
import os
import tempfile
import numpy as np
import librosa 
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
BATCH_SIZE = 5    # Saving frequently is safer
SLEEP_TIME = 1.0  # Respect API limits
SEARCH_LIMIT = 50 
MAX_PAGES = 3     
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. SETUP AUTH ---
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
    GCP_SECRETS = secrets["gcp_service_account"]
except Exception as e:
    print(f"❌ Error loading secrets: {e}")
    exit()

def get_sheet_connection():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # FIX: Explicitly fix line breaks in the key before use
    private_key_string = GCP_SECRETS["private_key"]
    if "\\n" in private_key_string:
        fixed_private_key = private_key_string.replace('\\n', '\n')
    else:
        fixed_private_key = private_key_string
        
    creds_info = {
        "type": GCP_SECRETS["type"],
        "project_id": GCP_SECRETS["project_id"],
        "private_key_id": GCP_SECRETS["private_key_id"],
        "private_key": fixed_private_key,
        "client_email": GCP_SECRETS["client_email"],
        "client_id": GCP_SECRETS["client_id"],
        "auth_uri": GCP_SECRETS["auth_uri"],
        "token_uri": GCP_SECRETS["token_uri"],
        "auth_provider_x509_cert_url": GCP_SECRETS["auth_provider_x509_cert_url"],
        "client_x509_cert_url": GCP_SECRETS["client_x509_cert_url"],
    }
    
    creds = Credentials.from_service_account_info(creds_info, scopes=scope)
    client = gspread.authorize(creds)
    return client.open("tu-nerr-db").sheet1

def load_current_db():
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # SCHEMA CHECK: Ensure we have the new columns
    expected_cols = ['Artist', 'Genre', 'Monthly Listeners', 'Tag_Energy', 'Valence', 'Audio_BPM', 'Audio_Brightness', 'Image URL']
    if df.empty or 'Artist' not in df.columns:
        return pd.DataFrame(columns=expected_cols)
        
    return df

def append_to_sheet(new_rows_df):
    sheet = get_sheet_connection()
    
    # If sheet is empty, write headers first
    if len(sheet.get_all_values()) == 0:
        sheet.append_row(new_rows_df.columns.tolist())
        
    values = new_rows_df.values.tolist()
    sheet.append_rows(values)
    print(f"💾 Batch Saved: {len(values)} artists added to Cloud.")

# --- 2. AUDIO ANALYSIS ENGINE ---
def analyze_audio(preview_url):
    """Downloads MP3 to temp file to fix Windows read errors."""
    tmp_path = None
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        if not preview_url: return 0, 0
        
        # 1. Download MP3 with Strict Timeout
        response = requests.get(preview_url, headers=headers, verify=False, timeout=10)
        
        # 2. Save to Temp File
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name
        
        # 3. Load into Librosa
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        # 4. Extract Features
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        
        if isinstance(tempo, np.ndarray):
            bpm = round(float(tempo[0]))
        elif isinstance(tempo, list):
             bpm = round(float(tempo[0]))
        else:
            bpm = round(float(tempo))
            
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        norm_brightness = min(brightness / 3000, 1.0)
        
        return bpm, round(norm_brightness, 2)

    except Exception as e:
        # print(f"      ⚠️ Audio skipped: {e}") 
        return 0, 0 
    finally:
        # 5. Cleanup
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

# --- 3. API HELPERS ---
def get_neighbors(artist, limit=50, page=1):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={API_KEY}&limit={limit}&page={page}&format=json"
    try:
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'similarartists' not in resp: return []
        return [a['name'] for a in resp['similarartists']['artist']]
    except: return []

def get_details_and_audio(artist):
    d_data = None
    preview_url = None
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)'}

    # 1. Deezer
    try:
        d_url = f"https://api.deezer.com/search/artist?q={artist}"
        d_resp = requests.get(d_url, headers=headers, verify=False, timeout=5).json()
        if d_resp.get('data'): 
            d_data = d_resp['data'][0]
            # Get Top Track
            track_url = f"https://api.deezer.com/artist/{d_data['id']}/top"
            t_resp = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
            if t_resp.get('data'):
                preview_url = t_resp['data'][0]['preview']
    except: pass

    if not d_data: return None

    # 2. Last.fm
    try:
        l_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={d_data['name']}&api_key={API_KEY}&format=json"
        l_resp = requests.get(l_url, verify=False, timeout=5).json()
        if 'error' in l_resp: return None
        tags = [t['name'].lower() for t in l_resp['artist']['tags']['tag']]
    except: return None

    # 3. Scoring
    ENERGY_SCORES = {'death': 1.0, 'thrash': 0.95, 'core': 0.95, 'metal': 0.9, 'punk': 0.9, 'heavy': 0.9, 'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5, 'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35, 'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15}
    VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8, 'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5, 'progressive': 0.5, 'alternative': 0.4, 'rock': 0.45, 'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05, 'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.3, 'metal': 0.3, 'heavy': 0.3, 'thrash': 0.2, 'death': 0.1}

    def calc_score(t_list, s_dict):
        scores = [score for tag, score in s_dict.items() for t in t_list if tag in t]
        return sum(scores)/len(scores) if scores else 0.5

    tag_valence = calc_score(tags, VALENCE_SCORES)
    tag_energy = calc_score(tags, ENERGY_SCORES)

    # 4. RUN REAL AUDIO ANALYSIS
    print(f"      🎧 Listening to {d_data['name']}...", end="", flush=True)
    real_bpm, real_brightness = analyze_audio(preview_url)
    print(f" Done. (BPM: {real_bpm})")
    
    if real_bpm == 0: 
        real_brightness = tag_energy # Fallback

    return {
        "Artist": d_data['name'], 
        "Genre": tags[0].title() if tags else "Unknown",
        "Monthly Listeners": d_data['nb_fan'],
        "Tag_Energy": tag_energy,         
        "Valence": tag_valence,           
        "Audio_BPM": real_bpm,            
        "Audio_Brightness": real_brightness, 
        "Image URL": d_data['picture_medium']
    }

# --- 4. MAIN EXECUTION ---
def run_bulk_harvest():
    print(f"🚀 Starting Audio-Visual Harvest...")
    
    try:
        df = load_current_db()
    except Exception as e:
        print(f"❌ Database Connection Error: {e}")
        return

    # Initialize with SEED if empty
    if df.empty or len(df) <= 1:
        print("🚨 Database empty. Initializing with SEED ARTISTS.")
        source_artists = SEED_ARTISTS
        existing_artists = set()
    else:
        source_artists = df['Artist'].tolist()
        existing_artists = set(df['Artist'].astype(str).str.strip().str.lower().tolist())
    
    print(f"📚 Loaded {len(existing_artists)} existing artists.")
    
    new_batch = []
    total_added = 0
    
    for seed_artist in source_artists:
        print(f"🔍 Scanning: {seed_artist}...")
        added_for_this_seed = 0
        page = 1
        
        while added_for_this_seed < 2 and page <= MAX_PAGES:
            candidates = get_neighbors(seed_artist, limit=SEARCH_LIMIT, page=page)
            if not candidates: break

            for candidate in candidates:
                if added_for_this_seed >= 2: break
                
                if candidate.strip().lower() in existing_artists: continue
                    
                # Check if already in current batch to avoid duplicate work
                if any(b['Artist'].lower() == candidate.strip().lower() for b in new_batch): continue

                print(f"   ✨ Found candidate: {candidate}")
                data = get_details_and_audio(candidate)
                time.sleep(SLEEP_TIME)
                
                if data:
                    if data['Artist'].strip().lower() in existing_artists: continue
                    new_batch.append(data)
                    existing_artists.add(data['Artist'].strip().lower())
                    added_for_this_seed += 1
                    total_added += 1
                    print(f"   ✅ QUEUED: {data['Artist']}")
            
            page += 1
            
        # Save batch
        if len(new_batch) >= BATCH_SIZE:
            append_to_sheet(pd.DataFrame(new_batch))
            new_batch = []
            
    if new_batch:
        append_to_sheet(pd.DataFrame(new_batch))
        
    print(f"🎉 Job Complete! Added {total_added} new artists.")

if __name__ == "__main__":
    run_bulk_harvest()