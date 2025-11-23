import pandas as pd
import requests
import gspread
import time
import toml
import urllib3
from google.oauth2.service_account import Credentials

# --- CONFIGURATION ---
BATCH_SIZE = 10   
SLEEP_TIME = 0.2  
SEARCH_LIMIT = 50 
MAX_PAGES = 3     
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# --- 0. SILENCE SSL WARNINGS ---
# This suppresses the "InsecureRequestWarning" caused by verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. SETUP AUTH ---
secrets = toml.load(".streamlit/secrets.toml")
API_KEY = secrets["lastfm_key"]
GCP_SECRETS = secrets["gcp_service_account"]

def get_sheet_connection():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(GCP_SECRETS, scopes=scope)
    client = gspread.authorize(creds)
    # Ensure client.open works (we assume 'tu-nerr-db' is correct)
    return client.open("tu-nerr-db").sheet1

def load_current_db():
    sheet = get_sheet_connection()
    # Read headers=1 to ensure the first row is used as column names.
    data = sheet.get_all_records()
    return pd.DataFrame(data)

def append_to_sheet(new_rows_df):
    sheet = get_sheet_connection()
    values = new_rows_df.values.tolist()
    sheet.append_rows(values)
    print(f"💾 Batch Saved: {len(values)} artists added to Cloud.")

# --- 2. API HELPERS ---
def get_neighbors(artist, limit=50, page=1):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist}&api_key={API_KEY}&limit={limit}&page={page}&format=json"
    try:
        resp = requests.get(url, verify=False, timeout=5).json()
        if 'similarartists' not in resp:
            return []
        return [a['name'] for a in resp['similarartists']['artist']]
    except Exception as e:
        print(f"   ⚠️ Network Error fetching neighbors: {e}")
        return []

def get_details(artist):
    d_data = None
    try:
        d_url = f"https://api.deezer.com/search/artist?q={artist}"
        d_resp = requests.get(d_url, verify=False, timeout=5).json()
        if d_resp.get('data'): 
            d_data = d_resp['data'][0]
    except:
        pass

    if not d_data: return None

    try:
        l_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={d_data['name']}&api_key={API_KEY}&format=json"
        l_resp = requests.get(l_url, verify=False, timeout=5).json()
        if 'error' in l_resp: return None
        tags = [t['name'].lower() for t in l_resp['artist']['tags']['tag']]
    except: return None

    # Scoring (Using same logic as app.py)
    ENERGY_SCORES = {'death': 1.0, 'thrash': 0.95, 'core': 0.95, 'metal': 0.9, 'punk': 0.9, 'heavy': 0.9, 'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5, 'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35, 'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15}
    VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8, 'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5, 'progressive': 0.5, 'alternative': 0.4, 'rock': 0.45, 'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05, 'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.3, 'metal': 0.3, 'heavy': 0.3, 'thrash': 0.2, 'death': 0.1}

    def calc_score(t_list, s_dict):
        scores = [score for tag, score in s_dict.items() for t in t_list if tag in t]
        return sum(scores)/len(scores) if scores else 0.5

    return {
        "Artist": d_data['name'], 
        "Genre": tags[0].title() if tags else "Unknown",
        "Monthly Listeners": d_data['nb_fan'],
        "Energy": calc_score(tags, ENERGY_SCORES),
        "Valence": calc_score(tags, VALENCE_SCORES),
        "Image URL": d_data['picture_medium']
    }

# --- 3. MAIN EXECUTION ---
def run_bulk_harvest():
    print(f"🚀 Starting Deep Drill Harvest...")
    
    df = load_current_db()
    
    # --- CRITICAL FIX: Determine source list safely ---
    if df.empty or 'Artist' not in df.columns or df['Artist'].empty:
        print("🚨 Database is empty or malformed. Initializing with SEED ARTISTS.")
        source_artists = SEED_ARTISTS
        existing_artists = set()
    else:
        source_artists = df['Artist'].tolist()
        # This will now work because df is guaranteed to have the 'Artist' column
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
            
            if not candidates:
                print(f"   ⚠️ No neighbors found on page {page}. Stopping scan for {seed_artist}.")
                break

            for candidate in candidates:
                if added_for_this_seed >= 2: break
                    
                if candidate.strip().lower() in existing_artists: continue
                    
                print(f"   ✨ Processing candidate: {candidate}...")
                data = get_details(candidate)
                time.sleep(SLEEP_TIME)
                
                if data:
                    if data['Artist'].strip().lower() in existing_artists:
                        print(f"      [Skip] {data['Artist']} (Resolved to Duplicate)")
                        continue
                        
                    new_batch.append(data)
                    existing_artists.add(data['Artist'].strip().lower())
                    added_for_this_seed += 1
                    total_added += 1
                    print(f"   ✅ ADDED: {data['Artist']}")
            
            page += 1
            
        if len(new_batch) >= BATCH_SIZE:
            append_to_sheet(pd.DataFrame(new_batch))
            new_batch = []
            
    if new_batch:
        append_to_sheet(pd.DataFrame(new_batch))
        
    print(f"🎉 Job Complete! Added {total_added} new artists.")

if __name__ == "__main__":
    run_bulk_harvest()