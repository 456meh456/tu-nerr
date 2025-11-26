import sys
import os
import requests
import time
import toml
import pandas as pd

# Import necessary functions from db_model and api_handler
from src.db_model import add_artist, get_supabase_client
from src.api_handler import get_deezer_data, get_artist_details, get_release_year

# Load Secrets for API keys
SECRETS_PATH = os.path.join(os.getcwd(), ".streamlit", "secrets.toml")
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
except:
    print("❌ ERROR: Could not load API Key from secrets.toml.")
    sys.exit(1)

# --- HELPER FUNCTIONS (Copied from manual_heal.py logic) ---

def fetch_all_artists_for_healing():
    """Fetches all existing artists' names from Supabase."""
    supabase = get_supabase_client()
    if not supabase:
        print("❌ CRITICAL: Failed to connect to Supabase.")
        sys.exit(1)
        
    # We only need the name, as the IDs are auto-generated and handled by UPSERT.
    response = supabase.table("artists").select("name").execute()
    return [artist['name'] for artist in response.data]

def construct_full_payload(artist_name):
    """Fetches ALL required data points for a full record update."""
    
    deezer_info = get_deezer_data(artist_name)
    if not deezer_info: return None
        
    clean_name = deezer_info['name']
    
    # 1. FETCH RELEASE YEAR (The crucial part)
    release_year = 0
    if deezer_info.get('id'):
        release_year = get_release_year(deezer_info['id'])

    # 2. FETCH LAST.FM TAGS
    lastfm_info = get_artist_details(clean_name, API_KEY)
    if not lastfm_info: return None
    
    tags = [t['name'].lower() for t in lastfm_info['tags']['tag']]
    
    # Scoring (Simplified for heal)
    VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
    ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
    
    def score(d):
        h = [v for k,v in d.items() for t in tags if k in t]
        return sum(h)/len(h) if h else 0.5
    
    # 3. Construct the final dictionary (Payload for add_artist)
    payload = {
        "Artist": clean_name,
        "Genre": tags[0].title() if tags else "Unknown",
        "Monthly Listeners": deezer_info['listeners'],
        "Image URL": deezer_info['image'],
        "Valence": score(VALENCE_SCORES),
        "Tag_Energy": score(ENERGY_SCORES),
        "First Release Year": release_year # <--- The data we are fixing
    }
    
    return payload

# --- MAIN AUTOMATION LOOP ---

def run_automated_heal():
    print("\n--- 🏥 STARTING FULL DATABASE HEAL ---")
    
    artists_to_heal = fetch_all_artists_for_healing()
    total_artists = len(artists_to_heal)
    healed_count = 0
    
    print(f"🎯 Target: {total_artists} artists.")
    
    for i, artist_name in enumerate(artists_to_heal):
        print(f"   [{i + 1}/{total_artists}] Healing: {artist_name}...", end=" ", flush=True)

        full_payload = construct_full_payload(artist_name)
        
        if full_payload:
            # The Magic: Calling add_artist triggers the SQL UPSERT
            # This updates the existing row with the new, correct data
            try:
                add_artist(full_payload)
                print("✅ UPDATED.")
                healed_count += 1
            except Exception as e:
                print(f"❌ DB FAIL: {e}")
        else:
            print("⚠️ SKIPPED (API Fail).")
            
        # Be polite to APIs
        time.sleep(1.0) 

    print(f"\n🎉 HEAL COMPLETE. Total records updated: {healed_count}/{total_artists}")
    
if __name__ == "__main__":
    run_automated_heal()