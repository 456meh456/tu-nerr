import sys
import os
import requests
import time
import toml
import datetime
import pandas as pd

# --- Import Core Modules (Note: Imports must be updated to match the latest API Handler) ---
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

def construct_full_payload(artist_name):
    """Fetches ALL required data points for a full record update."""
    
    print(f"\n--- 1. Fetching Metadata for: {artist_name} ---")
    deezer_info = get_deezer_data(artist_name)
    if not deezer_info:
        print(f"❌ Aborted: Artist '{artist_name}' not found on Deezer.")
        return None
        
    clean_name = deezer_info['name']
    
    # --- FETCH RELEASE YEAR (The crucial part) ---
    release_year = 0
    if deezer_info.get('id'):
        # Call the dedicated release year function (which performs the discography scan)
        release_year = get_release_year(deezer_info['id'])

    print(f"   ✅ Found: {deezer_info['name']}")
    print(f"   📅 Detected Release Year: {release_year}")

    # --- FETCH LAST.FM TAGS ---
    lastfm_info = get_artist_details(deezer_info['name'], API_KEY)
    tags = [t['name'].lower() for t in lastfm_info['tags']['tag']] if lastfm_info else []

    # Scoring (Simplified for heal—we just want non-NULL values)
    VALENCE_SCORES = {'happy': 0.9, 'pop': 0.8, 'sad': 0.2, 'metal': 0.3}
    ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
    
    # FIX APPLIED HERE: Changed 'd' to 's_dict'
    def calc_score(t_list, s_dict):
        h = [v for k,v in s_dict.items() for t in t_list if k in t]
        return sum(h)/len(h) if h else 0.5
    
    # 2. Construct the final dictionary (Payload for add_artist)
    payload = {
        "Artist": clean_name,
        "Genre": tags[0].title() if tags else "Unknown",
        "Monthly Listeners": deezer_info['listeners'],
        "Image URL": deezer_info['image'],
        "Valence": calc_score(tags, VALENCE_SCORES),
        "Tag_Energy": calc_score(tags, ENERGY_SCORES),
        "First Release Year": release_year # <--- The data we are fixing
    }
    
    return payload

def run_manual_heal():
    """Main loop for user interaction."""
    
    print("\n--- 🏥 MANUAL RECORD HEALER ---")
    
    if not get_supabase_client():
        print("❌ CRITICAL: Failed to connect to Supabase. Check secrets.toml.")
        return

    while True:
        target = input("\nEnter band name to HEAL (or 'q' to quit): ")
        if target.lower() == 'q':
            break
        
        full_payload = construct_full_payload(target)
        
        if full_payload:
            print(f"2. Sending UPSERT to Supabase...")
            
            # This calls your updated add_artist which now handles Updates
            artist_id = add_artist(full_payload)
            
            if artist_id:
                print(f"✅ HEAL SUCCESS! Artist ID {artist_id} updated.")
                print("   Check your Supabase 'artists' table now.")
            else:
                print("❌ HEAL FAILED: Could not commit changes to database.")
        
if __name__ == "__main__":
    run_manual_heal()