import sys
from src.api_handler import get_deezer_data, get_artist_details
from src.db_model import add_artist, get_supabase_client
import toml
import os

# Load Secrets for API calls
SECRETS_PATH = ".streamlit/secrets.toml"
try:
    secrets = toml.load(SECRETS_PATH)
    API_KEY = secrets["lastfm_key"]
except:
    print("❌ Error loading secrets.")
    sys.exit(1)

def fix_specific_artist(artist_name):
    print(f"\n--- 🔧 REPAIRING: {artist_name} ---")
    
    # 1. Get Real Data (including Year)
    print("1. Fetching fresh data from Deezer...")
    deezer_info = get_deezer_data(artist_name)
    
    if not deezer_info:
        print("❌ Artist not found on Deezer.")
        return

    print(f"   ✅ Found: {deezer_info['name']}")
    print(f"   📅 Detected Release Year: {deezer_info.get('year', 'Unknown')}")

    # 2. Get Last.fm Data (Need this to complete the record for the update)
    print("2. Fetching tags from Last.fm...")
    lastfm_info = get_artist_details(deezer_info['name'], API_KEY)
    tags = []
    if lastfm_info:
        tags = [t['name'].lower() for t in lastfm_info['tags']['tag']]
    
    # Scoring (Simplified for repair - we just want to save the year)
    # Note: We reuse existing scores if we wanted to be perfect, but recalculating is safer
    VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'pop': 0.8, 'sad': 0.2, 'dark': 0.15, 'metal': 0.3}
    ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6}
    
    def calc_score(t_list, s_dict):
        scores = [score for tag, score in s_dict.items() for t in t_list if tag in t]
        return sum(scores)/len(scores) if scores else 0.5

    # 3. Construct Payload
    artist_data = {
        "Artist": deezer_info['name'],
        "Genre": tags[0].title() if tags else "Unknown",
        "Monthly Listeners": deezer_info['listeners'],
        "Image URL": deezer_info['image'],
        "First Release Year": deezer_info.get('year'), # THE FIX
        "Valence": calc_score(tags, VALENCE_SCORES),
        "Tag_Energy": calc_score(tags, ENERGY_SCORES)
    }

    # 4. Force Update
    print("3. Sending UPSERT to Supabase...")
    try:
        # This calls your updated add_artist which now handles Updates
        artist_id = add_artist(artist_data)
        print(f"✅ SUCCESS! Artist ID {artist_id} updated with Year {artist_data['First Release Year']}")
    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    while True:
        target = input("\nEnter band to fix (or 'q' to quit): ")
        if target.lower() == 'q':
            break
        fix_specific_artist(target)