import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
import toml
import os
import sys

# --- CONFIGURATION (Load Secrets) ---
SECRETS_PATH = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")

if not os.path.exists(SECRETS_PATH):
    print("FATAL ERROR: secrets.toml not found.")
    sys.exit(1)

try:
    secrets = toml.load(SECRETS_PATH)
    GCP_SECRETS = secrets["gcp_service_account"]
    LASTFM_KEY = secrets["lastfm_key"]
except KeyError as e:
    print(f"FATAL ERROR: Missing key in secrets.toml: {e}")
    sys.exit(1)

def run_tests():
    print("--- 🛠️ STARTING CONNECTION DIAGNOSTIC ---")
    
    # Define the permissions we need (This was missing!)
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    print("Test 1: Authenticating with Google Sheets...")
    
    try:
        # FIX 1: Handle Newlines
        private_key_fixed = GCP_SECRETS["private_key"].replace('\\n', '\n')
        
        creds_info = {
            **GCP_SECRETS,
            "private_key": private_key_fixed
        }
        
        # FIX 2: Pass the SCOPES here!
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        print("✅ Authentication Success: Service Account key is valid.")
        
        client = gspread.authorize(creds)
        print("✅ Authorization Success: gspread client established.")
        
        print("Test 2: Loading Data from 'tu-nerr-db'...")
        sheet = client.open("tu-nerr-db").sheet1
        print("✅ SHEET OPEN SUCCESS: Sheet 'tu-nerr-db' found.")
        
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        
        if df.empty:
            print("⚠️ DATA LOAD WARNING: Sheet is empty (only headers found).")
        else:
            print(f"✅ DATA LOAD SUCCESSFUL: Found {len(df)} rows.")

        print("Test 3: Last.fm API Key Check...")
        test_url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=Nirvana&api_key={LASTFM_KEY}&format=json"
        response = requests.get(test_url, timeout=5)
        
        if response.status_code == 200:
            print("✅ LASTFM API SUCCESS.")
        else:
            print(f"❌ LASTFM API FAILURE: {response.status_code}")

        print("--- 🏁 DIAGNOSTIC COMPLETE ---")

    except Exception as e:
        print(f"❌ CRASH: {e}")
        
if __name__ == "__main__":
    run_tests()