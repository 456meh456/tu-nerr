import requests
import time
import os
import tempfile
import numpy as np
import librosa
import pandas as pd
import urllib3
import sys # Added for exception printing

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
# We compare "Simple" vs "Complex" artists to see the raw spread
TEST_TARGETS = {
    "The Ramones": "Simple (Punk)",
    "AC/DC": "Simple (Rock)",
    "Brian Eno": "Simple (Ambient)",
    "Dream Theater": "Complex (Prog)",
    "Snarky Puppy": "Complex (Jazz Fusion)",
    "Meshuggah": "Complex (Tech Metal)"
}

def get_deezer_preview(artist_name):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        # 1. Search
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
        if not resp.get('data'): return None
        artist_id = resp['data'][0]['id']
        
        # 2. Get Top Track
        track_url = f"https://api.deezer.com/artist/{artist_id}/top?limit=1"
        t_resp = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
        if t_resp.get('data'):
            return t_resp['data'][0]['preview']
    except: return None
    return None

def get_raw_complexity(preview_url):
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        r = requests.get(preview_url, headers=headers, verify=False, timeout=10)
        if r.status_code != 200: return None

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name
        
        # Librosa load must be robust against temporary errors
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        # --- THE CRITICAL METRIC ---
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        
        # Calculate Raw Variance (The number we are currently multiplying by 5)
        raw_complexity = np.mean(np.std(chroma, axis=1))
        
        return raw_complexity
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True) # Send errors to stderr
        return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

def run_audit():
    print("\n--- 🔬 COMPLEXITY AUDIT (RAW VALUES) ---", flush=True)
    results = []
    
    for artist, desc in TEST_TARGETS.items():
        # FIX: Added flush=True to guarantee immediate console output
        print(f"Fetching {artist} ({desc})...", end="", flush=True)
        url = get_deezer_preview(artist)
        
        if url:
            raw = get_raw_complexity(url)
            if raw:
                print(f" Raw Score: {raw:.4f}", flush=True)
                results.append({"Artist": artist, "Type": desc, "Raw_Complexity": raw})
            else:
                print(" Analysis Failed.", flush=True)
        else:
            print(" No Preview Found.", flush=True)
        
        time.sleep(1)

    if results:
        df = pd.DataFrame(results)
        print("\n--- 📊 RESULTS SUMMARY ---", flush=True)
        print(df.sort_values("Raw_Complexity"), flush=True)
        
        min_c = df['Raw_Complexity'].min()
        max_c = df['Raw_Complexity'].max()
        
        print(f"\nLowest: {min_c:.4f}", flush=True)
        print(f"Highest: {max_c:.4f}", flush=True)
        
        # Calculate ideal multiplier to map this range to 0.0 - 1.0
        # Ideal Multiplier = 1.0 / Max_Observed
        suggested_mult = 1.0 / max_c
        print(f"\n💡 Suggested Multiplier: * {suggested_mult:.2f}", flush=True)

if __name__ == "__main__":
    try:
        run_audit()
    except Exception as e:
        print(f"\nFATAL CRASH: {e}", file=sys.stderr, flush=True)