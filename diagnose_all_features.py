import requests
import time
import os
import tempfile
import numpy as np
import librosa
import pandas as pd
import urllib3
import sys
import warnings

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# --- CONFIGURATION ---
# Target artists to find the statistical ceiling and floor for ALL metrics
TEST_TARGETS = {
    "Dream Theater": "High Complexity/Prog",
    "Meshuggah": "High Density/Static",
    "The Ramones": "Low Complexity/Simple",
    "Brian Eno": "Low Energy/Ambient",
    "AC/DC": "High Energy/Simple",
    "Tycho": "High Warmth/Mid-BPM",
    "John Coltrane": "Jazz/High Harmonic Change",
    "Skrillex": "High Noise/Dubstep",
    "Norah Jones": "Low Brightness/Vocal Jazz"
}

def get_deezer_preview(artist_name):
    """Fetches Deezer Preview URL for a given artist."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        resp = requests.get(url, headers=headers, verify=False, timeout=5).json()
        if not resp.get('data'): return None
        artist_id = resp['data'][0]['id']
        
        track_url = f"https://api.deezer.com/artist/{artist_id}/top?limit=1"
        t_resp = requests.get(track_url, headers=headers, verify=False, timeout=5).json()
        if t_resp.get('data'):
            return t_resp['data'][0]['preview']
    except: return None
    return None

def get_raw_features(preview_url):
    """Downloads MP3 and extracts RAW, un-normalized feature values."""
    tmp_path = None
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        r = requests.get(preview_url, headers=headers, verify=False, timeout=10)
        if r.status_code != 200: return None

        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(r.content)
            tmp_path = tmp.name
        
        y, sr = librosa.load(tmp_path, duration=30, sr=22050, mono=True)
        
        # --- EXTRACT RAW METRICS ---
        
        # 1. BPM
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        tempo = librosa.beat.tempo(onset_envelope=onset_env, sr=sr)
        bpm = tempo[0] if isinstance(tempo, np.ndarray) else tempo
        
        # 2. Brightness (Centroid)
        # Measure: Average frequency (Hz)
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        
        # 3. Noisiness (ZCR)
        # Measure: Rate of zero-crossings (raw ratio)
        zcr = librosa.feature.zero_crossing_rate(y)
        
        # 4. Warmth (Rolloff)
        # Measure: Frequency (Hz) below which 85% of energy lies
        rolloff = librosa.feature.spectral_rolloff(y=y, sr=sr, roll_percent=0.85)[0]
        
        # 5. Complexity (Chroma Variance)
        # Measure: Standard Deviation of pitch classes
        chroma = librosa.feature.chroma_stft(y=y, sr=sr)
        
        return {
            "BPM_Raw": bpm,
            "Brightness_Raw": np.mean(spectral_centroids),
            "Noisiness_Raw": np.mean(zcr),
            "Warmth_Raw": np.mean(rolloff),
            "Complexity_Raw": np.mean(np.std(chroma, axis=1))
        }
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True) 
        return None
    finally:
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

def run_audit():
    print("\n--- 🔬 FEATURE AUDIT: RAW DATA GATHERING ---", flush=True)
    results = []
    
    for artist, desc in TEST_TARGETS.items():
        print(f"Fetching {artist} ({desc})...", end="", flush=True)
        url = get_deezer_preview(artist)
        
        if url:
            raw_data = get_raw_features(url)
            if raw_data:
                raw_data['Artist'] = artist
                raw_data['Type'] = desc
                results.append(raw_data)
                print(f" Success.", flush=True)
            else:
                print(" Analysis Failed.", flush=True)
        else:
            print(" No Preview Found.", flush=True)
        
        time.sleep(1)

    if results:
        df = pd.DataFrame(results)
        print("\n--- 📊 AUDIT RESULTS (Raw Values) ---", flush=True)
        # Print specific columns to see ranges
        print(df[['Artist', 'Brightness_Raw', 'Noisiness_Raw', 'Warmth_Raw', 'Complexity_Raw']].sort_values("Complexity_Raw"), flush=True)
        
        print("\n--- 💡 SUGGESTED DIVISORS (MAX + 10% Buffer) ---", flush=True)
        
        # Calculate Max Observed Value and determine a safe divisor
        # We add a 10% buffer so the loudest song sits at ~0.90, not 1.0
        for col in ['Brightness_Raw', 'Noisiness_Raw', 'Warmth_Raw', 'Complexity_Raw']:
            if col in df.columns:
                max_val = df[col].max()
                divisor = max_val * 1.10
                print(f"- {col.replace('_Raw', '')}: Max Observed: {max_val:.4f} -> DIVISOR: {divisor:.4f}", flush=True)

if __name__ == "__main__":
    try:
        run_audit()
    except Exception as e:
        print(f"\nFATAL CRASH: {e}", file=sys.stderr, flush=True)