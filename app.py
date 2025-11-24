import streamlit as st
import pandas as pd
import requests
import gspread
import urllib3
import time
import io
import os
import tempfile
import numpy as np
import librosa
from google.oauth2.service_account import Credentials
from streamlit_agraph import agraph, Node, Edge, Config
import json
# ML Imports
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# Silence SSL Warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def get_sheet_connection():
    """Connects to Google Sheets using secrets, ensuring key is parsed correctly."""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    try:
        gcp_secrets = st.secrets["gcp_service_account"]
        
        # Handle the private key newlines safely
        private_key_string = gcp_secrets["private_key"]
        if "\\n" in private_key_string:
            fixed_private_key = private_key_string.replace('\\n', '\n')
        else:
            fixed_private_key = private_key_string
            
        # Rebuild dictionary to ensure clean parsing
        creds_info = {
            "type": gcp_secrets["type"],
            "project_id": gcp_secrets["project_id"],
            "private_key_id": gcp_secrets["private_key_id"],
            "private_key": fixed_private_key,
            "client_email": gcp_secrets["client_email"],
            "client_id": gcp_secrets["client_id"],
            "auth_uri": gcp_secrets["auth_uri"],
            "token_uri": gcp_secrets["token_uri"],
            "auth_provider_x509_cert_url": gcp_secrets["auth_provider_x509_cert_url"],
            "client_x509_cert_url": gcp_secrets["client_x509_cert_url"],
        }
        
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        client = gspread.authorize(creds)
        return client.open("tu-nerr-db").sheet1
    except Exception as e:
        st.error(f"🚨 FATAL CONNECTION FAILURE: {e}")
        st.stop()

# --- 2. DATA FUNCTIONS ---
def load_data():
    """Fetches and cleans data."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # Handle empty DB
    expected_cols = ['Artist', 'Genre', 'Monthly Listeners', 'Tag_Energy', 'Valence', 'Audio_BPM', 'Audio_Brightness', 'Image URL', 'Artist_Lower']
    if df.empty or 'Artist' not in df.columns:
        return pd.DataFrame(columns=expected_cols)
    
    # Fix Numbers
    cols_to_fix = ['Monthly Listeners', 'Tag_Energy', 'Valence', 'Audio_BPM', 'Audio_Brightness']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean Text
    df['Artist'] = df['Artist'].astype(str).str.strip()
    df = df[df['Artist'].str.len() > 0]
    df['Artist_Lower'] = df['Artist'].str.lower()
    df = df.drop_duplicates(subset=['Artist_Lower'], keep='first')
    
    return df

def save_artist(artist_data):
    """Appends a new artist."""
    sheet = get_sheet_connection()
    row = [
        artist_data['Artist'], 
        artist_data['Genre'], 
        artist_data['Monthly Listeners'],
        artist_data['Tag_Energy'], 
        artist_data['Valence'], 
        artist_data.get('Audio_BPM', 0), 
        artist_data.get('Audio_Brightness', 0),
        artist_data['Image URL']
    ]
    sheet.append_row(row)

def delete_artist(artist_name):
    """Finds an artist in the sheet and deletes the row."""
    sheet = get_sheet_connection()
    try:
        cell = sheet.find(artist_name, in_column=1)
        if cell:
            sheet.delete_rows(cell.row)
            return True
        return False
    except Exception as e:
        st.error(f"Delete Error: {e}")
        return False

# --- 3. AUDIO ENGINE (LIVE) ---
def analyze_audio(preview_url):
    """Downloads MP3 to temp file and extracts real physics."""
    tmp_path = None
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        if not preview_url: return 0, 0
        
        # 1. Download MP3
        response = requests.get(preview_url, headers=headers, verify=False, timeout=5)
        
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
        else:
            bpm = round(float(tempo))
            
        spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)[0]
        brightness = np.mean(spectral_centroids)
        norm_brightness = min(brightness / 3000, 1.0)
        
        return bpm, round(norm_brightness, 2)

    except Exception as e:
        return 0, 0 
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except: pass

# --- 4. AI ENGINE (KNN) ---
@st.cache_data(ttl=600)
def get_ai_neighbors(center_artist, df_db):
    """Finds mathematically similar artists using Audio Physics and Mood."""
    if len(df_db) < 5: return pd.DataFrame()
    
    # HYBRID METRIC: Use Audio Brightness if available, else Tag Energy
    df_calc = df_db.copy()
    df_calc['Calc_Energy'] = df_calc.apply(
        lambda x: x['Audio_Brightness'] if x.get('Audio_Brightness', 0) > 0 else x['Tag_Energy'], axis=1
    )
    
    # Prepare features: Brightness (Timbre), Mood (Valence), Tempo (BPM)
    # Note: We fill NaNs with safe defaults
    features = df_calc[['Calc_Energy', 'Valence', 'Audio_BPM']].fillna(0).values
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    knn = NearestNeighbors(n_neighbors=min(6, len(df_db)), metric='euclidean')
    knn.fit(features_scaled)
    
    center_idx = df_db[df_db['Artist'] == center_artist].index
    if center_idx.empty: return pd.DataFrame()
    
    distances, indices = knn.kneighbors([features_scaled[center_idx[0]]])
    neighbor_indices = indices[0][1:] 
    return df_db.iloc[neighbor_indices]

# --- 5. API FUNCTIONS ---
def get_similar_artists(artist_name, api_key, limit=10):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist_name}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'similarartists' in data: return [a['name'] for a in data['similarartists']['artist']]
    except: pass
    return []

def get_top_artists_by_genre(genre, api_key, limit=12):
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag={genre}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'topartists' in data: return [a['name'] for a in data['topartists']['artist']]
    except: pass
    return []

def get_artist_details(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={api_key}&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'error' not in data:
            return data['artist']
    except:
        pass
    return None

def get_top_tracks(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={artist_name}&api_key={api_key}&limit=5&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'error' not in data:
            return data['toptracks']['track']
    except:
        pass
    return []

def get_deezer_data(artist_name):
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        response = requests.get(url, verify=False, timeout=5)
        data = response.json()
        if data.get('data'):
            artist = data['data'][0]
            return {
                "name": artist['name'], "id": artist['id'], "listeners": artist['nb_fan'],
                "image": artist['picture_medium'], "link": artist['link']
            }
    except:
        pass
    return None

def get_deezer_preview(artist_id):
    try:
        url = f"https://api.deezer.com/artist/{artist_id}/top"
        response = requests.get(url, verify=False, timeout=5)
        data = response.json()
        if data.get('data') and len(data['data']) > 0:
            track = data['data'][0]
            return { "title": track['title'], "preview": track['preview'] }
    except:
        pass
    return None

def process_artist(name, df_db, api_key, session_added_set):
    # Check Local Session Cache first (Duplicate Prevention)
    if name.strip().lower() in session_added_set:
        return None

    # Check Database
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty: return match.iloc[0].to_dict()

    deezer_info = get_deezer_data(name)
    clean_name = deezer_info['name'] if deezer_info else name
    
    # Double check Name after Deezer cleanup
    if clean_name.strip().lower() in session_added_set:
        return None

    lastfm_info = get_artist_details(clean_name, api_key)

    if lastfm_info:
        img = deezer_info['image'] if deezer_info else "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg"
        listeners = deezer_info['listeners'] if deezer_info else int(lastfm_info['stats']['listeners'])
        tags = [tag['name'].lower() for tag in lastfm_info['tags']['tag']]
        
        # Tag Scoring (Fallback)
        ENERGY_SCORES = {'death': 1.0, 'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6, 'acoustic': 0.2}
        VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'pop': 0.8, 'sad': 0.2, 'dark': 0.15, 'doom': 0.1, 'metal': 0.3}

        def calc_score(t_list, s_dict):
            scores = [score for tag, score in s_dict.items() for t in t_list if tag in t]
            return sum(scores)/len(scores) if scores else 0.5

        tag_energy = calc_score(tags, ENERGY_SCORES)
        tag_valence = calc_score(tags, VALENCE_SCORES)
        main_genre = tags[0].title() if tags else "Unknown"

        # LIVE AUDIO ANALYSIS
        real_bpm = 0
        real_brightness = 0
        if deezer_info and deezer_info.get('preview'):
            real_bpm, real_brightness = analyze_audio(deezer_info['preview'])
        
        if real_bpm == 0: 
            real_brightness = tag_energy # Fallback if audio fails

        new_data = {
            "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": listeners,
            "Tag_Energy": tag_energy, "Valence": tag_valence, 
            "Audio_BPM": real_bpm, "Audio_Brightness": real_brightness,
            "Image URL": img
        }
        
        save_artist(new_data)
        session_added_set.add(clean_name.strip().lower()) # Update session cache
        return new_data
    
    return None

# --- 6. DISCOVERY LOGIC ---
def run_discovery(center, mode, api_key, df_db):
    targets = []
    with st.spinner(f"Scanning: {center}..."):
        if mode == "Artist":
            targets.append(center)
            targets.extend(get_similar_artists(center, api_key))
        else:
            targets = get_top_artists_by_genre(center, api_key)
    
    session_data = []
    prog = st.progress(0)
    
    # Create a local set of artists added THIS session to prevent duplicates
    if not df_db.empty:
        session_added_set = set(df_db['Artist_Lower'].tolist())
    else:
        session_added_set = set()
        
    for i, artist in enumerate(set(targets)):
        prog.progress((i + 1) / len(set(targets)))
        data = process_artist(artist, df_db, api_key, session_added_set)
        if data: session_data.append(data)
        if i % 3 == 0: df_db = load_data()
    
    if session_data:
        st.session_state.view_df = pd.DataFrame(session_data).drop_duplicates(subset=['Artist'])
        st.session_state.center_node = center if mode == "Artist" else None
        return True
    return False

# --- 7. INITIAL LOAD ---
try:
    df_db = load_data()
except Exception as e:
    st.error(f"🚨 Startup Error: {e}")
    st.stop()

# --- 8. SIDEBAR ---
with st.sidebar:
    st.header("🚀 Discovery Engine")
    if st.button("🔄 Refresh Data"): st.cache_data.clear(); st.rerun()
    
    with st.form(key='search'):
        mode = st.radio("Search By:", ["Artist", "Genre"])
        query = st.text_input(f"Enter {mode} Name:")
        if st.form_submit_button("Launch"):
            if query:
                try:
                    key = st.secrets["lastfm_key"]
                    if run_discovery(query, mode, key, df_db): st.rerun()
                    else: st.error("No data found.")
                except Exception as e: st.error(f"Search error: {e}")
    
    st.divider()
    if st.button("🔄 Reset Map"):
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.cache_data.clear()
        st.rerun()

    with st.expander("🔐 Admin"):
        pw = st.text_input("Password:", type="password")
        if pw and pw == st.secrets.get("admin_password", ""):
            artist_del = st.selectbox("Delete Artist", df_db['Artist'].sort_values().unique() if not df_db.empty else [])
            if st.button("Delete"):
                delete_artist(artist_del)
                st.cache_data.clear()
                st.rerun()

# --- 9. VISUALIZATION ---
if 'view_df' not in st.session_state or st.session_state.view_df.empty:
    if not df_db.empty:
        st.session_state.view_df = df_db.sample(min(len(df_db), 20))
        st.session_state.center_node = st.session_state.view_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
    else:
        st.session_state.view_df = pd.DataFrame()

disp_df = st.session_state.view_df
center = st.session_state.get('center_node', 'Unknown')
st.subheader(f"🔭 System: {center}")

selected = None
if not disp_df.empty:
    nodes, edges, added = [], [], set()
    real_center = next((r['Artist'] for i, r in disp_df.iterrows() if str(r['Artist']).lower() == str(center).lower()), None)

    for i, r in disp_df.iterrows():
        if r['Artist'] in added: continue
        size = 30
        if real_center and r['Artist'] == real_center: size = 80
        
        # VISUALIZE: Brightness (Energy) & Mood
        audio_b = float(r.get('Audio_Brightness', 0))
        tag_e = float(r.get('Tag_Energy', 0.5))
        energy = audio_b if audio_b > 0 else tag_e
        
        bpm = int(r.get('Audio_BPM', 0))
        
        border = "#E74C3C" if energy > 0.7 else "#2ECC71" if energy < 0.3 else "#F1C40F"

        nodes.append(Node(
            id=r['Artist'],
            label=r['Artist'],
            size=size,
            shape="circularImage",
            image=r['Image URL'],
            title=f"BPM: {bpm}\nBrightness: {energy:.2f}\nMood: {r['Valence']}", borderWidth=4, color={'border': border}))
        added.add(r['Artist'])

    if real_center:
        for i, r in disp_df.iterrows():
            if r['Artist'] != real_center:
                edges.append(Edge(source=real_center, target=r['Artist'], color="#888888"))

    config = Config(width="100%", height=600, directed=False, physics=True, hierarchical=False, collapsible=True)
    selected = agraph(nodes=nodes, edges=edges, config=config)

# --- 10. DASHBOARD ---
if selected and not selected.startswith("g_"):
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1: st.header(f"🤿 {selected}")
    with c2:
        if st.button("🔭 Travel Here", type="primary"):
            run_discovery(selected, "Artist", st.secrets["lastfm_key"], df_db)
            st.rerun()
        if st.button("🤖 AI Neighbors"):
            ai_recs = get_ai_neighbors(selected, df_db)
            if not ai_recs.empty:
                st.session_state.view_df = ai_recs
                st.session_state.center_node = selected
                st.success("AI Trajectory Calculated.")
                time.sleep(1)
                st.rerun()
            else: st.error("Not enough data for AI analysis.")

    try:
        row = df_db[df_db['Artist'] == selected]
        img = row.iloc[0]['Image URL'] if not row.empty else None
        
        d_live = get_deezer_data(selected)
        if not img or "placeholder" in str(img): 
            if d_live: img = d_live['image']
            
        preview = None
        if d_live and d_live.get('id'): preview = get_deezer_preview(d_live['id'])

        col1, col2 = st.columns([1, 2])
        with col1:
            if img and str(img).startswith("http"): st.image(img)
            if preview: 
                st.audio(preview['preview'])
                st.caption(f"🎵 {preview['title']}")
            
            if not row.empty:
                st.metric("Fans", f"{int(row.iloc[0]['Monthly Listeners']):,}")
                e, v = float(row.iloc[0]['Energy']), float(row.iloc[0]['Valence'])
                st.caption(f"🔥 Energy: {e:.2f}")
                st.progress(e)
                st.caption(f"😊 Mood: {v:.2f}")
                st.progress(v)

        with col2:
            with st.spinner("Fetching info..."):
                det = get_artist_details(selected, st.secrets["lastfm_key"])
                tracks = get_top_tracks(selected, st.secrets["lastfm_key"])
            
            if det and 'bio' in det: st.info(det['bio']['summary'].split("<a href")[0])
            if tracks:
                t_data = [{"Song": t['name'], "Plays": f"{int(t['playcount']):,}", "Link": t['url']} for t in tracks]
                st.dataframe(pd.DataFrame(t_data), column_config={"Link": st.column_config.LinkColumn("Link")}, hide_index=True)
                
    except Exception as e: st.error(f"Error: {e}")

else:
    if df_db.empty:
        st.info("The database is empty! Use the sidebar to start your first search.")