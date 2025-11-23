import streamlit as st
import pandas as pd
import requests
import gspread
import urllib3
import time
from google.oauth2.service_account import Credentials
from streamlit_agraph import agraph, Node, Edge, Config

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# Silence SSL Warnings (Necessary for local Deezer calls)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION & SEED LIST ---
SEED_ARTISTS = ["Metallica", "The Beatles", "Gorillaz", "Chris Stapleton", "Dolly Parton"]

# --- 1. GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def get_sheet_connection():
    """Connects to Google Sheets using secrets."""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scope
        )
        client = gspread.authorize(creds)
        return client.open("tu-nerr-db").sheet1
    except Exception as e:
        st.error(f"🚨 Connection Error: {e}")
        st.stop()

# --- 2. DATA FUNCTIONS ---
@st.cache_data
def load_data():
    """Fetches and cleans data."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if not df.empty and 'Artist' in df.columns:
        df['Artist'] = df['Artist'].astype(str).str.strip()
        df = df[df['Artist'].str.len() > 0]
        df['Artist_Lower'] = df['Artist'].str.lower()
        df = df.drop_duplicates(subset=['Artist_Lower'], keep='first')
        
        import numpy as np
        df['Log_Listeners'] = np.log10(df['Monthly Listeners'].replace(0, 1))
    else:
        df['Artist_Lower'] = []
    return df

def save_artist(artist_data):
    """Appends a new artist row to the Google Sheet."""
    sheet = get_sheet_connection()
    row = [
        artist_data['Artist'], artist_data['Genre'], artist_data['Monthly Listeners'],
        artist_data['Energy'], artist_data['Valence'], artist_data['Image URL']
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

# --- 3. API FUNCTIONS ---
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
        if 'error' not in data: return data['artist']
    except: pass
    return None

def get_top_tracks(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={artist_name}&api_key={api_key}&limit=5&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'error' not in data: return data['toptracks']['track']
    except: pass
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
    except: pass
    return None

def get_deezer_preview(artist_id):
    try:
        url = f"https://api.deezer.com/artist/{artist_id}/top"
        response = requests.get(url, verify=False, timeout=5)
        data = response.json()
        if data.get('data') and len(data['data']) > 0:
            track = data['data'][0]
            return { "title": track['title'], "preview": track['preview'] }
    except: pass
    return None

def process_artist(name, df_db, api_key):
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty: return match.iloc[0].to_dict()

    deezer_info = get_deezer_data(name)
    clean_name = deezer_info['name'] if deezer_info else name
    lastfm_info = get_artist_details(clean_name, api_key)

    if lastfm_info:
        if deezer_info:
            final_listeners = deezer_info['listeners']
            final_image = deezer_info['image']
        else:
            final_listeners = int(lastfm_info['stats']['listeners'])
            final_image = "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg"

        tags = [tag['name'].lower() for tag in lastfm_info['tags']['tag']]
        
        # --- IMPROVED SCORING LOGIC (Valence Fix) ---
        ENERGY_SCORES = {'death': 1.0, 'thrash': 0.95, 'core': 0.95, 'metal': 0.9, 'punk': 0.9, 'heavy': 0.9,
                         'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5,
                         'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35, 'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15}
        
        VALENCE_SCORES = {
            'happy': 0.9, 'party': 0.85, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8,
            'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5,
            'progressive': 0.5,
            'alternative': 0.4, 
            'rock': 0.45,
            'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05,
            'doom': 0.1, 'gothic': 0.2, 
            'industrial': 0.3, 'angry': 0.3, 
            'metal': 0.3, 
            'heavy': 0.3, 
            'thrash': 0.2,
            'death': 0.1
        }

        def calculate_score(tag_list, score_dict):
            found_scores = []
            for tag in tag_list:
                for genre, score in score_dict.items():
                    if genre in tag:
                        found_scores.append(score)
            if not found_scores: return 0.5
            return sum(found_scores) / len(found_scores)

        energy = calculate_score(tags, ENERGY_SCORES)
        valence = calculate_score(tags, VALENCE_SCORES)
        main_genre = tags[0].title() if tags else "Unknown"

        new_data = {
            "Artist": clean_name, "Genre": main_genre, "Monthly Listeners": final_listeners,
            "Energy": energy, "Valence": valence, "Image URL": final_image
        }
        
        save_artist(new_data)
        return new_data
    
    return None

# --- 4. DISCOVERY ENGINE LOGIC ---
def run_discovery_sequence(center_entity, mode, api_key, df_db):
    target_list = []
    
    with st.spinner(f"Scanning the cosmos for {center_entity}..."):
        if mode == "Artist":
            target_list.append(center_entity)
            similar = get_similar_artists(center_entity, api_key, limit=10)
            target_list.extend(similar)
        else:
            target_list = get_top_artists_by_genre(center_entity, api_key, limit=12)
    
    target_list = list(set(target_list))
    current_session_data = []
    progress_bar = st.progress(0)
    
    for i, artist in enumerate(target_list):
        progress_bar.progress((i + 1) / len(target_list))
        data = process_artist(artist, df_db, api_key)
        if data:
            current_session_data.append(data)
        if i % 3 == 0: df_db = load_data() 

    if current_session_data:
        session_df = pd.DataFrame(current_session_data).drop_duplicates(subset=['Artist'])
        st.session_state.view_df = session_df
        st.session_state.center_node = center_entity if mode == "Artist" else None
        return True
    return False

# --- 5. LOAD DATA INITIAL ---
try:
    df_db = load_data()
except Exception as e:
    st.error("Could not load data. Check secrets.")
    st.stop()

# --- 6. SIDEBAR ---
with st.sidebar:
    st.header("🚀 Discovery Engine")
    
    with st.form(key='search_form'):
        search_mode = st.radio("Search By:", ["Artist", "Genre"])
        search_query = st.text_input(f"Enter {search_mode} Name:")
        submit_button = st.form_submit_button(label='Launch')
    
    if submit_button:
        if search_query:
            try:
                api_key = st.secrets["lastfm_key"]
                success = run_discovery_sequence(search_query, search_mode, api_key, df_db)
                if success:
                    st.rerun()
                else:
                    st.error("No data found.")
            except Exception as e:
                st.error(f"Search Error: {e}")
    
    st.divider()
    if st.button("🔄 Reset / Show Global Galaxy"):
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.cache_data.clear()
        st.rerun()

    if st.button("🎲 Random Jump"):
        if not df_db.empty:
            random_artist = df_db.sample(1).iloc[0]['Artist']
            st.session_state.center_node = random_artist
            
            try:
                api_key = st.secrets["lastfm_key"]
                run_discovery_sequence(random_artist, "Artist", api_key, df_db)
                st.rerun()
            except Exception as e:
                 st.error(f"Jump failed: {e}")

    # --- SECURE ADMIN ZONE ---
    with st.expander("🔐 Admin Zone"):
        st.write("Enter password to enable deletion.")
        admin_pass = st.text_input("Password:", type="password")
        
        is_authenticated = False
        if admin_pass:
            try:
                # Robust check against stripped secret
                input_clean = str(admin_pass).strip()
                secret_clean = str(st.secrets["admin_password"]).strip()
                if input_clean == secret_clean:
                    is_authenticated = True
                else:
                    st.error("Incorrect password.")
            except KeyError:
                st.error("Admin password not set in secrets.toml.")

        if is_authenticated:
            st.success("Unlocked!")
            if not df_db.empty:
                artist_to_delete = st.selectbox("Select Artist to Remove", options=df_db['Artist'].sort_values().unique(), key="del_sel")
                
                if st.button(f"🗑️ Delete {artist_to_delete}", type="primary"):
                    with st.spinner("Deleting from Google Sheets..."):
                        if delete_artist(artist_to_delete):
                            st.success(f"Deleted {artist_to_delete}!")
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Could not find artist in sheet.")

# --- 7. THE VISUALIZATION ---
# This is the Unified Graph View (Solar System or Global Cluster)

if 'view_df' in st.session_state and not st.session_state.view_df.empty:
    display_df = st.session_state.view_df
    view_title = f"🔭 System: {st.session_state.center_node or st.session_state.get('search_query', 'Unknown')}"
    is_global = False
else:
    display_df = df_db
    view_title = "🌍 The Universal Galaxy"
    is_global = True

st.subheader(view_title)

selected_artist = None

if not display_df.empty:
    nodes = []
    edges = []
    added_node_ids = set() 

    real_center = None
    center_node = st.session_state.get('center_node', None)
    if center_node:
        real_center = next((row['Artist'] for i, row in display_df.iterrows() if row['Artist'].lower() == str(center_node).lower()), None)

    for index, row in display_df.iterrows():
        if row['Artist'] in added_node_ids: continue
        
        size = 25
        if row['Monthly Listeners'] > 1000000: size = 40
        if row['Monthly Listeners'] > 10000000: size = 60
        if real_center and row['Artist'] == real_center: size = 80
        
        # Border Color = Energy (Heat)
        energy = float(row['Energy'])
        border_color = "#333333" # Default Grey
        if energy > 0.75: border_color = "#ff4b4b" # Hot Red (High Intensity)
        elif energy < 0.4: border_color = "#4b9eff" # Cool Blue (Low Intensity)

        nodes.append(Node(
            id=row['Artist'],
            label=row['Artist'],
            size=size,
            shape="circularImage",
            image=row['Image URL'],
            # NEW TOOLTIP: Includes Energy/Valence for inspection
            title=f"Genre: {row['Genre']}\nEnergy: {energy:.2f}\nMood: {row['Valence']:.2f}",
            borderWidth=4, 
            color={'border': border_color, 'background': '#1f273e'}
        ))
        added_node_ids.add(row['Artist'])

    if is_global:
        # GLOBAL VIEW: Cluster by Genre
        unique_genres = display_df['Genre'].unique()
        for genre in unique_genres:
            if f"g_{genre}" not in added_node_ids:
                 # Genre node is a star
                 nodes.append(Node(id=f"g_{genre}", label=genre, size=15, color="#f1c40f", shape="star"))
                 added_node_ids.add(f"g_{genre}")

        for index, row in display_df.iterrows():
            if f"g_{row['Genre']}" in added_node_ids:
                 edges.append(Edge(source=row['Artist'], target=f"g_{row['Genre']}", color="#888888", width=0.5))
            
    else:
        # SEARCH VIEW: Connect to Center
        if real_center:
            for index, row in display_df.iterrows():
                if row['Artist'] != real_center:
                    edges.append(Edge(source=real_center, target=row['Artist'], color="#888888"))

    config = Config(width="100%", height=600, directed=False, physics=True, hierarchical=False, nodeHighlightBehavior=True, highlightColor="#F7A7A6", collapsible=True)
    selected_artist = agraph(nodes=nodes, edges=edges, config=config)

    # --- 8. THE DASHBOARD ---
    if selected_artist and not selected_artist.startswith("g_"):
        st.divider()
        col_title, col_btn = st.columns([3, 1])
        with col_title:
            st.header(f"🤿 Deep Dive: {selected_artist}")
        with col_btn:
            if st.button(f"🔭 Center Map on {selected_artist}", type="primary"):
                try:
                    api_key = st.secrets["lastfm_key"]
                    run_discovery_sequence(selected_artist, "Artist", api_key, df_db)
                    st.rerun()
                except Exception as e:
                    st.error(f"Warp failed: {e}")

        try:
            api_key = st.secrets["lastfm_key"]
            col1, col2 = st.columns([1, 2])
            
            row = display_df[display_df['Artist'] == selected_artist]
            image_url = None
            if not row.empty: image_url = row.iloc[0]['Image URL']
            
            deezer_live = get_deezer_data(selected_artist)
            if not image_url or "placeholder" in str(image_url):
                 if deezer_live: image_url = deezer_live['image']
            
            audio_preview = None
            if deezer_live and deezer_live.get('id'):
                 audio_preview = get_deezer_preview(deezer_live['id'])

            with col1:
                if image_url and str(image_url).startswith("http"):
                    st.image(image_url, use_column_width=True)
                
                if audio_preview:
                    st.audio(audio_preview['preview'])
                    st.caption(f"🎵 {audio_preview['title']}")
                
                if not row.empty:
                    st.metric("Fans", f"{int(row.iloc[0]['Monthly Listeners']):,}")
                    st.write(f"**Genre:** {row.iloc[0]['Genre']}")
                    
                    st.divider()
                    energy_val = float(row.iloc[0]['Energy'])
                    valence_val = float(row.iloc[0]['Valence'])

                    st.caption(f"🔥 Intensity (Energy): {energy_val:.2f}")
                    st.progress(energy_val)
                    st.caption(f"😊 Mood (Valence): {valence_val:.2f}")
                    st.progress(valence_val)

            with col2:
                with st.spinner("Fetching tracks..."):
                    details = get_artist_details(selected_artist, api_key)
                    tracks = get_top_tracks(selected_artist, api_key)

                if details and 'bio' in details:
                    st.info(details['bio']['summary'].split("<a href")[0])
                
                if tracks:
                    track_data = [{"Song": t['name'], "Play": t['url']} for t in tracks]
                    st.dataframe(pd.DataFrame(track_data), column_config={"Play": st.column_config.LinkColumn("Link")}, hide_index=True, use_container_width=True)

        except Exception as e:
            st.error(f"Data Error: {e}")

else:
    st.info("The database is empty! Use the sidebar to start your first search.")