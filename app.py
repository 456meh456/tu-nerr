import streamlit as st
import pandas as pd
import requests
import gspread
import urllib3
import time
from google.oauth2.service_account import Credentials
from streamlit_agraph import agraph, Node, Edge, Config
import random

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# Silence SSL Warnings (Necessary for local Deezer API access)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def get_sheet_connection():
    """Connects to Google Sheets using secrets."""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    try:
        # Note: The secret must be correctly pasted into the Cloud dashboard for this to work.
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=scope
        )
        client = gspread.authorize(creds)
        return client.open("tu-nerr-db").sheet1
    except Exception as e:
        # Using a minimal error message here to ensure app loads quickly when troubleshooting.
        st.error(f"🚨 Connection Error: Could not connect to Google Sheets. Error: {e}")
        st.stop()

# --- 2. DATA FUNCTIONS ---
def load_data():
    """Fetches and cleans data."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # CRITICAL FIX 1: Handle empty database gracefully
    if df.empty or 'Artist' not in df.columns:
        return pd.DataFrame(columns=['Artist', 'Genre', 'Monthly Listeners', 'Energy', 'Valence', 'Image URL', 'Artist_Lower'])
    
    # CRITICAL FIX 2: Clean data types and text
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
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
        artist_data['Energy'],
        artist_data['Valence'],
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

# --- 3. API FUNCTIONS ---
# Standard API functions for LastFM and Deezer (left out for brevity, but assumed correct)

def get_similar_artists(artist_name, api_key, limit=10):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist_name}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'similarartists' in data:
            return [a['name'] for a in data['similarartists']['artist']]
    except:
        pass
    return []

def get_top_artists_by_genre(genre, api_key, limit=12):
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag={genre}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if 'topartists' in data:
            return [a['name'] for a in data['topartists']['artist']]
    except:
        pass
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
                "name": artist['name'],
                "id": artist['id'],
                "listeners": artist['nb_fan'],
                "image": artist['picture_medium'],
                "link": artist['link']
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
            return {
                "title": track['title'],
                "preview": track['preview']
            }
    except:
        pass
    return None

def process_artist(name, df_db, api_key):
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty:
            return match.iloc[0].to_dict()

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
        
        # VALENCE FIX: Expanded scoring dictionaries
        ENERGY_SCORES = {'death': 1.0, 'thrash': 0.95, 'core': 0.95, 'metal': 0.9, 'punk': 0.9, 'heavy': 0.9, 'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5, 'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35, 'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15}
        VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8, 'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5, 'progressive': 0.5, 'alternative': 0.4, 'rock': 0.45, 'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05, 'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.3, 'metal': 0.3, 'heavy': 0.3, 'thrash': 0.2, 'death': 0.1}

        def calculate_score(tag_list, score_dict):
            # FIXED: Explicitly defined score_dict is now used correctly
            scores = [score for tag, score in score_dict.items() for t in tag_list if tag in t]
            return sum(scores)/len(scores) if scores else 0.5

        energy = calculate_score(tags, ENERGY_SCORES)
        valence = calculate_score(tags, VALENCE_SCORES)
        main_genre = tags[0].title() if tags else "Unknown"

        new_data = {
            "Artist": clean_name,
            "Genre": main_genre,
            "Monthly Listeners": final_listeners,
            "Energy": energy,
            "Valence": valence,
            "Image URL": final_image
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
        else:
            st.warning("Database empty. Start your first search.")

    # --- SECURE ADMIN ZONE ---
    with st.expander("🔐 Admin Zone"):
        st.write("Enter password to enable deletion.")
        admin_pass = st.text_input("Password:", type="password")
        
        is_authenticated = False
        if admin_pass:
            try:
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
                
                if st.button(f"Delete {artist_to_delete}", type="primary"):
                    with st.spinner("Deleting..."):
                        if delete_artist(artist_to_delete):
                            st.success(f"Deleted {artist_to_delete}!")
                            time.sleep(1)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Could not find artist in sheet.")

# --- 7. THE SOLAR SYSTEM (GRAPH VIEW ONLY) ---

# View State Management
if 'view_df' not in st.session_state or st.session_state.view_df.empty:
    # Default State: Show a random sample to avoid empty screen
    if not df_db.empty:
        sample_size = min(len(df_db), 20)
        st.session_state.view_df = df_db.sample(n=sample_size)
        st.session_state.center_node = st.session_state.view_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
    else:
        st.session_state.view_df = pd.DataFrame()
        st.session_state.center_node = None

display_df = st.session_state.view_df
center_node = st.session_state.get('center_node', 'Unknown')

st.subheader(f"🔭 System: {center_node}")

selected_artist = None

if not display_df.empty:
    nodes = []
    edges = []
    added_node_ids = set() 

    real_center = None
    if center_node:
        real_center = next((row['Artist'] for i, row in display_df.iterrows() if row['Artist'].lower() == str(center_node).lower()), None)

    for index, row in display_df.iterrows():
        if row['Artist'] in added_node_ids: continue
        
        # Visuals: Size based on popularity
        size = 25
        if row['Monthly Listeners'] > 1000000: size = 40
        if row['Monthly Listeners'] > 10000000: size = 60
        
        if real_center and row['Artist'] == real_center: 
            size = 80 # Center Node is huge
        
        # Color the border based on energy
        energy_val = float(row.get('Energy', 0.5))
        valence_val = float(row.get('Valence', 0.5))
        
        # Simple color mapping: High Energy = Red/Orange, Low Energy = Blue/Green
        if energy_val > 0.8:
            border_color = "#E74C3C" # Red/High Energy
        elif energy_val < 0.4:
            border_color = "#2ECC71" # Green/Low Energy
        else:
            border_color = "#F1C40F" # Yellow/Neutral/Rock

        nodes.append(Node(
            id=row['Artist'],
            label=row['Artist'],
            size=size,
            shape="circularImage",
            image=row['Image URL'],
            title=f"{row['Genre']} | {int(row['Monthly Listeners']):,} Fans\nE:{energy_val:.2f} | V:{valence_val:.2f}",
            borderWidth=5,
            color={"border": border_color}
        ))
        added_node_ids.add(row['Artist'])
        
        # Connect neighbors to the center
        if real_center and row['Artist'] != real_center:
            edges.append(Edge(source=real_center, target=row['Artist'], color="#555555"))

    config = Config(width="100%", height=600, directed=False, physics=True, hierarchical=False, nodeHighlightBehavior=True, highlightColor="#F7A7A6", collapsible=True)
    
    selected_artist = agraph(nodes=nodes, edges=edges, config=config)

    # --- 8. THE DASHBOARD ---
    if selected_artist:
        st.divider()
        
        col_title, col_btn = st.columns([3, 1])
        with col_title:
            st.header(f"🤿 Deep Dive: {selected_artist}")
        with col_btn:
            if st.button(f"🔭 Travel to {selected_artist}", type="primary"):
                try:
                    api_key = st.secrets["lastfm_key"]
                    run_discovery_sequence(selected_artist, "Artist", api_key, df_db)
                except Exception as e:
                    st.error(f"Warp failed: {e}")

        try:
            api_key = st.secrets["lastfm_key"]
            col1, col2 = st.columns([1, 2])
            row = df_db[df_db['Artist'] == selected_artist]
            
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
                    st.image(image_url)
                
                if audio_preview:
                    st.audio(audio_preview['preview'])
                    st.caption(f"🎵 {audio_preview['title']}")
                
                if not row.empty:
                    st.metric("Fans", f"{int(row.iloc[0]['Monthly Listeners']):,}")
                    
                    energy_val = float(row.iloc[0]['Energy'])
                    valence_val = float(row.iloc[0]['Valence'])
                    
                    st.write(f"**Vibe Score:** {row.iloc[0]['Genre']}")
                    st.caption(f"Energy (Intensity): {energy_val:.2f}")
                    st.progress(energy_val)
                    st.caption(f"Mood (Happiness): {valence_val:.2f}")
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
            st.error(f"Could not load details. {e}")

else:
    st.info("The database is empty! Use the sidebar to start your first search.")