import streamlit as st
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from streamlit_agraph import agraph, Node, Edge, Config
import plotly.express as px 

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# --- 1. GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def get_sheet_connection():
    """Connects to Google Sheets using secrets."""
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
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
def load_data():
    """Fetches all data from the Google Sheet."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    if not df.empty:
        df['Artist_Lower'] = df['Artist'].str.strip().str.lower()
        # Create a Log scale for Z-Axis
        import numpy as np
        df['Log_Listeners'] = np.log10(df['Monthly Listeners'].replace(0, 1))
    else:
        df['Artist_Lower'] = []
    return df

def save_artist(artist_data):
    """Appends a new artist row to the Google Sheet."""
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

# --- 3. API FUNCTIONS ---
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
                "listeners": artist['nb_fan'],
                "image": artist['picture_medium'],
                "link": artist['link']
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
        
        ENERGY_SCORES = {'death metal': 1.0, 'thrash': 0.95, 'metalcore': 0.9, 'punk': 0.9, 'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5, 'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35, 'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15}
        VALENCE_SCORES = {'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8, 'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5, 'progressive': 0.45, 'alternative': 0.4, 'rock': 0.5, 'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05, 'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.2}

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

# --- 4. REUSABLE DISCOVERY ENGINE (RESTORED!) ---
def run_discovery_sequence(center_entity, mode, api_key, df_db):
    """Central logic for finding and mapping clusters."""
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
        # Refresh DB copy occasionally
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

# --- 6. SIDEBAR: THE EXPLORER ---
with st.sidebar:
    st.header("🚀 Discovery Engine")
    
    with st.form(key='search_form'):
        search_mode = st.radio("Search By:", ["Artist", "Genre"])
        search_query = st.text_input(f"Enter {search_mode} Name:")
        submit_button = st.form_submit_button(label='Launch Discovery')
    
    if submit_button:
        if search_query:
            try:
                api_key = st.secrets["lastfm_key"]
            except FileNotFoundError:
                st.error("API Key missing!")
                st.stop()

            success = run_discovery_sequence(search_query, search_mode, api_key, df_db)
            if success:
                st.success(f"Discovery complete!")
                st.rerun()
            else:
                st.error("No data found.")
    
    st.divider()
    if st.button("🔄 Reset / Show Global Galaxy"):
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.cache_data.clear()
        st.rerun()

# --- 7. VISUALIZATION (HYBRID VIEW) ---

selected_artist = None

# LOGIC: If we are in "Search Mode", show the Solar System Graph.
if 'view_df' in st.session_state and not st.session_state.view_df.empty:
    st.subheader(f"🔭 System: {st.session_state.center_node or search_query}")
    display_df = st.session_state.view_df
    
    nodes = []
    edges = []
    added_node_ids = set() 

    for index, row in display_df.iterrows():
        if row['Artist'] in added_node_ids: continue
        size = 25
        if row['Monthly Listeners'] > 1000000: size = 40
        if row['Monthly Listeners'] > 10000000: size = 60
        
        if 'center_node' in st.session_state and st.session_state.center_node:
            if row['Artist'].lower() == st.session_state.center_node.lower():
                size = 100
        
        tooltip_text = f"Genre: {row['Genre']}\nFans: {int(row['Monthly Listeners']):,}\nEnergy: {row['Energy']:.2f}\nMood: {row['Valence']:.2f}"
        nodes.append(Node(id=row['Artist'], label=row['Artist'], size=size, shape="circularImage", image=row['Image URL'], title=tooltip_text))
        added_node_ids.add(row['Artist'])

    center = st.session_state.center_node
    if center:
        real_center_name = next((row['Artist'] for i, row in display_df.iterrows() if row['Artist'].lower() == center.lower()), None)
        if real_center_name:
            for index, row in display_df.iterrows():
                if row['Artist'] != real_center_name:
                    edges.append(Edge(source=real_center_name, target=row['Artist'], color="#888888"))

    config = Config(width="100%", height=600, directed=False, physics=True, hierarchical=False, nodeHighlightBehavior=True, highlightColor="#F7A7A6", collapsible=True)
    selected_artist = agraph(nodes=nodes, edges=edges, config=config)

else:
    # --- VIEW B: GLOBAL GALAXY (3D SCATTER) ---
    st.subheader("🌍 The Universal Galaxy")
    
    if not df_db.empty:
        fig = px.scatter_3d(
            df_db,
            x='Valence',
            y='Energy',
            z='Log_Listeners', 
            color='Genre',
            hover_name='Artist',
            hover_data=['Monthly Listeners', 'Genre'],
            size='Monthly Listeners',
            size_max=50,
            opacity=0.8,
            template="plotly_dark",
            height=700
        )
        fig.update_layout(scene=dict(xaxis_title='Sad ⟵ Mood ⟶ Happy', yaxis_title='Mellow ⟵ Intensity ⟶ Heavy', zaxis_title='Underground ⟵ Fame ⟶ Mainstream'), margin=dict(l=0, r=0, b=0, t=0))
        
        event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points")
        
        if len(event.selection["points"]) > 0:
            point_index = event.selection["points"][0]["point_index"]
            selected_artist = df_db.iloc[point_index]["Artist"]
    else:
        st.info("Database empty.")

# --- 8. THE DASHBOARD ---
if selected_artist and not selected_artist.startswith("genre_"):
    st.divider()
    col_title, col_btn = st.columns([3, 1])
    with col_title:
        st.header(f"🤿 Deep Dive: {selected_artist}")
    with col_btn:
        if st.button(f"🔭 Center Map on {selected_artist}"):
            try:
                api_key = st.secrets["lastfm_key"]
                run_discovery_sequence(selected_artist, "Artist", api_key, df_db)
            except Exception as e:
                st.error(f"Error: {e}")

    try:
        api_key = st.secrets["lastfm_key"]
        col1, col2 = st.columns([1, 2])
        row = df_db[df_db['Artist'] == selected_artist]
        image_url = None
        if not row.empty: image_url = row.iloc[0]['Image URL']
        
        if not image_url or "placeholder" in str(image_url):
                live_deezer = get_deezer_data(selected_artist)
                if live_deezer: image_url = live_deezer['image']

        with col1:
            if image_url and str(image_url).startswith("http"):
                st.image(image_url)
            if not row.empty:
                st.metric("Monthly Listeners", f"{int(row.iloc[0]['Monthly Listeners']):,}")
                st.write(f"**Genre:** {row.iloc[0]['Genre']}")
                st.caption(f"Energy: {float(row.iloc[0]['Energy']):.2f}")
                st.progress(float(row.iloc[0]['Energy']))
                st.caption(f"Mood: {float(row.iloc[0]['Valence']):.2f}")
                st.progress(float(row.iloc[0]['Valence']))

        with col2:
            with st.spinner("Fetching tracks..."):
                details = get_artist_details(selected_artist, api_key)
                tracks = get_top_tracks(selected_artist, api_key)

            if details and 'bio' in details:
                    st.info(details['bio']['summary'].split("<a href")[0])
            
            track_data = [{"Song": t['name'], "Playcount": f"{int(t['playcount']):,}", "Link": t['url']} for t in tracks]
            st.dataframe(pd.DataFrame(track_data), column_config={"Link": st.column_config.LinkColumn("Listen")}, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load details. {e}")