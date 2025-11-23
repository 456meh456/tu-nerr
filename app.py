import streamlit as st
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials
from streamlit_agraph import agraph, Node, Edge, Config

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# --- 1. GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def get_sheet_connection():
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
    """Fetches, cleans, and deduplicates data."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # 1. Force Numbers
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. Clean Strings (Remove empty rows and whitespace)
    if not df.empty and 'Artist' in df.columns:
        df['Artist'] = df['Artist'].astype(str).str.strip()
        df = df[df['Artist'].str.len() > 0]
        # Normalize for matching
        df['Artist_Lower'] = df['Artist'].str.lower()
        # Deduplicate
        df = df.drop_duplicates(subset=['Artist_Lower'], keep='first')
    else:
        df = pd.DataFrame(columns=['Artist', 'Genre', 'Monthly Listeners', 'Energy', 'Valence', 'Image URL', 'Artist_Lower'])
        
    return df

def save_artist(artist_data):
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
    # 1. Check DB first (Case insensitive)
    if not df_db.empty:
        match = df_db[df_db['Artist_Lower'] == name.strip().lower()]
        if not match.empty:
            return match.iloc[0].to_dict()

    # 2. Fetch New Data
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

# --- 4. LOAD DATA INITIAL ---
try:
    df_db = load_data()
except Exception as e:
    st.error("Could not load data. Check secrets.")
    st.stop()

# --- 5. SIDEBAR ---
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

            target_list = []
            with st.spinner(f"Scanning the cosmos for {search_query}..."):
                if search_mode == "Artist":
                    target_list.append(search_query)
                    similar = get_similar_artists(search_query, api_key, limit=10)
                    target_list.extend(similar)
                else:
                    target_list = get_top_artists_by_genre(search_query, api_key, limit=12)
            
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
                st.session_state.center_node = search_query if search_mode == "Artist" else None
                st.success(f"Discovery complete! Found {len(session_df)} artists.")
                st.rerun()
            else:
                st.error("No data found.")
    
    st.divider()
    if st.button("🔄 Reset / Show Random Sample"):
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.cache_data.clear()
        st.rerun()

# --- 6. THE SOLAR SYSTEM (GRAPH VIEW) ---

# View Logic:
# 1. Search Results (Highest Priority)
# 2. Random Sample (Default / "Safe State")
# 3. Empty State (If DB is new)

is_global = False
if 'view_df' in st.session_state and not st.session_state.view_df.empty:
    display_df = st.session_state.view_df
    view_title = f"🔭 System: {st.session_state.center_node or search_query}"
elif not df_db.empty:
    # SAFE DEFAULT: Pick 20 random bands so the graph isn't empty
    sample_size = min(len(df_db), 20)
    display_df = df_db.sample(n=sample_size)
    view_title = "🎲 Random Discovery Sample"
    is_global = True
else:
    display_df = pd.DataFrame()
    view_title = "Waiting for data..."

st.subheader(view_title)

if not display_df.empty:
    
    nodes = []
    edges = []
    added_node_ids = set() # Track Artist IDs
    added_genre_ids = set() # Track Genre IDs (Separately)

    # 1. Create Artist Nodes
    for index, row in display_df.iterrows():
        # Duplicate Guard
        if row['Artist'] in added_node_ids:
            continue
            
        size = 25
        if row['Monthly Listeners'] > 1000000: size = 40
        if row['Monthly Listeners'] > 10000000: size = 60
        
        if 'center_node' in st.session_state and st.session_state.center_node:
            if row['Artist'].lower() == st.session_state.center_node.lower():
                size = 100
        
        nodes.append(Node(
            id=row['Artist'], # ID matches the Artist Name exactly
            label=row['Artist'],
            size=size,
            shape="circularImage",
            image=row['Image URL'],
            title=f"{row['Genre']} | {int(row['Monthly Listeners']):,} Fans"
        ))
        added_node_ids.add(row['Artist'])

    # 2. Create Edges
    if is_global:
        # Connect Artists to Genres (Cluster View)
        for index, row in display_df.iterrows():
            genre_id = f"genre_{row['Genre']}" # NAMESPACE FIX: "genre_Rock" vs "Rock"
            
            # Add Genre Node if new
            if genre_id not in added_genre_ids:
                nodes.append(Node(id=genre_id, label=row['Genre'], size=10, color="#555555", shape="dot"))
                added_genre_ids.add(genre_id)
            
            # Link Artist to Genre
            edges.append(Edge(source=row['Artist'], target=genre_id, color="#333333"))
            
    else:
        # Star Topology (Search View)
        center = st.session_state.center_node
        if center:
            # Find the exact casing of the center node
            real_center_name = next((row['Artist'] for i, row in display_df.iterrows() if row['Artist'].lower() == center.lower()), None)
            
            if real_center_name:
                for index, row in display_df.iterrows():
                    if row['Artist'] != real_center_name:
                        edges.append(Edge(source=real_center_name, target=row['Artist'], color="#888888"))

    config = Config(
        width="100%",
        height=600,
        directed=False, 
        physics=True, 
        hierarchical=False,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        collapsible=True
    )

    selected_artist = agraph(nodes=nodes, edges=edges, config=config)

    # --- 7. THE DASHBOARD ---
    if selected_artist and not selected_artist.startswith("genre_"):
        st.divider()
        st.header(f"🤿 Deep Dive: {selected_artist}")

        try:
            api_key = st.secrets["lastfm_key"]
            col1, col2 = st.columns([1, 2])
            
            # Check DB first for image
            row = df_db[df_db['Artist'] == selected_artist]
            
            # Live fetch for fresh image if DB is missing/placeholder
            image_url = None
            if not row.empty:
                image_url = row.iloc[0]['Image URL']
            
            if not image_url or "placeholder" in str(image_url):
                 live_deezer = get_deezer_data(selected_artist)
                 if live_deezer: image_url = live_deezer['image']

            with col1:
                if image_url and str(image_url).startswith("http"):
                    st.image(image_url)
                
                if not row.empty:
                    st.metric("Monthly Listeners", f"{int(row.iloc[0]['Monthly Listeners']):,}")
                    st.write(f"**Genre:** {row.iloc[0]['Genre']}")

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

else:
    st.info("The database is empty! Use the sidebar to start your first search.")