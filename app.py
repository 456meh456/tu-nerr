import streamlit as st
import pandas as pd
import altair as alt
import requests
import gspread
from google.oauth2.service_account import Credentials
import time

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
    
    # FORCE NUMBERS
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Create a lowercase column for easier searching
    if not df.empty:
        df['Artist_Lower'] = df['Artist'].str.strip().str.lower()
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
    """Fetches a list of similar artist names from Last.fm."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist={artist_name}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        if 'similarartists' in data:
            return [a['name'] for a in data['similarartists']['artist']]
    except:
        pass
    return []

def get_top_artists_by_genre(genre, api_key, limit=12):
    """Fetches top artists for a specific tag/genre."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag={genre}&api_key={api_key}&limit={limit}&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        if 'topartists' in data:
            return [a['name'] for a in data['topartists']['artist']]
    except:
        pass
    return []

def get_artist_details(artist_name, api_key):
    """Fetches Bio and Stats from Last.fm."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={api_key}&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        if 'error' not in data:
            return data['artist']
    except:
        pass
    return None

def get_top_tracks(artist_name, api_key):
    """Fetches Top 5 Tracks from Last.fm."""
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.gettoptracks&artist={artist_name}&api_key={api_key}&limit=5&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        if 'error' not in data:
            return data['toptracks']['track']
    except:
        pass
    return []

def get_deezer_data(artist_name):
    """Fetches Name, Image, and Fan Count from Deezer."""
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
    """Checks DB first, otherwise fetches from API and saves."""
    # 1. Check DB
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
        
        # Scoring
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

# --- 5. SIDEBAR: THE EXPLORER ---
with st.sidebar:
    st.header("🚀 Exploration Engine")
    
    search_mode = st.radio("Search By:", ["Artist", "Genre"])
    search_query = st.text_input(f"Enter {search_mode} Name:")
    
    if st.button("Launch Discovery"):
        if search_query:
            try:
                api_key = st.secrets["lastfm_key"]
            except FileNotFoundError:
                st.error("API Key missing!")
                st.stop()

            # 1. BUILD TARGET LIST
            target_list = []
            with st.spinner(f"Scanning the cosmos for {search_query}..."):
                if search_mode == "Artist":
                    target_list.append(search_query)
                    # Get 10 neighbors
                    similar = get_similar_artists(search_query, api_key, limit=10)
                    target_list.extend(similar)
                else:
                    target_list = get_top_artists_by_genre(search_query, api_key, limit=12)

            # 2. PROCESS BATCH
            current_session_data = []
            progress_bar = st.progress(0)
            
            for i, artist in enumerate(target_list):
                progress_bar.progress((i + 1) / len(target_list))
                data = process_artist(artist, df_db, api_key)
                if data:
                    current_session_data.append(data)
                
                # Refresh DB copy in memory occasionally
                if i % 3 == 0:
                    df_db = load_data() 

            # 3. SAVE RESULTS TO SESSION STATE
            if current_session_data:
                st.session_state.view_df = pd.DataFrame(current_session_data)
                st.session_state.center_node = search_query if search_mode == "Artist" else None
                st.success(f"Discovery complete! Found {len(current_session_data)} artists.")
                st.rerun()
            else:
                st.error("No data found. Try a different name.")
    
    st.divider()
    if st.button("🔄 Reset / Show Global Map"):
        if 'view_df' in st.session_state:
            del st.session_state['view_df']
        if 'center_node' in st.session_state:
            del st.session_state['center_node']
        st.cache_data.clear()
        st.rerun()

# --- 6. THE MAP (DYNAMIC VIEW) ---

# Logic: Are we looking at a search result, or the whole world?
if 'view_df' in st.session_state and not st.session_state.view_df.empty:
    display_df = st.session_state.view_df
    view_title = f"🔭 Results for: {st.session_state.center_node or search_query}"
else:
    display_df = df_db
    view_title = "🌍 Global Database View"

st.subheader(view_title)

if not display_df.empty:
    # Selection Logic
    selection = alt.selection_point(name="SelectArtist", fields=['Artist'], on='click', empty=False)
    
    base_colors = ['#e91e63', '#9b59b6', '#2e86c1', '#1abc9c', '#f1c40f', '#e67e22', '#e74c3c', '#34495e', '#7f8c8d', '#27ae60', '#2980b9', '#8e44ad', '#c0392b', '#d35400']

    chart = alt.Chart(display_df).mark_circle(stroke='black', strokeWidth=1).encode(
        x=alt.X('Valence', scale=alt.Scale(domain=[0, 1])),
        y=alt.Y('Energy', scale=alt.Scale(domain=[0, 1])),
        size=alt.Size('Monthly Listeners', scale=alt.Scale(range=[100, 1000]), legend=None),
        color=alt.Color('Genre', scale=alt.Scale(range=base_colors), legend=None),
        tooltip=['Artist', 'Genre', 'Monthly Listeners'],
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
    ).add_params(selection).properties(height=600).interactive()

    event = st.altair_chart(chart, use_container_width=True, on_select="rerun")

    # --- 7. THE DASHBOARD ---
    selected_artist = None
    if event.selection and "SelectArtist" in event.selection:
        selection_data = event.selection["SelectArtist"]
        if isinstance(selection_data, list) and len(selection_data) > 0:
            selected_artist = selection_data[0].get("Artist")
        elif isinstance(selection_data, pd.DataFrame) and not selection_data.empty:
            selected_artist = selection_data.iloc[0]["Artist"]

    if selected_artist:
        st.divider()
        st.header(f"🤿 Deep Dive: {selected_artist}")

        try:
            api_key = st.secrets["lastfm_key"]
            col1, col2 = st.columns([1, 2])
            
            # Grab image from the display_df (since we already have it there)
            row = display_df[display_df['Artist'] == selected_artist]
            if not row.empty:
                image_url = row.iloc[0]['Image URL']
            else:
                image_url = None

            with col1:
                if image_url and str(image_url).startswith("http"):
                    st.image(image_url)
                
                # Only fetch details if we don't have them (or to get fresh stats)
                with st.spinner("Fetching details..."):
                    details = get_artist_details(selected_artist, api_key)
                
                if details:
                    listeners_fmt = int(details['stats']['listeners'])
                    st.metric("Last.fm Listeners", f"{listeners_fmt:,}")
                    tags = [t['name'] for t in details['tags']['tag']]
                    st.write(f"**Style:** {', '.join(tags[:3])}")
                    st.info(details['bio']['summary'].split("<a href")[0])

            with col2:
                with st.spinner("Fetching top tracks..."):
                    tracks = get_top_tracks(selected_artist, api_key)
                
                track_data = [{"Song": t['name'], "Playcount": f"{int(t['playcount']):,}", "Link": t['url']} for t in tracks]
                
                st.dataframe(
                    pd.DataFrame(track_data),
                    column_config={"Link": st.column_config.LinkColumn("Listen")},
                    hide_index=True,
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Could not load details. {e}")

    # Optional: View raw data
    with st.expander("View Database"):
        st.dataframe(display_df)

else:
    st.info("The database is empty! Use the sidebar to start your first search.")