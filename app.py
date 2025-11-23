import streamlit as st
import pandas as pd
import altair as alt
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Live Music Map")

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
    """Fetches all data from the Google Sheet and cleans it."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # FORCE NUMBERS: This fixes "invisible" dots caused by text formatting
    # We convert these columns to numeric, turning errors (like empty strings) into NaN
    cols_to_fix = ['Monthly Listeners', 'Energy', 'Valence']
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
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

# --- 3. LAST.FM API FUNCTIONS ---
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

# --- 4. LOAD DATA ---
# Button to force a reload from Google Sheets
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

try:
    df = load_data()
except Exception as e:
    st.error("Could not load data. Check your Google Sheet connection.")
    st.stop()

# --- 5. SIDEBAR: THE "HARVESTER" (ADD BANDS) ---
with st.sidebar:
    st.header("🔭 Add to Database")
    search_query = st.text_input("Type a band name:")
    
    if st.button("Map It!"):
        if search_query:
            # Check if already exists (case insensitive)
            if not df.empty and search_query.lower() in df['Artist'].str.lower().values:
                st.warning(f"⚠️ {search_query} is already on the map!")
            else:
                try:
                    api_key = st.secrets["lastfm_key"]
                except FileNotFoundError:
                    st.error("❌ API Key missing! Check secrets.toml")
                    st.stop()

                with st.spinner(f"Hunting for {search_query}..."):
                    # 1. Fetch Basic Info
                    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={search_query}&api_key={api_key}&format=json"
                    response = requests.get(url)
                    data = response.json()

                    if 'error' not in data:
                        artist = data['artist']
                        name = artist['name']
                        listeners = int(artist['stats']['listeners'])
                        tags = [tag['name'].lower() for tag in artist['tags']['tag']]
                        
                        # 2. THE BRAIN: Weighted Scoring Logic
                        ENERGY_SCORES = {
                            'death metal': 1.0, 'thrash': 0.95, 'metalcore': 0.9, 'punk': 0.9, 
                            'industrial': 0.85, 'hard rock': 0.8, 'hip hop': 0.75, 'rock': 0.7, 
                            'electronic': 0.65, 'pop': 0.6, 'indie': 0.5, 'alternative': 0.5,
                            'folk': 0.3, 'soul': 0.3, 'country': 0.4, 'jazz': 0.35,
                            'ambient': 0.1, 'acoustic': 0.2, 'classical': 0.15
                        }

                        VALENCE_SCORES = {
                            'happy': 0.9, 'party': 0.9, 'dance': 0.85, 'pop': 0.8, 'upbeat': 0.8,
                            'funk': 0.75, 'soul': 0.7, 'country': 0.6, 'folk': 0.5,
                            'progressive': 0.45, 'alternative': 0.4, 'rock': 0.5,
                            'sad': 0.2, 'dark': 0.15, 'melancholic': 0.1, 'depressive': 0.05,
                            'doom': 0.1, 'gothic': 0.2, 'industrial': 0.3, 'angry': 0.2
                        }

                        def calculate_score(tag_list, score_dict):
                            found_scores = []
                            for tag in tag_list:
                                for genre, score in score_dict.items():
                                    if genre in tag:
                                        found_scores.append(score)
                            if not found_scores: return 0.5
                            return sum(found_scores) / len(found_scores)

                        energy_score = calculate_score(tags, ENERGY_SCORES)
                        valence_score = calculate_score(tags, VALENCE_SCORES)

                        # 3. Build Data Row
                        new_data = {
                            "Artist": name,
                            "Genre": tags[0].title() if tags else "Unknown",
                            "Monthly Listeners": listeners,
                            "Energy": energy_score,
                            "Valence": valence_score,
                            "Image URL": "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg"
                        }
                        
                        # 4. Save to Cloud
                        save_artist(new_data)
                        st.success(f"✅ Saved {name} to the database!")
                        st.cache_data.clear() # Clear cache so new data appears immediately
                        st.rerun()
                    else:
                        st.error("❌ Band not found on Last.fm.")

# --- 6. THE INTERACTIVE MAP ---
st.subheader("The Live Landscape")

if not df.empty:
    # A. Selection Logic (Named "SelectArtist" so we can find it later)
    selection = alt.selection_point(
        name="SelectArtist",
        fields=['Artist'],
        on='click',
        empty=False
    )

    # B. Color Palette
    # We use a broad range of colors to handle ANY genre that comes in
    base_colors = [
        '#e91e63', '#9b59b6', '#2e86c1', '#1abc9c', '#f1c40f', '#e67e22', '#e74c3c',
        '#34495e', '#7f8c8d', '#27ae60', '#2980b9', '#8e44ad', '#c0392b', '#d35400'
    ]

    # C. Build Chart
    chart = alt.Chart(df).mark_circle(
        opacity=0.6, stroke='black', strokeWidth=1
    ).encode(
        x=alt.X('Valence', scale=alt.Scale(domain=[0, 1]), title='Sad ⟵ Mood ⟶ Happy'),
        y=alt.Y('Energy', scale=alt.Scale(domain=[0, 1]), title='Mellow ⟵ Intensity ⟶ Heavy'),
        size=alt.Size('Monthly Listeners', scale=alt.Scale(range=[50, 1000]), legend=None),
        
        # Color by Genre (removed domain restriction to allow new genres)
        color=alt.Color('Genre', scale=alt.Scale(range=base_colors), legend=None),
        
        tooltip=['Artist', 'Genre', 'Monthly Listeners', 'Energy', 'Valence'],
        
        # Highlight clicked, dim others
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
    ).add_params(
        selection
    ).properties(
        height=600
    ).interactive()

    # D. Render & Capture Click Event
    event = st.altair_chart(chart, use_container_width=True, on_select="rerun")

    # --- 7. THE DASHBOARD (SHOWN ON CLICK) ---
    selected_artist = None
    
    # PARSE THE SELECTION
    # We look for our specific named selection "SelectArtist"
    if event.selection and "SelectArtist" in event.selection:
        selection_data = event.selection["SelectArtist"]
        if selection_data:
            # Altair returns a list of dicts, we grab the first one
            selected_artist = selection_data[0]["Artist"]

    if selected_artist:
        st.divider()
        st.header(f"🤿 Deep Dive: {selected_artist}")

        try:
            api_key = st.secrets["lastfm_key"]
            col1, col2 = st.columns([1, 2])
            
            with st.spinner(f"Fetching secret intel on {selected_artist}..."):
                details = get_artist_details(selected_artist, api_key)
                tracks = get_top_tracks(selected_artist, api_key)

            if details:
                # Left Column: Bio & Stats
                with col1:
                    listeners_fmt = int(details['stats']['listeners'])
                    st.metric("Global Listeners", f"{listeners_fmt:,}")
                    
                    tags = [t['name'] for t in details['tags']['tag']]
                    st.write(f"**Style:** {', '.join(tags[:3])}")
                    
                    # Clean up bio (remove links)
                    bio_summary = details['bio']['summary'].split("<a href")[0] 
                    st.info(bio_summary)

                # Right Column: Top Tracks
                with col2:
                    st.subheader("🔥 Top Tracks")
                    track_data = []
                    for t in tracks:
                        track_data.append({
                            "Song": t['name'],
                            "Playcount": f"{int(t['playcount']):,}",
                            "Link": t['url']
                        })
                    
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
        st.dataframe(df)

else:
    st.info("The database is empty! Use the sidebar to add the first band.")