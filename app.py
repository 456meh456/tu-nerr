import streamlit as st
import pandas as pd
import altair as alt
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Live Music Map")

# --- 1. CONNECT TO GOOGLE SHEETS ---
# This function connects to the sheet and caches the connection
@st.cache_resource
def get_sheet_connection():
    # Define the scope (what permissions we need)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Load credentials from secrets
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scope
    )
    
    client = gspread.authorize(creds)
    # Open the sheet by name
    return client.open("tu-nerr-db").sheet1

# --- 2. DATA FUNCTIONS ---
def load_data():
    sheet = get_sheet_connection()
    # Get all records as a list of dictionaries
    data = sheet.get_all_records()
    return pd.DataFrame(data)

def save_artist(artist_data):
    sheet = get_sheet_connection()
    # Prepare the row in the exact order of your Sheet headers
    row = [
        artist_data['Artist'],
        artist_data['Genre'],
        artist_data['Monthly Listeners'],
        artist_data['Energy'],
        artist_data['Valence'],
        artist_data['Image URL']
    ]
    sheet.append_row(row)

# --- 3. LOAD DATA ---
# Force a reload button to fetch updates from other users
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

# Initial Load
try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to Google Sheet. Check secrets. Error: {e}")
    st.stop()

# --- 4. THE SEARCH BAR (The Harvester) ---
with st.sidebar:
    st.header("🔭 Add to Database")
    search_query = st.text_input("Type a band name:")
    
    if st.button("Map It!"):
        if search_query:
            # Check if band already exists in the dataframe
            if not df.empty and search_query.lower() in df['Artist'].str.lower().values:
                st.warning("This artist is already on the map!")
            else:
                try:
                    api_key = st.secrets["lastfm_key"]
                except FileNotFoundError:
                    st.error("Last.fm API Key missing!")
                    st.stop()

                with st.spinner(f"Hunting for {search_query}..."):
                    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={search_query}&api_key={api_key}&format=json"
                    response = requests.get(url)
                    data = response.json()

                    if 'error' not in data:
                        artist = data['artist']
                        name = artist['name']
                        listeners = int(artist['stats']['listeners'])
                        tags = [tag['name'].lower() for tag in artist['tags']['tag']]
                        
                        # SCORING LOGIC
                        energy_score = 0.5
                        valence_score = 0.5
                        
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

                        new_data = {
                            "Artist": name,
                            "Genre": tags[0].title() if tags else "Unknown",
                            "Monthly Listeners": listeners,
                            "Energy": energy_score,
                            "Valence": valence_score,
                            "Image URL": "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg"
                        }
                        
                        # SAVE TO GOOGLE SHEET
                        save_artist(new_data)
                        st.success(f"Saved {name} to the permanent database!")
                        st.cache_data.clear() # Clear cache so the new data shows up
                        st.rerun()
                    else:
                        st.error("Band not found on Last.fm.")

# --- 5. THE VISUALIZATION ---
st.subheader("The Live Landscape")

if not df.empty:
    chart = alt.Chart(df).mark_circle().encode(
        x='Valence',
        y='Energy',
        size='Monthly Listeners',
        color=alt.Color('Genre', legend=None),
        tooltip=['Artist', 'Genre', 'Monthly Listeners'],
        href='Image URL'
    ).interactive()

    st.altair_chart(chart, use_container_width=True)
    
    with st.expander("View Database"):
        st.dataframe(df)
else:
    st.info("The database is empty! Use the sidebar to add the first band.")