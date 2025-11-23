import streamlit as st
import pandas as pd
import altair as alt
import requests

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Hybrid Music Map")

# --- 1. LOAD THE STATIC DATA ---
@st.cache_data # This makes the app load faster
def load_data():
    try:
        return pd.read_csv("ds_0001.csv")
    except FileNotFoundError:
        return pd.DataFrame(columns=["Artist", "Genre", "Monthly Listeners", "Energy", "Valence", "Image URL"])

# Load the initial CSV
df_static = load_data()

# --- 2. SETUP SESSION STATE (The "Temporary Memory") ---
# This holds new bands the user searches for during this visit
if 'new_bands' not in st.session_state:
    st.session_state.new_bands = pd.DataFrame(columns=df_static.columns)

# Combine static data with any new user-searched bands
df_combined = pd.concat([df_static, st.session_state.new_bands], ignore_index=True)

# --- 3. THE SEARCH BAR (The "Harvester" Logic) ---
with st.sidebar:
    st.header("🔭 Discover a Band")
    search_query = st.text_input("Type a band name:")
    
    if st.button("Map It!"):
        if search_query:
            # GET THE API KEY SAFELY
            # Try to get it from secrets, handle error if missing
            try:
                api_key = st.secrets["lastfm_key"]
            except FileNotFoundError:
                st.error("API Key missing! Set it in secrets.toml")
                st.stop()

            with st.spinner(f"Hunting for {search_query}..."):
                # CALL LAST.FM API
                url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={search_query}&api_key={api_key}&format=json"
                response = requests.get(url)
                data = response.json()

                if 'error' not in data:
                    artist = data['artist']
                    name = artist['name']
                    listeners = int(artist['stats']['listeners'])
                    tags = [tag['name'].lower() for tag in artist['tags']['tag']]
                    
                    # --- 🧠 BRAIN UPGRADE: Weighted Scoring System ---
                    
                    # 1. Define Dictionaries with Weights (0.0 to 1.0)
                    # This tells the bot exactly "how heavy" or "how happy" a genre is.
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
                        """Finds all matching tags and averages their scores."""
                        found_scores = []
                        for tag in tag_list:
                            # Check if our known genres are inside the Last.fm tag
                            # e.g., if tag is "progressive metal", it catches "metal" (0.9)
                            for genre, score in score_dict.items():
                                if genre in tag:
                                    found_scores.append(score)
                        
                        if not found_scores:
                            return 0.5  # Default to neutral if no tags match
                        
                        return sum(found_scores) / len(found_scores)

                    # 2. Run the Calculation
                    energy_score = calculate_score(tags, ENERGY_SCORES)
                    valence_score = calculate_score(tags, VALENCE_SCORES)

                    # -------------------------------------------------------

                    # CREATE NEW ROW
                    new_entry = pd.DataFrame([{
                        "Artist": name,
                        "Genre": tags[0].title() if tags else "Unknown",
                        "Monthly Listeners": listeners,
                        "Energy": energy_score,
                        "Valence": valence_score,
                        "Image URL": "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg"
                    }])
                    
                    # ADD TO SESSION STATE
                    st.session_state.new_bands = pd.concat([st.session_state.new_bands, new_entry], ignore_index=True)
                    st.success(f"Found {name}! (Energy: {energy_score:.2f}, Valence: {valence_score:.2f})")
                    st.rerun()
                else:
                    st.error("Band not found on Last.fm.")

# --- 4. THE VISUALIZATION ---
st.subheader("The Landscape")

# Define the Chart using the COMBINED data
chart = alt.Chart(df_combined).mark_circle().encode(
    x='Valence',
    y='Energy',
    size='Monthly Listeners',
    color=alt.Color('Genre', legend=None),
    tooltip=['Artist', 'Genre', 'Monthly Listeners'],
    href='Image URL' # Pro-tip: Clicking a bubble could open the image
).interactive()

st.altair_chart(chart, use_container_width=True)

# Show the data table
with st.expander("View Data"):
    st.dataframe(df_combined)