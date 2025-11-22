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
                    
                    # LOGIC: Guess Energy/Valence from tags
                    energy_score = 0.5
                    valence_score = 0.5
                    
                    # (Simplified keywords for the web app)
                    ENERGY_WORDS = {'metal': 0.9, 'punk': 0.9, 'rock': 0.7, 'pop': 0.6, 'acoustic': 0.2}
                    VALENCE_WORDS = {'happy': 0.9, 'party': 0.9, 'sad': 0.2, 'dark': 0.1}

                    matches_e = [s for t, s in ENERGY_WORDS.items() if any(t in tag for tag in tags)]
                    if matches_e: energy_score = sum(matches_e)/len(matches_e)
                    
                    matches_v = [s for t, s in VALENCE_WORDS.items() if any(t in tag for tag in tags)]
                    if matches_v: valence_score = sum(matches_v)/len(matches_v)

                    # CREATE NEW ROW
                    new_entry = pd.DataFrame([{
                        "Artist": name,
                        "Genre": tags[0].title() if tags else "Unknown",
                        "Monthly Listeners": listeners,
                        "Energy": energy_score,
                        "Valence": valence_score,
                        "Image URL": "https://commons.wikimedia.org/wiki/Special:FilePath/A_placeholder_box.svg" # Placeholder
                    }])
                    
                    # ADD TO SESSION STATE (Temporary Memory)
                    st.session_state.new_bands = pd.concat([st.session_state.new_bands, new_entry], ignore_index=True)
                    st.success(f"Found {name}! Added to the map.")
                    st.rerun() # Refresh the page to show the new dot
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