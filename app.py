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
    """Fetches all data from the Google Sheet."""
    sheet = get_sheet_connection()
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    
    # FORCE NUMBERS: This fixes "invisible" dots caused by text formatting
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

# --- 3. API FUNCTIONS ---
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
        
        # FIX 1: verify=False helps if you are behind a corporate firewall/proxy
        # FIX 2: A timeout prevents it from hanging forever
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
        else:
            # Debug: Let us know if Deezer just couldn't find the band
            st.warning(f"Deezer found no matches for '{artist_name}'")
            
    except Exception as e:
        # Debug: Print the specific error to the dashboard so we can see it
        st.error(f"Deezer API Error: {e}")
        pass
    return None

def get_lastfm_data(artist_name, api_key):
    url = f"http://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist={artist_name}&api_key={api_key}&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        if 'error' not in data:
            return data['artist']
    except:
        pass
    return None

# --- 4. LOAD DATA ---
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

try:
    df = load_data()
except Exception as e:
    st.error("Could not load data. Check your Google Sheet connection.")
    st.stop()

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("🔭 Add to Database")
    search_query = st.text_input("Type a band name:")
    
    if st.button("Map It!"):
        if search_query:
            if not df.empty and search_query.lower() in df['Artist'].str.lower().values:
                st.warning(f"⚠️ {search_query} is already on the map!")
            else:
                try:
                    api_key = st.secrets["lastfm_key"]
                except FileNotFoundError:
                    st.error("❌ API Key missing!")
                    st.stop()

                with st.spinner(f"Hunting for {search_query}..."):
                    deezer_info = get_deezer_data(search_query)
                    clean_name = deezer_info['name'] if deezer_info else search_query
                    lastfm_info = get_lastfm_data(clean_name, api_key)

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

                        energy_score = calculate_score(tags, ENERGY_SCORES)
                        valence_score = calculate_score(tags, VALENCE_SCORES)
                        main_genre = tags[0].title() if tags else "Unknown"

                        new_data = {
                            "Artist": clean_name,
                            "Genre": main_genre,
                            "Monthly Listeners": final_listeners,
                            "Energy": energy_score,
                            "Valence": valence_score,
                            "Image URL": final_image
                        }
                        
                        save_artist(new_data)
                        st.success(f"✅ Saved {clean_name} to the database!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("❌ Band not found on Last.fm.")

# --- 6. THE MAP ---
st.subheader("The Live Landscape")

if not df.empty:
    selection = alt.selection_point(name="SelectArtist", fields=['Artist'], on='click', empty=False)
    
    base_colors = ['#e91e63', '#9b59b6', '#2e86c1', '#1abc9c', '#f1c40f', '#e67e22', '#e74c3c', '#34495e', '#7f8c8d', '#27ae60', '#2980b9', '#8e44ad', '#c0392b', '#d35400']

    chart = alt.Chart(df).mark_circle(opacity=0.6, stroke='black', strokeWidth=1).encode(
        x=alt.X('Valence', scale=alt.Scale(domain=[0, 1])),
        y=alt.Y('Energy', scale=alt.Scale(domain=[0, 1])),
        size=alt.Size('Monthly Listeners', scale=alt.Scale(range=[50, 1000]), legend=None),
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
            
            with st.spinner(f"Fetching secret intel on {selected_artist}..."):
                # 1. Last.fm Data
                details = get_artist_details(selected_artist, api_key)
                tracks = get_top_tracks(selected_artist, api_key)
                
                # 2. Deezer Data (LIVE FETCH for fresh image)
                deezer_live = get_deezer_data(selected_artist)

            if details:
                # Decide which image to use (Deezer > Database)
                image_url = None
                if deezer_live and deezer_live.get('image'):
                    image_url = deezer_live['image']
                else:
                    # Fallback to DB if Deezer fails
                    artist_row = df[df['Artist'] == selected_artist].iloc[0]
                    image_url = artist_row.get('Image URL')

                with col1:
                    if image_url and str(image_url).startswith("http"):
                        st.image(image_url)
                    
                    listeners_fmt = int(details['stats']['listeners'])
                    st.metric("Last.fm Listeners", f"{listeners_fmt:,}")
                    
                    tags = [t['name'] for t in details['tags']['tag']]
                    st.write(f"**Style:** {', '.join(tags[:3])}")
                    
                    bio_summary = details['bio']['summary'].split("<a href")[0] 
                    st.info(bio_summary)

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

    with st.expander("View Database"):
        st.dataframe(df)

else:
    st.info("The database is empty! Use the sidebar to add the first band.")