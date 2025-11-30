import streamlit as st
import pandas as pd
import time
import random 

# --- IMPORT MODULES ---
from src.db_model import fetch_all_artists_df, delete_artist
from src.api_handler import get_similar_artists, get_top_artists_by_genre, process_artist, get_artist_details, get_top_tracks, get_deezer_data, get_deezer_preview, get_neighbors_for_view
from src.ai_engine import get_ai_neighbors, generate_territory_map, get_track_neighbors # FIX: Added get_track_neighbors
from src.visuals import render_graph 

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# --- CORE LOGIC FLOW ---

def run_discovery_and_commit(center, mode, api_key, df_db):
    """
    Original function for committing NEW data. This is only called when 
    a user launches a search for a band that may be missing.
    """
    targets = []
    with st.spinner(f"Scanning: {center}..."):
        if mode == "Artist":
            targets.append(center) # The center node itself
            similar = get_similar_artists(center, api_key, limit=20) 
            targets.extend(similar)
        else:
            targets = get_top_artists_by_genre(center, api_key, limit=20)
    
    targets = list(set(targets)) # Deduplicate initial targets

    session_data = []
    prog = st.progress(0)
    
    session_added_set = set(df_db['Artist_Lower'].tolist()) if not df_db.empty else set()
        
    for i, artist in enumerate(targets):
        prog.progress((i + 1) / len(targets))
        
        # Process: Checks DB -> Fetches API -> Analyzes Audio -> Saves to SQL (SLOW PATH)
        data = process_artist(artist, df_db, api_key, session_added_set)
        if data: 
            session_data.append(data)
            session_added_set.add(data['Artist'].lower())
    
    if session_data:
        st.session_state.view_df = pd.DataFrame(session_data).drop_duplicates(subset=['Artist'])
        
        if mode == "Artist":
            st.session_state.center_node = center
        else:
            st.session_state.center_node = None
            
        st.session_state.view_source = "Social"
        return True
    return False

# --- 1. INITIAL LOAD ---
try:
    df_db = fetch_all_artists_df()
except Exception as e:
    st.error(f"FATAL DB ERROR: Failed to load initial data. Details: {e}")
    st.stop()

# --- 2. INITIAL VIEW STATE CHECK ---
if 'initial_run_complete' not in st.session_state:
    st.session_state.initial_run_complete = False

if 'view_df' not in st.session_state and not st.session_state.initial_run_complete:
    if not df_db.empty:
        # Initial Random Cluster View
        MAX_RETRIES = 3
        
        for attempt in range(MAX_RETRIES):
            try:
                sample_df = df_db.sample(min(len(df_db), 30))
                random_center = sample_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
                
                key = st.secrets["lastfm_key"]
                st.cache_data.clear() 
                
                # FIX: We now call the fast read path for the initial view
                st.session_state.view_df = get_neighbors_for_view(random_center, "Artist", key, df_db)
                
                if not st.session_state.view_df.empty:
                     st.session_state.center_node = random_center
                     st.session_state.view_source = "Random Cluster"
                     st.session_state.initial_run_complete = True
                     st.rerun() 
                     break
                else:
                    time.sleep(0.5)
            except Exception:
                 time.sleep(1)
        
        if not st.session_state.initial_run_complete:
            st.session_state.view_df = pd.DataFrame()
            st.error("Initial load failed after 3 attempts. Please try manual search.")

# --- 3. SIDEBAR (CONTROLS) ---
with st.sidebar:
    st.header("🚀 Discovery Engine")
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    with st.form(key='search'):
        mode = st.radio("Search By:", ["Artist", "Genre"])
        query = st.text_input(f"Enter {mode} Name:")
        if st.form_submit_button("Launch"):
            if query:
                try:
                    key = st.secrets["lastfm_key"]
                    st.cache_data.clear() 
                    
                    # 1. Check if artist is known
                    is_known = query.lower() in df_db['Artist_Lower'].tolist()
                    
                    if is_known:
                        # 2. If known, run the fast read path
                        st.session_state.view_df = get_neighbors_for_view(query, mode, key, df_db)
                        st.session_state.center_node = query
                        st.session_state.view_source = "Social"
                        st.rerun()
                    else:
                        # 3. If new, run the slow commit path
                        if run_discovery_and_commit(query, mode, key, df_db): 
                            st.success(f"Artist '{query}' successfully analyzed and added to the database. Refreshing map...")
                            st.rerun()
                        else: 
                            st.error(f"'{query}' not found by APIs. If the band exists, please run the 'bulk_harvester.py' script locally to attempt addition.")
                            

                except Exception as e: st.error(f"Search error: {e}")
    
    st.divider()
    
    # --- VIBE FILTERS ---
    st.subheader("🎛️ Vibe Filters")
    texture_range = st.slider("Texture (Noisiness)", 0.0, 1.0, (0.0, 1.0), 
                              help="Filter by rhythmic density. Low = Melodic/Smooth. High = Percussive/Rap.")

    st.divider()
    if st.button("🔄 Reset / Global Map"):
        st.session_state.initial_run_complete = False 
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.rerun()

    # --- ADMIN ZONE (Janitor) ---
    with st.expander("🔐 Admin"):
        pw = st.text_input("Password:", type="password")
        if pw and pw == st.secrets.get("admin_password", ""):
            # Populate delete box if DB has data
            options = df_db['Artist'].sort_values().unique() if not df_db.empty else []
            artist_del = st.selectbox("Delete Artist", options)
            
            if st.button("Delete"):
                if delete_artist(artist_del):
                    st.success(f"Deleted {artist_del}")
                    time.sleep(1)
                    st.cache_data.clear() # Clear cache to force load fresh data
                    st.rerun()
                else:
                    st.error("Delete failed.")

# --- 4. VISUALIZATION CONTROLLER ---
disp_df = st.session_state.get('view_df', pd.DataFrame())
center = st.session_state.get('center_node', 'Unknown')
source = st.session_state.get('view_source', 'Social')

# --- FILTER LOGIC ---
if not disp_df.empty and 'Audio_Noisiness' in disp_df.columns:
    # Apply the slider filter to the dataframe before rendering
    min_noise, max_noise = texture_range
    disp_df = disp_df[
        (disp_df['Audio_Noisiness'] >= min_noise) & 
        (disp_df['Audio_Noisiness'] <= max_noise)
    ]

st.subheader(f"🔭 System: {center if center else 'Universal Galaxy'} ({source} Connection)")

selected = None
if not disp_df.empty:
    selected = render_graph(disp_df, center, source)
elif not df_db.empty:
    st.warning("No artists match your current Texture Filter.")

# --- 5. DASHBOARD ---

# Auto-select the center node for the dashboard immediately after search/load
if not selected and center and center != 'Unknown':
    if not disp_df.empty and not disp_df[disp_df['Artist'].str.lower() == center.lower()].empty:
        selected = center 

if selected:
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1: st.header(f"🤿 {selected}")
    with c2:
        # 1. TRAVEL BUTTON
        if st.button("🔭 Travel Here (Social)", type="primary"):
            # If traveling, we run the fast read function
            st.session_state.view_df = get_neighbors_for_view(selected, "Artist", st.secrets["lastfm_key"], df_db)
            st.session_state.center_node = selected
            st.session_state.view_source = "Social"
            st.rerun()
            
        # 2. AI BUTTON (Band-Level KNN)
        if st.button("🤖 AI Neighbors (Band)"):
            # Pass full DB to AI engine for best results
            ai_recs = get_ai_neighbors(selected, df_db)
            if not ai_recs.empty:
                st.session_state.view_df = ai_recs
                st.session_state.center_node = selected
                st.session_state.view_source = "AI (Audio)"
                st.rerun()
            else: st.error("Not enough data for AI analysis. (Need 5+ bands)")

    try:
        row = df_db[df_db['Artist'] == selected]
        
        # Handle case where selected node is in graph but not in current DB snapshot
        if row.empty:
            d_live = get_deezer_data(selected)
            r = {
                'Image URL': d_live['image'] if d_live else '',
                'Audio_BPM': 0, 'Audio_Brightness': 0.5, 'Tag_Energy': 0.5, 'Valence': 0.5,
                'Monthly Listeners': d_live['listeners'] if d_live else 0, 'Genre': 'Unknown',
                'Audio_Noisiness': 0.5 
            }
        else:
            r = row.iloc[0]

        col1, col2 = st.columns([1, 2])
        
        # COLUMN 1: Vitals & Audio
        with col1:
            img = r.get('Image URL')
            if img and str(img).startswith("http"): st.image(img)
            
            # Live Audio Fetch (we don't store the MP3 url in the main table to keep it light)
            d_live = get_deezer_data(selected)
            if d_live and d_live.get('id'):
                preview = get_deezer_preview(d_live['id'])
                if preview: 
                    st.audio(preview['preview'], format='audio/mp3')
                    st.caption(f"🎵 {preview['title']}")
            
            # Vibe Meters
            energy = float(r.get('Audio_Brightness', 0) or r.get('Tag_Energy', 0.5))
            noise = float(r.get('Audio_Noisiness', 0))
            v_val = float(r.get('Valence', 0.5))
            
            st.metric("Fans", f"{int(r['Monthly Listeners']):,}")
            st.metric("BPM", int(r.get('Audio_BPM', 0)))
            
            st.caption(f"🔥 Intensity: {energy:.2f}")
            st.progress(energy)
            
            st.caption(f"😊 Mood (Happiness): {v_val:.2f}")
            st.progress(v_val)

            st.caption(f"🌊 Texture (Noisiness): {noise:.2f}")
            st.progress(noise)

        with col2:
            key = st.secrets["lastfm_key"]
            with st.spinner("Fetching biography..."):
                det = get_artist_details(selected, key)
                tracks = get_top_tracks(selected, key)
            
            if det and 'bio' in det: st.info(det['bio']['summary'].split("<a href")[0])
            if tracks:
                st.subheader("Top Tracks Analysis") 
                
                # 1. Map Last.fm track list to a DataFrame
                track_data_list = []
                for track in tracks:
                    track_data_list.append({
                        "Song": track['name'], 
                        "Link": track.get('url', '#'),
                        "Map": False, # Placeholder for button/selection
                        "artist_name": selected 
                    })
                
                df_track_list = pd.DataFrame(track_data_list)
                
                # 2. Display with interactive button
                edited_df = st.data_editor(
                    df_track_list,
                    column_config={
                        "Link": st.column_config.LinkColumn("Listen", width="small"),
                        "Map": st.column_config.ButtonColumn("Map This Vibe", help="Find artists similar to this specific track.", disabled="Map This Vibe" != "Map This Vibe"),
                        "artist_name": None 
                    },
                    hide_index=True,
                    use_container_width=True,
                    num_rows="dynamic",
                )
                
                # 3. Check for button click (Track-Level Discovery)
                if 'Map' in edited_df.columns:
                    for i, row in edited_df.iterrows():
                        if row['Map']:
                            # Trigger track-based AI search
                            st.session_state.center_track_title = row['Song']
                            st.session_state.center_artist = selected
                            st.session_state.track_map_requested = True
                            st.rerun()

            # --- Handle Track-Map Result ---
            if st.session_state.get('track_map_requested', False):
                st.session_state.track_map_requested = False
                
                track_title = st.session_state.center_track_title
                artist_name = st.session_state.center_artist
                
                with st.spinner(f"Mapping vibe of: {track_title}..."):
                    track_recs_df = get_track_neighbors(artist_name, track_title)
                    
                    if not track_recs_df.empty:
                        # Extract unique artist names for the node display
                        artist_names = track_recs_df['artist_name'].unique().tolist()
                        
                        # Fetch the full artist profiles for the graph view
                        full_artist_profiles = df_db[df_db['Artist'].isin(artist_names)].copy()
                        
                        st.session_state.view_df = full_artist_profiles
                        st.session_state.center_node = selected 
                        st.session_state.view_source = "AI (Track)"
                        st.rerun()
                    else:
                        st.error(f"Could not find sufficient data in the tracks table for '{track_title}'.")

    except Exception as e:
        st.error(f"Dashboard Load Error: {e}")

else:
    if df_db.empty:
        st.info("The database is empty! Run bulk_harvester.py to seed data.")