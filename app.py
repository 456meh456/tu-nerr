import streamlit as st
import pandas as pd
import time
import random 

# --- IMPORT MODULES ---
from src.db_model import fetch_all_artists_df, delete_artist
from src.api_handler import get_similar_artists, get_top_artists_by_genre, process_artist, get_artist_details, get_top_tracks, get_deezer_data, get_deezer_preview, get_neighbors_for_view
from src.ai_engine import get_ai_neighbors, generate_territory_map, get_track_neighbors
from src.visuals import render_graph 

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
col_logo, col_title = st.columns([1, 6])
with col_logo:
    # Use the image if it exists, otherwise skip to prevent error
    try:
        st.image("trip-l_Logo_orange_crop.jpg", width=120)
    except:
        st.write("🎵") 
with col_title:
    st.title("tu-nerr: The Discovery Engine")

# --- CORE LOGIC FLOW ---

def run_discovery_and_commit(center, mode, api_key, df_db):
    targets = []
    with st.spinner(f"Scanning: {center}..."):
        if mode == "Artist":
            targets.append(center)
            similar = get_similar_artists(center, api_key, limit=20) 
            targets.extend(similar)
        else:
            targets = get_top_artists_by_genre(center, api_key, limit=20)
    
    targets = list(set(targets))

    session_data = []
    prog = st.progress(0)
    
    session_added_set = set(df_db['Artist_Lower'].tolist()) if not df_db.empty else set()
        
    for i, artist in enumerate(targets):
        prog.progress((i + 1) / len(targets))
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

# Initialize key for resetting data editor
if 'track_editor_key' not in st.session_state:
    st.session_state.track_editor_key = 0

if 'view_df' not in st.session_state and not st.session_state.initial_run_complete:
    if not df_db.empty:
        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            try:
                sample_df = df_db.sample(min(len(df_db), 30))
                random_center = sample_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
                
                key = st.secrets["lastfm_key"]
                st.cache_data.clear() 
                
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

# --- 3. SIDEBAR ---
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
                    is_known = query.lower() in df_db['Artist_Lower'].tolist()
                    if is_known:
                        st.session_state.view_df = get_neighbors_for_view(query, mode, key, df_db)
                        st.session_state.center_node = query
                        st.session_state.view_source = "Social"
                        st.rerun()
                    else:
                        if run_discovery_and_commit(query, mode, key, df_db): 
                            st.success(f"Artist '{query}' added. Refreshing...")
                            st.rerun()
                        else: 
                            st.error(f"'{query}' not found by APIs. Queueing for harvest.")
                except Exception as e: st.error(f"Search error: {e}")
    
    st.divider()
    st.subheader("🎛️ Vibe Filters")
    texture_range = st.slider("Texture (Noisiness)", 0.0, 1.0, (0.0, 1.0), help="Filter by rhythmic density.")

    st.divider()
    if st.button("🔄 Reset / Global Map"):
        st.session_state.initial_run_complete = False 
        if 'view_df' in st.session_state: del st.session_state['view_df']
        if 'center_node' in st.session_state: del st.session_state['center_node']
        st.rerun()

    with st.expander("🔐 Admin"):
        pw = st.text_input("Password:", type="password")
        if pw and pw == st.secrets.get("admin_password", ""):
            options = df_db['Artist'].sort_values().unique() if not df_db.empty else []
            artist_del = st.selectbox("Delete Artist", options)
            if st.button("Delete"):
                if delete_artist(artist_del):
                    st.success(f"Deleted {artist_del}")
                    time.sleep(1)
                    st.cache_data.clear()
                    st.rerun()

# --- 4. VISUALIZATION CONTROLLER ---
disp_df = st.session_state.get('view_df', pd.DataFrame())
center = st.session_state.get('center_node', 'Unknown')
source = st.session_state.get('view_source', 'Social')

if not disp_df.empty and 'Audio_Noisiness' in disp_df.columns:
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
if not selected and center and center != 'Unknown':
    if not disp_df.empty and not disp_df[disp_df['Artist'].str.lower() == center.lower()].empty:
        selected = center 

if selected:
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1: st.header(f"🤿 {selected}")
    with c2:
        if st.button("🔭 Travel Here (Social)", type="primary"):
            st.session_state.view_df = get_neighbors_for_view(selected, "Artist", st.secrets["lastfm_key"], df_db)
            st.session_state.center_node = selected
            st.session_state.view_source = "Social"
            st.rerun()
            
        if st.button("🤖 AI Neighbors (Band)"):
            ai_recs = get_ai_neighbors(selected, df_db)
            if not ai_recs.empty:
                st.session_state.view_df = ai_recs
                st.session_state.center_node = selected
                st.session_state.view_source = "AI (Audio)"
                st.rerun()
            else: st.error("Not enough data.")

    try:
        row = df_db[df_db['Artist'] == selected]
        if row.empty:
            d_live = get_deezer_data(selected)
            r = {'Image URL': d_live['image'] if d_live else '', 'Audio_BPM': 0, 'Audio_Brightness': 0.5, 'Tag_Energy': 0.5, 'Valence': 0.5, 'Monthly Listeners': 0, 'Genre': 'Unknown', 'Audio_Noisiness': 0.5}
        else:
            r = row.iloc[0]

        col1, col2 = st.columns([1, 2])
        with col1:
            img = r.get('Image URL')
            if img and str(img).startswith("http"): st.image(img)
            
            d_live = get_deezer_data(selected)
            if d_live and d_live.get('id'):
                preview = get_deezer_preview(d_live['id'])
                if preview: 
                    st.audio(preview['preview'], format='audio/mp3')
                    st.caption(f"🎵 {preview['title']}")
            
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
            with st.spinner("Fetching info..."):
                det = get_artist_details(selected, key)
                tracks = get_top_tracks(selected, key)
            
            if det and 'bio' in det: st.info(det['bio']['summary'].split("<a href")[0])
            
            # --- TRACK TABLE & MAP BUTTON ---
            if tracks:
                st.subheader("Top Tracks Analysis")
                track_data_list = []
                for track in tracks:
                    track_data_list.append({
                        "Song": track['name'], 
                        "Link": track.get('url', '#'),
                        "Map": False, # Using Checkbox instead of Button
                        "artist_name": selected 
                    })
                
                df_track_list = pd.DataFrame(track_data_list)
                
                # Use dynamic key to force reset after interaction
                editor_key = f"track_editor_{st.session_state.track_editor_key}"

                # FIX: Use CheckboxColumn instead of ButtonColumn
                edited_df = st.data_editor(
                    df_track_list,
                    column_config={
                        "Link": st.column_config.LinkColumn("Listen", width="small"),
                        "Map": st.column_config.CheckboxColumn("Map Vibe", help="Check to map this track", default=False),
                        "artist_name": None 
                    },
                    hide_index=True,
                    width="stretch", # FIX: Replaces use_container_width=True
                    key=editor_key
                )
                
                # Check for checkbox toggle
                if 'Map' in edited_df.columns:
                    # Find rows where Map is True
                    selected_tracks = edited_df[edited_df['Map'] == True]
                    if not selected_tracks.empty:
                        # Trigger action on first selected track
                        sel_row = selected_tracks.iloc[0]
                        st.session_state.center_track_title = sel_row['Song']
                        st.session_state.center_artist = selected
                        st.session_state.track_map_requested = True
                        
                        # Increment key to reset the editor on next run (breaking the loop)
                        st.session_state.track_editor_key += 1
                        st.rerun()

            # --- HANDLE TRACK MAP ---
            if st.session_state.get('track_map_requested', False):
                st.session_state.track_map_requested = False
                track_title = st.session_state.center_track_title
                artist_name = st.session_state.center_artist
                
                with st.spinner(f"Mapping vibe of: {track_title}..."):
                    track_recs_df = get_track_neighbors(artist_name, track_title)
                    
                    if not track_recs_df.empty:
                        artist_names = track_recs_df['artist_name'].unique().tolist()
                        full_artist_profiles = df_db[df_db['Artist'].isin(artist_names)].copy()
                        st.session_state.view_df = full_artist_profiles
                        st.session_state.center_node = selected 
                        st.session_state.view_source = "AI (Track)"
                        st.rerun()
                    else:
                        st.error(f"No audio data found for '{track_title}'. Run harvester to analyze.")

    except Exception as e:
        st.error(f"Dashboard Load Error: {e}")
else:
    if df_db.empty: st.info("Database empty.")