import streamlit as st
import pandas as pd
import time

# --- IMPORT MODULES ---
from src.db_model import fetch_all_artists_df, delete_artist
from src.api_handler import get_similar_artists, get_top_artists_by_genre, process_artist, get_artist_details, get_top_tracks, get_deezer_data, get_deezer_preview, get_neighbors_for_view
from src.ai_engine import get_ai_neighbors, generate_territory_map
from src.visuals import render_graph 

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="tu-nerr")
st.title("🎵 tu-nerr: The Discovery Engine")

# --- CORE LOGIC FLOW ---

def run_discovery(center, mode, api_key, df_db):
    """Central logic for finding and processing a cluster of artists."""
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
    
    # Initialize set to track processed artists during this run
    session_added_set = set()
        
    for i, artist in enumerate(targets):
        prog.progress((i + 1) / len(targets))
        
        if artist.strip().lower() in session_added_set: continue
            
        # Process: Checks DB -> Fetches API -> Analyzes Audio -> Saves to SQL
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

def run_discovery_and_commit(center, mode, api_key, df_db):
    """
    Original function for committing NEW data. This is only called when 
    a user launches a search for a band that may be missing.
    """
    return run_discovery(center, mode, api_key, df_db)

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
                            st.rerun()
                        else: 
                            st.error(f"'{query}' not found. It will be added to the harvest queue. Please check back in 5 minutes.")

                except Exception as e: st.error(f"Search error: {e}")
    
    st.divider()
    
    # --- NEW: TEXTURE SLIDER ---
    st.subheader("🎛️ Vibe Filters")
    # Default 0.0 to 1.0 shows everything. User can constrict the range.
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
            
        # 2. AI BUTTON
        if st.button("🤖 AI Neighbors"):
            # Pass full DB to AI engine for best results
            ai_recs = get_ai_neighbors(selected, df_db)
            if not ai_recs.empty:
                st.session_state.view_df = ai_recs
                st.session_state.center_node = selected
                st.session_state.view_source = "AI (Audio)"
                st.rerun()
            else: st.error("Not enough data for AI analysis. (Need 5+ bands)")

    try:
        # Load detailed row data from DB
        row = df_db[df_db['Artist'] == selected]
        
        # Handle case where selected node is in graph but not in current DB snapshot
        if row.empty:
            d_live = get_deezer_data(selected)
            r = {
                'Image URL': d_live['image'] if d_live else '',
                'Audio_BPM': 0, 'Audio_Brightness': 0.5, 'Tag_Energy': 0.5, 'Valence': 0.5,
                'Monthly Listeners': d_live['listeners'] if d_live else 0, 'Genre': 'Unknown',
                'Audio_Noisiness': 0.5 # Default for unknown
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
            # Prioritize Audio Brightness if available
            audio_b = float(r.get('Audio_Brightness', 0))
            tag_e = float(r.get('Tag_Energy', 0.5))
            energy = audio_b if audio_b > 0 else tag_e
            v_val = float(r.get('Valence', 0.5))
            noise = float(r.get('Audio_Noisiness', 0))
            
            st.metric("Fans", f"{int(r['Monthly Listeners']):,}")
            st.metric("BPM", int(r.get('Audio_BPM', 0)))
            
            st.caption(f"🔥 Energy (Intensity): {energy:.2f}")
            st.progress(energy)
            st.caption(f"😊 Mood (Happiness): {v_val:.2f}")
            st.progress(v_val)
            st.caption(f"🌊 Texture (Noisiness): {noise:.2f}")
            st.progress(noise)

        # COLUMN 2: Bio & Top Tracks
        with col2:
            key = st.secrets["lastfm_key"]
            with st.spinner("Fetching biography and track list..."):
                det = get_artist_details(selected, key)
                tracks = get_top_tracks(selected, key)
            
            if det and 'bio' in det: 
                st.info(det['bio']['summary'].split("<a href")[0])
            
            if tracks:
                t_data = [{"Song": t['name'], "Plays": f"{int(t['playcount']):,}", "Link": t.get('url', '#')} for t in tracks]
                st.dataframe(pd.DataFrame(t_data), column_config={"Link": st.column_config.LinkColumn("Link")}, hide_index=True)

    except Exception as e:
        st.error(f"Dashboard Load Error: {e}")

else:
    if df_db.empty:
        st.info("The database is empty! Run bulk_harvester.py to seed data.")