import streamlit as st
import pandas as pd
import time
import random 

# --- IMPORT MODULES ---
from src.db_model import fetch_all_artists_df, delete_artist
from src.api_handler import get_similar_artists, get_top_artists_by_genre, process_artist, get_artist_details, get_top_tracks, get_deezer_data, get_deezer_preview
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
    session_data_names = set() 
        
    for i, artist in enumerate(targets):
        prog.progress((i + 1) / len(targets))
        
        if artist.strip().lower() in session_data_names: continue
            
        # Process: Checks DB -> Fetches API -> Analyzes Audio -> Saves to SQL
        data = process_artist(artist, df_db, api_key, session_data_names)
        
        if data: 
            session_data.append(data)
            session_data_names.add(data['Artist'].lower())
    
    if session_data:
        st.session_state.view_df = pd.DataFrame(session_data).drop_duplicates(subset=['Artist'])
        st.session_state.center_node = center if mode == "Artist" else None
        st.session_state.view_source = "Social"
        return True
    return False

# --- FIX: ISOLATE INITIAL DISCOVERY RUNNER ---
def set_initial_view(df_db):
    """Executes the FIRST discovery run (random cluster) and forces a page refresh."""
    if not df_db.empty:
        # 1. Select a random artist to be the anchor
        sample_df = df_db.sample(min(len(df_db), 30))
        random_center = sample_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
        
        # 2. RUN DISCOVERY: Force the network to fetch neighbors for the random anchor
        try:
            key = st.secrets["lastfm_key"]
            # We don't need to check success, we just need to trigger the run
            run_discovery(random_center, "Artist", key, df_db)
            st.session_state.initial_run_complete = True
            st.rerun() # Forces Streamlit to jump to the display stage
        except Exception as e:
             st.error(f"Initial Load Discovery Failed: {e}")
             st.session_state.view_df = pd.DataFrame() # Fallback

# --- 1. INITIAL LOAD ---
try:
    df_db = fetch_all_artists_df()
except Exception as e:
    st.error(f"FATAL DB ERROR: Failed to load initial data. Details: {e}")
    st.stop()


# --- 2. INITIAL VIEW STATE CHECK ---
# Check 1: Has the app loaded before? (Prevents infinite loop)
if 'initial_run_complete' not in st.session_state:
    st.session_state.initial_run_complete = False

# Check 2: If the session has no data AND the initial run hasn't finished, run the setup.
if 'view_df' not in st.session_state and not st.session_state.initial_run_complete:
    if not df_db.empty:
        # Execute the one-time initial discovery
        set_initial_view(df_db)
    else:
        st.session_state.view_df = pd.DataFrame()


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
                    # Before running discovery, clear the cache to ensure we get a fresh DB load during the run
                    st.cache_data.clear() 
                    if run_discovery(query, mode, key, df_db): st.rerun()
                    else: st.error("No data found.")
                except Exception as e: st.error(f"Search error: {e}")
    
    st.divider()
    if st.button("🔄 Reset Map"):
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
disp_df = st.session_state.view_df
center = st.session_state.get('center_node', 'Unknown')
source = st.session_state.get('view_source', 'Social')

st.subheader(f"🔭 System: {center if center else 'Universal Galaxy'} ({source} Connection)")

selected = None
if not disp_df.empty:
    # Render the graph via the visuals module
    selected = render_graph(disp_df, center, source)

# --- 5. DASHBOARD ---
if selected:
    st.divider()
    c1, c2 = st.columns([3, 1])
    with c1: st.header(f"🤿 {selected}")
    with c2:
        # 1. TRAVEL BUTTON
        if st.button("🔭 Travel Here (Social)", type="primary"):
            run_discovery(selected, "Artist", st.secrets["lastfm_key"], df_db)
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
            # Fallback: Fetch live minimal data
            d_live = get_deezer_data(selected)
            r = {
                'Image URL': d_live['image'] if d_live else '',
                'Audio_BPM': 0, 'Audio_Brightness': 0.5, 'Tag_Energy': 0.5, 'Valence': 0.5,
                'Monthly Listeners': d_live['listeners'] if d_live else 0, 'Genre': 'Unknown'
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
            
            st.metric("Fans", f"{int(r['Monthly Listeners']):,}")
            st.metric("BPM", int(r.get('Audio_BPM', 0)))
            
            st.caption(f"🔥 Energy (Intensity): {energy:.2f}")
            st.progress(energy)
            st.caption(f"😊 Mood (Happiness): {v_val:.2f}")
            st.progress(v_val)

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
                st.dataframe(pd.DataFrame(t_data), column_config={"Link": st.column_config.LinkColumn("Listen")}, hide_index=True)

    except Exception as e:
        st.error(f"Dashboard Load Error: {e}")

else:
    if df_db.empty:
        st.info("The database is empty! Run bulk_harvester.py to seed data.")