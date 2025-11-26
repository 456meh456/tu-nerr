import streamlit as st
import pandas as pd
import time

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
    """
    Finds a cluster of artists (center + neighbors) and draws the map.
    Does NOT write to the database (Pure Read Mode).
    """
    targets = []
    
    with st.spinner(f"Scanning: {center}..."):
        if mode == "Artist":
            targets.append(center)
            # Fetch social neighbors
            targets.extend(get_similar_artists(center, api_key, limit=20))
        else:
            targets = get_top_artists_by_genre(center, api_key, limit=20)
    
    targets = list(set(targets))
    
    # --- CRITICAL FIX: Filter targets to only include known artists ---
    # This prevents the app from trying to process new bands or crash the DB.
    
    # 1. Get lowercase target names
    target_lower_names = [t.lower().strip() for t in targets]
    
    # 2. Filter the main database DF to create the session view
    disp_df = df_db[df_db['Artist_Lower'].isin(target_lower_names)].copy()
    
    if disp_df.empty:
        return False
        
    # 3. Set Session State for rendering
    st.session_state.view_df = disp_df
    st.session_state.center_node = center if mode == "Artist" else None
    st.session_state.view_source = "Social"
    
    return True

# --- 1. INITIAL LOAD ---
try:
    # Load data from Supabase (SQL)
    df_db = fetch_all_artists_df()
except Exception as e:
    st.error(f"FATAL DB ERROR: Failed to load initial data. Details: {e}")
    st.stop()

# --- 2. INITIAL VIEW STATE CHECK ---
if 'view_df' not in st.session_state or st.session_state.view_df.empty:
    if not df_db.empty:
        # FIX: Random Sample for initial load (Local Neighborhood View)
        # Randomly select a cluster of bands and anchor them
        
        sample_size = min(len(df_db), 30)
        sample_df = df_db.sample(n=sample_size)
        
        st.session_state.view_df = sample_df
        st.session_state.center_node = sample_df.sort_values('Monthly Listeners', ascending=False).iloc[0]['Artist']
        st.session_state.view_source = "Random Cluster"
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
                # FIX: Run discovery on the current loaded database copy (df_db)
                try:
                    key = st.secrets["lastfm_key"]
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
                # Note: delete_artist is the only function that writes to DB live now
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
        # 1. TRAVEL BUTTON (Now a pure read operation)
        if st.button("🔭 Travel Here (Social)", type="primary"):
            # Clear cache to ensure we use the latest global DB copy
            st.cache_data.clear() 
            if run_discovery(selected, "Artist", st.secrets["lastfm_key"], fetch_all_artists_df()): 
                 st.rerun()
            else: st.error("No data found.")
            
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
        
        # Handle data for dashboard display (all the same logic as before)
        if not row.empty:
            r = row.iloc[0]
            
            col1, col2 = st.columns([1, 2])
            
            # COLUMN 1: Vitals & Audio
            with col1:
                img = r.get('Image URL')
                if img and str(img).startswith("http"): st.image(img)
                
                # Metrics (BPM, Energy, Mood)
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

            # COLUMN 2: Bio & Tracks
            with col2:
                key = st.secrets["lastfm_key"]
                with st.spinner("Fetching biography and track list..."):
                    det = get_artist_details(selected, key)
                    tracks = get_top_tracks(selected, key)
                
                if det and 'bio' in det: st.info(det['bio']['summary'].split("<a href")[0])
                if tracks:
                    t_data = [{"Song": t['name'], "Link": t.get('url', '#')} for t in tracks]
                    st.dataframe(pd.DataFrame(t_data), column_config={"Link": st.column_config.LinkColumn("Listen")}, hide_index=True)

    except Exception as e:
        st.error(f"Dashboard Load Error: {e}")

else:
    if df_db.empty:
        st.info("The database is empty! Run bulk_harvester.py to seed data.")