import os
import toml
import datetime
from supabase import create_client

# --- CONFIGURATION ---
# We read from the local secrets file to get the Service Role key
SECRETS_PATH = os.path.join(os.path.dirname(__file__), ".streamlit", "secrets.toml")
BACKUP_DIR = "backups"

def get_supabase_client():
    if not os.path.exists(SECRETS_PATH):
        print(f"❌ Error: {SECRETS_PATH} not found.")
        return None
        
    try:
        secrets = toml.load(SECRETS_PATH)
        url = secrets["supabase"]["url"]
        key = secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        return None

def escape_sql(value):
    """Helper to format Python values into SQL strings."""
    if value is None: 
        return "NULL"
    if isinstance(value, str):
        # Escape single quotes by doubling them (Postgres standard)
        safe_str = value.replace("'", "''")
        return f"'{safe_str}'"
    return str(value)

def run_backup():
    print("--- 💾 STARTING DATABASE DUMP ---")
    
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)

    client = get_supabase_client()
    if not client: return

    # Create a filename with a timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{BACKUP_DIR}/tu_nerr_seed_{timestamp}.sql"

    try:
        # 1. FETCH DATA
        # Note: Supabase API defaults to 1000 rows. If you grow larger, 
        # we will need to add pagination logic here later.
        print("   ⬇️  Downloading Artists...")
        artists = client.table("artists").select("*").execute().data
        
        print("   ⬇️  Downloading Tracks...")
        tracks = client.table("tracks").select("*").execute().data

        print(f"   ✅ Extracted {len(artists)} artists and {len(tracks)} tracks.")

        # 2. WRITE SQL FILE
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"-- tu-nerr Database Backup\n")
            f.write(f"-- Generated: {datetime.datetime.now()}\n")
            f.write(f"-- Contains: {len(artists)} Artists, {len(tracks)} Tracks\n\n")
            
            # A. The Nuclear Option (Included in the backup)
            f.write("-- 1. RESET TABLES (Wipe before restore)\n")
            f.write("TRUNCATE TABLE tracks, artists RESTART IDENTITY CASCADE;\n\n")
            
            # B. Restore Artists (Must be first due to Foreign Keys)
            f.write("-- 2. RESTORE ARTISTS\n")
            for a in artists:
                # Explicitly define column order to match your Schema
                cols = [
                    "id", "name", "genre", "listeners", "image_url", "first_release_year", 
                    "valence", "tag_energy", 
                    "avg_bpm", "avg_brightness", "avg_noisiness", "avg_warmth", "avg_complexity"
                ]
                
                # Extract values safely
                vals = [escape_sql(a.get(c)) for c in cols]
                
                # Write the INSERT statement
                # ON CONFLICT DO NOTHING prevents crashes if you run it twice
                sql = f"INSERT INTO artists ({', '.join(cols)}) VALUES ({', '.join(vals)}) ON CONFLICT (id) DO NOTHING;\n"
                f.write(sql)
            
            # C. Restore Tracks
            f.write("\n-- 3. RESTORE TRACKS\n")
            for t in tracks:
                cols = [
                    "artist_id", "title", "preview_url", 
                    "bpm", "brightness", "noisiness", "warmth", "complexity"
                ]
                vals = [escape_sql(t.get(c)) for c in cols]
                
                sql = f"INSERT INTO tracks ({', '.join(cols)}) VALUES ({', '.join(vals)});\n"
                f.write(sql)

        print(f"🎉 Backup saved to: {filename}")
        print("   👉 To Restore: Open file, Copy All, Paste into Supabase SQL Editor.")

    except Exception as e:
        print(f"❌ Backup Failed: {e}")

if __name__ == "__main__":
    run_backup()