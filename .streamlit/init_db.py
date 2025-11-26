import sqlite3
import os

DB_FILE = "tunerr.db"

def init_db():
    # Delete old DB if exists to start fresh (Optional - comment out to keep data)
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"🗑️ Deleted old {DB_FILE}")

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. ARTISTS TABLE (The Summary)
    # Stores the aggregated "Vibe" scores used by the App/AI
    c.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            genre TEXT,
            monthly_listeners INTEGER,
            image_url TEXT,
            
            -- Composite Scores (Calculated from Tracks)
            composite_bpm REAL,
            composite_brightness REAL,
            sonic_variance REAL,
            
            -- Legacy/Fallback Scores
            tag_energy REAL,
            valence REAL,
            
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 2. TRACKS TABLE (The Raw Telemetry)
    # Stores the deep analysis for every single song we scan
    c.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist_id INTEGER,
            track_name TEXT,
            preview_url TEXT,
            
            -- Physics Data
            raw_bpm REAL,
            raw_brightness REAL,
            raw_zero_crossing REAL,
            
            -- Advanced Features (Stored as JSON strings)
            raw_mfcc_mean TEXT,
            raw_chroma_mean TEXT,
            
            FOREIGN KEY (artist_id) REFERENCES artists (id)
        )
    ''')

    conn.commit()
    conn.close()
    print(f"✅ Database initialized: {DB_FILE} created with 2 tables.")

if __name__ == "__main__":
    init_db()