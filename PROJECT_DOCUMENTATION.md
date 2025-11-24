🎵 tu-nerr: Master Project Documentation

Version: 9.2 (The Discovery Engine)
Status: Production / Stable
Tech Stack: Python, Streamlit, Google Sheets, Scikit-learn, Librosa

1. Executive Summary

tu-nerr is an AI-powered music discovery engine that visualizes the musical universe as an interactive network graph. Unlike traditional recommenders that rely solely on "Similar Artists" lists, tu-nerr uses a Hybrid AI Engine that combines social tagging (Last.fm), popularity metrics (Deezer), and physical audio analysis (Librosa) to find mathematical relationships between artists based on their "Vibe" (Energy vs. Valence).

The application features a persistent cloud database (Google Sheets) that grows organically as users search, creating a crowdsourced map of music.

2. System Architecture

A. Frontend (The Interface)

Framework: Streamlit (Python-based Web UI).

Visualization: streamlit-agraph.

Solar System View: Centers the selected artist (The Sun) and orbits similar artists (Planets) around it.

Universal Galaxy View: A clustered view of the entire database, grouped by Genre.

Dashboard: A "Deep Dive" panel featuring:

Real-time Artist Stats (Listeners, Genre).

Audio Preview Player (30s MP3).

"Vibe Check" Meters (Energy/Mood).

Top Tracks Table.

B. Backend (The Logic)

API Gateway: Handles requests to Last.fm and Deezer.

AI Engine (sklearn):

Uses K-Nearest Neighbors (KNN) to calculate geometric distance between artists.

Features: Audio_Brightness (Timbre), Valence (Mood), Audio_BPM (Tempo).

Audio Analyst (librosa):

Downloads preview MP3s into memory.

Extracts BPM (Tempo) and Spectral Centroid (Brightness).

C. Data Layer (The Memory)

Storage: Google Sheets (accessed via gspread).

Authentication: Google Service Account (JSON key stored in Streamlit Secrets).

Caching: Streamlit @st.cache_data prevents API rate-limiting and speeds up user interactions.

3. Data Schema

The application maintains a single "Source of Truth" database with the following 8 columns:

Column

Source

Description

Artist

User/API

The standardized band name.

Genre

Last.fm

Top user tag (e.g., "Progressive Metal").

Monthly Listeners

Deezer

Fan count (used for Bubble Size).

Tag_Energy

Internal

Heuristic score (0.0-1.0) based on Genre keywords. Used as fallback if Audio fails.

Valence

Internal

Heuristic score (0.0-1.0) representing Mood (Happy/Sad), derived from Last.fm tags.

Audio_BPM

Librosa

Actual Tempo of the artist's top track.

Audio_Brightness

Librosa

Spectral Centroid (0.0-1.0) representing "Timbre/Intensity". Primary metric for Energy.

Image URL

Deezer

Direct link to the artist's profile picture.

4. Workflows

A. The User "Discovery" Flow

User types "Tool" in the Sidebar.

App checks Database. If missing, it triggers the Harvester.

Harvester:

Fetches Metadata from Last.fm.

Fetches Audio/Image from Deezer.

Calculates Audio Physics (Librosa).

Saves new row to Google Sheet.

App displays "Tool" as the Center Node and fetches 10 "Similar Artists" from Last.fm to populate the orbit.

B. The AI "Neighbor" Flow

User clicks "Tool" on the graph.

User clicks "🤖 AI Neighbors".

System loads the entire database into memory.

System normalizes features (scales Listeners vs Energy vs BPM).

KNN Algorithm finds the 5 mathematical nearest neighbors in n-dimensional space.

Graph updates to show only those 5 matches.

C. The "Bulk Harvester" Flow (Local Utility)

Script: bulk_harvester.py

Purpose: Rapidly populates the database without user interaction.

Logic: "Deep Drill" — Finds neighbors of existing bands, checks for duplicates, and adds them until the database grows by a set factor. Handles rate limiting and SSL bypass automatically.

5. Maintenance & Administration

The "Janitor" (Admin Zone)

Location: Sidebar Expander (Password Protected).

Function: Allows the administrator to delete specific rows from the Google Sheet to remove duplicates or bad data.

The "Reset"

Location: "🔄 Reset Map" Button.

Function: Clears Session State variables (center_node, view_df) to return the user to the global view.

6. Future Roadmap

Mobile Optimization: Refine the graph physics for touch screens.

Spotify Integration: Authenticated user playback.

Advanced AI: Implement "Cluster Analysis" to auto-color genres based on audio similarity rather than text tags.