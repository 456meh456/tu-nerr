🎵 tu-nerr: The Discovery Engine

tu-nerr is an interactive music discovery platform that visualizes the relationship between artists based on their "Vibe" (Energy vs. Valence).

🌟 Features

3D Galaxy Map: Explore the musical universe in three dimensions (Mood, Intensity, Popularity).

Solar System View: Zoom into a specific artist to see their direct network of similar bands.

Deep Dive Dashboard: Click any artist to see their Bio, Top Tracks, and Real-time Stats.

Community Database: Every search adds to a permanent, shared Google Sheet database.

🛠️ Tech Stack

Frontend: Streamlit, Plotly, AgGraph

Backend: Python

Data: Last.fm API (Genre/Mood), Deezer API (Images/Popularity), Google Sheets (Storage)

🚀 How to Run Locally

Clone the repo.

Install dependencies:

pip install -r requirements.txt


Run the app:

streamlit run app.py


🤖 The Harvester Bot

To bulk-load data, run the harvester script locally:

py bulk_harvester.py


(Note: Requires local secrets.toml configuration)