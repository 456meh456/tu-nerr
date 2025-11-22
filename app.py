import streamlit as st
import pandas as pd
import altair as alt  # This is the "Pro" charting tool built into Streamlit

st.set_page_config(layout="wide")
st.title("🎵 My Hybrid Music Map")
st.write("Welcome to the prototype. Hover over any bubble to see the Artist.")

try:
    df = pd.read_csv("ds_0001.csv")
    
    st.subheader("The Music Landscape")

    # The "Pro" Chart Definition
    chart = alt.Chart(df).mark_circle().encode(
        # 1. Position (X and Y)
        x='Valence',
        y='Energy',
        
        # 2. Size and Color
        size='Monthly Listeners',
        color='Genre',
        
        # 3. THE FIX: The Tooltip (Hover) List
        tooltip=['Artist', 'Genre', 'Monthly Listeners', 'Valence', 'Energy']
    ).interactive() # This makes it zoomable and pannable

    # Render the chart
    st.altair_chart(chart, use_container_width=True)

    with st.expander("See raw data"):
        st.dataframe(df)

except FileNotFoundError:
    st.error("⚠️ CSV file not found! Please make sure 'ds_0001.csv' is in the folder.")