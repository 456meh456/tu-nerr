import pandas as pd
import gspread
import toml
from google.oauth2.service_account import Credentials

# --- SETUP ---
secrets = toml.load(".streamlit/secrets.toml")
GCP_SECRETS = secrets["gcp_service_account"]

def get_sheet_connection():
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Fix private key line breaks
    private_key_fixed = GCP_SECRETS["private_key"].replace('\\n', '\n')
    creds_info = {**GCP_SECRETS, "private_key": private_key_fixed}
    
    creds = Credentials.from_service_account_info(creds_info, scopes=scope)
    client = gspread.authorize(creds)
    return client.open("tu-nerr-db").sheet1

def clean_database():
    print("🧹 Connecting to Google Sheets...")
    sheet = get_sheet_connection()
    
    # 1. Fetch Data
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    print(f"📉 Current Row Count: {len(df)}")
    
    if df.empty:
        print("✅ Database is already empty.")
        return

    # 2. Normalize and Deduplicate
    # Create a temporary column for lowercase comparison
    df['temp_lower'] = df['Artist'].astype(str).str.strip().str.lower()
    
    # Drop duplicates based on that lowercase name
    df_clean = df.drop_duplicates(subset=['temp_lower'], keep='first')
    
    # Remove the temp column
    df_clean = df_clean.drop(columns=['temp_lower'])
    
    removed_count = len(df) - len(df_clean)
    print(f"🗑️  Found and removing {removed_count} duplicates...")

    if removed_count > 0:
        # 3. Wipe and Rewrite
        print("💾 Updating Cloud Database...")
        sheet.clear() # Wipes everything
        
        # Prepare data for upload (Headers + Rows)
        # update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
        # gspread requires list of lists
        sheet.update(range_name='A1', values=[df_clean.columns.values.tolist()] + df_clean.values.tolist())
        
        print(f"✅ Done! New Row Count: {len(df_clean)}")
    else:
        print("✅ No duplicates found. Database is clean.")

if __name__ == "__main__":
    clean_database()