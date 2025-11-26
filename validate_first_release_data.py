import requests
import json

def get_headers():
    # Mimic the browser headers used in the real app to avoid 403s
    return {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

def test_release_date_logic(artist_name):
    print(f"\n--- 🕵️‍♀️ Investigating: {artist_name} ---")
    
    # 1. Search for Artist
    print("1. Searching Deezer API for artist ID...")
    try:
        url = f"https://api.deezer.com/search/artist?q={artist_name}"
        resp = requests.get(url, headers=get_headers(), timeout=5).json()
        
        if not resp.get('data'):
            print("❌ Artist not found on Deezer.")
            return
            
        artist = resp['data'][0]
        artist_id = artist['id']
        print(f"   ✅ Found: {artist['name']} (ID: {artist_id})")
        
    except Exception as e:
        print(f"❌ Error during search: {e}")
        return

    # 2. Fetch Albums
    print(f"2. Fetching albums for Artist ID {artist_id}...")
    try:
        # Note: We request 'limit=999' to try and get the full discography
        alb_url = f"https://api.deezer.com/artist/{artist_id}/albums?limit=999"
        alb_resp = requests.get(alb_url, headers=get_headers(), timeout=5).json()
        
        if not alb_resp.get('data'):
            print("❌ No albums found for this artist.")
            return
            
        albums = alb_resp['data']
        print(f"   ✅ Found {len(albums)} albums/singles.")
        
    except Exception as e:
        print(f"❌ Error fetching albums: {e}")
        return

    # 3. Analyze Dates
    print("3. Analyzing Release Dates...")
    valid_dates = []
    
    print("   --- Raw Data Sample (First 3) ---")
    for alb in albums[:3]:
        print(f"   - {alb.get('title', 'Unknown')}: {alb.get('release_date', 'No Date')}")
    
    for album in albums:
        date = album.get('release_date')
        if date:
            valid_dates.append(date)
            
    if not valid_dates:
        print("❌ No valid release dates found in album data.")
        return
        
    valid_dates.sort()
    first_release = valid_dates[0]
    first_year = int(first_release[:4])
    
    print(f"\n🎯 CONCLUSION:")
    print(f"   Earliest Date Found: {first_release}")
    print(f"   Extracted Year: {first_year}")
    
    # Verification logic check
    if first_year > 0:
        print("   ✅ Logic Check: PASS (This should save correctly)")
    else:
        print("   ❌ Logic Check: FAIL (Year is invalid)")

if __name__ == "__main__":
    while True:
        user_input = input("\nEnter band name to test (or 'q' to quit): ")
        if user_input.lower() == 'q':
            break
        test_release_date_logic(user_input)