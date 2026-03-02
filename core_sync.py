import os
import requests
import time
from supabase import create_client

# Load Secrets
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")

def run_hivescout():
    print(f"--- 🚀 Starting HiveStream Factory (Rate Limit Protected) ---")
    
    try:
        supabase = create_client(url, key)
        print("✅ Supabase Client Initialized")
    except Exception as e:
        print(f"❌ Supabase Init Error: {e}")
        return

    # TMDB Request
    tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}"
    
    try:
        res = requests.get(tmdb_url)
        # If we hit a rate limit (429), wait 10 seconds and try one more time
        if res.status_code == 429:
            print("⚠️ Rate limit hit. Cooling down for 10s...")
            time.sleep(10)
            res = requests.get(tmdb_url)
            
        if res.status_code != 200:
            print(f"❌ TMDB API Error: {res.status_code} - {res.text}")
            return

        movies = res.json().get('results', [])
        print(f"📦 Found {len(movies)} movies. Processing with throttle...")

        for movie in movies:
            title = movie.get('title', 'Unknown Title')
            tmdb_id = str(movie.get('id'))
            
            data = {
                "tmdb_id": tmdb_id,
                "title": title,
                "status": "active",
                "last_verified": "now()"
            }

            try:
                supabase.table("movie_mappings").upsert(data).execute()
                print(f"✅ Synced: {title}")
                
                # --- PERFECTION: THE THROTTLE ---
                # We wait 0.25 seconds between movies. 
                # This limits us to 4 requests/sec (way below the 40/sec limit).
                time.sleep(0.25) 
                
            except Exception as e:
                print(f"❌ Supabase Error for {title}: {e}")

    except Exception as e:
        print(f"❌ Connection Error: {e}")

    print("--- 🏁 Factory Run Complete ---")

if __name__ == "__main__":
    run_hivescout()
