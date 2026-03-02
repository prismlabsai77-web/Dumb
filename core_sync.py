import os
import requests
import libtorrent as lt
import time
from supabase import create_client

# Load Secrets
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")

print(f"--- 🚀 Starting HiveStream Factory ---")

try:
    supabase = create_client(url, key)
    print("✅ Supabase Client Initialized")
except Exception as e:
    print(f"❌ Supabase Init Error: {e}")

def run_hivescout():
    # 1. TMDB Check
    print(f"📡 Requesting trending movies from TMDB...")
    tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}"
    res = requests.get(tmdb_url)
    
    if res.status_code != 200:
        print(f"❌ TMDB API Error: {res.status_code} - {res.text}")
        return

    movies = res.json().get('results', [])
    print(f"📦 Found {len(movies)} movies in trending list.")

    for movie in movies:
        title = movie['title']
        tmdb_id = str(movie['id'])
        
        # 2. Search Placeholder (We will refine this next)
        print(f"🔍 Processing: {title} (ID: {tmdb_id})")
        
        # Test Data to verify connection
        data = {
            "tmdb_id": tmdb_id,
            "title": title,
            "status": "active",
            "last_verified": "now()"
        }

        try:
            # 3. Push to Supabase
            response = supabase.table("movie_mappings").upsert(data).execute()
            print(f"✅ Saved to Supabase: {title}")
        except Exception as e:
            print(f"❌ Supabase Save Error for {title}: {e}")

if __name__ == "__main__":
    run_hivescout()
    print("--- 🏁 Factory Run Complete ---")
