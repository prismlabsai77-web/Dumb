import os
import requests
import time
from supabase import create_client

# Setup Environment
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
TMDB_KEY = os.environ.get("TMDB_KEY")

try:
    supabase = create_client(URL, KEY)
    print("✅ Supabase Connection Secure")
except Exception as e:
    print(f"❌ Supabase Connection Failed: {e}")

def run_hivescout():
    print(f"--- 🚀 HiveStream Factory: Metadata Mode ---")

    # We will loop through 10 pages now (200 movies!) 
    # since we aren't slowed down by the failing YTS search.
    for page in range(1, 11):
        print(f"📄 Harvesting TMDB Page {page}...")
        tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={TMDB_KEY}&page={page}"
        
        try:
            response = requests.get(tmdb_url, timeout=10).json()
            movies = response.get('results', [])
            
            for movie in movies:
                title = movie.get('title')
                tmdb_id = str(movie.get('id'))
                # Get the high-quality poster path
                poster_path = movie.get('poster_path')
                poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
                
                # Prepare clean data
                data = {
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "poster_url": poster_url,
                    "status": "active", # Marked active for the App to find
                    "last_verified": "now()"
                }

                try:
                    # Upsert keeps our database fresh without duplicates
                    supabase.table("movie_mappings").upsert(data).execute()
                    print(f"📦 Synced: {title}")
                except Exception as e:
                    print(f"❌ DB Error: {e}")
                
                # Tiny sleep to respect Supabase limits
                time.sleep(0.1)

        except Exception as e:
            print(f"❌ Page {page} failed: {e}")

    print("--- 🏁 Metadata Harvest Complete ---")

if __name__ == "__main__":
    run_hivescout()
