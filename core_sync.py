import os
import requests
import time
from supabase import create_client

# Config
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
TMDB_KEY = os.environ.get("TMDB_KEY")
supabase = create_client(URL, KEY)

def harvest_21st_century():
    print("📅 Target: 21st Century (2000 - 2026)")
    
    # We iterate through years to bypass the 500-page limit
    for year in range(2000, 2027):
        print(f"🔎 Harvesting Year: {year}")
        
        # We fetch the top 10 pages (200 movies) per year
        # This gives us the ~5,400 most important movies of the century
        for page in range(1, 11):
            url = (
                f"https://api.themoviedb.org/3/discover/movie?"
                f"api_key={TMDB_KEY}&"
                f"primary_release_year={year}&"
                f"sort_by=popularity.desc&"
                f"page={page}"
            )
            
            try:
                response = requests.get(url).json()
                movies = response.get('results', [])
                
                if not movies:
                    break
                
                batch = []
                for m in movies:
                    batch.append({
                        "tmdb_id": m['id'],
                        "title": m['title'],
                        "popularity": m['popularity'],
                        "release_date": m.get('release_date'),
                        "poster_url": f"https://image.tmdb.org/t/p/w500{m.get('poster_path')}",
                        "overview": m.get('overview')
                    })
                
                # Bulk push to Supabase
                supabase.table("movie_library").upsert(batch).execute()
                print(f"✅ Year {year} | Page {page} synced.")
                time.sleep(0.2) # API respect
                
            except Exception as e:
                print(f"❌ Error in {year} P{page}: {e}")
                continue

if __name__ == "__main__":
    harvest_21st_century()
