import os
import requests
import time
from supabase import create_client

# Load Secrets
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")

def get_magnet(movie_title):
    """Scouts a high-quality magnet link from YTS."""
    try:
        search_url = f"https://yts.mx/api/v2/list_movies.json?query_term={movie_title}&limit=1"
        res = requests.get(search_url, timeout=10).json()
        if res.get('data', {}).get('movie_count', 0) > 0:
            hash = res['data']['movies'][0]['torrents'][0]['hash']
            return f"magnet:?xt=urn:btih:{hash}&dn={movie_title.replace(' ', '+')}"
    except:
        pass
    return None

def run_hivescout():
    print(f"--- 🚀 Starting Deep Scout Factory ---")
    supabase = create_client(url, key)
    
    # PERFECTION: Loop through the first 5 pages (100 movies)
    # Change '6' to '21' if you want 400 movies!
    for page in range(1, 6): 
        print(f"📄 Scraping TMDB Page {page}...")
        tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}&page={page}"
        
        try:
            res = requests.get(tmdb_url).json()
            movies = res.get('results', [])
            
            for movie in movies:
                title = movie.get('title')
                tmdb_id = str(movie.get('id'))
                
                # Check for magnet
                magnet = get_magnet(title)
                
                data = {
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "magnet_url": magnet,
                    "status": "ready" if magnet else "searching",
                    "last_verified": "now()"
                }

                # Push to Supabase
                supabase.table("movie_mappings").upsert(data).execute()
                print(f"✅ Page {page} | Synced: {title}")
                
                # Throttle to stay safe
                time.sleep(0.3) 

        except Exception as e:
            print(f"❌ Error on page {page}: {e}")
            continue

    print("--- 🏁 Deep Scout Complete ---")

if __name__ == "__main__":
    run_hivescout()
