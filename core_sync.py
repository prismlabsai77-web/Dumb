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
except Exception as e:
    print(f"❌ Supabase Connection Failed: {e}")

def get_magnet(movie_title):
    """
    Improved Magnet Search:
    Uses browser headers to avoid being blocked by YTS anti-bot filters.
    """
    # Clean title to keep only letters and numbers
    clean_title = "".join(c for c in movie_title if c.isalnum() or c.isspace()).strip()
    
    # PERFECTION: Browser Headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        # Search YTS via query_term
        search_url = "https://yts.mx/api/v2/list_movies.json"
        params = {"query_term": clean_title, "limit": 1}
        
        response = requests.get(search_url, headers=headers, params=params, timeout=15)
        
        if response.status_code == 200:
            res = response.json()
            if res.get('data', {}).get('movie_count', 0) > 0:
                movie_data = res['data']['movies'][0]
                torrent_hash = movie_data['torrents'][0]['hash']
                # Create the Magnet Link
                magnet = f"magnet:?xt=urn:btih:{torrent_hash}&dn={clean_title.replace(' ', '+')}"
                return magnet
        else:
            print(f"⚠️ YTS API error code: {response.status_code} for {clean_title}")
            
    except Exception as e:
        print(f"⚠️ Search error for {movie_title}: {e}")
    
    return None

def run_hivescout():
    print(f"--- 🚀 Starting HiveStream Factory: Bot-Bypass Mode ---")

    # TEST: Inception must work now!
    print(f"🧪 Testing searcher with 'Inception'...")
    test_link = get_magnet("Inception")
    if test_link:
        print(f"✅ Test Passed: Link Found!")
    else:
        print(f"❌ Test Failed: Still blocked. Check YTS status.")

    for page in range(1, 6):
        print(f"📄 Scraping TMDB Page {page}...")
        tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={TMDB_KEY}&page={page}"
        
        try:
            response = requests.get(tmdb_url).json()
            movies = response.get('results', [])
            
            for movie in movies:
                title = movie.get('title')
                tmdb_id = str(movie.get('id'))
                poster = f"https://image.tmdb.org/t/p/w500{movie.get('poster_path')}"
                
                # Scout for Magnet
                magnet = get_magnet(title)
                
                data = {
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "poster_url": poster,
                    "magnet_url": magnet,
                    "status": "ready" if magnet else "searching",
                    "last_verified": "now()"
                }

                try:
                    supabase.table("movie_mappings").upsert(data).execute()
                    icon = "🔗" if magnet else "⌛"
                    print(f"{icon} Synced: {title}")
                except Exception as e:
                    print(f"❌ DB Error: {e}")
                
                time.sleep(0.5) # Politeness delay

        except Exception as e:
            print(f"❌ Page {page} failed: {e}")

    print("--- 🏁 Deep Scout Complete ---")

if __name__ == "__main__":
    run_hivescout()
