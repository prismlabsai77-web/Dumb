import os
import requests
import time
from supabase import create_client

# 1. Setup Environment & Clients
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
TMDB_KEY = os.environ.get("TMDB_KEY")

try:
    supabase = create_client(URL, KEY)
except Exception as e:
    print(f"❌ Supabase Connection Failed: {e}")

def get_magnet(movie_title):
    """
    Searches YTS for a high-quality magnet.
    Cleans the title to improve search 'hit' rate.
    """
    # Remove special characters like ':' or '!' that break search
    clean_title = "".join(c for c in movie_title if c.isalnum() or c.isspace())
    
    try:
        search_url = f"https://yts.mx/api/v2/list_movies.json?query_term={clean_title}&limit=1"
        res = requests.get(search_url, timeout=10).json()
        
        if res.get('data', {}).get('movie_count', 0) > 0:
            movie_data = res['data']['movies'][0]
            # Get the first torrent hash (usually 720p or 1080p)
            torrent_hash = movie_data['torrents'][0]['hash']
            magnet = f"magnet:?xt=urn:btih:{torrent_hash}&dn={clean_title.replace(' ', '+')}"
            return magnet
    except:
        pass
    return None

def run_hivescout():
    print(f"--- 🚀 Starting HiveStream Deep Scout ---")

    # TEST: Verify the searcher works with a known movie
    print(f"🧪 Testing searcher with 'Inception'...")
    test_link = get_magnet("Inception")
    print(f"✅ Test Result: {'Link Found' if test_link else 'Link NULL'}")

    # 2. Loop through 5 pages of Trending Movies (100 movies total)
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
                
                # 3. Scout for Magnet
                magnet = get_magnet(title)
                
                # 4. Prepare Data for Supabase
                data = {
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "poster_url": poster,
                    "magnet_url": magnet,
                    "status": "ready" if magnet else "searching",
                    "last_verified": "now()"
                }

                # 5. Upsert (Update if exists, Insert if new)
                try:
                    supabase.table("movie_mappings").upsert(data).execute()
                    status_icon = "🔗" if magnet else "⏳"
                    print(f"{status_icon} Synced: {title}")
                except Exception as e:
                    print(f"❌ DB Error for {title}: {e}")
                
                # Stay below rate limits (4 requests per second)
                time.sleep(0.25)

        except Exception as e:
            print(f"❌ Page {page} failed: {e}")

    print("--- 🏁 Deep Scout Complete ---")

if __name__ == "__main__":
    run_hivescout()
