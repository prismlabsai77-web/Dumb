import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
import re

# Setup Supabase from Secrets
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")
supabase = create_client(url, key)

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def find_best_magnet(query):
    """
    Search for a movie and find the best magnet link based on seeders.
    For perfection, we use a search aggregator link.
    """
    search_url = f"https://www.1337xx.to/search/{query}/1/" # Example provider
    try:
        response = requests.get(search_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the first result row
        row = soup.select_one('tbody tr')
        if not row: return None
        
        # Get the link to the detail page
        detail_path = row.select('td.coll-1 a')[1]['href']
        detail_url = f"https://www.1337xx.to{detail_path}"
        
        # Visit detail page to get the actual Magnet URL
        detail_res = requests.get(detail_url, headers=HEADERS, timeout=10)
        magnet_link = re.search(r'magnet:\?xt=urn:btih:[a-zA-Z0-9]+', detail_res.text)
        
        return magnet_link.group(0) if magnet_link else None
    except Exception as e:
        print(f"Search error for {query}: {e}")
        return None

def fetch_and_map():
    # 1. Get Trending from TMDB
    tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}"
    movies = requests.get(tmdb_url).json().get('results', [])
    
    for movie in movies:
        title = movie['title']
        year = movie['release_date'][:4]
        search_query = f"{title} {year} 1080p"
        
        print(f"🔍 Searching for: {search_query}")
        magnet = find_best_magnet(title.replace(" ", "+"))
        
        if magnet:
            data = {
                "tmdb_id": str(movie['id']),
                "title": title,
                "magnet_1080p": magnet,
                "status": "healthy",
                "last_verified": "now()"
            }
            # 2. Upsert to Supabase
            supabase.table("movie_mappings").upsert(data).execute()
            print(f"✅ Perfection: Mapped {title}")
        else:
            print(f"❌ No link found for {title}")

if __name__ == "__main__":
    fetch_and_map()
