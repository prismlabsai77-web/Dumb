import os
import requests
from supabase import create_client

# GitHub will inject these from your Secrets automatically
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")

supabase = create_client(url, key)

def fetch_and_map():
    # 1. Get Trending from TMDB
    tmdb_url = f"https://api.themoviedb.org/3/trending/movie/day?api_key={tmdb_key}"
    response = requests.get(tmdb_url).json()
    
    for movie in response.get('results', []):
        # 2. Logic to find magnets (Insert your search logic here)
        # For now, we'll use a placeholder to test the connection
        data = {
            "tmdb_id": str(movie['id']),
            "title": movie['title'],
            "status": "healthy"
        }
        
        # 3. Upsert to Supabase Honeycomb
        supabase.table("movie_mappings").upsert(data).execute()
        print(f"Mapped: {movie['title']}")

if __name__ == "__main__":
    fetch_and_map()
