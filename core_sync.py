import os
import requests
import time
from supabase import create_client

# Config
URL = os.environ.get("SUPABASE_URL")
KEY = os.environ.get("SUPABASE_KEY")
TMDB_KEY = os.environ.get("TMDB_KEY")
supabase = create_client(URL, KEY)

def get_last_sync_state(year):
    """Checks the DB to see where we left off for this year."""
    res = supabase.table("sync_progress").select("*").eq("year", year).execute()
    if res.data:
        return res.data[0]
    return None

def update_sync_state(year, page, total_pages, completed=False):
    """Saves the current progress to the DB."""
    data = {
        "year": year,
        "last_page_synced": page,
        "total_pages_available": total_pages,
        "is_completed": completed,
        "updated_at": "now()"
    }
    supabase.table("sync_progress").upsert(data).execute()

def harvest_21st_century_industrial():
    print("🏗️ Starting Industrial Harvest: 2000 - 2026")
    
    for year in range(2000, 2027):
        state = get_last_sync_state(year)
        
        if state and state['is_completed']:
            print(f"⏩ Year {year} already fully synced. Skipping.")
            continue
            
        current_page = state['last_page_synced'] + 1 if state else 1
        total_pages = 500 # Default TMDB limit
        
        print(f"🔎 Processing Year: {year} (Starting from Page {current_page})")
        
        while current_page <= total_pages:
            url = (
                f"https://api.themoviedb.org/3/discover/movie?"
                f"api_key={TMDB_KEY}&"
                f"primary_release_year={year}&"
                f"sort_by=popularity.desc&"
                f"page={current_page}"
            )
            
            try:
                response = requests.get(url, timeout=15)
                
                # Handle Rate Limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print(f"⏳ Rate limited. Sleeping for {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                data = response.json()
                movies = data.get('results', [])
                total_pages = min(data.get('total_pages', 500), 500)

                if not movies:
                    update_sync_state(year, current_page, total_pages, True)
                    break
                
                # Prepare Batch
                batch = []
                for m in movies:
                    batch.append({
                        "tmdb_id": m['id'],
                        "title": m['title'],
                        "popularity": m.get('popularity', 0),
                        "release_date": m.get('release_date'),
                        "poster_url": f"https://image.tmdb.org/t/p/w500{m.get('poster_path')}" if m.get('poster_path') else None,
                        "status": "active"
                    })
                
                # Bulk Upsert
                supabase.table("movie_library").upsert(batch).execute()
                
                # Progress Update
                is_last = (current_page >= total_pages)
                update_sync_state(year, current_page, total_pages, is_last)
                
                print(f"✅ {year} | Page {current_page}/{total_pages} | Synced {len(batch)} movies")
                
                current_page += 1
                time.sleep(0.25) # Respect TMDB 40 req/sec limit

            except Exception as e:
                print(f"❌ Critical Error on {year} P{current_page}: {e}")
                time.sleep(5) # Cooldown on error
                break

    print("🏁 Industrial Harvest Cycle Complete.")

if __name__ == "__main__":
    harvest_21st_century_industrial()
