# citizen_data.py — fixed
import requests
import json
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def fetch_citizen_incidents(min_lat, min_lon, max_lat, max_lon, limit=50):
    url = "https://citizen.com/api/incident/trending"
    params = {
        "lowerLatitude": min_lat,
        "lowerLongitude": min_lon,
        "upperLatitude": max_lat,
        "upperLongitude": max_lon,
        "fullResponse": "true",
        "limit": limit,
    }

    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as e:
        print(f"[citizen] Fetch error: {e}")
        return []

def normalize_incident(raw):
    """Normalize a raw Citizen incident to WYVERN schema."""
    updates = raw.get("updates", {})
    latest_text = "No updates."
    if updates:
        update_list = sorted(updates.values(), key=lambda x: x.get("ts", 0), reverse=True)
        latest_text = update_list[0].get("text", "")

    return {
        "id": raw.get("key", raw.get("id", "")),
        "source": "citizen",
        "timestamp": raw.get("ts", 0),
        "lat": raw.get("latitude", raw.get("lat")),
        "lng": raw.get("longitude", raw.get("lng")),
        "type": raw.get("category", "unknown"),
        "title": raw.get("title", ""),
        "severity": raw.get("level", 0),
        "description": latest_text,
        "confirmed": raw.get("confirmed", False),
        "_simulated": False,
    }

if __name__ == "__main__":
    MIN_LAT, MIN_LON = 33.30, -112.10
    MAX_LAT, MAX_LON = 33.50, -111.80

    raw = fetch_citizen_incidents(MIN_LAT, MIN_LON, MAX_LAT, MAX_LON)
    normalized = [normalize_incident(r) for r in raw]

    data_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(data_dir, exist_ok=True)
    cache_path = os.path.join(data_dir, "incidents_cache.json")

    with open(cache_path, "w") as f:
        json.dump(normalized, f, indent=2)
    print(f"Cached {len(normalized)} incidents → {cache_path}")

    for inc in sorted(normalized, key=lambda x: x["severity"], reverse=True):
        print(f"[{inc['severity']}] {inc['title']}")
        print(f"    {inc['description'][:80]}")
        print("-" * 40)