from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from schemas import RiskArea

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "risk_areas_cache.json"

CROWD_MODIFIER = {
    "school": 0.6,
    "hospital": 0.8,
    "stadium": 0.95,
    "place_of_worship": 0.4,
    "university": 0.75,
    "default": 0.5,
}

OVERPASS_QUERY = (
    "[out:json][timeout:25];"
    "(" 
    'node["amenity"~"school|hospital|place_of_worship|theatre|bar|nightclub|community_centre|arts_centre|cinema|library|college|conference_centre|civic_centre|event_venue"](33.36,-111.99,33.46,-111.89);'
    'node["leisure"~"stadium|sports_centre|park|recreation_ground"](33.36,-111.99,33.46,-111.89);'
    'node["building"~"university|public|civic|stadium|theatre|arena"](33.36,-111.99,33.46,-111.89);'
    ");"
    "out body;"
)

# Manual aliases for known high-risk venues/platforms (add more as needed)
MANUAL_RISK_AREA_ALIASES = [
    {
        "name": "Desert Financial Arena",
        "lat": 33.4166,
        "lng": -111.9336,
        "type": "arena",
        "capacity": 14198,
        "aliases": ["DFA", "ASU Sun Devil Athletics", "Desert Financial Arena", "Sun Devil Arena"]
    },
    {
        "name": "Sun Devil Stadium",
        "lat": 33.4269,
        "lng": -111.9327,
        "type": "stadium",
        "capacity": 53599,
        "aliases": ["Sun Devil Stadium", "ASU Stadium", "ASU Sun Devil Stadium"]
    },
    {
        "name": "Downtown Tempe",
        "lat": 33.4265,
        "lng": -111.9400,
        "type": "district",
        "capacity": 10000,
        "aliases": ["Downtown Tempe", "Mill Avenue", "Downtown Tempe, AZ"]
    },
    {
        "name": "Eventbrite Platform",
        "lat": 33.4255,
        "lng": -111.9400,
        "type": "platform",
        "capacity": 5000,
        "aliases": ["Eventbrite", "Meetup", "Facebook events", "Eventbrite Platform"]
    },
    # Add more as needed
]


def _manual_risk_area_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for alias in MANUAL_RISK_AREA_ALIASES:
        records.append(
            RiskArea(
                id=f"manual_{alias['name'].replace(' ', '_').lower()}",
                type=alias["type"],
                name=alias["name"],
                lat=alias["lat"],
                lng=alias["lng"],
                capacity=alias.get("capacity"),
                crowd_modifier=1.0,
                current_status="normal",
                _simulated=True,
            ).to_dict()
        )
    return records


def _merge_manual_risk_areas(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = [row for row in records if isinstance(row, dict)]
    existing_ids = {str(row.get("id") or "").strip() for row in merged}
    existing_names = {str(row.get("name") or "").strip().lower() for row in merged}

    for manual in _manual_risk_area_records():
        manual_id = str(manual.get("id") or "").strip()
        manual_name = str(manual.get("name") or "").strip().lower()
        if manual_id in existing_ids or manual_name in existing_names:
            continue
        merged.append(manual)

    return merged


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache() -> list[dict[str, Any]] | None:
    if not CACHE_PATH.exists():
        return None
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _write_cache(payload: list[dict[str, Any]]) -> None:
    _ensure_data_dir()
    tmp_path = CACHE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, CACHE_PATH)


def _cache_is_fresh(hours: int) -> bool:
    if not CACHE_PATH.exists():
        return False
    modified = datetime.fromtimestamp(CACHE_PATH.stat().st_mtime)
    return datetime.now() - modified < timedelta(hours=hours)


def _map_type(tags: dict[str, Any]) -> str:
    amenity = tags.get("amenity")
    leisure = tags.get("leisure")
    building = tags.get("building")
    if amenity in ("school", "hospital", "place_of_worship"):
        return str(amenity)
    if leisure == "stadium":
        return "stadium"
    if building == "university":
        return "university"
    return "default"


def fetch_risk_areas() -> list[dict[str, Any]]:
    try:
        if _cache_is_fresh(23):
            cached = _load_cache() or []
            merged_cached = _merge_manual_risk_areas(cached)
            if len(merged_cached) != len(cached):
                _write_cache(merged_cached)
            _log(f"risk_areas updated: {len(merged_cached)} records")
            return merged_cached

        response = requests.post(
            "https://overpass-api.de/api/interpreter",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"data": OVERPASS_QUERY},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()

        records: list[dict[str, Any]] = []
        for element in payload.get("elements", []):
            lat = element.get("lat")
            lng = element.get("lon")
            if lat is None or lng is None:
                continue
            tags = element.get("tags", {})
            item_type = _map_type(tags)
            records.append(
                RiskArea(
                    id=f"osm_{element.get('id')}",
                    type=item_type,
                    name=tags.get("name", f"Unnamed {item_type}"),
                    lat=float(lat),
                    lng=float(lng),
                    capacity=None,
                    crowd_modifier=CROWD_MODIFIER.get(item_type, CROWD_MODIFIER["default"]),
                    current_status="normal",
                    _simulated=False,
                ).to_dict()
            )

        merged = _merge_manual_risk_areas(records)
        _write_cache(merged)
        _log(f"risk_areas updated: {len(merged)} records (including manual aliases)")
        return merged

    except Exception as err:
        cached = _load_cache()
        _log(f"risk_areas error: {err}")
        if cached is not None:
            return cached
        return []


if __name__ == "__main__":
    rows = fetch_risk_areas()
    _log(f"risk_areas ready: {len(rows)} records")
