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
    'node["amenity"~"school|hospital|place_of_worship"](33.36,-111.99,33.46,-111.89);'
    'node["leisure"="stadium"](33.36,-111.99,33.46,-111.89);'
    'node["building"="university"](33.36,-111.99,33.46,-111.89);'
    ");"
    "out body;"
)


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
            _log(f"risk_areas updated: {len(cached)} records")
            return cached

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

        _write_cache(records)
        _log(f"risk_areas updated: {len(records)} records")
        return records

    except Exception as err:
        cached = _load_cache()
        _log(f"risk_areas error: {err}")
        if cached is not None:
            return cached
        return []


if __name__ == "__main__":
    rows = fetch_risk_areas()
    _log(f"risk_areas ready: {len(rows)} records")
