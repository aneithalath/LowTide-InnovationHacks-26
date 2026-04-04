from __future__ import annotations

import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from schemas import HistoricalCell

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "historical_cache.json"

ARCGIS_URL = (
    "https://services.arcgis.com/lQySeXwbBg53XWDi/ArcGIS/rest/services/"
    "Calls_For_Service/FeatureServer/0/query"
)
PAGE_SIZE = 2000
MAX_RECORDS = 20000
GRID_SIZE = 0.005

SIMULATED_HISTORICAL_CELLS = [
    {"cell_id": "grid_33.420_-111.934", "lat": 33.420, "lng": -111.934, "incident_count": 96, "primary_type": "DISTURBANCE", "peak_hours": [21, 22, 23, 0], "escalation_rate": 0.12, "heatmap_weight": 0.91},
    {"cell_id": "grid_33.424_-111.928", "lat": 33.424, "lng": -111.928, "incident_count": 88, "primary_type": "ASSIST MEDICAL", "peak_hours": [18, 19, 20], "escalation_rate": 0.10, "heatmap_weight": 0.83},
    {"cell_id": "grid_33.400_-111.900", "lat": 33.400, "lng": -111.900, "incident_count": 74, "primary_type": "TRAFFIC STOP", "peak_hours": [16, 17, 18], "escalation_rate": 0.08, "heatmap_weight": 0.70},
    {"cell_id": "grid_33.389_-111.917", "lat": 33.389, "lng": -111.917, "incident_count": 61, "primary_type": "WELFARE CHECK", "peak_hours": [20, 21, 22], "escalation_rate": 0.09, "heatmap_weight": 0.58},
]


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


def _cache_is_fresh(days: int) -> bool:
    if not CACHE_PATH.exists():
        return False
    modified = datetime.fromtimestamp(CACHE_PATH.stat().st_mtime)
    return datetime.now() - modified < timedelta(days=days)


def _floor_grid(value: float) -> float:
    return round(math.floor(value / GRID_SIZE) * GRID_SIZE, 3)


def _extract_call_type(attributes: dict[str, Any]) -> str:
    for key in (
        "FinalCaseType",
        "InitialCaseType",
        "CALL_TYPE",
        "INCIDENT_TYPE",
        "INCIDENT_CATEGORY",
        "NATURE",
        "TYPE",
        "DESCRIPTION",
    ):
        if attributes.get(key):
            return str(attributes[key])
    return "UNKNOWN"


def _extract_priority(attributes: dict[str, Any]) -> str:
    for key in ("Priority", "PRIORITY", "PRIORITY_LEVEL", "CALL_PRIORITY"):
        if attributes.get(key) is not None:
            return str(attributes[key]).strip()
    return ""


def _parse_incident_hour(attributes: dict[str, Any]) -> int:
    hour_value = attributes.get("OccurrenceHour")
    if hour_value is not None:
        try:
            hour = int(hour_value)
            if 0 <= hour <= 23:
                return hour
        except Exception:
            pass

    value = (
        attributes.get("OccurrenceDatetime")
        or attributes.get("INCIDENT_DATE")
        or attributes.get("CALL_RECEIVED")
    )
    if value is None:
        return 0

    try:
        if isinstance(value, (int, float)):
            # ArcGIS dates are often epoch milliseconds.
            if value > 10_000_000_000:
                value = value / 1000.0
            return datetime.utcfromtimestamp(float(value)).hour
        text = str(value)
        text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text).hour
    except Exception:
        return 0


def _fetch_pages() -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []
    offset = 0

    while len(features) < MAX_RECORDS:
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "resultRecordCount": PAGE_SIZE,
            "resultOffset": offset,
            "orderByFields": "OccurrenceDatetime DESC",
            "f": "json",
        }
        response = requests.get(ARCGIS_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()

        page_features = payload.get("features", [])
        if not page_features:
            break

        features.extend(page_features)
        exceeded = bool(payload.get("exceededTransferLimit"))
        if not exceeded:
            break

        offset += PAGE_SIZE
        if offset >= MAX_RECORDS:
            break

    return features[:MAX_RECORDS]


def _aggregate(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cells: dict[tuple[float, float], dict[str, Any]] = {}

    for feature in features:
        geometry = feature.get("geometry") or {}
        attributes = feature.get("attributes") or {}

        lng = geometry.get("x")
        lat = geometry.get("y")
        if lat is None or lng is None:
            continue

        cell_lat = _floor_grid(float(lat))
        cell_lng = _floor_grid(float(lng))
        cell_key = (cell_lat, cell_lng)

        if cell_key not in cells:
            cells[cell_key] = {
                "count": 0,
                "type_counter": Counter(),
                "hour_counter": defaultdict(int),
                "priority_1": 0,
            }

        record = cells[cell_key]
        record["count"] += 1
        record["type_counter"][_extract_call_type(attributes)] += 1
        hour = _parse_incident_hour(attributes)
        record["hour_counter"][hour] += 1

        priority = _extract_priority(attributes).lower()
        if priority in ("1", "p1", "priority 1", "high"):
            record["priority_1"] += 1

    if not cells:
        return []

    max_count = max(entry["count"] for entry in cells.values())
    output: list[dict[str, Any]] = []

    for (lat, lng), entry in cells.items():
        count = entry["count"]
        avg_per_hour = count / 24.0
        peak_hours = sorted(
            hour for hour, total in entry["hour_counter"].items() if total > avg_per_hour
        )

        if not peak_hours and entry["hour_counter"]:
            peak_hours = [max(entry["hour_counter"], key=entry["hour_counter"].get)]

        output.append(
            HistoricalCell(
                cell_id=f"grid_{lat:.3f}_{lng:.3f}",
                lat=lat,
                lng=lng,
                incident_count=count,
                primary_type=entry["type_counter"].most_common(1)[0][0],
                peak_hours=peak_hours,
                escalation_rate=round(entry["priority_1"] / count, 2),
                heatmap_weight=round(count / max_count, 2),
                _simulated=False,
            ).to_dict()
        )

    return output


def _simulated_cells() -> list[dict[str, Any]]:
    return [
        HistoricalCell(
            cell_id=item["cell_id"],
            lat=item["lat"],
            lng=item["lng"],
            incident_count=item["incident_count"],
            primary_type=item["primary_type"],
            peak_hours=item["peak_hours"],
            escalation_rate=item["escalation_rate"],
            heatmap_weight=item["heatmap_weight"],
            _simulated=True,
        ).to_dict()
        for item in SIMULATED_HISTORICAL_CELLS
    ]


def fetch_historical_cells() -> list[dict[str, Any]]:
    try:
        if _cache_is_fresh(7):
            cached = _load_cache() or []
            if cached:
                _log(f"historical updated: {len(cached)} records")
                return cached

        features = _fetch_pages()
        cells = _aggregate(features)
        if not cells:
            cells = _simulated_cells()
        _write_cache(cells)
        _log(f"historical updated: {len(cells)} records")
        return cells

    except Exception as err:
        cached = _load_cache()
        _log(f"historical error: {err}")
        if cached is not None:
            return cached
        fallback = _simulated_cells()
        _write_cache(fallback)
        return fallback


if __name__ == "__main__":
    rows = fetch_historical_cells()
    _log(f"historical ready: {len(rows)} records")
