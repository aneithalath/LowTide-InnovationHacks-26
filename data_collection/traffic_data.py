from __future__ import annotations

import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from schemas import RoadSegment

# AZ511 required headers
pythonheaders = {
    "Accept": "application/json",
    "User-Agent": "SENTINEL/1.0"
}

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "traffic_cache.json"

TEMPE_INTERSECTIONS = [
    (33.4192, -111.9340, "Mill Ave / University Dr"),
    (33.4150, -111.9000, "Apache Blvd / Rural Rd"),
    (33.4050, -111.9100, "Broadway Rd / McClintock Dr"),
    (33.4280, -111.9400, "Tempe Town Lake / Mill Ave"),
    (33.4220, -111.9340, "ASU Campus / Palm Walk"),
    (33.4300, -111.9300, "Rural Rd / Rio Salado"),
    (33.3900, -111.9800, "Price Rd / US-60"),
    (33.3700, -112.0000, "Elliot Rd / I-10 area"),
    (33.3800, -111.9300, "Southern Ave / Rural Rd"),
    (33.4100, -111.9700, "Broadway / 48th St"),
    (33.3600, -111.9100, "Baseline Rd / McClintock"),
    (33.3500, -111.9300, "Guadalupe Rd / Rural Rd"),
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


def _slug(label: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return f"tomtom_{cleaned}"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def simulated_congestion(hour: int) -> float:
    if 7 <= hour <= 9 or 16 <= hour <= 18:
        return round(random.uniform(0.65, 0.90), 2)
    if 10 <= hour <= 15:
        return round(random.uniform(0.25, 0.45), 2)
    return round(random.uniform(0.05, 0.20), 2)


def _simulated_segment(lat: float, lng: float, label: str) -> dict[str, Any]:
    congestion = simulated_congestion(datetime.now().hour)
    speed_limit = 35
    current_speed = round(speed_limit * (1 - congestion), 2)
    return RoadSegment(
        road_id=_slug(label),
        label=label,
        lat=lat,
        lng=lng,
        speed_limit_mph=speed_limit,
        current_speed_mph=current_speed,
        congestion_score=congestion,
        closed=False,
        _simulated=True,
    ).to_dict()


def _parse_closure_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("events", "Event", "value", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _is_maricopa(event: dict[str, Any]) -> bool:
    blob = " ".join(
        str(event.get(field, ""))
        for field in ("county", "County", "description", "Description", "location", "Location")
    ).lower()
    return "maricopa" in blob or "tempe" in blob


def _is_closed(event: dict[str, Any]) -> bool:
    blob = " ".join(
        str(event.get(field, ""))
        for field in ("eventType", "event_type", "status", "Status", "description", "Description")
    ).lower()
    return "close" in blob or "closure" in blob or "blocked" in blob


def _extract_float(event: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = event.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _closure_to_segment(event: dict[str, Any], fallback_idx: int) -> dict[str, Any]:
    lat = _extract_float(event, "lat", "latitude", "Latitude")
    lng = _extract_float(event, "lon", "lng", "longitude", "Longitude")
    label = str(event.get("location") or event.get("Location") or event.get("description") or "AZ511 closure")

    if lat is None or lng is None:
        lat, lng, _ = TEMPE_INTERSECTIONS[fallback_idx % len(TEMPE_INTERSECTIONS)]

    road_id = f"az511_closure_{fallback_idx}"
    return RoadSegment(
        road_id=road_id,
        label=label[:120],
        lat=lat,
        lng=lng,
        speed_limit_mph=35,
        current_speed_mph=0.0,
        congestion_score=1.0,
        closed=True,
        _simulated=False,
    ).to_dict()


def fetch_traffic_data() -> list[dict[str, Any]]:
    load_dotenv(BASE_DIR.parent / ".env")
    tomtom_key = os.getenv("TOMTOM_API_KEY")

    try:
        segments: list[dict[str, Any]] = []
        quota_hit = False

        for idx, (lat, lng, label) in enumerate(TEMPE_INTERSECTIONS):
            if not tomtom_key or quota_hit:
                segments.append(_simulated_segment(lat, lng, label))
                continue

            try:
                response = requests.get(
                    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json",
                    params={"point": f"{lat},{lng}", "key": tomtom_key},
                    timeout=10,
                )

                if response.status_code == 429:
                    quota_hit = True
                    segments.append(_simulated_segment(lat, lng, label))
                    _log("traffic warning: TomTom quota hit, simulating remaining intersections")
                    continue

                response.raise_for_status()
                payload = response.json().get("flowSegmentData", {})

                free_flow_speed = float(payload.get("freeFlowSpeed") or payload.get("currentSpeed") or 35.0)
                current_speed = float(payload.get("currentSpeed") or free_flow_speed)
                speed_limit = int(round(free_flow_speed)) if free_flow_speed > 0 else 35
                congestion = _clamp(1 - (current_speed / max(free_flow_speed, 1)), 0.0, 1.0)

                segments.append(
                    RoadSegment(
                        road_id=_slug(label),
                        label=label,
                        lat=lat,
                        lng=lng,
                        speed_limit_mph=speed_limit,
                        current_speed_mph=round(current_speed, 2),
                        congestion_score=round(congestion, 2),
                        closed=False,
                        _simulated=False,
                    ).to_dict()
                )
            except Exception as err:
                _log(f"traffic TomTom warning: {err}")
                segments.append(_simulated_segment(lat, lng, label))

            if idx < len(TEMPE_INTERSECTIONS) - 1:
                time.sleep(0.5)


        try:
            closures_response = requests.get(
                "https://az511.com/api/v2/get/event",
                headers=pythonheaders,
                timeout=10
            )
            if closures_response.status_code != 200:
                _log(f"[{datetime.now().strftime('%H:%M:%S')}] az511 warning: endpoint returned {closures_response.status_code}, using fallback")
                raise Exception("AZ511 non-200")
            closure_events = _parse_closure_rows(closures_response.json())
            closure_idx = 0
            for event in closure_events:
                if not _is_maricopa(event):
                    continue
                if not _is_closed(event):
                    continue
                closure_idx += 1
                segments.append(_closure_to_segment(event, closure_idx))
        except Exception as err:
            _log(f"traffic AZ511 warning: {err}")

        _write_cache(segments)
        _log(f"traffic updated: {len(segments)} records")
        return segments

    except Exception as err:
        cached = _load_cache()
        _log(f"traffic error: {err}")
        if cached is not None:
            return cached
        simulated = [_simulated_segment(lat, lng, label) for lat, lng, label in TEMPE_INTERSECTIONS]
        _write_cache(simulated)
        return simulated


if __name__ == "__main__":
    rows = fetch_traffic_data()
    _log(f"traffic ready: {len(rows)} records")
