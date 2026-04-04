from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from schemas import Event

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "events_cache.json"

VENUE_CAPACITY = {
    "sun devil stadium": 53599,
    "desert financial arena": 14198,
    "tempe center for the arts": 1000,
    "default": 500,
}

TURNOUT_RATE = {
    "sports": 0.92,
    "concert": 0.82,
    "festival": 0.68,
    "community": 0.45,
    "default": 0.55,
}

FALLBACK_EVENTS = [
    {
        "event_id": "sim_evt_001",
        "name": "Tempe Downtown Community Patrol Briefing",
        "type": "community",
        "lat": 33.4192,
        "lng": -111.9340,
        "start_time": datetime.utcnow().isoformat(),
        "end_time": datetime.utcnow().isoformat(),
        "venue": "Tempe Civic Plaza",
    },
    {
        "event_id": "sim_evt_002",
        "name": "ASU Weekend Sports Event",
        "type": "sports",
        "lat": 33.4264,
        "lng": -111.9325,
        "start_time": datetime.utcnow().isoformat(),
        "end_time": datetime.utcnow().isoformat(),
        "venue": "Sun Devil Stadium",
    },
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


def _infer_event_type(name: str) -> str:
    lowered = (name or "").lower()
    if any(word in lowered for word in ("football", "basketball", "game", "match", "vs")):
        return "sports"
    if any(word in lowered for word in ("concert", "band", "music", "dj", "tour")):
        return "concert"
    if any(word in lowered for word in ("festival", "fair", "market")):
        return "festival"
    return "community"


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _estimate_crowd(venue_name: str, event_type: str) -> int:
    venue_key = (venue_name or "").strip().lower()
    capacity = VENUE_CAPACITY.get(venue_key, VENUE_CAPACITY["default"])
    turnout = TURNOUT_RATE.get(event_type, TURNOUT_RATE["default"])
    return int(round(capacity * turnout))


def _normalize_event(
    event_id: str,
    name: str,
    event_type: str,
    lat: float | None,
    lng: float | None,
    start_time: str,
    end_time: str,
    venue_name: str,
    expected_crowd: int | None,
    simulated: bool,
) -> dict[str, Any]:
    crowd = expected_crowd
    crowd_source = "reported"
    is_simulated = simulated

    if crowd is None:
        crowd = _estimate_crowd(venue_name, event_type)
        crowd_source = "estimated"
        is_simulated = True

    return Event(
        event_id=event_id,
        name=name,
        type=event_type,
        lat=lat,
        lng=lng,
        start_time=start_time,
        end_time=end_time,
        expected_crowd=crowd,
        crowd_source=crowd_source,
        _simulated=is_simulated,
    ).to_dict()


def _pull_eventbrite(token: str | None) -> list[dict[str, Any]]:
    if not token:
        return []

    response = requests.get(
        "https://www.eventbriteapi.com/v3/events/search/",
        params={
            "location.address": "Tempe,AZ",
            "location.within": "10km",
        },
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("events", [])


def _pull_asu_localist() -> list[dict[str, Any]]:
    try:
        response = requests.get(
            "https://asu.edu/api/2/events",
            params={"pp": 50, "days": 3},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("events", [])
        normalized: list[dict[str, Any]] = []
        for row in rows:
            event = row.get("event", {})
            if event:
                normalized.append(event)
        return normalized
    except (requests.exceptions.ConnectionError, requests.exceptions.SSLError):
        _log(f"[{datetime.now().strftime('%H:%M:%S')}] events ASU warning: could not reach ASU events endpoint, skipping")
        return []


def fetch_events_data() -> list[dict[str, Any]]:
    load_dotenv(BASE_DIR.parent / ".env")
    eventbrite_key = os.getenv("EVENTBRITE_API_KEY")

    try:
        merged: list[dict[str, Any]] = []

        try:
            eventbrite_rows = _pull_eventbrite(eventbrite_key)
        except Exception as err:
            _log(f"events Eventbrite warning: {err}")
            eventbrite_rows = []

        time.sleep(0.5)

        for row in eventbrite_rows:
            venue = row.get("venue") or {}
            address = venue.get("address") or {}
            name = (row.get("name") or {}).get("text") or "Unnamed Event"
            event_type = _infer_event_type(name)
            start_time = (row.get("start") or {}).get("utc") or datetime.utcnow().isoformat()
            end_time = (row.get("end") or {}).get("utc") or start_time
            lat = _parse_float(address.get("latitude") or venue.get("latitude"))
            lng = _parse_float(address.get("longitude") or venue.get("longitude"))

            merged.append(
                _normalize_event(
                    event_id=f"evtb_{row.get('id', 'unknown')}",
                    name=name,
                    event_type=event_type,
                    lat=lat,
                    lng=lng,
                    start_time=start_time,
                    end_time=end_time,
                    venue_name=venue.get("name") or row.get("location", {}).get("name", ""),
                    expected_crowd=None,
                    simulated=False,
                )
            )

        try:
            localist_rows = _pull_asu_localist()
        except Exception as err:
            _log(f"events ASU Localist warning: {err}")
            localist_rows = []

        for row in localist_rows:
            name = row.get("title") or "Unnamed ASU Event"
            venue = row.get("venue") or {}
            event_type = _infer_event_type(name)
            start_time = row.get("first_date_occurrence") or datetime.utcnow().isoformat()
            end_time = row.get("last_date_occurrence") or start_time
            lat = _parse_float(venue.get("latitude"))
            lng = _parse_float(venue.get("longitude"))

            merged.append(
                _normalize_event(
                    event_id=f"asu_{row.get('id', 'unknown')}",
                    name=name,
                    event_type=event_type,
                    lat=lat,
                    lng=lng,
                    start_time=start_time,
                    end_time=end_time,
                    venue_name=row.get("location_name") or venue.get("name") or "",
                    expected_crowd=None,
                    simulated=False,
                )
            )

        if not merged:
            merged = [
                _normalize_event(
                    event_id=item["event_id"],
                    name=item["name"],
                    event_type=item["type"],
                    lat=item["lat"],
                    lng=item["lng"],
                    start_time=item["start_time"],
                    end_time=item["end_time"],
                    venue_name=item["venue"],
                    expected_crowd=None,
                    simulated=True,
                )
                for item in FALLBACK_EVENTS
            ]

        _write_cache(merged)
        _log(f"events updated: {len(merged)} records")
        return merged

    except Exception as err:
        cached = _load_cache()
        _log(f"events error: {err}")
        if cached is not None:
            return cached
        fallback = [
            _normalize_event(
                event_id=item["event_id"],
                name=item["name"],
                event_type=item["type"],
                lat=item["lat"],
                lng=item["lng"],
                start_time=item["start_time"],
                end_time=item["end_time"],
                venue_name=item["venue"],
                expected_crowd=None,
                simulated=True,
            )
            for item in FALLBACK_EVENTS
        ]
        _write_cache(fallback)
        return fallback


if __name__ == "__main__":
    rows = fetch_events_data()
    _log(f"events ready: {len(rows)} records")
