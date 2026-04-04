from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from schemas import Camera

# AZ511 required headers
pythonheaders = {
    "Accept": "application/json",
    "User-Agent": "SENTINEL/1.0"
}

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "cameras_cache.json"

MIN_LAT = 33.36
MAX_LAT = 33.46
MIN_LNG = -111.99
MAX_LNG = -111.89

FALLBACK_CAMERAS = [
    {"cam_id": "sim_001", "label": "Mill Ave / University Dr", "lat": 33.4192, "lng": -111.9340},
    {"cam_id": "sim_002", "label": "Apache Blvd / Rural Rd", "lat": 33.4150, "lng": -111.9000},
    {"cam_id": "sim_003", "label": "Tempe Town Lake East", "lat": 33.4280, "lng": -111.9200},
    {"cam_id": "sim_004", "label": "ASU Campus / Stadium", "lat": 33.4264, "lng": -111.9325},
    {"cam_id": "sim_005", "label": "Broadway / McClintock", "lat": 33.4050, "lng": -111.9100},
    {"cam_id": "sim_006", "label": "Southern Ave / Rural Rd", "lat": 33.3800, "lng": -111.9300},
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


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _inside_tempe(lat: float, lng: float) -> bool:
    return MIN_LAT <= lat <= MAX_LAT and MIN_LNG <= lng <= MAX_LNG


def _fallback_records() -> list[dict[str, Any]]:
    return [
        Camera(
            cam_id=item["cam_id"],
            lat=item["lat"],
            lng=item["lng"],
            label=item["label"],
            stream_url="",
            type="snapshot",
            refresh_sec=60,
            live=False,
            _simulated=True,
        ).to_dict()
        for item in FALLBACK_CAMERAS
    ]


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("cameras", "Camera", "value", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def fetch_cameras() -> list[dict[str, Any]]:
    try:

        response = requests.get("https://az511.com/api/v2/get/camera", headers=pythonheaders, timeout=10)
        if response.status_code != 200:
            _log(f"[{datetime.now().strftime('%H:%M:%S')}] az511 warning: endpoint returned {response.status_code}, using fallback")
            raise Exception("AZ511 non-200")
        rows = _extract_rows(response.json())

        cameras: list[dict[str, Any]] = []
        for row in rows:
            lat = _coerce_float(row.get("lat") or row.get("latitude") or row.get("Latitude"))
            lng = _coerce_float(
                row.get("lon") or row.get("lng") or row.get("longitude") or row.get("Longitude")
            )
            if lat is None or lng is None:
                continue
            if not _inside_tempe(lat, lng):
                continue

            cam_id = row.get("id") or row.get("cameraId") or row.get("CameraId") or len(cameras)
            label = row.get("name") or row.get("description") or row.get("location") or "AZ511 Camera"
            stream_url = row.get("imageUrl") or row.get("url") or row.get("image") or ""

            cameras.append(
                Camera(
                    cam_id=f"az511_{cam_id}",
                    lat=lat,
                    lng=lng,
                    label=str(label),
                    stream_url=str(stream_url),
                    type="snapshot",
                    refresh_sec=60,
                    live=False,
                    _simulated=False,
                ).to_dict()
            )

        if not cameras:
            cameras = _fallback_records()

        _write_cache(cameras)
        _log(f"cameras updated: {len(cameras)} records")
        return cameras

    except Exception as err:
        cached = _load_cache()
        _log(f"cameras error: {err}")
        if cached is not None:
            return cached
        fallback = _fallback_records()
        _write_cache(fallback)
        return fallback


if __name__ == "__main__":
    rows = fetch_cameras()
    _log(f"cameras ready: {len(rows)} records")
