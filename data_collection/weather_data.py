from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from schemas import WeatherSnapshot

TEMPE_LAT = 33.4255
TEMPE_LNG = -111.9400
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "weather_cache.json"


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache() -> dict[str, Any] | None:
    if not CACHE_PATH.exists():
        return None
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _write_cache(payload: dict[str, Any]) -> None:
    _ensure_data_dir()
    tmp_path = CACHE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, CACHE_PATH)


def _simulated_snapshot() -> dict[str, Any]:
    return WeatherSnapshot(
        timestamp=datetime.now(timezone.utc).isoformat(),
        lat=TEMPE_LAT,
        lng=TEMPE_LNG,
        temperature_f=85.0,
        feels_like_f=87.0,
        humidity_pct=18,
        wind_speed_mph=7.0,
        visibility_miles=10.0,
        conditions="clear sky",
        alert_type=None,
        alert_description=None,
        _simulated=True,
    ).to_dict()


def _parse_alert(nws_payload: dict[str, Any]) -> tuple[str | None, str | None]:
    features = nws_payload.get("features", [])
    if not features:
        return None, None

    properties = features[0].get("properties", {})
    event = properties.get("event")
    description = properties.get("description")
    return event, description


def fetch_weather_snapshot() -> dict[str, Any]:
    load_dotenv(BASE_DIR.parent / ".env")
    openweather_key = os.getenv("OPENWEATHER_API_KEY")
    nws_user_agent = os.getenv("NWS_USER_AGENT", "SENTINEL/1.0 unknown@example.com")

    try:
        alert_type = None
        alert_description = None
        nws_ok = False

        try:
            nws_response = requests.get(
                "https://api.weather.gov/alerts/active",
                params={"point": f"{TEMPE_LAT},{TEMPE_LNG}"},
                headers={"User-Agent": nws_user_agent, "Accept": "application/geo+json"},
                timeout=10,
            )
            nws_response.raise_for_status()
            alert_type, alert_description = _parse_alert(nws_response.json())
            nws_ok = True
        except Exception as err:
            _log(f"weather NWS warning: {err}")

        weather_payload: dict[str, Any] | None = None
        owm_ok = False
        if openweather_key:
            try:
                owm_response = requests.get(
                    "https://api.openweathermap.org/data/2.5/weather",
                    params={
                        "lat": TEMPE_LAT,
                        "lon": TEMPE_LNG,
                        "appid": openweather_key,
                        "units": "imperial",
                    },
                    timeout=10,
                )
                owm_response.raise_for_status()
                weather_payload = owm_response.json()
                owm_ok = True
            except Exception as err:
                _log(f"weather OpenWeather warning: {err}")

        if not nws_ok and not owm_ok:
            cached = _load_cache()
            if cached:
                _log("weather fallback: cache")
                return cached
            simulated = _simulated_snapshot()
            _write_cache(simulated)
            _log("weather updated: 1 record")
            return simulated

        if weather_payload:
            main = weather_payload.get("main", {})
            wind = weather_payload.get("wind", {})
            visibility = weather_payload.get("visibility", 16093.4) / 1609.34
            conditions = ""
            if weather_payload.get("weather"):
                conditions = weather_payload["weather"][0].get("description", "")

            snapshot = WeatherSnapshot(
                timestamp=datetime.now(timezone.utc).isoformat(),
                lat=TEMPE_LAT,
                lng=TEMPE_LNG,
                temperature_f=float(main.get("temp", 85.0)),
                feels_like_f=float(main.get("feels_like", 87.0)),
                humidity_pct=int(main.get("humidity", 18)),
                wind_speed_mph=float(wind.get("speed", 7.0)),
                visibility_miles=round(float(visibility), 2),
                conditions=conditions or "clear sky",
                alert_type=alert_type,
                alert_description=alert_description,
                _simulated=False,
            ).to_dict()
        else:
            # Keep alert information even when OpenWeather is missing.
            snapshot = WeatherSnapshot(
                timestamp=datetime.now(timezone.utc).isoformat(),
                lat=TEMPE_LAT,
                lng=TEMPE_LNG,
                temperature_f=85.0,
                feels_like_f=87.0,
                humidity_pct=18,
                wind_speed_mph=7.0,
                visibility_miles=10.0,
                conditions="clear sky",
                alert_type=alert_type,
                alert_description=alert_description,
                _simulated=True,
            ).to_dict()

        _write_cache(snapshot)
        _log("weather updated: 1 record")
        return snapshot

    except Exception as err:
        cached = _load_cache()
        _log(f"weather error: {err}")
        if cached:
            return cached
        simulated = _simulated_snapshot()
        _write_cache(simulated)
        return simulated


if __name__ == "__main__":
    result = fetch_weather_snapshot()
    _log(f"weather snapshot ready: {1 if result else 0} record")
