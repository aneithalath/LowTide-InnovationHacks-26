from __future__ import annotations

import copy
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

from cameras_data import fetch_cameras
from citizen_data import fetch_citizen_incidents, normalize_incident
from events_data import fetch_events_data
from gemini_trigger import evaluate_trigger
from historical_data import fetch_historical_cells
from risk_areas_data import fetch_risk_areas
from traffic_data import fetch_traffic_data
from unit_simulation import get_unit_states, tick
from weather_data import fetch_weather_snapshot

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WORLD_STATE_PATH = DATA_DIR / "world_state.json"
INCIDENTS_CACHE_PATH = DATA_DIR / "incidents_cache.json"

MIN_LAT, MIN_LON = 33.30, -112.10
MAX_LAT, MAX_LON = 33.50, -111.80

_LOCK = threading.Lock()

WORLD_STATE: dict[str, Any] = {
    "incidents": [],
    "weather": {},
    "traffic": [],
    "events": [],
    "risk_areas": [],
    "cameras": [],
    "units": [],
    "historical": [],
    "last_updated": datetime.now(timezone.utc).isoformat(),
}


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _atomic_write_json(path: Path, payload: Any, tmp_name: str) -> None:
    _ensure_data_dir()
    tmp_path = path.with_name(tmp_name)
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, path)


def _load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_world_state() -> None:
    _atomic_write_json(WORLD_STATE_PATH, WORLD_STATE, "world_state.tmp")


def _update_world_slice(key: str, value: Any) -> None:
    with _LOCK:
        WORLD_STATE[key] = value
        WORLD_STATE["last_updated"] = datetime.now(timezone.utc).isoformat()
        _save_world_state()


def get_world_state() -> dict[str, Any]:
    with _LOCK:
        return copy.deepcopy(WORLD_STATE)


def _fetch_incidents() -> list[dict[str, Any]]:
    try:
        rows = fetch_citizen_incidents(MIN_LAT, MIN_LON, MAX_LAT, MAX_LON)
        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized_row = normalize_incident(row)
            normalized_row.setdefault("_simulated", False)
            normalized_row["record_type"] = "incident"
            normalized.append(normalized_row)

        _atomic_write_json(INCIDENTS_CACHE_PATH, normalized, "incidents_cache.tmp")
        _log(f"incidents updated: {len(normalized)} records")
        return normalized
    except Exception as err:
        _log(f"incidents error: {err}")
        cached = _load_json_if_exists(INCIDENTS_CACHE_PATH)
        if isinstance(cached, list):
            return cached
        return []


def _run_job(name: str, world_key: str, fetcher) -> None:
    try:
        data = fetcher()
        _update_world_slice(world_key, data)
    except Exception as err:
        _log(f"{name} job error: {err}")


def _job_incidents() -> None:
    _run_job("incidents", "incidents", _fetch_incidents)


def _job_weather() -> None:
    _run_job("weather", "weather", fetch_weather_snapshot)


def _job_traffic() -> None:
    _run_job("traffic", "traffic", fetch_traffic_data)


def _job_events() -> None:
    _run_job("events", "events", fetch_events_data)


def _job_risk_areas() -> None:
    _run_job("risk_areas", "risk_areas", fetch_risk_areas)


def _job_units() -> None:
    try:
        traffic = WORLD_STATE.get("traffic", [])
        incidents = WORLD_STATE.get("incidents", [])
        units = tick(traffic_data=traffic, incidents=incidents, dt_seconds=30)
        _update_world_slice("units", units)
    except Exception as err:
        _log(f"units job error: {err}")


def _job_gemini_trigger() -> None:
    try:
        fired = evaluate_trigger()
        if not fired:
            _log("gemini trigger: no trigger conditions met")
    except Exception as err:
        _log(f"gemini trigger job error: {err}")


def _print_key_status() -> None:
    load_dotenv(BASE_DIR.parent / ".env")

    keys = {
        "OPENWEATHER_API_KEY": bool(os.getenv("OPENWEATHER_API_KEY")),
        "TOMTOM_API_KEY": bool(os.getenv("TOMTOM_API_KEY")),
        "EXA_API_KEY": bool(os.getenv("EXA_API_KEY")),
        "NWS_USER_AGENT": bool(os.getenv("NWS_USER_AGENT")),
    }

    _log("API key status:")
    for key, present in keys.items():
        status = "configured" if present else "missing"
        _log(f"  {key}: {status}")

    weather_mode = "real weather + NWS alerts" if keys["OPENWEATHER_API_KEY"] else "simulated baseline + NWS alerts"
    traffic_mode = "real TomTom flow" if keys["TOMTOM_API_KEY"] else "simulated congestion"
    events_mode = "Exa web search" if keys["EXA_API_KEY"] else "cache-only (EXA_API_KEY missing)"
    nws_mode = "NWS alerts enabled" if keys["NWS_USER_AGENT"] else "NWS alerts with default user agent"

    _log(f"weather source mode: {weather_mode}")
    _log(f"traffic source mode: {traffic_mode}")
    _log(f"events source mode: {events_mode}")
    _log(f"NWS mode: {nws_mode}")


def _startup_bootstrap() -> None:
    _log("startup bootstrap: collecting initial datasets")

    _job_incidents()
    _job_weather()
    _job_traffic()
    _job_risk_areas()
    _job_events()

    try:
        cameras = fetch_cameras()
        _update_world_slice("cameras", cameras)
    except Exception as err:
        _log(f"cameras startup error: {err}")

    try:
        historical = fetch_historical_cells()
        _update_world_slice("historical", historical)
    except Exception as err:
        _log(f"historical startup error: {err}")

    try:
        units = get_unit_states()
        _update_world_slice("units", units)
    except Exception as err:
        _log(f"units startup error: {err}")

    _job_gemini_trigger()


def _configure_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler()

    scheduler.add_job(_job_incidents, "interval", minutes=2, id="incidents", max_instances=1, coalesce=True)
    scheduler.add_job(_job_weather, "interval", minutes=5, id="weather", max_instances=1, coalesce=True)
    scheduler.add_job(_job_traffic, "interval", minutes=10, id="traffic", max_instances=1, coalesce=True)
    scheduler.add_job(_job_events, "interval", minutes=60, id="events", max_instances=1, coalesce=True)
    scheduler.add_job(_job_risk_areas, "interval", hours=24, id="risk_areas", max_instances=1, coalesce=True)
    scheduler.add_job(_job_units, "interval", seconds=30, id="units", max_instances=1, coalesce=True)
    scheduler.add_job(_job_gemini_trigger, "interval", seconds=60, id="gemini_trigger", max_instances=1, coalesce=True)

    return scheduler


def main() -> None:
    _ensure_data_dir()
    _print_key_status()
    _startup_bootstrap()
    _save_world_state()

    scheduler = _configure_scheduler()
    _log("collector started: scheduler running")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        _log("collector stopped by user")


if __name__ == "__main__":
    main()
