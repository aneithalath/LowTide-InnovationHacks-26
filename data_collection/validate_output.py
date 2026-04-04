from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
WORLD_STATE_PATH = BASE_DIR / "data" / "world_state.json"
REQUIRED_KEYS = [
    "incidents",
    "weather",
    "traffic",
    "events",
    "risk_areas",
    "cameras",
    "units",
    "historical",
    "last_updated",
]


class ValidationState:
    def __init__(self) -> None:
        self.failures = 0
        self.total_records = 0
        self.simulated_records = 0

    def pass_line(self, message: str) -> None:
        print(f"[PASS] {message}")

    def warn_line(self, message: str) -> None:
        print(f"[WARN] {message}")

    def fail_line(self, message: str) -> None:
        self.failures += 1
        print(f"[FAIL] {message}")


def _to_iterable(slice_value: Any) -> list[dict[str, Any]]:
    if isinstance(slice_value, dict):
        return [slice_value]
    if isinstance(slice_value, list):
        return [item for item in slice_value if isinstance(item, dict)]
    return []


def _count_simulated(rows: list[dict[str, Any]]) -> tuple[int, int]:
    total = len(rows)
    simulated = sum(1 for row in rows if bool(row.get("_simulated")))
    return total, simulated


def _parse_iso(ts: str) -> datetime | None:
    try:
        text = ts.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def validate() -> int:
    state = ValidationState()

    print("============ SENTINEL DATA VALIDATION ============")

    if not WORLD_STATE_PATH.exists():
        state.fail_line("world_state.json not found")
        print("==================================================")
        return 1

    try:
        with WORLD_STATE_PATH.open("r", encoding="utf-8") as handle:
            world_state = json.load(handle)
        state.pass_line("world_state.json found and valid JSON")
    except Exception as err:
        state.fail_line(f"world_state.json invalid: {err}")
        print("==================================================")
        return 1

    for key in REQUIRED_KEYS:
        if key not in world_state:
            state.fail_line(f"missing top-level key: {key}")

    incidents = world_state.get("incidents", [])
    incident_rows = _to_iterable(incidents)
    total, simulated = _count_simulated(incident_rows)
    state.total_records += total
    state.simulated_records += simulated
    if total == 0:
        state.warn_line("incidents: 0 records - may just be quiet")
    else:
        state.pass_line(f"incidents: {total} record(s) ({simulated} simulated)")

    weather_rows = _to_iterable(world_state.get("weather", {}))
    total, simulated = _count_simulated(weather_rows)
    state.total_records += total
    state.simulated_records += simulated
    if total == 0:
        state.fail_line("weather: empty")
    else:
        valid_temp = any(0 <= float(row.get("temperature_f", -999)) <= 130 for row in weather_rows)
        if valid_temp:
            state.pass_line(f"weather: {total} record(s) ({simulated} simulated)")
        else:
            state.fail_line("weather temperature_f out of expected range")

    traffic_rows = _to_iterable(world_state.get("traffic", []))
    total, simulated = _count_simulated(traffic_rows)
    state.total_records += total
    state.simulated_records += simulated
    if total >= 1:
        state.pass_line(f"traffic: {total} record(s) ({simulated} simulated)")
    else:
        state.fail_line("traffic: expected at least 1 RoadSegment")

    for key in ("events", "risk_areas", "cameras", "historical"):
        rows = _to_iterable(world_state.get(key, []))
        total, simulated = _count_simulated(rows)
        state.total_records += total
        state.simulated_records += simulated
        if total > 0:
            state.pass_line(f"{key}: {total} record(s) ({simulated} simulated)")
        else:
            state.fail_line(f"{key}: empty")

    units_rows = _to_iterable(world_state.get("units", []))
    total, simulated = _count_simulated(units_rows)
    state.total_records += total
    state.simulated_records += simulated
    if total >= 6:
        state.pass_line(f"units: {total} record(s) ({simulated} simulated)")
    else:
        state.fail_line("units: expected at least 6 records")

    missing_required_fields = 0
    for key in ("incidents", "weather", "traffic", "events", "risk_areas", "cameras", "units", "historical"):
        for row in _to_iterable(world_state.get(key, [] if key != "weather" else {})):
            if "_simulated" not in row or "record_type" not in row:
                missing_required_fields += 1

    if missing_required_fields == 0:
        state.pass_line("all records have required fields")
    else:
        state.fail_line(f"{missing_required_fields} records missing _simulated or record_type")

    last_updated = world_state.get("last_updated")
    parsed_last_updated = _parse_iso(str(last_updated)) if last_updated else None
    if parsed_last_updated is None:
        state.fail_line("last_updated missing or invalid")
    else:
        age = datetime.now(timezone.utc) - parsed_last_updated
        if age <= timedelta(minutes=10):
            state.pass_line(f"last_updated: {int(age.total_seconds())} seconds ago")
        else:
            state.fail_line(f"last_updated too old: {int(age.total_seconds())} seconds ago")

    print("--------------------------------------------------")
    if state.total_records > 0:
        simulated_pct = round((state.simulated_records / state.total_records) * 100)
        real_records = state.total_records - state.simulated_records
        real_pct = 100 - simulated_pct
    else:
        simulated_pct = 0
        real_records = 0
        real_pct = 0

    print(
        f"Simulated: {state.simulated_records} / {state.total_records} total records "
        f"({simulated_pct}%)"
    )
    print(
        f"Real data: {real_records} / {state.total_records} total records "
        f"({real_pct}%)"
    )
    print("==================================================")

    return 1 if state.failures > 0 else 0


if __name__ == "__main__":
    raise SystemExit(validate())
