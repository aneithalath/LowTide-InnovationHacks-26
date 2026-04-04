from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WORLD_STATE_PATH = DATA_DIR / "world_state.json"
TRIGGER_PATH = DATA_DIR / "gemini_trigger.json"
SNAPSHOT_PATH = DATA_DIR / "last_trigger_snapshot.json"

TRIGGER_TIERS = {
    "critical": {
        "cooldown_seconds": 0,
        "conditions": [
            "New incident with severity >= 8",
            "Weather alert changed from null to non-null",
            "4 or more units simultaneously dispatched",
        ],
    },
    "high": {
        "cooldown_seconds": 180,
        "conditions": [
            "New incident with severity >= 6",
            "Any congestion_score crossed 0.85 near a hotspot",
            "Event with expected_crowd >= 10000 starting within 90 minutes",
        ],
    },
    "moderate": {
        "cooldown_seconds": 600,
        "conditions": [
            "Total incident count increased by 5 or more since last snapshot",
            "Any congestion_score crossed 0.75 near a hotspot",
            "Event with expected_crowd >= 5000 starting within 90 minutes",
        ],
    },
}

HOTSPOTS = [
    {"name": "Mill Ave Corridor", "lat": 33.4192, "lng": -111.9340},
    {"name": "ASU Campus", "lat": 33.4242, "lng": -111.9281},
    {"name": "Tempe Marketplace", "lat": 33.4000, "lng": -111.9000},
    {"name": "Tempe Town Lake", "lat": 33.4280, "lng": -111.9350},
]


def _log_trigger(tier: str, reason_count: int) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] gemini trigger: {tier.upper()} - {reason_count} conditions met")


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any, temp_name: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(temp_name)
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(temp_path, path)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _distance_deg(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    return math.sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2)


def _nearest_hotspot(lat: float, lng: float, radius_deg: float = 0.02) -> str | None:
    for hotspot in HOTSPOTS:
        hotspot_lat = float(hotspot["lat"])
        hotspot_lng = float(hotspot["lng"])
        if _distance_deg(lat, lng, hotspot_lat, hotspot_lng) <= radius_deg:
            return str(hotspot["name"])
    return None


def _parse_event_start(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    parsed = _parse_iso(text)
    if parsed is not None:
        return parsed

    # Accept plain date strings as a fallback.
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def _event_key(event: dict[str, Any]) -> str:
    event_id = str(event.get("event_id") or "").strip()
    if event_id:
        return event_id
    name = str(event.get("name") or "unknown-event").strip().lower()
    start_time = str(event.get("start_time") or "unknown-time").strip()
    return f"{name}:{start_time}"


def _build_current_state(world_state: dict[str, Any], now_utc: datetime) -> dict[str, Any]:
    incidents = world_state.get("incidents") if isinstance(world_state.get("incidents"), list) else []
    weather = world_state.get("weather") if isinstance(world_state.get("weather"), dict) else {}
    traffic = world_state.get("traffic") if isinstance(world_state.get("traffic"), list) else []
    events = world_state.get("events") if isinstance(world_state.get("events"), list) else []
    units = world_state.get("units") if isinstance(world_state.get("units"), list) else []

    incident_ids: set[str] = set()
    incident_by_id: dict[str, dict[str, Any]] = {}
    for incident in incidents:
        if not isinstance(incident, dict):
            continue
        incident_id = str(incident.get("id") or "").strip()
        if not incident_id:
            continue
        incident_ids.add(incident_id)
        incident_by_id[incident_id] = incident

    dispatched_units = 0
    for unit in units:
        if not isinstance(unit, dict):
            continue
        assigned_incident = unit.get("assigned_incident")
        available = unit.get("availability")
        status = str(unit.get("status") or "").lower()
        if assigned_incident is not None or available is False or status in {"en_route", "on_scene", "returning"}:
            dispatched_units += 1

    traffic_near_hotspots: dict[str, dict[str, Any]] = {}
    for segment in traffic:
        if not isinstance(segment, dict):
            continue
        road_id = str(segment.get("road_id") or "").strip()
        lat = _safe_float(segment.get("lat"))
        lng = _safe_float(segment.get("lng"))
        score = _safe_float(segment.get("congestion_score"))
        if not road_id or lat is None or lng is None or score is None:
            continue

        hotspot_name = _nearest_hotspot(lat, lng)
        if hotspot_name is None:
            continue

        traffic_near_hotspots[road_id] = {
            "score": score,
            "label": str(segment.get("label") or road_id),
            "hotspot": hotspot_name,
        }

    high_event_keys: set[str] = set()
    moderate_event_keys: set[str] = set()
    high_event_meta: dict[str, str] = {}
    moderate_event_meta: dict[str, str] = {}

    window_limit = now_utc + timedelta(minutes=90)
    for event in events:
        if not isinstance(event, dict):
            continue
        crowd = _safe_int(event.get("expected_crowd"))
        start_time = _parse_event_start(event.get("start_time"))
        if crowd is None or start_time is None:
            continue

        if not (now_utc <= start_time <= window_limit):
            continue

        key = _event_key(event)
        name = str(event.get("name") or "Unnamed Event")
        start_label = str(event.get("start_time") or "")
        meta = f"{name} ({crowd}) at {start_label}"

        if crowd >= 10000:
            high_event_keys.add(key)
            high_event_meta[key] = meta

        if crowd >= 5000:
            moderate_event_keys.add(key)
            moderate_event_meta[key] = meta

    return {
        "captured_at": now_utc.isoformat(),
        "incident_ids": sorted(incident_ids),
        "incident_count": len(incident_ids),
        "incident_by_id": incident_by_id,
        "weather_alert_active": bool(weather.get("alert_type")),
        "traffic_near_hotspots": traffic_near_hotspots,
        "dispatched_units": dispatched_units,
        "high_event_keys": sorted(high_event_keys),
        "moderate_event_keys": sorted(moderate_event_keys),
        "high_event_meta": high_event_meta,
        "moderate_event_meta": moderate_event_meta,
    }


def _seconds_since_last_trigger(last_trigger_at: str | None, now_utc: datetime) -> float:
    parsed = _parse_iso(last_trigger_at)
    if parsed is None:
        return float("inf")
    return max((now_utc - parsed).total_seconds(), 0.0)


def _build_trigger_payload(tier: str, reasons: list[str], now_utc: datetime) -> dict[str, Any]:
    """Build a payload when a trigger fires.
    
    Downstream system should ONLY send to Gemini API when:
    - triggered == True
    - ready_to_send_gemini == True
    - consumed == False
    
    After sending to Gemini, mark consumed as True to avoid duplicate API calls.
    """
    return {
        "evaluated_at": now_utc.isoformat(),
        "triggered": True,
        "triggered_at": now_utc.isoformat(),
        "ready_to_send_gemini": True,  # Explicit flag: only send Gemini API when this is True
        "severity_tier": tier,
        "reasons": reasons,
        "world_state_snapshot_path": "data/world_state.json",
        "consumed": False,  # Downstream marks as True after sending to Gemini API
    }


def _build_no_trigger_payload(
    now_utc: datetime,
    previous_snapshot: dict[str, Any],
    reason_counts: dict[str, int] | None = None,
    status: str = "no trigger conditions met",
) -> dict[str, Any]:
    """Build a payload when a trigger does NOT fire.
    
    When triggered == False, DO NOT send to Gemini API.
    This file is written every cycle for observability/health-check purposes,
    so downstream systems can verify the pipeline is running.
    """
    payload: dict[str, Any] = {
        "evaluated_at": now_utc.isoformat(),
        "triggered": False,
        "ready_to_send_gemini": False,  # No trigger fired, do not send to Gemini API
        "status": status,
        "severity_tier": "none",
        "reasons": [],
        "world_state_snapshot_path": "data/world_state.json",
        "consumed": False,
        "last_triggered_at": previous_snapshot.get("last_trigger_at"),
        "last_trigger_tier": previous_snapshot.get("last_trigger_tier"),
    }

    if reason_counts is not None:
        payload["reason_counts"] = reason_counts

    return payload


def evaluate_trigger() -> bool:
    """Evaluate trigger conditions and write gemini_trigger.json.
    
    NOTE: This file is written EVERY evaluation cycle (every 30 seconds),
    regardless of whether a trigger fires. This is intentional for observability.
    
    KEY RULE FOR DOWNSTREAM: Only send a Gemini API request when:
        triggered == True AND ready_to_send_gemini == True AND consumed == False
    
    This ensures Gemini API keys are only used when a real trigger condition is met.
    
    Returns True if a trigger was written (and Gemini API should be called),
    False otherwise.
    """
    now_utc = datetime.now(timezone.utc)

    world_state = _load_json(WORLD_STATE_PATH)
    if not isinstance(world_state, dict):
        fallback_payload = _build_no_trigger_payload(
            now_utc,
            previous_snapshot={},
            status="world_state missing or invalid",
        )
        _write_json(TRIGGER_PATH, fallback_payload, "gemini_trigger.tmp")
        return False

    previous_snapshot = _load_json(SNAPSHOT_PATH)
    if not isinstance(previous_snapshot, dict):
        previous_snapshot = {}

    current_state = _build_current_state(world_state, now_utc)

    previous_incident_ids = set(previous_snapshot.get("incident_ids") or [])
    previous_incident_count = int(previous_snapshot.get("incident_count") or 0)
    previous_weather_alert_active = bool(previous_snapshot.get("weather_alert_active"))
    previous_dispatched_units = int(previous_snapshot.get("dispatched_units") or 0)

    previous_traffic = previous_snapshot.get("traffic_near_hotspots")
    if not isinstance(previous_traffic, dict):
        previous_traffic = {}

    previous_high_event_keys = set(previous_snapshot.get("high_event_keys") or [])
    previous_moderate_event_keys = set(previous_snapshot.get("moderate_event_keys") or [])

    critical_reasons: list[str] = []
    high_reasons: list[str] = []
    moderate_reasons: list[str] = []

    for incident_id in current_state["incident_ids"]:
        if incident_id in previous_incident_ids:
            continue
        incident = current_state["incident_by_id"].get(incident_id, {})
        severity = _safe_int(incident.get("severity")) or 0
        title = str(incident.get("title") or incident.get("type") or "incident")

        if severity >= 8:
            critical_reasons.append(f"New severity-{severity} incident: {title}")
        elif severity >= 6:
            high_reasons.append(f"New severity-{severity} incident: {title}")

    if (not previous_weather_alert_active) and bool(current_state["weather_alert_active"]):
        critical_reasons.append("Weather alert changed from null to non-null")

    current_dispatched_units = int(current_state["dispatched_units"])
    if previous_dispatched_units < 4 and current_dispatched_units >= 4:
        critical_reasons.append(f"{current_dispatched_units} units currently dispatched")

    for road_id, current_meta in current_state["traffic_near_hotspots"].items():
        current_score = _safe_float(current_meta.get("score")) or 0.0
        previous_meta = previous_traffic.get(road_id)
        if not isinstance(previous_meta, dict):
            previous_meta = {}
        previous_score = _safe_float(previous_meta.get("score")) or 0.0

        label = str(current_meta.get("label") or road_id)
        hotspot_name = str(current_meta.get("hotspot") or "hotspot")

        if previous_score <= 0.85 < current_score:
            high_reasons.append(
                f"Congestion crossed 0.85 near {hotspot_name}: {label} now {current_score:.2f}"
            )

        if previous_score <= 0.75 < current_score:
            moderate_reasons.append(
                f"Congestion crossed 0.75 near {hotspot_name}: {label} now {current_score:.2f}"
            )

    current_high_event_keys = set(current_state["high_event_keys"])
    current_moderate_event_keys = set(current_state["moderate_event_keys"])

    for key in sorted(current_high_event_keys - previous_high_event_keys):
        details = current_state["high_event_meta"].get(key, key)
        high_reasons.append(f"High-crowd event starting within 90 minutes: {details}")

    for key in sorted(current_moderate_event_keys - previous_moderate_event_keys):
        details = current_state["moderate_event_meta"].get(key, key)
        moderate_reasons.append(f"Moderate-crowd event starting within 90 minutes: {details}")

    incident_count_delta = int(current_state["incident_count"]) - previous_incident_count
    if incident_count_delta >= 5:
        moderate_reasons.append(f"Incident count increased by {incident_count_delta} since last snapshot")

    tier_to_fire: str | None = None
    reasons_to_fire: list[str] = []
    seconds_since_last = _seconds_since_last_trigger(previous_snapshot.get("last_trigger_at"), now_utc)

    if critical_reasons:
        tier_to_fire = "critical"
        reasons_to_fire = critical_reasons
    elif high_reasons and seconds_since_last >= TRIGGER_TIERS["high"]["cooldown_seconds"]:
        tier_to_fire = "high"
        reasons_to_fire = high_reasons
    elif moderate_reasons and seconds_since_last >= TRIGGER_TIERS["moderate"]["cooldown_seconds"]:
        tier_to_fire = "moderate"
        reasons_to_fire = moderate_reasons

    snapshot_payload = {
        "captured_at": current_state["captured_at"],
        "incident_ids": current_state["incident_ids"],
        "incident_count": current_state["incident_count"],
        "weather_alert_active": current_state["weather_alert_active"],
        "traffic_near_hotspots": current_state["traffic_near_hotspots"],
        "dispatched_units": current_state["dispatched_units"],
        "high_event_keys": current_state["high_event_keys"],
        "moderate_event_keys": current_state["moderate_event_keys"],
        "last_trigger_at": previous_snapshot.get("last_trigger_at"),
        "last_trigger_tier": previous_snapshot.get("last_trigger_tier"),
    }

    if tier_to_fire is None:
        no_trigger_payload = _build_no_trigger_payload(
            now_utc,
            previous_snapshot=previous_snapshot,
            reason_counts={
                "critical": len(critical_reasons),
                "high": len(high_reasons),
                "moderate": len(moderate_reasons),
            },
        )
        _write_json(TRIGGER_PATH, no_trigger_payload, "gemini_trigger.tmp")
        _write_json(SNAPSHOT_PATH, snapshot_payload, "last_trigger_snapshot.tmp")
        return False

    trigger_payload = _build_trigger_payload(tier_to_fire, reasons_to_fire, now_utc)
    _write_json(TRIGGER_PATH, trigger_payload, "gemini_trigger.tmp")

    snapshot_payload["last_trigger_at"] = trigger_payload["triggered_at"]
    snapshot_payload["last_trigger_tier"] = tier_to_fire
    _write_json(SNAPSHOT_PATH, snapshot_payload, "last_trigger_snapshot.tmp")

    _log_trigger(tier_to_fire, len(reasons_to_fire))
    return True
