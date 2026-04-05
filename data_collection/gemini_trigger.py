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
VEHICLE_STATE_PATH = DATA_DIR / "emergency_vehicle_state.json"
INCIDENT_STATE_PATH = DATA_DIR / "incident_state.json"
GEMINI_INPUT_PATH = DATA_DIR / "gemini_input.json"
GEMINI_BRIEF_PATH = DATA_DIR / "gemini_brief.json"
TRIGGER_PATH = DATA_DIR / "gemini_trigger.json"
SNAPSHOT_PATH = DATA_DIR / "last_trigger_snapshot.json"
FALLBACK_OUTPUT_PATH = DATA_DIR / "fallback_output.json"

TRIGGER_TIERS = {
    "critical": {
        "cooldown_seconds": 0,
        "conditions": [
            "New incident with normalized severity >= 8",
            "Weather alert changed from null to non-null",
            "4 or more units simultaneously dispatched",
        ],
    },
    "high": {
        "cooldown_seconds": 180,
        "conditions": [
            "New incident with normalized severity 6-7",
            "Congestion crossed 0.85 near a hotspot",
            "Incident cluster detected",
        ],
    },
    "moderate": {
        "cooldown_seconds": 600,
        "conditions": [
            "Total incident count increased by 5 or more since last snapshot",
            "Congestion crossed 0.75 near a hotspot",
            "Large event starting within 90 minutes",
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


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _safe_int(value: Any, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


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


def _load_incident_state() -> dict[str, Any]:
    try:
        with INCIDENT_STATE_PATH.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _count_dispatched_units_from_vehicle_state() -> int:
    try:
        with VEHICLE_STATE_PATH.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return 0
        vehicles = payload.get("vehicles") if isinstance(payload.get("vehicles"), list) else []
    except Exception:
        return 0

    return sum(
        1
        for vehicle in vehicles
        if str(vehicle.get("status") or "").lower() not in {"patrolling", "available", "idle"}
    )


def _build_current_state(world_state: dict[str, Any], now_utc: datetime) -> dict[str, Any]:
    incidents = world_state.get("incidents") if isinstance(world_state.get("incidents"), list) else []
    weather = world_state.get("weather") if isinstance(world_state.get("weather"), dict) else {}
    traffic = world_state.get("traffic") if isinstance(world_state.get("traffic"), list) else []
    events = world_state.get("events") if isinstance(world_state.get("events"), list) else []

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

    incident_state = _load_incident_state()
    for inc_id, inc_data in incident_state.items():
        if inc_id in incident_by_id and isinstance(inc_data, dict):
            incident_by_id[inc_id]["normalized_severity"] = _safe_int(inc_data.get("last_severity"), 0)
            incident_by_id[inc_id]["change_type"] = str(inc_data.get("change_type") or "stable")

    dispatched_units = _count_dispatched_units_from_vehicle_state()

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

        crowd = _safe_int(event.get("expected_crowd"), fallback=-1)
        start_time = _parse_event_start(event.get("start_time"))
        if crowd < 0 or start_time is None:
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
    return {
        "evaluated_at": now_utc.isoformat(),
        "triggered": True,
        "triggered_at": now_utc.isoformat(),
        "severity_tier": tier,
        "reasons": reasons,
        "gemini_input_path": "data_collection/data/gemini_brief.json",
        "consumed": False,
    }


def _build_no_trigger_payload(
    now_utc: datetime,
    previous_snapshot: dict[str, Any],
    reason_counts: dict[str, int] | None = None,
    status: str = "no trigger conditions met",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "evaluated_at": now_utc.isoformat(),
        "triggered": False,
        "severity_tier": "none",
        "reasons": [],
        "gemini_input_path": "data_collection/data/gemini_brief.json",
        "consumed": False,
        "status": status,
        "last_triggered_at": previous_snapshot.get("last_trigger_at"),
        "last_trigger_tier": previous_snapshot.get("last_trigger_tier"),
    }

    if reason_counts is not None:
        payload["reason_counts"] = reason_counts

    return payload


def _inject_trigger_into_gemini_files(tier: str, reasons: list[str]) -> None:
    for path, tmp in [
        (GEMINI_INPUT_PATH, "gemini_input.tmp"),
        (GEMINI_BRIEF_PATH, "gemini_brief.tmp"),
    ]:
        try:
            payload = _load_json(path)
            if not isinstance(payload, dict):
                continue
            payload["trigger_tier"] = tier
            payload["trigger_reasons"] = reasons
            _write_json(path, payload, tmp)
        except Exception:
            pass


def generate_fallback_output() -> dict[str, Any]:
    """
    Called when Gemini is unavailable or trigger fires but Gemini call fails.
    Reads gemini_input.json and produces a condensed human-readable summary dict.
    Writes to data/fallback_output.json and logs a WARNING to console.
    """
    now_utc = datetime.now(timezone.utc)
    gemini_input = _load_json(GEMINI_INPUT_PATH)
    if not isinstance(gemini_input, dict):
        gemini_input = {}

    situation_summary = gemini_input.get("situation_summary")
    if not isinstance(situation_summary, dict):
        situation_summary = {}

    highest = situation_summary.get("highest_severity_incident")
    if not isinstance(highest, dict):
        highest = {}

    top_zones_raw = gemini_input.get("top_risk_zones")
    if not isinstance(top_zones_raw, list):
        top_zones_raw = []

    recommended_actions_raw = gemini_input.get("recommended_actions")
    if not isinstance(recommended_actions_raw, list):
        recommended_actions_raw = []

    top_3_zones: list[dict[str, Any]] = []
    for zone in top_zones_raw[:3]:
        if not isinstance(zone, dict):
            continue
        top_3_zones.append(
            {
                "rank": int(zone.get("rank") or 0),
                "zone_id": zone.get("zone_id"),
                "score": float(zone.get("composite_risk_score") or 0.0),
                "drivers": zone.get("risk_drivers") if isinstance(zone.get("risk_drivers"), list) else [],
            }
        )

    immediate_actions: list[str] = []
    for action in recommended_actions_raw[:5]:
        if not isinstance(action, dict):
            continue
        action_text = str(action.get("action") or "").strip()
        if not action_text:
            continue
        unit = action.get("unit_code")
        if unit:
            action_text = action_text.replace("nearest available unit", str(unit))
        immediate_actions.append(action_text)

    incident_count = int(situation_summary.get("active_incident_count") or 0)
    highest_sev = int(highest.get("severity") or 0)
    highest_title = str(highest.get("title") or highest.get("type") or "unknown")
    units_deployed = int(situation_summary.get("units_deployed") or 0)

    fallback_payload = {
        "fallback": True,
        "generated_at": now_utc.isoformat(),
        "warning": "Gemini unavailable - automated fallback summary",
        "overall_risk_level": str(situation_summary.get("overall_risk_level") or "LOW"),
        "trigger_tier": gemini_input.get("trigger_tier"),
        "situation": (
            f"{incident_count} active incidents. Highest severity: {highest_sev} "
            f"({highest_title}). {units_deployed} units deployed."
        ),
        "top_3_zones": top_3_zones,
        "immediate_actions": immediate_actions,
        "units_available": int(situation_summary.get("units_available") or 0),
    }

    _write_json(FALLBACK_OUTPUT_PATH, fallback_payload, "fallback_output.tmp")
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] WARNING: Gemini unavailable - fallback output written to data/fallback_output.json")
    return fallback_payload


def evaluate_trigger() -> bool:
    """Evaluate event-driven trigger conditions and write gemini_trigger.json."""
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

    critical_reasons: list[str] = []
    high_reasons: list[str] = []
    moderate_reasons: list[str] = []

    prev_triggered_critical = set(previous_snapshot.get("triggered_critical_ids") or [])
    prev_triggered_high = set(previous_snapshot.get("triggered_high_ids") or [])
    new_triggered_critical = set(prev_triggered_critical)
    new_triggered_high = set(prev_triggered_high)

    for inc_id, incident in current_state["incident_by_id"].items():
        sev = _safe_int(incident.get("normalized_severity"), 0)
        change = str(incident.get("change_type") or "stable")
        title = str(incident.get("title") or incident.get("type") or "incident")

        if sev >= 8 and (inc_id not in prev_triggered_critical or change == "escalated"):
            prefix = "Escalated" if change == "escalated" else "New"
            critical_reasons.append(f"{prefix} severity-{sev} incident: {title}")
            new_triggered_critical.add(inc_id)

    if (not bool(previous_snapshot.get("weather_alert_active"))) and bool(current_state["weather_alert_active"]):
        critical_reasons.append("Weather alert activated")

    if current_state["dispatched_units"] >= 4 and _safe_int(previous_snapshot.get("dispatched_units"), 0) < 4:
        critical_reasons.append(f"{current_state['dispatched_units']} units now deployed simultaneously")

    for inc_id, incident in current_state["incident_by_id"].items():
        sev = _safe_int(incident.get("normalized_severity"), 0)
        change = str(incident.get("change_type") or "stable")
        title = str(incident.get("title") or incident.get("type") or "incident")

        if 6 <= sev < 8 and (inc_id not in prev_triggered_high or change == "escalated"):
            prefix = "Escalated" if change == "escalated" else "New"
            high_reasons.append(f"{prefix} severity-{sev} incident: {title}")
            new_triggered_high.add(inc_id)

    incident_coords: list[tuple[float, float]] = []
    for incident in current_state["incident_by_id"].values():
        if not isinstance(incident, dict):
            continue
        lat = _safe_float(incident.get("lat"))
        lng = _safe_float(incident.get("lng"))
        if lat is None or lng is None:
            continue
        incident_coords.append((lat, lng))

    cluster_found = False
    for lat1, lng1 in incident_coords:
        nearby_count = sum(
            1
            for lat2, lng2 in incident_coords
            if abs(lat1 - lat2) + abs(lng1 - lng2) < 0.05
        )
        if nearby_count >= 3:
            cluster_found = True
            break

    if cluster_found and not bool(previous_snapshot.get("cluster_detected")):
        high_reasons.append("Incident cluster detected: 3+ incidents in close proximity")

    previous_traffic = previous_snapshot.get("traffic_near_hotspots")
    if not isinstance(previous_traffic, dict):
        previous_traffic = {}

    for road_id, meta in current_state["traffic_near_hotspots"].items():
        prev_meta = previous_traffic.get(road_id, {})
        if not isinstance(prev_meta, dict):
            prev_meta = {}

        curr_score = _safe_float(meta.get("score"), 0.0) or 0.0
        prev_score = _safe_float(prev_meta.get("score"), 0.0) or 0.0

        if prev_score <= 0.85 < curr_score:
            high_reasons.append(
                f"Congestion spike near {meta.get('hotspot')}: {meta.get('label')} now {curr_score:.2f}"
            )

    count_delta = current_state["incident_count"] - _safe_int(previous_snapshot.get("incident_count"), 0)
    if count_delta >= 5:
        moderate_reasons.append(f"Incident surge: +{count_delta} since last check")

    for road_id, meta in current_state["traffic_near_hotspots"].items():
        prev_meta = previous_traffic.get(road_id, {})
        if not isinstance(prev_meta, dict):
            prev_meta = {}

        curr_score = _safe_float(meta.get("score"), 0.0) or 0.0
        prev_score = _safe_float(prev_meta.get("score"), 0.0) or 0.0

        if prev_score <= 0.75 < curr_score:
            moderate_reasons.append(
                f"Congestion near {meta.get('hotspot')}: {meta.get('label')} now {curr_score:.2f}"
            )

    previous_high_event_keys = set(previous_snapshot.get("high_event_keys") or [])
    for key in set(current_state["high_event_keys"]) - previous_high_event_keys:
        details = current_state["high_event_meta"].get(key, key)
        moderate_reasons.append(f"Large event starting within 90min: {details}")

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
        "triggered_critical_ids": sorted(new_triggered_critical),
        "triggered_high_ids": sorted(new_triggered_high),
        "cluster_detected": cluster_found,
        "last_trigger_at": previous_snapshot.get("last_trigger_at"),
        "last_trigger_tier": previous_snapshot.get("last_trigger_tier"),
    }

    incident_state_now = _load_incident_state()
    active_ids = {
        incident_id
        for incident_id, value in incident_state_now.items()
        if isinstance(value, dict) and value.get("status") == "active"
    }
    snapshot_payload["triggered_critical_ids"] = sorted(set(snapshot_payload["triggered_critical_ids"]) & active_ids)
    snapshot_payload["triggered_high_ids"] = sorted(set(snapshot_payload["triggered_high_ids"]) & active_ids)

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
    _inject_trigger_into_gemini_files(tier_to_fire, reasons_to_fire)
    _write_json(TRIGGER_PATH, trigger_payload, "gemini_trigger.tmp")

    snapshot_payload["last_trigger_at"] = trigger_payload["triggered_at"]
    snapshot_payload["last_trigger_tier"] = tier_to_fire
    _write_json(SNAPSHOT_PATH, snapshot_payload, "last_trigger_snapshot.tmp")

    _log_trigger(tier_to_fire, len(reasons_to_fire))
    return True
