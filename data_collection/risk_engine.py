from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WORLD_STATE_PATH = DATA_DIR / "world_state.json"
VEHICLE_STATE_PATH = DATA_DIR / "emergency_vehicle_state.json"
HISTORICAL_PATH = DATA_DIR / "historical_cache.json"
INCIDENT_STATE_PATH = DATA_DIR / "incident_state.json"
GEMINI_INPUT_PATH = DATA_DIR / "gemini_input.json"
GEMINI_BRIEF_PATH = DATA_DIR / "gemini_brief.json"

SEVERITY_KEYWORD_MAP = [
    (10, ["shooting", "shot", "gunshot", "gunfire", "homicide", "murder"]),
    (9, ["stabbing", "stabbed", "assault with weapon", "armed robbery", "carjacking"]),
    (8, ["trapped", "entrapment", "structure fire", "building fire", "explosion", "major crash", "dui crash"]),
    (7, ["fire", "vehicle fire", "assault", "robbery", "fight", "brawl", "overdose", "unconscious"]),
    (6, ["collision", "crash", "accident", "medical", "injury", "fall", "hit and run"]),
    (5, ["disturbance", "domestic", "dispute", "threatening", "harassment"]),
    (4, ["theft", "burglary", "vandalism", "trespassing", "suspicious"]),
    (3, ["noise", "parking", "traffic stop", "civil matter"]),
    (2, []),
    (1, []),
]


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] risk_engine: {message}")


def _atomic_write_json(path: Path, payload: Any, tmp_name: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(tmp_name)
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, path)


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


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


def _clamp_0_1(value: float) -> float:
    return max(0.0, min(value, 1.0))


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


def _load_vehicle_state_with_retry() -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    for attempt in range(2):
        try:
            with VEHICLE_STATE_PATH.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                raise ValueError("vehicle state payload is not a dict")

            vehicles = payload.get("vehicles") if isinstance(payload.get("vehicles"), list) else []
            patrol_routes = payload.get("patrolRoutes") if isinstance(payload.get("patrolRoutes"), list) else []
            return vehicles, patrol_routes, True
        except Exception:
            if attempt == 0:
                time.sleep(0.1)
                continue

    _log("vehicle state unavailable, proceeding without unit data")
    return [], [], False


def haversine(a: dict[str, Any], b: dict[str, Any]) -> float:
    """Returns distance in miles between two dicts with lat/lng keys."""
    r = 3958.8
    lat1, lon1 = radians(_safe_float(a.get("lat"), 0.0) or 0.0), radians(_safe_float(a.get("lng"), 0.0) or 0.0)
    lat2, lon2 = radians(_safe_float(b.get("lat"), 0.0) or 0.0), radians(_safe_float(b.get("lng"), 0.0) or 0.0)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return r * 2 * atan2(sqrt(h), sqrt(1 - h))


def _as_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _parse_peak_hours(cell: dict[str, Any]) -> set[int]:
    raw = cell.get("peak_hours")
    parsed: set[int] = set()

    if isinstance(raw, list):
        for item in raw:
            hour = _safe_int(item, fallback=-1)
            if 0 <= hour <= 23:
                parsed.add(hour)
        return parsed

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return parsed

        if text.startswith("[") and text.endswith("]"):
            try:
                loaded = json.loads(text)
                if isinstance(loaded, list):
                    for item in loaded:
                        hour = _safe_int(item, fallback=-1)
                        if 0 <= hour <= 23:
                            parsed.add(hour)
                    return parsed
            except Exception:
                return parsed

        for part in text.split(","):
            hour = _safe_int(part.strip(), fallback=-1)
            if 0 <= hour <= 23:
                parsed.add(hour)

    return parsed


def normalize_severity(incident: dict[str, Any]) -> int:
    """Map Citizen incident to 0-10 severity. Adds 'normalized_severity' field."""
    title = str(incident.get("title") or "").lower()
    inc_type = str(incident.get("type") or "").lower()
    combined = f"{title} {inc_type}"

    for score, keywords in SEVERITY_KEYWORD_MAP:
        if any(keyword in combined for keyword in keywords):
            return score

    raw = _safe_int(incident.get("severity"), fallback=0)
    return {2: 5, 1: 3, 0: 1}.get(raw, 2)


def update_incident_state(current_incidents: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Compare current incidents against persistent state.
    Returns:
        updated_state: full updated dict to write back
        delta_incidents: list of NEW and ESCALATED incidents only (for Gemini brief)
        resolved_ids: list of incident IDs that disappeared this cycle
    """
    now = datetime.now(timezone.utc).isoformat()

    try:
        with INCIDENT_STATE_PATH.open("r", encoding="utf-8") as fh:
            state = json.load(fh)
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

    current_ids: set[str] = set()
    delta_incidents: list[dict[str, Any]] = []

    for inc in current_incidents:
        inc_id = str(inc.get("id") or "").strip()
        if not inc_id:
            continue

        current_ids.add(inc_id)
        sev = _safe_int(inc.get("normalized_severity"), fallback=0)

        if inc_id not in state:
            change_type = "new"
            delta_incidents.append({**inc, "change_type": "new"})
        elif sev > _safe_int(state[inc_id].get("last_severity"), fallback=0):
            change_type = "escalated"
            delta_incidents.append({**inc, "change_type": "escalated"})
        else:
            change_type = "stable"

        state[inc_id] = {
            "first_seen": state.get(inc_id, {}).get("first_seen", now),
            "last_seen": now,
            "last_severity": sev,
            "last_title": str(inc.get("title") or ""),
            "last_lat": _safe_float(inc.get("lat"), 0.0) or 0.0,
            "last_lng": _safe_float(inc.get("lng"), 0.0) or 0.0,
            "status": "active",
            "change_type": change_type,
        }

    resolved_ids: list[dict[str, Any]] = []
    for inc_id in list(state.keys()):
        if inc_id not in current_ids and state[inc_id].get("status") == "active":
            state[inc_id]["status"] = "resolved"
            state[inc_id]["change_type"] = "resolved"
            resolved_ids.append({"id": inc_id})

    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    pruned_state: dict[str, Any] = {}
    for key, value in state.items():
        if not isinstance(value, dict):
            continue
        if value.get("status") == "active":
            pruned_state[key] = value
            continue

        last_seen = _parse_iso(str(value.get("last_seen") or ""))
        if last_seen is not None and last_seen > cutoff:
            pruned_state[key] = value

    _atomic_write_json(INCIDENT_STATE_PATH, pruned_state, "incident_state.tmp")
    return pruned_state, delta_incidents, resolved_ids


def _nearby_rows(cell: dict[str, Any], rows: list[dict[str, Any]], radius_miles: float) -> list[dict[str, Any]]:
    nearby: list[dict[str, Any]] = []
    cell_lat = _safe_float(cell.get("lat"))
    cell_lng = _safe_float(cell.get("lng"))
    if cell_lat is None or cell_lng is None:
        return []
    cell_point = {"lat": cell_lat, "lng": cell_lng}

    for row in rows:
        row_lat = _safe_float(row.get("lat") or row.get("latitude"))
        row_lng = _safe_float(row.get("lng") or row.get("longitude"))
        if row_lat is None or row_lng is None:
            continue
        row_point = {"lat": row_lat, "lng": row_lng}
        try:
            if haversine(cell_point, row_point) <= radius_miles:
                nearby.append(row)
        except Exception:
            continue
    return nearby


def _incident_modifier(cell: dict[str, Any], incidents: list[dict[str, Any]]) -> tuple[float, list[dict[str, Any]], int]:
    nearby = _nearby_rows(cell, incidents, 0.5)
    if not nearby:
        return 0.0, [], 0
    max_severity = max(int(i.get("normalized_severity") or i.get("severity") or 0) for i in nearby)
    count_factor = min(len(nearby) / 5.0, 1.0)
    score = min((max_severity / 10.0) * 0.7 + count_factor * 0.3, 1.0)
    return score, nearby, max_severity


def _crowd_modifier(
    cell: dict[str, Any],
    events: list[dict[str, Any]],
    risk_areas: list[dict[str, Any]],
) -> tuple[float, list[dict[str, Any]], list[dict[str, Any]]]:
    nearby_events = _nearby_rows(cell, events, 0.4)
    nearby_areas = _nearby_rows(cell, risk_areas, 0.4)

    event_score = max((_safe_int(event.get("expected_crowd")) / 55000.0 for event in nearby_events), default=0.0)
    area_score = max((_safe_float(area.get("crowd_modifier"), 0.0) or 0.0 for area in nearby_areas), default=0.0)
    return min(max(event_score, area_score), 1.0), nearby_events, nearby_areas


def _traffic_modifier(
    cell: dict[str, Any],
    traffic: list[dict[str, Any]],
) -> tuple[float, list[dict[str, Any]], dict[str, Any] | None]:
    nearby = _nearby_rows(cell, traffic, 0.3)
    if not nearby:
        return 0.0, [], None

    avg = sum(_safe_float(segment.get("congestion_score"), 0.0) or 0.0 for segment in nearby) / len(nearby)
    busiest = max(nearby, key=lambda seg: _safe_float(seg.get("congestion_score"), 0.0) or 0.0)
    return _clamp_0_1(avg), nearby, busiest


def _time_modifier(cell: dict[str, Any], now_utc: datetime) -> float:
    return 1.0 if now_utc.hour in _parse_peak_hours(cell) else 0.0


def _weather_multiplier(weather: dict[str, Any]) -> float:
    if weather.get("alert_type"):
        return 1.25
    if (_safe_float(weather.get("wind_speed_mph"), 0.0) or 0.0) > 30:
        return 1.10
    if (_safe_float(weather.get("visibility_miles"), 10.0) or 10.0) < 3:
        return 1.15
    return 1.0


def _nearest_congestion_score(point: dict[str, Any], traffic: list[dict[str, Any]]) -> float:
    best_distance: float | None = None
    best_score = 0.0

    for segment in traffic:
        lat = _safe_float(segment.get("lat"))
        lng = _safe_float(segment.get("lng"))
        if lat is None or lng is None:
            continue
        distance = haversine(point, {"lat": lat, "lng": lng})
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_score = _safe_float(segment.get("congestion_score"), 0.0) or 0.0

    return _clamp_0_1(best_score)


def nearest_units(
    zone: dict[str, Any],
    vehicles: list[dict[str, Any]],
    traffic: list[dict[str, Any]],
    n: int = 3,
) -> list[dict[str, Any]]:
    available = [vehicle for vehicle in vehicles if str(vehicle.get("status") or "").lower() == "patrolling"]
    ordered = sorted(
        available,
        key=lambda vehicle: haversine(
            zone,
            {
                "lat": _safe_float(vehicle.get("latitude"), 0.0) or 0.0,
                "lng": _safe_float(vehicle.get("longitude"), 0.0) or 0.0,
            },
        ),
    )

    results: list[dict[str, Any]] = []
    for vehicle in ordered[:n]:
        position = {
            "lat": _safe_float(vehicle.get("latitude"), 0.0) or 0.0,
            "lng": _safe_float(vehicle.get("longitude"), 0.0) or 0.0,
        }
        distance = haversine(zone, position)
        base_eta = (distance / 35.0) * 60.0
        congestion_penalty = 1.0 + (_nearest_congestion_score(position, traffic) * 0.5)
        eta = base_eta * congestion_penalty

        results.append(
            {
                "unit_code": str(vehicle.get("unitCode") or "unknown"),
                "vehicle_type": str(vehicle.get("vehicleType") or "unknown"),
                "distance_miles": round(distance, 2),
                "eta_minutes": round(eta, 1),
                "current_lat": _safe_float(vehicle.get("latitude"), 0.0) or 0.0,
                "current_lng": _safe_float(vehicle.get("longitude"), 0.0) or 0.0,
                "route_label": str(vehicle.get("routeLabel") or "unknown"),
            }
        )

    return results


def _derive_weather_status(weather: dict[str, Any]) -> str:
    if not weather:
        return "weather unavailable"

    conditions = str(weather.get("conditions") or "unknown conditions")
    temp = _safe_float(weather.get("temperature_f"), 0.0) or 0.0
    alert = weather.get("alert_type")
    if alert:
        return f"{conditions}, {round(temp)}F, alert: {alert}"
    return f"{conditions}, {round(temp)}F, no alerts"


def _derive_overall_risk_level(top_score: float) -> str:
    if top_score >= 0.80:
        return "CRITICAL"
    if top_score >= 0.60:
        return "HIGH"
    if top_score >= 0.40:
        return "MODERATE"
    return "LOW"


def _build_risk_drivers(
    cell: dict[str, Any],
    contributions: dict[str, float],
    incident_context: tuple[float, list[dict[str, Any]], int],
    crowd_context: tuple[float, list[dict[str, Any]], list[dict[str, Any]]],
    traffic_context: tuple[float, list[dict[str, Any]], dict[str, Any] | None],
    time_mod: float,
    weather: dict[str, Any],
    weighted_sum: float,
    weather_mult: float,
) -> list[str]:
    drivers: list[str] = []

    if contributions["incident"] > 0.05:
        _, nearby_incidents, max_sev = incident_context
        drivers.append(f"{len(nearby_incidents)} active incidents within 0.5mi (max severity: {max_sev})")

    if contributions["crowd"] > 0.05:
        _, nearby_events, nearby_areas = crowd_context
        if nearby_events:
            top_event = max(nearby_events, key=lambda event: _safe_int(event.get("expected_crowd"), 0))
            drivers.append(
                "Event: "
                f"{top_event.get('name', 'Unnamed Event')}, "
                f"est. {_safe_int(top_event.get('expected_crowd'), 0):,} attendees within 0.4mi"
            )
        elif nearby_areas:
            top_area = max(nearby_areas, key=lambda area: _safe_float(area.get("crowd_modifier"), 0.0) or 0.0)
            drivers.append(
                f"Risk area pressure: {top_area.get('name', top_area.get('id', 'unknown'))} crowd modifier "
                f"{_safe_float(top_area.get('crowd_modifier'), 0.0) or 0.0:.2f}"
            )

    if contributions["time"] > 0.05 and time_mod >= 1.0:
        escalation_rate = _safe_float(cell.get("escalation_rate"), 1.0) or 1.0
        drivers.append(
            "Historical peak hour - this zone averages "
            f"{max(escalation_rate, 1.0):.1f}x normal incident rate at this time"
        )

    if contributions["traffic"] > 0.05:
        _, _, busiest = traffic_context
        if busiest is not None:
            drivers.append(
                f"Congestion score {_safe_float(busiest.get('congestion_score'), 0.0) or 0.0:.2f} "
                f"on {busiest.get('label', busiest.get('road_id', 'nearby corridor'))}"
            )

    weather_delta = (weighted_sum * weather_mult) - weighted_sum
    if weather_delta > 0.05:
        if weather.get("alert_type"):
            drivers.append(f"Active weather alert: {weather.get('alert_type')}")
        elif (_safe_float(weather.get("wind_speed_mph"), 0.0) or 0.0) > 30:
            drivers.append("Severe wind conditions increasing response risk")
        elif (_safe_float(weather.get("visibility_miles"), 10.0) or 10.0) < 3:
            drivers.append("Reduced visibility conditions increasing response risk")

    if contributions["base"] > 0.05:
        drivers.append(f"Historical baseline risk weight {_safe_float(cell.get('heatmap_weight'), 0.0) or 0.0:.2f}")

    return drivers


def _score_cells(
    cells: list[dict[str, Any]],
    incidents: list[dict[str, Any]],
    events: list[dict[str, Any]],
    risk_areas: list[dict[str, Any]],
    traffic: list[dict[str, Any]],
    weather: dict[str, Any],
    now_utc: datetime,
) -> list[dict[str, Any]]:
    weather_mult = _weather_multiplier(weather)
    scored: list[dict[str, Any]] = []

    for cell in cells:
        if _safe_float(cell.get("lat")) is None or _safe_float(cell.get("lng")) is None:
            continue

        base_weight = _clamp_0_1(_safe_float(cell.get("heatmap_weight"), 0.0) or 0.0)
        incident_score, nearby_incidents, max_sev = _incident_modifier(cell, incidents)
        crowd_score, nearby_events, nearby_areas = _crowd_modifier(cell, events, risk_areas)
        traffic_score, nearby_traffic, busiest_road = _traffic_modifier(cell, traffic)
        time_score = _time_modifier(cell, now_utc)

        nearby_incident_count = len(nearby_incidents)
        cluster_boost = 1.4 if nearby_incident_count >= 3 else 1.0
        incident_component = min(incident_score * cluster_boost, 1.0)

        weighted_sum = (
            incident_component * 0.50
            + crowd_score * 0.20
            + base_weight * 0.15
            + traffic_score * 0.10
            + time_score * 0.05
        )
        composite = _clamp_0_1(weighted_sum * weather_mult)

        contributions = {
            "base": base_weight * 0.15 * weather_mult,
            "incident": incident_component * 0.50 * weather_mult,
            "crowd": crowd_score * 0.20 * weather_mult,
            "traffic": traffic_score * 0.10 * weather_mult,
            "time": time_score * 0.05 * weather_mult,
        }

        drivers = _build_risk_drivers(
            cell,
            contributions,
            (incident_score, nearby_incidents, max_sev),
            (crowd_score, nearby_events, nearby_areas),
            (traffic_score, nearby_traffic, busiest_road),
            time_score,
            weather,
            weighted_sum,
            weather_mult,
        )

        scored.append(
            {
                "zone_id": str(cell.get("cell_id") or f"grid_{(_safe_float(cell.get('lat'), 0.0) or 0.0):.3f}_{(_safe_float(cell.get('lng'), 0.0) or 0.0):.3f}"),
                "lat": _safe_float(cell.get("lat"), 0.0) or 0.0,
                "lng": _safe_float(cell.get("lng"), 0.0) or 0.0,
                "composite_risk_score": composite,
                "risk_drivers": drivers,
                "nearby_incidents": sorted(
                    nearby_incidents,
                    key=lambda incident: _safe_int(incident.get("normalized_severity") or incident.get("severity"), 0),
                    reverse=True,
                ),
                "nearby_events": sorted(
                    nearby_events,
                    key=lambda event: _safe_int(event.get("expected_crowd"), 0),
                    reverse=True,
                ),
                "_base_weight": base_weight,
                "_time_modifier": time_score,
            }
        )

    scored.sort(key=lambda row: _safe_float(row.get("composite_risk_score"), 0.0) or 0.0, reverse=True)
    return scored


def _build_recommended_actions(top_zones: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    priority = 1

    for zone in top_zones:
        score = _safe_float(zone.get("composite_risk_score"), 0.0) or 0.0
        if score < 0.45:
            zone["recommended_action"] = ""
            continue

        zone_label = str(zone.get("zone_id") or "zone")
        incidents = zone.get("nearby_incidents") if isinstance(zone.get("nearby_incidents"), list) else []
        events = zone.get("nearby_events") if isinstance(zone.get("nearby_events"), list) else []
        units = zone.get("nearest_available_units") if isinstance(zone.get("nearest_available_units"), list) else []

        selected_unit = units[0] if units else None
        selected_unit_code = str(selected_unit.get("unit_code")) if isinstance(selected_unit, dict) else None

        if incidents:
            max_severity = max(_safe_int(incident.get("normalized_severity") or incident.get("severity"), 0) for incident in incidents)
            action_text = f"Dispatch {selected_unit_code or 'nearest available unit'} to {zone_label}"
            rationale = (
                f"{len(incidents)} incident(s) within 0.5mi, max severity {max_severity}, "
                f"zone score {score:.2f}."
            )
            zone["recommended_action"] = action_text
            actions.append(
                {
                    "priority": priority,
                    "action": action_text,
                    "rationale": rationale,
                    "unit_code": selected_unit_code,
                    "destination_lat": _safe_float(zone.get("lat"), 0.0) or 0.0,
                    "destination_lng": _safe_float(zone.get("lng"), 0.0) or 0.0,
                }
            )
            priority += 1
        elif events and _safe_int(events[0].get("expected_crowd"), 0) >= 1000:
            top_event = events[0]
            event_name = str(top_event.get("name") or "event")
            action_text = f"Pre-position {selected_unit_code or 'nearest available unit'} near {event_name}"
            rationale = (
                f"Large event crowd expected ({_safe_int(top_event.get('expected_crowd'), 0):,}) with "
                f"zone score {score:.2f}."
            )
            zone["recommended_action"] = action_text
            actions.append(
                {
                    "priority": priority,
                    "action": action_text,
                    "rationale": rationale,
                    "unit_code": selected_unit_code,
                    "destination_lat": _safe_float(zone.get("lat"), 0.0) or 0.0,
                    "destination_lng": _safe_float(zone.get("lng"), 0.0) or 0.0,
                }
            )
            priority += 1
        elif (_safe_float(zone.get("_base_weight"), 0.0) or 0.0) >= 0.6 and (_safe_float(zone.get("_time_modifier"), 0.0) or 0.0) >= 1.0:
            action_text = f"Increase patrol frequency in {zone_label}"
            rationale = f"Historical hotspot is currently in peak hour and zone score is {score:.2f}."
            zone["recommended_action"] = action_text
            actions.append(
                {
                    "priority": priority,
                    "action": action_text,
                    "rationale": rationale,
                    "unit_code": None,
                    "destination_lat": _safe_float(zone.get("lat"), 0.0) or 0.0,
                    "destination_lng": _safe_float(zone.get("lng"), 0.0) or 0.0,
                }
            )
            priority += 1
        else:
            zone["recommended_action"] = ""

        if len(actions) >= 5:
            break

    return actions


def _summarize_units(vehicles: list[dict[str, Any]]) -> tuple[dict[str, Any], int, int, int]:
    by_type: dict[str, dict[str, int]] = {
        "police": {"total": 0, "available": 0, "deployed": 0},
        "ambulance": {"total": 0, "available": 0, "deployed": 0},
        "firetruck": {"total": 0, "available": 0, "deployed": 0},
    }

    total = len(vehicles)
    available = 0

    type_map = {
        "police": "police",
        "ambulance": "ambulance",
        "firetruck": "firetruck",
        "fire": "firetruck",
        "fire_truck": "firetruck",
        "ems": "ambulance",
    }

    for vehicle in vehicles:
        raw_type = str(vehicle.get("vehicleType") or vehicle.get("vehicle_type") or "unknown").lower().strip()
        vehicle_type = type_map.get(raw_type, raw_type)
        is_available = str(vehicle.get("status") or "").lower() in {"patrolling", "available", "idle"}

        if vehicle_type not in by_type:
            by_type[vehicle_type] = {"total": 0, "available": 0, "deployed": 0}

        by_type[vehicle_type]["total"] += 1
        if is_available:
            by_type[vehicle_type]["available"] += 1
            available += 1
        else:
            by_type[vehicle_type]["deployed"] += 1

    deployed = max(total - available, 0)

    for stats in by_type.values():
        stats["deployed"] = max(stats["total"] - stats["available"], 0)

    summary = {
        "total": total,
        "available": available,
        "deployed": deployed,
        "by_type": by_type,
        "all_units": vehicles,
    }
    return summary, total, available, deployed


def _data_quality(world_state: dict[str, Any], vehicles: list[dict[str, Any]], vehicle_data_available: bool) -> dict[str, Any]:
    record_rows: list[dict[str, Any]] = []

    for key in ("incidents", "weather", "traffic", "events", "risk_areas", "cameras", "historical"):
        record_rows.extend(_as_rows(world_state.get(key)))

    record_rows.extend(_as_rows(vehicles))

    simulated = sum(1 for row in record_rows if bool(row.get("_simulated")))
    total = len(record_rows)
    real = max(total - simulated, 0)
    simulated_pct = round((simulated / total) * 100) if total > 0 else 0

    degraded_sources: list[str] = []
    if not vehicle_data_available:
        degraded_sources.append("emergency_vehicle_state")

    return {
        "vehicle_data_available": vehicle_data_available,
        "simulated_record_count": simulated,
        "real_record_count": real,
        "simulated_pct": simulated_pct,
        "degraded_sources": degraded_sources,
    }


def _build_gemini_brief(
    payload: dict[str, Any],
    delta_incidents: list[dict[str, Any]],
    incident_state: dict[str, Any],
    resolved_ids: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Compact decision brief for Gemini.
    Contains only what changed + top zones + actionable summary.
    """
    situation = payload.get("situation_summary") if isinstance(payload.get("situation_summary"), dict) else {}
    top_zones = payload.get("top_risk_zones") if isinstance(payload.get("top_risk_zones"), list) else []

    context_note = (
        "TRIGGERED UPDATE" if payload.get("trigger_tier") else "STABLE CYCLE - no new escalations this interval"
    )

    # Priority 1: new or escalated incidents this cycle.
    delta_incs = sorted(
        [item for item in delta_incidents if _safe_int(item.get("normalized_severity"), 0) >= 3],
        key=lambda item: _safe_int(item.get("normalized_severity"), 0),
        reverse=True,
    )[:5]

    # Priority 2: if no deltas, include top active incidents by severity.
    if not delta_incs:
        active_incidents = payload.get("active_incidents") if isinstance(payload.get("active_incidents"), list) else []

        zone_points: list[dict[str, float]] = []
        for zone in top_zones[:3]:
            if not isinstance(zone, dict):
                continue
            z_lat = _safe_float(zone.get("lat"))
            z_lng = _safe_float(zone.get("lng"))
            if z_lat is None or z_lng is None:
                continue
            zone_points.append({"lat": z_lat, "lng": z_lng})

        def _distance_to_top_zones(incident: dict[str, Any]) -> float:
            i_lat = _safe_float(incident.get("lat"))
            i_lng = _safe_float(incident.get("lng"))
            if i_lat is None or i_lng is None or not zone_points:
                return float("inf")
            point = {"lat": i_lat, "lng": i_lng}
            return min(haversine(point, zone_point) for zone_point in zone_points)

        delta_incs = sorted(
            [item for item in active_incidents if isinstance(item, dict) and _safe_int(item.get("normalized_severity"), 0) >= 3],
            key=lambda item: (
                -_safe_int(item.get("normalized_severity"), 0),
                _distance_to_top_zones(item),
            ),
        )[:5]
        delta_incs = [{**item, "change_type": str(item.get("change_type") or "stable")} for item in delta_incs]

    def _slim_incident(inc: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": inc.get("id"),
            "title": inc.get("title"),
            "type": inc.get("type"),
            "normalized_severity": _safe_int(inc.get("normalized_severity"), 0),
            "lat": _safe_float(inc.get("lat"), 0.0) or 0.0,
            "lng": _safe_float(inc.get("lng"), 0.0) or 0.0,
            "description": str(inc.get("description") or "")[:150],
            "change_type": str(inc.get("change_type") or "stable"),
        }

    brief_incidents = [_slim_incident(item) for item in delta_incs]

    slim_zones: list[dict[str, Any]] = []
    for zone in top_zones[:3]:
        if not isinstance(zone, dict):
            continue

        nearest = zone.get("nearest_available_units") if isinstance(zone.get("nearest_available_units"), list) else []
        nearest_unit_code = None
        if nearest:
            first = nearest[0] if isinstance(nearest[0], dict) else {}
            nearest_unit_code = first.get("unit_code")

        slim_zones.append(
            {
                "rank": zone.get("rank"),
                "zone_id": zone.get("zone_id"),
                "composite_risk_score": zone.get("composite_risk_score"),
                "risk_drivers": [str(driver)[:80] for driver in (zone.get("risk_drivers") if isinstance(zone.get("risk_drivers"), list) else [])[:4]],
                "incident_count_nearby": len(zone.get("nearby_incidents") if isinstance(zone.get("nearby_incidents"), list) else []),
                "recommended_action": zone.get("recommended_action", ""),
                "nearest_unit": nearest_unit_code,
            }
        )

    active_critical = []
    for incident_id, value in incident_state.items():
        if not isinstance(value, dict):
            continue
        if value.get("status") == "active" and _safe_int(value.get("last_severity"), 0) >= 7:
            active_critical.append(
                {
                    "id": incident_id,
                    "severity": _safe_int(value.get("last_severity"), 0),
                }
            )
    active_critical = active_critical[:2]

    worst_traffic_rows = payload.get("traffic_summary", {}).get("worst_corridors", [])
    if not isinstance(worst_traffic_rows, list):
        worst_traffic_rows = []
    worst_traffic_rows = sorted(
        [row for row in worst_traffic_rows if isinstance(row, dict)],
        key=lambda row: _safe_float(row.get("congestion_score"), 0.0) or 0.0,
        reverse=True,
    )[:1]
    worst_traffic = [
        {
            "label": row.get("label") or row.get("road_id"),
            "congestion_score": _safe_float(row.get("congestion_score"), 0.0) or 0.0,
        }
        for row in worst_traffic_rows
    ]

    actions = payload.get("recommended_actions") if isinstance(payload.get("recommended_actions"), list) else []
    slim_actions = [
        str(action.get("action") or "").strip()
        for action in actions[:3]
        if isinstance(action, dict) and str(action.get("action") or "").strip()
    ]

    brief_payload = {
        "generated_at": payload.get("generated_at"),
        "context_note": context_note,
        "trigger_tier": payload.get("trigger_tier"),
        "trigger_reasons": payload.get("trigger_reasons", []),
        "overall_risk_level": situation.get("overall_risk_level"),
        "situation": {
            "total_active_incidents": situation.get("active_incident_count"),
            "new_or_escalated_this_cycle": len(delta_incidents),
            "resolved_this_cycle": len(resolved_ids),
            "units_available": situation.get("units_available"),
            "units_deployed": situation.get("units_deployed"),
            "weather": situation.get("weather_status"),
        },
        "delta_incidents": brief_incidents,
        "ongoing_critical": active_critical,
        "top_3_zones": slim_zones,
        "traffic_concern": worst_traffic,
        "recommended_actions": slim_actions,
        "vehicle_data_available": payload.get("vehicle_data_available"),
    }

    def _line_count(value: dict[str, Any]) -> int:
        return len(json.dumps(value, indent=2).splitlines())

    if _line_count(brief_payload) > 100:
        brief_payload["delta_incidents"] = brief_payload.get("delta_incidents", [])[:4]

    if _line_count(brief_payload) > 100:
        brief_payload["delta_incidents"] = brief_payload.get("delta_incidents", [])[:3]

    if _line_count(brief_payload) > 100:
        brief_payload["top_3_zones"] = [
            {
                **zone,
                "risk_drivers": (zone.get("risk_drivers") if isinstance(zone.get("risk_drivers"), list) else [])[:2],
            }
            for zone in brief_payload.get("top_3_zones", [])
            if isinstance(zone, dict)
        ]

    if _line_count(brief_payload) > 100:
        brief_payload["recommended_actions"] = brief_payload.get("recommended_actions", [])[:2]

    return brief_payload


def run_risk_engine() -> dict[str, Any]:
    """Runs full pipeline and returns the gemini_input dict. Also writes to file."""
    now_utc = datetime.now(timezone.utc)

    world_state = _load_json(WORLD_STATE_PATH, default={})
    if not isinstance(world_state, dict):
        world_state = {}

    historical_cache = _load_json(HISTORICAL_PATH, default=[])
    if not isinstance(historical_cache, list):
        historical_cache = []

    vehicles, _patrol_routes, vehicle_data_available = _load_vehicle_state_with_retry()

    incidents = _as_rows(world_state.get("incidents"))
    for incident in incidents:
        incident["normalized_severity"] = normalize_severity(incident)

    incident_state, delta_incidents, resolved_ids = update_incident_state(incidents)

    weather_rows = _as_rows(world_state.get("weather"))
    weather = weather_rows[0] if weather_rows else {}
    traffic = _as_rows(world_state.get("traffic"))
    for segment in traffic:
        score = _safe_float(segment.get("congestion_score"), 0.0) or 0.0
        if score >= 0.7:
            segment["congestion_level"] = "high"
        elif score >= 0.4:
            segment["congestion_level"] = "medium"
        else:
            segment["congestion_level"] = "low"

    events = _as_rows(world_state.get("events"))
    risk_areas = _as_rows(world_state.get("risk_areas"))
    historical = _as_rows(world_state.get("historical"))
    if not historical and historical_cache:
        historical = _as_rows(historical_cache)

    scored_cells = _score_cells(
        cells=historical,
        incidents=incidents,
        events=events,
        risk_areas=risk_areas,
        traffic=traffic,
        weather=weather,
        now_utc=now_utc,
    )

    top_zones: list[dict[str, Any]] = []
    for rank, zone in enumerate(scored_cells[:8], start=1):
        zone_output = {
            "rank": rank,
            "zone_id": zone["zone_id"],
            "lat": round(_safe_float(zone.get("lat"), 0.0) or 0.0, 6),
            "lng": round(_safe_float(zone.get("lng"), 0.0) or 0.0, 6),
            "composite_risk_score": round(_safe_float(zone.get("composite_risk_score"), 0.0) or 0.0, 4),
            "risk_drivers": zone.get("risk_drivers", []),
            "nearby_incidents": zone.get("nearby_incidents", []),
            "nearby_events": zone.get("nearby_events", []),
            "nearest_available_units": nearest_units(zone, vehicles, traffic, n=3),
            "recommended_action": "",
            "_base_weight": zone.get("_base_weight"),
            "_time_modifier": zone.get("_time_modifier"),
        }
        top_zones.append(zone_output)

    recommended_actions = _build_recommended_actions(top_zones)

    for zone in top_zones:
        zone.pop("_base_weight", None)
        zone.pop("_time_modifier", None)

    active_incidents = sorted(
        incidents,
        key=lambda incident: _safe_int(incident.get("normalized_severity") or incident.get("severity"), 0),
        reverse=True,
    )

    highest_incident = active_incidents[0] if active_incidents else {}
    highest_incident_payload = {
        "id": highest_incident.get("id"),
        "title": highest_incident.get("title"),
        "severity": _safe_int(highest_incident.get("normalized_severity") or highest_incident.get("severity"), 0),
        "lat": _safe_float(highest_incident.get("lat"), 0.0) or 0.0,
        "lng": _safe_float(highest_incident.get("lng"), 0.0) or 0.0,
        "description": highest_incident.get("description"),
        "type": highest_incident.get("type"),
    }

    unit_summary, units_total, units_available, units_deployed = _summarize_units(vehicles)

    sorted_traffic = sorted(
        traffic,
        key=lambda segment: _safe_float(segment.get("congestion_score"), 0.0) or 0.0,
        reverse=True,
    )

    event_risk_summary = sorted(
        [event for event in events if _safe_int(event.get("expected_crowd"), 0) >= 1000],
        key=lambda event: _safe_int(event.get("expected_crowd"), 0),
        reverse=True,
    )

    top_score = _safe_float(top_zones[0].get("composite_risk_score"), 0.0) if top_zones else 0.0
    top_score = top_score or 0.0
    overall_risk = _derive_overall_risk_level(top_score)

    payload = {
        "generated_at": now_utc.isoformat(),
        "vehicle_data_available": vehicle_data_available,
        "trigger_tier": None,
        "trigger_reasons": [],
        "situation_summary": {
            "active_incident_count": len(active_incidents),
            "highest_severity_incident": highest_incident_payload,
            "weather_status": _derive_weather_status(weather),
            "overall_risk_level": overall_risk,
            "units_total": units_total,
            "units_available": units_available,
            "units_deployed": units_deployed,
        },
        "top_risk_zones": top_zones,
        "active_incidents": active_incidents,
        "unit_status_summary": unit_summary,
        "traffic_summary": {
            "worst_corridors": sorted_traffic[:3],
            "any_closures": any(bool(segment.get("closed")) for segment in traffic),
            "high_congestion_count": sum(
                1 for segment in traffic if (_safe_float(segment.get("congestion_score"), 0.0) or 0.0) >= 0.75
            ),
        },
        "event_risk_summary": event_risk_summary,
        "recommended_actions": recommended_actions,
        "data_quality": _data_quality(world_state, vehicles, vehicle_data_available),
    }

    _atomic_write_json(GEMINI_INPUT_PATH, payload, "gemini_input.tmp")

    brief = _build_gemini_brief(payload, delta_incidents, incident_state, resolved_ids)
    _atomic_write_json(GEMINI_BRIEF_PATH, brief, "gemini_brief.tmp")

    _log(f"updated - overall risk: {overall_risk}, top zone score: {top_score:.2f}")
    return payload
