from __future__ import annotations

import math
import random
from datetime import datetime
from pathlib import Path
from typing import Any

import networkx as nx
import osmnx as ox

from schemas import Unit

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
GRAPH_PATH = DATA_DIR / "tempe_road_graph.graphml"

STATIONS = [
    {"id": "TFD_1", "type": "fire", "lat": 33.4170, "lng": -111.9102, "name": "Tempe Fire Station 1"},
    {"id": "TFD_2", "type": "fire", "lat": 33.3895, "lng": -111.9167, "name": "Tempe Fire Station 2"},
    {"id": "TFD_3", "type": "fire", "lat": 33.3790, "lng": -111.9580, "name": "Tempe Fire Station 3"},
    {"id": "TFD_6", "type": "fire", "lat": 33.4090, "lng": -111.9580, "name": "Tempe Fire Station 6"},
    {"id": "TPD_HQ", "type": "police", "lat": 33.4136, "lng": -111.9367, "name": "Tempe PD HQ"},
    {"id": "TPD_S", "type": "police", "lat": 33.3724, "lng": -111.9289, "name": "Tempe PD South"},
]

HOTSPOTS = [
    {"name": "Mill Ave Corridor", "lat": 33.4192, "lng": -111.9340, "weight": 0.35},
    {"name": "ASU Campus", "lat": 33.4242, "lng": -111.9281, "weight": 0.30},
    {"name": "Tempe Marketplace", "lat": 33.4000, "lng": -111.9000, "weight": 0.20},
    {"name": "Tempe Town Lake", "lat": 33.4280, "lng": -111.9350, "weight": 0.15},
]

PATROL_UNIT_COUNT = 8

_GRAPH: nx.MultiDiGraph | None = None
_UNITS: list[dict[str, Any]] = []
_META: dict[str, dict[str, Any]] = {}
_SIM_SECONDS = 0
_LAST_DISPATCH = 0


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _node_lat_lng(node_id: Any) -> tuple[float, float]:
    if _GRAPH is None:
        return 33.42, -111.93
    node = _GRAPH.nodes[node_id]
    return float(node["y"]), float(node["x"])


def _nearest_node(lat: float, lng: float) -> Any | None:
    if _GRAPH is None:
        return None
    try:
        return ox.distance.nearest_nodes(_GRAPH, X=lng, Y=lat)
    except Exception:
        return None


def _bearing(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta = math.radians(lng2 - lng1)
    y = math.sin(delta) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(delta)
    brng = math.degrees(math.atan2(y, x))
    return (brng + 360.0) % 360.0


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 3958.8
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lng / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _best_edge(u: Any, v: Any) -> dict[str, Any]:
    if _GRAPH is None:
        return {}
    edge_data = _GRAPH.get_edge_data(u, v, default={})
    if not edge_data:
        return {}
    return min(edge_data.values(), key=lambda e: float(e.get("travel_time", 10**9)))


def _edge_length_miles(u: Any, v: Any) -> float:
    edge = _best_edge(u, v)
    length_m = float(edge.get("length", 0.0))
    return max(length_m / 1609.34, 0.0001)


def _extract_mph_from_maxspeed(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, list) and value:
        value = value[0]
    text = str(value).lower().strip()
    number = "".join(ch for ch in text if ch.isdigit() or ch == ".")
    if not number:
        return None
    try:
        speed = float(number)
        if "km" in text or "kph" in text:
            return speed * 0.621371
        return speed
    except ValueError:
        return None


def _edge_speed_limit_mph(u: Any, v: Any) -> float:
    edge = _best_edge(u, v)
    maxspeed_mph = _extract_mph_from_maxspeed(edge.get("maxspeed"))
    if maxspeed_mph is not None:
        return max(maxspeed_mph, 10.0)

    speed_kph = edge.get("speed_kph")
    if speed_kph is not None:
        try:
            return max(float(speed_kph) * 0.621371, 10.0)
        except (TypeError, ValueError):
            pass

    return 35.0


def _nearest_congestion(lat: float, lng: float, traffic_data: list[dict[str, Any]]) -> float:
    if not traffic_data:
        return 0.2
    best_score = 0.2
    best_dist = float("inf")
    for segment in traffic_data:
        s_lat = segment.get("lat")
        s_lng = segment.get("lng")
        if s_lat is None or s_lng is None:
            continue
        dist = _haversine_miles(lat, lng, float(s_lat), float(s_lng))
        if dist < best_dist:
            best_dist = dist
            best_score = float(segment.get("congestion_score", 0.2))
    return max(0.0, min(1.0, best_score))


def _load_graph() -> nx.MultiDiGraph | None:
    _ensure_data_dir()
    try:
        if GRAPH_PATH.exists():
            graph = ox.load_graphml(GRAPH_PATH)
        else:
            graph = ox.graph_from_place("Tempe, Arizona, USA", network_type="drive")
            ox.save_graphml(graph, GRAPH_PATH)

        graph = ox.routing.add_edge_speeds(graph)
        graph = ox.routing.add_edge_travel_times(graph)
        _log("units graph ready: drive network loaded")
        return graph
    except Exception as err:
        _log(f"units graph warning: {err}")
        return None


def _new_unit(unit_id: str, unit_type: str, lat: float, lng: float) -> dict[str, Any]:
    return Unit(
        unit_id=unit_id,
        type=unit_type,
        lat=lat,
        lng=lng,
        status="available",
        availability=True,
        assigned_incident=None,
        siren_active=False,
        current_speed_mph=0.0,
        heading_deg=0.0,
        route_node_index=0,
        route_nodes=[],
        _simulated=True,
    ).to_dict()


def _initialize() -> None:
    global _GRAPH, _UNITS, _META
    if _UNITS:
        return

    _GRAPH = _load_graph()

    for station in STATIONS:
        unit = _new_unit(station["id"], station["type"], station["lat"], station["lng"])
        _UNITS.append(unit)
        _META[unit["unit_id"]] = {
            "home": station,
            "home_node": _nearest_node(station["lat"], station["lng"]),
            "target_lat": station["lat"],
            "target_lng": station["lng"],
            "target_node": None,
            "on_scene_remaining": 0,
            "route_total": 0,
        }

    patrol_seed_stations = [s for s in STATIONS if s["type"] == "police"]
    for idx in range(PATROL_UNIT_COUNT):
        station = random.choice(patrol_seed_stations)
        lat = station["lat"] + random.uniform(-0.003, 0.003)
        lng = station["lng"] + random.uniform(-0.003, 0.003)
        unit_id = f"TPD_{idx + 10}"
        unit = _new_unit(unit_id, "police", lat, lng)
        _UNITS.append(unit)
        _META[unit_id] = {
            "home": station,
            "home_node": _nearest_node(station["lat"], station["lng"]),
            "target_lat": lat,
            "target_lng": lng,
            "target_node": None,
            "on_scene_remaining": 0,
            "route_total": 0,
        }

    _log(f"units initialized: {len(_UNITS)} records")


def _build_route(start_lat: float, start_lng: float, target_lat: float, target_lng: float) -> list[Any]:
    if _GRAPH is None:
        return []

    start_node = _nearest_node(start_lat, start_lng)
    target_node = _nearest_node(target_lat, target_lng)
    if start_node is None or target_node is None:
        return []

    try:
        return nx.shortest_path(_GRAPH, source=start_node, target=target_node, weight="travel_time")
    except Exception:
        return []


def _set_route(unit: dict[str, Any], target_lat: float, target_lng: float) -> None:
    route = _build_route(unit["lat"], unit["lng"], target_lat, target_lng)
    unit["route_nodes"] = route
    unit["route_node_index"] = 0
    meta = _META[unit["unit_id"]]
    meta["target_lat"] = target_lat
    meta["target_lng"] = target_lng
    meta["target_node"] = route[-1] if route else None
    meta["route_total"] = len(route)


def _weighted_hotspot() -> dict[str, Any]:
    choices = HOTSPOTS
    weights = [item["weight"] for item in choices]
    return random.choices(choices, weights=weights, k=1)[0]


def _has_fire_medical_incident(incidents: list[dict[str, Any]]) -> bool:
    for incident in incidents:
        text = " ".join(
            str(incident.get(k, "")) for k in ("type", "title", "description", "category")
        ).lower()
        if any(word in text for word in ("fire", "medical", "ems", "ambulance")):
            return True
    return False


def _pick_incident_point(incidents: list[dict[str, Any]]) -> tuple[float, float, str] | None:
    ranked: list[tuple[float, float, str]] = []
    for incident in incidents:
        lat = incident.get("lat")
        lng = incident.get("lng")
        if lat is None or lng is None:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (TypeError, ValueError):
            continue
        incident_id = str(incident.get("id") or incident.get("key") or "incident")
        ranked.append((lat_f, lng_f, incident_id))

    if not ranked:
        return None
    return random.choice(ranked)


def _dispatch_units(incidents: list[dict[str, Any]]) -> None:
    police_units = [u for u in _UNITS if u["type"] == "police" and u["availability"]]
    if police_units:
        unit = random.choice(police_units)
        hotspot = _weighted_hotspot()
        target_lat = hotspot["lat"] + random.uniform(-0.005, 0.005)
        target_lng = hotspot["lng"] + random.uniform(-0.005, 0.005)
        _set_route(unit, target_lat, target_lng)
        unit["status"] = "dispatched"
        unit["availability"] = False
        unit["assigned_incident"] = hotspot["name"]
        unit["siren_active"] = True

    if not _has_fire_medical_incident(incidents):
        return

    fire_units = [u for u in _UNITS if u["type"] == "fire" and u["availability"]]
    if not fire_units:
        return

    incident_point = _pick_incident_point(incidents)
    if incident_point is None:
        return

    lat, lng, incident_id = incident_point
    unit = random.choice(fire_units)
    _set_route(unit, lat, lng)
    unit["status"] = "dispatched"
    unit["availability"] = False
    unit["assigned_incident"] = incident_id
    unit["siren_active"] = True


def _move_unit_along_route(unit: dict[str, Any], traffic_data: list[dict[str, Any]], dt_seconds: int) -> None:
    route_nodes = unit["route_nodes"]
    if _GRAPH is None or len(route_nodes) < 2:
        return

    siren = bool(unit["siren_active"])
    speed_limit = _edge_speed_limit_mph(route_nodes[0], route_nodes[1])
    if siren:
        speed_mph = min(speed_limit * 1.5, 65.0)
    else:
        congestion = _nearest_congestion(unit["lat"], unit["lng"], traffic_data)
        speed_mph = max(speed_limit * (1 - congestion * 0.6), 8.0)

    miles_budget = speed_mph * (dt_seconds / 3600.0)
    unit["current_speed_mph"] = round(speed_mph, 2)

    while miles_budget > 0 and len(route_nodes) >= 2:
        u = route_nodes[0]
        v = route_nodes[1]
        edge_miles = _edge_length_miles(u, v)

        u_lat, u_lng = _node_lat_lng(u)
        v_lat, v_lng = _node_lat_lng(v)

        if miles_budget >= edge_miles:
            unit["lat"], unit["lng"] = v_lat, v_lng
            route_nodes.pop(0)
            miles_budget -= edge_miles
            if not siren:
                miles_budget -= speed_mph * (2 / 3600.0)
        else:
            ratio = max(0.0, min(1.0, miles_budget / edge_miles))
            unit["lat"] = u_lat + (v_lat - u_lat) * ratio
            unit["lng"] = u_lng + (v_lng - u_lng) * ratio
            miles_budget = 0

    if len(route_nodes) >= 2:
        n_lat, n_lng = _node_lat_lng(route_nodes[1])
        unit["heading_deg"] = round(_bearing(unit["lat"], unit["lng"], n_lat, n_lng), 1)

    route_total = _META[unit["unit_id"]].get("route_total", len(route_nodes))
    unit["route_node_index"] = max(route_total - len(route_nodes), 0)


def _arrive_if_needed(unit: dict[str, Any]) -> None:
    if unit["route_nodes"] and len(unit["route_nodes"]) > 1:
        return

    if unit["status"] == "dispatched":
        unit["status"] = "on_scene"
        unit["siren_active"] = False
        unit["current_speed_mph"] = 0.0
        unit["route_nodes"] = []
        _META[unit["unit_id"]]["on_scene_remaining"] = random.randint(180, 480)
        return

    if unit["status"] == "returning":
        unit["status"] = "available"
        unit["availability"] = True
        unit["assigned_incident"] = None
        unit["siren_active"] = False
        unit["current_speed_mph"] = 0.0
        unit["route_nodes"] = []
        unit["route_node_index"] = 0


def _process_on_scene(unit: dict[str, Any], dt_seconds: int) -> None:
    if unit["status"] != "on_scene":
        return

    meta = _META[unit["unit_id"]]
    meta["on_scene_remaining"] = max(0, meta.get("on_scene_remaining", 0) - dt_seconds)
    if meta["on_scene_remaining"] > 0:
        unit["current_speed_mph"] = 0.0
        return

    home = meta["home"]
    _set_route(unit, home["lat"], home["lng"])
    unit["status"] = "returning"
    unit["availability"] = False
    unit["siren_active"] = False


def get_unit_states() -> list[dict[str, Any]]:
    _initialize()
    return [dict(unit) for unit in _UNITS]


def tick(traffic_data: list, incidents: list, dt_seconds: int = 30) -> list[dict[str, Any]]:
    global _SIM_SECONDS, _LAST_DISPATCH
    _initialize()

    _SIM_SECONDS += dt_seconds
    if _SIM_SECONDS - _LAST_DISPATCH >= 45:
        _dispatch_units(incidents)
        _LAST_DISPATCH = _SIM_SECONDS

    for unit in _UNITS:
        if unit["status"] in ("dispatched", "returning"):
            _move_unit_along_route(unit, traffic_data, dt_seconds)
            _arrive_if_needed(unit)
        elif unit["status"] == "on_scene":
            _process_on_scene(unit, dt_seconds)
        else:
            unit["current_speed_mph"] = 0.0
            unit["route_node_index"] = 0
            unit["route_nodes"] = []

    _log(f"units updated: {len(_UNITS)} records")
    return get_unit_states()


if __name__ == "__main__":
    states = get_unit_states()
    _log(f"units ready: {len(states)} records")
