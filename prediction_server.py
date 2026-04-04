from __future__ import annotations

import hashlib
import json
import math
from functools import lru_cache
from pathlib import Path
from threading import Lock

import networkx as nx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

app = FastAPI(title='LowTide Risk Prediction API', version='1.0.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

GRAPH_PATH = Path(__file__).resolve().parent / 'data_collection' / 'data' / 'tempe_road_graph.graphml'
EMERGENCY_SNAPSHOT_PATH = (
    Path(__file__).resolve().parent / 'data_collection' / 'data' / 'emergency_vehicle_state.json'
)
_emergency_snapshot_lock = Lock()
_last_emergency_snapshot_digest: str | None = None


class CoordinatePayload(BaseModel):
    latitude: float
    longitude: float


class DispatchRouteRequest(BaseModel):
    start: CoordinatePayload
    target: CoordinatePayload


class WaypointRouteRequest(BaseModel):
    waypoints: list[CoordinatePayload]
    is_loop: bool = False


class EmergencyTelemetryRoute(BaseModel):
    id: str
    label: str
    points: list[tuple[float, float]]


class EmergencyTelemetryVehicle(BaseModel):
    key: str
    unitCode: str
    vehicleType: str
    status: str
    latitude: float
    longitude: float
    headingDegrees: float
    speedMph: float
    routeLabel: str
    patrolRouteId: str | None = None
    activeRouteType: str
    dispatchRoutePoints: list[tuple[float, float]] | None = None


class EmergencyTelemetrySnapshot(BaseModel):
    timestamp: int
    simulationIntervalMs: int
    patrolRoutes: list[EmergencyTelemetryRoute]
    vehicles: list[EmergencyTelemetryVehicle]

PREDICTION_RESPONSE = {
    'prediction_id': 'risk-analysis-2026-04-04-8832',
    'timestamp': '2026-04-04T13:25:00Z',
    'risk_assessment': {
        'level': 'High',
        'coordinates': {
            'latitude': 33.4255,
            'longitude': -111.9400,
        },
        'location_name': 'Mill Avenue & University Drive',
        'risk_factors': [
            'Heavy congestion following a stadium event',
            'Historical data indicating high pedestrian-vehicle conflict at this hour',
            'Recent social media reports of an unsanctioned street gathering nearby',
        ],
        'explanation': "A high-risk event is predicted due to the convergence of 'after-stadium' foot traffic and peak-hour vehicle congestion. The risk is compounded by recent citizen incident reports of aggressive driving in the immediate vicinity and a scheduled large-scale street festival nearby that has exceeded its planned capacity, creating a high probability of crowd crush or pedestrian-involved collisions.",
    },
    'mitigation_strategy': {
        'police_dispatch': {
            'action': 'Deploy for traffic calming and crowd monitoring',
            'assigned_units': [
                {
                    'vehicle_id': 'P-104',
                    'status': 'En route',
                },
                {
                    'vehicle_id': 'P-212',
                    'status': 'En route',
                },
            ],
        },
        'medical_standby': {
            'unit_id': 'AMB-09',
            'instruction': 'Pre-notification',
            'message': 'Potential high-density incident at Mill & University. No immediate dispatch required; remain on standby at Station 4 for rapid response if situation escalates.',
            'standby_location': {
                'latitude': 33.4220,
                'longitude': -111.9350,
            },
        },
        'traffic_control': {
            're-routing': 'Automated signal timing adjustment implemented for Northbound traffic to reduce pedestrian dwell time.',
        },
    },
}


def _to_float(value: object) -> float | None:
    if isinstance(value, (int, float)):
        value_float = float(value)
        return value_float if math.isfinite(value_float) else None

    if isinstance(value, str):
        candidate = value.strip()

        if not candidate:
            return None

        if ',' in candidate and candidate.count(',') == 1 and '[' in candidate:
            candidate = candidate.strip('[]').split(',')[0].strip()

        try:
            value_float = float(candidate)
            return value_float if math.isfinite(value_float) else None
        except ValueError:
            return None

    return None


def _haversine_meters(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    earth_radius_m = 6_371_000
    latitude_a = math.radians(lat_a)
    latitude_b = math.radians(lat_b)
    latitude_delta = math.radians(lat_b - lat_a)
    longitude_delta = math.radians(lon_b - lon_a)

    chord = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(latitude_a) * math.cos(latitude_b) * math.sin(longitude_delta / 2) ** 2
    )

    return 2 * earth_radius_m * math.asin(math.sqrt(chord))


def _edge_length_meters(edge_data: dict) -> float:
    parsed_length = _to_float(edge_data.get('length'))

    if parsed_length is not None and parsed_length > 0:
        return parsed_length

    return 1.0


def _add_routing_edge(
    routing_graph: nx.DiGraph,
    source_node: str,
    target_node: str,
    edge_data: dict,
) -> None:
    source_node_key = str(source_node)
    target_node_key = str(target_node)

    if not routing_graph.has_node(source_node_key) or not routing_graph.has_node(target_node_key):
        return

    length_meters = _edge_length_meters(edge_data)

    if routing_graph.has_edge(source_node_key, target_node_key):
        if length_meters < float(routing_graph[source_node_key][target_node_key]['length']):
            routing_graph[source_node_key][target_node_key]['length'] = length_meters
        return

    routing_graph.add_edge(source_node_key, target_node_key, length=length_meters)


@lru_cache(maxsize=1)
def _load_routing_graph() -> nx.DiGraph:
    if not GRAPH_PATH.exists():
        raise FileNotFoundError(f'GraphML file not found at {GRAPH_PATH}')

    raw_graph = nx.read_graphml(GRAPH_PATH)
    routing_graph = nx.DiGraph()

    for node_id, node_data in raw_graph.nodes(data=True):
        longitude = _to_float(node_data.get('x'))
        latitude = _to_float(node_data.get('y'))

        if longitude is None or latitude is None:
            continue

        routing_graph.add_node(str(node_id), x=longitude, y=latitude)

    if isinstance(raw_graph, (nx.MultiDiGraph, nx.MultiGraph)):
        for source_node, target_node, _edge_key, edge_data in raw_graph.edges(keys=True, data=True):
            _add_routing_edge(routing_graph, source_node, target_node, edge_data)
    else:
        for source_node, target_node, edge_data in raw_graph.edges(data=True):
            _add_routing_edge(routing_graph, source_node, target_node, edge_data)

    if routing_graph.number_of_nodes() == 0 or routing_graph.number_of_edges() == 0:
        raise ValueError('Loaded graph has no routable nodes/edges.')

    return routing_graph


def _nearest_node_id(routing_graph: nx.DiGraph, coordinate: CoordinatePayload) -> str:
    nearest_node = None
    nearest_distance = math.inf

    for node_id, node_data in routing_graph.nodes(data=True):
        node_latitude = float(node_data['y'])
        node_longitude = float(node_data['x'])
        candidate_distance = _haversine_meters(
            coordinate.latitude,
            coordinate.longitude,
            node_latitude,
            node_longitude,
        )

        if candidate_distance < nearest_distance:
            nearest_distance = candidate_distance
            nearest_node = str(node_id)

    if nearest_node is None:
        raise ValueError('Unable to resolve nearest graph node.')

    return nearest_node


def _polyline_distance_meters(polyline: list[tuple[float, float]]) -> float:
    if len(polyline) < 2:
        return 0.0

    total_distance = 0.0

    for index in range(len(polyline) - 1):
        lon_a, lat_a = polyline[index]
        lon_b, lat_b = polyline[index + 1]
        total_distance += _haversine_meters(lat_a, lon_a, lat_b, lon_b)

    return total_distance


def _write_json_atomic(path: Path, serialized_json: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f'{path.suffix}.tmp')
    temp_path.write_text(serialized_json, encoding='utf-8')
    temp_path.replace(path)


def _shortest_path_coordinates(
    routing_graph: nx.DiGraph,
    start_coordinate: CoordinatePayload,
    target_coordinate: CoordinatePayload,
) -> tuple[list[tuple[float, float]], float]:
    start_node = _nearest_node_id(routing_graph, start_coordinate)
    target_node = _nearest_node_id(routing_graph, target_coordinate)
    shortest_node_path = nx.shortest_path(
        routing_graph,
        source=start_node,
        target=target_node,
        weight='length',
    )

    coordinates = [
        (float(routing_graph.nodes[node_id]['x']), float(routing_graph.nodes[node_id]['y']))
        for node_id in shortest_node_path
    ]

    return coordinates, _polyline_distance_meters(coordinates)


def _waypoint_route_coordinates(
    routing_graph: nx.DiGraph,
    waypoints: list[CoordinatePayload],
    is_loop: bool,
) -> tuple[list[tuple[float, float]], float]:
    if len(waypoints) < 2:
        raise ValueError('At least two waypoints are required to compute a route.')

    route_waypoints = waypoints.copy()

    if is_loop:
        first_waypoint = route_waypoints[0]
        last_waypoint = route_waypoints[-1]

        if (
            abs(first_waypoint.latitude - last_waypoint.latitude) > 1e-8
            or abs(first_waypoint.longitude - last_waypoint.longitude) > 1e-8
        ):
            route_waypoints.append(first_waypoint)

    route_coordinates: list[tuple[float, float]] = []
    route_distance = 0.0

    for waypoint_index in range(len(route_waypoints) - 1):
        segment_coordinates, segment_distance = _shortest_path_coordinates(
            routing_graph,
            route_waypoints[waypoint_index],
            route_waypoints[waypoint_index + 1],
        )

        if waypoint_index > 0 and route_coordinates and segment_coordinates:
            segment_coordinates = segment_coordinates[1:]

        route_coordinates.extend(segment_coordinates)
        route_distance += segment_distance

    return route_coordinates, route_distance


@app.get('/')
def healthcheck() -> dict[str, str]:
    return {'status': 'ok', 'service': 'risk-prediction-api'}


@app.get('/prediction')
def get_prediction() -> dict:
    return PREDICTION_RESPONSE


@app.post('/route/dispatch')
def route_dispatch(payload: DispatchRouteRequest) -> dict:
    try:
        routing_graph = _load_routing_graph()
        route_coordinates, route_distance = _shortest_path_coordinates(
            routing_graph,
            payload.start,
            payload.target,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except (ValueError, nx.NetworkXNoPath, nx.NodeNotFound) as exc:
        raise HTTPException(status_code=404, detail='No routable path found on Tempe road graph.') from exc

    return {
        'coordinates': route_coordinates,
        'distance_meters': route_distance,
        'source': 'tempe_road_graph.graphml',
    }


@app.post('/route/waypoints')
def route_waypoints(payload: WaypointRouteRequest) -> dict:
    try:
        routing_graph = _load_routing_graph()
        route_coordinates, route_distance = _waypoint_route_coordinates(
            routing_graph,
            payload.waypoints,
            payload.is_loop,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except (ValueError, nx.NetworkXNoPath, nx.NodeNotFound) as exc:
        raise HTTPException(status_code=404, detail='No waypoint route found on Tempe road graph.') from exc

    return {
        'coordinates': route_coordinates,
        'distance_meters': route_distance,
        'source': 'tempe_road_graph.graphml',
    }


@app.get('/telemetry/emergency-snapshot')
def get_emergency_snapshot() -> dict:
    if not EMERGENCY_SNAPSHOT_PATH.exists():
        return {
            'timestamp': 0,
            'simulationIntervalMs': 0,
            'patrolRoutes': [],
            'vehicles': [],
        }

    try:
        return json.loads(EMERGENCY_SNAPSHOT_PATH.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail='Snapshot file is corrupted.') from exc


@app.post('/telemetry/emergency-snapshot')
def save_emergency_snapshot(payload: EmergencyTelemetrySnapshot) -> dict:
    global _last_emergency_snapshot_digest

    snapshot_dict = payload.model_dump()
    serialized_snapshot = json.dumps(snapshot_dict, separators=(',', ':'), ensure_ascii=False)
    snapshot_digest = hashlib.sha256(serialized_snapshot.encode('utf-8')).hexdigest()

    with _emergency_snapshot_lock:
        if snapshot_digest == _last_emergency_snapshot_digest:
            return {
                'status': 'unchanged',
                'vehicles': len(payload.vehicles),
                'file': str(EMERGENCY_SNAPSHOT_PATH),
            }

        _write_json_atomic(EMERGENCY_SNAPSHOT_PATH, serialized_snapshot)
        _last_emergency_snapshot_digest = snapshot_digest

    return {
        'status': 'written',
        'vehicles': len(payload.vehicles),
        'file': str(EMERGENCY_SNAPSHOT_PATH),
    }


if __name__ == '__main__':
    uvicorn.run('prediction_server:app', host='0.0.0.0', port=8000, reload=True)
