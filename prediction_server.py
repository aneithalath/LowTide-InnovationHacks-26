from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from threading import Lock

import networkx as nx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pydantic import BaseModel
import uvicorn
from openai import OpenAI

app = FastAPI(title='LowTide Risk Prediction API', version='1.0.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

# Load local environment variables (for example: NVIDIA_API_KEY).
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / '.env')


@lru_cache(maxsize=1)
def _get_nvidia_client() -> OpenAI:
    nvidia_api_key = os.getenv('NVIDIA_API_KEY', '').strip()
    if not nvidia_api_key:
        raise RuntimeError('NVIDIA_API_KEY is not set. Add it to .env before starting the server.')
    return OpenAI(
        base_url='https://integrate.api.nvidia.com/v1',
        api_key=nvidia_api_key,
    )

GRAPH_PATH = PROJECT_ROOT / 'data_collection' / 'data' / 'tempe_road_graph.graphml'
EMERGENCY_SNAPSHOT_PATH = (
    PROJECT_ROOT / 'data_collection' / 'data' / 'emergency_vehicle_state.json'
)
PREDICTION_SNAPSHOT_PATH = (
    PROJECT_ROOT / 'data_collection' / 'data' / 'prediction_snapshot.json'
)

_emergency_snapshot_lock  = Lock()
_prediction_snapshot_lock = Lock()
_last_emergency_snapshot_digest:  str | None = None
_last_prediction_snapshot_digest: str | None = None

_DISPATCHABLE_STATUSES = {'patrolling', 'staged'}
_UNIT_TYPE_TO_VEHICLE_TYPE = {
    'patrol': 'police',
    'ems': 'ambulance',
    'fire': 'firetruck',
}
_VEHICLE_TYPE_LABELS = {
    'police': 'patrol car',
    'ambulance': 'ambulance',
    'firetruck': 'firetruck',
}
_VEHICLE_TYPE_ALIASES = {
    'police': 'police',
    'patrol': 'police',
    'patrolcar': 'police',
    'patrol car': 'police',
    'policecar': 'police',
    'police car': 'police',
    'ambulance': 'ambulance',
    'ems': 'ambulance',
    'medic': 'ambulance',
    'fire': 'firetruck',
    'firetruck': 'firetruck',
    'fire truck': 'firetruck',
    'engine': 'firetruck',
}


# ─────────────────────────────────────────────────────────────────
# INCIDENT DATA & UNIT POOLS (FROM JOUT.PY)
# ─────────────────────────────────────────────────────────────────

INCIDENTS_SOURCE = [
    {
        'id': 'inc_ds_citizen_1',
        'location_name': 'Tempe Marketplace Transit Corridor',
        'coordinates': {'latitude': 33.4306, 'longitude': -111.8997},
        "reports": [
            'Source feed: Citizen Trending API | data_type=both | auth_required=no',
            'Signal summary: Crowd panic and suspicious-person reports increasing in the same 10 minute window.',
            'Update frequency: every 2 min | fake_score=0.88',
        ],
        'source_api': 'Citizen Trending API',
        'source_data_type': 'both',
        'api_key_name': None,
        'fake_score': 0.88,
    },
    {
        'id': 'inc_ds_weather_1',
        'location_name': 'Tempe Town Lake East Basin',
        'coordinates': {'latitude': 33.4332, 'longitude': -111.9239},
        "reports": [
            'Source feed: OpenWeatherMap Current Weather | data_type=both | api_key=OPENWEATHER_API_KEY',
            'Signal summary: Wind gust and heat index spike could stress large outdoor crowds.',
            'Update frequency: every 5 min | fake_score=0.74',
        ],
        'source_api': 'OpenWeatherMap Current Weather',
        'source_data_type': 'both',
        'api_key_name': 'OPENWEATHER_API_KEY',
        'fake_score': 0.74,
    },
    {
        'id': 'inc_ds_traffic_1',
        'location_name': 'Mill Ave Bridge and Rio Salado Pkwy',
        'coordinates': {'latitude': 33.4361, 'longitude': -111.9392},
        "reports": [
            'Source feed: TomTom Traffic Flow | data_type=both | api_key=TOMTOM_API_KEY',
            'Signal summary: Congestion crossed severe threshold with queue growth near stadium egress routes.',
            'Update frequency: every 10 min | fake_score=0.82',
        ],
        'source_api': 'TomTom Traffic Flow',
        'source_data_type': 'both',
        'api_key_name': 'TOMTOM_API_KEY',
        'fake_score': 0.82,
    },
    {
        'id': 'inc_ds_event_1',
        'location_name': 'ASU Stadium District Perimeter',
        'coordinates': {'latitude': 33.4269, 'longitude': -111.9325},
        "reports": [
            'Source feed: Exa Search API | data_type=both | api_key=EXA_API_KEY',
            'Signal summary: Multiple large event mentions indicate attendance beyond venue baseline capacity.',
            'Update frequency: every 60 min | fake_score=0.79',
        ],
        'source_api': 'Exa Search API',
        'source_data_type': 'both',
        'api_key_name': 'EXA_API_KEY',
        'fake_score': 0.79,
    },
    {
        'id': 'inc_ds_historical_1',
        'location_name': 'Apache Blvd and McClintock Dr',
        'coordinates': {'latitude': 33.4147, 'longitude': -111.9093},
        "reports": [
            'Source feed: Tempe ArcGIS Calls For Service FeatureServer | data_type=real | auth_required=no',
            'Signal summary: Historical call density hotspot and active corridor load overlap during peak hour.',
            'Update frequency: startup/background cache | fake_score=0.69',
        ],
        'source_api': 'Tempe ArcGIS Calls For Service FeatureServer',
        'source_data_type': 'real',
        'api_key_name': None,
        'fake_score': 0.69,
    },
    {
        'id': 'inc_ds_weather_alert_1',
        'location_name': 'Downtown Tempe and University Dr',
        'coordinates': {'latitude': 33.4242, 'longitude': -111.9405},
        'reports': [
            'Source feed: NOAA/NWS Alerts (api.weather.gov) | data_type=real | auth_required=no',
            'Signal summary: Active weather alert intersects dense pedestrian and traffic corridors.',
            'Update frequency: every 5 min | fake_score=0.77',
        ],
        'source_api': 'NOAA/NWS Alerts',
        'source_data_type': 'real',
        'api_key_name': None,
        'fake_score': 0.77,
    },
]

PATROL_UNITS = [("P-104", "En route"), ("P-212", "En route"), ("P-305", "Available")]
EMS_UNITS    = [
    ("TEMPE-AMB-01", 33.4220, -111.9350),
    ("TEMPE-AMB-02", 33.4410, -111.9600),
]
FIRE_UNITS   = [
    ("TEMPE-FIRE-01", 33.4500, -111.9900),
    ("TEMPE-FIRE-02", 33.4100, -112.0200),
]

_POLICE_DISPATCH_ACTIONS = {
    'dispatch_to_risk': (
        'Dispatch assigned patrol units to {location_name} and hold position for rapid deterrence and scene control.'
    ),
    'establish_perimeter': (
        'Dispatch assigned patrol units to {location_name} and stage on the perimeter for access control.'
    ),
    'intersection_control': (
        'Dispatch assigned patrol units to {location_name} and stage at adjacent intersections for flow control.'
    ),
}
_EMS_DISPATCH_ACTIONS = {
    'dispatch_to_risk': (
        'Dispatch assigned ambulances to {location_name} for immediate triage-ready coverage.'
    ),
    'staged_triage': (
        'Dispatch assigned ambulances to {location_name} and stage a triage-ready treatment point.'
    ),
    'casualty_collection_point': (
        'Dispatch assigned ambulances to {location_name} and stage for patient pickup and transport rotation.'
    ),
}
_FIRE_DISPATCH_ACTIONS = {
    'dispatch_to_risk': (
        'Dispatch assigned firetrucks to {location_name} for immediate scene safety support.'
    ),
    'hazard_assessment': (
        'Dispatch assigned firetrucks to {location_name} for hazard assessment and safety staging.'
    ),
    'water_supply_staging': (
        'Dispatch assigned firetrucks to {location_name} and stage for rapid suppression support if conditions worsen.'
    ),
}
_MEDICAL_STANDBY_ACTIONS = {
    'preposition_at_standby': (
        'Pre-position {unit_id} at the standby point near {location_name} for rapid escalation coverage.'
    ),
    'reserve_for_escalation': (
        'Hold {unit_id} at the standby point near {location_name} and remain ready for immediate escalation.'
    ),
}
_TRAFFIC_CONTROL_ACTIONS = {
    'reroute_event_egress': (
        'Apply event-egress rerouting around {location_name} to reduce conflict density.'
    ),
    'signal_priority_corridor': (
        'Apply temporary signal timing priority around {location_name} to clear emergency approaches.'
    ),
    'meter_inbound_flow': (
        'Meter inbound flow near {location_name} and divert through alternate arterials.'
    ),
}

_DEFAULT_POLICE_DISPATCH_ACTION_ID = 'dispatch_to_risk'
_DEFAULT_EMS_DISPATCH_ACTION_ID = 'staged_triage'
_DEFAULT_FIRE_DISPATCH_ACTION_ID = 'hazard_assessment'
_DEFAULT_MEDICAL_STANDBY_ACTION_ID = 'preposition_at_standby'
_DEFAULT_TRAFFIC_CONTROL_ACTION_ID = 'signal_priority_corridor'


# ─────────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────
# DEFAULT PREDICTION RESPONSE
# Served until a live AI refresh succeeds.
# ─────────────────────────────────────────────────────────────────

_DEFAULT_PREDICTION_RESPONSE: list[dict] = [
    {
        'prediction_id': 'risk-analysis-2026-04-04-8832',
        'timestamp': '2026-04-04T13:25:00Z',
        'id': 'inc_default',
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
            'explanation': (
                "A high-risk event is predicted due to the convergence of 'after-stadium' "
                'foot traffic and peak-hour vehicle congestion. The risk is compounded by '
                'recent citizen incident reports of aggressive driving in the immediate '
                'vicinity and a scheduled large-scale street festival nearby that has '
                'exceeded its planned capacity, creating a high probability of crowd crush '
                'or pedestrian-involved collisions.'
            ),
        },
        'mitigation_strategy': {
            'police_dispatch': {
                'action': 'Deploy for traffic calming and crowd monitoring',
                'assigned_units': [
                    {'vehicle_id': 'P-104', 'status': 'En route'},
                    {'vehicle_id': 'P-212', 'status': 'En route'},
                ],
            },
            'ems_dispatch': {
                'action': 'Dispatch nearest ambulances for triage-ready standby at the risk corridor.',
                'assigned_units': [
                    {'vehicle_id': 'TEMPE-AMB-01', 'status': 'En route'},
                ],
            },
            'fire_dispatch': {
                'action': 'Dispatch nearest firetrucks for hazard assessment and safety staging.',
                'assigned_units': [
                    {'vehicle_id': 'TEMPE-FIRE-01', 'status': 'En route'},
                ],
            },
            'medical_standby': {
                'unit_id': 'TEMPE-AMB-01',
                'instruction': 'Pre-notification',
                'message': (
                    'Potential high-density incident at Mill & University. '
                    'No immediate dispatch required; remain on standby at Station 4 '
                    'for rapid response if situation escalates.'
                ),
                'standby_location': {
                    'latitude': 33.4220,
                    'longitude': -111.9350,
                },
            },
            'traffic_control': {
                're-routing': (
                    'Automated signal timing adjustment implemented for Northbound '
                    'traffic to reduce pedestrian dwell time.'
                ),
            },
        },
        'triage_meta': {
            'is_life_threatening': True,
            'required_unit_type': 'patrol',
            'confidence_score': 0.87,
            'priority_score': 0.91,
        },
    }
]

# In-memory store — replaced atomically on each POST /prediction
_prediction_response: list[dict] = _DEFAULT_PREDICTION_RESPONSE.copy()
_prediction_response_lock = Lock()


# ─────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────

def _normalize_vehicle_status(raw_status: object) -> str:
    status = str(raw_status or '').strip().lower()
    if status in {'responding', 'en route', 'enroute', 'active', 'dispatched'}:
        return 'responding'
    if status in {'staged', 'standby', 'stationed', 'ready'}:
        return 'staged'
    if status in {'patrolling', 'patrol'}:
        return 'patrolling'
    return 'patrolling'


def _normalize_vehicle_type(raw_vehicle_type: object) -> str:
    normalized_type = str(raw_vehicle_type or '').strip().lower()
    if not normalized_type:
        return ''
    normalized_type = normalized_type.replace('-', ' ').replace('_', ' ')
    condensed_type = re.sub(r'\s+', ' ', normalized_type).strip()
    alias_key = condensed_type.replace(' ', '')
    return _VEHICLE_TYPE_ALIASES.get(condensed_type, _VEHICLE_TYPE_ALIASES.get(alias_key, condensed_type))


def _vehicle_type_label(vehicle_type: str) -> str:
    return _VEHICLE_TYPE_LABELS.get(vehicle_type, vehicle_type or 'unknown')


def _load_live_emergency_vehicles() -> list[dict[str, object]]:
    if not EMERGENCY_SNAPSHOT_PATH.exists():
        return []
    try:
        snapshot_payload = json.loads(EMERGENCY_SNAPSHOT_PATH.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError):
        return []

    vehicles_payload = snapshot_payload.get('vehicles') if isinstance(snapshot_payload, dict) else None
    if not isinstance(vehicles_payload, list):
        return []

    live_vehicles: list[dict[str, object]] = []
    for vehicle_payload in vehicles_payload:
        if not isinstance(vehicle_payload, dict):
            continue

        vehicle_id = str(
            vehicle_payload.get('unitCode')
            or vehicle_payload.get('id')
            or vehicle_payload.get('key')
            or ''
        ).strip()
        vehicle_type = _normalize_vehicle_type(vehicle_payload.get('vehicleType'))
        latitude = _to_float(vehicle_payload.get('latitude'))
        longitude = _to_float(vehicle_payload.get('longitude'))

        if not vehicle_id or not vehicle_type or latitude is None or longitude is None:
            continue

        live_vehicles.append(
            {
                'vehicle_id': vehicle_id,
                'vehicle_type': vehicle_type,
                'vehicle_type_label': _vehicle_type_label(vehicle_type),
                'status': _normalize_vehicle_status(vehicle_payload.get('status')),
                'latitude': latitude,
                'longitude': longitude,
            },
        )

    return live_vehicles


def _vehicles_with_distance_to_incident(
    live_vehicles: list[dict[str, object]],
    incident: dict,
) -> list[dict[str, object]]:
    incident_coordinates = incident.get('coordinates') if isinstance(incident.get('coordinates'), dict) else {}
    risk_latitude = _to_float(incident_coordinates.get('latitude'))
    risk_longitude = _to_float(incident_coordinates.get('longitude'))
    if risk_latitude is None or risk_longitude is None:
        return []

    vehicles_with_distance: list[dict[str, object]] = []
    for vehicle in live_vehicles:
        vehicle_latitude = _to_float(vehicle.get('latitude'))
        vehicle_longitude = _to_float(vehicle.get('longitude'))
        if vehicle_latitude is None or vehicle_longitude is None:
            continue

        distance_meters = _haversine_meters(
            risk_latitude,
            risk_longitude,
            vehicle_latitude,
            vehicle_longitude,
        )
        vehicle_with_distance = dict(vehicle)
        vehicle_with_distance['distance_meters'] = distance_meters
        vehicles_with_distance.append(vehicle_with_distance)

    vehicles_with_distance.sort(key=lambda vehicle: float(vehicle.get('distance_meters', math.inf)))
    return vehicles_with_distance


def _build_vehicle_prompt_context(
    incident: dict,
    live_vehicles: list[dict[str, object]],
) -> str:
    vehicles_with_distance = _vehicles_with_distance_to_incident(live_vehicles, incident)
    if not vehicles_with_distance:
        return '- No live emergency fleet telemetry is currently available.'

    lines: list[str] = []
    for vehicle in vehicles_with_distance:
        distance_meters = float(vehicle.get('distance_meters', 0.0))
        distance_miles = distance_meters / 1609.344
        status = str(vehicle.get('status') or 'patrolling')
        vehicle_type = str(vehicle.get('vehicle_type') or '')
        vehicle_type_label = str(vehicle.get('vehicle_type_label') or _vehicle_type_label(vehicle_type))
        dispatchability = 'yes' if status in _DISPATCHABLE_STATUSES else 'no'
        lines.append(
            (
                f"- id={vehicle['vehicle_id']} | vehicle_type={vehicle_type_label} | "
                f"canonical_vehicle_type={vehicle_type} | status={status} | "
                f"dispatchable_now={dispatchability} | latitude={float(vehicle['latitude']):.6f} | "
                f"longitude={float(vehicle['longitude']):.6f} | "
                f"distance_to_risk_meters={distance_meters:.1f} | "
                f"distance_to_risk_miles={distance_miles:.2f}"
            ),
        )
    return '\n'.join(lines)


def _normalize_action_id(
    raw_action_id: object,
    action_templates: dict[str, str],
    default_action_id: str,
) -> str:
    action_id = str(raw_action_id or '').strip().lower()
    if action_id in action_templates:
        return action_id
    return default_action_id


def _render_supported_action(
    action_id: str,
    action_templates: dict[str, str],
    **template_values: object,
) -> str:
    template = action_templates.get(action_id) or action_templates[next(iter(action_templates.keys()))]
    try:
        return template.format(**template_values)
    except KeyError:
        return template


def _dispatch_count_for_unit_type(
    required_unit_type: str,
    candidate_unit_type: str,
    priority_score: float,
    is_life_threatening: bool,
) -> int:
    primary_count = 2 if priority_score >= 0.8 else 1
    if required_unit_type == candidate_unit_type:
        return primary_count
    return 1 if is_life_threatening or priority_score >= 0.75 else 0


def _normalize_vehicle_id_list(raw_vehicle_ids: object) -> list[str]:
    if isinstance(raw_vehicle_ids, list):
        normalized_ids = []
        for value in raw_vehicle_ids:
            if not isinstance(value, str):
                continue
            candidate = value.strip()
            if candidate:
                normalized_ids.append(candidate)
        return normalized_ids
    if isinstance(raw_vehicle_ids, str):
        candidate = raw_vehicle_ids.strip()
        if not candidate:
            return []
        return [vehicle_id.strip() for vehicle_id in candidate.split(',') if vehicle_id.strip()]
    return []


def _extract_assigned_vehicle_ids(dispatch_plan_payload: object) -> list[str]:
    if not isinstance(dispatch_plan_payload, dict):
        return []

    assigned_units_payload = dispatch_plan_payload.get('assigned_units')
    if not isinstance(assigned_units_payload, list):
        return []

    vehicle_ids: list[str] = []
    seen_vehicle_ids: set[str] = set()

    for assigned_unit_payload in assigned_units_payload:
        if not isinstance(assigned_unit_payload, dict):
            continue

        candidate_vehicle_id = str(
            assigned_unit_payload.get('vehicle_id')
            or assigned_unit_payload.get('unit_id')
            or '',
        ).strip()
        if not candidate_vehicle_id:
            continue

        vehicle_id_key = candidate_vehicle_id.casefold()
        if vehicle_id_key in seen_vehicle_ids:
            continue

        seen_vehicle_ids.add(vehicle_id_key)
        vehicle_ids.append(candidate_vehicle_id)

    return vehicle_ids


def _coerce_prediction_candidate(raw_payload: object, incident_id: str) -> dict | None:
    if isinstance(raw_payload, dict):
        return raw_payload

    if isinstance(raw_payload, list):
        dict_candidates = [candidate for candidate in raw_payload if isinstance(candidate, dict)]
        if not dict_candidates:
            return None

        for candidate in dict_candidates:
            candidate_id = str(candidate.get('id') or '').strip()
            if candidate_id == incident_id:
                return candidate

        return dict_candidates[0]

    return None


def _coerce_prediction_schema(parsed_prediction: dict) -> dict | None:
    if _is_valid_prediction(parsed_prediction):
        mitigation_strategy = (
            parsed_prediction.get('mitigation_strategy')
            if isinstance(parsed_prediction.get('mitigation_strategy'), dict)
            else {}
        )
        normalized_prediction = dict(parsed_prediction)
        if not _normalize_vehicle_id_list(normalized_prediction.get('police_unit_ids')):
            normalized_prediction['police_unit_ids'] = _extract_assigned_vehicle_ids(
                mitigation_strategy.get('police_dispatch'),
            )
        if not _normalize_vehicle_id_list(normalized_prediction.get('ems_unit_ids')):
            normalized_prediction['ems_unit_ids'] = _extract_assigned_vehicle_ids(
                mitigation_strategy.get('ems_dispatch'),
            )
        if not _normalize_vehicle_id_list(normalized_prediction.get('fire_unit_ids')):
            normalized_prediction['fire_unit_ids'] = _extract_assigned_vehicle_ids(
                mitigation_strategy.get('fire_dispatch'),
            )
        return normalized_prediction

    risk_assessment = (
        parsed_prediction.get('risk_assessment')
        if isinstance(parsed_prediction.get('risk_assessment'), dict)
        else {}
    )
    triage_meta = (
        parsed_prediction.get('triage_meta')
        if isinstance(parsed_prediction.get('triage_meta'), dict)
        else {}
    )
    mitigation_strategy = (
        parsed_prediction.get('mitigation_strategy')
        if isinstance(parsed_prediction.get('mitigation_strategy'), dict)
        else {}
    )

    threat_level = parsed_prediction.get('threat_level')
    if not isinstance(threat_level, str) or threat_level.strip() not in {'High', 'Medium', 'Low'}:
        risk_level = str(risk_assessment.get('level') or '').strip()
        threat_level = risk_level if risk_level in {'High', 'Medium', 'Low'} else None

    required_unit_type = str(
        parsed_prediction.get('required_unit_type')
        or triage_meta.get('required_unit_type')
        or '',
    ).strip()

    is_life_threatening_value = parsed_prediction.get('is_life_threatening')
    if not isinstance(is_life_threatening_value, bool):
        is_life_threatening_value = triage_meta.get('is_life_threatening')

    confidence_value = parsed_prediction.get('confidence_score')
    if not isinstance(confidence_value, (int, float)):
        confidence_value = triage_meta.get('confidence_score')

    priority_value = parsed_prediction.get('priority_score')
    if not isinstance(priority_value, (int, float)):
        priority_value = triage_meta.get('priority_score')

    risk_factors_value = parsed_prediction.get('risk_factors')
    if not isinstance(risk_factors_value, list):
        risk_factors_value = risk_assessment.get('risk_factors')

    explanation_value = parsed_prediction.get('explanation')
    if not isinstance(explanation_value, str):
        explanation_value = risk_assessment.get('explanation')

    normalized_prediction = {
        'threat_level': threat_level,
        'is_life_threatening': is_life_threatening_value,
        'required_unit_type': required_unit_type,
        'confidence_score': confidence_value,
        'priority_score': priority_value,
        'risk_factors': risk_factors_value,
        'explanation': explanation_value,
        'police_dispatch_action_id': parsed_prediction.get('police_dispatch_action_id'),
        'ems_dispatch_action_id': parsed_prediction.get('ems_dispatch_action_id'),
        'fire_dispatch_action_id': parsed_prediction.get('fire_dispatch_action_id'),
        'medical_standby_action_id': parsed_prediction.get('medical_standby_action_id'),
        'traffic_control_action_id': parsed_prediction.get('traffic_control_action_id'),
        'police_unit_ids': _normalize_vehicle_id_list(parsed_prediction.get('police_unit_ids')),
        'ems_unit_ids': _normalize_vehicle_id_list(parsed_prediction.get('ems_unit_ids')),
        'fire_unit_ids': _normalize_vehicle_id_list(parsed_prediction.get('fire_unit_ids')),
    }

    if not normalized_prediction['police_unit_ids']:
        normalized_prediction['police_unit_ids'] = _extract_assigned_vehicle_ids(
            mitigation_strategy.get('police_dispatch'),
        )
    if not normalized_prediction['ems_unit_ids']:
        normalized_prediction['ems_unit_ids'] = _extract_assigned_vehicle_ids(
            mitigation_strategy.get('ems_dispatch'),
        )
    if not normalized_prediction['fire_unit_ids']:
        normalized_prediction['fire_unit_ids'] = _extract_assigned_vehicle_ids(
            mitigation_strategy.get('fire_dispatch'),
        )

    if _is_valid_prediction(normalized_prediction):
        return normalized_prediction

    return None


def _select_vehicles_for_dispatch(
    typed_vehicles: list[dict[str, object]],
    preferred_vehicle_ids: list[str],
    count: int,
) -> list[dict[str, object]]:
    if count <= 0:
        return []

    vehicles_by_id = {
        str(vehicle.get('vehicle_id')).casefold(): vehicle
        for vehicle in typed_vehicles
        if str(vehicle.get('vehicle_id')).strip()
    }
    ranked_candidates: list[dict[str, object]] = []
    selected_candidate_ids: set[str] = set()

    for preferred_vehicle_id in preferred_vehicle_ids:
        candidate = vehicles_by_id.get(preferred_vehicle_id.casefold())
        if candidate is None:
            continue
        candidate_id_key = str(candidate.get('vehicle_id')).casefold()
        if candidate_id_key in selected_candidate_ids:
            continue
        ranked_candidates.append(candidate)
        selected_candidate_ids.add(candidate_id_key)

    for candidate in typed_vehicles:
        candidate_id_key = str(candidate.get('vehicle_id')).casefold()
        if candidate_id_key in selected_candidate_ids:
            continue
        ranked_candidates.append(candidate)
        selected_candidate_ids.add(candidate_id_key)

    dispatchable_candidates = [
        candidate
        for candidate in ranked_candidates
        if str(candidate.get('status')) in _DISPATCHABLE_STATUSES
    ]
    selected_candidates: list[dict[str, object]] = dispatchable_candidates[:count]
    if len(selected_candidates) >= count:
        return selected_candidates

    selected_ids = {str(candidate.get('vehicle_id')).casefold() for candidate in selected_candidates}
    for candidate in ranked_candidates:
        candidate_id_key = str(candidate.get('vehicle_id')).casefold()
        if candidate_id_key in selected_ids:
            continue
        selected_candidates.append(candidate)
        selected_ids.add(candidate_id_key)
        if len(selected_candidates) >= count:
            break

    return selected_candidates


def _derive_standby_location(incident: dict) -> dict[str, float]:
    incident_coordinates = incident.get('coordinates') if isinstance(incident.get('coordinates'), dict) else {}
    incident_latitude = _to_float(incident_coordinates.get('latitude'))
    incident_longitude = _to_float(incident_coordinates.get('longitude'))
    if incident_latitude is None or incident_longitude is None:
        fallback = EMS_UNITS[0]
        return {'latitude': fallback[1], 'longitude': fallback[2]}

    return {
        'latitude': round(max(min(incident_latitude + 0.0012, 90.0), -90.0), 6),
        'longitude': round(max(min(incident_longitude + 0.0012, 180.0), -180.0), 6),
    }


def _get_dispatch_units(
    unit_type: str,
    priority: float,
    incident: dict,
    live_vehicles: list[dict[str, object]],
    count_override: int | None = None,
    preferred_vehicle_ids: list[str] | None = None,
) -> list[dict[str, str]]:
    """Return dispatch units for a service type, optionally overriding the dispatch count."""
    count = (2 if priority >= 0.8 else 1) if count_override is None else max(0, int(count_override))
    if count <= 0:
        return []

    target_vehicle_type = _UNIT_TYPE_TO_VEHICLE_TYPE.get(unit_type)
    preferred_ids = preferred_vehicle_ids or []
    if target_vehicle_type:
        typed_vehicles = [
            vehicle
            for vehicle in _vehicles_with_distance_to_incident(live_vehicles, incident)
            if str(vehicle.get('vehicle_type')) == target_vehicle_type
        ]
        selected_vehicles: list[dict[str, object]] = []
        selected_vehicle_keys: set[str] = set()

        if preferred_ids:
            vehicles_by_id = {
                str(vehicle.get('vehicle_id')).casefold(): vehicle
                for vehicle in typed_vehicles
                if str(vehicle.get('vehicle_id')).strip()
            }
            for preferred_vehicle_id in preferred_ids:
                candidate = vehicles_by_id.get(preferred_vehicle_id.casefold())
                if candidate is None:
                    continue
                candidate_key = str(candidate.get('vehicle_id')).casefold()
                if candidate_key in selected_vehicle_keys:
                    continue
                selected_vehicles.append(candidate)
                selected_vehicle_keys.add(candidate_key)
                if len(selected_vehicles) >= count:
                    break

        if len(selected_vehicles) < count:
            remaining_vehicles = [
                vehicle
                for vehicle in typed_vehicles
                if str(vehicle.get('vehicle_id')).casefold() not in selected_vehicle_keys
            ]
            selected_vehicles.extend(
                _select_vehicles_for_dispatch(
                    remaining_vehicles,
                    [],
                    count - len(selected_vehicles),
                ),
            )

        if selected_vehicles:
            return [
                {
                    'vehicle_id': str(vehicle['vehicle_id']),
                    'status': 'En route' if str(vehicle.get('status')) in _DISPATCHABLE_STATUSES else 'Responding',
                }
                for vehicle in selected_vehicles
            ]

    if unit_type == 'patrol':
        return [{'vehicle_id': u[0], 'status': u[1]} for u in PATROL_UNITS[:count]]
    if unit_type == 'ems':
        return [{'vehicle_id': u[0], 'status': 'En route'} for u in EMS_UNITS[:count]]
    if unit_type == 'fire':
        return [{'vehicle_id': u[0], 'status': 'En route'} for u in FIRE_UNITS[:count]]
    return []


def _get_standby_unit(
    unit_type: str,
    incident: dict,
    live_vehicles: list[dict[str, object]],
    ems_dispatch_units: list[dict[str, str]],
) -> dict:
    standby_location = _derive_standby_location(incident)
    if ems_dispatch_units:
        return {
            'unit_id': ems_dispatch_units[0]['vehicle_id'],
            'standby_location': standby_location,
        }

    nearest_ambulances = [
        vehicle
        for vehicle in _vehicles_with_distance_to_incident(live_vehicles, incident)
        if str(vehicle.get('vehicle_type')) == 'ambulance'
    ]
    dispatchable_ambulances = [
        vehicle
        for vehicle in nearest_ambulances
        if str(vehicle.get('status')) in _DISPATCHABLE_STATUSES
    ]
    standby_candidate = (dispatchable_ambulances or nearest_ambulances)
    if standby_candidate:
        return {
            'unit_id': str(standby_candidate[0]['vehicle_id']),
            'standby_location': standby_location,
        }

    fallback_unit = EMS_UNITS[0] if unit_type in ('patrol', 'fire') else EMS_UNITS[1]
    return {
        'unit_id': fallback_unit[0],
        'standby_location': {'latitude': fallback_unit[1], 'longitude': fallback_unit[2]},
    }


def _clean_json(raw: str) -> str:
    """Extract first {...} block, strip markdown fences."""
    raw = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        return match.group(0).strip()
    return raw.strip()


def _is_valid_prediction(parsed: dict) -> bool:
    """Validate all required LLM output fields and their types/ranges."""
    if not isinstance(parsed, dict):
        return False
    if parsed.get("threat_level") not in {"High", "Medium", "Low"}:
        return False
    if not isinstance(parsed.get("is_life_threatening"), bool):
        return False
    if parsed.get("required_unit_type") not in {"patrol", "ems", "fire"}:
        return False
    confidence = parsed.get("confidence_score")
    if not isinstance(confidence, (float, int)) or not (0.0 <= float(confidence) <= 1.0):
        return False
    priority = parsed.get("priority_score")
    if not isinstance(priority, (float, int)) or not (0.0 <= float(priority) <= 1.0):
        return False
    if not isinstance(parsed.get("risk_factors"), list) or len(parsed["risk_factors"]) == 0:
        return False
    if not isinstance(parsed.get("explanation"), str) or len(parsed["explanation"]) < 10:
        return False

    for action_id_key in (
        'police_dispatch_action_id',
        'ems_dispatch_action_id',
        'fire_dispatch_action_id',
        'medical_standby_action_id',
        'traffic_control_action_id',
    ):
        action_id_value = parsed.get(action_id_key)
        if action_id_value is not None and not isinstance(action_id_value, str):
            return False

    for vehicle_list_key in ('police_unit_ids', 'ems_unit_ids', 'fire_unit_ids'):
        vehicle_list_value = parsed.get(vehicle_list_key)
        if vehicle_list_value is None:
            continue
        if not isinstance(vehicle_list_value, list):
            return False
        for vehicle_id in vehicle_list_value:
            if not isinstance(vehicle_id, str) or not vehicle_id.strip():
                return False

    return True


def _call_gpt_oss_prediction(
    incident: dict,
    live_vehicles: list[dict[str, object]],
) -> dict | None:
    """Call NVIDIA Hosted GPT OSS 20B for one incident."""
    reports_text = "\n".join(f"- {r}" for r in incident["reports"])
    location     = incident.get("location_name", "Unknown Location")
    source_api = str(incident.get('source_api', 'Unknown source'))
    source_data_type = str(incident.get('source_data_type', 'unknown'))
    api_key_name = incident.get('api_key_name') or 'none'
    fake_score_value = incident.get('fake_score')
    fake_score = f'{float(fake_score_value):.2f}' if isinstance(fake_score_value, (int, float)) else 'n/a'
    emergency_fleet_context = _build_vehicle_prompt_context(incident, live_vehicles)

    prompt = f"""You are an emergency dispatch triage AI system.

Analyze the incident below and return ONLY a single valid JSON object.
No markdown. No backticks. No explanation. No extra text before or after.

LOCATION: {location}

SOURCE FEED METADATA:
- source_api: {source_api}
- source_data_type: {source_data_type}
- source_api_key_name: {api_key_name}
- source_fake_score: {fake_score}

LIVE EMERGENCY FLEET (sorted nearest to this risk point):
{emergency_fleet_context}

DISPATCH STATUS POLICY:
- dispatchable statuses: patrolling, staged
- already committed status: responding
- prioritize nearest dispatchable units when recommending actions
- recommend unit IDs directly from the fleet list for each service type

SIMULATION CAPABILITIES (hard constraint):
- The simulator can route police, ambulance, and firetruck units to coordinates.
- The simulator can keep units in patrolling/responding/staged states.
- The simulator can store one text traffic re-routing instruction.
- Do not mention actions outside this scope (no aircraft, drones, arrests, evacuations, utility controls, or shelter operations).

SUPPORTED ACTION IDS (choose one per field):
- police_dispatch_action_id: dispatch_to_risk | establish_perimeter | intersection_control
- ems_dispatch_action_id: dispatch_to_risk | staged_triage | casualty_collection_point
- fire_dispatch_action_id: dispatch_to_risk | hazard_assessment | water_supply_staging
- medical_standby_action_id: preposition_at_standby | reserve_for_escalation
- traffic_control_action_id: reroute_event_egress | signal_priority_corridor | meter_inbound_flow

UNIT-ID SELECTION OUTPUT (optional but strongly preferred):
- police_unit_ids: list of 1-3 police unit IDs from LIVE EMERGENCY FLEET.
- ems_unit_ids: list of 0-2 ambulance unit IDs from LIVE EMERGENCY FLEET.
- fire_unit_ids: list of 0-2 firetruck unit IDs from LIVE EMERGENCY FLEET.
- Use exact IDs as listed.
- Prefer nearest units with status patrolling or staged.
- Only use responding units when no suitable dispatchable unit is available.

INCIDENT REPORTS:
{reports_text}

UNIT ASSIGNMENT RULES:
- "patrol": armed threats, suspicious persons, weapons, civil disturbances
- "ems": injuries, cardiac arrest, unconscious persons, medical emergencies
- "fire": fires, smoke, explosions, hazmat

PRIORITY SCORE GUIDELINES:
- 1.0       → immediate life-threatening (cardiac arrest, active shooter)
- 0.8–0.9   → high urgency (weapon confirmed, serious injury, structure fire)
- 0.5–0.7   → moderate urgency (unconfirmed threat, minor injury, traffic)
- below 0.5 → low urgency (noise, minor disturbance)

REQUIRED JSON — include every field exactly as shown:
{{
  "threat_level": "High",
  "is_life_threatening": true,
  "required_unit_type": "patrol",
  "confidence_score": 0.85,
  "priority_score": 0.9,
  "risk_factors": [
    "Specific risk factor derived from reports",
    "Second specific risk factor",
    "Third specific risk factor"
  ],
  "explanation": "A 2-4 sentence paragraph explaining why this risk level was assigned, what convergence of factors is driving the score, and the likely outcome if not addressed promptly.",
    "police_dispatch_action_id": "dispatch_to_risk",
    "ems_dispatch_action_id": "staged_triage",
    "fire_dispatch_action_id": "hazard_assessment",
    "medical_standby_action_id": "preposition_at_standby",
        "traffic_control_action_id": "signal_priority_corridor",
    "police_unit_ids": ["<POLICE_UNIT_ID_FROM_LIVE_FLEET>"],
    "ems_unit_ids": ["<EMS_UNIT_ID_FROM_LIVE_FLEET>"],
    "fire_unit_ids": ["<FIRE_UNIT_ID_FROM_LIVE_FLEET>"]
}}
Return ONLY the JSON object:"""

    try:
        completion = _get_nvidia_client().chat.completions.create(
            model='openai/gpt-oss-20b',
            messages=[{'role': 'user', 'content': prompt}],
            temperature=1,
            top_p=1,
            max_tokens=4096,
            stream=True,
        )

        content_chunks: list[str] = []
        for chunk in completion:
            choices = getattr(chunk, 'choices', None)
            if not choices:
                continue
            choice = choices[0]
            delta = getattr(choice, 'delta', None)
            if delta is None:
                continue

            # NVIDIA streaming responses may include reasoning content separately.
            _reasoning = getattr(delta, 'reasoning_content', None)
            content = getattr(delta, 'content', None)
            if content is not None:
                content_chunks.append(str(content))

        raw = ''.join(content_chunks).strip()
        if not raw:
            print(f"[ERROR] [{incident['id']}] NVIDIA API returned an empty response.", file=sys.stderr)
            return None
    except Exception as e:
        print(f"[ERROR] [{incident['id']}] NVIDIA API call failed: {e}", file=sys.stderr)
        return None

    try:
        cleaned = _clean_json(raw)
        parsed_raw = json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"[ERROR] [{incident['id']}] JSON parse error: {e}", file=sys.stderr)
        return None

    parsed_candidate = _coerce_prediction_candidate(parsed_raw, str(incident.get('id') or ''))
    if parsed_candidate is None:
        print(
            f"[ERROR] [{incident['id']}] Prediction payload did not include an object candidate.",
            file=sys.stderr,
        )
        return None

    parsed = _coerce_prediction_schema(parsed_candidate)
    if parsed is None:
        print(f"[ERROR] [{incident['id']}] Prediction payload failed schema normalization.", file=sys.stderr)
        return None

    # Normalization & Assembly
    unit_type = str(parsed['required_unit_type'])
    priority = float(parsed['priority_score'])
    is_life_threatening = bool(parsed['is_life_threatening'])

    police_dispatch_count = _dispatch_count_for_unit_type(
        unit_type,
        'patrol',
        priority,
        is_life_threatening,
    )
    ems_dispatch_count = _dispatch_count_for_unit_type(
        unit_type,
        'ems',
        priority,
        is_life_threatening,
    )
    fire_dispatch_count = _dispatch_count_for_unit_type(
        unit_type,
        'fire',
        priority,
        is_life_threatening,
    )

    preferred_police_ids = _normalize_vehicle_id_list(parsed.get('police_unit_ids'))
    preferred_ems_ids = _normalize_vehicle_id_list(parsed.get('ems_unit_ids'))
    preferred_fire_ids = _normalize_vehicle_id_list(parsed.get('fire_unit_ids'))

    police_dispatch_count = max(police_dispatch_count, min(len(preferred_police_ids), 3))
    ems_dispatch_count = max(ems_dispatch_count, min(len(preferred_ems_ids), 2))
    fire_dispatch_count = max(fire_dispatch_count, min(len(preferred_fire_ids), 2))

    police_dispatch_units = _get_dispatch_units(
        'patrol',
        priority,
        incident,
        live_vehicles,
        count_override=police_dispatch_count,
        preferred_vehicle_ids=preferred_police_ids,
    )
    ems_dispatch_units = _get_dispatch_units(
        'ems',
        priority,
        incident,
        live_vehicles,
        count_override=ems_dispatch_count,
        preferred_vehicle_ids=preferred_ems_ids,
    )
    fire_dispatch_units = _get_dispatch_units(
        'fire',
        priority,
        incident,
        live_vehicles,
        count_override=fire_dispatch_count,
        preferred_vehicle_ids=preferred_fire_ids,
    )

    standby = _get_standby_unit(unit_type, incident, live_vehicles, ems_dispatch_units)

    police_dispatch_action_id = _normalize_action_id(
        parsed.get('police_dispatch_action_id'),
        _POLICE_DISPATCH_ACTIONS,
        _DEFAULT_POLICE_DISPATCH_ACTION_ID,
    )
    ems_dispatch_action_id = _normalize_action_id(
        parsed.get('ems_dispatch_action_id'),
        _EMS_DISPATCH_ACTIONS,
        _DEFAULT_EMS_DISPATCH_ACTION_ID,
    )
    fire_dispatch_action_id = _normalize_action_id(
        parsed.get('fire_dispatch_action_id'),
        _FIRE_DISPATCH_ACTIONS,
        _DEFAULT_FIRE_DISPATCH_ACTION_ID,
    )
    medical_standby_action_id = _normalize_action_id(
        parsed.get('medical_standby_action_id'),
        _MEDICAL_STANDBY_ACTIONS,
        _DEFAULT_MEDICAL_STANDBY_ACTION_ID,
    )
    traffic_control_action_id = _normalize_action_id(
        parsed.get('traffic_control_action_id'),
        _TRAFFIC_CONTROL_ACTIONS,
        _DEFAULT_TRAFFIC_CONTROL_ACTION_ID,
    )

    police_dispatch_action = _render_supported_action(
        police_dispatch_action_id,
        _POLICE_DISPATCH_ACTIONS,
        location_name=location,
    )
    ems_dispatch_action = _render_supported_action(
        ems_dispatch_action_id,
        _EMS_DISPATCH_ACTIONS,
        location_name=location,
    )
    fire_dispatch_action = _render_supported_action(
        fire_dispatch_action_id,
        _FIRE_DISPATCH_ACTIONS,
        location_name=location,
    )
    medical_standby_message = _render_supported_action(
        medical_standby_action_id,
        _MEDICAL_STANDBY_ACTIONS,
        location_name=location,
        unit_id=standby['unit_id'],
    )
    traffic_control_action = _render_supported_action(
        traffic_control_action_id,
        _TRAFFIC_CONTROL_ACTIONS,
        location_name=location,
    )

    return {
        "prediction_id": f"risk-analysis-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-{str(uuid.uuid4())[:4]}",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "id": incident["id"],
        "risk_assessment": {
            "level": parsed["threat_level"],
            "coordinates": incident["coordinates"],
            "location_name": location,
            "risk_factors": parsed["risk_factors"],
            "explanation": parsed["explanation"]
        },
        "mitigation_strategy": {
            "police_dispatch": {
                "action": police_dispatch_action,
                "assigned_units": police_dispatch_units,
            },
            "ems_dispatch": {
                "action": ems_dispatch_action,
                "assigned_units": ems_dispatch_units,
            },
            "fire_dispatch": {
                "action": fire_dispatch_action,
                "assigned_units": fire_dispatch_units,
            },
            "medical_standby": {
                "unit_id": standby["unit_id"],
                "instruction": "Pre-notification",
                "message": medical_standby_message,
                "standby_location": standby["standby_location"]
            },
            "traffic_control": {"re-routing": traffic_control_action}
        },
        "triage_meta": {
            "is_life_threatening": is_life_threatening,
            "required_unit_type": unit_type,
            "confidence_score": float(parsed["confidence_score"]),
            "priority_score": float(priority)
        }
    }


def _update_live_predictions() -> list[dict]:
    """Process all incidents and update global state."""
    global _prediction_response, _last_prediction_snapshot_digest

    results = []
    live_vehicles = _load_live_emergency_vehicles()
    for inc in INCIDENTS_SOURCE:
        res = _call_gpt_oss_prediction(inc, live_vehicles)
        if res:
            results.append(res)
    results.sort(key=lambda r: r["triage_meta"]["priority_score"], reverse=True)

    with _prediction_response_lock:
        if results:
            _prediction_response = results

    if results:
        serialized = json.dumps(results, separators=(',', ':'), ensure_ascii=False)
        digest = hashlib.sha256(serialized.encode('utf-8')).hexdigest()
        with _prediction_snapshot_lock:
            if digest != _last_prediction_snapshot_digest:
                _write_json_atomic(PREDICTION_SNAPSHOT_PATH, serialized)
                _last_prediction_snapshot_digest = digest

    return results


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
    latitude_a     = math.radians(lat_a)
    latitude_b     = math.radians(lat_b)
    latitude_delta  = math.radians(lat_b - lat_a)
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
    raw_graph    = nx.read_graphml(GRAPH_PATH)
    routing_graph = nx.DiGraph()
    for node_id, node_data in raw_graph.nodes(data=True):
        longitude = _to_float(node_data.get('x'))
        latitude  = _to_float(node_data.get('y'))
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
    nearest_node    = None
    nearest_distance = math.inf
    for node_id, node_data in routing_graph.nodes(data=True):
        node_latitude  = float(node_data['y'])
        node_longitude = float(node_data['x'])
        candidate_distance = _haversine_meters(
            coordinate.latitude, coordinate.longitude,
            node_latitude, node_longitude,
        )
        if candidate_distance < nearest_distance:
            nearest_distance = candidate_distance
            nearest_node     = str(node_id)
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
    start_node  = _nearest_node_id(routing_graph, start_coordinate)
    target_node = _nearest_node_id(routing_graph, target_coordinate)
    shortest_node_path = nx.shortest_path(
        routing_graph, source=start_node, target=target_node, weight='length',
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
        last_waypoint  = route_waypoints[-1]
        if (
            abs(first_waypoint.latitude  - last_waypoint.latitude)  > 1e-8
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


# ─────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────

@app.get('/')
def healthcheck() -> dict[str, str]:
    return {'status': 'ok', 'service': 'risk-prediction-api'}


@app.get('/prediction')
def get_prediction() -> list[dict]:
    """
    Returns a sorted prediction list generated from dummy world-state signals.
    """
    _update_live_predictions()

    with _prediction_response_lock:
        return _prediction_response


@app.post('/prediction/refresh')
def force_refresh_prediction() -> dict:
    """
    Manually trigger GPT OSS 20B to re-analyze all incidents.
    """
    results = _update_live_predictions()
    return {
        'status': 'refreshed',
        'incidents_count': len(results),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }


@app.post('/prediction')
def update_prediction(payload: list[dict]) -> dict:
    """
    Receives a full sorted prediction list and stores it.
    Persists atomically to disk so predictions survive a server restart.
    Deduplicates by SHA-256 — identical payloads are acknowledged without disk I/O.
    """
    global _prediction_response, _last_prediction_snapshot_digest

    if not payload:
        raise HTTPException(status_code=422, detail='Payload must be a non-empty list.')

    serialized = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)
    digest      = hashlib.sha256(serialized.encode('utf-8')).hexdigest()

    with _prediction_snapshot_lock:
        if digest == _last_prediction_snapshot_digest:
            return {
                'status':     'unchanged',
                'incidents':  len(payload),
                'digest':     digest,
            }
        _write_json_atomic(PREDICTION_SNAPSHOT_PATH, serialized)
        _last_prediction_snapshot_digest = digest

    with _prediction_response_lock:
        _prediction_response = payload

    return {
        'status':    'updated',
        'incidents': len(payload),
        'digest':    digest,
        'file':      str(PREDICTION_SNAPSHOT_PATH),
    }


@app.post('/route/dispatch')
def route_dispatch(payload: DispatchRouteRequest) -> dict:
    try:
        routing_graph = _load_routing_graph()
        route_coordinates, route_distance = _shortest_path_coordinates(
            routing_graph, payload.start, payload.target,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except (ValueError, nx.NetworkXNoPath, nx.NodeNotFound) as exc:
        raise HTTPException(status_code=404, detail='No routable path found on Tempe road graph.') from exc
    return {
        'coordinates':     route_coordinates,
        'distance_meters': route_distance,
        'source':          'tempe_road_graph.graphml',
    }


@app.post('/route/waypoints')
def route_waypoints(payload: WaypointRouteRequest) -> dict:
    try:
        routing_graph = _load_routing_graph()
        route_coordinates, route_distance = _waypoint_route_coordinates(
            routing_graph, payload.waypoints, payload.is_loop,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except (ValueError, nx.NetworkXNoPath, nx.NodeNotFound) as exc:
        raise HTTPException(status_code=404, detail='No waypoint route found on Tempe road graph.') from exc
    return {
        'coordinates':     route_coordinates,
        'distance_meters': route_distance,
        'source':          'tempe_road_graph.graphml',
    }


@app.get('/telemetry/emergency-snapshot')
def get_emergency_snapshot() -> dict:
    if not EMERGENCY_SNAPSHOT_PATH.exists():
        return {
            'timestamp':            0,
            'simulationIntervalMs': 0,
            'patrolRoutes':         [],
            'vehicles':             [],
        }
    try:
        return json.loads(EMERGENCY_SNAPSHOT_PATH.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail='Snapshot file is corrupted.') from exc


@app.post('/telemetry/emergency-snapshot')
def save_emergency_snapshot(payload: EmergencyTelemetrySnapshot) -> dict:
    global _last_emergency_snapshot_digest

    snapshot_dict       = payload.model_dump()
    serialized_snapshot = json.dumps(snapshot_dict, separators=(',', ':'), ensure_ascii=False)
    snapshot_digest     = hashlib.sha256(serialized_snapshot.encode('utf-8')).hexdigest()

    with _emergency_snapshot_lock:
        if snapshot_digest == _last_emergency_snapshot_digest:
            return {
                'status':   'unchanged',
                'vehicles': len(payload.vehicles),
                'file':     str(EMERGENCY_SNAPSHOT_PATH),
            }
        _write_json_atomic(EMERGENCY_SNAPSHOT_PATH, serialized_snapshot)
        _last_emergency_snapshot_digest = snapshot_digest

    return {
        'status':   'written',
        'vehicles': len(payload.vehicles),
        'file':     str(EMERGENCY_SNAPSHOT_PATH),
    }


if __name__ == '__main__':
    uvicorn.run('prediction_server:app', host='0.0.0.0', port=8000, reload=True)