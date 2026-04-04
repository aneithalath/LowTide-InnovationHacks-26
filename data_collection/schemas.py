from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


def _to_dict(instance: Any) -> dict[str, Any]:
    return asdict(instance)


@dataclass
class Incident:
    id: str
    source: str
    timestamp: Any
    lat: Optional[float]
    lng: Optional[float]
    type: str
    title: str
    severity: Any
    description: str
    confirmed: bool
    _simulated: bool
    record_type: str = field(default="incident")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class WeatherSnapshot:
    timestamp: str
    lat: float
    lng: float
    temperature_f: float
    feels_like_f: float
    humidity_pct: int
    wind_speed_mph: float
    visibility_miles: float
    conditions: str
    alert_type: Optional[str]
    alert_description: Optional[str]
    _simulated: bool
    record_type: str = field(default="weathersnapshot")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class RoadSegment:
    road_id: str
    label: str
    lat: float
    lng: float
    speed_limit_mph: int
    current_speed_mph: float
    congestion_score: float
    closed: bool
    _simulated: bool
    record_type: str = field(default="roadsegment")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class Event:
    event_id: str
    name: str
    type: str
    lat: Optional[float]
    lng: Optional[float]
    start_time: str
    end_time: str
    expected_crowd: Optional[int]
    crowd_source: str
    _simulated: bool
    record_type: str = field(default="event")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class RiskArea:
    id: str
    type: str
    name: str
    lat: float
    lng: float
    capacity: Optional[int]
    crowd_modifier: float
    current_status: str
    _simulated: bool
    record_type: str = field(default="riskarea")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class Camera:
    cam_id: str
    lat: float
    lng: float
    label: str
    stream_url: str
    type: str
    refresh_sec: int
    live: bool
    _simulated: bool
    record_type: str = field(default="camera")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class Unit:
    unit_id: str
    type: str
    lat: float
    lng: float
    status: str
    availability: bool
    assigned_incident: Optional[str]
    siren_active: bool
    current_speed_mph: float
    heading_deg: float
    route_node_index: int
    route_nodes: list[int]
    _simulated: bool
    record_type: str = field(default="unit")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)


@dataclass
class HistoricalCell:
    cell_id: str
    lat: float
    lng: float
    incident_count: int
    primary_type: str
    peak_hours: list[int]
    escalation_rate: float
    heatmap_weight: float
    _simulated: bool
    record_type: str = field(default="historicalcell")

    def to_dict(self) -> dict[str, Any]:
        return _to_dict(self)
