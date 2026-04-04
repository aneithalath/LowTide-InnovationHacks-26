# SENTINEL Data Collection Layer

## Purpose
The data collection layer pulls live and semi-live public safety context for Tempe, AZ and normalizes it into a single world-state file consumed by downstream SENTINEL components (LLM decision support, map/heatmap views, and simulation views).

This layer is designed so one broken source does not crash the system. Each module catches exceptions, logs one summary line, and falls back to last cached data or simulation when required.

## Data Sources

| Source | Type (real/simulated/both) | Update frequency | Auth required | Notes |
|---|---|---|---|---|
| Citizen Trending API | both | every 2 min | no | Unofficial incident feed; normalized as incident records and cached. |
| NOAA/NWS Alerts (`api.weather.gov`) | real | every 5 min | no key, requires User-Agent | Pulled first for active weather alerts at Tempe coordinates. |
| OpenWeatherMap Current Weather | both | every 5 min | `OPENWEATHER_API_KEY` | Current conditions; if unavailable, weather values are simulated baseline. |
| TomTom Traffic Flow | both | every 2 min | `TOMTOM_API_KEY` | 12 fixed intersections; handles 429 by simulating remaining intersections in batch. |
| AZ511 Events (closures) | real | every 2 min (with traffic) | no | Adds closure road segments for Maricopa/Tempe entries. |
| Eventbrite Event Search | both | every 60 min | `EVENTBRITE_API_KEY` | Tempe-area events; expected crowd is estimated when missing. |
| ASU Localist | both | every 60 min | no | ASU events in next 3 days; crowd usually estimated via venue/type heuristics. |
| Overpass API (OSM POIs) | real | every 24 hours | no | Schools/hospitals/stadiums/university/etc; cached daily. |
| Tempe ArcGIS Calls For Service FeatureServer | real | startup/background only (7-day cache) | no | Paged pull up to 20,000 records, aggregated into 0.005-degree grid cells. |
| AZ511 Cameras | both | startup only | no | Snapshot camera metadata filtered to Tempe bbox; uses 6 placeholders if empty/unavailable. |
| OSMnx + NetworkX road graph | simulated behavior over real map | loaded at startup, reused | no | Unit movement is simulated on Tempe drive graph with routing and congestion effects. |

## File Inventory

- `citizen_data.py`: Existing Citizen incident fetch + normalization helper used by collector.
- `schemas.py`: Dataclass schemas for all normalized record types (`to_dict()` on each schema).
- `weather_data.py`: NWS + OpenWeather merge into one weather snapshot and weather cache.
- `traffic_data.py`: TomTom flow by intersection + AZ511 closures + congestion simulation fallback.
- `events_data.py`: Eventbrite + ASU Localist normalization with expected crowd estimation.
- `risk_areas_data.py`: Overpass POI ingestion and once-daily cache refresh logic.
- `historical_data.py`: ArcGIS pagination and grid-cell heatmap aggregation.
- `cameras_data.py`: AZ511 camera metadata pull and fallback camera list.
- `unit_simulation.py`: Police/fire unit state machine and movement tick on Tempe road graph.
- `collector.py`: Master orchestrator and scheduler writing unified `world_state.json`.
- `validate_output.py`: Sanity checker/report for `world_state.json`.
- `data/`: Cache directory for all JSON outputs and saved road graph.

## How To Run

1. Open a terminal at the project root.
2. Activate virtual environment:

```powershell
venv\Scripts\Activate.ps1
```

3. Install dependencies:

```powershell
pip install requests python-dotenv apscheduler osmnx networkx shapely
```

4. Refresh pinned dependencies:

```powershell
pip freeze > requirements.txt
```

5. Start collector:

```powershell
python data_collection/collector.py
```

6. After at least ~30 seconds, run validator in a separate terminal:

```powershell
python data_collection/validate_output.py
```

## Output Contract

The primary output is `data_collection/data/world_state.json` with this top-level structure:

- `incidents`: list of incident records.
- `weather`: single weather snapshot object.
- `traffic`: list of road segments (flow + closures).
- `events`: list of event records.
- `risk_areas`: list of sensitive-zone POIs.
- `cameras`: list of camera descriptors.
- `units`: list of simulated police/fire unit states.
- `historical`: list of historical grid cells for heatmaps.
- `last_updated`: ISO8601 UTC timestamp.

Each record includes:

- `_simulated`: whether the record is simulated/estimated.
- `record_type`: normalized schema type (lowercase dataclass name).

Downstream modules should read `world_state.json` and treat each slice independently.

## Simulation Notes

- Weather simulation is used when OpenWeather fails (and fully simulated if both NWS + OpenWeather fail and no cache exists).
- Traffic congestion is simulated per-intersection when TomTom fails, key is missing, or quota is hit.
- Event crowds are estimated when source data does not include expected attendance.
- Camera fallback provides fixed placeholder cameras if AZ511 camera list is empty/unavailable.
- Units are always simulated, but movement follows the real Tempe drive graph when graph data is available.
- Historical and risk-area data are real-source but cache-gated to avoid excessive pull frequency.

## Known Limitations

- Citizen API is unofficial and can change without notice.
- AZ511 camera feeds are snapshot references, not continuous true live streams.
- Unit positions and dispatch behavior are fully simulated for decision-support testing.
- Some source schemas (especially AZ511/Eventbrite variants) can vary over time; parser logic uses defensive fallbacks.

## Contact / Source References

- Citizen Trending: https://citizen.com/api/incident/trending
- NWS Alerts: https://api.weather.gov/alerts/active
- OpenWeather Current Weather: https://api.openweathermap.org/data/2.5/weather
- TomTom Flow Segment: https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json
- AZ511 Events: https://az511.com/api/v2/get/event
- AZ511 Cameras: https://az511.com/api/v2/get/camera
- Eventbrite Search: https://www.eventbriteapi.com/v3/events/search/
- ASU Localist: https://events.asu.edu/api/2/events
- Overpass API: https://overpass-api.de/api/interpreter
- Tempe ArcGIS Calls For Service: https://services.arcgis.com/lQySeXwbBg53XWDi/ArcGIS/rest/services/Calls_For_Service/FeatureServer/0/query
