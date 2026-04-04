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
| TomTom Traffic Flow | both | every 10 min | `TOMTOM_API_KEY` | 12 fixed intersections; handles 429 by simulating remaining intersections in batch. |
| Exa Search API | both | every 60 min (4 searches per cycle) | `EXA_API_KEY` | Web search for Tempe/ASU event signals with attendance extraction from page text. |
| ASU Localist | removed | n/a | no | Removed from active ingestion path. |
| Overpass API (OSM POIs) | real | every 24 hours | no | Schools/hospitals/stadiums/university/etc; cached daily. |
| Tempe ArcGIS Calls For Service FeatureServer | real | startup/background only (7-day cache) | no | Paged pull up to 20,000 records, aggregated into 0.005-degree grid cells. |
| AZ511 Cameras | both | startup only | no | Snapshot camera metadata filtered to Tempe bbox; uses 6 placeholders if empty/unavailable. |
| OSMnx + NetworkX road graph | simulated behavior over real map | loaded at startup, reused | no | Unit movement is simulated on Tempe drive graph with routing and congestion effects. |

## File Inventory

- `citizen_data.py`: Existing Citizen incident fetch + normalization helper used by collector.
- `schemas.py`: Dataclass schemas for all normalized record types (`to_dict()` on each schema).
- `weather_data.py`: NWS + OpenWeather merge into one weather snapshot and weather cache.
- `traffic_data.py`: TomTom flow by intersection + congestion simulation fallback.
- `events_data.py`: Exa-powered event ingestion, crowd extraction, risk-area venue matching, and event cache writes.
- `risk_areas_data.py`: Overpass POI ingestion and once-daily cache refresh logic.
- `historical_data.py`: ArcGIS pagination and grid-cell heatmap aggregation.
- `cameras_data.py`: AZ511 camera metadata pull and fallback camera list.
- `unit_simulation.py`: Police/fire unit state machine and movement tick on Tempe road graph.
- `gemini_trigger.py`: Trigger evaluator that compares world-state snapshots and writes severity-tier trigger flags.
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
pip install requests python-dotenv apscheduler osmnx networkx shapely exa-py
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

Runtime notes:
- Traffic now updates every 10 minutes.
- Gemini trigger evaluation runs every 60 seconds and writes `gemini_trigger.json` on every cycle (for observability), regardless of whether a trigger fires.

## Output Contract

The primary output is `data_collection/data/world_state.json` with this top-level structure:

- `incidents`: list of incident records.
- `weather`: single weather snapshot object.
- `traffic`: list of road segments.
- `events`: list of event records.
- `risk_areas`: list of sensitive-zone POIs.
- `cameras`: list of camera descriptors.
- `units`: list of simulated police/fire unit states.
- `historical`: list of historical grid cells for heatmaps.
- `last_updated`: ISO8601 UTC timestamp.

Each record includes:

- `_simulated`: whether the record is simulated/estimated.
- `record_type`: normalized schema type (lowercase dataclass name).

Additional trigger artifact:

- `data_collection/data/gemini_trigger.json`

Writes on every evaluation cycle (every 60 seconds). Example when no trigger fires:

```json
{
  "evaluated_at": "2026-04-04T22:24:28.666049+00:00",
  "triggered": false,
  "ready_to_send_gemini": false,
  "status": "no trigger conditions met",
  "severity_tier": "none",
  "reasons": [],
  "world_state_snapshot_path": "data/world_state.json",
  "consumed": false,
  "last_triggered_at": "2026-04-04T22:07:31.768065+00:00",
  "last_trigger_tier": "critical",
  "reason_counts": {"critical": 0, "high": 0, "moderate": 0}
}
```

Example when a trigger fires:

```json
{
  "evaluated_at": "2026-04-04T22:24:28.666049+00:00",
  "triggered": true,
  "ready_to_send_gemini": true,
  "triggered_at": "2026-04-04T22:24:28.666049+00:00",
  "severity_tier": "critical|high|moderate",
  "reasons": ["human-readable reason"],
  "world_state_snapshot_path": "data/world_state.json",
  "consumed": false
}
```

**Key rule for downstream systems:** Only send a Gemini API request when `triggered=true AND ready_to_send_gemini=true AND consumed=false`. This prevents unnecessary API key consumption.

## Gemini Trigger Logic

`gemini_trigger.py` compares the current world state with `data/last_trigger_snapshot.json` and evaluates three trigger tiers:

- `critical` (cooldown: 0 sec):
  - New incident with severity >= 8.
  - Weather alert transition from null to non-null.
  - Dispatch state reaches 4+ simultaneously dispatched units.
- `high` (cooldown: 180 sec since last trigger of any tier):
  - New incident with severity >= 6.
  - Traffic congestion crossing above 0.85 within 0.02 degrees of a hotspot.
  - Event with expected crowd >= 10,000 starting within 90 minutes.
- `moderate` (cooldown: 600 sec since last trigger of any tier):
  - Incident count increase >= 5 since prior snapshot.
  - Traffic congestion crossing above 0.75 within 0.02 degrees of a hotspot.
  - Event with expected crowd >= 5,000 starting within 90 minutes.

Cooldown behavior:
- Critical triggers always fire immediately.
- High triggers are blocked until 180 seconds have elapsed since the most recent trigger.
- Moderate triggers are blocked until 600 seconds have elapsed since the most recent trigger.

Field meanings:
- `evaluated_at`: ISO8601 UTC timestamp of the evaluation cycle (even if no trigger fires).
- `triggered`: Boolean; true if a trigger condition was met and fired, false otherwise.
- `ready_to_send_gemini`: Explicit signal for downstream; true only when fired=true, false otherwise.
- `status`: Human-readable status when no trigger fires (e.g., "no trigger conditions met", "world_state missing or invalid").
- `reason_counts`: Only present when not triggered; shows how many conditions were met per tier (for debugging).
- `consumed`: Boolean reserved for the Gemini integration layer; set to true after the Gemini API request is sent (prevents duplicate calls).
- File writes every cycle for observability and health-check purposes; **only trigger actual Gemini API calls when ready_to_send_gemini=true**.

## Simulation Notes

- Weather simulation is used when OpenWeather fails (and fully simulated if both NWS + OpenWeather fail and no cache exists).
- Traffic congestion is simulated per-intersection when TomTom fails, key is missing, or quota is hit.
- Event crowds may use simulated estimates when Exa results do not include attendance figures.
- Camera fallback provides fixed placeholder cameras if AZ511 camera list is empty/unavailable.
- Units are always simulated, but movement follows the real Tempe drive graph when graph data is available.
- Historical and risk-area data are real-source but cache-gated to avoid excessive pull frequency.

## Gemini Trigger Observability

The trigger file writes every evaluation cycle for pipeline health and observability, but **does not consume Gemini API keys during write-only cycles**. Downstream systems must check `ready_to_send_gemini=true` before calling the Gemini API. This design allows:

- Monitoring: Confirm the scheduler is running via `evaluated_at` timestamps.
- Debugging: See `reason_counts` to understand why a trigger did or did not fire.
- Cost control: Gemini API quota is only consumed when a real trigger condition fires.

## Known Limitations

- Citizen API is unofficial and can change without notice.
- Exa results depend on web indexing freshness.
- Venue matching for events requires venue names that exist in Overpass-derived risk-area data.
- AZ511 endpoint support for traffic closures was removed due to broken endpoint behavior.
- AZ511 camera feeds are snapshot references, not continuous true live streams.
- Unit positions and dispatch behavior are fully simulated for decision-support testing.

## Source References

- Citizen Trending: https://citizen.com/api/incident/trending
- NWS Alerts: https://api.weather.gov/alerts/active
- OpenWeather Current Weather: https://api.openweathermap.org/data/2.5/weather
- TomTom Flow Segment: https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json
- Exa docs: https://exa.ai/docs
- Exa dashboard: https://dashboard.exa.ai
- Overpass API: https://overpass-api.de/api/interpreter
- Tempe ArcGIS Calls For Service: https://services.arcgis.com/lQySeXwbBg53XWDi/ArcGIS/rest/services/Calls_For_Service/FeatureServer/0/query

## Request Budget Guidance

- TomTom traffic polling consumes approximately 144 requests/hour at 10-minute intervals across 12 intersections.
- Exa event ingestion consumes 4 requests/hour (4 queries per hourly event cycle).
- Both fit typical free-tier dev/demo sessions, but long overnight runs can still exceed free-tier limits.
