# LowTide Sentinel
Real-time operational intelligence layer for emergency response: stateful risk detection, trigger-gated AI summaries, and actionable dispatch recommendations.

## Problem Statement
Emergency response teams are flooded with fragmented, high-volume feeds (incidents, traffic, weather, events, units). Typical pipelines are stateless, reprocess everything every cycle, and fail to distinguish what is actually new, escalated, or resolved. That creates alert fatigue, inconsistent severity handling, and slower dispatch decisions when minutes matter.

## Solution Overview
LowTide Sentinel is an event-driven decision support system built to reduce noise and surface action.

- Maintains persistent incident state across cycles.
- Detects deltas (new, escalated, resolved) instead of re-alerting unchanged data.
- Computes dynamic zone-level risk from incident, crowd, traffic, historical, and weather signals.
- Evaluates tiered trigger logic to decide when Gemini should run.
- Produces concise operational briefs and recommended actions instead of raw data dumps.

This project is an intelligence layer for dispatch support. It is not predictive policing, and it is not a generic "AI wrapper."

## System Architecture

### Core Components
- `data_collection/collector.py`
	- Orchestrates ingestion jobs on intervals and writes canonical world state.
	- Triggers risk scoring and trigger evaluation jobs.
- `data_collection/risk_engine.py`
	- Normalizes incident severity.
	- Tracks incident lifecycle state.
	- Scores historical grid cells into composite risk zones.
	- Writes `gemini_input.json` and compact `gemini_brief.json`.
- `data_collection/gemini_trigger.py`
	- Compares current state vs previous snapshot.
	- Applies critical/high/moderate trigger tiers with cooldowns.
	- Emits `gemini_trigger.json` with send-gating flags (`triggered`, `ready_to_send_gemini`, `consumed`).
- `prediction_server.py` (FastAPI)
	- Serves predictions and routing/telemetry APIs.
	- Runs collector one-shot, reads compact brief artifacts, and returns decision-ready prediction payloads.
	- Supports OpenAI-compatible backends (Gemini by default, NVIDIA GPT OSS as toggle).
- `mapping-demo/` (React + Vite + TypeScript)
	- Interactive operational map consuming backend prediction, route, and emergency telemetry endpoints.

### End-to-End Flow
```text
Live + Cached Data Sources
				-> collector.py (state updates)
				-> risk_engine.py (risk scoring + compact briefs)
				-> gemini_trigger.py (tiered trigger decision)
				-> Gemini call only when trigger gate allows
				-> prediction_server.py (actionable API payloads)
				-> mapping-demo (operator-facing map and threat panel)
```

## Key Features
- Stateful incident tracking (`new`, `escalated`, `stable`, `resolved`).
- Dynamic multi-factor risk scoring per zone.
- Trigger-tier AI gating to avoid redundant LLM calls.
- Real-time interval processing with fallback/cached data paths.
- Decision-focused outputs (`gemini_brief.json`) for rapid operator interpretation.
- Dispatch-aware recommendations with nearest-unit context.

## Tech Stack

### Backend / Data Layer
- Python
- FastAPI + Uvicorn
- APScheduler
- NetworkX + OSMnx (routing and road-graph simulation)
- OpenAI SDK (used with Gemini OpenAI-compatible endpoint and optional NVIDIA-hosted GPT OSS)
- python-dotenv

### Frontend
- React 19
- Vite 8
- TypeScript
- Material UI (`@mui/material`, `@mui/icons-material`)
- MapTiler SDK
- Tailwind CSS
- hls.js

### External Data / APIs Integrated
- Google Gemini API
- Citizen incident feed
- NOAA/NWS alerts
- OpenWeatherMap
- TomTom traffic flow
- Exa search (event signals)
- Overpass API (risk-area POIs)
- Tempe ArcGIS calls-for-service
- AZ511 cameras

## Setup

### Backend Setup
From the repository root:

```bash
pip install -r requirements.txt
pip install -r requirements-webserver.txt
uvicorn prediction_server:app --host 0.0.0.0 --port 8000 --reload
```

Optional: run the continuous collector/scheduler directly:

```bash
python data_collection/collector.py
```

### Frontend Setup
Frontend directory: `mapping-demo` (React + Vite).

```bash
cd mapping-demo
npm install
npm run dev
```

The frontend defaults to these backend endpoints:
- `http://127.0.0.1:8000/prediction`
- `http://127.0.0.1:8000/route/dispatch`
- `http://127.0.0.1:8000/route/waypoints`
- `http://127.0.0.1:8000/telemetry/emergency-snapshot`

## Project Structure
```text
.
|- prediction_server.py              # FastAPI backend for prediction, routing, and telemetry APIs
|- data_collection/
|  |- collector.py                   # Scheduled ingestion and world-state orchestration
|  |- risk_engine.py                 # Stateful incident diffing + zone risk scoring + brief generation
|  |- gemini_trigger.py              # Tiered trigger evaluation and Gemini send gating
|  |- data/
|     |- world_state.json            # Canonical merged operational state
|     |- gemini_input.json           # Full scored payload for downstream reasoning
|     |- gemini_brief.json           # Compact decision brief for operator/LLM consumption
|     |- gemini_trigger.json         # Trigger gate artifact with readiness and status
|- mapping-demo/                     # React/Vite operator map frontend
```

## How It Works (Step-by-Step)
1. Ingestion jobs collect incidents, weather, traffic, events, risk areas, historical data, and unit telemetry.
2. `collector.py` merges these into `world_state.json` and updates timestamped state.
3. `risk_engine.py` compares incidents with persistent state to identify true deltas, then scores zones with weighted multi-signal logic.
4. `gemini_trigger.py` evaluates trigger tiers and cooldowns against snapshot history.
5. Gemini is considered only when trigger conditions are met and send-gating flags permit it.
6. `prediction_server.py` assembles decision-ready prediction payloads and serves frontend/API clients.

## API Endpoints
- `GET /` health check
- `GET /prediction` current sorted prediction list
- `POST /prediction/refresh` force refresh predictions
- `POST /prediction` replace stored prediction list
- `POST /route/dispatch` shortest path between start and target
- `POST /route/waypoints` route through waypoint sequence
- `GET /telemetry/emergency-snapshot` read snapshot
- `POST /telemetry/emergency-snapshot` persist snapshot

## Example Output (Condensed)
Short, decision-oriented shape (from `gemini_brief.json`):

```json
{
	"trigger_tier": "critical",
	"overall_risk_level": "MODERATE",
	"situation": {
		"total_active_incidents": 50,
		"new_or_escalated_this_cycle": 1,
		"units_available": 37,
		"units_deployed": 0
	},
	"delta_incidents": [
		{
			"title": "Ford Focus Involved in Hit-and-Run Crash Near McDonald's",
			"normalized_severity": 6,
			"change_type": "new"
		}
	],
	"top_3_zones": [
		{
			"zone_id": "grid_33.430_-111.930",
			"composite_risk_score": 0.5345,
			"recommended_action": "Dispatch TEMPE-AMB-03 to grid_33.430_-111.930"
		}
	]
}
```

## Future Improvements
- More robust crowd estimation from multimodal event signals.
- Better congestion forecasting and route-time reliability.
- Smarter multi-unit resource balancing under concurrent incidents.
- Learning-based calibration of risk weights and trigger thresholds.
