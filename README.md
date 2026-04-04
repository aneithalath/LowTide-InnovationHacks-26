# LowTide-InnovationHacks-26
InnovationHacks2026

## Risk Prediction Web Server

### Install server dependencies

```powershell
pip install -r requirements-webserver.txt
```

### Run with Uvicorn

```powershell
uvicorn prediction_server:app --host 0.0.0.0 --port 8000 --reload
```

### Endpoints

- Health check: http://127.0.0.1:8000/
- Prediction JSON: http://127.0.0.1:8000/prediction
- Dispatch route (GraphML shortest path): http://127.0.0.1:8000/route/dispatch
- Waypoint route snap (GraphML): http://127.0.0.1:8000/route/waypoints

The mapping frontend reads this endpoint by default using:

- `VITE_RISK_PREDICTION_API=http://127.0.0.1:8000/prediction`
