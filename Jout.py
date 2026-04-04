import ollama
import json
import re
import sys
import uuid
from datetime import datetime, timezone

# =========================
# INPUT DATA
# =========================

incidents = [
    {
        "id": "inc_1",
        "location_name": "Downtown Mall & 5th Avenue",
        "coordinates": {"latitude": 33.4480, "longitude": -112.0740},
        "reports": [
            "Man possibly armed near downtown mall, people running",
            "Hearing shouting and panic near shopping center",
            "Security reporting suspicious individual, weapon unconfirmed"
        ]
    },
    {
        "id": "inc_2",
        "location_name": "Oak Street & Riverside Drive",
        "coordinates": {"latitude": 33.4320, "longitude": -111.9560},
        "reports": [
            "Car accident at intersection, two vehicles involved",
            "One person might be injured, not moving",
            "Traffic backing up heavily in the area"
        ]
    },
    {
        "id": "inc_3",
        "location_name": "Maple Apartments, 800 West Block",
        "coordinates": {"latitude": 33.4510, "longitude": -111.9810},
        "reports": [
            "Smoke coming from second floor apartment window",
            "Residents evacuating building on their own",
            "Caller says they can hear fire alarm going off"
        ]
    },
    {
        "id": "inc_4",
        "location_name": "Central Park South Entrance",
        "coordinates": {"latitude": 33.4270, "longitude": -111.9430},
        "reports": [
            "Elderly person collapsed on sidewalk",
            "Not breathing according to bystander",
            "Bystander attempting CPR"
        ]
    },
    {
        "id": "inc_5",
        "location_name": "Westside Residential Block, Elm Street",
        "coordinates": {"latitude": 33.4190, "longitude": -112.0100},
        "reports": [
            "Noise complaint from neighbor about loud music",
            "Party still going at 2am",
            "No violence reported"
        ]
    }
]

# =========================
# UNIT POOL
# =========================

PATROL_UNITS = [("P-104", "En route"), ("P-212", "En route"), ("P-305", "Available")]
EMS_UNITS    = [("AMB-09", 33.4220, -111.9350), ("AMB-14", 33.4410, -111.9600)]
FIRE_UNITS   = [("F-01",  33.4500, -111.9900), ("F-03",  33.4100, -112.0200)]


def get_dispatch_units(unit_type: str, priority: float) -> list:
    """Return 1 or 2 units depending on priority level."""
    count = 2 if priority >= 0.8 else 1
    if unit_type == "patrol":
        return [{"vehicle_id": u[0], "status": u[1]} for u in PATROL_UNITS[:count]]
    if unit_type == "ems":
        return [{"vehicle_id": u[0], "status": "En route"} for u in EMS_UNITS[:count]]
    if unit_type == "fire":
        return [{"vehicle_id": u[0], "status": "En route"} for u in FIRE_UNITS[:count]]
    return []


def get_standby_unit(unit_type: str) -> dict:
    """
    Always return a medical standby unit.
    If primary dispatch is patrol or fire, use AMB-09.
    If primary dispatch is ems, use AMB-14 as secondary standby.
    """
    if unit_type in ("patrol", "fire"):
        u = EMS_UNITS[0]
    else:
        u = EMS_UNITS[1]
    return {"unit_id": u[0], "standby_location": {"latitude": u[1], "longitude": u[2]}}


# =========================
# CLEAN JSON FUNCTION
# =========================

def clean_json(raw: str) -> str:
    """Extract first {...} block, strip markdown fences."""
    raw = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).strip()
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        return match.group(0).strip()
    return raw.strip()


# =========================
# VALIDATION FUNCTION
# =========================

def is_valid(parsed: dict) -> bool:
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
    if not isinstance(parsed.get("police_dispatch_action"), str):
        return False
    if not isinstance(parsed.get("medical_standby_message"), str):
        return False
    if not isinstance(parsed.get("traffic_control_action"), str):
        return False
    return True


# =========================
# LLM CALL + EXTRACTION
# =========================

def extract_incident(incident: dict) -> dict | None:
    """Call LLM for one incident, validate response, assemble full output schema."""
    reports_text = "\n".join(f"- {r}" for r in incident["reports"])
    location     = incident.get("location_name", "Unknown Location")

    prompt = f"""You are an emergency dispatch triage AI system.

Analyze the incident below and return ONLY a single valid JSON object.
No markdown. No backticks. No explanation. No extra text before or after.

LOCATION: {location}

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
  "police_dispatch_action": "One specific instruction for patrol units at this scene.",
  "medical_standby_message": "One specific pre-notification message for EMS standby.",
  "traffic_control_action": "One specific signal timing or re-routing instruction for this location."
}}

Constraints:
- threat_level: exactly one of "High", "Medium", "Low"
- is_life_threatening: exactly true or false (no quotes)
- required_unit_type: exactly one of "patrol", "ems", "fire"
- confidence_score: float 0.0–1.0
- priority_score: float 0.0–1.0
- risk_factors: 2–4 strings specific to these reports
- explanation: specific to this incident, not generic

Return ONLY the JSON object now:"""

    try:
        response = ollama.chat(
            model="llama3",
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.2}
        )
    except Exception as e:
        print(f"[ERROR] [{incident['id']}] LLM call failed: {e}", file=sys.stderr)
        return None

    raw = response["message"]["content"]

    try:
        cleaned = clean_json(raw)
        parsed  = json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"[ERROR] [{incident['id']}] JSON parse error: {e}", file=sys.stderr)
        print(f"[ERROR] Raw: {repr(raw[:400])}", file=sys.stderr)
        return None

    if not is_valid(parsed):
        print(f"[ERROR] [{incident['id']}] Validation failed: {parsed}", file=sys.stderr)
        return None

    # Normalize numerics
    parsed["confidence_score"] = float(parsed["confidence_score"])
    parsed["priority_score"]   = float(parsed["priority_score"])

    unit_type = parsed["required_unit_type"]
    priority  = parsed["priority_score"]

    # Dispatch units for primary response
    dispatch_units = get_dispatch_units(unit_type, priority)

    # Medical standby block (always present)
    standby        = get_standby_unit(unit_type)
    medical_standby = {
        "unit_id":          standby["unit_id"],
        "instruction":      "Pre-notification",
        "message":          parsed["medical_standby_message"],
        "standby_location": standby["standby_location"]
    }

    # Assemble output schema — mirrors sample exactly
    result = {
        "prediction_id": (
            f"risk-analysis-"
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}-"
            f"{str(uuid.uuid4())[:4]}"
        ),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "id": incident["id"],
        "risk_assessment": {
            "level":         parsed["threat_level"],
            "coordinates":   incident["coordinates"],
            "location_name": location,
            "risk_factors":  parsed["risk_factors"],
            "explanation":   parsed["explanation"]
        },
        "mitigation_strategy": {
            "police_dispatch": {
                "action":         parsed["police_dispatch_action"],
                "assigned_units": dispatch_units
            },
            "medical_standby": medical_standby,
            "traffic_control": {
                "re-routing": parsed["traffic_control_action"]
            }
        },
        "triage_meta": {
            "is_life_threatening": parsed["is_life_threatening"],
            "required_unit_type":  unit_type,
            "confidence_score":    parsed["confidence_score"],
            "priority_score":      priority
        }
    }

    return result


# =========================
# PROCESS ALL INCIDENTS
# =========================

def process_incidents(incidents: list) -> list:
    """Run all incidents, return sorted by priority_score descending."""
    results = []
    for inc in incidents:
        result = extract_incident(inc)
        if result:
            results.append(result)

    results.sort(
        key=lambda r: r["triage_meta"]["priority_score"],
        reverse=True
    )
    return results


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    results = process_incidents(incidents)
    print(json.dumps(results, indent=2))