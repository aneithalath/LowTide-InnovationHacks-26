import ollama
import json
import re

# =========================
# INPUT DATA
# =========================

incidents = [
    {
        "id": "inc_1",
        "reports": [
            "Man possibly armed near downtown mall, people running",
            "Hearing shouting and panic near shopping center",
            "Security reporting suspicious individual, weapon unconfirmed"
        ]
    },
    {
        "id": "inc_2",
        "reports": [
            "Car accident at intersection, two vehicles involved",
            "One person might be injured, not moving",
            "Traffic backing up heavily in the area"
        ]
    },
    {
        "id": "inc_3",
        "reports": [
            "Smoke coming from second floor apartment window",
            "Residents evacuating building on their own",
            "Caller says they can hear fire alarm going off"
        ]
    },
    {
        "id": "inc_4",
        "reports": [
            "Elderly person collapsed on sidewalk",
            "Not breathing according to bystander",
            "Bystander attempting CPR"
        ]
    },
    {
        "id": "inc_5",
        "reports": [
            "Noise complaint from neighbor about loud music",
            "Party still going at 2am",
            "No violence reported"
        ]
    }
]

# =========================
# CLEAN JSON FUNCTION
# =========================

def clean_json(raw: str) -> str:
    """
    Extracts the first valid JSON object from raw LLM output.
    Strips markdown fences, extra text, and whitespace.
    """
    # Remove markdown code fences if present
    raw = re.sub(r"```(?:json)?", "", raw, flags=re.IGNORECASE).strip()

    # Extract the first {...} block (greedy match across newlines)
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        return match.group(0).strip()

    return raw.strip()

# =========================
# VALIDATION FUNCTION
# =========================

def is_valid(parsed: dict) -> bool:
    """
    Validates a parsed LLM response against the required schema.
    Returns True only if all fields are present and within acceptable bounds.
    """
    if not isinstance(parsed, dict):
        return False

    # Required field: threat_level
    if parsed.get("threat_level") not in {"High", "Medium", "Low"}:
        return False

    # Required field: is_life_threatening (must be strict bool, not 0/1)
    if not isinstance(parsed.get("is_life_threatening"), bool):
        return False

    # Required field: required_unit_type
    if parsed.get("required_unit_type") not in {"patrol", "ems", "fire"}:
        return False

    # Required field: confidence_score (numeric, 0–1)
    confidence = parsed.get("confidence_score")
    if not isinstance(confidence, (float, int)):
        return False
    if not (0.0 <= float(confidence) <= 1.0):
        return False

    # Required field: priority_score (numeric, 0–1)
    priority = parsed.get("priority_score")
    if not isinstance(priority, (float, int)):
        return False
    if not (0.0 <= float(priority) <= 1.0):
        return False

    return True

# =========================
# LLM CALL + EXTRACTION
# =========================

def extract_incident(incident: dict) -> dict | None:
    """
    Sends a single incident to the LLM, parses and validates the response.
    Returns a result dict or None if the output was invalid.
    """
    reports_text = "\n".join(f"- {r}" for r in incident["reports"])

    prompt = f"""You are an emergency dispatch triage system. Analyze the incident reports below and return a JSON assessment.

INCIDENT REPORTS:
{reports_text}

UNIT ASSIGNMENT RULES:
- "patrol": threats, weapons, suspicious persons, civil disturbances
- "ems": injuries, medical emergencies, unconscious persons, cardiac events
- "fire": fires, smoke, explosions, hazmat

PRIORITY SCORE GUIDELINES:
- 1.0: immediate life-threatening emergency (cardiac arrest, active shooter)
- 0.8–0.9: high urgency (weapon present, serious injury, fire)
- 0.5–0.7: moderate urgency (unconfirmed threat, minor injury, traffic incident)
- below 0.5: low urgency (noise complaint, minor disturbance)

You MUST return ONLY a single JSON object. No markdown. No explanation. No backticks. No extra text.

Required output format:
{{
  "threat_level": "High",
  "is_life_threatening": true,
  "required_unit_type": "patrol",
  "confidence_score": 0.85,
  "priority_score": 0.9
}}

Allowed values:
- threat_level: exactly one of "High", "Medium", "Low"
- is_life_threatening: exactly true or false
- required_unit_type: exactly one of "patrol", "ems", "fire"
- confidence_score: float between 0.0 and 1.0
- priority_score: float between 0.0 and 1.0

Return ONLY the JSON object now:"""

    try:
        response = ollama.chat(
            model="llama3",
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0.2}
        )
    except Exception as e:
        print(f"\n❌ [{incident['id']}] LLM call failed: {e}")
        return None

    raw = response["message"]["content"]

    try:
        cleaned = clean_json(raw)
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"\n❌ [{incident['id']}] JSON parse error: {e}")
        print(f"   Raw output: {repr(raw[:300])}")
        return None

    if not is_valid(parsed):
        print(f"\n❌ [{incident['id']}] Validation failed")
        print(f"   Parsed output: {parsed}")
        return None

    # Normalize numeric fields to float
    parsed["confidence_score"] = float(parsed["confidence_score"])
    parsed["priority_score"] = float(parsed["priority_score"])

    return {
        "id": incident["id"],
        "extracted": parsed
    }

# =========================
# PROCESS ALL INCIDENTS
# =========================

def process_incidents(incidents: list) -> list:
    """
    Processes all incidents through the LLM pipeline.
    Returns a list of valid results sorted by priority_score descending.
    Maintains stable order for ties (preserves original input order).
    """
    results = []

    print(f"Processing {len(incidents)} incident(s)...\n")

    for i, inc in enumerate(incidents, 1):
        print(f"  [{i}/{len(incidents)}] Processing {inc['id']}...", end=" ", flush=True)
        result = extract_incident(inc)
        if result:
            results.append(result)
            score = result["extracted"]["priority_score"]
            level = result["extracted"]["threat_level"]
            print(f"✅  priority={score:.2f}  threat={level}")
        else:
            print(f"⚠️  Skipped (invalid output)")

    # Sort by priority_score descending; stable sort preserves original order for ties
    results.sort(key=lambda r: r["extracted"]["priority_score"], reverse=True)

    return results

# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    print("=" * 50)
    print("  EMERGENCY INCIDENT TRIAGE SYSTEM")
    print("  Model: llama3 | Mode: Local (Ollama)")
    print("=" * 50 + "\n")

    results = process_incidents(incidents)

    print("\n" + "=" * 50)
    print("  FINAL TRIAGE OUTPUT (sorted by priority)")
    print("=" * 50 + "\n")

    if not results:
        print("⚠️  No valid results to display.")
    else:
        print(json.dumps(results, indent=2))

    print(f"\n✅ Processed {len(results)}/{len(incidents)} incidents successfully.")