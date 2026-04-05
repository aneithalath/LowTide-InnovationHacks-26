from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from exa_py import Exa

from schemas import Event

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "events_cache.json"
RISK_AREAS_CACHE_PATH = DATA_DIR / "risk_areas_cache.json"

EXA_QUERIES = [
    "Sun Devil Stadium ASU game concert event 2026 tickets attendance expected",
    "Desert Financial Arena Tempe event 2026 sold out capacity crowd",
    "Tempe Beach Park Mill Avenue festival concert 2026 attendance expected",
    "ASU Gammage Broadway show 2026 capacity audience tickets",
]

VENUE_CAPACITY = {
    "sun devil stadium": 53599,
    "desert financial arena": 14198,
    "tempe center for the arts": 1000,
    "default": 500,
}

TURNOUT_RATE = {
    "sports": 0.92,
    "concert": 0.82,
    "festival": 0.68,
    "community": 0.45,
    "default": 0.55,
}

GENERIC_SOURCE_TOKENS = {
    "eventbrite",
    "meetup",
    "facebook events",
    "wikipedia",
    "downtowntempe",
    "tempenews",
    "signals az",
}

VENUE_ALIAS_TO_CANONICAL = {
    "asu sun devil athletics": "Sun Devil Stadium",
    "asu stadium": "Sun Devil Stadium",
    "dfa": "Desert Financial Arena",
    "marquee theatre": "Downtown Tempe",
    "mill avenue": "Downtown Tempe",
    "downtown tempe, az": "Downtown Tempe",
}


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache() -> list[dict[str, Any]] | None:
    if not CACHE_PATH.exists():
        return None
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _write_cache(payload: list[dict[str, Any]]) -> None:
    _ensure_data_dir()
    tmp_path = CACHE_PATH.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp_path, CACHE_PATH)


def _load_risk_areas() -> list[dict[str, Any]]:
    if not RISK_AREAS_CACHE_PATH.exists():
        return []
    try:
        with RISK_AREAS_CACHE_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
    except Exception:
        return []
    return []


def _infer_event_type(name: str) -> str:
    lowered = (name or "").lower()
    if any(word in lowered for word in ("football", "basketball", "game", "match", "vs")):
        return "sports"
    if any(word in lowered for word in ("concert", "band", "music", "dj", "tour")):
        return "concert"
    if any(word in lowered for word in ("festival", "fair", "market")):
        return "festival"
    return "community"


def _parse_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _obj_get(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _normalize_text(value: str) -> str:
    lowered = (value or "").lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def _is_generic_source_label(value: str) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return False
    return any(token in normalized for token in GENERIC_SOURCE_TOKENS)


def _canonicalize_candidate(value: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        return ""

    for alias, canonical in VENUE_ALIAS_TO_CANONICAL.items():
        if _normalize_text(alias) in normalized:
            return canonical

    return value


def _event_id_from_url(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"exa_{digest}"


def extract_crowd(text: str) -> int | None:
    if not text:
        return None

    exclusion_patterns = [
        r"\d[\d,]+\s*students",
        r"\d[\d,]+\s*faculty",
        r"\d[\d,]+\s*employees",
        r"\d[\d,]+\s*alumni",
        r"\d[\d,]+\s*square\s*feet",
        r"\d[\d,]+\s*acres",
        r"enroll\w*\s*\d[\d,]+",
        r"\d[\d,]+\s*enroll",
    ]

    clean_text = text
    for exc in exclusion_patterns:
        clean_text = re.sub(exc, "", clean_text, flags=re.IGNORECASE)

    patterns = [
        r"(\d[\d,]+)\s*(?:people|attendees|fans|guests|spectators)\s*(?:expected|anticipated|attend)",
        r"(?:expected|estimated|anticipated)\s+(?:attendance|crowd|turnout)[^\d]*(\d[\d,]+)",
        r"(?:seats?|capacity)[^\d]*(\d[\d,]+)",
        r"(\d[\d,]+)\s*(?:tickets?\s*(?:sold|available|remaining))",
        r"(\d[\d,]+)\s*(?:fans|attendees|guests)\s*(?:will|are\s*expected)",
    ]

    for pattern in patterns:
        match = re.search(pattern, clean_text, re.IGNORECASE)
        if match:
            value = int(match.group(1).replace(",", ""))
            if 50 <= value <= 100000:
                return value

    return None


def extract_event_name(result_title: str, result_text: str | None) -> str:
    """Try to extract a specific event name rather than a generic page title."""
    generic_titles = [
        "upcoming",
        "events",
        "calendar",
        "schedule",
        "what's on",
        "things to do",
        "activities",
        "news",
        "home",
    ]
    title = str(result_title or "Unnamed Event").strip()
    title_lower = title.lower()

    if not any(token in title_lower for token in generic_titles) and len(title) > 10:
        return title

    if result_text:
        lines = result_text.split("\n")
        for line in lines[:20]:
            candidate = line.strip()
            if not (10 < len(candidate) < 80):
                continue
            if any(token in candidate.lower() for token in generic_titles):
                continue
            if any(ch.isupper() for ch in candidate):
                return candidate

    return title


def _estimate_crowd(venue_name: str, event_type: str) -> int:
    venue_key = (venue_name or "").strip().lower()
    capacity = VENUE_CAPACITY["default"]
    for key, known_capacity in VENUE_CAPACITY.items():
        if key == "default":
            continue
        if key in venue_key or venue_key in key:
            capacity = known_capacity
            break

    turnout = TURNOUT_RATE.get(event_type, TURNOUT_RATE["default"])
    return int(round(capacity * turnout))


def _extract_venue_candidate(title: str, text: str) -> str:
    title_text = (title or "").strip()
    if not title_text:
        return ""

    patterns = [
        r"\bat\s+([A-Za-z0-9 '&\-\.]{3,80})",
        r"@\s*([A-Za-z0-9 '&\-\.]{3,80})",
    ]
    for pattern in patterns:
        match = re.search(pattern, title_text, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if not _is_generic_source_label(candidate):
                return candidate

    if " - " in title_text:
        parts = [part.strip() for part in title_text.split(" - ") if part.strip()]
        if len(parts) >= 2:
            candidate = parts[-1]
            if not _is_generic_source_label(candidate):
                return candidate

    if "|" in title_text:
        parts = [part.strip() for part in title_text.split("|") if part.strip()]
        if len(parts) >= 2:
            candidate = parts[-1]
            if not _is_generic_source_label(candidate):
                return candidate

    # Fallback to title itself for substring matching against risk area names.
    if _is_generic_source_label(title_text):
        return ""
    return title_text


def _find_risk_area_by_name(name: str, risk_areas: list[dict[str, Any]]) -> dict[str, Any] | None:
    target = _normalize_text(name)
    if not target:
        return None

    for row in risk_areas:
        area_name = str(row.get("name") or "").strip()
        area_normalized = _normalize_text(area_name)
        if not area_normalized:
            continue
        if target in area_normalized or area_normalized in target:
            return row

    return None


def _match_risk_area(venue_candidate: str, risk_areas: list[dict[str, Any]]) -> dict[str, Any] | None:
    canonical_candidate = _canonicalize_candidate(venue_candidate)
    candidate = _normalize_text(canonical_candidate)
    if not candidate:
        return None

    for row in risk_areas:
        area_name = str(row.get("name") or "").strip()
        if not area_name:
            continue
        area_lower = _normalize_text(area_name)
        if candidate in area_lower or area_lower in candidate:
            return row
    return None


def _match_risk_area_from_text(title: str, text: str, risk_areas: list[dict[str, Any]]) -> dict[str, Any] | None:
    haystack = _normalize_text(f"{title} {text}")

    for alias, canonical in VENUE_ALIAS_TO_CANONICAL.items():
        if _normalize_text(alias) in haystack:
            matched = _find_risk_area_by_name(canonical, risk_areas)
            if matched is not None:
                return matched

    for row in risk_areas:
        area_name = _normalize_text(str(row.get("name") or "").strip())
        if area_name and area_name in haystack:
            return row
    return None


def _extract_date_string(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "unknown-date"
    return raw[:10]


def fetch_events_data() -> list[dict[str, Any]]:
    load_dotenv(BASE_DIR.parent / ".env")
    exa_api_key = os.getenv("EXA_API_KEY")

    try:
        if not exa_api_key:
            raise RuntimeError("EXA_API_KEY missing")

        risk_areas = _load_risk_areas()
        if not risk_areas:
            raise RuntimeError("risk_areas_cache.json missing or empty")

        exa = Exa(api_key=exa_api_key)
        merged: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for query in EXA_QUERIES:
            search_results = exa.search(
                query,
                type="auto",
                num_results=5,
                contents={"text": {"max_characters": 10000}}
            )

            results_count = len(getattr(search_results, "results", []))
            matched_count = 0

            for row in getattr(search_results, "results", []):
                title = str(_obj_get(row, "title") or "Unnamed Event")
                url = str(_obj_get(row, "url") or "").strip()
                if not url:
                    continue
                text = str(_obj_get(row, "text") or "")
                name = extract_event_name(title, text)
                start_time = str(_obj_get(row, "published_date") or datetime.now(UTC).isoformat())
                event_type = _infer_event_type(f"{query} {title}")

                venue_candidate = _extract_venue_candidate(title, text)
                matched_venue = _match_risk_area(venue_candidate, risk_areas)
                if matched_venue is None:
                    matched_venue = _match_risk_area_from_text(title, text, risk_areas)
                if matched_venue is None:
                    continue

                venue_name = str(matched_venue.get("name") or "").strip()
                if not venue_name:
                    continue

                date_string = _extract_date_string(start_time)
                dedupe_key = (venue_name.lower(), date_string)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                crowd = extract_crowd(text)
                crowd_source = "extracted"
                simulated = False

                if crowd is None:
                    crowd = _estimate_crowd(venue_name, event_type)
                    crowd_source = "estimated"
                    simulated = True

                event_id = _event_id_from_url(url)

                merged.append(
                    Event(
                        event_id=event_id,
                        name=name,
                        type=event_type,
                        lat=_parse_float(matched_venue.get("lat")),
                        lng=_parse_float(matched_venue.get("lng")),
                        start_time=start_time,
                        end_time=None,
                        expected_crowd=crowd,
                        crowd_source=crowd_source,
                        _simulated=simulated,
                    ).to_dict()
                )
                matched_count += 1

            _log(f"events query matched {matched_count}/{results_count} results")

        _write_cache(merged)
        _log(f"events updated: {len(merged)} records")
        return merged

    except Exception as err:
        cached = _load_cache()
        _log(f"events error: {err}")
        if cached is not None:
            return cached
        return []


if __name__ == "__main__":
    rows = fetch_events_data()
    _log(f"events ready: {len(rows)} records")
