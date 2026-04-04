from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WORLD_STATE_PATH = DATA_DIR / "world_state.json"
GEMINI_TRIGGER_PATH = DATA_DIR / "gemini_trigger.json"


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def _get_world_last_updated() -> str | None:
    payload = _read_json(WORLD_STATE_PATH)
    if not isinstance(payload, dict):
        return None
    value = payload.get("last_updated")
    return str(value) if value is not None else None


def _get_trigger_eval_timestamp() -> str | None:
    payload = _read_json(GEMINI_TRIGGER_PATH)
    if not isinstance(payload, dict):
        return None

    evaluated_at = payload.get("evaluated_at")
    if evaluated_at is not None:
        return str(evaluated_at)

    triggered_at = payload.get("triggered_at")
    if triggered_at is not None:
        return str(triggered_at)

    return None


def _run_validate_output() -> int:
    result = subprocess.run(
        [sys.executable, str(BASE_DIR / "validate_output.py")],
        cwd=str(BASE_DIR.parent),
        capture_output=True,
        text=True,
        check=False,
    )

    print("\n=== validate_output.py ===")
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip())

    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Launch collector.py, monitor world_state and gemini_trigger freshness, "
            "and run validate_output.py at the end."
        )
    )
    parser.add_argument("--duration", type=int, default=140, help="Monitoring duration in seconds")
    parser.add_argument("--poll", type=int, default=15, help="Polling interval in seconds")
    args = parser.parse_args()

    if args.duration < 60:
        print("duration must be at least 60 seconds to observe scheduler updates")
        return 2

    print("Starting collector for live verification...")
    proc = subprocess.Popen(
        [sys.executable, str(BASE_DIR / "collector.py")],
        cwd=str(BASE_DIR.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    world_updates = 0
    trigger_updates = 0
    prev_world = _get_world_last_updated()
    prev_trigger = _get_trigger_eval_timestamp()

    print("Monitoring file freshness...")
    started = time.time()

    try:
        while (time.time() - started) < args.duration:
            time.sleep(args.poll)

            current_world = _get_world_last_updated()
            current_trigger = _get_trigger_eval_timestamp()

            if current_world and current_world != prev_world:
                world_updates += 1
                prev_world = current_world

            if current_trigger and current_trigger != prev_trigger:
                trigger_updates += 1
                prev_trigger = current_trigger

            world_age = None
            parsed_world = _parse_iso(current_world)
            if parsed_world is not None:
                world_age = int((datetime.now(timezone.utc) - parsed_world).total_seconds())

            trigger_age = None
            parsed_trigger = _parse_iso(current_trigger)
            if parsed_trigger is not None:
                trigger_age = int((datetime.now(timezone.utc) - parsed_trigger).total_seconds())

            print(
                "tick: "
                f"world_updates={world_updates}, "
                f"trigger_updates={trigger_updates}, "
                f"world_age_s={world_age}, "
                f"trigger_age_s={trigger_age}"
            )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()

    validate_code = _run_validate_output()

    passed = world_updates >= 2 and trigger_updates >= 1 and validate_code == 0

    print("\n=== Live Check Summary ===")
    print(f"world_state updates observed: {world_updates}")
    print(f"gemini_trigger evaluations observed: {trigger_updates}")
    print(f"validate_output.py exit code: {validate_code}")

    if passed:
        print("PASS: pipeline updates are live and validation passed")
        return 0

    print("FAIL: insufficient live updates or validation failures detected")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
