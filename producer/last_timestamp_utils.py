import json
import os
from datetime import datetime, timezone
from pathlib import Path

MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc).isoformat()

STATE_FILE = os.environ.get("PRODUCER_STATE_FILE", "state/state.json")


async def read_last_ts() -> str:
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        return state.get("last_ts") or MIN_TIMESTAMP


async def write_last_ts(ts: str):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_ts": ts}, f)


async def validate_state_file():
    path = Path(STATE_FILE)
    path.parent.mkdir(exist_ok=True, parents=True)

    if not path.is_file():
        await write_last_ts(MIN_TIMESTAMP)
    with open(path) as f:
        try:
            json.load(f)
        except Exception:
            await write_last_ts(MIN_TIMESTAMP)
