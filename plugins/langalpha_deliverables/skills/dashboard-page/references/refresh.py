"""Write the Dashboard's custom data. Run with ExecuteCode, which can import tools.*:

    import runpy; runpy.run_path("dashboard/refresh.py", run_name="__main__")

Each file holds what its panel draws and an ``as_of`` the panel shows.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"


def write(name: str, payload: dict) -> None:
    DATA.mkdir(exist_ok=True)
    payload = {"as_of": datetime.now(timezone.utc).isoformat(), **payload}
    tmp = DATA / f".{name}.json.tmp"
    tmp.write_text(json.dumps(payload))
    tmp.replace(DATA / f"{name}.json")


def screen() -> dict:
    # Replace with the computation the panel needs, using the data tools your
    # code imports (from tools.<module> import ...).
    rows = [{"symbol": "AAPL", "change_pct": 1.2}]
    return {"rows": rows}


if __name__ == "__main__":
    write("screen", screen())
