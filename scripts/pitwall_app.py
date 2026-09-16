"""F1 Pit Wall — live race dashboard for workshop attendees.

  uv run python scripts/pitwall_app.py                       # card resolved from credentials.env
  uv run python scripts/pitwall_app.py --creds <prefix>.env

Standalone adaptation of the main repo's scripts/pitwall/app.py. Starts a local
web server, opens a browser, and streams the race — leaderboard, car #88
telemetry gauges, and (once you build them in Lab 3 / Lab 4) the anomaly and AI
pit-decision panels. Reads topics directly with the Kafka + Schema Registry keys
on your credential card; it only consumes, so it never touches your Flink
compute pool. Stop with Ctrl-C.

Not ported from the main repo: the ``--mock`` offline demo feed
(scripts/pitwall/mock.py) — a dev convenience, not needed for a live workshop.
Everything else (the FastAPI app, the websocket push, the static frontend) is
unchanged.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import webbrowser
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

import uvicorn  # noqa: E402

from credentials import load_card  # noqa: E402
from pitwall_server import create_app  # noqa: E402
from pitwall_state import RaceState  # noqa: E402

logger = logging.getLogger("f1-pitwall")


def main() -> None:
    parser = argparse.ArgumentParser(description="F1 Pit Wall live dashboard (API-key access, no login)")
    parser.add_argument("--creds", help="Path to your <prefix>.env credential card (default: read from credentials.env)")
    parser.add_argument("--port", type=int, default=8000, help="Local port (default 8000)")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open a browser")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Also log the errors the consumer suppresses (e.g. car_state missing before Lab 3)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    state = RaceState()
    stop = threading.Event()

    path, creds = load_card(args.creds)
    logger.info("Using credential card: %s", path)

    from pitwall_consumer import run_consumer

    feed = threading.Thread(target=run_consumer, args=(creds, state, stop), daemon=True)
    feed.start()

    url = f"http://localhost:{args.port}"
    logger.info("Pit Wall live at %s  (Ctrl-C to stop)", url)
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    app = create_app(state)
    try:
        uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()


if __name__ == "__main__":
    main()
