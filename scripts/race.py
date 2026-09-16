"""Run the F1 race simulator locally from a credential card.

  uv run python scripts/race.py
  uv run python scripts/race.py --20                 # shorthand for --seconds-per-lap 20
  uv run python scripts/race.py --creds <card>.env --seconds-per-lap 60 --once

Standalone adaptation of the main repo's scripts/selfservice/race.py: same
simulator, same pacing/warmup logic, retargeted at gemini-workshop's own
``datagen/`` copy and local ``credentials.py``/``deployment.py`` modules.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
_GEMINI_WORKSHOP_ROOT = _THIS_DIR.parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
if str(_GEMINI_WORKSHOP_ROOT) not in sys.path:
    sys.path.insert(0, str(_GEMINI_WORKSHOP_ROOT))  # so `from datagen import simulator` resolves

import deployment as meta  # noqa: E402
from credentials import load_card  # noqa: E402


def _strip_scheme(bootstrap: str) -> str:
    """confluent-kafka wants host:port; the card bootstrap may carry a scheme."""
    return bootstrap.split("://", 1)[-1] if "://" in bootstrap else bootstrap


def _expand_numeric_flags(argv: list[str]) -> list[str]:
    """Rewrite the ``--<N>`` shorthand to ``--seconds-per-lap <N>``."""
    out: list[str] = []
    for arg in argv:
        if arg.startswith("--") and arg[2:].isdigit():
            out += ["--seconds-per-lap", arg[2:]]
        else:
            out.append(arg)
    return out


def _resolve_seconds_per_lap(explicit: int | None, root: Path) -> int:
    if explicit is not None:
        raw: str | int = explicit
        source = "--seconds-per-lap"
    else:
        saved = meta.load_meta(root).get(meta.KEY_SECONDS_PER_LAP)
        raw = saved or meta.DEFAULT_SECONDS_PER_LAP
        source = "runs/deployment.env" if saved else "default"

    value, problem = meta.validate_seconds_per_lap(raw)
    if problem:
        sys.exit(f"Error: {problem}")
    print(f"Pacing: {value}s/lap (~{60 * value // 60}-minute race, from {source})")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the F1 race simulator locally from a credential card",
        epilog="Shorthand: --20 is the same as --seconds-per-lap 20.",
    )
    parser.add_argument("--creds", help="Path to your <prefix>.env credential card (default: read from credentials.env)")
    parser.add_argument(
        "--seconds-per-lap",
        type=int,
        default=None,
        metavar="N",
        help=f"Wall-clock seconds per simulated lap (minimum {meta.MIN_SECONDS_PER_LAP}). Defaults to 20.",
    )
    parser.add_argument("--once", action="store_true", help="Run a single race instead of looping continuously")
    args = parser.parse_args(_expand_numeric_flags(sys.argv[1:]))

    root = _GEMINI_WORKSHOP_ROOT
    path, card = load_card(args.creds, root=root)
    print(f"Using credential card: {path}")
    seconds_per_lap = _resolve_seconds_per_lap(args.seconds_per_lap, root)

    def need(key: str) -> str:
        v = card.get(key)
        if not v:
            sys.exit(f"Credential card is missing {key}. Regenerate it with `uv run python scripts/cli.py up`.")
        return v

    # Map card F1_* vars to the simulator's expected env (datagen/config.py).
    os.environ["KAFKA_BOOTSTRAP"] = _strip_scheme(need("F1_KAFKA_BOOTSTRAP"))
    os.environ["KAFKA_API_KEY"] = need("F1_KAFKA_API_KEY")
    os.environ["KAFKA_API_SECRET"] = need("F1_KAFKA_API_SECRET")
    os.environ["SR_URL"] = need("F1_SCHEMA_REGISTRY_URL")
    os.environ["SR_API_KEY"] = need("F1_SR_API_KEY")
    os.environ["SR_API_SECRET"] = need("F1_SR_API_SECRET")
    os.environ["SECONDS_PER_LAP"] = str(seconds_per_lap)
    os.environ["RACE_LOOP"] = "false" if args.once else "true"

    # Skip the pre-race warmup laps — see the main repo's identical note in
    # scripts/selfservice/race.py for why (no race_standings on lap 0, so LAB
    # 3's inner temporal join drops those rows anyway).
    os.environ.setdefault("PRE_RACE_WARMUP_LAPS", "0")

    # Import after setting env — datagen.config reads os.environ at import time.
    from datagen import simulator

    simulator.main()


if __name__ == "__main__":
    main()
