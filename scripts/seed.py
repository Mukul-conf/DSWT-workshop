"""Seed ``driver_race_history`` with a bounded Flink INSERT (no Postgres/CDC).

Standalone adaptation of the main repo's scripts/selfservice/seed.py: same
idempotent count-before-insert logic, retargeted at gemini-workshop's own
``datagen/`` copy and local ``deployment.py``/``sql_shell.py`` modules instead
of the main repo's ``scripts.common``/``scripts.workshop`` packages.
"""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import requests

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from deployment import seed_marker_path  # noqa: E402
from sql_shell import FlinkSession  # noqa: E402
from terraform_utils import project_root  # noqa: E402

TABLE = "driver_race_history"

# A statement is only finished at one of these. `RUNNING` is deliberately absent.
TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}

# A streaming global aggregate emits a changelog, not one final row: 198 inserts
# produce up to ~395 update_before/update_after rows. Read generously and keep
# the largest value seen rather than assuming the last page is the final one.
COUNT_MAX_ROWS = 2000


def _load_rows() -> list[dict]:
    """Import ``build_all_rows`` from datagen/data/ (not a package) by file path."""
    path = project_root() / "datagen" / "data" / "generate_driver_race_history.py"
    spec = importlib.util.spec_from_file_location("generate_driver_race_history", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.build_all_rows()


def expected_rows() -> int:
    return len(_load_rows())


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _row_literal(r: dict) -> str:
    return (
        f"({_sql_str(r['race_id'])}, {_sql_str(r['gp_name'])}, DATE {_sql_str(r['race_date'])}, "
        f"{r['car_number']}, {_sql_str(r['driver'])}, {_sql_str(r['team'])}, "
        f"{r['starting_grid']}, {r['finishing_pos']}, {r['positions_gained']}, {r['pit_stops']}, "
        f"{_sql_str(r['stint_1_tire'])}, {_sql_str(r['stint_2_tire'])}, {_sql_str(r['stint_3_tire'])})"
    )


def build_insert() -> str:
    values = ",\n".join(_row_literal(r) for r in _load_rows())
    return f"INSERT INTO `{TABLE}` VALUES\n" + values


def _wait_terminal(session: FlinkSession, name: str, timeout: int) -> tuple[str, str]:
    url = f"{session.base}/{name}"
    deadline = time.time() + timeout
    phase = "UNKNOWN"
    detail = ""
    while time.time() < deadline:
        try:
            status = requests.get(url, auth=session.auth, timeout=30).json()
        except Exception as e:
            return "UNKNOWN", f"could not read statement status: {e}"
        state = status.get("status") or {}
        phase = state.get("phase", "UNKNOWN")
        detail = (state.get("detail") or "").strip()
        if phase in TERMINAL_PHASES:
            return phase, detail
        time.sleep(2)
    return "TIMEOUT", detail


def _as_int(row) -> int | None:
    value = row[0] if isinstance(row, (list, tuple)) and row else row
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def count_rows(session: FlinkSession, expected: int, timeout: int = 45) -> tuple[int | None, str]:
    """``(count, problem)`` for ``driver_race_history``. count is None if unknown."""
    try:
        name = session.submit(f"SELECT COUNT(*) AS row_count FROM `{TABLE}`")
    except Exception as e:
        return None, f"could not submit the row count query ({e})"

    try:
        status = session.wait(name, timeout=60)
        phase = (status.get("status") or {}).get("phase", "UNKNOWN")
        if phase not in ("RUNNING", "COMPLETED"):
            detail = ((status.get("status") or {}).get("detail") or "").strip()
            return None, f"row count query reached {phase} ({detail or 'no detail returned'})"

        started = time.time()
        best: int | None = None
        for row in session.results(name, max_rows=COUNT_MAX_ROWS, timeout=timeout):
            value = _as_int(row)
            if value is None:
                continue
            best = value if best is None else max(best, value)
            if best >= expected:
                break
        elapsed = time.time() - started
    except Exception as e:
        return None, f"row count query failed ({e})"
    finally:
        session.stop(name)

    if best is not None:
        return best, ""

    if elapsed < timeout / 2:
        return None, f"row count query returned nothing after only {elapsed:.0f}s of a {timeout}s window"
    return 0, ""


def run_insert(session: FlinkSession, timeout: int = 240) -> tuple[bool, str]:
    try:
        name = session.submit(build_insert())
    except Exception as e:
        return False, f"could not submit the INSERT ({e})"

    phase, detail = _wait_terminal(session, name, timeout=timeout)
    if phase == "COMPLETED":
        return True, ""
    if phase == "TIMEOUT":
        return False, f"INSERT did not finish within {timeout}s (last detail: {detail or 'none'})"
    return False, f"INSERT reached {phase}: {detail[:500] or 'no detail returned'}"


def _write_marker(root: Path, environment_id: str) -> None:
    marker = seed_marker_path(root)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"{environment_id}\n")


def ensure_driver_race_history(
    card: dict[str, str],
    root: Path | None = None,
    insert_timeout: int = 240,
    count_timeout: int = 45,
) -> bool:
    """Make sure ``driver_race_history`` holds exactly the expected rows.

    Idempotent and safe to re-run: counts first, inserts only into an empty
    table, re-counts afterwards, and writes the marker only once the count
    matches.
    """
    root = root or project_root()
    expected = expected_rows()
    env_id = (card.get("F1_ENVIRONMENT_ID") or "").strip()
    marker = seed_marker_path(root)

    if marker.exists():
        recorded = marker.read_text().strip()
        if env_id and recorded == env_id:
            print(f"  {TABLE} already seeded and verified for {env_id}.")
            return True
        print(f"  Seed marker is for {recorded or '(unknown)'}, not {env_id or '(unknown)'} — re-verifying.")

    session = FlinkSession(card)

    count, problem = count_rows(session, expected, timeout=count_timeout)
    if count is None:
        print(f"  Could not verify {TABLE}: {problem}")
        print("  Re-run `uv run python scripts/cli.py up` — it counts before inserting, so it cannot double-seed.")
        return False

    if count == expected:
        print(f"  {TABLE} already holds {count} rows — nothing to seed.")
        _write_marker(root, env_id)
        return True

    if count != 0:
        print(f"  {TABLE} holds {count} rows, expected {expected}.")
        print("  Refusing to insert again — that would add a second copy of every row.")
        print("  Tear down and re-provision (`uv run python scripts/cli.py down` then `up`).")
        return False

    print(f"  Seeding {TABLE} ({expected} rows) via a bounded Flink INSERT...")
    ok, problem = run_insert(session, timeout=insert_timeout)
    if not ok:
        print(f"  Seed failed: {problem}")
        return False

    count, problem = count_rows(session, expected, timeout=count_timeout)
    if count is None:
        print(f"  INSERT completed but the row count could not be confirmed: {problem}")
        return False
    if count != expected:
        print(f"  INSERT completed but {TABLE} holds {count} rows, expected {expected}.")
        return False

    print(f"  Seeded and verified {count} rows.")
    _write_marker(root, env_id)
    return True
