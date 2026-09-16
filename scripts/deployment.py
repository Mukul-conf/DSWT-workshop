"""Deployment bookkeeping for this one workshop environment.

Standalone, single-track simplification of the main repo's
scripts/common/deployment_meta.py. That file's complexity (a `Track` dataclass,
per-user prefix derivation, cross-track collision avoidance) exists because one
checkout of the main repo can have *multiple* deployment tracks (standalone,
self-service) live at once and must keep their prefixes, cards, and metadata
from colliding. gemini-workshop is always exactly one attendee's one
environment, so none of that applies: a fixed default prefix
(overridable via `TF_VAR_prefix`) and a flat metadata file are enough.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values

# Confluent display names have to stay short; 12 alphanumeric characters
# mirrors the main repo's convention.
MAX_PREFIX_LEN = 12
DEFAULT_PREFIX = "GEMINI"

# Below ~10s/lap the anomaly function cannot accumulate its 12 windows before
# the lap-24 anomaly, so the anomaly never fires. See docs/ATTENDEE-GUIDE.md.
MIN_SECONDS_PER_LAP = 10
DEFAULT_SECONDS_PER_LAP = "20"

KEY_RESOLVED_PREFIX = "F1_RESOLVED_PREFIX"
KEY_CARD = "F1_CARD_PATH"
KEY_SECONDS_PER_LAP = "F1_SECONDS_PER_LAP"


def validate_prefix(prefix: str) -> str | None:
    """None when usable, else the reason it isn't."""
    if not prefix:
        return "Prefix is empty."
    if not prefix.isalnum():
        return f"Prefix {prefix!r} must be alphanumeric (letters and digits only)."
    if not prefix.isascii():
        return f"Prefix {prefix!r} must be ASCII."
    if len(prefix) > MAX_PREFIX_LEN:
        return f"Prefix {prefix!r} is {len(prefix)} chars; maximum is {MAX_PREFIX_LEN}."
    return None


def validate_seconds_per_lap(raw: str | int | None) -> tuple[int | None, str | None]:
    """(value, error)."""
    if raw is None or str(raw).strip() == "":
        return None, "Seconds per lap is not set."
    text = str(raw).strip()
    if not text.isdigit():
        return None, f"Seconds per lap {text!r} is not a whole number."
    value = int(text)
    if value < MIN_SECONDS_PER_LAP:
        return None, (
            f"Seconds per lap {value} is below the {MIN_SECONDS_PER_LAP}s minimum — "
            "anomaly detection can't accumulate its 12 windows before the lap-24 anomaly."
        )
    return value, None


def meta_path(root: Path) -> Path:
    return root / "runs" / "deployment.env"


def load_meta(root: Path) -> dict[str, str]:
    path = meta_path(root)
    if not path.exists():
        return {}
    return {k: v for k, v in dotenv_values(path).items() if v is not None}


def save_meta(root: Path, **fields: str) -> Path:
    """Merge `fields` into the local deployment metadata. Values are plain, not secrets."""
    path = meta_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    merged = load_meta(root)
    merged.update({k: str(v) for k, v in fields.items() if v is not None and str(v) != ""})

    lines = [
        "# Gemini workshop — resolved deployment inputs. Not secrets — see credentials.env.",
        "# Written by scripts/cli.py; read by re-runs, race.py, and reset.py.",
        *(f"{k}={v}" for k, v in sorted(merged.items())),
        "",
    ]
    path.write_text("\n".join(lines))
    return path


def clear_meta(root: Path) -> None:
    meta_path(root).unlink(missing_ok=True)


# Written once driver_race_history is populated, so a re-run doesn't re-seed.
SEED_MARKER = ".seeded"


def seed_marker_path(root: Path) -> Path:
    return root / "runs" / SEED_MARKER


def tf_state_path(root: Path) -> Path:
    return root / "terraform" / "terraform.tfstate"


def has_state(root: Path) -> bool:
    return tf_state_path(root).exists()
