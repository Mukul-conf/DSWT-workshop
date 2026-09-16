"""Credential loading and management — standalone, single-deployment version.

Two distinct files, both local to gemini-workshop/ (never at the main repo's
root):

- ``credentials.env`` — deploy secrets (``TF_VAR_*``), plus an ``F1_CARD``
  pointer to the credential card below.
- a credential **card**, ``runs/credentials/<prefix>.env`` (``F1_*`` keys) —
  what race.py / sql_shell.py / pitwall_app.py / reset.py authenticate with.

Simplified from the main repo's scripts/common/credentials.py: that module
resolves a card among potentially several deployment tracks in one checkout
(``runs/*/credentials/*.env``). gemini-workshop is always exactly one
deployment, so resolution only ever considers ``runs/credentials/*.env``.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import dotenv_values

CARD_POINTER_KEY = "F1_CARD"
CARD_ENV_VAR = "F1_CREDS"  # one-off override, e.g. F1_CREDS=... python race.py


def load_or_create_credentials_file(root: Path) -> tuple[Path, dict[str, str]]:
    """Load existing credentials.env or create an empty one."""
    creds_file = root / "credentials.env"

    if creds_file.exists():
        return creds_file, dotenv_values(creds_file)

    creds_file.write_text("TF_VAR_confluent_username=''\nTF_VAR_confluent_password=''\n")
    return creds_file, {}


def _is_card(values: dict[str, str | None]) -> bool:
    return any(key.startswith("F1_") and key != CARD_POINTER_KEY for key in values)


def resolve_card(explicit: str | None = None, root: Path | None = None) -> Path:
    """Work out which credential card to use.

    Order, first hit wins:
      1. an explicit --creds value
      2. $F1_CREDS
      3. credentials.env's F1_CARD pointer, or the file itself when it holds
         F1_* keys directly
      4. the only card under runs/credentials/

    Exits with an actionable message when nothing is found or the choice is
    ambiguous, rather than raising.
    """
    from terraform_utils import project_root

    root = root or project_root()

    if explicit:
        return Path(explicit)

    from_env = os.environ.get(CARD_ENV_VAR)
    if from_env:
        return Path(from_env)

    creds_file = root / "credentials.env"
    if creds_file.exists():
        values = dotenv_values(creds_file)
        pointer = values.get(CARD_POINTER_KEY)
        if pointer:
            card = Path(pointer)
            if not card.is_absolute():
                card = root / card
            if card.exists():
                return card
        if _is_card(values):
            return creds_file

    candidates = sorted((root / "runs" / "credentials").glob("*.env")) if (root / "runs" / "credentials").is_dir() else []
    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        sys.exit(
            "No credential card found.\n"
            "  Run `uv run python scripts/cli.py up` from gemini-workshop/,\n"
            "  or pass `--creds <path>` explicitly."
        )

    listed = "\n".join(f"    {c.relative_to(root)}" for c in candidates)
    sys.exit(
        "Multiple credential cards found under runs/credentials/ — I won't guess which one you meant:\n"
        f"{listed}\n"
        "  Pass `--creds <path>`, or set F1_CARD in credentials.env."
    )


def load_card(explicit: str | None = None, root: Path | None = None) -> tuple[Path, dict[str, str]]:
    """Resolve a credential card and parse it. Exits if the path is bad."""
    path = resolve_card(explicit, root=root)
    if not path.exists():
        sys.exit(f"Credential file not found: {path}")
    return path, dict(dotenv_values(path))


def set_active_card(root: Path, card: Path) -> None:
    """Record `card` as the active one in credentials.env."""
    creds_file = root / "credentials.env"
    try:
        rel = card.resolve().relative_to(root.resolve())
        value = str(rel)
    except ValueError:
        value = str(card)

    line = f"{CARD_POINTER_KEY}={value}\n"
    existing = creds_file.read_text().splitlines(keepends=True) if creds_file.exists() else []

    for i, current in enumerate(existing):
        if current.lstrip().startswith(f"{CARD_POINTER_KEY}="):
            existing[i] = line
            break
    else:
        if existing and not existing[-1].endswith("\n"):
            existing[-1] += "\n"
        existing.append(line)

    creds_file.write_text("".join(existing))


def clear_active_card(root: Path) -> None:
    """Drop the F1_CARD pointer and delete the card files it and any others point at."""
    creds_dir = root / "runs" / "credentials"
    if creds_dir.is_dir():
        for card in sorted(list(creds_dir.glob("*.env")) + list(creds_dir.glob("*.md"))):
            card.unlink(missing_ok=True)

    creds_file = root / "credentials.env"
    if not creds_file.exists():
        return
    kept = [
        line
        for line in creds_file.read_text().splitlines(keepends=True)
        if not line.lstrip().startswith(f"{CARD_POINTER_KEY}=")
    ]
    creds_file.write_text("".join(kept))


def generate_confluent_api_keys(prefix: str = "gemini-f1") -> tuple[str | None, str | None]:
    """Generate Confluent API keys via the CLI (service account + OrganizationAdmin key).

    Returns (api_key, api_secret) or (None, None) if generation fails.
    """
    try:
        timestamp = str(int(time.time()))[-6:]
        sa_name = f"{prefix}-setup-sa-{timestamp}"

        print(f"Creating service account: {sa_name}...")
        sa_result = subprocess.run(
            ["confluent", "iam", "service-account", "create", sa_name, "--description", f"Service account for {prefix} setup"],
            capture_output=True,
            text=True,
            check=True,
        )

        sa_id = None
        for line in sa_result.stdout.split("\n"):
            if "| ID" in line and "sa-" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2 and "ID" in parts[0]:
                    sa_id = parts[1]
                    break

        if not sa_id:
            print("Error: Failed to extract service account ID.")
            return None, None

        print("Creating API key with Cloud Resource Management scope...")
        key_result = subprocess.run(
            ["confluent", "api-key", "create", "--service-account", sa_id, "--resource", "cloud", "--description", f"{prefix} setup key"],
            capture_output=True,
            text=True,
            check=True,
        )

        api_key = api_secret = None
        for line in key_result.stdout.split("\n"):
            if "API Key" in line and "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2 and "API Key" in parts[0]:
                    api_key = parts[1]
            elif "API Secret" in line and "|" in line:
                parts = [p.strip() for p in line.split("|") if p.strip()]
                if len(parts) >= 2 and "API Secret" in parts[0]:
                    api_secret = parts[1]

        if api_key and api_secret:
            print("Assigning OrganizationAdmin role...")
            try:
                subprocess.run(
                    ["confluent", "iam", "rbac", "role-binding", "create", "--principal", f"User:{sa_id}", "--role", "OrganizationAdmin"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                print("API keys generated successfully!")
                return api_key, api_secret
            except subprocess.CalledProcessError:
                print("Warning: Role assignment failed, but API keys were created.")
                return api_key, api_secret

    except subprocess.CalledProcessError as e:
        print(f"Error generating API keys: {e}")

    return None, None
