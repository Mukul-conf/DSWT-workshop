"""``gemini-workshop`` provisioning CLI — one attendee's Confluent environment,
backed by Gemini.

  uv run python scripts/cli.py up      # provision + seed history
  uv run python scripts/cli.py down    # tear it down

Fully standalone: every module this imports lives in dswt-workshop/scripts/,
and the Terraform it applies (dswt-workshop/terraform/) has its own local
copy of the modules it needs. Nothing here reaches into the main repo's
scripts/ or terraform/ trees.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from dotenv import dotenv_values, set_key  # noqa: E402

import card as card_mod  # noqa: E402
import deployment as meta  # noqa: E402
from credentials import (  # noqa: E402
    clear_active_card,
    load_or_create_credentials_file,
    set_active_card,
)
from login_checks import check_terraform_installed  # noqa: E402
from seed import ensure_driver_race_history  # noqa: E402
from terraform_runner import run_terraform, run_terraform_destroy  # noqa: E402
from terraform_utils import cleanup_terraform_artifacts, project_root, run_terraform_output  # noqa: E402
from ui import prompt_with_default  # noqa: E402

REGION = "us-east-1"

TF_DIR = _THIS_DIR.parent / "terraform"

REQUIRED = {
    "TF_VAR_confluent_cloud_api_key": "Confluent Cloud API Key",
    "TF_VAR_confluent_cloud_api_secret": "Confluent Cloud API Secret",
    "TF_VAR_owner_email": "Owner email (tags the Confluent environment)",
    "TF_VAR_gemini_api_key": "Gemini API key",
}


def _tf_env(cfg: dict[str, str]) -> dict[str, str]:
    return {
        "TF_VAR_prefix": cfg["prefix"],
        "TF_VAR_owner_email": cfg["owner_email"],
        "TF_VAR_region": REGION,
        "TF_VAR_confluent_cloud_api_key": cfg["api_key"],
        "TF_VAR_confluent_cloud_api_secret": cfg["api_secret"],
        "TF_VAR_gemini_api_key": cfg["gemini_api_key"],
    }


def _resolve_prefix(root: Path, explicit: str | None) -> tuple[str, str]:
    if explicit:
        return explicit, "explicit (TF_VAR_prefix)"
    saved = meta.load_meta(root).get(meta.KEY_RESOLVED_PREFIX)
    if saved:
        return saved, "saved (runs/deployment.env)"
    return meta.DEFAULT_PREFIX, "default"


def _explicit_prefix() -> str | None:
    return (os.environ.get("TF_VAR_prefix") or "").strip() or None


def _validated_prefix_or_exit(prefix: str) -> str:
    problem = meta.validate_prefix(prefix)
    if problem:
        sys.exit(f"\nError: {problem}")
    return prefix


def _seconds_per_lap(root: Path, creds: dict) -> str:
    raw = (
        os.environ.get("TF_VAR_seconds_per_lap")
        or meta.load_meta(root).get(meta.KEY_SECONDS_PER_LAP)
        or creds.get("TF_VAR_seconds_per_lap")
        or meta.DEFAULT_SECONDS_PER_LAP
    )
    value, problem = meta.validate_seconds_per_lap(raw)
    if problem:
        sys.exit(f"\nError: {problem}\nFix TF_VAR_seconds_per_lap (exported, credentials.env, or runs/deployment.env).")
    return str(value)


def _collect_config(root: Path, creds_file: Path, creds: dict, automated: bool) -> dict[str, str]:
    if automated:
        cfg = {
            "api_key": creds.get("TF_VAR_confluent_cloud_api_key", ""),
            "api_secret": creds.get("TF_VAR_confluent_cloud_api_secret", ""),
            "owner_email": creds.get("TF_VAR_owner_email", ""),
            "gemini_api_key": creds.get("TF_VAR_gemini_api_key", ""),
        }
        missing = [label for key, label in REQUIRED.items() if not creds.get(key)]
        if missing:
            sys.exit(f"Error: credentials.env is missing required values: {', '.join(missing)}")
        prefix, source = _resolve_prefix(root, _explicit_prefix())
        cfg["prefix"] = _validated_prefix_or_exit(prefix)
        print(f"  Prefix: {cfg['prefix']}  ({source})")
        cfg["seconds_per_lap"] = _seconds_per_lap(root, creds)
        _persist(root, creds_file, cfg, interactive=False)
        return cfg

    print("\n--- Configuration ---\n")
    cfg = {
        "api_key": prompt_with_default("Confluent Cloud API Key", creds.get("TF_VAR_confluent_cloud_api_key", "")),
        "api_secret": prompt_with_default("Confluent Cloud API Secret", creds.get("TF_VAR_confluent_cloud_api_secret", "")),
        "owner_email": prompt_with_default("Owner email", creds.get("TF_VAR_owner_email", "")),
        "gemini_api_key": prompt_with_default(
            "Gemini API key (from your instructor, or https://aistudio.google.com/apikey)",
            creds.get("TF_VAR_gemini_api_key", ""),
        ),
    }

    resolved, source = _resolve_prefix(root, _explicit_prefix())
    cfg["prefix"] = _validated_prefix_or_exit(resolved)
    print(f"  Prefix: {cfg['prefix']}  ({source})")
    cfg["seconds_per_lap"] = _seconds_per_lap(root, creds)

    _persist(root, creds_file, cfg, interactive=True)
    return cfg


def _persist(root: Path, creds_file: Path, cfg: dict[str, str], interactive: bool) -> None:
    meta.save_meta(
        root,
        **{
            meta.KEY_RESOLVED_PREFIX: cfg["prefix"],
            meta.KEY_SECONDS_PER_LAP: cfg["seconds_per_lap"],
        },
    )
    if not interactive:
        return
    for key, value in {
        "TF_VAR_confluent_cloud_api_key": cfg["api_key"],
        "TF_VAR_confluent_cloud_api_secret": cfg["api_secret"],
        "TF_VAR_owner_email": cfg["owner_email"],
        "TF_VAR_gemini_api_key": cfg["gemini_api_key"],
    }.items():
        set_key(str(creds_file), key, value)


def up(args: argparse.Namespace) -> None:
    print("=== F1 Pit Wall Workshop — Gemini Self-Service (Confluent + Gemini API) ===\n")
    root = project_root()

    if not check_terraform_installed():
        sys.exit("Error: Terraform not found. Install from https://developer.hashicorp.com/terraform/install")

    creds_file, creds = load_or_create_credentials_file(root)
    cfg = _collect_config(root, creds_file, creds, args.automated)

    print("\n--- Summary ---")
    print(f"  Region:  {REGION}")
    print(f"  Prefix:  {cfg['prefix']}   (Confluent env: RIVER-RACING-{cfg['prefix']}-ENV)")
    print(f"  Owner:   {cfg['owner_email']}")
    print(f"  Pacing:  {cfg['seconds_per_lap']}s/lap for scripts/race.py")
    print("  Creates: Confluent env + cluster + Flink pool + topics + a Gemini-backed LLM model")
    if not args.automated and input("\nReady to provision? (y/n): ").strip().lower() != "y":
        print("Cancelled.")
        sys.exit(0)

    for k, v in _tf_env(cfg).items():
        os.environ[k] = v

    print("\n=== Provisioning Confluent environment (dswt-workshop/terraform) ===")
    if not run_terraform(TF_DIR):
        sys.exit("\nProvisioning failed. `uv run python scripts/cli.py down` to clean up, then retry.")

    out = run_terraform_output(TF_DIR / "terraform.tfstate")

    creds_dir = root / "runs" / "credentials"
    creds_dir.mkdir(parents=True, exist_ok=True)
    fields = card_mod.card_fields(cfg["prefix"], cfg["owner_email"], out, region=REGION)
    card_mod.write_env(creds_dir, fields)
    card_mod.write_md(creds_dir, fields)
    card_path = creds_dir / f"{cfg['prefix']}.env"
    set_active_card(root, card_path)
    meta.save_meta(root, **{meta.KEY_CARD: str(card_path.relative_to(root))})
    print(f"\nCredential card: {card_path}  (recorded as F1_CARD in credentials.env)")

    print("\n=== driver_race_history ===")
    seeded = ensure_driver_race_history(dotenv_values(card_path), root)

    print("\n=== Ready ===\n")
    print("1. Start the live race feed (leave running in its own terminal):")
    print(f"     uv run python scripts/race.py          # {cfg['seconds_per_lap']}s/lap, from this deployment's config")
    print("2. Open the live dashboard:")
    print("     uv run python scripts/pitwall_app.py")
    if fields.get("rtce_api_key"):
        print("4. (optional) Real-Time Context Engine is enabled — see the credential card's .md file.")
    print("\nFollow dswt-workshop/docs/README.md from Lab 2 onward.")
    print("Tear down when finished:  uv run python scripts/cli.py down")

    if not seeded:
        sys.exit(1)


def down(args: argparse.Namespace) -> None:
    print("=== F1 Pit Wall Workshop — Gemini Self-Service teardown ===\n")
    root = project_root()
    _creds_file, creds = load_or_create_credentials_file(root)

    if not (TF_DIR / "terraform.tfstate").exists():
        print("No dswt-workshop Terraform state found — nothing to destroy.")
        return

    prefix, _source = _resolve_prefix(root, _explicit_prefix())
    cfg = {
        "prefix": prefix,
        "owner_email": creds.get("TF_VAR_owner_email", ""),
        "api_key": creds.get("TF_VAR_confluent_cloud_api_key", ""),
        "api_secret": creds.get("TF_VAR_confluent_cloud_api_secret", ""),
        "gemini_api_key": creds.get("TF_VAR_gemini_api_key", ""),
    }
    for k, v in _tf_env(cfg).items():
        os.environ[k] = v

    if not args.yes and input("Destroy the workshop Confluent environment? (y/n): ").strip().lower() != "y":
        print("Cancelled.")
        return

    if run_terraform_destroy(TF_DIR):
        clear_active_card(root)
        meta.seed_marker_path(root).unlink(missing_ok=True)
        meta.clear_meta(root)
        cleanup_terraform_artifacts(TF_DIR)
        print("\nDestroy complete.")
    else:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dswt-workshop",
        description="Provision or tear down one attendee's Confluent + Gemini F1 Pit Wall environment",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_up = sub.add_parser("up", help="Provision your Confluent environment + Gemini model")
    p_up.add_argument("--automated", action="store_true", help="Don't prompt; read credentials.env only")
    p_up.set_defaults(func=up)

    p_down = sub.add_parser("down", help="Tear down your Confluent environment")
    p_down.add_argument("--yes", action="store_true", help="Skip the confirmation prompt")
    p_down.set_defaults(func=down)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
