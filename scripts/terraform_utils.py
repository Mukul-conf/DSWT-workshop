"""Terraform utilities: project root, state-file reading, and cleanup.

Standalone adaptation of the main repo's scripts/common/terraform.py. The only
change is `project_root()`: the original locates the repo root by searching
upward for pyproject.toml, because the whole repo (with its own pyproject.toml)
is the unit of work. Here the unit of work is this directory —
gemini-workshop/ — so `project_root()` just returns it directly from this
file's own location, with no upward search needed.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def project_root() -> Path:
    """The gemini-workshop/ directory — the root every path here is relative to."""
    return Path(__file__).resolve().parents[1]


def cleanup_terraform_artifacts(env_path: Path) -> None:
    """Remove terraform artifacts from a directory after a *successful* destroy.

    Removes *.tfstate*, *.tfvars*, .terraform/, .terraform.lock.hcl.

    Only ever call this when the destroy succeeded — deleting state after a
    failed destroy orphans whatever Terraform did not manage to delete, leaving
    live cloud resources that nothing points at.
    """
    try:
        for tfstate_file in env_path.glob("*.tfstate*"):
            tfstate_file.unlink()

        for tfvars_file in env_path.glob("*.tfvars*"):
            tfvars_file.unlink()

        terraform_dir = env_path / ".terraform"
        if terraform_dir.exists():
            shutil.rmtree(terraform_dir)

        lock_file = env_path / ".terraform.lock.hcl"
        if lock_file.exists():
            lock_file.unlink()

    except Exception:
        pass


def run_terraform_output(state_path: Path) -> dict:
    """Run `terraform output -json` against `state_path` and flatten the result."""
    try:
        cmd = ["terraform", "output", "-json", f"-state={state_path}"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        outputs = json.loads(result.stdout)
        return {key: value["value"] for key, value in outputs.items()}
    except FileNotFoundError:
        logger.error("Terraform binary not found. Please install terraform.")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"Terraform output failed: {e.stderr}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse terraform output JSON: {e}")
        raise
