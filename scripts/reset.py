"""Reset this workshop's lab state for a fresh run of Lab 3 / Lab 4.

  uv run python scripts/reset.py             # blank slate
  uv run python scripts/reset.py --keep-source  # keep accumulated race data

Drops `pit_decisions`, `car_state`, and `pit_strategy_agent` (and their backing
topics + Schema Registry subjects), then clears the `car_telemetry` /
`race_standings` source topics so the next race starts clean. Works directly
against this one Terraform-tracked environment — it reads
gemini-workshop/terraform/terraform.tfstate for connection details, not the
credential card, so it still works even if the card was regenerated or deleted.

Standalone, single-environment simplification of the main repo's
scripts/reset.py (843 lines, built for multiple simultaneous deployment
tracks). This version drops the multi-track selection, the `--with-labs`
rebuild, and the local-race-process detection — gemini-workshop is always
exactly one environment, and `race.py` is a plain foreground process you stop
with Ctrl-C yourself before resetting.

**Stop `race.py` first.** Clearing the source topics while it's still running
puts the records straight back.

This is a Confluent-only operation — it does not run Terraform, and it needs
the Confluent CLI logged in (`confluent login`) to delete topics and Schema
Registry subjects.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from base64 import b64encode
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from deployment import seed_marker_path, tf_state_path  # noqa: E402
from terraform_utils import project_root, run_terraform_output  # noqa: E402

DROP_TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}
DROP_SUCCESS_PHASE = "COMPLETED"

# Drop order matters: pit_decisions reads from car_state via pit_strategy_agent,
# so it must go first, or the DROP TABLE/AGENT below it fails with "in use".
DROPS: list[tuple[str, str]] = [
    ("pit_decisions", "DROP TABLE IF EXISTS `pit_decisions`"),
    ("car_state", "DROP TABLE IF EXISTS `car_state`"),
    ("pit_strategy_agent", "DROP AGENT IF EXISTS `pit_strategy_agent`"),
]
LAB_TOPICS = ["car_state", "pit_decisions"]
SOURCE_TOPICS = ["car_telemetry", "race_standings"]


def run_cli(cmd: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(cmd, capture_output=True, text=True, input="y\n")
    return result.returncode, result.stdout, result.stderr


def first_error_line(stderr: str) -> str:
    return stderr.strip().splitlines()[0] if stderr.strip() else ""


# Verbatim from the main repo's scripts/reset.py: substrings that mean "the
# target was already gone", not a real failure.
BENIGN_MISSING = (
    "not found",
    "does not exist",
    "unknown topic",
    "no such",
    "40401",
    "40403",
)


def is_benign_missing(stderr: str) -> bool:
    lowered = stderr.lower()
    return any(marker in lowered for marker in BENIGN_MISSING)


# --- Flink -------------------------------------------------------------------


def flink_api(tf: dict) -> tuple[str, dict]:
    rest = tf["flink_rest_endpoint"].rstrip("/")
    token = b64encode(f"{tf['flink_api_key']}:{tf['flink_api_secret']}".encode()).decode()
    url = f"{rest}/sql/v1/organizations/{tf['organization_id']}/environments/{tf['environment_id']}/statements"
    return url, {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def _get_json(url: str, headers: dict) -> dict:
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers)) as resp:
        return json.loads(resp.read())


def _wait_for_phase(statement_url: str, headers: dict, timeout: int) -> tuple[str, str]:
    phase, detail = "UNKNOWN", ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = _get_json(statement_url, headers).get("status", {})
        except Exception as e:
            return "UNREACHABLE", str(e)
        phase = status.get("phase", "UNKNOWN")
        detail = (status.get("detail") or "").strip()
        if phase in DROP_TERMINAL_PHASES:
            return phase, detail
        time.sleep(2)
    return f"TIMED OUT after {timeout}s in {phase}", detail


def drop_flink_objects(tf: dict, timeout: int = 180) -> list[str]:
    """Submit each DROP in order and wait for it to reach COMPLETED. Returns problems."""
    url, headers = flink_api(tf)
    problems: list[str] = []

    for label, sql in DROPS:
        name = f"reset-{label}-{int(time.time() * 1000) % 10**9}"
        body = json.dumps(
            {
                "name": name,
                "spec": {
                    "statement": sql,
                    "compute_pool_id": tf["compute_pool_id"],
                    "properties": {
                        "sql.current-catalog": tf["environment_name"],
                        "sql.current-database": tf["cluster_name"],
                    },
                },
            }
        ).encode()

        try:
            urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers, method="POST"))
        except Exception as e:
            detail = e.read().decode()[:300] if isinstance(e, urllib.error.HTTPError) else str(e)
            print(f"  {sql}: submit failed — {detail}")
            problems.append(f"{sql} was not submitted ({detail})")
            continue

        phase, detail = _wait_for_phase(f"{url}/{name}", headers, timeout)
        if phase == DROP_SUCCESS_PHASE:
            print(f"  {sql}: {phase}")
            try:
                urllib.request.urlopen(urllib.request.Request(f"{url}/{name}", headers=headers, method="DELETE"))
            except Exception:
                pass
            continue

        print(f"  {sql}: {phase}{f' — {detail}' if detail else ''}")
        problems.append(f"{sql} did not complete (last phase {phase}{f': {detail}' if detail else ''})")

    return problems


# --- Kafka / Schema Registry -------------------------------------------------


def kafka_admin(tf: dict):
    from confluent_kafka.admin import AdminClient

    ac = tf.get("attendee_credentials", {})
    return AdminClient(
        {
            "bootstrap.servers": tf["cluster_bootstrap"].split("://", 1)[-1],
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": ac["kafka_api_key"],
            "sasl.password": ac["kafka_api_secret"],
        }
    )


def existing_topics(admin) -> set[str] | None:
    try:
        return set(admin.list_topics(timeout=30).topics)
    except Exception as e:
        print(f"  Could not list topics: {e}")
        return None


def delete_topic_and_subjects(topic: str, env_id: str, cluster_id: str, exists: bool | None) -> list[str]:
    problems: list[str] = []

    if exists is False:
        print(f"  Topic {topic}: already gone")
    else:
        rc, _, stderr = run_cli(
            ["confluent", "kafka", "topic", "delete", topic, "--environment", env_id, "--cluster", cluster_id]
        )
        if rc == 0:
            print(f"  Topic {topic}: deleted")
        elif is_benign_missing(stderr):
            print(f"  Topic {topic}: already gone")
        else:
            print(f"  Topic {topic}: FAILED — {first_error_line(stderr)}")
            problems.append(f"could not delete topic {topic} ({first_error_line(stderr)})")

    for subject in [f"{topic}-key", f"{topic}-value"]:
        base_cmd = ["confluent", "schema-registry", "schema", "delete", "--subject", subject, "--version", "all", "--environment", env_id]
        soft_rc, _, soft_err = run_cli(base_cmd)
        hard_rc, _, hard_err = run_cli([*base_cmd, "--permanent"])

        if hard_rc == 0:
            print(f"  SR {subject}: cleaned")
        elif is_benign_missing(hard_err) and (soft_rc == 0 or is_benign_missing(soft_err)):
            print(f"  SR {subject}: already gone")
        else:
            failure = first_error_line(hard_err) or first_error_line(soft_err)
            print(f"  SR {subject}: FAILED — {failure}")
            problems.append(f"could not delete Schema Registry subject {subject} ({failure})")

    return problems


def truncate_topics(admin, topics: list[str], present: set[str] | None) -> list[str]:
    """Delete every record in `topics`, leaving the topics and schemas in place."""
    from confluent_kafka import OFFSET_END, TopicPartition

    problems: list[str] = []

    for topic in topics:
        if present is not None and topic not in present:
            print(f"  Topic {topic}: MISSING — nothing to clear")
            problems.append(f"source topic {topic} does not exist (re-apply Terraform to recreate it)")
            continue

        try:
            meta = admin.list_topics(topic=topic, timeout=30)
        except Exception as e:
            print(f"  Topic {topic}: metadata lookup failed: {e}")
            problems.append(f"could not read metadata for {topic} ({e})")
            continue

        topic_meta = meta.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            print(f"  Topic {topic}: not readable ({topic_meta.error if topic_meta else 'no metadata'})")
            problems.append(f"could not read {topic} to clear it")
            continue

        partitions = [TopicPartition(topic, p, OFFSET_END) for p in topic_meta.partitions]
        futures = admin.delete_records(partitions)

        deleted, errors = 0, []
        for tp, future in futures.items():
            try:
                future.result()
                deleted += 1
            except Exception as e:
                errors.append((tp.partition, e))

        if not errors:
            print(f"  Topic {topic}: cleared ({deleted} partition(s))")
            continue

        # race_standings is compacted (it's the keyed upsert side of the LAB 3
        # temporal join); Kafka refuses delete-records on a compacted topic.
        # Harmless: the next race overwrites all 22 keys on lap 0.
        if any("POLICY_VIOLATION" in str(e) for _, e in errors):
            print(f"  Topic {topic}: kept (compacted topic — records can't be deleted)")
            print("    Harmless: the next race overwrites every key on lap 0.")
            continue

        for partition, e in errors:
            print(f"    partition {partition}: {e}")
        print(f"  Topic {topic}: {deleted} partition(s) cleared, {len(errors)} failed")
        problems.append(f"could not clear {len(errors)} partition(s) of {topic}")

    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset this workshop's lab state for a fresh Lab 3 / Lab 4 run")
    parser.add_argument(
        "--keep-source",
        action="store_true",
        help="Leave car_telemetry / race_standings data in place (default: clear it for a blank slate)",
    )
    args = parser.parse_args()

    root = project_root()
    state = tf_state_path(root)
    if not state.exists():
        sys.exit(f"No Terraform state found at {state} — nothing to reset. Run `uv run python scripts/cli.py up` first.")

    print("=== Gemini workshop reset ===\n")
    print("Make sure `scripts/race.py` is NOT running before continuing — clearing the")
    print("source topics while it's still producing puts the records straight back.")
    if input("Continue? (y/n): ").strip().lower() != "y":
        print("Cancelled.")
        sys.exit(0)

    tf = run_terraform_output(state)
    env_id = tf["environment_id"]
    cluster_id = tf.get("attendee_credentials", {}).get("cluster_id") or tf.get("cluster_id")

    problems: list[str] = []

    print("\n--- Dropping lab objects (pit_decisions, car_state, pit_strategy_agent) ---")
    problems += drop_flink_objects(tf)

    print("\n--- Deleting lab topics + Schema Registry subjects ---")
    admin = kafka_admin(tf)
    present = existing_topics(admin)
    for topic in LAB_TOPICS:
        exists = None if present is None else topic in present
        problems += delete_topic_and_subjects(topic, env_id, cluster_id, exists)

    if not args.keep_source:
        print("\n--- Clearing source topics (car_telemetry, race_standings) ---")
        problems += truncate_topics(admin, SOURCE_TOPICS, present)
    else:
        print("\n--- --keep-source: leaving car_telemetry / race_standings data in place ---")

    seed_marker_path(root).unlink(missing_ok=True)

    print()
    if problems:
        print("=== Reset INCOMPLETE ===")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)

    print("=== Reset complete ===")
    print("Next: recreate car_state (Lab 3), wait for it to show RUNNING, then")
    print("`uv run python scripts/race.py` before recreating pit_decisions (Lab 4).")


if __name__ == "__main__":
    main()
