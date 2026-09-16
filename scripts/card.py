"""Build the credential card from Terraform outputs.

Trimmed, single-workshop adaptation of the main repo's scripts/workshop/creds.py
(``_card_fields``/``_write_env``/``_write_md``). Dropped entirely: wsa
build-output.csv handling, 1Password password resolution, the dispenser CSV
column, and the LAB 5 watsonx Orchestrate section — none apply here, since this
workshop has no organizer-provisioned shared environment and ends at LAB 4.
"""

from __future__ import annotations

import base64
from pathlib import Path


def rtce_endpoint(region: str, org_id: str, env_id: str, cluster_id: str) -> str:
    """RTCE MCP endpoint for this cluster. Empty unless all IDs resolve."""
    if not (region and org_id and env_id and cluster_id):
        return ""
    return (
        f"https://mcp.{region}.aws.confluent.cloud/mcp/v1/context-engine"
        f"/organizations/{org_id}/environments/{env_id}/kafka-clusters/{cluster_id}"
    )


def card_fields(prefix: str, email: str, out: dict, region: str = "") -> dict[str, str]:
    """Flatten gemini-workshop/terraform's outputs into a single dict.

    ``out`` is terraform-output-shaped: flat top-level keys (environment_id,
    cluster_bootstrap, ...) plus a nested ``attendee_credentials`` dict for the
    Kafka/Schema-Registry secrets — exactly what
    ``gemini-workshop/terraform/outputs.tf`` produces.
    """
    ac = out.get("attendee_credentials", {})
    cluster_id = ac.get("cluster_id", "") or out.get("cluster_id", "")
    rtce_key, rtce_secret = out.get("rtce_api_key", ""), out.get("rtce_api_secret", "")
    return {
        "prefix": prefix,
        "email": email,
        "environment_id": out.get("environment_id", ""),
        "environment_url": ac.get("environment_url", ""),
        "flink_rest_endpoint": out.get("flink_rest_endpoint", ""),
        "organization_id": out.get("organization_id", ""),
        "compute_pool_id": out.get("compute_pool_id", ""),
        "catalog": out.get("environment_name", ""),
        "database": out.get("cluster_name", ""),
        "flink_api_key": ac.get("flink_api_key", ""),
        "flink_api_secret": ac.get("flink_api_secret", ""),
        "kafka_bootstrap": out.get("cluster_bootstrap", ""),
        "kafka_api_key": ac.get("kafka_api_key", ""),
        "kafka_api_secret": ac.get("kafka_api_secret", ""),
        "schema_registry_url": ac.get("schema_registry_url", ""),
        "sr_api_key": ac.get("sr_api_key", ""),
        "sr_api_secret": ac.get("sr_api_secret", ""),
        "rtce_mcp_endpoint": rtce_endpoint(region, out.get("organization_id", ""), out.get("environment_id", ""), cluster_id),
        "rtce_api_key": rtce_key,
        "rtce_api_secret": rtce_secret,
    }


def write_env(creds_dir: Path, f: dict[str, str]) -> None:
    # F1_-namespaced so it can be sourced without clobbering other env vars.
    lines = [f"F1_{k.upper()}={v}" for k, v in f.items()]
    (creds_dir / f"{f['prefix']}.env").write_text("\n".join(lines) + "\n")


def _rtce_command(f: dict[str, str]) -> str:
    if not (f.get("rtce_api_key") and f.get("rtce_mcp_endpoint")):
        return ""
    token = base64.b64encode(f"{f['rtce_api_key']}:{f['rtce_api_secret']}".encode()).decode()
    return (
        f"claude mcp add --transport http real-time-context-engine {f['rtce_mcp_endpoint']} "
        f'--header "Authorization: Basic {token}"'
    )


def _rtce_section(f: dict[str, str]) -> str:
    command = _rtce_command(f)
    if not command:
        return ""
    wrapped = command.replace(" --header ", " \\\n  --header ")
    return f"""
## Optional bonus — ask an AI agent about the live race (Real-Time Context Engine)

`car_telemetry` is already published to Confluent's Real-Time Context Engine, so
an AI agent can query the live sensor stream directly — no Kafka client and no
consumer group.

Register it with Claude Code (one line, run it anywhere):

```bash
{wrapped}
```

Then just ask, in plain English:

- "What are car 88's front-left tire temperatures over the last few laps?"
- "Is car 88's front-left tire flagged as anomalous?" (after enabling RTCE on `car_state` in the Console — Lab 5)
"""


def write_md(creds_dir: Path, f: dict[str, str]) -> None:
    md = f"""# F1 Pit Wall Workshop (Gemini Edition) — Your Environment

**Attendee:** `{f["prefix"]}`  ·  **Driver:** John Doe (#88)  ·  **Circuit:** Silverstone

## Your environment

| | |
|--|--|
| Confluent environment | `{f["environment_id"]}` ([open in Console]({f["environment_url"]})) |
| Compute pool | `{f["compute_pool_id"]}` |
| Catalog / Database | `{f["catalog"]}` / `{f["database"]}` |
| Flink endpoint | `{f["flink_rest_endpoint"]}` |

## Getting started

1. Open the [Confluent Cloud Console](https://confluent.cloud/), find your
   `{f["catalog"]}` environment, open its **Flink** tab, and **Open SQL workspace**.
2. Set the workspace's catalog/database to the values above.
3. Confirm your environment is live:

   ```sql
   SHOW TABLES;                     -- car_telemetry, race_standings, driver_race_history
   SELECT * FROM race_standings;    -- 22 cars, updating live
   ```

## The live race feed and dashboard

Save the companion file `{f["prefix"]}.env` somewhere safe — it holds your Kafka,
Schema Registry, and Flink API keys. From `gemini-workshop/`:

```bash
uv run python scripts/race.py       # leave running in its own terminal
uv run python scripts/pitwall_app.py   # opens http://localhost:8000
```
{_rtce_section(f)}"""
    (creds_dir / f"{f['prefix']}.md").write_text(md)
