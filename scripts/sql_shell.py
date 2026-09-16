"""Flink SQL shell driven by a credential card — no Console login needed.

  uv run python scripts/sql_shell.py                      # card resolved from credentials.env
  uv run python scripts/sql_shell.py --creds <prefix>.env
  uv run python scripts/sql_shell.py --exec 'SHOW TABLES'
  uv run python scripts/sql_shell.py --file ../docs/demo-reference/enrichment_anomaly.sql

Standalone adaptation of the main repo's scripts/workshop/sql_shell.py. The
ATTENDEE-GUIDE.md walkthrough uses the Confluent Cloud Console SQL workspace
instead — this shell is a terminal fallback, and its ``FlinkSession`` class is
also what ``seed.py`` reuses to seed ``driver_race_history``.

Submit a statement by ending it with ';'. SELECT/SHOW results stream into a
table (Ctrl-C stops a long-running query); CREATE/DROP/INSERT statements are
submitted and left running. Meta-commands: \\help, \\q.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

import requests  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402

from credentials import load_card  # noqa: E402

try:
    import readline  # noqa: F401
except ImportError:  # pragma: no cover - platform dependent
    pass

console = Console()

TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}
KEEP_PREFIXES = ("CREATE", "INSERT", "DROP", "ALTER")
MAX_STREAM_ROWS = 50

CARD_REMEDIATION = "If this came from a credential card, recreate it: uv run python scripts/cli.py up"


def strip_leading_comments(sql: str) -> str:
    rest = sql.lstrip()
    while rest:
        if rest.startswith("--"):
            _, _, rest = rest.partition("\n")
            rest = rest.lstrip()
        elif rest.startswith("/*"):
            _, _, rest = rest.partition("*/")
            rest = rest.lstrip()
        else:
            break
    return rest


def is_durable(sql: str) -> bool:
    return strip_leading_comments(sql).upper().startswith(KEEP_PREFIXES)


def is_terminator(line: str) -> bool:
    return line.endswith(";") and not line.startswith("--")


def normalize(lines: list[str]) -> str:
    return "\n".join(lines).strip().rstrip(";").strip()


def split_statements(text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in text.splitlines():
        buffer.append(line)
        if is_terminator(line.strip()):
            sql = normalize(buffer)
            buffer = []
            if sql:
                statements.append(sql)
    tail = normalize(buffer)
    if tail and strip_leading_comments(tail):
        statements.append(tail)
    return statements


class FlinkSession:
    def __init__(self, creds: dict[str, str]):
        try:
            self.rest = creds["F1_FLINK_REST_ENDPOINT"].rstrip("/")
            self.org = creds["F1_ORGANIZATION_ID"]
            self.env = creds["F1_ENVIRONMENT_ID"]
            self.pool = creds["F1_COMPUTE_POOL_ID"]
            self.catalog = creds["F1_CATALOG"]
            self.database = creds["F1_DATABASE"]
            self.auth = (creds["F1_FLINK_API_KEY"], creds["F1_FLINK_API_SECRET"])
        except KeyError as e:
            raise SystemExit(f"Credential file is missing {e}.\n  {CARD_REMEDIATION}") from e
        self.base = f"{self.rest}/sql/v1/organizations/{self.org}/environments/{self.env}/statements"
        self._seq = 0

    def _name(self) -> str:
        self._seq += 1
        return f"f1sql-{int(time.time() * 1000) % 10**9}-{self._seq}"

    def submit(self, sql: str) -> str:
        name = self._name()
        body = {
            "name": name,
            "spec": {
                "statement": sql,
                "compute_pool_id": self.pool,
                "properties": {
                    "sql.current-catalog": self.catalog,
                    "sql.current-database": self.database,
                },
            },
        }
        r = requests.post(self.base, json=body, auth=self.auth, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"submit failed ({r.status_code}): {r.text[:600]}")
        return name

    def wait(self, name: str, timeout: int = 120) -> dict:
        deadline = time.time() + timeout
        st = {}
        while time.time() < deadline:
            st = requests.get(f"{self.base}/{name}", auth=self.auth, timeout=30).json()
            if st["status"]["phase"] in TERMINAL_PHASES | {"RUNNING"}:
                return st
            time.sleep(2)
        return st

    def results(self, name: str, max_rows: int, timeout: int = 60, idle_pages: int = 6):
        url = f"{self.base}/{name}/results"
        seen = 0
        empty_streak = 0
        deadline = time.time() + timeout
        while seen < max_rows and time.time() < deadline:
            page = requests.get(url, auth=self.auth, timeout=30).json()
            data = (page.get("results") or {}).get("data") or []
            if data:
                empty_streak = 0
                for item in data:
                    yield item.get("row")
                    seen += 1
                    if seen >= max_rows:
                        return
            else:
                empty_streak += 1
                if seen > 0 and empty_streak >= idle_pages:
                    return
            nxt = (page.get("metadata") or {}).get("next")
            if not nxt:
                return
            url = nxt
            time.sleep(0.5 if data else 1.0)

    def stop(self, name: str) -> None:
        try:
            requests.delete(f"{self.base}/{name}", auth=self.auth, timeout=30)
        except requests.RequestException:
            pass


def _columns(status: dict) -> list[str]:
    cols = status.get("status", {}).get("traits", {}).get("schema", {}).get("columns", [])
    return [c["name"] for c in cols]


def run_statement(session: FlinkSession, sql: str) -> bool:
    keep = is_durable(sql)
    try:
        name = session.submit(sql)
    except RuntimeError as e:
        console.print(f"[red]{e}[/red]")
        return False

    status = session.wait(name)
    phase = status["status"]["phase"]

    if phase == "FAILED":
        console.print(f"[red]FAILED:[/red] {status['status'].get('detail', '')[:1000]}")
        return False

    cols = _columns(status)
    if not cols:
        console.print(f"[green]{phase}[/green]" + ("  (statement left running)" if keep else ""))
        if keep:
            console.print(f"[dim]statement: {name}[/dim]")
        return True

    table = Table(*cols, show_lines=False)
    rows = 0
    try:
        for row in session.results(name, MAX_STREAM_ROWS):
            table.add_row(*[str(v) for v in (row or [])])
            rows += 1
    except KeyboardInterrupt:
        console.print("[yellow](stopped)[/yellow]")
    finally:
        if not keep:
            session.stop(name)

    if rows:
        console.print(table)
        console.print(f"[dim]{rows} row(s){' (truncated)' if rows >= MAX_STREAM_ROWS else ''}[/dim]")
    else:
        console.print("[yellow]no rows (a streaming query may still be warming up — try again)[/yellow]")
    return True


def run_file(session: FlinkSession, path: str | Path) -> int:
    file = Path(path)
    if not file.exists():
        console.print(f"[red]File not found:[/red] {file}")
        return 1

    statements = split_statements(file.read_text())
    if not statements:
        console.print(f"[yellow]No SQL statements in {file}[/yellow]")
        return 1

    total = len(statements)
    for i, sql in enumerate(statements, 1):
        if total > 1:
            console.print(f"[dim]-- {file.name}: statement {i}/{total}[/dim]")
        if not run_statement(session, sql):
            console.print(f"[red]Stopped at statement {i} of {total} — see the error above.[/red]")
            return 1
    return 0


HELP = """[bold]F1 Flink SQL shell[/bold]
  End a statement with ';' to run it (multi-line is fine).
  \\help   show this help
  \\q      quit
Examples:
  SHOW TABLES;
  SELECT * FROM race_standings;
  SELECT car_number, lap, tire_temp_fl_c FROM car_telemetry LIMIT 5;"""


def repl(session: FlinkSession) -> None:
    console.print(HELP)
    buffer: list[str] = []
    while True:
        try:
            prompt = "f1-sql> " if not buffer else "    ...> "
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            console.print("\nbye")
            return
        stripped = line.strip()
        if not buffer and stripped in ("\\q", "\\quit", "exit", "quit"):
            console.print("bye")
            return
        if not buffer and stripped in ("\\help", "\\h", "?"):
            console.print(HELP)
            continue
        buffer.append(line)
        if is_terminator(stripped):
            sql = normalize(buffer)
            buffer = []
            if sql:
                run_statement(session, sql)


def main() -> None:
    parser = argparse.ArgumentParser(description="F1 workshop Flink SQL shell (API-key access, no login)")
    parser.add_argument("--creds", help="Path to your <prefix>.env credential card (default: read from credentials.env)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--exec", help="Run a single statement and exit (non-interactive)")
    mode.add_argument("--file", help="Run every statement in a .sql file, in order, and exit")
    args = parser.parse_args()

    path, creds = load_card(args.creds)
    console.print(f"[dim]card: {path}[/dim]")
    session = FlinkSession(creds)

    if args.exec:
        run_statement(session, normalize([args.exec]))
    elif args.file:
        raise SystemExit(run_file(session, args.file))
    else:
        console.print(f"[green]Connected[/green] to {session.catalog} / {session.database}")
        repl(session)


if __name__ == "__main__":
    main()
