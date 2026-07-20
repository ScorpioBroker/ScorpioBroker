#!/usr/bin/env python3
"""Export Scorpio PostgreSQL schema for agent reference.

Usage:
  python export-schema.py                  # try live DB, fallback to migrations
  python export-schema.py from-db          # live DB only
  python export-schema.py from-migrations  # replay Flyway migrations via Docker
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_DIR = SCRIPT_DIR.parent
REPO_ROOT = SKILL_DIR.parents[1]  # agent/scorpio-db-schema → repo root
DEFAULT_PROPERTIES = (
    REPO_ROOT / "AllInOneRunner" / "src" / "main" / "resources" / "application.properties"
)
MIGRATIONS_DIR = REPO_ROOT / "AllInOneRunner" / "src" / "main" / "resources" / "db" / "migration"
OUTPUT_MD = SKILL_DIR / "schema-reference.md"
OUTPUT_SQL = SKILL_DIR / "schema-reference.sql"

DOCKER_IMAGE = "postgis/postgis:16-3.4"
DOCKER_CONTAINER = "scorpio-schema-export"

TABLE_PURPOSES = {
    "entity": "NGSI-LD entities (JSONB `entity` column, extracted index fields)",
    "csource": "Context source registrations",
    "csourceinformation": "Denormalized csource query index used by DAOs",
    "temporalentity": "Temporal entity headers",
    "temporalentityattrinstance": "Temporal attribute instances",
    "subscriptions": "Entity subscriptions",
    "registry_subscriptions": "Registry subscriptions",
    "contexts": "Stored JSON-LD contexts",
    "entitymap": "Distributed query entity map cache",
    "entitymap_management": "Legacy entity map management (if present)",
    "tenant": "Multi-tenant routing",
    "core_context_store": "Core context cache",
    "flyway_schema_history": "Flyway migration history (internal)",
}


def find_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise RuntimeError(
            f"Required tool '{name}' not found on PATH. "
            "Install PostgreSQL client tools (pg_dump, psql)."
        )
    return path


def resolve_placeholder(value: str) -> str:
    match = re.fullmatch(r"\$\{([^:}]+)(?::([^}]*))?\}", value.strip())
    if not match:
        return value.strip()
    env_name, default = match.group(1), match.group(2) or ""
    return os.environ.get(env_name, default)


def load_db_config(properties_path: Path, overrides: dict[str, str]) -> dict[str, str]:
    keys = {
        "scorpio.postgres.host": "host",
        "scorpio.postgres.port": "port",
        "scorpio.postgres.username": "username",
        "scorpio.postgres.password": "password",
        "scorpio.postgres.database-name": "database",
    }
    values: dict[str, str] = {}
    if properties_path.is_file():
        for line in properties_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, raw = line.split("=", 1)
            key = key.strip()
            if key in keys:
                values[keys[key]] = resolve_placeholder(raw)
    defaults = {
        "host": "localhost",
        "port": "5432",
        "username": "ngb",
        "password": "ngb",
        "database": "ngb",
    }
    config = {**defaults, **values, **{k: v for k, v in overrides.items() if v}}
    config["port"] = str(config["port"])
    return config


def can_connect(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def run_command(cmd: list[str], env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(cmd, capture_output=True, text=True, env=merged, check=check)


def pg_dump_schema(config: dict[str, str]) -> str:
    pg_dump = find_tool("pg_dump")
    cmd = [
        pg_dump,
        "--schema-only",
        "--no-owner",
        "--no-privileges",
        "--schema=public",
        "-h",
        config["host"],
        "-p",
        config["port"],
        "-U",
        config["username"],
        "-d",
        config["database"],
    ]
    result = run_command(cmd, env={"PGPASSWORD": config["password"]})
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "pg_dump failed")
    return result.stdout


def psql_exec(config: dict[str, str], sql: str, database: str | None = None) -> None:
    psql = find_tool("psql")
    cmd = [
        psql,
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        config["host"],
        "-p",
        config["port"],
        "-U",
        config["username"],
        "-d",
        database or config["database"],
        "-c",
        sql,
    ]
    result = run_command(cmd, env={"PGPASSWORD": config["password"]}, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "psql failed")


def psql_file(config: dict[str, str], path: Path, database: str | None = None) -> None:
    psql = find_tool("psql")
    cmd = [
        psql,
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        config["host"],
        "-p",
        config["port"],
        "-U",
        config["username"],
        "-d",
        database or config["database"],
        "-f",
        str(path),
    ]
    result = run_command(cmd, env={"PGPASSWORD": config["password"]}, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Migration failed: {path.name}\n{result.stderr.strip() or result.stdout.strip()}"
        )


def docker_available() -> bool:
    return shutil.which("docker") is not None


def stop_container() -> None:
    if not docker_available():
        return
    run_command(["docker", "rm", "-f", DOCKER_CONTAINER], check=False)


def wait_for_postgres(config: dict[str, str], attempts: int = 30) -> None:
    for _ in range(attempts):
        if can_connect(config["host"], int(config["port"]), timeout=1.0):
            try:
                psql_exec(config, "SELECT 1", database=config["database"])
                return
            except RuntimeError:
                pass
        time.sleep(1)
    raise RuntimeError("Postgres did not become ready in time")


def export_from_db(config: dict[str, str], source_label: str) -> str:
    if not can_connect(config["host"], int(config["port"])):
        raise RuntimeError(
            f"Cannot connect to {config['host']}:{config['port']}. "
            "Start Scorpio Postgres or run with from-migrations."
        )
    dump = pg_dump_schema(config)
    write_outputs(dump, source_label, config)
    return dump


def export_from_migrations(base_config: dict[str, str]) -> str:
    if not MIGRATIONS_DIR.is_dir():
        raise RuntimeError(f"Migrations directory not found: {MIGRATIONS_DIR}")
    migrations = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migrations:
        raise RuntimeError(f"No migration files in {MIGRATIONS_DIR}")

    if not docker_available():
        raise RuntimeError(
            "Docker is required for from-migrations. Install Docker or use from-db "
            "against a running Postgres instance."
        )

    stop_container()
    port = str(base_config.get("port", "5432"))
    user = base_config.get("username", "ngb")
    password = base_config.get("password", "ngb")
    database = base_config.get("database", "ngb")

    run_command(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            DOCKER_CONTAINER,
            "-e",
            f"POSTGRES_USER={user}",
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-e",
            f"POSTGRES_DB={database}",
            "-p",
            f"127.0.0.1:{port}:5432",
            DOCKER_IMAGE,
        ]
    )

    config = {
        "host": "127.0.0.1",
        "port": port,
        "username": user,
        "password": password,
        "database": database,
    }

    try:
        wait_for_postgres(config)
        for migration in migrations:
            print(f"Applying {migration.name}...")
            psql_file(config, migration)
        dump = pg_dump_schema(config)
        write_outputs(dump, "migrations", config)
        return dump
    finally:
        stop_container()


def split_dump_sections(dump: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {
        "extensions": [],
        "tables": [],
        "functions": [],
        "triggers": [],
        "indexes": [],
        "other": [],
    }
    current = "other"
    buffer: list[str] = []

    def flush() -> None:
        if buffer:
            sections[current].append("".join(buffer).rstrip() + "\n")
            buffer.clear()

    for line in dump.splitlines(keepends=True):
        upper = line.upper()
        if line.startswith("CREATE EXTENSION"):
            flush()
            current = "extensions"
            buffer.append(line)
            continue
        if re.match(r"^CREATE TABLE\b", line, re.IGNORECASE):
            flush()
            current = "tables"
            buffer.append(line)
            continue
        if re.match(r"^CREATE (OR REPLACE )?FUNCTION\b", line, re.IGNORECASE):
            flush()
            current = "functions"
            buffer.append(line)
            continue
        if re.match(r"^CREATE TRIGGER\b", line, re.IGNORECASE):
            flush()
            current = "triggers"
            buffer.append(line)
            continue
        if re.match(r"^CREATE (UNIQUE )?INDEX\b", line, re.IGNORECASE):
            flush()
            current = "indexes"
            buffer.append(line)
            continue
        if line.strip() == "" and not buffer:
            continue
        buffer.append(line)

    flush()
    return sections


def extract_table_names(table_blocks: list[str]) -> list[str]:
    names: list[str] = []
    for block in table_blocks:
        match = re.search(
            r"CREATE TABLE(?: IF NOT EXISTS)?\s+(?:public\.)?([\"']?)(\w+)\1",
            block,
            re.IGNORECASE,
        )
        if match:
            names.append(match.group(2).lower())
    return sorted(set(names))


def extract_function_names(function_blocks: list[str]) -> list[str]:
    names: list[str] = []
    for block in function_blocks:
        match = re.search(
            r"CREATE (?:OR REPLACE )?FUNCTION\s+(?:public\.)?([\"']?)(\w+)\1",
            block,
            re.IGNORECASE,
        )
        if match:
            names.append(match.group(2))
    return sorted(set(names), key=str.lower)


def anchor(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def write_outputs(dump: str, source: str, config: dict[str, str]) -> None:
    OUTPUT_SQL.write_text(dump, encoding="utf-8")
    sections = split_dump_sections(dump)
    tables = extract_table_names(sections["tables"])
    functions = extract_function_names(sections["functions"])
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines: list[str] = [
        f"<!-- generated: {generated_at} | source: {source} | host: {config['host']}:{config['port']} | db: {config['database']} -->",
        "",
        "# Scorpio PostgreSQL Schema Reference",
        "",
        "Auto-generated local schema snapshot for DAO development. Regenerate with:",
        "",
        "```bash",
        "python agent/scorpio-db-schema/scripts/export-schema.py",
        "```",
        "",
        "## Table index",
        "",
        "| Table | Purpose |",
        "|-------|---------|",
    ]

    for table in tables:
        purpose = TABLE_PURPOSES.get(table, "See DDL below")
        lines.append(f"| `{table}` | {purpose} | [ddl](#table-{anchor(table)}) |")

    if functions:
        lines.extend(["", "## Function index", ""])
        for fn in functions:
            lines.append(f"- `{fn}` — [signature](#function-{anchor(fn)})")

    lines.extend(["", "## Tables", ""])
    for block in sections["tables"]:
        match = re.search(
            r"CREATE TABLE(?: IF NOT EXISTS)?\s+(?:public\.)?([\"']?)(\w+)\1",
            block,
            re.IGNORECASE,
        )
        title = match.group(2) if match else "unknown"
        lines.extend([f"### table-{anchor(title)}", "", "```sql", block.rstrip(), "```", ""])

    lines.extend(["", "## Functions", ""])
    if sections["functions"]:
        for block in sections["functions"]:
            match = re.search(
                r"CREATE (?:OR REPLACE )?FUNCTION\s+(?:public\.)?([\"']?)(\w+)\1",
                block,
                re.IGNORECASE,
            )
            title = match.group(2) if match else "unknown"
            lines.extend([f"### function-{anchor(title)}", "", "```sql", block.rstrip(), "```", ""])
    else:
        lines.append("_No functions in public schema dump._")
        lines.append("")

    lines.extend(["", "## Triggers", ""])
    if sections["triggers"]:
        lines.append("```sql")
        lines.append("".join(sections["triggers"]).rstrip())
        lines.append("```")
    else:
        lines.append("_No triggers in public schema dump._")

    lines.extend(["", "## Extensions", ""])
    if sections["extensions"]:
        lines.append("```sql")
        lines.append("".join(sections["extensions"]).rstrip())
        lines.append("```")
    else:
        lines.append("_No extensions in public schema dump._")

    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT_MD}")
    print(f"Wrote {OUTPUT_SQL}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Scorpio DB schema for Cursor agents.")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("from-db", "from-migrations"),
        default=None,
        help="Export source (default: try from-db then from-migrations)",
    )
    parser.add_argument(
        "--properties",
        type=Path,
        default=DEFAULT_PROPERTIES,
        help="Path to application.properties",
    )
    parser.add_argument("--host", dest="host")
    parser.add_argument("--port", dest="port")
    parser.add_argument("--username", dest="username")
    parser.add_argument("--password", dest="password")
    parser.add_argument("--database", dest="database")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    overrides = {
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "password": args.password,
        "database": args.database,
    }
    config = load_db_config(args.properties, overrides)

    try:
        if args.mode == "from-db":
            export_from_db(config, "live-db")
            return 0
        if args.mode == "from-migrations":
            export_from_migrations(config)
            return 0

        # default: live DB first, migrations fallback
        try:
            export_from_db(config, "live-db")
            return 0
        except RuntimeError as exc:
            print(f"Live DB export failed: {exc}", file=sys.stderr)
            print("Falling back to from-migrations...", file=sys.stderr)
            export_from_migrations(config)
            return 0
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
