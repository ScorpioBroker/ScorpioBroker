#!/usr/bin/env python3
"""Merge testfest AGENT.md + master AGENTS.md into merged-agent.md."""
from __future__ import annotations

import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTFEST = Path(r"C:\Users\hebgen\scorpiodev\testfest\scorpiobroker\AGENT.md")
OUT = ROOT / "merged-agent.md"
SPEC_URL = (
    "https://raw.githubusercontent.com/GoogleCloudPlatform/knowledge-catalog/"
    "main/okf/SPEC.md"
)


def demote_safe(md: str) -> str:
    """Demote markdown headings by one level; leave fenced code blocks alone."""
    out: list[str] = []
    in_fence = False
    fence = "```"
    for line in md.splitlines():
        if line.strip().startswith(fence):
            in_fence = not in_fence
            out.append(line)
            continue
        if not in_fence and line.startswith("#"):
            out.append("#" + line)
        else:
            out.append(line)
    return "\n".join(out)


def main() -> None:
    testfest = TESTFEST.read_text(encoding="utf-8")
    lines = testfest.splitlines()
    i = 1  # skip '# AGENT.md'
    while i < len(lines) and not lines[i].strip():
        i += 1
    tf_core = "\n".join(lines[i:]).strip()

    # Soften CLAUDE.md-only recipe pointer
    tf_core = tf_core.replace(
        "Run it locally with a\n"
        '  `postman/newman` docker container (never install newman) — full recipe in\n'
        '  `CLAUDE.md` §"Running the Postman API test gate locally". The FIWARE NGSI-LD\n'
        "  Robot suite (`api-etsi-test.yml`) is the secondary functional check.",
        "Run it locally with a `postman/newman` Docker container (never install newman "
        "locally); follow `.github/workflows/api-test.yml` (and `CLAUDE.md` if present "
        "for an expanded recipe). The FIWARE NGSI-LD Robot suite (`api-etsi-test.yml`) "
        "is the secondary functional check.",
    )

    spec = urllib.request.urlopen(SPEC_URL).read().decode("utf-8")
    spec_nested = demote_safe(spec)

    merged = f"""# Agent setup (Scorpio Broker)

Standalone guidance for **any coding agent** working on **ScorpioBroker**, an NGSI-LD compliant
context broker (FIWARE Generic Enabler). This is a mature, production codebase.
Respect the existing architecture and conventions. Make minimal, targeted changes.

This document merges project/runtime/convention guidance with standalone setup for the
NGSI-LD OKF wiki, DB schema skill, and the full OKF SPEC (inlined).

During setup, materialize the inlined rules/skills below into whatever locations your agent host
uses for persistent instructions (project rules, skills, system prompts, includes, etc.).
Destinations are host-specific; the canonical text lives here.

---

## Repository artifacts

| Path | Purpose |
|------|---------|
| This file (`merged-agent.md`) / [`AGENTS.md`](AGENTS.md) | Standalone agent setup + project conventions |
| [`llm_wiki/`](llm_wiki/) | OKF v0.1 NGSI-LD knowledge bundle (ETSI GS CIM 009 V1.9.1); start at `llm_wiki/index.md` |
| [`llm_wiki/_tools/`](llm_wiki/_tools/) | Optional wiki regenerators |
| [`agent/scorpio-db-schema/`](agent/scorpio-db-schema/) | Schema export scripts + skill mirror; writes gitignored `schema-reference.*` |

## Setup phase (any coding agent)

1. Clone the repository and install prerequisites (Java 21 / Maven for builds; Python 3, `pg_dump`/`psql` for schema export; Docker optional).
2. Generate the DB schema reference (see **One-time setup** / **Skill: Scorpio database schema** below).
3. **Materialize guidance into your agent host** from the inlined sections in this file:
   - **Always-on:** project conventions (§1–6), NGSI-LD wiki usage rule, OKF SPEC appendix.
   - **On `*DAO.java` edits:** DAO database schema rule.
   - **DAO / SQL / migrations work:** Scorpio database schema skill.
4. Point the agent at [`llm_wiki/index.md`](llm_wiki/index.md) for NGSI-LD progressive disclosure (do not load the whole tree).

Do **not** invent NGSI-LD semantics without consulting `llm_wiki/`.
Do **not** change DAO SQL without consulting `agent/scorpio-db-schema/schema-reference.md` after export.

---

{tf_core}

### DB / schema changes — agent workflow

In addition to Flyway naming rules above: before editing any `*DAO.java` or writing SQL,
follow **Rule: DAO database schema** and **Skill: Scorpio database schema** later in this file.
Export `schema-reference.md` first; trust it over stale Java constants when they disagree.

---

## NGSI-LD OKF knowledge wiki

Agent-readable knowledge bundle for **ETSI GS CIM 009 V1.9.1** (NGSI-LD API), stored as an OKF v0.1 directory of markdown concepts with YAML frontmatter under [`llm_wiki/`](llm_wiki/).

**How to use:** read [`llm_wiki/index.md`](llm_wiki/index.md) → subdirectory `index.md` → leaf concept. Prefer wiki + clause citations over memory. The [normative PDF](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) remains authoritative for compliance claims.

### Rule: NGSI-LD wiki usage (always-on)

When answering or implementing anything about NGSI-LD API behaviour, data model, query languages, HTTP/MQTT bindings, or ETSI CIM 009 requirements:

1. Start at [`llm_wiki/index.md`](llm_wiki/index.md) (OKF v0.1 progressive disclosure).
2. Open the relevant subdirectory `index.md`, then the leaf concept — do **not** load the whole tree.
3. Prefer wiki content + clause citations over inventing semantics from memory.
4. Cite the concept's `clause` field (ETSI GS CIM 009 V1.9.1) when stating normative behaviour.
5. Reconcile wiki requirements with Scorpio code; if they conflict, flag the mismatch explicitly.

**Bundle map**

| Path | Spec area |
|------|-----------|
| `llm_wiki/overview/` | Clauses 1–3 |
| `llm_wiki/framework/` | Clause 4 |
| `llm_wiki/api-operations/` | Clause 5 |
| `llm_wiki/http-binding/` | Clause 6 |
| `llm_wiki/mqtt-binding/` | Clause 7 |
| `llm_wiki/annexes/` | Annexes A–H |

Reserved filenames: `index.md` (listings), `log.md` (history). All other `.md` files are OKF concepts with YAML frontmatter (`type` required). Use bundle-absolute links like `/api-operations/provision/merge-entity.md` relative to `llm_wiki/`.

`llm_wiki/index.md` declares `okf_version: "0.1"`.

---

## Rule: DAO database schema (apply when editing `*DAO.java`)

Before modifying any `*DAO.java` file:

1. Read the Scorpio database schema skill (next section).
2. Ensure [`agent/scorpio-db-schema/schema-reference.md`](agent/scorpio-db-schema/schema-reference.md) exists and is up to date (re-run `agent/scorpio-db-schema/scripts/export-schema.py` if missing or older than the latest Flyway migration).
3. Consult `schema-reference.md` for every table, column, and stored function used in your SQL changes.

### Validation checklist

- Table and column names match the schema reference (unquoted identifiers are lowercase).
- JSONB entity column is `entity` (not legacy `data`) unless working on pre-migration code paths.
- Stored function names and signatures match the **Functions** section (`NGSILD_*`, `ngsild_*`, `getoperations`, etc.).
- `csourceinformation` denormalized columns (`e_id`, `e_type`, operation booleans) match the reference `SELECT` lists in sibling DAOs.

### On mismatch

If DAO SQL disagrees with `schema-reference.md`:

- Prefer the schema reference as source of truth for the current database state.
- Flag whether the fix belongs in Java (DAO), a new Flyway migration, or outdated `DBConstants`.

---

## Skill: Scorpio database schema

**Use when:** editing or reviewing `*DAO.java` files, writing SQL in repository classes, adding Flyway migrations, or debugging query/schema mismatches.

### Schema reference location

Generated locally (gitignored) under [`agent/scorpio-db-schema/`](agent/scorpio-db-schema/):

- `schema-reference.md` — agent-readable markdown
- `schema-reference.sql` — raw `pg_dump` output

Regenerate when missing, stale, or after migration changes.

### Regenerate schema

From repo root:

```bash
py -3 agent/scorpio-db-schema/scripts/export-schema.py
```

On Linux/macOS, use `python3` if `py` is unavailable.

Modes:

```bash
py -3 agent/scorpio-db-schema/scripts/export-schema.py from-db
py -3 agent/scorpio-db-schema/scripts/export-schema.py from-migrations
```

Windows:

```powershell
agent/scorpio-db-schema/scripts/export-schema.ps1
```

Connection defaults come from [AllInOneRunner/src/main/resources/application.properties](AllInOneRunner/src/main/resources/application.properties):

| Property | Env override | Default |
|----------|--------------|---------|
| `scorpio.postgres.host` | `dbhost` | `localhost` |
| `scorpio.postgres.port` | `dbport` | `5432` |
| `scorpio.postgres.username` | `dbuser` | `ngb` |
| `scorpio.postgres.password` | `dbpass` | `ngb` |
| `scorpio.postgres.database-name` | `dbname` | `ngb` |

CLI overrides: `--host`, `--port`, `--username`, `--password`, `--database`.

**Prerequisites:** Python 3, PostgreSQL client tools (`pg_dump`, `psql`). `from-migrations` also requires Docker.

Canonical migrations: `AllInOneRunner/src/main/resources/db/migration/`.

### Workflow for `*DAO.java` changes

1. Ensure schema is current (export if missing; re-export if migrations are newer than `schema-reference.md`).
2. Read **Table index** and **Function index** in `schema-reference.md` for every object your change touches.
3. Cross-check [DBConstants.java](Commons/src/main/java/eu/neclab/ngsildbroker/commons/constants/DBConstants.java); if it disagrees with the schema reference, **trust the schema reference**.
4. Validate SQL conventions: lowercase unquoted identifiers; JSONB column `entity`; `csourceinformation` denormalized columns; verify stored function names/args in **Functions**.
5. After migration changes, re-run export before further DAO edits.

### Key tables (quick map)

| Table | DAO modules |
|-------|-------------|
| `entity` | EntityManager (`EntityInfoDAO`), QueryManager (`QueryDAO`) |
| `csource`, `csourceinformation` | Entity, Query, Registry, Subscription, History managers |
| `temporalentity`, `temporalentityattrinstance` | HistoryEntityManager, HistoryQueryManager, QueryManager |
| `subscriptions`, `registry_subscriptions` | SubscriptionManager, RegistrySubscriptionManager |
| `contexts` | Subscription DAOs |
| `entitymap` | QueryManager, EntityManager |
| `tenant` | Multi-tenant routing |

### Staleness check

- Latest migration: newest file in `AllInOneRunner/src/main/resources/db/migration/`
- Schema reference: `agent/scorpio-db-schema/schema-reference.md`

Re-export when migrations are newer.

### Skill troubleshooting

| Problem | Action |
|---------|--------|
| Connection refused on `from-db` | Start local Postgres / Scorpio stack, or use `from-migrations` |
| `pg_dump` not found | Install PostgreSQL client tools and add to PATH |
| Docker required | Use `from-migrations` only when Docker is available |
| Migration apply fails | Fix failing SQL in `AllInOneRunner/.../db/migration/` first |

---

## One-time setup (database schema reference)

### Prerequisites

- **Python 3**
- **PostgreSQL client tools** (`pg_dump`, `psql`) on your PATH
- **Docker** (optional, only for `from-migrations` without a running Scorpio DB)

### Generate the local schema reference

```bash
py -3 agent/scorpio-db-schema/scripts/export-schema.py
```

Writes gitignored files:

- `agent/scorpio-db-schema/schema-reference.md`
- `agent/scorpio-db-schema/schema-reference.sql`

### Export modes

| Command | When to use |
|---------|-------------|
| `export-schema.py` (default) | Try live Postgres first, fall back to replaying migrations in Docker |
| `export-schema.py from-db` | Scorpio Postgres is already running |
| `export-schema.py from-migrations` | No local DB; Docker replays `AllInOneRunner/.../db/migration/` |

---

## When to refresh (schema)

Re-run the schema export script:

- After `git pull` changes files in `AllInOneRunner/src/main/resources/db/migration/`
- Before opening a PR that modifies any `*DAO.java`
- When `schema-reference.md` is missing

## New clone checklist

1. Clone repository
2. Install Java 21, Maven, Python 3, PostgreSQL client tools (Docker optional)
3. Run `py -3 agent/scorpio-db-schema/scripts/export-schema.py`
4. Ensure your coding agent loads this document (or materialize inlined rules/skills into the host)
5. For local dev, run AllInOneRunner with the in-memory profile (see §2)
6. Use [`llm_wiki/index.md`](llm_wiki/index.md) for NGSI-LD knowledge

---

## Appendix: Open Knowledge Format (OKF) — full SPEC v0.1 (inlined)

Upstream: https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md

Inlined below so any agent can set up without fetching external docs. Heading levels are shifted one level deeper to nest under this appendix. Code fences are left unchanged.

{spec_nested}
"""

    OUT.write_text(merged, encoding="utf-8", newline="\n")
    print(f"Wrote {OUT} ({OUT.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
