#!/usr/bin/env python3
"""Assemble standalone AGENTS.md with full inlined OKF SPEC."""
from pathlib import Path
import urllib.request

root = Path(__file__).resolve().parents[1]
spec = urllib.request.urlopen(
    "https://raw.githubusercontent.com/GoogleCloudPlatform/knowledge-catalog/main/okf/SPEC.md"
).read().decode("utf-8")


def demote(md: str) -> str:
    out = []
    for line in md.splitlines():
        if line.startswith("#"):
            out.append("#" + line)
        else:
            out.append(line)
    return "\n".join(out)


spec_nested = demote(spec)

agents = f'''# Agent setup (Scorpio Broker)

This file is the **standalone setup guide for any coding agent** (not tied to a specific IDE or host).
Read it on clone. During setup, materialize the inlined rules/skills below into whatever locations
your agent host uses for persistent instructions (project rules, skills, system prompts, CLAUDE.md
includes, etc.). Destinations are host-specific; the canonical text lives here.

## Repository artifacts

| Path | Purpose |
|------|---------|
| [`AGENTS.md`](AGENTS.md) (this file) | Standalone agent setup: inlined OKF SPEC, rules, and skills |
| [`llm_wiki/`](llm_wiki/) | OKF v0.1 NGSI-LD knowledge bundle (ETSI GS CIM 009 V1.9.1); start at `llm_wiki/index.md` |
| [`llm_wiki/_tools/`](llm_wiki/_tools/) | Optional wiki regenerators |
| [`agent/scorpio-db-schema/`](agent/scorpio-db-schema/) | Schema export scripts + skill mirror; writes gitignored `schema-reference.*` |

## Setup phase (any coding agent)

1. Clone the repository and install prerequisites (Python 3, `pg_dump`/`psql`; Docker optional).
2. Generate the DB schema reference (see **One-time setup** below).
3. **Materialize guidance into your agent host** from the inlined sections in this file:
   - **Always-on:** NGSI-LD wiki usage rule + OKF SPEC (this file already contains them — ensure your host loads `AGENTS.md` or a copy of those sections).
   - **On `*DAO.java` edits:** DAO database schema rule (inlined below).
   - **DAO / SQL / migrations work:** Scorpio database schema skill (inlined below).
4. Point the agent at [`llm_wiki/index.md`](llm_wiki/index.md) for NGSI-LD progressive disclosure (do not load the whole tree).

Do **not** invent NGSI-LD semantics without consulting `llm_wiki/`.
Do **not** change DAO SQL without consulting `agent/scorpio-db-schema/schema-reference.md` after export.

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
# Live Scorpio Postgres (reads AllInOneRunner application.properties)
py -3 agent/scorpio-db-schema/scripts/export-schema.py from-db

# Replay Flyway migrations in ephemeral Docker Postgres (no running DB)
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

Override with CLI flags: `--host`, `--port`, `--username`, `--password`, `--database`.

**Prerequisites:** Python 3, PostgreSQL client tools (`pg_dump`, `psql`). `from-migrations` also requires Docker.

Canonical migrations: `AllInOneRunner/src/main/resources/db/migration/`.

### Workflow for `*DAO.java` changes

1. **Ensure schema is current**
   - If `schema-reference.md` is missing, run the export script.
   - If any file under `AllInOneRunner/.../db/migration/` is newer than `schema-reference.md`, re-run export.
2. **Read relevant sections**
   - Open the **Table index** and **Function index** in `schema-reference.md`.
   - Read DDL/signatures for every table, column, and function referenced in your change.
3. **Cross-check Java constants**
   - See [Commons/.../DBConstants.java](Commons/src/main/java/eu/neclab/ngsildbroker/commons/constants/DBConstants.java).
   - If `DBConstants` disagrees with `schema-reference.md`, **trust the schema reference**.
4. **Validate SQL conventions**
   - Unquoted PostgreSQL identifiers are lowercased (`entity`, `csourceinformation`, `createdat`).
   - Current entity JSONB column is `entity` (legacy migrations used `data`).
   - `csourceinformation` exposes denormalized columns (`e_id`, `e_type`, operation flags).
   - Stored functions are often called as `SELECT * FROM NGSILD_APPENDBATCH(...)` — verify exact name and argument types in **Functions**.
5. **After migration changes** — re-run export before further DAO edits.

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

## When to refresh

Re-run the schema export script:

- After `git pull` changes files in `AllInOneRunner/src/main/resources/db/migration/`
- Before opening a PR that modifies any `*DAO.java`
- When `schema-reference.md` is missing

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Cannot connect to localhost:5432` | Start Postgres / Scorpio, or run `from-migrations` |
| `pg_dump not found` | Install PostgreSQL client tools |
| `Docker is required for from-migrations` | Install Docker, or use `from-db` with a running DB |
| Migration apply error | Inspect the failing file in `AllInOneRunner/src/main/resources/db/migration/` |

## New clone checklist

1. Clone repository
2. Install prerequisites above
3. Run `py -3 agent/scorpio-db-schema/scripts/export-schema.py` (or `python3` …)
4. Ensure your coding agent loads this `AGENTS.md` (or materialize the inlined rules/skills into the host’s config locations)
5. Use [`llm_wiki/index.md`](llm_wiki/index.md) for NGSI-LD knowledge

---

## Appendix: Open Knowledge Format (OKF) — full SPEC v0.1 (inlined)

Upstream: https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md

Inlined below so any agent can set up without fetching external docs. Heading levels are shifted one level deeper to nest under this appendix.

{spec_nested}
'''

(root / "AGENTS.md").write_text(agents, encoding="utf-8", newline="\n")
print("Wrote", root / "AGENTS.md", "bytes", (root / "AGENTS.md").stat().st_size)
text = agents
print(".cursor refs:", text.count(".cursor/"))
print("Open Knowledge Format in appendix:", "Open Knowledge Format (OKF)" in text)
print("4.1 Frontmatter inlined:", "### 4.1 Frontmatter" in text or "#### 4.1 Frontmatter" in text)
