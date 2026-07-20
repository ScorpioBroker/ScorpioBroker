---
name: scorpio-db-schema
description: >-
  Consult and refresh the Scorpio PostgreSQL schema (tables, columns, stored
  functions, triggers). Use when editing or reviewing *DAO.java files, writing
  SQL in repository classes, adding Flyway migrations, or debugging query/schema
  mismatches in scorpiobroker.
---

# Scorpio Database Schema

Canonical guidance is also inlined in [AGENTS.md](../../AGENTS.md). Prefer that file for standalone agent setup.

## Schema reference location

Generated locally (gitignored):

- [schema-reference.md](schema-reference.md) — agent-readable markdown
- [schema-reference.sql](schema-reference.sql) — raw `pg_dump` output

Regenerate when missing, stale, or after migration changes.

## Regenerate schema

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

Connection defaults come from [AllInOneRunner/src/main/resources/application.properties](../../AllInOneRunner/src/main/resources/application.properties):

- `scorpio.postgres.host` (`dbhost`, default `localhost`)
- `scorpio.postgres.port` (`dbport`, default `5432`)
- `scorpio.postgres.username` (`dbuser`, default `ngb`)
- `scorpio.postgres.password` (`dbpass`, default `ngb`)
- `scorpio.postgres.database-name` (`dbname`, default `ngb`)

Override with CLI flags: `--host`, `--port`, `--username`, `--password`, `--database`.

**Prerequisites:** Python 3, PostgreSQL client tools (`pg_dump`, `psql`). `from-migrations` also requires Docker.

Canonical migrations: `AllInOneRunner/src/main/resources/db/migration/`.

## Workflow for `*DAO.java` changes

Before editing any DAO:

1. **Ensure schema is current**
   - If `schema-reference.md` is missing, run the export script.
   - If any file under `AllInOneRunner/.../db/migration/` is newer than `schema-reference.md`, re-run export.

2. **Read relevant sections**
   - Open the **Table index** and **Function index** in `schema-reference.md`.
   - Read DDL/signatures for every table, column, and function referenced in your change.

3. **Cross-check Java constants**
   - See [Commons/src/main/java/eu/neclab/ngsildbroker/commons/constants/DBConstants.java](../../Commons/src/main/java/eu/neclab/ngsildbroker/commons/constants/DBConstants.java).
   - If `DBConstants` disagrees with `schema-reference.md`, **trust the schema reference** and note whether Java or migrations need updating.

4. **Validate SQL conventions**
   - Unquoted PostgreSQL identifiers are lowercased (`entity`, `csourceinformation`, `createdat`).
   - Current entity JSONB column is `entity` (legacy migrations used `data`).
   - `csourceinformation` exposes denormalized columns (`e_id`, `e_type`, operation flags) used heavily in DAO `SELECT` lists.
   - Stored functions are often called as `SELECT * FROM NGSILD_APPENDBATCH(...)` — verify exact name and argument types in the **Functions** section.

5. **After migration changes**
   - Re-run export before further DAO edits in the same session.

## Key tables (quick map)

| Table | DAO modules |
|-------|-------------|
| `entity` | EntityManager (`EntityInfoDAO`), QueryManager (`QueryDAO`) |
| `csource`, `csourceinformation` | Entity, Query, Registry, Subscription, History managers |
| `temporalentity`, `temporalentityattrinstance` | HistoryEntityManager, HistoryQueryManager, QueryManager |
| `subscriptions`, `registry_subscriptions` | SubscriptionManager, RegistrySubscriptionManager |
| `contexts` | Subscription DAOs |
| `entitymap` | QueryManager, EntityManager |
| `tenant` | Multi-tenant routing |

## Staleness check

Compare modification times:

- Latest migration: newest file in `AllInOneRunner/src/main/resources/db/migration/`
- Schema reference: `agent/scorpio-db-schema/schema-reference.md`

Re-export when migrations are newer.

## Troubleshooting

| Problem | Action |
|---------|--------|
| Connection refused on `from-db` | Start local Postgres / Scorpio stack, or use `from-migrations` |
| `pg_dump` not found | Install PostgreSQL client tools and add to PATH |
| Docker required | Use `from-migrations` only when Docker is available |
| Migration apply fails | Fix failing SQL in `AllInOneRunner/.../db/migration/` first |
