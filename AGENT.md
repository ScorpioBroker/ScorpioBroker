# Agent setup (Scorpio Broker)

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

Guidance for AI coding agents working on **ScorpioBroker**, an NGSI-LD compliant
context broker (FIWARE Generic Enabler). This is a mature, production codebase.
Respect the existing architecture and conventions. Make minimal, targeted changes.

---

## 1. What this project is

Scorpio implements the full ETSI NGSI-LD API: create/update/append/delete and
query of context entities, subscribe/notify, temporal (history) queries, and
context source registration/discovery for distributed and federated deployments.

- **Language / build:** Java 21, Maven (multi-module), Quarkus 3.x
- **Reactive stack:** SmallRye Mutiny (`Uni`/`Multi`), Vert.x, RESTEasy Reactive
- **Persistence:** PostgreSQL (with PostGIS), Flyway migrations, reactive PG client
- **Messaging:** SmallRye Reactive Messaging — Kafka / MQTT / AMQP / ActiveMQ / SQS,
  or an **in-memory** bus for single-process runs
- **Group / version:** `eu.neclab.ngsildbroker`, currently `6.0.1-SNAPSHOT`

## 2. Running for development — use this command

When developing or debugging, **always run the AllInOneRunner in dev mode with the
in-memory profile**:

```bash
mvn quarkus:dev -DskipTests -Din-memory -Pin-memory -D"quarkus.profile=in-memory"
```

Run it from the `AllInOneRunner` module (or with `-pl AllInOneRunner -am` from the
root). The in-memory profile uses an internal pseudo message bus and avoids
needing Kafka/MQTT/etc. A reachable PostgreSQL is still required (defaults:
host `localhost:5432`, db/user/pass `ngb`/`ngb`/`ngb`, configurable via
`dbhost`/`dbport`/`dbname`/`dbuser`/`dbpass`). Flyway runs migrations at startup.

- HTTP port for the AllInOneRunner is `9090`.
- Do **not** silently switch the run command to a different profile or messaging
  backend. If a task genuinely needs Kafka/MQTT/SQS, call it out explicitly.

## 3. Module / architecture map

Microservice design. Each manager is an independent Quarkus app; the
**AllInOneRunner** bundles them all into one process for non-scaled deployments.

| Module | Responsibility |
|---|---|
| `Commons` | Shared datatypes, constants, NGSI-LD/JSON-LD handling, DB connection mgmt, tools. The biggest, most central module. |
| `AtContextServer` | Serves and caches `@context` documents |
| `EntityManager` | Entity CRUD + batch entity operations |
| `QueryManager` | Entity queries, distributed/federated query resolution |
| `SubscriptionManager` | Subscriptions + notifications |
| `RegistryManager` | Context source registrations |
| `RegistrySubscriptionManager` | Subscriptions over registrations |
| `HistoryEntityManager` | Temporal entity writes |
| `HistoryQueryManager` | Temporal queries |
| `InfoManager` | Broker info / `/version`-style endpoints |
| `SnsFanoutMessaging` | AWS SNS fanout helper (used in AWS/Garnet deployments) |
| `AllInOneRunner` | Aggregates all managers into one deployable; owns the canonical `application.properties` and Flyway migrations |
| `BrokerParent` | Parent POM: dependency versions, plugins, build profiles |

Typical layout inside a manager: `controller/` (JAX-RS resources, `@Path`),
`service(s)/` (business logic, returns `Uni`), `repository/` or `dao` (DB access),
`messaging/` (reactive-messaging consumers/producers).

## 4. Conventions to follow

### Reactive style
Almost everything is non-blocking and returns Mutiny `Uni`/`Multi`. **Do not
introduce blocking calls** (`.await()`, blocking JDBC, `Thread.sleep`, blocking
I/O) on the request/event path. Chain with `onItem()`, `onFailure()`, etc.,
matching the surrounding code.

### Generics and type checking — IMPORTANT
Scorpio has made a **deliberate design decision not to perform internal generic /
type-safety checks**. Incoming data and requests are validated and guarded at the
API layer; internal code trusts that data shape. You will see `@SuppressWarnings("unchecked")`,
raw `Map`/`List` casts, and direct casting of JSON-derived structures throughout.
This is intentional.

- **Do not** add defensive type checks, `instanceof` guards, null-checking
  scaffolding, or generic-bound rewrites to internal methods just because the
  compiler or a linter warns about unchecked operations.
- **Do not** "fix" `@SuppressWarnings` by adding runtime validation.
- Trust that data reaching service/repository layers has already been validated
  upstream. Add validation only at genuine API entry points, and only if the task
  explicitly calls for it.
- If you spot a real, demonstrable bug (not a theoretical type concern), point it
  out — but don't pre-emptively armor working code.

### Messaging profiles
Messaging consumers are split by build profile via `@IfBuildProfile`, e.g.
`EntityMessagingInMemory` (`in-memory`), `EntityMessagingString`
(`kafka`/`amqp`/`sqs`), `EntityMessagingByteArray` (`mqtt`), all extending a
shared `*MessagingBase`. When adding messaging behavior, put shared logic in the
base class and keep the profile-specific subclasses thin. Don't break the
in-memory path.

### Configuration
- Config keys are namespaced `scorpio.*` and injected with
  `@ConfigProperty(name = "...")`. The authoritative property file is
  `AllInOneRunner/src/main/resources/application.properties`; each manager also
  has its own.
- Add new config with a sensible default and document it where the existing keys live.

### DB / schema changes
Schema changes go through **Flyway migrations** in
`AllInOneRunner/src/main/resources/db/migration/` using the existing
`V<date>.<n>__description.sql` naming. Never edit an already-released migration —
add a new one.

## 5. Build, test, lint

- Full build: `mvn clean install` (add `-DskipTests` to skip tests).
- Tests are JUnit-based and sparse (~21 test classes); not every module has
  meaningful coverage. Don't assume a green local test run proves much, and don't
  add large speculative test suites unless asked.
- **PMD** runs in CI (`pmd-ruleset.xml`, broad rule set) but is NOT a gate — ignore
  it; never spend time running or appeasing it. Follow local style (see generics
  note).
- **THE absolute gate for everything is the Postman API test workflow**:
  `.github/workflows/api-test.yml` runs Newman with `api-test.json` +
  `api-test-aaio-environment.json` against the dockerized broker (job
  `build-java-aaio`). No change is done until this passes. Run it locally with a `postman/newman` Docker container (never install newman locally); follow `.github/workflows/api-test.yml` (and `CLAUDE.md` if present for an expanded recipe). The FIWARE NGSI-LD Robot suite (`api-etsi-test.yml`) is the secondary functional check.

## 6. Working style for agents

- This is mature code. Prefer the smallest change that solves the task; mirror
  surrounding patterns rather than refactoring broadly.
- Don't reformat untouched code, reorganize imports project-wide, or "modernize"
  working idioms unprompted.
- Don't add type checks / null guards / validation that the codebase deliberately
  omits (see §4).
- Keep the reactive (non-blocking) contract intact.
- Preserve the in-memory dev path and the AllInOneRunner bundling.
- When in doubt about a cross-cutting change, check how the same thing is done in a
  sibling manager module first — they are deliberately consistent.

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

## Open Knowledge Format (OKF)

**Version 0.1 — Draft**

OKF is an open, human- and agent-friendly format for representing
*knowledge* — the metadata, context, and curated insight that surrounds
data and systems. It is designed to be authored by people, generated by
agents, exchanged across organizations, and consumed by both.

The format is intentionally minimal: a directory of markdown files with
YAML frontmatter. There is no schema registry, no central authority, and
no required tooling. If you can `cat` a file, you can read OKF; if you
can `git clone` a repo, you can ship it.

---

### 1. Motivation

The space of knowledge representation for AI agents is evolving quickly,
and many incompatible conventions are emerging. OKF takes the position
that knowledge is best represented in commonly accessible, established
formats that are:

- **Readable** by humans without tooling.
- **Parseable** by agents without bespoke SDKs.
- **Diffable** in version control.
- **Portable** across tools, organizations, and time.

The format is minimally opinionated. It standardizes only the small set
of structural conventions needed to make a knowledge corpus
*self-describing* — anything beyond that is left to the producer.

#### Goals

1. Define a universal format that **enrichment agents** can write into.
2. Inform how **consumption agents** should read and traverse it.
3. Facilitate **exchange** of knowledge across systems and organizations.
4. Standardize the small number of **required** fields that must be
   present for content to be meaningfully consumed.

#### Non-goals

- Defining a fixed taxonomy of concept types.
- Prescribing storage, serving, or query infrastructure.
- Replacing domain-specific schemas (Avro, Protobuf, OpenAPI, etc.) —
  OKF *references* them; it does not subsume them.

---

### 2. Terminology

- **Knowledge Bundle** — A self-contained, hierarchical collection of
  knowledge documents. The unit of distribution.
- **Concept** — A single unit of knowledge within a bundle. Represented
  as one markdown document. May describe a tangible asset (a table, an
  API), an abstract idea (a metric, a business process), or anything in
  between.
- **Concept ID** — The path of the concept's file within the bundle,
  with the `.md` suffix removed. For example, `tables/users.md` has
  concept ID `tables/users`.
- **Frontmatter** — YAML metadata block delimited by `---` at the top of
  a markdown file.
- **Body** — Everything in the file after the frontmatter.
- **Link** — A standard markdown link from one concept to another, used
  to express relationships beyond the implicit parent/child hierarchy.
- **Citation** — A link from a concept to an external source that
  supports a claim in the body.

---

### 3. Bundle Structure

A bundle is a directory tree of markdown files. The directory structure
is independent of the domain — producers organize concepts however makes
sense for the knowledge being captured.

```
path/to/bundle/
├── index.md                      # Optional. Directory listing for progressive disclosure.
├── log.md                        # Optional. Chronological history of updates.
├── <concept>.md                  # A concept at the bundle root.
└── <subdirectory>/               # Subdirectories organize concepts into groups.
    ├── index.md
    ├── <concept>.md
    └── <subdirectory>/
        └── …
```

A bundle MAY be distributed as:

- A git repository (recommended — provides history, attribution, diffs).
- A tarball or zip archive of the directory.
- A subdirectory within a larger repository.

#### 3.1 Reserved filenames

The following filenames have defined meaning at any level of the
hierarchy and MUST NOT be used for concept documents:

| Filename     | Purpose                                                |
|--------------|--------------------------------------------------------|
| `index.md`   | Directory listing. See §6.                             |
| `log.md`     | Update history. See §7.                                |

All other `.md` files are concept documents.

Tags themselves remain a first-class concept — see the `tags`
frontmatter field in §4.1. OKF does not specify a separate file format
for aggregating documents by tag; producers that want a tag-browsing
view can synthesize one at consumption time by scanning frontmatter.

---

### 4. Concept Documents

Every concept is a UTF-8 markdown file. It has two parts:

1. A **YAML frontmatter block**, delimited by `---` on its own line at
   the start of the file and a closing `---` on its own line.
2. A **markdown body**, containing free-form content.

#### 4.1 Frontmatter

```yaml
---
type: <Type name>                  # REQUIRED
title: <Optional display name>
description: <Optional one-line summary>
resource: <Optional canonical URI for the underlying asset>
tags: [<tag>, <tag>, …]            # Optional
timestamp: <ISO 8601 datetime>     # Optional last-modified time
# … other producer-defined key/value pairs
---
```

**Required:**

- `type` — A short string identifying the kind of concept. Consumers
  use this for routing, filtering, and presentation. Example values:
  `BigQuery Table`, `BigQuery Dataset`, `API Endpoint`, `Metric`,
  `Playbook`, `Reference`.

  Type values are **not** registered centrally. Producers SHOULD pick
  values that are descriptive and self-explanatory; consumers MUST
  tolerate unknown types gracefully (typically by treating them as
  generic concepts).

**Recommended (in priority order):**

- `title` — Human-readable display name. If omitted, consumers MAY
  derive a title from the filename.
- `description` — A single sentence summarizing the concept. Used by
  `index.md` generators, search snippets, and previews.
- `resource` — A URI that uniquely identifies the underlying asset the
  concept describes. Absent for concepts that describe abstract ideas
  rather than physical resources.
- `tags` — A YAML list of short strings for cross-cutting categorization.
- `timestamp` — ISO 8601 datetime of last meaningful change.

**Extensions:** Producers MAY include any additional keys. Consumers
SHOULD preserve unknown keys when round-tripping and SHOULD NOT reject
documents with unrecognized fields.

#### 4.2 Body

The body is standard markdown. Producers SHOULD favor structural
markdown — headings, lists, tables, fenced code blocks — over freeform
prose, since structure aids both human reading and agent retrieval.

There are no required body sections. The following section headings have
**conventional** meaning and SHOULD be used when applicable:

| Heading        | Purpose                                                |
|----------------|--------------------------------------------------------|
| `# Schema`     | Structured description of an asset's columns/fields.   |
| `# Examples`   | Concrete usage examples, often as fenced code blocks.  |
| `# Citations`  | External sources backing claims in the body. See §8.   |

#### 4.3 Example: a concept bound to a resource

```markdown
---
type: BigQuery Table
title: Customer Orders
description: One row per completed customer order across all channels.
resource: https://console.cloud.google.com/bigquery?p=acme&d=sales&t=orders
tags: [sales, orders, revenue]
timestamp: 2026-05-28T14:30:00Z
---

# Schema

| Column        | Type      | Description                              |
|---------------|-----------|------------------------------------------|
| `order_id`    | STRING    | Globally unique order identifier.        |
| `customer_id` | STRING    | Foreign key into [customers](/tables/customers.md). |
| `total_usd`   | NUMERIC   | Order total in US dollars.               |
| `placed_at`   | TIMESTAMP | When the customer submitted the order.   |

# Joins

Joined with [customers](/tables/customers.md) on `customer_id`.

# Citations

[1] [BigQuery table schema](https://console.cloud.google.com/bigquery?p=acme&d=sales&t=orders)
```

#### 4.4 Example: a concept not bound to a resource

```markdown
---
type: Playbook
title: Incident response — data freshness alert
description: Steps to triage a freshness alert on the orders pipeline.
tags: [oncall, incident]
timestamp: 2026-04-12T09:00:00Z
---

# Trigger

A freshness alert fires when `orders` lags more than 30 minutes behind
its expected SLA. See the [orders table](/tables/orders.md).

# Steps

1. Check the [ingestion job dashboard](https://example.com/dash).
2. …
```

---

### 5. Cross-linking

Concepts MAY link to other concepts using standard markdown links. Two
forms are supported:

#### 5.1 Absolute (bundle-relative) links

Begin with `/`, interpreted relative to the bundle root.

```markdown
See the [customers table](/tables/customers.md) for the join key.
```

This is the **recommended** form because it is stable when documents are
moved within their subdirectory.

#### 5.2 Relative links

Standard markdown relative paths.

```markdown
See the [neighboring concept](./other.md).
```

#### 5.3 Link semantics

A link from concept A to concept B asserts a *relationship*. The
specific kind of relationship (parent/child, references, joins-with,
depends-on, etc.) is conveyed by the surrounding prose, not by the link
itself. Consumers that build a graph view typically treat all links as
directed edges of an untyped relationship.

Consumers MUST tolerate broken links — a link whose target does not
exist in the bundle is not malformed; it may simply represent
not-yet-written knowledge.

---

### 6. Index Files

An `index.md` file MAY appear in any directory, including the bundle
root. It enumerates the directory's contents to support **progressive
disclosure** — letting a human or agent see what is available before
opening individual documents.

Index files contain no frontmatter. The body uses one or more sections,
each grouping concepts under a heading:

```markdown
# Section / Group Heading

* [Title 1](relative-url-1) - short description of item 1
* [Title 2](relative-url-2) - short description of item 2

# Another Section

* [Subdirectory](subdir/) - short description of the subdirectory
```

Entries SHOULD include the description from the linked concept's
frontmatter. Producers MAY generate `index.md` automatically; consumers
MAY synthesize one on the fly when none is present.

---

### 7. Log Files (optional)

A `log.md` file MAY appear at any level of the hierarchy to record the
history of changes to that scope. The format is a flat list of
date-grouped entries, newest first:

```markdown
# Directory Update Log

## 2026-05-22
* **Update**: Added new BigQuery table reference for [Customer Metrics](/tables/customer-metrics.md).
* **Creation**: Established the [Dataplex Playbook](/playbooks/dataplex.md).

## 2026-05-15
* **Initialization**: Created foundational directory structure.
* **Update**: Added progressive-disclosure guidelines to the root [index](/index.md).
```

Date headings MUST use ISO 8601 `YYYY-MM-DD` form. Log entries are
prose; the leading bold word (`**Update**`, `**Creation**`,
`**Deprecation**`, etc.) is a convention, not a requirement.

---

### 8. Citations

When a concept's body makes claims sourced from external material,
those sources SHOULD be listed under a `# Citations` heading at the
bottom of the document, numbered:

```markdown
# Citations

[1] [BigQuery public dataset announcement](https://cloud.google.com/blog/products/data-analytics/...)
[2] [Internal data quality runbook](https://wiki.acme.internal/data/quality)
```

Citation links MAY be absolute URLs, bundle-relative paths, or paths
into a `references/` subdirectory that mirrors external material as
first-class OKF concepts.

---

### 9. Conformance

A bundle is **conformant** with OKF v0.1 if:

1. Every non-reserved `.md` file in the tree contains a parseable YAML
   frontmatter block.
2. Every frontmatter block contains a non-empty `type` field.
3. Every reserved filename (`index.md`, `log.md`) follows the structure
   described in §6 and §7 respectively when present.

Consumers SHOULD treat all other constraints as soft guidance. In
particular, consumers MUST NOT reject a bundle because of:

- Missing optional frontmatter fields.
- Unknown `type` values.
- Unknown additional frontmatter keys.
- Broken cross-links.
- Missing `index.md` files.

This permissive consumption model is intentional: OKF is meant to
remain useful as bundles grow, get refactored, and are partially
generated by agents.

---

### 10. Relationship to other formats

OKF is intentionally close to several established patterns:

- **LLM "wiki" repositories** that use markdown + frontmatter as
  agent-readable knowledge bases.
- **Personal knowledge tools** like Obsidian and Notion, which use
  hierarchical markdown with cross-links.
- **"Metadata as code"** approaches that store catalog metadata
  alongside source code rather than in a separate registry.

OKF differs primarily in being **specified** — pinning down the small
set of rules needed for interoperability without dictating tooling.

---

### 11. Versioning

This document specifies OKF version **0.1**. Future revisions will be
versioned in the form `<major>.<minor>`:

- A **minor** version bump introduces backward-compatible additions
  (new optional fields, new conventional section headings).
- A **major** version bump may make breaking changes (renaming required
  fields, changing reserved filenames).

Bundles MAY declare the OKF version they target by including
`okf_version: "0.1"` in a bundle-root `index.md` frontmatter block (the
only place frontmatter is permitted in an `index.md`). Consumers that
do not understand the declared version SHOULD attempt best-effort
consumption rather than refusing the bundle.

---

### Appendix A — Minimal example bundle

```
my_bundle/
├── index.md
├── datasets/
│   ├── index.md
│   └── sales.md
└── tables/
    ├── index.md
    ├── orders.md
    └── customers.md
```

`datasets/sales.md`:

```markdown
---
type: BigQuery Dataset
title: Sales
description: All sales-related tables for the retail business.
resource: https://console.cloud.google.com/bigquery?p=acme&d=sales
tags: [sales]
timestamp: 2026-05-28T00:00:00Z
---

The sales dataset contains transactional tables, including
[orders](/tables/orders.md) and [customers](/tables/customers.md).
```

`tables/orders.md`:

```markdown
---
type: BigQuery Table
title: Orders
description: One row per completed customer order.
resource: https://console.cloud.google.com/bigquery?p=acme&d=sales&t=orders
tags: [sales, orders]
timestamp: 2026-05-28T00:00:00Z
---

# Schema

| Column        | Type      | Description                  |
|---------------|-----------|------------------------------|
| `order_id`    | STRING    | Unique order identifier.     |
| `customer_id` | STRING    | FK to [customers](/tables/customers.md). |
| `total_usd`   | NUMERIC   | Order total in USD.          |

Part of the [sales dataset](/datasets/sales.md).
```
