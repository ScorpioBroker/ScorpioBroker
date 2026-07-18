# AGENT.md

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
  `build-java-aaio`). No change is done until this passes. Run it locally with a
  `postman/newman` docker container (never install newman) — full recipe in
  `CLAUDE.md` §"Running the Postman API test gate locally". The FIWARE NGSI-LD
  Robot suite (`api-etsi-test.yml`) is the secondary functional check.

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
