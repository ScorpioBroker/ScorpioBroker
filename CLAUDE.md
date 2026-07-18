# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Read `AGENT.md` first.** It is the canonical, detailed guidance for AI agents on this
> codebase (architecture, conventions, reactive style, the deliberate no-internal-type-checks
> rule, messaging profiles, DB migrations, working style). This file summarizes the essentials
> and adds command details not covered there. Where the two overlap, `AGENT.md` wins.

## What this is

ScorpioBroker — an ETSI **NGSI-LD** compliant context broker (FIWARE Generic Enabler).
Java 21, Maven multi-module, Quarkus 3.x. Reactive throughout: SmallRye Mutiny
(`Uni`/`Multi`), Vert.x, RESTEasy Reactive. Persistence is PostgreSQL + PostGIS via the
reactive PG client with Flyway migrations. Group `eu.neclab.ngsildbroker`, version
`6.0.1-SNAPSHOT`. Current branch: `development-quarkus` (also the main/PR target branch).

## Run for development

Always run the **AllInOneRunner** in dev mode with the in-memory profile (no Kafka/MQTT needed;
a reachable PostgreSQL is still required — defaults `localhost:5432`, db/user/pass `ngb`/`ngb`/`ngb`):

```bash
docker compose -f compose-files/docker-compose-postgis.yml up    # start the required Postgres+PostGIS
mvn quarkus:dev -DskipTests -Din-memory -Pin-memory -D"quarkus.profile=in-memory"
```

Run from the `AllInOneRunner` module, or `-pl AllInOneRunner -am` from the root. HTTP port is
`9090`; base API path `http://localhost:9090/ngsi-ld/v1/`. Do not silently switch to a different
profile or messaging backend — call it out if a task genuinely needs one.

## Build & test

```bash
mvn clean install                 # full build with tests
mvn clean package -DskipTests -Din-memory -Pin-memory -Dquarkus.profile=in-memory   # what CI builds
mvn -pl EntityManager test                              # tests for one module
mvn -pl EntityManager test -Dtest=EntityServiceTest     # a single test class
mvn -pl EntityManager test -Dtest=EntityServiceTest#methodName   # a single test method
```

- Tests are JUnit-based and sparse (~21 classes); a green run proves little. Don't add large
  speculative suites unless asked.
- **THE absolute gate for everything is the Postman API test workflow**
  (`.github/workflows/api-test.yml`, Newman running `api-test.json` with
  `api-test-aaio-environment.json` against the dockerized broker). Run it before considering any
  change done — see "Running the Postman API test gate locally" below. The FIWARE NGSI-LD Robot
  suite (`api-etsi-test.yml`) is the second functional check.
- **PMD**: ignore it. It only runs as a GitHub action (`pmd-ruleset.xml`, literally "every Java
  rule"), the CLI isn't installed locally, and its findings don't gate anything — never spend
  time running or appeasing it. Follow local style instead (see the generics note in `AGENT.md`).

## Running the Postman API test gate locally (mirrors `api-test.yml` job `build-java-aaio`)

Verified working on this Windows machine (2026-07-17). Uses a `postman/newman` docker container —
do NOT install newman/node locally.

```bash
# 0. Free the ports: stop any local broker (Get-Process java) and the standalone postgis compose
# 1. Build the notification testserver image (once per testserver change)
docker build -t testserver ./testserver
# 2. Build the broker docker image exactly like CI (in-memory profile)
mvn clean package -DskipTests -Din-memory -Pin-memory -Ddocker -Ddocker-tag=java-latest -Dquarkus.profile=in-memory
# 3. Start the test stack (scorpio + postgis + testserver; broker on host port 9090)
docker compose -f compose-files/docker-compose-java-aaio-test.yml up -d   # wait for /q/health -> 200
# 4. Newman needs container-visible hosts: copy api-test-aaio-environment.json and replace
#    http://localhost:9090 -> http://scorpio:9090 and http://localhost:8080 -> http://testserver:8080
#    (keep the copy OUT of the repo, e.g. in the scratchpad; the checked-in file stays untouched)
# 5. Run the collection on the compose network
docker run --rm --network compose-files_default -v "C:\Users\Benni\scorpiobroker:/etc/newman" \
  -v "<scratchpad>:/envdir" postman/newman run api-test.json \
  --environment /envdir/api-test-aaio-environment-docker.json \
  --reporters cli,json --reporter-json-export /envdir/newman-report.json
# 6. Tear down: docker compose -f compose-files/docker-compose-java-aaio-test.yml down
```

- Newman exits non-zero on any failed assertion; the CLI summary table at the end has the counts.
- The `notificationserver` value must stay `http://testserver:8080` — the BROKER dereferences it
  for subscription notifications on the compose network.
- If failures appear, diff against a baseline run of the same command on the unmodified tree
  before assuming a regression (same approach as the ETSI suite).

## Build profiles

Selected with `-P<id>`; they wire the messaging backend and packaging:
`in-memory` (dev default), `kafka`, `mqtt`, `amqp`, `sqs`, `activemq`, `azureeventhub`,
`googlepubsub`; `native` (GraalVM native image), plus `macos`/`windowsext` platform helpers.

## Module map

Microservice design: each manager is an independent Quarkus app; **AllInOneRunner** bundles them
into one process and owns the canonical `application.properties` and Flyway migrations.

| Module | Responsibility |
|---|---|
| `Commons` | Shared datatypes, NGSI-LD/JSON-LD handling, DB connection mgmt — biggest, most central |
| `AtContextServer` | Serves/caches `@context` documents |
| `EntityManager` / `QueryManager` | Entity CRUD+batch / queries incl. distributed & federated resolution |
| `SubscriptionManager` / `RegistrySubscriptionManager` | Subscriptions+notifications / subscriptions over registrations |
| `RegistryManager` | Context source registrations |
| `HistoryEntityManager` / `HistoryQueryManager` | Temporal entity writes / temporal queries |
| `InfoManager` | Broker info / version endpoints |
| `SnsFanoutMessaging` | AWS SNS fanout helper (AWS/Garnet) |
| `AllInOneRunner` | Aggregates all managers; canonical config + migrations |
| `BrokerParent` | Parent POM: versions, plugins, build profiles |

Typical layout in a manager: `controller/` (JAX-RS `@Path`), `service(s)/` (returns `Uni`),
`repository`/`dao` (DB), `messaging/` (reactive-messaging consumers/producers).

## Key conventions (see AGENT.md §4 for the full version)

- **Stay non-blocking.** Don't add `.await()`, blocking JDBC, `Thread.sleep`, or blocking I/O on
  the request/event path. Chain with `onItem()`/`onFailure()` like the surrounding code.
- **No internal type checks.** The codebase deliberately omits internal generic/type-safety checks;
  `@SuppressWarnings("unchecked")` and raw `Map`/`List` casts are intentional. Validate only at API
  entry points. Don't add defensive `instanceof`/null guards to service/repository code.
- **Messaging is split by build profile** via `@IfBuildProfile` (`*MessagingInMemory` / `*MessagingString`
  / `*MessagingByteArray`, extending a shared `*MessagingBase`). Put shared logic in the base; keep
  the in-memory path working.
- **DB schema changes** go through new Flyway migrations in
  `AllInOneRunner/src/main/resources/db/migration/` (`V<date>.<n>__description.sql`). Never edit a
  released migration.
- **Config** keys are `scorpio.*`, injected with `@ConfigProperty`. Authoritative file:
  `AllInOneRunner/src/main/resources/application.properties`. Env-var form: uppercase, dots→underscores
  (`quarkus.http.port` → `QUARKUS_HTTP_PORT`).
- Mature code — make minimal, targeted changes; mirror sibling managers (they're deliberately
  consistent) rather than refactoring broadly.

## Running the ETSI NGSI-LD test suite locally (mirrors `.github/workflows/api-etsi-test.yml`)

Verified working on this Windows machine (2026-07):

```bash
# 1. Fresh PostGIS (down -v first for a clean DB like CI gets)
docker compose -f compose-files/docker-compose-postgis.yml down -v
docker compose -f compose-files/docker-compose-postgis.yml up -d
# (Docker Desktop must be running: Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe")

# 2. Build exactly like CI
mvn clean package -DskipTests -Din-memory -Pin-memory -Dquarkus.profile=in-memory

# 3. Start broker WITH Scorpio debug logging (same switch that is commented out in
#    AllInOneRunner/src/main/resources/application.properties)
cd AllInOneRunner/target/quarkus-app
java '-Dquarkus.log.category."eu.neclab".level=DEBUG' -jar quarkus-run.jar > scorpio.log 2>&1 &
# wait for http://localhost:9090/q/health -> 200; log line "Profile in-memory activated"

# 4. Test suite (clone once, configure, run)
git clone https://forge.etsi.org/rep/cim/ngsi-ld-test-suite.git
# edit ngsi-ld-test-suite/resources/variables.py: first two lines ->
#   url = 'http://localhost:9090/ngsi-ld/v1'
#   temporal_api_url = 'http://localhost:9090/ngsi-ld/v1'
# and replace 0.0.0.0 / 127.0.0.1 host values with localhost
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt   # robot 7.x, py3.13 OK
.venv/Scripts/robot --outputdir ./results ./TP/NGSI-LD/CommonBehaviours # or full ./TP/NGSI-LD
```

- Robot exit code == number of failed tests. Results in `results/report.html` / `log.html`.
- CI splits suites per job (CommonBehaviours, Consumption/*, Provision/*, Subscription,
  ContextSource, DistributedOperations, jsonldContext — the latter uses branch `fix/jsonldContext`).
- To send query strings the way sloppy clients do (literal `#`, unescaped specials), normal HTTP
  clients strip/encode them — use a raw TCP socket (PowerShell `System.Net.Sockets.TcpClient`,
  write the request line manually). Confirmed: current stack truncates the query at literal `#`.

## Active work state (2026-07-16, branch `testfest`)

- IMPLEMENTED (uncommitted, awaiting review): central NGSI-LD query parser replacing ALL
  `@QueryParam` parsing on /ngsi-ld/v1 with strict per-operation allowlists (unknown param →
  400 InvalidRequest, spec 6.3.20). New in Commons: tools/QueryParamParser,
  datatypes/ParsedQueryParams, enums/NgsiLdOperation. All listed workarounds removed
  (raw-URI q/type slicing, QueryParser double-decodes, getQueryParamMap, params() paging echo,
  HistoryQueryService split("=") federation re-parse). Plan (design/allowlists/checklists):
  `C:\Users\Benni\.claude\plans\polymorphic-knitting-harbor.md`.
- Two reviewed-pending deviations from the plan: '+' decodes to SPACE (literal '+' broke 12
  ETSI Consumption geoquery/q tests — python-requests sends spaces as '+'; %2B gives literal
  '+', same as before), and RETRIEVE_CONTEXT additionally allows `type`
  (Scorpio itself appends ?type=implicitlyCreated to hosted context URLs).
  Refinement (user-approved): in scopeQ ONLY, a raw '+' stays literal — it is the scope
  single-level wildcard (grammar, not data; ParsedQueryParams.getScopeQ() decodes from the raw
  wire value). %2B → '+', %20 → space stay distinguishable. Raw '#' (multi-level wildcard) also
  works now; note curl strips '#' as a fragment — test it via raw TCP.
- ETSI state after change: CommonBehaviours 32/33 (059_01_05 + 059_01_07 flipped to PASS; only
  059_01_08 Purge Entities red — separate task). Consumption/Entity 42 fails and
  Provision/Entities 6 fails are PRE-EXISTING (identical failure sets on unmodified baseline).

## Active work state addendum (2026-07-18, branch `testfest`)

- IMPLEMENTED (uncommitted, awaiting review): NGSI-LD **Purge Entities** (spec 1.9.1 clause
  5.6.21 / 6.4.3.3, `DELETE /ngsi-ld/v1/entities`), full impl incl. distributed forwarding.
  Plan: `C:\Users\Benni\.claude\plans\foamy-dancing-dongarra.md`. Pieces:
  - Commons: `PURGE_ENTITIES` allowlist in NgsiLdOperation; `QUERY_PARAMETER_DROP/KEEP` +
    `NGSI_LD_REG_OPERATION_PURGEENTITY` constants; RegistrationEntry `purgeEntity` op boolean
    (parsed from `purgeEntity` + `redirectionOps` group); DBUtil reads it as column 56.
  - EntityManager: `EntityController.purgeEntities` (min-filter rule → 400 BadRequestData,
    drop×keep exclusion, `type=*` → implicit local, id URI validation, drop/keep IRI expansion);
    `EntityInfoDAO.queryForPurge` (ids[+attr keys] by filter, WHERE composed from Commons terms'
    toSql); `EntityService.purgeEntities` — entity mode reuses extracted `localDeleteBatch`
    (BATCH_DELETE_REQUEST + prevPayload → subscriptions deletedAt + history soft-delete for
    free), drop/keep mode loops `localDeleteAttrib` per (entity, present attr) minus system
    keys; forwarding sends the ORIGINAL request params (spec: "matching input data is
    forwarded") to registrations with `purgeEntity` op, matched in-memory via
    `RegistrationEntry.matches` + csf eval + Via-loop skip; 204/207 via generateBatchResult
    (empty result guard — it throws on empty lists).
  - Migration `V20260718.1__purgeentity.sql` in ALL 10 module migration dirs: csourceinformation
    `purgeEntity` column, `getoperations` 41→42 slots (+`purgeEntity` WHEN + redirectionOps
    grant — previously an explicit `purgeEntity` op name crashed the trigger, CASE_NOT_FOUND),
    trigger function's 5 INSERTs extended. All 6 registration SELECT column lists (2×
    EntityInfoDAO, QueryDAO, both HistoryDAOs, SubscriptionInfoDAO) appended `purgeEntity`
    (positional lockstep with DBUtil).
  - Allowlist fix found via ETSI dist-ops: ALL mutation ops on /entities, /entityOperations,
    /temporal/entities now accept `local`/`localOnly` (spec 6.3.18 mandates it "for all
    operations" there; P.NONE broke every DistributedOperations exc/inc setup with
    `POST /entities?local=true` → 400). Value is accepted-and-ignored where Scorpio has no
    distributed variant (= pre-refactor behavior).
- VERIFIED: ETSI CommonBehaviours **33/33**, Provision/PurgeEntities **8/8**, DistributedOps
  D017_01 exc/inc/red **3/3**, Provision/Entities unchanged (same 6 pre-existing fails),
  Postman gate **2724/2724** (fresh aaio stack; the collection needs a fresh stack per run),
  Commons QueryParamParserTest 26/26 (2 new purge tests). Migration verified both fresh and
  incremental; registration trigger smoke-tested with explicit `purgeEntity` and
  `redirectionOps` payloads.
- Known pre-existing (NOT purge-related, reproduced on stock batch delete): the history
  manager's buffered flush can lose the temporalentity soft-delete mark when an entity's
  create/upsert and delete land in the same ~1s flush window (parallel Unis race in
  HistoryMessagingBase.checkBuffer).
