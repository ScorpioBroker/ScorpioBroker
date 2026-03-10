# Subscription Manager — Startup Sequence Analysis

## Focus: Restart Scenarios with Existing Subscriptions & Unprocessed Kafka Entities in Multi-Tenant Setup

---

## 1. Startup Sequence Overview

The Subscription Manager startup proceeds through these phases on the Vert.x event loop / Quarkus CDI initialization thread:

### Phase A — CDI Bean Construction & `@PostConstruct` chains (blocking the startup thread)

| Order | Bean | What Happens |
|-------|------|--------------|
| 1 | `ConnectionManager.setup()` | Runs `ALTER USER ... SET jit = off` **synchronously** via `.await().indefinitely()`. Initializes the default PgPool and stores it in `tenant2Client`. |
| 2 | `MicroServiceUtils.startup()` | Resolves gateway URL, sets internal state (source alias, etc.). |
| 3 | **`SubscriptionService.startup()`** | **The critical one** — loads all subscriptions and registrations from all tenants, blocks with `.await().indefinitely()`, then registers itself as a BaseRequest/CSource receiver. See detailed breakdown below. |
| 4 | `SubscriptionSyncServiceString.startup()` *(Kafka profile)* | Calls `subService.addSyncService(this)`, creates a 2-thread executor, and **immediately publishes an `AliveAnnouncement`** to the Kafka `SUB_ALIVE` topic. |

### Phase B — Smallrye Reactive Messaging Connector Startup

After all `@PostConstruct` methods complete, Quarkus starts the Smallrye Reactive Messaging connectors. The Kafka consumers for channels `entityretrieve`, `registryretrieve`, `subaliveretrieve`, and `subsyncretrieve` begin polling. From this point, incoming entity/registry/sync messages flow into the `@Incoming`-annotated methods.

### Phase C — HTTP Server Opens

Quarkus opens the HTTP port (default: 10092) and the application is considered "ready."

---

## 2. Detailed `SubscriptionService.startup()` Breakdown

**File:** `SubscriptionManager/src/main/java/.../service/SubscriptionService.java` (lines 372–453)

```java
@PostConstruct
void startup() {
    this.webClient = WebClient.create(vertx);
    ALL_TYPES_SUB = NGSIConstants.NGSI_LD_DEFAULT_PREFIX + allTypeSubType;

    // Step 1: Load ALL subscriptions from ALL tenants
    Uni<Void> loadSubs = subDAO.loadSubscriptions().onItem().transformToUni(subs -> {
        // For each sub: parse JSON-LD context, construct SubscriptionRequest,
        // put into tenant2subscriptionId2Subscription or
        // tenant2subscriptionId2IntervalSubscription tables
        // request.setSendTimestamp(-1);  ← marks "accept all messages regardless of time"
    });

    // Step 2: Load ALL registration entries from ALL tenants
    Uni<Void> loadRegs = subDAO.getAllRegistries().onItem().transformToUni(regs -> {
        // Populates queryTenant2CId2RegEntries and subscriptionTenant2CId2RegEntries
    });

    // *** BLOCKING CALL - blocks the startup thread ***
    Uni.combine().all().unis(loadSubs, loadRegs).with(l -> l).await().indefinitely();

    // Step 3: Register as message handler
    this.microServiceUtils.registerBaseRequestReceiver(this);
    this.microServiceUtils.registerCSourceReceiver(this);
}
```

### Multi-Tenant Loading Detail (`subDAO.loadSubscriptions()`)

**File:** `SubscriptionManager/src/main/java/.../repository/SubscriptionInfoDAO.java` (lines ~222–270)

1. Queries `SELECT tenant_id FROM tenant` on the **master DB's** tenant table
2. For **each tenant**, issues a separate query:
   ```sql
   SELECT '<tenant_id>', subscriptions.subscription, context as contextId,
          contexts.body as contextBody
   FROM subscriptions LEFT JOIN contexts ON subscriptions.context = contexts.id
   ```
3. Also queries the default/internal tenant (`INTERNAL_NULL_KEY`)
4. Uses `Uni.combine().all().unis(unis)` to parallelize all tenant queries, then aggregates results

**`getAllRegistries()`** similarly queries `csourceinformation` across all tenants via `DBUtil.getAllRegistries()`.

Both are combined and **blocked on with `.await().indefinitely()`** — this keeps the thread blocked until all DB queries for all tenants complete.

---

## 3. Kafka Consumer Configuration (Kafka Profile)

**File:** `SubscriptionManager/src/main/resources/application-kafka.properties`

| Channel | Topic | Offset Reset | Broadcast | Purpose |
|---------|-------|-------------|-----------|---------|
| `entityretrieve` | `ENTITY` | `latest` | `true` | Entity change events |
| `registryretrieve` | `REGISTRY` | `latest` | `true` | Registration change events |
| `subaliveretrieve` | `SUB_ALIVE` | `latest` | `true` | Sync: instance alive announcements |
| `subsyncretrieve` | `SUB_SYNC` | `latest` | `true` | Sync: subscription CRUD sync messages |

- **Acknowledgment strategy:** `PRE_PROCESSING` — Kafka offsets are committed **before** the message is fully processed.
- **Consumer group:** `group.id=$[quarkus.application.name}$[quarkus.uuid}` — uses a random UUID, meaning a **new consumer group** is created on each restart. With `auto.offset.reset=latest`, old messages are skipped.
  - **Exception:** If production overrides `group.id` to a stable value (common for multi-instance sync), the consumer **will** read unprocessed messages from the last committed offset.

### Message Flow After Consumer Starts

```
Kafka ENTITY topic
  → SubscriptionMessagingString.handleEntity(String)            [@Incoming, PRE_PROCESSING]
    → SubscriptionMessagingBase.handleEntityRaw(String)
      → objectMapper.readValue(byteMessage, BaseRequest.class)
      → SubscriptionService.handleBaseRequest(BaseRequest)
        → iterate tenant2subscriptionId2Subscription for matching tenant
        → checkSubscriptions(message, potentialSubs)
          → for each matching sub: sendNotification(...)
            → webClient.postAbs(...) or mqttClient.publish(...)
```

---

## 4. Restart Risks — Blocked Async Vert.x Threads

### RISK 1: `.await().indefinitely()` in `@PostConstruct` blocks the Vert.x event loop thread

**Severity:** CRITICAL

The `startup()` method calls:
```java
Uni.combine().all().unis(loadSubs, loadRegs).with(l -> l).await().indefinitely();
```

In Quarkus with Vert.x, `@PostConstruct` methods of `@ApplicationScoped @Startup` beans run during application bootstrap. If this runs on a Vert.x event loop thread, it **blocks that thread indefinitely** while waiting for:
- N+1 database queries (one per tenant + default) for subscriptions
- M+1 database queries for registrations
- JSON-LD context parsing (`ldService.parsePure()`) for each subscription

**Impact with many tenants (e.g., 50+) and many subscriptions (e.g., thousands):**
- **Exhausts the reactive PgPool** (max-size=20) because all tenant queries fire in parallel
- **Blocks the event loop for seconds to minutes**, triggering Vert.x's "blocked thread checker" warnings
- **Prevents the HTTP server from starting**, causing health check failures
- If the PgPool connections are all used by `loadSubscriptions` queries and one needs another connection (e.g., for context resolution), a **deadlock** occurs

### RISK 2: Kafka consumers start BEFORE subscriptions are fully loaded

**Severity:** HIGH

- `SubscriptionMessagingString` has `@Startup` — bean is eagerly initialized
- Kafka consumers are started by Smallrye **after** all `@PostConstruct` methods complete
- With a **stable consumer group** in production, the consumer reads unprocessed messages from the last committed offset
- `handleBaseRequest()` iterates `tenant2subscriptionId2Subscription` — if still being populated during a race, some subscriptions may be missed
- The `.await().indefinitely()` in `@PostConstruct` _should_ prevent this for initial load, but sync service and registry loading create windows of inconsistent state

### RISK 3: Notification delivery during startup floods outbound connections

**Severity:** MEDIUM-HIGH

When entity messages from Kafka are processed, each matching subscription triggers notification delivery. During restart with a Kafka backlog:
- `webClient` tries to POST to all notification endpoints simultaneously
- Slow/unresponsive endpoints consume **Vert.x event loop time** and **Netty connection pool**
- HTTP notification includes `.onFailure().retry().atMost(3)` — **tripling load** under failure
- **No backpressure:** `PRE_PROCESSING` ack means Kafka offsets are committed before processing completes — messages won't be redelivered but the processing pipeline gets overwhelmed

### RISK 4: `synchronized(tableLock)` contention between startup and message processing

**Severity:** MEDIUM

The `tableLock` object is used pervasively:
- **During startup:** `loadSubs` and `loadRegs` acquire `tableLock` for each subscription/registration insert
- **During message processing:** `handleBaseRequest` copies subscription list under `tableLock`
- **During sync:** `syncUpdateSubscription` and `syncDeleteSubscription` read/write under `tableLock`

If Kafka message processing starts while startup is still populating tables, or if sync messages arrive immediately after `@PostConstruct`, there's **contention on `tableLock`** causing latency spikes and, worst case, contributing to event loop blocking.

### RISK 5: Sync service publishes alive announcement before Kafka consumers exist

**Severity:** LOW-MEDIUM

`SubscriptionSyncServiceString.startup()` immediately emits to the `subalive` Kafka topic:
```java
microServiceUtils.serializeAndSplitObjectAndEmit(INSTANCE_ID, messageSize, aliveEmitter, objectMapper);
```
But the `subaliveretrieve` consumer hasn't started yet. Other instances may try to interact with this instance before it can receive sync messages, leading to missed sync coordination during the startup window.

---

## 5. Blocked Thread Scenarios Summary

| Scenario | Root Cause | Symptom |
|----------|-----------|---------|
| Startup with 50+ tenants | `loadSubscriptions()` fires 50+ DB queries in parallel under `.await().indefinitely()`, but PgPool max-size=20 | Vert.x "Thread blocked" warnings; health check timeouts; Kubernetes restarts the pod |
| Large subscription count per tenant | JSON-LD `parsePure()` for each subscription is CPU-bound; all executed synchronously in `Uni.combine().all()` | Prolonged `@PostConstruct`, delayed HTTP port opening |
| Kafka backlog after restart | `handleBaseRequest()` processes rapidly; each triggers `sendNotification()` → `webClient.postAbs()` → potentially slow HTTP target; `.onFailure().retry().atMost(3)` amplifies | Event loop saturated with outbound HTTP requests |
| DB connection pool exhaustion | loadSubs fires N parallel queries + loadRegs fires M parallel queries against PgPool(maxSize=20) | Connection timeout → `await().indefinitely()` hangs forever → startup never completes |

---

## 6. Proposed Improvements

### Improvement 1: Defer Kafka Consumption Until Startup Is Complete

Use Smallrye Reactive Messaging's lazy channel initialization:

```properties
# application-kafka.properties
mp.messaging.incoming.entityretrieve.lazy=true
mp.messaging.incoming.registryretrieve.lazy=true
```

Then start channels programmatically after initialization, or use a `StartupEvent` observer:

```java
void onStart(@Observes StartupEvent ev) {
    // Start Kafka consumers only after full initialization
}
```

### Improvement 2: Replace `.await().indefinitely()` with Non-Blocking Startup

Instead of blocking in `@PostConstruct`, use a readiness-gated approach:

```java
private volatile boolean ready = false;

@PostConstruct
void startup() {
    this.webClient = WebClient.create(vertx);
    ALL_TYPES_SUB = NGSIConstants.NGSI_LD_DEFAULT_PREFIX + allTypeSubType;

    Uni.combine().all().unis(loadSubs, loadRegs).with(l -> l)
        .subscribe().with(
            result -> {
                this.microServiceUtils.registerBaseRequestReceiver(this);
                this.microServiceUtils.registerCSourceReceiver(this);
                this.ready = true;
                logger.info("Subscription manager fully initialized");
            },
            failure -> logger.error("Failed to initialize subscription manager", failure)
        );
}

public Uni<Void> handleBaseRequest(BaseRequest message) {
    if (!ready) {
        return Uni.createFrom().voidItem(); // silently skip
    }
    // ... existing logic
}
```

Add a Quarkus readiness health check:
```java
@Readiness
@ApplicationScoped
public class SubscriptionReadinessCheck implements HealthCheck {
    @Inject SubscriptionService subService;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("subscriptions-loaded")
            .status(subService.isReady()).build();
    }
}
```

### Improvement 3: Batch Tenant Loading with Connection Pool Awareness

Instead of firing all tenant queries in parallel (which can exceed PgPool's max-size of 20), use Mutiny's `Multi` with concurrency control:

```java
Multi.createFrom().iterable(tenantIds)
    .onItem().transformToUniAndMerge(tenantId -> loadForTenant(tenantId))
    .collect().asList()
    // ...
```

Or process in batches of configurable size (e.g., 10).

### Improvement 4: Use `POST_PROCESSING` Acknowledgment for Entity Messages

Replace:
```java
@Acknowledgment(Strategy.PRE_PROCESSING)
```
With:
```java
@Acknowledgment(Strategy.POST_PROCESSING)
```

This ensures Kafka offsets are committed **after** the notification is actually sent. With `PRE_PROCESSING`, if the app crashes mid-notification, those messages are lost. With `POST_PROCESSING`, they'll be redelivered on restart (at the cost of potential duplicate notifications, which is preferable to lost ones).

### Improvement 5: Add Startup-Phase Message Buffering

Buffer incoming Kafka messages in memory during the startup phase, then process them once initialization is complete:

```java
private final Queue<BaseRequest> startupBuffer = new ConcurrentLinkedQueue<>();

public Uni<Void> handleBaseRequest(BaseRequest message) {
    if (!ready) {
        startupBuffer.offer(message);
        return Uni.createFrom().voidItem();
    }
    // ... normal processing
}

// After ready=true, drain the buffer
private void drainStartupBuffer() {
    BaseRequest msg;
    while ((msg = startupBuffer.poll()) != null) {
        handleBaseRequest(msg).subscribe().with(v -> {}, e -> logger.error("drain error", e));
    }
}
```

### Improvement 6: Rate-Limit Notifications During Post-Restart Catchup

Add a configurable concurrency limiter for outbound notification delivery to prevent flooding targets when processing a Kafka backlog:

```java
private final Semaphore notificationLimiter = new Semaphore(MAX_CONCURRENT_NOTIFICATIONS);
```

---

## 7. Key Configuration Properties

| Property | Default | File | Impact |
|----------|---------|------|--------|
| `scorpio.startupdelay` | `5s` | `application.properties` | Delay before scheduled tasks (interval subs, sync) begin |
| `scorpio.subscription.checkinterval` | `2s` | `application.properties` | How often interval subscriptions are checked |
| `quarkus.vertx.event-loops-pool-size` | `20` | `application.properties` | Number of Vert.x event loop threads |
| `quarkus.datasource.reactive.max-size` | `20` | `application.properties` | Max reactive PgPool connections |
| `auto.offset.reset` | `latest` | `application-kafka.properties` | Kafka consumer offset reset policy |
| `scorpio.sync.announcement-time` | `30s` | `application.properties` | How often alive announcements are sent |
| `scorpio.sync.check-time` | `90s` | `application.properties` | How often sync check runs |
| `pool.minsize` / `pool.maxsize` / `pool.initialSize` | `5` / `20` / `10` | Commons `application.properties` | JDBC connection pool sizing |

---

## 8. Key Source Files

| File | Role |
|------|------|
| `SubscriptionManager/.../service/SubscriptionService.java` | Core service — startup, subscription matching, notification delivery |
| `SubscriptionManager/.../messaging/SubscriptionMessagingBase.java` | Base class for Kafka message handling |
| `SubscriptionManager/.../messaging/SubscriptionMessagingString.java` | Kafka/SQS/AMQP profile message consumer |
| `SubscriptionManager/.../messaging/SubscriptionSyncServiceString.java` | Multi-instance subscription sync (Kafka profile) |
| `SubscriptionManager/.../repository/SubscriptionInfoDAO.java` | Database access — subscription/registration loading |
| `Commons/.../storage/ConnectionManager.java` | Multi-tenant DB connection management |
| `Commons/.../tools/MicroServiceUtils.java` | Shared utilities, handler registration |
