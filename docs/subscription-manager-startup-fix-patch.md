# Subscription Manager Startup Fix — Patch Summary

## Problem

On restart with many tenants and existing subscriptions, the `SubscriptionService.startup()` method called `.await().indefinitely()` inside `@PostConstruct`, blocking the Vert.x event loop thread while loading all subscriptions and registrations from all tenant databases in parallel. This caused:

- **Vert.x event loop thread blocked** for seconds to minutes with many tenants
- **PgPool connection exhaustion** — all tenant queries fired in parallel against a pool of max 20 connections
- **Health check timeouts** and Kubernetes pod restarts
- **Kafka messages processed against incomplete state** — race between subscription loading and message consumption
- **No backpressure** — unprocessed Kafka entity messages flooded notification delivery immediately after startup

## Files Changed

### 1. `SubscriptionService.java` (core fix)

**Path:** `SubscriptionManager/src/main/java/.../service/SubscriptionService.java`

| Change | Detail |
|--------|--------|
| Removed `.await().indefinitely()` | Replaced with non-blocking `.subscribe().with(...)` in `@PostConstruct`. The Vert.x event loop thread is no longer blocked during startup. |
| Added `volatile boolean ready` flag | Set to `true` only after all subscriptions and registrations are loaded from all tenants. |
| Added startup message buffering | `handleBaseRequest()` and `handleRegistryChange()` buffer incoming Kafka messages in `ConcurrentLinkedQueue` instances while `ready == false`, instead of dropping or processing them against incomplete state. |
| Added `drainStartupBuffers()` | Once `ready` becomes `true`, all buffered entity and csource messages are drained and processed in order. |
| Guarded `checkIntervalSubs()` | The scheduled method returns immediately if not yet ready. |
| Added `isReady()` accessor | Used by the new readiness health check. |

### 2. `SubscriptionInfoDAO.java` (DB pool fix)

**Path:** `SubscriptionManager/src/main/java/.../repository/SubscriptionInfoDAO.java`

| Change | Detail |
|--------|--------|
| Sequential tenant loading | `loadSubscriptions()` now uses `Multi.createFrom().iterable(tenantIds).onItem().transformToUniAndConcatenate(...)` so tenant DB queries execute one at a time, preventing PgPool exhaustion (max-size=20) with many tenants. |
| Per-tenant error recovery | A failed tenant query logs the error and returns an empty list instead of failing the entire startup. |

### 3. `SubscriptionReadinessCheck.java` (new file)

**Path:** `SubscriptionManager/src/main/java/.../service/SubscriptionReadinessCheck.java`

| Change | Detail |
|--------|--------|
| Quarkus `@Readiness` health check | Reports the application as NOT ready until all subscriptions and registrations are fully loaded. Kubernetes/orchestrators will not route traffic until `/q/health/ready` returns UP. |

### 4. `pom.xml` (dependency)

**Path:** `SubscriptionManager/pom.xml`

| Change | Detail |
|--------|--------|
| Added `quarkus-smallrye-health` | Required for the `@Readiness` health check annotation. Version managed by BrokerParent. |

## Risks Addressed

| Risk | Mitigation |
|------|-----------|
| Vert.x event loop blocked during startup | `@PostConstruct` no longer blocks — uses async `.subscribe()` |
| PgPool exhaustion with many tenants | Tenant queries execute sequentially via `Multi.transformToUniAndConcatenate` |
| Kafka messages processed before subscriptions loaded | `ready` flag gates `handleBaseRequest()` and `handleRegistryChange()`; messages buffered and drained after init |
| Interval subs firing before ready | `checkIntervalSubs()` guarded by `ready` flag |
| No readiness signal to orchestrator | `SubscriptionReadinessCheck` exposes `/q/health/ready` |
| Single tenant failure crashes entire startup | Per-tenant `.onFailure().recoverWithItem()` in DAO |
