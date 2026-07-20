---
type: NGSI-LD Clause
title: "Snapshot Behaviour"
description: "If a Snapshot (see clause 4.3.7) is specified for an NGSI-LD operation, the operation shall only be applied to information related to the specific Snapshot."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.15
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.15"
---

If a Snapshot (see clause 4.3.7) is specified for an NGSI-LD operation, the operation shall only be applied
to information related to the specific Snapshot. Operations permitted on Snapshots are the
same as those specified for the Core API and, if supported, the Temporal API
(see Table 4.3.5-1). This means all operations are implicitly limited to a local
scope with all relaxations allowing broader operations (see clause 5.5.13) and
there is no need to explicitly set the local scope as a request parameter. The
implicit limitation to local scope also means that Context Source Registrations from the Context
Registry will not be used.
Snapshots are explicitly created, and the Snapshot identifier is provided on creation. How the Snapshot
identifier is specified for an API operation is protocol binding specific.
The Snapshot concept is orthogonal to the Tenant concept (see clause 4.14). Snapshots can also be created on
Tenants. To execute an operation on a Snapshot created on a Tenant, both Snapshot and Tenant (see clause
5.5.10) have to be specified for an API operation.
If an implementation determines that it is low on resources, it may delete one or more snapshots. For determining the
order in which snapshots are to be deleted, the snapshotPriority, ranging from 1 (minimum) to 10 (maximum)
with 5 being the default priority, should be considered. An implementation may also consider other aspects like the
expiresAt, or lastUsedAt timestamp for such a decision.

# Related

* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
* [Clause 4.3.7](/framework/architecture/snapshots.md)
* [Clause 5.5.10](/api-operations/common-behaviours/multi-tenant-behaviour.md)
* [Clause 5.5.13](/api-operations/common-behaviours/limiting-operations-to-local-scope.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Snapshot Behaviour
