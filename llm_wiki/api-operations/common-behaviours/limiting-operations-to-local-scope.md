---
type: NGSI-LD Clause
title: "Limiting operations to local scope"
description: "The API provides a binding-specific mechanism to limit the execution of operations to a local scope, i.e."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.13
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.13"
---

The API provides a binding-specific mechanism to limit the execution of operations to a local scope, i.e. to only execute it based on information available in the Context Source or Context Broker directly targeted by the request. This enables limiting cascading distributed operations (clause 4.3.6.4) and, in some cases, also enables broader local operations, e.g. pertaining to all entities, which would be too expensive in the distributed case. The localOnly member in the Subscription (clause 5.5.12) requests that the subscription only matches Entities stored locally.

The localOnly member in the RegistrationManagementInfo (clause 5.2.34) of a Context Source Registration request that, when contacting the registered Context Source, a local scope shall be specified.


Thus, the registered Context Source only provides its local information, not information from further Context
Sources in its own Context Registry.
If the request limits the execution of the operation to the local scope, Context Source Registrations from the
Context Registry will not be used.
Operations on a Snapshot (see clause 5.5.15) are always implicitly local scope, overriding setting a local scope to false,
i.e. such a setting is to be ignored by implementations.

# Related

* [Clause 4.3.6.4](/framework/architecture/limiting-cascading-distributed-operations.md)
* [Clause 5.2.34](/api-operations/data-types/registrationmanagementinfo.md)
* [Clause 5.5.12](/api-operations/common-behaviours/merge-patch-behaviour.md)
* [Clause 5.5.15](/api-operations/common-behaviours/snapshot-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Limiting operations to local scope
