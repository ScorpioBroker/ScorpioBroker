---
type: NGSI-LD Clause
title: "Distributed Transactional Behaviour"
description: "The following operations may occur as part of a distributed transactional request: - Retrieve Entity (clause 5.7.1)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.14
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.14"
---

The following operations may occur as part of a distributed transactional request:

- Retrieve Entity (clause 5.7.1).
- Query Entities (clause 5.7.2).
- Retrieve Temporal Evolution of an Entity (clause 5.7.3).
- Query Temporal Evolution of Entities (clause 5.7.4). In the case that a client wishes to indicate to a Context Broker that a request is part of a unitary sequence of requests, the Context Broker shall create and cache an EntityMap for future use and should return the location of said EntityMap in a specific field. The caching strategy and expiry time shall take into account a suggested expiry time, if present, and depend on implementation specific configurations. Context Sources should indicate that they do not support EntityMaps, through declining to return the location of an EntityMap when requested to do so. If a subsequent request references an existent EntityMap, it shall be used for the purposes of Entity registration matching, and queries shall be filtered to only consider the Entities listed. Subsequent requests referencing an EntityMap shall use the same parameters as in the original request that created the EntityMap, except for the specification of Entity identifiers or parameters related to pagination, or, in the case of temporal requests, the temporal query. If an EntityMap has expired, or cannot be accessed, no inference can be made as to which entities are held within the Context Sources and a new one shall be created. An EntityMap fixes the Entities to be considered for subsequent requests based on it. The creating Context Source shall remove Entities from the EntityMap that do not match the filters of the query at the time of processing. Other components shall only be allowed to update the expiry timestamp of the EntityMap, which can optionally be extended if the Context Sources implementation allows for it.

# Related

* [Clause 5.7.1](/api-operations/consumption/retrieve-entity.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
* [Clause 5.7.3](/api-operations/consumption/retrieve-temporal-evolution-of-an-entity.md)
* [Clause 5.7.4](/api-operations/consumption/query-temporal-evolution-of-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Distributed Transactional Behaviour
