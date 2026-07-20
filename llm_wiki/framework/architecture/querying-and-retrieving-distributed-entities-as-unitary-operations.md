---
type: NGSI-LD Clause
title: "Querying and Retrieving Distributed Entities as Unitary Operations"
description: "Context Broker architectures assume that Entity data does not need to be centralized within a single Context Broker, however, when querying context information, Entity data retrieval can be considered"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.7
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.7"
---

Context Broker architectures assume that Entity data does not need to be centralized within a single Context
Broker, however, when querying context information, Entity data retrieval can be considered as a unitary operation,
masking the fact that each registered Context Broker is receiving a separate distributed Context Consumption
request.
To process each Context Consumption request efficiently, and to support consistent pagination, it is necessary for the
Context Broker to initially make a broad request to each registered Context Source whose registration is
matching the request.
In the case of a query Entities operation (clause 5.7.2) or query Temporal Evolution of Entities operation
(clause 5.7.4), a list of Entity identifiers is returned and stored together with the registration information in an Entity
map. Only the Entities whose identifiers are contained in the Entity map are considered when rendering the result pages.
Filtering based on queries, geoqueries, scope queries or attribute filters, as provided in the original query request, is
applied to the Entities before adding the Entity to a result page, i.e. identifiers of Entities not matching the filters at the
time of checking are removed from the Entity map.
In the case of a retrieve Entity operation (clause 5.7.1) or retrieve Temporal Evolution of an Entity operation
(clause 5.7.3), an Entity map can be used to make subsequent retrievals of the same Entity more efficient, as the Entity
map provides the information about which Context Source(s) store relevant Entity information, and other Context
Sources do not have to be considered.
This Entity mapping is an internal operation, not usually exposed to the end user, however it is necessary to explicitly
define a consistent mechanism for Entity map creation, caching and retrieval.
A specific field pointing to the location of a cached EntityMap (e.g. a custom header in the response in case of HTTP
binding, see Table 6.4.3.2-2) shall be returned within the response of a query, whenever this is requested by the client.
Similarly, the reuse of a previously created EntityMap can be requested by passing the same specific field into a
request.
Since an exclusive Context Source Registration already specifies that all context data is held in a single
location, its relevance to a distributed query can be inferred within the Registered Context Source without the use of an
EntityMap.

When executing Context Consumption or Subscription operations, a significant optimization in performance can be
achieved if it is known a priori whether individual Entities are themselves distributed among Context Brokers and
Context Sources, or if each Context Broker and Context Source always stores complete Entities. In the
latter case all parameters used for filtering such as queries, geoqueries, scope queries or attribute filters can be
forwarded and applied locally, whereas in the former case, the Entity first has to be assembled by the Context
Broker and only then the filtering can be applied. Since being able to apply filters locally is significantly more
efficient, a parameter can indicate that, for the given request, only Entities are to be expected that are stored in their
entirety on each Context Broker and each Context Sources, and there are no Entities that are themselves
stored in a distributed fashion. Such Entities are also referred to as split Entities. Context Broker implementations
should enable configuring a default for this parameter, so deployments where no split Entities are to be expected can
filter locally and thus be more efficient.
In the case of split Entities, EntityMaps initially only store "candidate Entities" as no filters could be applied, because
only a part of the Entity was available. In the process of pagination, the filters will be (re-)checked. Any Entity not (or
no longer) fulfilling the filter shall be removed from the EntityMap.

# Related

* [Clause 5.7.1](/api-operations/consumption/retrieve-entity.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
* [Clause 5.7.3](/api-operations/consumption/retrieve-temporal-evolution-of-an-entity.md)
* [Clause 5.7.4](/api-operations/consumption/query-temporal-evolution-of-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Querying and Retrieving Distributed Entities as Unitary Operations
