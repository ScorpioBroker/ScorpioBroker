---
type: NGSI-LD Operation
title: "Architecture-related aspects of retrieval of Entity Types and Attributes"
description: "Retrieving information about available types or attributes can be an expensive operation depending on the scale and architectural design decisions of the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.11
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.11"
---

Retrieving information about available types or attributes can be an expensive operation depending on the scale and
architectural design decisions of the NGSI-LD system. This is in particular the case for retrieving the information about
all available entity types and attributes related to all entity information available in an NGSI-LD system. Especially in
the case of distributed architecture (clause 4.3.3) and federated architecture (clause 4.3.4) checking all entities can be so
expensive that it can become practically infeasible.
Therefore, implementations may only take into account only information that is available or can be derived from a local
datastore and the Context Registry, when implementing the retrieval of available entity types and attributes, as
described in clauses 5.7.5, 5.7.6, 5.7.7, 5.7.8, 5.7.9 and 5.7.10. Context registrations do not always reflect which entity
instances are actually available from a Context Source at a particular point in time, but only which entity instances
are possibly available from a Context Source, thus in this case the information about available entity types and
attributes is to be interpreted as "possibly available". Also, context registrations can have different granularities, i.e.
they possibly only contain entity type or attribute information, and thus the provided information about available entity
types and attributes is possibly incomplete as a result. In particular the attributeNames in the EntityType data structure
(clause 5.2.25), the attributeDetails in the EntityTypeInfo data structure (clause 5.2.26), and the attributeTypes and
typeNames in the Attribute data structure (clause 5.2.27) may be provided as empty arrays if the information is not
included in the respective context registration. Implementations may also provide estimates for the entity count or
attribute count instead of the accurate count.


As an alternative to relying on local information only, the request can be forwarded to all Context Sources which support the respective operation according to the Context Source Registration describing them. In this case the returned lists are merged with the local list of entity types before returning them. This approach is more expensive but leads to a more accurate result.

# Related

* [Clause 4.3.3](/framework/architecture/distributed-architecture.md)
* [Clause 4.3.4](/framework/architecture/federated-architecture.md)
* [Clause 5.2.25](/api-operations/data-types/entitytype.md)
* [Clause 5.2.26](/api-operations/data-types/entitytypeinfo.md)
* [Clause 5.2.27](/api-operations/data-types/attributelist.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Architecture-related aspects of retrieval of Entity Types and Attributes
