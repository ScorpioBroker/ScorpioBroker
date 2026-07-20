---
type: NGSI-LD Operation
title: "Retrieve Details of Available Entity Types"
description: "5.7.6.1 Description This operation allows retrieving a list with a detailed representation of NGSI-LD entity types for which entity instances exist within the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.6"
---

5.7.6.1 Description

This operation allows retrieving a list with a detailed representation of NGSI-LD entity types for which entity instances exist within the NGSI-LD system. The detailed representation includes the type name (as short name if available in the provided @context) and the attribute names that existing instances of this entity type have.

5.7.6.2 Use case diagram

A Context Consumer can retrieve a list with a detailed representation of NGSI-LD entity types from the system as shown in Figure 5.7.6.2-1.


Figure 5.7.6.2-1: Retrieve Details of Available Entity Types use case

5.7.6.3 Input data - An optional JSON-LD context.

5.7.6.4 Behaviour

- Return a list of JSON-LD objects representing the details of available entity types as mandated by clause 5.2.25 for which entity instances exist within the NGSI-LD system. See clause 5.7.11 for architecture-related implementation aspects, in particular the distributed behaviour.

5.7.6.5 Output data

A list of JSON-LD objects representing the details of available entity types as mandated by clause 5.2.25.

# Related

* [HTTP: Resource: types/](/http-binding/resource-types.md)
* [Clause 5.2.25](/api-operations/data-types/entitytype.md)
* [Clause 5.7.11](/api-operations/consumption/architecture-related-aspects-of-retrieval-of-entity-types-and-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Details of Available Entity Types
