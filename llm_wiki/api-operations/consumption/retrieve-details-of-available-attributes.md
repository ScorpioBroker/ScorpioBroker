---
type: NGSI-LD Operation
title: "Retrieve Details of Available Attributes"
description: "5.7.9.1 Description This operation allows retrieving a list with a detailed representation of NGSI-LD attributes that belong to entity instances existing within the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.9
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.9"
---

5.7.9.1 Description

This operation allows retrieving a list with a detailed representation of NGSI-LD attributes that belong to entity instances existing within the NGSI-LD system. The detailed representation includes the attribute name (as short name if available in the provided @context) and the type names for which entity instances exist that have the respective attribute.

5.7.9.2 Use case diagram

A context consumer can retrieve a list with a detailed representation of NGSI-LD attributes from the system as shown in Figure 5.7.9.2-1.


Figure 5.7.9.2-1: Retrieve Details of Available Attributes use case

5.7.9.3 Input data

An optional JSON-LD context.

5.7.9.4 Behaviour

Return a list of JSON-LD objects representing the details of available attributes as mandated by clause 5.2.28 (restricted to the elements id, type, attributeName and typeNames) that belong to entity instances existing within the NGSI-LD system. See clause 5.7.11 for architecture-related implementation aspects, in particular the distributed behaviour.

5.7.9.5 Output data

A list of JSON-LD objects representing the details of available attributes as mandated by clause 5.2.28 (restricted to the elements id, type, attributeName and typeNames).

# Related

* [HTTP: Resource: attributes/](/http-binding/resource-attributes.md)
* [Clause 5.2.28](/api-operations/data-types/attribute.md)
* [Clause 5.7.11](/api-operations/consumption/architecture-related-aspects-of-retrieval-of-entity-types-and-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Details of Available Attributes
