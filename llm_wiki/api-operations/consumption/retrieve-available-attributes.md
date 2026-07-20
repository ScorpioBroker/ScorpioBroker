---
type: NGSI-LD Operation
title: "Retrieve Available Attributes"
description: "5.7.8.1 Description This operation allows retrieving a list of NGSI-LD attributes that belong to entity instances existing within the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.8
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.8"
---

5.7.8.1 Description

This operation allows retrieving a list of NGSI-LD attributes that belong to entity instances existing within the NGSI-LD system.

5.7.8.2 Use case diagram

A Context Consumer can retrieve a list of NGSI-LD attributes from the system as shown in Figure 5.7.8.2-1.


Figure 5.7.8.2-1: Retrieve Available Attributes use case

5.7.8.3 Input data - An optional JSON-LD context.

5.7.8.4 Behaviour

- Return a JSON-LD object representing the list of attributes as mandated by clause 5.2.27 that belong to entity instances existing within the NGSI-LD system. See clause 5.7.11 for architecture-related implementation aspects, in particular the distributed behaviour.

5.7.8.5 Output data

A JSON-LD object representing the list of available attributes as mandated by clause 5.2.27.

# Related

* [HTTP: Resource: attributes/](/http-binding/resource-attributes.md)
* [Clause 5.2.27](/api-operations/data-types/attributelist.md)
* [Clause 5.7.11](/api-operations/consumption/architecture-related-aspects-of-retrieval-of-entity-types-and-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Available Attributes
