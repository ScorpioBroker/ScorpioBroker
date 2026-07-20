---
type: NGSI-LD Operation
title: "Retrieve Available Entity Type Information"
description: "5.7.7.1 Description This operation allows retrieving detailed entity type information about a specified NGSI-LD entity type for which entity instances exist within the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.7"
---

5.7.7.1 Description

This operation allows retrieving detailed entity type information about a specified NGSI-LD entity type for which entity instances exist within the NGSI-LD system. The detailed representation includes the type name (as short name if available in the provided @context), the count of available entity instances and details about attributes that existing instances of this entity type have, including their name (as short name if available in the provided @context) and a list of types the attribute can have (e.g. Property, Relationship or GeoProperty).

5.7.7.2 Use case diagram

A Context Consumer can retrieve a detailed representation of a specified NGSI-LD entity type from the system as shown in Figure 5.7.7.2-1.

Context Consumer

Retrieve Details of Available Entity Types

NGSI-LD Client

NGSI-LD System

Response with list of entity type details

1..*

EntityType Array

retrieve details of available entity types


Figure 5.7.7.2-1: Retrieve Available Entity Type Information use case

5.7.7.3 Input data

- Entity type name for which detailed information is to be retrieved.
- An optional JSON-LD context.

5.7.7.4 Behaviour

- Return a JSON-LD object representing the details of the specified entity type as mandated by clause 5.2.26, for which instances exist within the NGSI-LD system. See clause 5.7.11 for architecture-related implementation aspects.

5.7.7.5 Output data

A JSON-LD object representing the details of the specified entity type as mandated by clause 5.2.26.

# Related

* [HTTP: Resource: types/{type}](/http-binding/resource-types-type.md)
* [Clause 5.2.26](/api-operations/data-types/entitytypeinfo.md)
* [Clause 5.7.11](/api-operations/consumption/architecture-related-aspects-of-retrieval-of-entity-types-and-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Available Entity Type Information
