---
type: NGSI-LD Clause
title: "Entity Type List Representation"
description: "The entity type list representation is used to consume information about entity types."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.10
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.10"
---

The entity type list representation is used to consume information about entity types. The entity type list representation
shall be a JSON-LD object containing the following members:
Mandatory
- "id" whose value shall be a URI that identifies the entity type list.
- "type": the fixed value "EntityTypeList".
- "typeList": JSON-LD array containing the entity type names. Optional
- "@context" a JSON-LD @context as described in clause 4.4.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity Type List Representation
