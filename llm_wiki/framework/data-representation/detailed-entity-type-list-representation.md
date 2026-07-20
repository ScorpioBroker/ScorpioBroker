---
type: NGSI-LD Clause
title: "Detailed Entity Type List Representation"
description: "The detailed entity type list representation is used to consume detailed information about entity types including the names of attributes that instances of each entity type can have."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.11
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.11"
---

The detailed entity type list representation is used to consume detailed information about entity types including the
names of attributes that instances of each entity type can have. The detailed entity type list representation shall be an
array of JSON-LD objects containing the following members:
Mandatory
- "attributeNames": JSON-LD array containing the names of attributes that instances of the entity type can have.
- "id" whose value shall be the URI that identifies the entity type.


- "type": the fixed value "EntityType".
- "typeName": name of entity type, short name if contained in @context. Optional
- "@context" a JSON-LD @context as described in clause 4.4.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Detailed Entity Type List Representation
