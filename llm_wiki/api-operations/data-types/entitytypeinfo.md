---
type: NGSI-LD Data Type
title: "EntityTypeInfo"
description: "This type represents the data needed to define the detailed entity type information representation as mandated by clause 4.5.12."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.26
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.26"
---

This type represents the data needed to define the detailed entity type information representation as mandated by clause 4.5.12. The supported JSON members shall follow the requirements provided in Table 5.2.26-1.

Table 5.2.26-1: NGSI-LD EntityTypeInfo data type definition Name Data Type Restriction Cardinality Description id String Valid URI 1 Fully Qualified Name (FQN) of the entity type being described.

type String It shall be equal to "EntityTypeInfo"

1 JSON-LD @type.

attributeDetails Attribute[] See data type definition in clause 5.2.28. Attribute with only the elements "id", "type", "attributeName" and "attributeTypes"

1 List of attributes that entity instances with the specified entity type can have.

entityCount Number Unsigned integer 1 Number of entity instances of this entity
type.
typeName String 1 Name of the entity type, short name if
contained in @context.

# Related

* [Clause 4.5.12](/framework/data-representation/entity-type-information-representation.md)
* [Clause 5.2.28](/api-operations/data-types/attribute.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.26](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — EntityTypeInfo
