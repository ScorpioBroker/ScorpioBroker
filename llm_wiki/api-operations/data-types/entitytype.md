---
type: NGSI-LD Data Type
title: "EntityType"
description: "This type represents the data needed to define the elements of the detailed entity type list representation as mandated by clause 4.5.11."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.25
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.25"
---

This type represents the data needed to define the elements of the detailed entity type list representation as mandated by clause 4.5.11. The supported JSON members shall follow the requirements provided in Table 5.2.25-1.


Table 5.2.25-1: NGSI-LD EntityType data type definition Name Data Type Restriction Cardinality Description id String Valid URI 1 Fully Qualified Name (FQN) of the entity type being described.

type String It shall be equal to "EntityType"

1 JSON-LD @type.

attributeNames String[] 1 List containing the names of attributes
that instances of the entity type can
have.
typeName String 1 Name of the entity type, short name if
contained in @context.

# Related

* [Clause 4.5.11](/framework/data-representation/detailed-entity-type-list-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.25](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — EntityType
