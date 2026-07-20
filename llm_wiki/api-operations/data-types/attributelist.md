---
type: NGSI-LD Data Type
title: "AttributeList"
description: "This type represents the data needed to define the attribute list representation as mandated by clause 4.5.13."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.27
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.27"
---

This type represents the data needed to define the attribute list representation as mandated by clause 4.5.13.
The supported JSON members shall follow the requirements provided in Table 5.2.27-1.

Table 5.2.27-1: NGSI-LD AttributeList data type definition Name Data Type Restriction Cardinality Description id String Valid URI 1 URI that is unique within the system scope. Identifier for the attribute list.

type String It shall be equal to "AttributeList"

1 JSON-LD @type.

attributeList String[] 1 List containing the attribute names.

# Related

* [Clause 4.5.13](/framework/data-representation/attribute-list-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.27](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — AttributeList
