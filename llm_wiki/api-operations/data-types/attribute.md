---
type: NGSI-LD Data Type
title: "Attribute"
description: "This type represents the data needed to define the attribute information needed as: - part of the entity type information representation as mandated by clause 4.5.12; - the detailed attribute list rep"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.28
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.28"
---

This type represents the data needed to define the attribute information needed as:

- part of the entity type information representation as mandated by clause 4.5.12;

- the detailed attribute list representation as mandated by clause 4.5.14;

- the attribute information representation as mandated by clause 4.5.15. The supported JSON members shall follow the requirements provided in Table 5.2.28-1.

| | Name | | | Data Type | | | | Restriction | | Cardinality | | | Description | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | id | | | String | | Valid URI | | | 1 | | | Full URI of attribute name. | | |
| | type | | | String | | It shall be equal to | | | 1 | | | JSON-LD @type. | | |
| | | | | | | "Attribute" | | | | | | | | |
| | attributeName | | | String | | | | | 1 | | | Name of the attribute, short name if | | |

Table 5.2.28-1: NGSI-LD Attribute data type definition

attributeName String 1 Name of the attribute, short name if
contained in @context.
attributeCount Number Unsigned integer 0..1 Number of attribute instances with this
attribute name.
attributeTypes String[] 0..1 List of attribute types (e.g. Property,
Relationship, GeoProperty) for which
entity instances exist, which contain an
attribute with this name.
typeNames String[] 0..1 List of entity type names for which entity
instances exist containing attributes that
have the respective name.

# Related

* [Clause 4.5.12](/framework/data-representation/entity-type-information-representation.md)
* [Clause 4.5.14](/framework/data-representation/detailed-attribute-list-representation.md)
* [Clause 4.5.15](/framework/data-representation/attribute-information-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.28](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Attribute
