---
type: NGSI-LD Data Type
title: "EntityInfo"
description: "This type represents what Entities, Entity Types or group of Entity IDs (as a regular expression pattern mandated by IEEE 1003.2™ [11]) can be provided (by Context Sources)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.8
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.8"
---

This type represents what Entities, Entity Types or group of Entity IDs (as a regular expression pattern mandated by IEEE 1003.2™ [11]) can be provided (by Context Sources).

The JSON members shall follow the indications provided in Table 5.2.8-1. id takes precedence over idPattern. Notice that Cardinality of type being 1 implies that it is not possible to register what Entities can be provided by a Context Source just by their id or idPattern (i.e. without specifying their type).

| Name | Data Type | | Restrictions | Cardinality | | | Description | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| type | String or | | Fully Qualified Name of an Entity | 1 | Entity Type (or JSON array, in case of | | | | |
| | String[] | | Type or the Entity Type name as | | Entities with multiple Entity Types). | | | | |

Table 5.2.8-1: EntityInfo data type definition a short-hand string. See clause 4.6.2

id String Valid URI 0..1 Entity identifier.
idPattern String Regular expression as per
IEEE 1003.2™ [11]

0..1 A regular expression which denotes a pattern that shall be matched by the provided or subscribed Entities.

# Related

* [Clause 4.6.2](/framework/restrictions/supported-names.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — EntityInfo
