---
type: NGSI-LD Data Type
title: "OrderingParams"
description: "This datatype represents the parameters that convey the definition used whilst ordering Entities."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.43
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.43"
---

This datatype represents the parameters that convey the definition used whilst ordering Entities.
The supported JSON members shall follow the requirements provided in Table 5.2.43-1.

Table 5.2.43-1: OrderingParams data type definition Name Data Type Restriction Cardinality Description collation String An ICU collation (see IETF RFC 6067 [36]).

0..1 The Entities returned in the payload shall be ordered according to the collation given. By default, it shall be ICU "root" collation order ("und-x-icu").

| coordinates | JSON Array A JSON Array coherent with | 0..1 | Coordinates of a Geometry | | |
| --- | --- | --- | --- | --- | --- |
| | the geometry type as per | It shall be | used when sorting by | | |
| | IETF RFC 7946 [8]. | one if | distance. | | |

0..1
It shall be
one if
orderBy
uses order
by distance

| geometry | String A valid GeoJSON [8] | 0..1 | The type of geometry whose | | |
| --- | --- | --- | --- | --- | --- |
| | geometry type excepting | | coordinates are used when | | |
| | GeometryCollection. | | sorting by distance. By | | |

default, it shall be a "Point" geometry.

| orderBy | String[] Each String is an Entity | 1 | When defined, the Entities | | |
| --- | --- | --- | --- | --- | --- |
| | member ("id", "type", | | returned in the payload shall | | |
| | "scope" or an Attribute | | be ordered according to | | |
| | name) appended with an | | members defined. | | |

orderBy String[] Each String is an Entity member ("id", "type", "scope" or an Attribute name) appended with an optional sorting style ("asc", "desc", "dist-asc", "dist desc") as per clause 4.23.

# Related

* [Clause 4.23](/framework/languages/entity-ordering.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.43](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — OrderingParams
