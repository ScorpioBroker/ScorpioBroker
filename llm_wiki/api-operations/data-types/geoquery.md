---
type: NGSI-LD Data Type
title: "GeoQuery"
description: "This datatype represents a geoquery used for Subscriptions."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.13
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.13"
---

This datatype represents a geoquery used for Subscriptions.
The supported JSON members shall follow the requirements provided in Table 5.2.13-1.

Table 5.2.13-1: GeoQuery data type definition Name Data Type Restrictions Cardinality Description coordinates JSON Array or String A JSON Array coherent with the geometry type as per IETF RFC 7946 [8]

1 Coordinates of the reference geometry. For the sake of JSON-LD compatibility It can be encoded as a string as described in clause 4.7.1.

1 Type of the reference geometry.

geometry String A valid GeoJSON [8] geometry type excepting GeometryCollection

| georel | String | | A valid geo-relationship as | | 1 | | Geo-relationship ("near", | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | defined by clause 4.10 | | | | "within", etc.). | | | |
| geoproperty | String | | Attribute name as short hand | | 0..1 | | Specifies the GeoProperty to | | | |
| | | | string or URI | | | | which the GeoQuery is to be | | | |

applied. If not present, the default GeoProperty is location.

# Related

* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.7.1](/framework/geo-temporal/geojson-geometries.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — GeoQuery
