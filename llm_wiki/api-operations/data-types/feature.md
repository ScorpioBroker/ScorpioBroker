---
type: NGSI-LD Data Type
title: "Feature"
description: "This data type represents a spatially bounded Entity in GeoJSON format, as mandated by IETF RFC 7946 [8]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.29
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.29"
---

This data type represents a spatially bounded Entity in GeoJSON format, as mandated by IETF RFC 7946 [8]. The supported JSON members shall follow the requirements provided in Table 5.2.29-1.

| | Name | | | Data Type | | | | Restriction | | | Cardinality | | Description | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | | | String | | | | | Valid URI | | | 1 | | Entity ID. | |
| type | | | String | | | | | It shall be equal to | | | 1 | | GeoJSON Type. | |

Table 5.2.29-1: Feature data type definition "Feature"

1 null if no matching GeoProperty.

geometry GeoJSON Object The value field from the matching GeoProperty (as specified in clause 4.5.16) or null

properties FeatureProperties See data type definition 1 List of attributes as mandated by clause 5.2.31.

@context URI, JSON Object, or JSON Array

See [2], section 5.1. 0..1 JSON-LD @context. This field is only present if requested in the payload by the HTTP Prefer Header (IETF RFC 7240 [26]).

# Related

* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 5.2.31](/api-operations/data-types/featureproperties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.29](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Feature
