---
type: NGSI-LD Data Type
title: "FeatureCollection"
description: "This data type represents a list of spatially bounded Entities in GeoJSON format, as mandated by IETF RFC 7946 [8]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.30
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.30"
---

This data type represents a list of spatially bounded Entities in GeoJSON format, as mandated by IETF RFC 7946 [8].
The supported JSON members shall follow the requirements provided in Table 5.2.30-1.


Table 5.2.30-1: FeatureCollection data type definition Name Data Type Restriction Cardinality Description type String It shall be equal to "FeatureCollection"

1 GeoJSON Type.

features Feature[] See data type definition 1..N In the case that no matches are found, features will be an empty array.

@context URI, JSON Object, or JSON Array

See [2], section 5.1. 0..1 JSON-LD @context. This field is only present if requested in the payload by the HTTP Prefer Header (IETF RFC 7240 [26]).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.30](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — FeatureCollection
