---
type: NGSI-LD Clause
title: "Simplified GeoJSON Representation of multiple Entities"
description: "The simplified GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection object containing an array of GeoJSON Feature objects as follows: Mandato"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.17.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.17.2"
---

The simplified GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection object containing an array of GeoJSON Feature objects as follows: Mandatory

- "features": a JSON array of simplified GeoJSON Feature objects as defined in clause 4.5.17.1. Note that separate @context elements for each Feature will not be present in the payload body.
- "type": the fixed value "FeatureCollection". Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with FeatureCollection as defined within IETF RFC 7946 [8].

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.17.1](/framework/data-representation/simplified-geojson-representation-of-an-individual-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.17.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Simplified GeoJSON Representation of multiple Entities
