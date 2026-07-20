---
type: NGSI-LD Clause
title: "GeoJSON Representation of Multiple Entities"
description: "The GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection object containing an array of GeoJSON Feature objects as follows: Mandatory - \"type\""
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.16.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.16.3"
---

The GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection
object containing an array of GeoJSON Feature objects as follows:
Mandatory
- "type": the fixed value "FeatureCollection".
- "features": a JSON array of GeoJSON Feature objects as defined in clause 4.5.16.2. Note that separate @context elements for each Feature will not be present in the payload body. Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with FeatureCollection as defined within IETF RFC 7946 [8]. An example can be found in annex C, clause C.2.3.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.16.2](/framework/data-representation/geojson-representation-of-an-individual-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.16.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — GeoJSON Representation of Multiple Entities
