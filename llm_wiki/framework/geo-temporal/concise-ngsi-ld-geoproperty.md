---
type: NGSI-LD Clause
title: "Concise NGSI-LD GeoProperty"
description: "Notwithstanding the restrictions defined in clause 4.5.2.3, an NGSI-LD GeoProperty without additional sub-attributes shall be represented in a concise but lossless representation by a member whose key"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.7.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.7.3"
---

Notwithstanding the restrictions defined in clause 4.5.2.3, an NGSI-LD GeoProperty without additional sub-attributes
shall be represented in a concise but lossless representation by a member whose key is the Property name (a term) and
whose value is the Property Value (see definition of terms in clause 3.1) which itself is also a supported GeoJSON
geometry.
Mandatory
- "type": shall be a supported GeoJSON geometry type as defined in clause 4.7.1.
- "coordinates": shall be present, as defined by the relevant GeoJSON Geometry [8]. When parsing a geospatial value submitted in the concise representation, it shall be possible for the NGSI-LD system to infer the GeoProperty type. Error handing of the payload is left ambiguous if the NGSI-LD system is unable to distinguish a payload as either a Property or a GeoProperty. Furthermore, an NGSI-LD GeoProperty which includes additional Properties or Relationships shall be treated in the same manner as an ordinary NGSI-LD Property (see clause 4.5.2.3) with the exception that if the Property value resolves to a supported GeoJSON geometry, the type "GeoProperty" shall be inferred.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.7.1](/framework/geo-temporal/geojson-geometries.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.7.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Concise NGSI-LD GeoProperty
