---
type: NGSI-LD Clause
title: "GeoJSON Representation of an individual Entity"
description: "The GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object including the following members: Mandatory - \"geometry\": The value of the selected GeoProperty (a"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.16.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.16.2"
---

The GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object including the
following members:
Mandatory
- "geometry": The value of the selected GeoProperty (a GeoJSON geometry object) used to define the spatial location of the Entity. Note that no sub-Attributes of the selected GeoProperty are present in the representation.
- "id": the Entity id.


- "properties": A JSON object containing the following members: - "type": the Entity Type name of the Entity or an unordered JSON array with the Entity Type names of the Entity. - One member for each Property (including the selected GeoProperty) as per the rules stated in clause 4.5.2. In case of multiple Property instances with the same Property name as described in clause 4.5.5, all instances are provided as an unordered JSON array. - One member for each Relationship as per the rules stated in clause 4.5.3. In case of multiple Relationship instances with the same Relationship name as described in clause 4.5.5, all instances are provided as an unordered JSON array.
- "type": the fixed value "Feature". Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with Feature as defined within IETF RFC 7946 [8]. An example can be found in annex C, clause C.2.3.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.3](/framework/data-representation/ngsi-ld-relationship-representations.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.16.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — GeoJSON Representation of an individual Entity
