---
type: NGSI-LD Clause
title: "GeoJSON Representation of Entities"
description: "4.5.16.0 Foreword The NGSI-LD specification defines an alternative representation of Entities, to make NGSI-LD responses compatible with GIS (Geographic Information System) applications which support"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.16
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.16"
---

4.5.16.0 Foreword

The NGSI-LD specification defines an alternative representation of Entities, to make NGSI-LD responses compatible
with GIS (Geographic Information System) applications which support the GeoJSON format [8] and/or
GeoJSON-LD [i.20].
Every NGSI-LD Entity can be represented as a GeoJSON Feature object, where a Feature object represents any
spatially bounded thing as defined by its geometry.

4.5.16.1 Top-level "geometry" field selection algorithm

A parameter of the request (named geometryProperty) may be used to indicate the name of the GeoProperty to be selected. If this parameter is not present, then the default name of "location" shall be used. If the selected GeoProperty has multiple instances as described in clause 4.5.5, either a datasetId shall be specified, in order to define which instance of the value is to be selected, or a default attribute instance exists, which is then selected, if no datasetId was specified. If an entity lacks the GeoProperty as specified or the value does not hold a valid GeoJSON geometry object then the geometry shall be undefined and returned with a value of null - which is syntactically valid GeoJSON.

4.5.16.2 GeoJSON Representation of an individual Entity

The GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object including the
following members:
Mandatory
- "geometry": The value of the selected GeoProperty (a GeoJSON geometry object) used to define the spatial location of the Entity. Note that no sub-Attributes of the selected GeoProperty are present in the representation.
- "id": the Entity id.


- "properties": A JSON object containing the following members: - "type": the Entity Type name of the Entity or an unordered JSON array with the Entity Type names of the Entity. - One member for each Property (including the selected GeoProperty) as per the rules stated in clause 4.5.2. In case of multiple Property instances with the same Property name as described in clause 4.5.5, all instances are provided as an unordered JSON array. - One member for each Relationship as per the rules stated in clause 4.5.3. In case of multiple Relationship instances with the same Relationship name as described in clause 4.5.5, all instances are provided as an unordered JSON array.
- "type": the fixed value "Feature". Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with Feature as defined within IETF RFC 7946 [8]. An example can be found in annex C, clause C.2.3.

4.5.16.3 GeoJSON Representation of Multiple Entities

The GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection
object containing an array of GeoJSON Feature objects as follows:
Mandatory
- "type": the fixed value "FeatureCollection".
- "features": a JSON array of GeoJSON Feature objects as defined in clause 4.5.16.2. Note that separate @context elements for each Feature will not be present in the payload body. Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with FeatureCollection as defined within IETF RFC 7946 [8]. An example can be found in annex C, clause C.2.3.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.16.2](/framework/data-representation/geojson-representation-of-an-individual-entity.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.3](/framework/data-representation/ngsi-ld-relationship-representations.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.16](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — GeoJSON Representation of Entities
