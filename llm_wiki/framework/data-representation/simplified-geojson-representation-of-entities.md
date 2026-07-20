---
type: NGSI-LD Clause
title: "Simplified GeoJSON Representation of Entities"
description: "4.5.17.0 Foreword When both simplified (see clause 4.5.4) and GeoJSON representation is requested, the following simplified GeoJSON representation compatible with GIS systems shall be returned."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.17
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.17"
---

4.5.17.0 Foreword

When both simplified (see clause 4.5.4) and GeoJSON representation is requested, the following simplified GeoJSON representation compatible with GIS systems shall be returned.

4.5.17.1 Simplified GeoJSON Representation of an individual Entity

The simplified GeoJSON representation of a spatially bounded Entity is defined as a single GeoJSON Feature object as
follows:
Mandatory
- "geometry": The value of the selected GeoProperty (a GeoJSON geometry object) used to define the spatial location of the Entity.
- "id": the Entity id.
- "type": the fixed value "Feature".


- "properties": An array containing the following attributes: - "type": Mandatory - the Entity Type name of the Entity or an unordered JSON array with the Entity Type names of the Entity. - For each Property (including the selected GeoProperty) a member whose key is the Property name (a term) and whose value is the Property Value. In the multi-attribute case, each Property consists of a key-value pair, the key being the Property name (a term) and the value being a JSON Object, which contains a single Attribute with a key called "dataset", and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId, where the value corresponds to the simplified representation of the property value. The default datasetId (where present) is represented by the JSON-LD keyword "@none". - For each Relationship a term whose key is the Relationship name (a term) and whose value is the Relationship's Object (represented as a URI). In the multi-attribute case, each Relationship consists of a key-value pair, the key being the Relationship name (a term) and the value being a JSON Object containing a single Attribute with a key called "dataset" and its value in turn is a JSON Object holding a series of key-value pairs, one for each datasetId where the value corresponds to the object of the relationship. The default datasetId (where present) is represented by the JSON-LD keyword "@none".

Optional

- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. The selection of the geometry field is defined in clause 4.5.16.1. This representation shall be fully compliant with Feature as defined within IETF RFC 7946 [8]. An example can be found in annex C, clause C.2.3.

4.5.17.2 Simplified GeoJSON Representation of multiple Entities

The simplified GeoJSON representation of a list of spatially bounded Entities is defined as a single GeoJSON FeatureCollection object containing an array of GeoJSON Feature objects as follows: Mandatory

- "features": a JSON array of simplified GeoJSON Feature objects as defined in clause 4.5.17.1. Note that separate @context elements for each Feature will not be present in the payload body.
- "type": the fixed value "FeatureCollection". Optional
- A JSON-LD @context as described in clause 4.4 if requested as part of the payload body. This representation shall be fully compliant with FeatureCollection as defined within IETF RFC 7946 [8].

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.16.1](/framework/data-representation/top-level-geometry-field-selection-algorithm.md)
* [Clause 4.5.17.1](/framework/data-representation/simplified-geojson-representation-of-an-individual-entity.md)
* [Clause 4.5.4](/framework/data-representation/simplified-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.17](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Simplified GeoJSON Representation of Entities
