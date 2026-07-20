---
type: NGSI-LD Clause
title: "NGSI-LD Entity Representation"
description: "An NGSI-LD Entity shall be represented by an object encoded using JSON-LD [2]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.1"
---

An NGSI-LD Entity shall be represented by an object encoded using JSON-LD [2]. The rules described below state the
encoding that shall be supported by implementations. Annex D provides a computational description of this process in
terms of an algorithm.
The JSON-LD object contains the following members:
Mandatory
- "id" whose value shall be a URI that identifies the Entity.
- "type" whose value shall be equal to the Entity Type name or an unordered JSON array with multiple Entity Type names in case of an Entity that has multiple Entity Types.

Optional - "expiresAt": a string as mandated by clause 4.22.


- "scope" whose value shall be a Scope as defined in clause 4.18 or an unordered JSON array with multiple Scopes in case of an Entity that has multiple Scopes.
- "@context" a JSON-LD @context as described in clause 4.4.
- One member for each Property as per the rules stated in clause 4.5.2. In case of multiple Property instances with the same Property name as described in clause 4.5.5, all instances are provided as an unordered JSON array, and datasetId (see clause 4.5.5) shall be used to distinguish them.
- One member for each Relationship as per the rules stated in clause 4.5.3. In case of multiple Relationship instances with the same Relationship name as described in clause 4.5.5, all instances are provided as an unordered JSON array, and datasetId (see clause 4.5.5) shall be used to distinguish them.

NOTE 1: In the following, the term Attribute is used when referring in the text to both a Property and a Relationship (see definition of NGSI-LD Attribute in clause 3.1). NOTE 2: When GeoJSON representation is selected, the layout of the Entities changes, see clause 4.5.16 for details. Terms defined in the Core Context as non-reified Properties (such as "datasetId", "instanceId", etc.) shall not be used as Attribute names. Attributes shall not contain any embedded @context, as described in clause 5.5.7.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.3](/framework/data-representation/ngsi-ld-relationship-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Entity Representation
