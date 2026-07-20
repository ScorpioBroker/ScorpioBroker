---
type: NGSI-LD Data Type
title: "FeatureProperties"
description: "This data type represents the type and the associated attributes (Properties and Relationships) of an Entity in GeoJSON format."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.31
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.31"
---

This data type represents the type and the associated attributes (Properties and Relationships) of an Entity in GeoJSON format.

Table 5.2.31-1: FeatureProperties data type definition Name Data Type Restriction Cardinality Description type String or String[] Entity Type 1 Entity Type (or JSON array, in case of Entities with multiple Entity Types). Both short hand string (type name) or URI are allowed.

| | | Property or Property[], | | See data type | | | | 0..N | | Property as mandated by | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | see note 1 | | definition in | | | | | | clause 4.5.2. | |
| | | | | clause 5.2.5 | | | | | | | |
| | | GeoProperty or | | See datatype | | | | 0..N | | GeoProperty as mandated by | |
| | | GeoProperty[], see | | definition in | | | | | | clause 4.5.2. | |
| | | note 1 | | clause 5.2.7 | | | | | | | |
| | | LanguageProperty or | | See datatype | | | | 0..N | | LanguageProperty as | |
| | | LanguageProperty[], | | definition in | | | | | | mandated by clause 4.5.18. | |
| | | see note 1 | | clause 5.2.32 | | | | | | | |
| | | JsonProperty or | | See datatype | | | | 0..N | | JsonProperties as | |
| | | JsonProperty[] | see | definition in | | | | | | mandated by clause 4.5.24. | |
| | | note 1 | | clause 5.2.38 | | | | | | | |
| | | VocabProperty or | | See datatype | | | | 0..N | | VocabProperty as mandated | |
| | | VocabProperty[] | | , see definition in | | | | | | by clause 4.5.20. | |
| | | note 1 | | clause 5.2.35 | | | | | | | |
| | | ListProperty or | | See datatype | | | | 0..N | | ListProperty as mandated | |
| | | ListProperty[], see | | definition in | | | | | | by clause 4.5.21. | |
| | | note 1 | | clause 5.2.36 | | | | | | | |
| | | Relationship or | | See data type | | | | 0..N | | Relationship as mandated by | |
| | | Relationship[], see | | definition in | | | | | | clause 4.5.3. | |
| | | note 2 | | clause 5.2.6 | | | | | | | |
| | | ListRelationship or | | See datatype | | | | 0..N | | ListRelationship as | |
| | | ListRelationship[], see | | definition in | | | | | | mandated by clause 4.5.22. | |
| | | note 2 | | clause 5.2.37 | | | | | | | |

0..N ListRelationship as
mandated by clause 4.5.22.
NOTE 1: For each Property (or subclass of Property) identified by the same Property name, there can be one or more
instances separated by datasetId.
NOTE 2: For each Relationship (or subclass of Relationship) identified by the same Relationship name, there can be
one or more instances separated by datasetId.

# Related

* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.20](/framework/data-representation/ngsi-ld-vocabproperty-representations.md)
* [Clause 4.5.21](/framework/data-representation/ngsi-ld-listproperty-representations.md)
* [Clause 4.5.22](/framework/data-representation/ngsi-ld-listrelationship-representations.md)
* [Clause 4.5.24](/framework/data-representation/ngsi-ld-jsonproperty-representations.md)
* [Clause 4.5.3](/framework/data-representation/ngsi-ld-relationship-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.31](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — FeatureProperties
