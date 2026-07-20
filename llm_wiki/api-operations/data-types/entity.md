---
type: NGSI-LD Data Type
title: "Entity"
description: "This type represents the data needed to define an NGSI-LD Entity as mandated by clause 4.5.1."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.4"
---

This type represents the data needed to define an NGSI-LD Entity as mandated by clause 4.5.1.
The supported JSON members shall follow the requirements provided in Table 5.2.4-1.

| | Name | | | Data Type | | Restriction | | | | Cardinality | Description | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| id | | | | String | Valid URI | | | | | 1 | Entity ID. | |
| type | | | | String or String[] | | | | | | 1 | Entity Type(s). Both short hand | |

Table 5.2.4-1: NGSI-LD Entity data type definition string(s) (type name) or URI(s) are allowed.

expiresAt String DateTime (see clause 4.6.3)

0..1 System temporal Property representing the expiration date for the storage of the Entity. See clause 4.22.

| location | | | | GeoProperty | See datatype | | | | | 0..1 | Default geospatial Property of | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | definition in | | | | | | an entity. See clause 4.7. | |
| | | | | | clause 5.2.7 | | | | | | | |
| observationSpace | | | | GeoProperty | See datatype | | | | | 0..1 | See clause 4.7. | |
| | | | | | definition in | | | | | | | |
| | | | | | clause 5.2.7 | | | | | | | |
| operationSpace | | | | GeoProperty | See datatype | | | | | 0..1 | See clause 4.7. | |
| | | | | | definition in | | | | | | | |
| | | | | | clause 5.2.7 | | | | | | | |
| scope | | | | String or String[] | See clause 4.18 | | | | | 0..1 | Scope. | |


| | | | Property or Property[] | See datatype | | | 0..N | Property as mandated by | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | (see note 1) | definitions in | | | | clause 4.5.2. | | |
| | | | | clause 5.2.5 | | | | | | |
| | | | GeoProperty or | See datatype | | | 0..N | GeoProperty as mandated by | | |
| | | | GeoProperty[] (see | definition in | | | | clause 4.5.2. | | |
| | | | note 1) | clause 5.2.7 | | | | | | |
| | | | LanguageProperty or | See datatype | | | 0..N | LanguageProperty as | | |
| | | | LanguageProperty[] | definition in | | | | mandated by clause 4.5.18. | | |
| | | | (see note 1) | clause 5.2.32 | | | | | | |
| | | | JsonProperty or | See datatype | | | 0..N | JsonProperty as mandated by | | |
| | | | JsonProperty[] | (see definition in | | | | clause 4.5.24. | | |
| | | | note 1) | clause 5.2.38 | | | | | | |
| | | | VocabProperty or | See datatype | | | 0..N | VocabProperty as mandated by | | |
| | | | VocabProperty[] | (see definition in | | | | clause 4.5.20. | | |
| | | | note 1) | clause 5.2.35 | | | | | | |
| | | | ListProperty or | See datatype | | | 0..N | ListProperty as mandated by | | |
| | | | ListProperty[] | (see definition in clause | | | | clause 4.5.21. | | |
| | | | note 1) | 5.2.36 | | | | | | |
| | | | Relationship or | See datatype | | | 0..N | Relationship as mandated by | | |
| | | | Relationship[] | (see definition in | | | | clause 4.5.3. | | |
| | | | note 2) | clause 5.2.6 | | | | | | |
| | | | ListRelationship or | See datatype | | | 0..N | ListRelationship as mandated | | |
| | | | ListRelationship[] | (see definition in | | | | by clause 4.5.22. | | |
| | | | note 2) | clause 5.2.37 | | | | | | |
| NOTE 1: | | For each Property (or subclass of Property) identified by the same Property name, there can be one or more | | | | | | | | |

Name Data Type Restriction Cardinality Description

NOTE 1: For each Property (or subclass of Property) identified by the same Property name, there can be one or more
instances separated by datasetId.
NOTE 2: For each Relationship (or subclass of Relationship) identified by the same Relationship name, there can be
one or more instances separated by datasetId.

# Related

* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.20](/framework/data-representation/ngsi-ld-vocabproperty-representations.md)
* [Clause 4.5.21](/framework/data-representation/ngsi-ld-listproperty-representations.md)
* [Clause 4.5.22](/framework/data-representation/ngsi-ld-listrelationship-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity
