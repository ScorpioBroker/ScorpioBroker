---
type: NGSI-LD Data Type
title: "Property"
description: "This type represents the data needed to define a Property as mandated by clause 4.5.2."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.5"
---

This type represents the data needed to define a Property as mandated by clause 4.5.2.
The supported JSON members shall follow the requirements provided in Table 5.2.5-1 below. The datatype definition
defines all the required attributes for the normalized representation. In the concise representation, the Attribute type
member can be omitted as type="Property" can be inferred from the presence of the value member. Furthermore,
in the concise representation of a Property, the value member cannot be a GeoJSON Object (as defined in clause 4.7) as
it would be interpreted as a GeoProperty (see clause 5.2.7).

Table 5.2.5-1: NGSI-LD Property data type definition Name Data Type Restriction Cardinality Description type String It shall be equal to "Property"

| | type | String | | It shall be equal to | | | 1 | | Node type. | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | "Property" | | | | | | |
| | value | Any JSON value as defined | | See NGSI-LD Value | | | 1 | | Property Value. | |
| | | by IETF RFC 8259 [6] | | definition in clause 3.1. | | | | | | |
| | datasetId | String | | Valid URI | | | 0..1 | | It allows identifying a set | |

datasetId String Valid URI 0..1 It allows identifying a set or group of property values.

expiresAt String DateTime (see clause 4.6.3)

0..1 System temporal Property representing the expiration date for the storage of the Property. See clause 4.22.


| | ngsildproof | Property | | Property with the non | | | | 0..1 | | | Cryptographic signature | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | reified subproperties | | | | | | | of the Property | | |
| | | | | "entityIdSealed" and | | | | | | | guaranteeing its | | |
| | | | | "entityTypeSealed" as | | | | | | | integrity. See | | |
| | | | | specified in [35]. The | | | | | | | clause C.11 for an | | |
| | | | | value of its "value" | | | | | | | example. | | |
| | | | | element shall be an object | | | | | | | | | |
| | | | | containing the W3C | ® Data | | | | | | | | |
| | | | | integrity "proof" structure | | | | | | | | | |

Name Data Type Restriction Cardinality Description [35].

observedAt String DateTime (clause 4.6.3) 0..1 Timestamp. See
clause 4.8.
unitCode String As mandated by [15] 0..1 Property Value's unit
code.
valueType String 0..1 The native JSON-LD
@type for the Property
Value. A String Value
which shall be type
coerced to a URI based
on the supplied
@context.

| | | | | | | | | | | | @context. | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | Property or Property[] (see | | See datatype definition in | | | | 0..N | | | Properties of the | | |
| | | note 1) | | clause 5.2.5 | | | | | | | Property. | | |
| | | GeoProperty or | | See datatype definition in | | | | 0..N | | | GeoProperties of the | | |
| | | GeoProperty[] (see note 1) | | clause 5.2.7 | | | | | | | Property. | | |
| | | LanguageProperty or | | See datatype definition in | | | | 0..N | | | LanguageProperties of | | |
| | | LanguageProperty[] (see | | clause 5.2.32 | | | | | | | the Property. | | |
| | | note 1) | | | | | | | | | | | |
| | | JsonProperty or | | See datatype definition in | | | | 0..N | | | JsonProperties of the | | |
| | | JsonProperty[] (see note 1) | | clause 5.2.38 | | | | | | | Property. | | |
| | | VocabProperty or | | See datatype definition in | | | | 0..N | | | VocabProperties of the | | |
| | | VocabProperty[] (see note | | clause 5.2.35 | | | | | | | Property. | | |

1)

| | | ListProperty or | | See datatype definition in | | | | 0..N | | | ListProperties of the | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | ListProperty[] (see note 1) | | clause 5.2.36 | | | | | | | Property. | | |
| |  | Relationship[] (see note 2) | | clause 5.2.6 | | | | | | | Property. | | |
| | | ListRelationship or | | See datatype definition in | | | | 0..N | | | ListRelationships of the | | |
| | | ListRelationship[] (see | | clause 5.2.37 | | | | | | | Property. | | |
| | | note 2) | | | | | | | | | | | |
| | NOTE 1: | For each Property (or subclass of Property) identified by the same Property name, there can be one or | | | | | | | | | | | |

NOTE 1: For each Property (or subclass of Property) identified by the same Property name, there can be one or
more instances separated by datasetId.
NOTE 2: For each Relationship (or subclass of Relationship) identified by the same Relationship name, there can
be one or more instances separated by datasetId.

The following output only members (defined by Table 5.2.5-2) of the Property data structure are also defined. They are read-only and shall be generated by NGSI-LD implementations. They shall not be provided by Context Producers. In the event that they are provided (in update or create operations) NGSI-LD implementations shall ignore them.


| Name | Data Type | | Restrictions | | | Cardinality | | Description | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| createdAt | String | DateTime | (clause 4.6.3) | | | 0..1 | | System generated | | | |
| | | | | | | | | creation timestamp. | | | |
| | | | | | | | | See clause 4.8. | | | |
| deletedAt | String | DateTime | (clause 4.6.3) | | | 0..1 | | System generated | | | |
| | | It is only used in notifications | | | | | | deletion timestamp. | | | |
| | | reporting deletions | | | | | | See clause 4.8. | | | |
| instanceId | String | Valid URI. Only used in | | | | 0..1 | | URI uniquely | | | |
| | | temporal representation of | | | | | | identifying a Property | | | |
| | | Properties | | | | | | instance as mandated | | | |
| | | | | | | | | by clause 4.5.7. | | | |
| modifiedAt | String | DateTime | (clause 4.6.3) | | | 0..1 | | System generated | | | |
| | | | | | | | | last modification | | | |
| | | | | | | | | timestamp. See | | | |

Table 5.2.5-2: Output only members of the NGSI-LD Property data type clause 4.8.

| previousValue | Any JSON value as | Only used in Notifications, if the | | | | 0..1 | | Previous Property | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | defined by IETF | showChanges option is | | | | | | Value. | | | |
| | RFC 8259 [6] | explicitly requested | | | | | | | | | |

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.7](/framework/data-representation/temporal-representation-of-a-property.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 5.2.32](/api-operations/data-types/languageproperty.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Property
