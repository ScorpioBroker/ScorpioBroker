---
type: NGSI-LD Clause
title: "Backwards compatibility of Context Source payloads"
description: "When retrieving Entity data found distributed across multiple associated Context Brokers each Context Source is sent a context consumption request."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.3.6.8
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.3.6.8"
---

When retrieving Entity data found distributed across multiple associated Context Brokers each Context
Source is sent a context consumption request. A Context Broker shall assume that every Context Source
will return valid NGSI-LD Entity data in a format that it understands and it shall reject data that is invalid. However,
since the definition of a valid NGSI-LD Entity has broadened with each version of the NGSI-LD specification, it is
possible that a registered Context Source could respond with valid NGSI-LD Entity data which does not fit the
narrower confines of a previous NGSI-LD specification.
Therefore, when making a context consumption request, a Context Broker may wish to indicate that it is only
capable of interpreting responses which conform to a specific NGSI-LD specification, in which case the Context
Source shall endeavour to amend its payload accordingly. Table 4.3.6.8-1 describes a minimal level of support for a
NGSI-LD Entity data as specified by each version of the NGSI-LD specification, and the expected fallback behaviour
required if a context consumption request is made to receive data conformant to an earlier version of the NGSI-LD
specification. Table 4.3.6.8-2 describes conformance fallbacks for an NGSI-LD Property (and its subclasses) and Table
4.3.6.8-2 describes conformance fallbacks for an NGSI-LD Relationship (and its subclasses).
The version of the NGSI-LD specification requested and the conformant version returned is defined in the form
major.minor, for example 1.5.

Table 4.3.6.8-1: NGSI-LD Entity data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

id String Valid URI 1.0
type String or String[]
(see note 1)

1.0

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload

location GeoProperty Default geospatial Property of an entity. See clause 4.7

| observationSpace | GeoProperty | | See clause 4.7 | 1.0 | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| operationSpace | GeoProperty | | See clause 4.7 | 1.0 | | | | | |
| scope | String or String[] | | See clause 4.18 | 1.4 | | Remove attribute from | | | |

1.0 payload

| | Property or | | Property as mandated by | 1.0 | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | Property[] | | clause 4.5.2. | | | | | | |
| | (see note 2) | | | | | | | | |
| | GeoProperty or | | GeoProperty as mandated | 1.0 | | | | | |
| | GeoProperty[] | | by clause 4.5.2. | | | | | | |
| | (see note 2) | | | | | | | | |
| | LanguageProperty or | | LanguageProperty as | 1.4 | | Reformat attribute as | | | |
| | LanguageProperty[] | | mandated by clause 4.5.18. | | | Property | | | |
| | (see note 2) | | | | | | | | |
| | JsonProperty or | | JsonProperty as | 1.8 | | Reformat attribute as | | | |
| | JsonProperty[] | | mandated by clause 4.5.24. | | | Property | | | |
| | (see note 2) | | | | | | | | |


Name Data Type Definition Version Introduced

Conformant Data Fallback

VocabProperty or VocabProperty[] (see note 2)

VocabProperty as mandated by clause 4.5.20.

1.8 Reformat attribute as Property

ListProperty or ListProperty[] (see note 1)

ListProperty as mandated by clause 4.5.21.

1.8 Reformat attribute as Property

 Relationship or Relationship[] (see note 3)

Relationship as mandated by clause 4.5.3.

1.0

ListRelationship or ListRelationship[] (see note 3)

ListRelationship as mandated by clause 4.5.22.

1.8 Reformat attribute as Relationship

NOTE 1: From 1.3 onwards, an Entity type can be assigned multiple values. For 1.0 backwards compatibility only
return a single element with preference to the first instance.
NOTE 2: From 1.3 onwards, multiple instances of a Property (or subclass of Property) identified by the same Property
name can be separated by datasetId. For 1.0 backwards compatibility only return a single element of
Property Array with preference to the default instance.
NOTE 3: From 1.3 onwards, multiple instances of a Relationship (or subclass of Relationship) identified by the same
Relationship name can be separated by datasetId. From 1.3 onwards. For 1.0 backwards compatibility only
return a single element of Relationship Array with preference to the default instance.

Table 4.3.6.8-2: NGSI-LD Property data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

type String 1.0 value Any JSON value as defined by IETF RFC 8259 [6]

See NGSI-LD Value definition in clause 3.1

1.0

datasetId String Valid URI as mandated by clause 4.5.5

1.3 Remove attribute from payload

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload

observedAt String DateTime as mandated by clause 4.8

1.3 Remove attribute
from payload
unitCode String As mandated by [15] 1.3 Remove attribute
from payload
valueType String See clause 4.5.2 1.9 Remove attribute
from payload

Table 4.3.6.8-3: NGSI-LD Relationship data type attribute support Name Data Type Definition Version Introduced

Conformant Data Fallback

type String Valid URI 1.0 object String or String[] 1.0 datasetId String Valid URI as mandated by clause 4.5.5

1.3 Remove attribute from payload

expiresAt String DateTime as mandated by clause 4.22

1.9 Remove attribute from payload objectType String or String[] See clause 4.5.23 1.8 Remove attribute from payload

observedAt String DateTime as mandated by clause 4.8

1.3 Remove attribute from payload

When responding to a context consumption request to supply data conforming to a specific NGSI-LD specification, Context Sources should indicate the version of the specification the returned payload actually conforms to. In general, Context Sources will not be expected to be flexible enough to supply payloads conformant to all past and future versions of the specification, but the requesting Context Broker may use supplied version information when collating data from multiple Context Sources. and to validate and amend received payloads.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.18](/framework/languages/ngsi-ld-scopes.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.20](/framework/data-representation/ngsi-ld-vocabproperty-representations.md)
* [Clause 4.5.21](/framework/data-representation/ngsi-ld-listproperty-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.3.6.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Backwards compatibility of Context Source payloads
