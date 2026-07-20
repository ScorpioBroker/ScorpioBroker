---
type: Reference
title: "Date Representation"
description: "In NGSI-LD, a TemporalProperty is represented only by its value, i.e."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.6
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.6"
---

In NGSI-LD, a TemporalProperty is represented only by its value, i.e. no sub-Properties of TemporalProperty nor sub

Relationships of TemporalProperty can be conveyed. In more formal language, a TemporalProperty does not allow reification. The term TemporalProperty has been reserved for non-reified structural timestamps (observedAt, createdAt, modifiedAt, deletedAt), which capture the temporal evolution of Attributes. Only such structural timestamps can be used as timeproperty in Temporal Queries as mandated by clause 4.11.

The following examples show how time values (Date, Time, or DateTime) can be represented in NGSI-LD as reified Properties. For a reified Property whose value is assigned the JSON type Date, DateTime or Time, one mechanism is to use the Property's valueType to hold the datatype ("Date", "Datetime" or "Time"), as shown below:

{

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"testedAt": {
"type": "Property",
"value":"2018-12-04T12:00:00Z"
"valueType": "DateTime"
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Alternatively the data can be a structured to use the JSON-LD @value syntax structure, as shown below:

{

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"testedAt": {
"type": "Property",
"value": {
"@type": "DateTime",
"@value": "2018-12-04T12:00:00Z"
}
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

A third alternative to achieve the same result would be to use JSON-LD "type coercion". With type coercion, values with a special data type are defined with @type in the @context. This enforces the correct type for any occurrence. Such

an @context fragment is shown below:

"testedAt": { "@type": "https://uri.etsi.org/ngsi-ld/DateTime", "@id": "http://example.org/test/testedAt"

}

The above does not work, when using the @context to perform compaction, in the normalized and compact representation of NGSI-LD, due to reification of the Property, because in this case testedAt is a complex JSON object, which cannot be compacted to a DateTime type as the @context specifies. Thus, the full URI http://example.org/test/testedAt is kept, instead of the short name testedAt. In summary, user @contexts used for the normalized and compact NGSI-LD representation cannot use the JSON-LD type coercion feature.

However, in the simplified (keyValue) representation case, such an @context with the specification of testedAt could be

used, as there is no reification.

As a side note, when using the above @value + @type approach, since type is mapped to @type in the NGSI-LD core @context, JSON-LD compaction will result in the following compacted value, instead of the one shown above, because

@type is compacted to type:

"value": { "type": "DateTime", "@value": "2018-12-04T12:00:00Z"

}

# Related

* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Date Representation
