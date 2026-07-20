---
type: Reference
title: "ValueType datatype utilization clarifications"
description: "Using JSON-LD [2] syntax, typed values can be expressed using the JSON-LD @type keyword when defining a term, where @type value holds a URI which indicates the value's datatype."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.10
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.10"
---

Using JSON-LD [2] syntax, typed values can be expressed using the JSON-LD @type keyword when defining a term, where @type value holds a URI which indicates the value's datatype. However, it can be desirable for a Context Broker to be able to hold simpler untyped values within a Property's value attribute and separately use a Property's valueType to hold the value's datatype. This format can be used to accommodate multiple acceptable datatype formats for the data to be held within a single Property and still hold sufficient meta data to be able to distinguish between them.

For example, consider an ontology for an Entity of type "Place" which has an address Property, where the address Property can either be an unstructured address in the form of a "String" or a structured "PostalAddress" JSON Object with street, city and country attributes - the following JSON-LD schema can be defined:

Example JSON-LD schema

{

"example": "http://example.org/",
"xsd": "http://www.w3.org/2001/XMLSchema#",
"address": "example:address",
"city": "example:city",
"country": "example:country",
"street": "example:street",
"Place": "example:Place"
"PostalAddress": "example:PostalAddress",
"String": "xsd:String"

}

Then the following two Entities of type "Place" can be created using the address.valueType Property to distinguish

between the two formats:

[ {

"id": "urn:ngsi-ld:Place:27182",
"type": "Place",
"address": {
"type": "Property",
"value": "Pariser Platz, Berlin, Germany",
"valueType": "String"

} }, {

"id": "urn:ngsi-ld:Place:31415",
"type": "Place",
"address": {
"type": "Property",
"value": {
"street": "Straße des 17. Juni",
"city": "Berlin",
"country": "Germany"

}, "valueType": "PostalAddress"

} } ]

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — ValueType datatype utilization clarifications
