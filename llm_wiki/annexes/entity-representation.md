---
type: Reference
title: "Entity Representation"
description: "C.2.1 Property Graph Figure C.2.1-1 shows a diagram representing a property graph to be used for the examples discussed in this clause."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.2"
---

C.2.1 Property Graph

Figure C.2.1-1 shows a diagram representing a property graph to be used for the examples discussed in this clause.

Figure C.2.1-1: Reference example

As per the algorithms described above and as per the rules for generating the JSON-LD representation of NGSI-LD entities the above graph will result in the following JSON-LD representations. The syntax has been checked using the

JSON-LD Playground tool [i.5].

Vehicle

urn:ngsi-ld:
Vehicle:
A4567

"Mercedes"

2017-07-29T12:00:04Z

urn:ngsi-ld:
OffStreetParking:
Downtown1
urn:ngsi-ld:
Person:
Bob

Person

OffStreetParking urn:ngsi-ld: Camera:C1

Camera

0.7 121

brandName parkingDate reliability availableSpot Number

isParked provided By

provided By

Property Relationship Entity

Entity Type

type

hasValue hasObject


C.2.2 Vehicle Entity

Normalized Representation

The normalized representation is a lossless representation of an Entity, where every Property is defined by a type and a value and every Relationship is defined by a type and an object.

Below there is a representation of an Entity of Type "Vehicle". It can be observed that the @context is composed of different parts, namely the Core @context and several vocabulary-specific @contexts.

It is noteworthy that the @context corresponding to the Parking domain is included as it is referenced through the

isParked Relationship.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": {
"type": "Property",
"value": "Mercedes"
},
"street": {
"type": "LanguageProperty",
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt

}

},
"isParked": {
"type": "Relationship",
"objectType": "OffStreetParking",
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Person:Bob"

}

},
"category": {
"type": "VocabProperty",
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"type": "ListProperty",
"valueList": [300, 300, 120, 6],
"valueType": "Integer",
"unitCode": "MMT"
},
"passengers": {
"type": "Relationship",
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"type": "ListRelationship",
"objectType": "City",
"objectList": [
{"object": "urn:ngsi-ld:City:Antwerp"},
{"object": "urn:ngsi-ld:City:Rotterdam"}
{"object": "urn:ngsi-ld:City:Amsterdam"}

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }


Normalized Representation when inline Linked Entity retrieval is used

When inline Linked Entity retrieval (see clause 4.5.23.2) is specified, any Relationships which target Entities stored locally or include an objectType Attribute are returned in an expanded format. Attributes of type "Relationship" are returned with an additional entity sub-Attribute, which holds the normalized Linked Entity data corresponding to the Relationship's target object URI. Attributes of type "ListRelationship" are returned with an additional entityList sub-Attribute which in turn holds an ordered array of the normalized Linked

Entities corresponding to the target "objectList" URIs.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": {
"type": "Property",
"value": "Mercedes"
},
"street": {
"type": "LanguageProperty",
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt

}

},
"isParked": {
"type": "Relationship",
"objectType": "OffStreetParking",
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"entity": {
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": {
"value": "Top Parking",
"type": "Property"
},
"operatedBy": {
"object" "urn:ngsi-ld:Company:BigParkingCorp",
"type": "Relationship"

},

},
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"

}

},
"category": {
"type": "VocabProperty",
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"type": "ListProperty",
"valueList": [300, 300, 120, 6],
"valueType": "Integer",
"unitCode": "MMT"
},
"passengers": {
"type": "Relationship",
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"],
"entity": [
{
"id": "urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": {
"value": "Alice",
"type": "Property"

} }, {

"id": "urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": {
"value": "Bob",
"type": "Property"

} } ]


},
"route": {
"type": "ListRelationship",
"objectType": "City",
"objectList": [
{"object": "urn:ngsi-ld:City:Antwerp"},
{"object": "urn:ngsi-ld:City:Rotterdam"}
{"object": "urn:ngsi-ld:City:Amsterdam"}
],
"entityList": [
{
"id": "urn:ngsi-ld:City:Antwerp",
"type": "City",
"name": {
"value": "Antwerp",
"type": "Property"

} }, {

"id": "urn:ngsi-ld:City:Rotterdam",
"type": "City",
"name": {
"value": "Rotterdam",
"type": "Property"

} }, {

"id": "urn:ngsi-ld:City:Amsterdam",
"type": "City",
"name": {
"value": "Amsterdam",
"type": "Property"

} } ]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Normalized Representation when flattened Linked Entity retrieval is used

When flattened Linked Entity retrieval (see clause 4.5.23.3) is specified, an array of normalized Entities is returned. Whenever a Relationship Attribute targets an Entity stored locally or includes an objectType, an additional normalized Linked Entity holding data corresponding to the Relationship's target object URI is appended to the array. For Attributes of type "ListRelationship", an array of normalized Linked Entities is appended, which hold the data corresponding to each of the target URIs found within its objectList.

[ {

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": {
"type": "Property",
"value": "Mercedes"
},
"street": {
"type": "LanguageProperty",
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt

}

},
"isParked": {
"type": "Relationship",
"objectType": "OffStreetParking",
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Person:Bob"

}


},
"category": {
"type": "VocabProperty",
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"type": "ListProperty",
"valueList": [300, 300, 120, 6],
"valueType": "Integer"
"unitCode": "MMT"
},
"passengers": {
"type": "Relationship",
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"type": "ListRelationship",
"objectType": "City",
"objectList": [
{"object": "urn:ngsi-ld:City:Antwerp"},
{"object": "urn:ngsi-ld:City:Rotterdam"}
{"object": "urn:ngsi-ld:City:Amsterdam"}

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": {
"value": "Top Parking",
"type": "Property"
},
"operatedBy": {
"object": "urn:ngsi-ld:Company:BigParkingCorp",
"type": "Relationship"
},
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": {
"value": "Alice",
"type": "Property"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": {
"value": "Bob",
"type": "Property"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",


"http://example.org/ngsi-ld/latest/parking.jsonld", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Antwerp",
"type": "City",
"name": {
"value": "Antwerp",
"type": "Property"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Rotterdam",
"type": "City",
"name": {
"value": "Rotterdam",
"type": "Property"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Amsterdam",
"type": "City",
"name": {
"value": "Amsterdam",
"type": "Property"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

Normalized Representation when Language Filter is used

When the Language Filter (see clause 4.15) is used, Properties of type "LanguageProperty" are returned as type "Property", and their languageMaps are reduced to simple strings. For example if the language filter lang=fr is

specified, only the value for French language is present.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": {
"type": "Property",
"value": "Mercedes"
},
"street": {
"type": "Property",
"value": "Grand Place",
"lang": "fr"
},
"isParked": {
"type": "Relationship",
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Person:Bob"

}

}, "category": {


"type": "VocabProperty", "vocab": "non-commercial"

},
"tyreTreadDepths": {
"type": "ListProperty",
"valueList": [300, 300, 120, 6],
"valueType": "Integer",
"unitCode": "MMT"
},
"passengers": {
"type": "Relationship",
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"type": "ListRelationship",
"objectType": "City",
"objectList": [
{"object": "urn:ngsi-ld:City:Antwerp"},
{"object": "urn:ngsi-ld:City:Rotterdam"}
{"object": "urn:ngsi-ld:City:Amsterdam"}

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Concise Representation

The concise representation is a terser, lossless form of the normalized representation, where redundant Attribute type

members are omitted and the following rules are applied:

- Every Property without further sub-attributes is represented directly by the Property value only.

- Every Property that includes further sub-attributes is represented by a value key-value pair.

- Every GeoProperty without further sub-attributes is represented by the GeoProperty's GeoJSON representation only.

- Every GeoProperty that includes further sub-attributes is represented by a value key-value pair.

- Every LanguageProperty is represented by a languageMap key-value pair.

- Every ListProperty is represented directly by the array of Property values.

- Every JsonProperty is represented by a json the value of which is raw JSON which is not available for

JSON-LD representation.

- Every VocabProperty is represented by a vocab the value of which is a compacted URI.

- Every Relationship is represented by an object key-value pair.

- Every ListRelationship is represented by an array of URIs.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": {
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt"

}

},
"isParked": {
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"objectType": "OffStreetParking",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"


}

},
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"valueList": [300, 300, 120, 6],
"valueType": "Integer"
"unitCode": "MMT"
},
"passengers": {
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"objectType": "City",
"objectList": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Concise Representation when inline Linked Entity retrieval is used

When inline Linked Entity retrieval (see clause 4.5.23.2) is specified, any Relationships which target Entities stored locally or include an objectType Attribute are returned in an expanded format. The concise Linked Entity data corresponding to the Relationship's target object URI is returned within an entity sub-Attribute. Attributes of type "ListRelationship" are returned within an entityList sub-Attribute which in turn holds an ordered array of the

Linked Entities in the concise format corresponding to each of the target objectList URIs.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": {
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt"

}

},
"isParked": {
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"objectType": "OffStreetParking",
"entity": {
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": "Top Parking",
"operatedBy": {
"object": "urn:ngsi-ld:Company:BigParkingCorp"

}

},
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"

}

},
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"valueList": [300, 300, 120, 6],
"valueType": "Integer",
"unitCode": "MMT"
},
"passengers": {
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"],
"entity": [


{

"id": "urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": "Alice"

}, {

"id": "urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": "Bob"

} ]

},
"route": {
"objectType": "City",
"objectList": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"
],
"entityList": [
{

"id": "urn:ngsi-ld:City:Antwerp", "type": "City",

"name": " Antwerp"

}, {

"id": "urn:ngsi-ld:City:Rotterdam", "type": "City",

"name": "Rotterdam

}, {

"id": "urn:ngsi-ld:City:Amsterdam", "type": "City",

"name": "Amsterdam"

} ]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Concise Representation when flattened Linked Entity retrieval is used

When flattened Linked Entity retrieval (see clause 4.5.23.3) is specified, an array of concise Entities is returned. Whenever a Relationship Attribute targets an Entity stored locally or includes an objectType, an additional concise Linked Entity holding data corresponding to the Relationship's target object URI is appended to the response. For

Attributes of type "ListRelationship", an array of concise Linked Entities is appended to the response which hold the data corresponding to each of the target URIs found within its objectList.

[ {

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": {
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt"

}

},
"isParked": {
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"objectType": "OffStreetParking",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"

}

},
"category": {
"vocab": "non-commercial"


},
"tyreTreadDepths": {
"valueList": [300, 300, 120, 6],
"valueType": "Integer",

"unitCode": "MMT"

},
"passengers": {
"objectType": "Person",
"object": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"objectType": "City",
"objectList": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": "Top Parking",
"operatedBy": {
"object" "urn:ngsi-ld:Company:BigParkingCorp",
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": " urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": "Alice",
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": " urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": "Bob",
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Antwerp",
"type": "City",
"name": " Antwerp",
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Rotterdam",
"type": "City",
"name": "Rotterdam",
"@context": [


"http://example.org/ngsi-ld/latest/commonTerms.jsonld", "http://example.org/ngsi-ld/latest/vehicle.jsonld", "http://example.org/ngsi-ld/latest/parking.jsonld", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:City:Amsterdam",
"type": "City",
"name": "Amsterdam",
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

Concise representation when Language Filter is used

The rules apply as defined in the previous examples. For example if the language filter lang=fr is specified.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes"
},
"street": {
"value": "Grand Place",
"lang": "fr"
},
"isParked": {
"objectType": "OffStreetParking",
"object": "urn:ngsi-ld:OffStreetParking:Downtown1",
"observedAt": "2017-07-29T12:00:04Z",
"providedBy": {
"object": "urn:ngsi-ld:Person:Bob"

}

},
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": {
"valueList": [300, 300, 120, 6],
"valueType": "Integer",
"unitCode": "MMT"
},
"passengers": {
"objectType": "Person",
"objectList": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"]
},
"route": {
"objectType": "City",
"objectList": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"

]

},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Simplified Representation

The simplified representation is a collapsed, lossy representation of an Entity, which focuses on Property Values and

Relationship objects present at the first level of the graph only.

{

"id": "urn:ngsi-ld:Vehicle:A4567", "type": "Vehicle", "brandName": "Mercedes",


"street": { "languageMap": { "fr": "Grand Place", "nl": "Grote Markt"

}

}
"isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": [300, 300, 120, 6],
"passengers": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"],
"route": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"
],
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Simplified Representation when inline Linked Entity retrieval is used

When inline Linked Entity retrieval (see clause 4.5.23.2) is specified, any Relationships which target Entities stored locally or include an objectType Attribute are returned as a JSON object holding key-value pairs corresponding to the data from the Relationship's target object URI in simplified format. Attributes of type "ListRelationship" are returned as array of JSON objects each holding key-value pairs corresponding to the data obtained from the target

objectList URIs.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": {
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt"

}

},
"isParked": {
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": " OffStreetParking",
"name": "Top Parking",
"operatedBy": "urn:ngsi-ld:Company:BigParkingCorp"
},
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": [300, 300, 120, 6],
"passengers": [
{
"id": "urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": "Alice"

}, {

"id": "urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": "Bob"

}

],
"route": [
{
"id": "urn:ngsi-ld:City:Antwerp",
"type": "City",
"name": " Antwerp"

}, {

"id": "urn:ngsi-ld:City:Rotterdam",
"type": "City",
"name": "Rotterdam

}, {


"id": "urn:ngsi-ld:City:Amsterdam",
"type": "City",
"name": "Amsterdam"

}

],
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Simplified Representation when flattened Linked Entity retrieval is used

When flattened Linked Entity retrieval (see clause 4.5.23.3) is specified, an array of JSON Objects is returned. Whenever a Relationship Attribute targets an Entity stored locally or includes an objectType, an additional JSON Object of key-value pairs holding data corresponding to the Relationship's target object URI is appended to the response. For

Attributes of type "ListRelationship", an array of JSON Objects each holding key-value pairs corresponding to the data obtained from the target objectList URIs is appended to the response.

[ {

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": {
"languageMap": {
"fr": "Grand Place",
"nl": "Grote Markt"

}

}
"isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
"category": {
"vocab": "non-commercial"
},
"tyreTreadDepths": [300, 300, 120, 6],
"passengers": ["urn:ngsi-ld:Person:Alice", "urn:ngsi-ld:Person:Bob"],
"route": [
"urn:ngsi-ld:City:Antwerp",
"urn:ngsi-ld:City:Rotterdam",
"urn:ngsi-ld:City:Amsterdam"
],
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": " OffStreetParking",
"name": "Top Parking",
"operatedBy": "urn:ngsi-ld:Company:BigParkingCorp"

}, {

"id": "urn:ngsi-ld:Person:Alice",
"type": "Person",
"name": "Alice"

} {

"id": "urn:ngsi-ld:Person:Bob",
"type": "Person",
"name": "Bob"

}, {

"id": "urn:ngsi-ld:City:Antwerp",
"type": "City",
"name": " Antwerp"

}, {

"id": "urn:ngsi-ld:City:Rotterdam",
"type": "City",
"name": "Rotterdam

},


{

"id": "urn:ngsi-ld:City:Amsterdam",
"type": "City",
"name": "Amsterdam"

} ]

Simplified Representation of Natural Language Attributes

The simplified natural language representation is a collapsed representation of an Entity, which focuses on Property Values and Relationship objects present at the first level of the graph, and where languageMaps are reduced to simple string properties. For example if the language filter lang=fr is specified.

{

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"brandName": "Mercedes",
"street": "Grand Pla

… (truncated; see full clause in the PDF).

# Related

* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.5.23.2](/framework/data-representation/inline-linked-entity-representation.md)
* [Clause 4.5.23.3](/framework/data-representation/flattened-linked-entity-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity Representation
