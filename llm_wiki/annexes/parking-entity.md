---
type: Reference
title: "Parking Entity"
description: "Normalized Representation The normalized representation is a lossless representation of an Entity, where every Property is defined by a type and a value and every Relationship is defined by a type and"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.2.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.2.3"
---

Normalized Representation

The normalized representation is a lossless representation of an Entity, where every Property is defined by a type and a value and every Relationship is defined by a type and an object.

Below there is a representation of an Entity of Type "OffStreetParking". It can be observed that the @context is composed of two different elements, the Core one and the vocabulary-specific one.

{

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": {
"type": "Property",
"value": "Downtown One"
},
"availableSpotNumber": {
"type": "Property",
"value": 121,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": {
"type": "Property",
"value": 0.7
},
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Camera:C1"

}

},
"totalSpotNumber": {
"type": "Property",
"value": 200
},
"location": {
"type": "GeoProperty",
"value": {
"type": "Point",
"coordinates": [-8.5, 41.2]

}

},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Concise Representation

The concise representation is a terser, lossless form of the normalized representation, where redundant Attribute type

members are omitted and the following rules are applied:

- Every Property without further sub-attributes is represented by the Property value only.

- Every Property that includes further sub-attributes is represented by a value key-value pair.

- Every GeoProperty without further sub-attributes is represented by the GeoProperty's GeoJSON representation only.

- Every GeoProperty that includes further sub-attributes is represented by a value key-value pair.

- Every LanguageProperty is defined by a languageMap key-value pair.

- Every VocabProperty is represented by a vocab the value of which is a compacted URI.

- Every Relationship is defined by an object key-value pair.

{

| | | "value": 121, | | |
| --- | --- | --- | --- | --- |
| | | "observedAt": "2017-07-29T12:05:02Z", | | |
| | | "reliability": 0.7, | | |
| | | "providedBy": { | | |
| | | | "object": "urn:ngsi-ld:Camera:C1" | |
| | | } | | |
| | }, | | | |

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": "Downtown One",
"availableSpotNumber": {
}
},
"totalSpotNumber": 200,
"location": {
"type": "Point",
"coordinates": [
-8.5,
41.2
]
},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Simplified representation

The Simplified Representation (also known as "keyValues") is a lossy, collapsed representation of an Entity, which focuses on Property Values and Relationship objects present at the first level of the graph only.

| { | | | | |
| --- | --- | --- | --- | --- |
| | | "id": "urn:ngsi-ld:OffStreetParking:Downtown1", | | |
| | | "type": "OffStreetParking", | | |
| | | "name": "Downtown One", | | |
| | | "availableSpotNumber": 121, | | |
| | | "totalSpotNumber": 200, | | |
| | | "location": { | | |

"type": "Point", "coordinates": [-8.5, 41.2]

},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Normalized GeoJSON Representation

The normalized GeoJSON representation of a single Entity is defined as a single GeoJSON Feature object as follows:

{

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",


"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.51, 41.1]
},
"properties": {
"type": "OffStreetParking",
"name": {
"type": "Property",
"value": "Downtown One"
},
"availableSpotNumber": {
"type": "Property",
"value": 121,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": {
"type": "Property",
"value": 0.7
},
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Camera:C1"
}
},
"location": {
"type": "GeoProperty",
"value": {
"type": "Point",
"coordinates": [-8.51, 41.1]

}

},
"totalSpotNumber": {
"type": "Property",
"value": 200
},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } }

The GeoJSON representation of multiple Entities is defined as a GeoJSON FeatureCollection object containing an array of GeoJSON features corresponding to the individual Entity representations.

{

"type": "FeatureCollection",
"features": [
{
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.5, 41.1]
},
"properties": {
"type": "OffStreetParking",
"name": {
"type": "Property",
"value": "Downtown One"
},
"availableSpotNumber": {
"type": "Property",
"value": 121,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": {
"type": "Property",
"value": 0.7
},
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Camera:C1"

} }, "totalSpotNumber": { "type": "Property", "value": 200 },


"location": { "type": "GeoProperty", "value": { "type": "Point", "coordinates": [-8.51, 41.1]

} }

} }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown2",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.51, 41.1]
},
"properties": {
"type": "OffStreetParking",
"name": {
"type": "Property",
"value": "Downtown Two"
},
"availableSpotNumber": {
"type": "Property",
"value": 99,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": {
"type": "Property",
"value": 0.8
},
"providedBy": {
"type": "Relationship",
"object": "urn:ngsi-ld:Camera:C2"
}
},
"totalSpotNumber": {
"type": "Property",
"value": 100
},
"location": {
"type": "GeoProperty",
"value": {
"type": "Point",
"coordinates": [-8.51, 41.1]

} } }

}
],
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Concise GeoJSON Representation

The concise GeoJSON representation of a single Entity is defined as a single GeoJSON Feature object as follows:

{

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [
-8.51,
41.1
]
},
"properties": {
"type": "OffStreetParking",
"name": "Downtown One",
"availableSpotNumber": {
"value": 121,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": 0.7,
"providedBy": {
"object": "urn:ngsi-ld:Camera:C1"


}
},
"location": {
"type": "Point",
"coordinates": [
-8.51,
41.1
]
},
"totalSpotNumber": 200,
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } }

The concise GeoJSON representation of multiple Entities is defined as a GeoJSON FeatureCollection object containing an array of GeoJSON features corresponding to the individual Entity representations in concise GeoJSON format.

{

"type": "FeatureCollection",
"features": [
{
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [
-8.5,
41.1
]
},
"properties": {
"type": "OffStreetParking",
"name": "Downtown One",
"availableSpotNumber": {
"value": 121,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": 0.7,
"providedBy": {
"object": "urn:ngsi-ld:Camera:C1"
}
},
"totalSpotNumber": 200,
"location": {
"type": "Point",
"coordinates": [
-8.51,
41.1

] }

} }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown2",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [
-8.51,
41.1
]
},
"properties": {
"type": "OffStreetParking",
"name": "Downtown Two",
"availableSpotNumber": {
"value": 99,
"observedAt": "2017-07-29T12:05:02Z",
"reliability": 0.8,
"providedBy": {
"object": "urn:ngsi-ld:Camera:C2"
}
},
"totalSpotNumber": 100,
"location": {
"type": "Point",


"coordinates": [ -8.51, 41.1

] } }

}
],
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Simplified GeoJSON Representation

The simplified GeoJSON representation of a single Entity is defined as a single GeoJSON Feature object where the properties represent a collapsed representation of the Entity, which focuses on Property Values and Relationship objects

present at the first level of the graph.

{

"id": "urn:ngsi-ld:offstreetparking:Downtown1",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.51, 41.1]
},
"properties": {
"type": "OffStreetParking",
"name": "Downtown One",
"availableSpotNumber": 121,
"totalSpotNumber": 200,
"location": {
"type": "Point",
"coordinates": [-8.51, 41.1]
}
},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

The simplified GeoJSON representation of multiple Entities is defined as a GeoJSON FeatureCollection object containing an array of GeoJSON features corresponding to the individual Entity representations in simplified GeoJSON format.

{

"type": "FeatureCollection",
"features": [
{
"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.5, 41.2]
},
"properties": {
"type": "OffStreetParking",
"name": "Downtown One",
"availableSpotNumber": 121,
"totalSpotNumber": 200,
"location": {
"type": "Point",
"coordinates": [-8.5, 41.2]

}

} }, {

"id": "urn:ngsi-ld:OffStreetParking:Downtown2",
"type": "Feature",
"geometry": {
"type": "Point",
"coordinates": [-8.51, 41.1]
},
"properties": {


"type": "OffStreetParking",
"name": "Downtown Two",
"availableSpotNumber": 99,
"totalSpotNumber": 100,
"location": {
"type": "Point",
"coordinates": [-8.51, 41.1]

} }

}
],
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.2.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Parking Entity
