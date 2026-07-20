---
type: Reference
title: "HTTP REST API Examples"
description: "C.5.1 Introduction This clause introduces some simple usage examples of the NGSI-LD API (HTTP REST binding)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5"
---

C.5.1 Introduction

This clause introduces some simple usage examples of the NGSI-LD API (HTTP REST binding). They are not intended to be exhaustive but just a sample for helping readers to understand better the present document. ETSI ISG CIM published a Developer's Primer with many more examples, see ETSI GR CIM 008 [i.21].

C.5.2 Create Entity of Type Vehicle

C.5.2.1 HTTP Request

POST /ngsi-ld/v1/entities/
Content-Type: application/ld+json
Content-Length: 556


C.5.2.2 HTTP Response

201 Created Location: /ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A4567

C.5.3 Query Entities

C.5.3.1 Introduction

EXAMPLE: Give back all the Entities of type "Vehicle" whose brandName attribute is not "Mercedes". Only give back the brandName attribute and provide the data in the NGSI-LD Simplified Format.


C.5.3.2 HTTP Request

GET /ngsi-ld/v1/entities/?type=Vehicle&q=brandName!="Mercedes"&format=simplified

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.3.3 HTTP Response

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"brandName": "Volvo",
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

C.5.4 Query Entities (Pagination)

C.5.4.1 Introduction

EXAMPLE: Give back all the Entities of type "Vehicle". Only give back the brandName attribute and provide the data in the NGSI-LD Simplified Format. Limit the number of entities retrieved to 2.

C.5.4.2 HTTP Request

GET /ngsi-ld/v1/entities/?type= Vehicle&format=simplified&limit=2

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.4.3 HTTP Response

200 OK

Content-Type: application/ld+json

Link:; rel="next"; type="application/ld+json"

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"brandName": "Volvo",
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:Vehicle:A456", "type": "Vehicle", "brandName": "Mercedes",

"@context": [ "http://example.org/ngsi-ld/latest/vehicle.jsonld", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

C.5.5 Temporal Query

C.5.5.1 Introduction

EXAMPLE 1: Give back the temporal evolution of the attribute speed of Entities of type "Vehicle" whose brandName attribute is not "Mercedes" between the 1 st of August at noon and the 1 st of August

at 01 PM.

EXAMPLE 2: Give back the temporal evolution of the attribute speed and brandName of Entities of type "Vehicle" whose brandName attribute is not "Mercedes" between the 1 st of August at noon and the 1 st of August at 01 PM. As brandName attribute does not have any temporal evolution, brandName attribute is omitted in the response.

C.5.5.2 HTTP Request #1

GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018-

08-01T12:00:00Z&endTimeAt=2018-08-01T13:00:00Z

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.5.3 HTTP Response #1

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"speed": [
{

| | "speed": [ | | | |
| --- | --- | --- | --- | --- |
| | | { | | |
| | | "type": "Property", | | |
| | | "value": 120, | | |
| | | "observedAt": "2018-08-01T12:03:00Z" | | |

}, {

| | | { | | |
| --- | --- | --- | --- | --- |
| | | "type": "Property", | | |
| | | "value": 80, | | |
| | | "observedAt": "2018-08-01T12:05:00Z" | | |

}, {

| | | { | | |
| --- | --- | --- | --- | --- |
| | | "type": "Property", | | |
| | | "value": 100, | | |
| | | "observedAt": "2018-08-01T12:07:00Z" | | |
| | | } | | |
| | ], | | | |

}
],
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]


C.5.5.4 HTTP Request #2

GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed,brandName&timerel=between&tim eAt=2018-08-01T12:00:00Z&endTimeAt=2018-08-01T13:00:00Z

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.5.5 HTTP Response #2

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"speed": [
{

| | "speed": [ | | | |
| --- | --- | --- | --- | --- |
| | | { | | |
| | | "type": "Property", | | |
| | | "value": 120, | | |
| | | "observedAt": "2018-08-01T12:03:00Z" | | |

}, {

| | | { | | |
| --- | --- | --- | --- | --- |
| | | "type": "Property", | | |
| | | "value": 80, | | |
| | | "observedAt": "2018-08-01T12:05:00Z" | | |

}, {

| | | { | | |
| --- | --- | --- | --- | --- |
| | | "type": "Property", | | |
| | | "value": 100, | | |
| | | "observedAt": "2018-08-01T12:07:00Z" | | |
| | | } | | |
| | ], | | | |

}
],
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

C.5.6 Temporal Query (Simplified Representation)

C.5.6.1 Introduction

EXAMPLE: Give back the temporal evolution of the speed attribute for Entities of type "Vehicle" whose brandName attribute is not "Mercedes" between the 1 st of August at noon and the 1 st of August

at 01 PM. Simplified representation is required.

C.5.6.2 HTTP Request

GET /ngsi
ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018-
08-01T12:00:00Z&endTimeAt=2018-08-01T13:00:00Z&format=temporalValues

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"


C.5.6.3 HTTP Response

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"speed": {
"type": "Property",
"values": [
[

120, "2018-08-01T12:03:00Z" ], [

80, "2018-08-01T12:05:00Z" ], [

100, "2018-08-01T12:07:00Z"

]

]
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

C.5.7 Retrieve Available Entity Types

C.5.7.1 Introduction

EXAMPLE: Give back all entity types for which entity instances are currently available in the NGSI-LD

system.

C.5.7.2 HTTP Request

GET /ngsi-ld/v1/types

Accept: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.7.3 HTTP Response

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

{

"id": "urn:ngsi-ld:EntityTypeList:34534657",
"type": "EntityTypeList",
"typeList": [
"Vehicle",
"OffStreetParking",
"http://example.org/parking/ParkingSpot"

] }

NOTE: All entity types that can be found in the provided @context are given as short names, the others as Fully

Qualified Names (FQNs).

C.5.8 Retrieve Details of Available Entity Types

C.5.8.1 Introduction

EXAMPLE: Give back the details of all entity types for which entity instances are currently available in the

NGSI-LD system.

C.5.8.2 HTTP Request

GET /ngsi-ld/v1/types?details=true

Accept: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.8.3 HTTP Response

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

[ {

"id": "http://example.org/vehicle/Vehicle",
"type": "EntityType",
"typeName": "Vehicle",
"attributeNames": [
"brandName",
"isParked",
"location",
"speed"

| | | "brandName", | |
| --- | --- | --- | --- |
| | | "isParked", | |
| | | "location", | |

] }, {

"id": "http://example.org/parking/OffStreetParking",
"type": "EntityType",
"typeName": "OffStreetParking",
"attributeNames": [
"availableSpotNumber",
"isNextToBuilding",
"location",
"totalSpotNumber"

] }, {

"id": "http://example.org/parking/ParkingSpot",
"type": "EntityType",
"typeName": "http://example.org/parking/ParkingSpot",
"attributeNames":[
"location",
"http://example.org/parking/status"

] } ]

NOTE: The type name of all entity types and all attribute names that can be found in the provided @context are given as short names, the others as Fully Qualified Names (FQNs). The id is always an FQN.


C.5.9 Retrieve Available Entity Type Information

C.5.9.1 Introduction

EXAMPLE: Give back the details of entity type "Vehicle" (for which entity instances are currently available in the NGSI-LD system).

C.5.9.2 HTTP Request

GET /ngsi-ld/v1/types/Vehicle

[Alternative with FQN: GET /ngsi-ld/v1/attributes/http%3A%2F%2Fexample.org%2Fvehicle%2FVehicle]

Accept: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.9.3 HTTP Response

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

{

"id": "http://example.org/vehicle/Vehicle",
"type": "EntityTypeInfo",
"typeName": "Vehicle",
"entityCount": 2,
"attributeDetails": [
{
"id": "http://example.org/vehicle/brandName",
"type": "Attribute",
"attributeName": "brandName",
"attributeTypes": [
"Property"

] }, {

"id": "http://example.org/vehicle/isParked",
"type": "Attribute",
"attributeName": "isParked",
"attributeTypes": [
"Relationship"

] }, {

"id": "https://uri.etsi.org/ngsi-ld/location",
"type": "Attribute",
"attributeName": "location",
"attributeTypes": [
"GeoProperty"

] }, {

"id": "http://example.org/vehicle/speed",
"type": "Attribute",
"attributeName": "speed",
"attributeTypes": [
"Property"

] } ] }

C.5.10 Retrieve Available Attributes

C.5.10.1 Introduction

EXAMPLE: Give back all attribute names for which entity instances are currently available in the NGSI-LD system that have an attribute with the respective name.

C.5.10.2 HTTP Request

GET /ngsi-ld/v1/attributes
Accept: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

C.5.10.3 HTTP Response

200 OK
Content-Type: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

{

"id": "urn:ngsi-ld:AttributeList:56534657",
"type": "AttributeList",
"attributeList": [
"brandName",
"isParked",
"location",
"speed",
"http://example.org/parking/status"

| | | "brandName", | |
| --- | --- | --- | --- |
| | | "isParked", | |
| | | "location", | |

]

}
NOTE: The attribute names that can be found in the provided @context are given as short names, the others as
Fully Qualified Names (FQNs).

C.5.11 Retrieve Details of Available Attributes

C.5.11.1 Introduction

EXAMPLE: Give back the details of all attributes for which entity instances are currently available in the NGSI-LD system to which an attribute with the respective attribute name belongs.

C.5.11.2 HTTP Request

GET /ngsi-ld/v1/attributes?details=true
Accept: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"


C.5.11.3 HTTP Response

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

[ {

"id": "http://example.org/vehicle/brandName",
"type": "Attribute",
"attributeName": "brandName",
"typeNames": [
"Vehicle"

] }, {

"id": "http://example.org/vehicle/isParked",
"type": "Attribute",
"attributeName": "isParked",
"typeNames": [
"Vehicle"

] }, {

"id": "https://uri.etsi.org/ngsi-ld/location",
"type": "Attribute",
"attributeName": "location",
"typeNames": [
"Vehicle",
"OffStreetParking",
"http://example.org/parking/ParkingSpot"

] }, {

"id": "http://example.org/vehicle/speed",
"type": "Attribute",
"attributeName": "speed",
"typeNames": [
"Vehicle"

] }, {

"id": "http://example.org/parking/status",
"type": "Attribute",
"attributeName": "http://example.org/parking/status",
"typeNames": [
"http://example.org/parking/ParkingSpot"

] } ]

NOTE: The attribute name and all type names that can be found in the provided @context are given as short names, the others as Fully Qualified Names (FQNs). The id is always an FQN.

C.5.12 Retrieve Available Attribute Information

C.5.12.1 Introduction

EXAMPLE: Give back the details of the attribute named brandName (for which entity instances with an attribute of this name are currently available in the NGSI-LD system).

C.5.12.2 HTTP Request

GET /ngsi-ld/v1/attributes/brandName

[Alternative with FQN: GET /ngsi-ld/v1/attributes/http%3A%2F%2Fexample.org%2Fvehicle%2FbrandName]


Accept: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

C.5.12.3 HTTP Response

200 OK
Content-Type: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

{

"id": "http://example.org/vehicle/brandName",
"type": "Attribute",
"attributeName": "brandName",
"attributeTypes": ["Property"],
"typeNames": ["Vehicle"],
"attributeCount": 2

}

C.5.13 Query Entities (Natural Language Filtering)

C.5.13.1 Introduction

EXAMPLE: Give back all the Entities of type "Vehicle" where the marque attribute in British English is "Vauxhall Viva". Only give back the marque attribute and provide the data in the NGSI-LD Simplified Format and only return language strings in German.

C.5.13.2 HTTP Request

GET /ngsi-ld/v1/entities/?type=Vehicle&attrs=marque&q=marque[en-GB]== "Vauxhall Viva"&format
=simplified&lang=de
Accept: application/ld+json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

C.5.13.3 HTTP Response

200 OK Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"marque": "Opel Karl",
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]


C.5.14 Temporal Query (Aggregated Representation)

C.5.14.1 Introduction

EXAMPLE: Give back the maximum and average speed of Entities of type "Vehicle" whose brandName attribute is not "Mercedes" between the 1 st of August at noon and the 1 st of August at 01 PM,

aggregated by periods of 4 minutes.

C.5.14.2 HTTP Request

GET /ngsi ld/v1/temporal/entities/?type=Vehicle&q=brandName!=Mercedes&attrs=speed&timerel=between&timeAt=2018-

08-01T12:00:00Z&endTimeAt=2018-08-

01T13:00:00Z&aggrMethods=max,avg&aggrPeriodDuration=PT4M&format=aggregatedValues

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.14.3 HTTP Response

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"speed": {
"type": "Property",
"max": [
[

120, "2018-08-01T12:00:00Z", "2018-08-01T12:04:00Z" ], [

100, "2018-08-01T12:04:00Z", "2018-08-01T12:08:00Z"

]
],
"avg": [
[

120, "2018-08-01T12:00:00Z", "2018-08-01T12:04:00Z" ], [

90, "2018-08-01T12:04:00Z", "2018-08-01T12:08:00Z"

]

]
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]


C.5.15 Scope Queries

C.5.15.1 Introduction

EXAMPLE: Give back all the Entities of type "OffStreetParking" that are within the Scope /Madrid/Centro or /Madrid/Cortes.

C.5.15.2 HTTP Request

GET /ngsi-ld/v1/entities/?type=OffStreetParking&scopeQ="/Madrid/Centro,/Madrid/Cortes"

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.15.3 HTTP Response

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"scope": "/Madrid/Centro",
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
"coordinates": [
-8.5,
41.2

] }

},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:OffStreetParking:Corte4",
"type": "OffStreetParking",
"scope": [
"/Madrid/Cortes",
"/Company894/UnitC"

], "name": {


"type": "Property", "value": "Corte4"

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
"value": 100
},
"location": {
"type": "GeoProperty",
"value": {
"type": "Point",
"coordinates": [
-8.6,
41.3

] }

},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

C.5.16 Temporal Scope Queries

C.5.16.1 Introduction

EXAMPLE: Give back the speed of all the Entities of type "Vehicle" that have been within the Scope /Madrid/Centro between the 1 st of August 2018 at noon and the 1 st of August 2018 at 01 PM. Note that the value of the Scope has to match for the given timeframe, which means it is possible that it has been set before, e.g. on 1 st of August 2018 at 11 AM.

C.5.16.2 HTTP Request

GET /ngsi-ld/v1/temporal/entities/?type=Vehicle&attrs=speed,scope&timerel=between&timeAt=2018-08-01T12:00:00Z&endTimeAt=2018-08-01T13:00:00Z&scopeQ="/Madrid/Centro"

Accept: application/ld+json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

C.5.16.3 HTTP Response

200 OK

Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:B9211",
"type": "Vehicle",
"scope": {
"type": "Property",
"values": [


[

"/Madrid/Centro", "2018-08-01T11:00:00Z"

]

]
},
"speed": {
"type": "Property",
"values": [
[

30, "2018-08-01T12:03:00Z" ], [

60, "2018-08-01T12:05:00Z" ], [

50, "2018-08-01T12:07:00Z"

]

]
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }, {

"id": "urn:ngsi-ld:Vehicle:A8311",
"type": "Vehicle",
"scope": {
"type": "Property",
"values": [
[

[

"/Madrid/Centro", "/Company123/UnitA" ], "2018-08-01T12:10:00Z"

]

]
},
"speed": {
"type": "Property",
"values": [
[

40, "2018-08-01T12:12:00Z" ], [

60, "2018-08-01T12:14:00Z" ], [

50, "2018-08-01T12:16:00Z"

]

]
},
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

Vehicle B9211 has already been within the Scope /Madrid/Centro before the beginning of the request interval, whereas Vehicle A8311 only entered the Scope within the request interval. Thus in the latter case only Property values are included that have been observed after the Scope has become valid.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP REST API Examples
