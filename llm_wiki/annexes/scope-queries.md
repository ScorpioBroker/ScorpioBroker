---
type: Reference
title: "Scope Queries"
description: "C.5.15.1 Introduction EXAMPLE: Give back all the Entities of type \"OffStreetParking\" that are within the Scope /Madrid/Centro or /Madrid/Cortes."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.15
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.15"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Scope Queries
