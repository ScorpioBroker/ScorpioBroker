---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/ld+json [ { \"id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\", \"type\": \"OffStreetParking\", \"scope\": \"/Madrid/Centro\", \"name\": { \"type\": \"Property\", \"value\": \"Downtown One"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.15.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.15.3"
---

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

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.15.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
