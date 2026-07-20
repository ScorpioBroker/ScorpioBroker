---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/ld+json [ { \"id\": \"urn:ngsi-ld:Vehicle:B9211\", \"type\": \"Vehicle\", \"scope\": { \"type\": \"Property\", \"values\": [ [ \"/Madrid/Centro\", \"2018-08-01T11:00:00Z\" ] ] }, \"speed\":"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.16.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.16.3"
---

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

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.16.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
