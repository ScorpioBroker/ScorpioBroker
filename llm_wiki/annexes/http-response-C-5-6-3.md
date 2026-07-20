---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/ld+json [ { \"id\": \"urn:ngsi-ld:Vehicle:B9211\", \"type\": \"Vehicle\", \"speed\": { \"type\": \"Property\", \"values\": [ [ 120, \"2018-08-01T12:03:00Z\" ], [ 80, \"2018-08-01T12:05:0"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.6.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.6.3"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.6.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
