---
type: Reference
title: "Temporal Query (Aggregated Representation)"
description: "C.5.14.1 Introduction EXAMPLE: Give back the maximum and average speed of Entities of type \"Vehicle\" whose brandName attribute is not \"Mercedes\" between the 1 st of August at noon and the 1 st of Augu"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.14
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.14"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Query (Aggregated Representation)
