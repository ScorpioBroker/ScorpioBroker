---
type: Reference
title: "Temporal Query (Simplified Representation)"
description: "C.5.6.1 Introduction EXAMPLE: Give back the temporal evolution of the speed attribute for Entities of type \"Vehicle\" whose brandName attribute is not \"Mercedes\" between the 1 st of August at noon and"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.6
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.6"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Query (Simplified Representation)
