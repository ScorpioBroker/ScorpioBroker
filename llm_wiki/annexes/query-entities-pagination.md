---
type: Reference
title: "Query Entities (Pagination)"
description: "C.5.4.1 Introduction EXAMPLE: Give back all the Entities of type \"Vehicle\"."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.4
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.4"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Entities (Pagination)
