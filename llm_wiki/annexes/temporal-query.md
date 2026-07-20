---
type: Reference
title: "Temporal Query"
description: "C.5.5.1 Introduction EXAMPLE 1: Give back the temporal evolution of the attribute speed of Entities of type \"Vehicle\" whose brandName attribute is not \"Mercedes\" between the 1 st of August at noon and"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.5
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.5"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Query
