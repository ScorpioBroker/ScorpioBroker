---
type: Reference
title: "Retrieve Available Entity Types"
description: "C.5.7.1 Introduction EXAMPLE: Give back all entity types for which entity instances are currently available in the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.7
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.7"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Available Entity Types
