---
type: Reference
title: "Retrieve Available Attribute Information"
description: "C.5.12.1 Introduction EXAMPLE: Give back the details of the attribute named brandName (for which entity instances with an attribute of this name are currently available in the NGSI-LD system)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.12
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.12"
---

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

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Available Attribute Information
