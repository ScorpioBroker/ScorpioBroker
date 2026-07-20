---
type: Reference
title: "Query Entities (Natural Language Filtering)"
description: "C.5.13.1 Introduction EXAMPLE: Give back all the Entities of type \"Vehicle\" where the marque attribute in British English is \"Vauxhall Viva\"."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.13
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.13"
---

C.5.13.1 Introduction

EXAMPLE: Give back all the Entities of type "Vehicle" where the marque attribute in British English is "Vauxhall Viva". Only give back the marque attribute and provide the data in the NGSI-LD Simplified Format and only return language strings in German.

C.5.13.2 HTTP Request

GET /ngsi-ld/v1/entities/?type=Vehicle&attrs=marque&q=marque[en-GB]== "Vauxhall Viva"&format
=simplified&lang=de
Accept: application/ld+json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

C.5.13.3 HTTP Response

200 OK Content-Type: application/ld+json

[ {

"id": "urn:ngsi-ld:Vehicle:A4567",
"type": "Vehicle",
"marque": "Opel Karl",
"@context": [
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] } ]

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Entities (Natural Language Filtering)
