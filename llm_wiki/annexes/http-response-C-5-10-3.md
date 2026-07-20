---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/json Link:; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\" { \"id\": \"urn:ngsi-ld:AttributeList:56534657\", \"type\": \"AttributeList\", \"attributeLis"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.10.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.10.3"
---

200 OK
Content-Type: application/json
Link:; rel="http://www.w3.org/ns/json-ld#context";
type="application/ld+json"

{

"id": "urn:ngsi-ld:AttributeList:56534657",
"type": "AttributeList",
"attributeList": [
"brandName",
"isParked",
"location",
"speed",
"http://example.org/parking/status"

| | | "brandName", | |
| --- | --- | --- | --- |
| | | "isParked", | |
| | | "location", | |

]

}
NOTE: The attribute names that can be found in the provided @context are given as short names, the others as
Fully Qualified Names (FQNs).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.10.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
