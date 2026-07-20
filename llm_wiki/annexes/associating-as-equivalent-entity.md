---
type: Reference
title: "Associating as equivalent entity"
description: "Where equivalent context entities in multiple natural languages exist, they may be associated with each other through the use of a one-to-many relationship, where each relationship holds an additional"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.1.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.1.3"
---

Where equivalent context entities in multiple natural languages exist, they may be associated with each other through the use of a one-to-many relationship, where each relationship holds an additional sub-Property indicating the natural

language of the equivalent entities.

For example, three Events (such as a walking tour which is available in English, French and German) may be associated

to each other as follows:

{

"type": "Event",
"id": "urn:ngsi-ld:Event:bonjourLeMonde",
"name": {
"type": "Property",
"value": "Bonjour le Monde"
},
"sameAs": [
{
"type": "Relationship",
"datasetId" : "urn:ngsi-ld:Relationship:1",
"object": "urn:ngsi-ld:Event:helloWorld",
"inLanguage": {
"type": "Property",
"value": "en"

} }, {

"type": "Relationship",
"object": "urn:ngsi-ld:Event:halloWelt",
"inLanguage": {
"type": "Property",
"value": "de"

} } ] }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.1.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Associating as equivalent entity
