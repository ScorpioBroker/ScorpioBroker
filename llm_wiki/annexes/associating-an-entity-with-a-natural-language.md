---
type: Reference
title: "Associating an Entity with a Natural Language"
description: "Where a context Entity is associated with a single natural language, include a well-defined Property indicating the natural language of the content."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.1.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.1.1"
---

Where a context Entity is associated with a single natural language, include a well-defined Property indicating the natural language of the content. For example an Event taking place in French may be defined as follows:

{

"type": "Event",
"id": "urn:ngsi-ld:Event:bonjourLeMonde",
"name": {
"type": "Property",
"value": "Bonjour le Monde"

}, "description": { "type": "Property", "value": "«Bonjour le monde» sont les mots traditionnellement écrits par un programme informatique simple" }, "inLanguage": { "type": "Property", "value": "fr"

} }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.1.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Associating an Entity with a Natural Language
