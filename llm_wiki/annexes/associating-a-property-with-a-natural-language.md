---
type: Reference
title: "Associating a Property with a Natural Language"
description: "Where a Property of a context entity can be associated to one more natural language, include additional metadata as a sub-Property of that Property."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.1.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.1.2"
---

Where a Property of a context entity can be associated to one more natural language, include additional metadata as a sub-Property of that Property. For example, a Hotel with booking forms available in English, French and German may

be defined as follows:

{

"type": "Hotel,
"id": "urn:ngsi-ld:Hotel:XXXXX",
"name": {
"type": "Property",
"value": "Grand Hotel"


},

"bookingUrl": { "type": "Property", "value": [ "http://example.com/booking-in-french/", "http://example.com/booking-in-english/", "http://example.com/booking-in-german/"

],
"inLanguage": {
"type": "Property",
"value": ["fr", "en", "de" ]

} } }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.1.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Associating a Property with a Natural Language
