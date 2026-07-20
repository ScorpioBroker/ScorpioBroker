---
type: Reference
title: "Introduction"
description: "G.1.0 Foreword Since Internationalization is not core to context information management, any direct support within NGSI-LD systems is limited."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.1"
---

G.1.0 Foreword

Since Internationalization is not core to context information management, any direct support within NGSI-LD systems is limited. Annex G proposes a series of best practices for maintaining, querying and displaying interoperable

internationalized data.

The content of the @context utilized for the referred Entities within these examples uses pre-existing URNs used for

internationalization and is as follows:

{

"inLanguage": "http://schema.org/inLanguage", "sameAs": "http://schema.org/sameAs"

}

G.1.1 Associating an Entity with a Natural Language

Where a context Entity is associated with a single natural language, include a well-defined Property indicating the natural language of the content. For example an Event taking place in French may be defined as follows:

{

"type": "Event",
"id": "urn:ngsi-ld:Event:bonjourLeMonde",
"name": {
"type": "Property",
"value": "Bonjour le Monde"

}, "description": { "type": "Property", "value": "«Bonjour le monde» sont les mots traditionnellement écrits par un programme informatique simple" }, "inLanguage": { "type": "Property", "value": "fr"

} }

G.1.2 Associating a Property with a Natural Language

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

G.1.3 Associating as equivalent entity

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

[1] [ETSI GS CIM 009 V1.9.1 clause G.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
