---
type: NGSI-LD Clause
title: "NGSI-LD Fragments"
description: "When updating NGSI-LD elements (Entities, Attributes, Context Source Registrations or Subscriptions) it is necessary to have a means of describing a set of modifications to their content."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.4"
---

When updating NGSI-LD elements (Entities, Attributes, Context Source Registrations or Subscriptions) it is necessary to have a means of describing a set of modifications to their content. An NGSI-LD Fragment is a JSON Merge Patch document [16] and [i.10] which describes changes to be made to a target JSON-LD document using a syntax that closely mimics the document being modified. An NGSI-LD Fragment is a JSON-LD Object which shall include the following members:

- id (optional for certain bindings where it can be determined from the operation signature). It shall be equal to the id of the target (mutated) NGSI-LD element. Attribute Fragments do not contain explicit ids.

- type (optional for certain bindings where it can be determined from the operation signature). It shall contain the Type name(s) of the target NGSI-LD element.


- A member (following the same data representation and nesting structure) for each new member to be added to

the target NGSI-LD element.

- A member (following the same data representation and nesting structure) for each new member to be modified in the target NGSI-LD element, which value shall correspond to the new member value to be given.

EXAMPLE 1: The following Subscription Fragment allows the modification of a Subscription by changing its endpoint's URI:

{

"id": "urn:ngsi-ld:Subscription:MySubscription",
"type": "Subscription",
"endpoint": {
"uri": "http://example.org/newNotificationEndPoint"

} }

- A member (following the same data representation and nesting structure) with value equal to an NGSI-LD

Null shall cause for the member to be removed from the target NGSI-LD element.

EXAMPLE 2: The following NGSI-LD Fragment allows the modification of an Entity by changing its batteryLevel Attribute, updating the observedAt sub-Attribute, removing the providedBy sub-Attribute and removing the uncharged Attribute from the Entity:

{

"id": "urn:ngsi-ld:TemperatureSensor:001",
"type": "TemperatureSensor",
"batteryLevel": {
"type": "Property",
"value": 7,
"observedAt": "2022-03-14T12:51:02.000Z",
"providedBy": "urn:ngsi-ld:null"
},
"uncharged" : {
"type": "Property",
"value": "urn:ngsi-ld:null"

} }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Fragments
