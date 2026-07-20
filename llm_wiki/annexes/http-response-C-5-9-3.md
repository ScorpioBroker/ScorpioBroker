---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/json Link:; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\" { \"id\": \"http://example.org/vehicle/Vehicle\", \"type\": \"EntityTypeInfo\", \"typeName\":"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.9.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.9.3"
---

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

{

"id": "http://example.org/vehicle/Vehicle",
"type": "EntityTypeInfo",
"typeName": "Vehicle",
"entityCount": 2,
"attributeDetails": [
{
"id": "http://example.org/vehicle/brandName",
"type": "Attribute",
"attributeName": "brandName",
"attributeTypes": [
"Property"

] }, {

"id": "http://example.org/vehicle/isParked",
"type": "Attribute",
"attributeName": "isParked",
"attributeTypes": [
"Relationship"

] }, {

"id": "https://uri.etsi.org/ngsi-ld/location",
"type": "Attribute",
"attributeName": "location",
"attributeTypes": [
"GeoProperty"

] }, {

"id": "http://example.org/vehicle/speed",
"type": "Attribute",
"attributeName": "speed",
"attributeTypes": [
"Property"

] } ] }

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.9.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
