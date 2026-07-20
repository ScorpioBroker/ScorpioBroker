---
type: Reference
title: "HTTP Response"
description: "200 OK Content-Type: application/json Link:; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\" [ { \"id\": \"http://example.org/vehicle/brandName\", \"type\": \"Attribute\", \"attributeNam"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.5.11.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.5.11.3"
---

200 OK

Content-Type: application/json

Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

[ {

"id": "http://example.org/vehicle/brandName",
"type": "Attribute",
"attributeName": "brandName",
"typeNames": [
"Vehicle"

] }, {

"id": "http://example.org/vehicle/isParked",
"type": "Attribute",
"attributeName": "isParked",
"typeNames": [
"Vehicle"

] }, {

"id": "https://uri.etsi.org/ngsi-ld/location",
"type": "Attribute",
"attributeName": "location",
"typeNames": [
"Vehicle",
"OffStreetParking",
"http://example.org/parking/ParkingSpot"

] }, {

"id": "http://example.org/vehicle/speed",
"type": "Attribute",
"attributeName": "speed",
"typeNames": [
"Vehicle"

] }, {

"id": "http://example.org/parking/status",
"type": "Attribute",
"attributeName": "http://example.org/parking/status",
"typeNames": [
"http://example.org/parking/ParkingSpot"

] } ]

NOTE: The attribute name and all type names that can be found in the provided @context are given as short names, the others as Fully Qualified Names (FQNs). The id is always an FQN.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.5.11.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — HTTP Response
