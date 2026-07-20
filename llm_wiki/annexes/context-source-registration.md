---
type: Reference
title: "Context Source Registration"
description: "Below there is an example representation of a Context Source Registration."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.3
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.3"
---

Below there is an example representation of a Context Source Registration. It makes use of the @context

formerly described.

{

"id": "urn:ngsi-ld:ContextSourceRegistration:csr1a3456",
"type": "ContextSourceRegistration",
"information": [
{

"entities": [ {

"id": "urn:ngsi-ld:Vehicle:A456", "type": "Vehicle"

}

], "propertyNames": ["brandName","speed"], "relationshipNames": ["isParked"] }, {

"entities": [ {

"idPattern": ".*downtown$", "type": "OffStreetParking"

}, {

"idPattern": ".*47$", "type": "OffStreetParking"

}

], "propertyNames": ["availableSpotNumber","totalSpotNumber"], "relationshipNames": ["isNextToBuilding"]

}
],
"endpoint": "http://my.csource.org:1026",
"location": {
"type": "Polygon",
"coordinates": [
[ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
[100.0, 1.0], [100.0, 0.0]] ]
},
"managementInterval": {
"startAt": " 2017-11-29T14:53:15Z"
},
"@context": [
"http://example.org/ngsi-ld/latest/commonTerms.jsonld",
"http://example.org/ngsi-ld/latest/vehicle.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

The Registration is referring to a Temporal Context Source capable of providing temporal information from Entities of type "Vehicle" and "OffStreetParking", meeting certain id requirements. More concretely, it can only provide the referenced Properties and Relationships. Temporal information is provided for the given managementInterval, i.e. related to createdAt and modifiedAt Temporal Properties. The managementInterval is

specified as an open interval, so only a starting point is given. In addition, the Registration example covers a particular

geographical area.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Source Registration
