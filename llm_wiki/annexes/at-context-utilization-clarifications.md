---
type: Reference
title: "@context utilization clarifications"
description: "When expanding or compacting JSON-LD terms, the JSON-LD @context to be used is always the one provided in the current API request."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.7
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.7"
---

When expanding or compacting JSON-LD terms, the JSON-LD @context to be used is always the one provided in the current API request. For the benefit of users and implementers the following examples illustrate this concept.

The scenario starts with the creation of an Entity using a JSON-LD @context as follows:

POST /ngsi-ld/v1/entities/

Content-Type: application/ld+json

Content-Length: 200

{

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": "Downtown One" | |
| | | }, | | |

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": 121, | |
| | | | "observedAt": "2017-07-29T12:05:02Z" | |
| | | }, | | |

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffStreetParking",
"name": {
"type": "Property",
"value": "Downtown One"
},
"availableSpotNumber": {
"totalSpotNumber": {
"type": "Property",
"value": 200
},
"@context": [
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": 200 | |
| | | }, | | |

] }

The content of the @context utilized for the referred Entity creation (at http://example.org/ngsi-ld/latest/parking.jsonld) is as follows:

{

"OffStreetParking": "http://example.org/parking/OffStreetParking", "availableSpotNumber": "http://example.org/parking/availableSpotNumber", "totalSpotNumber": "http://example.org/parking/totalSpotNumber", "name": "http://example.org/parking/name"

}

At Entity creation time the implementation will perform the expansion of terms using the JSON-LD @context depicted

above.

Now it is needed to retrieve our initial Entity. For retrieving such Entity, this time, a different JSON-LD @context is

going to be utilized, as follows:

{

"OffP": "http://example.org/parking/OffStreetParking", "ava": "http://example.org/parking/availableSpotNumber", "total": "http://example.org/parking/totalSpotNumber"

}

This new @context, even though it makes use of the same set of Fully Qualified Names, is defining new short strings as terms. The reasons for that could be multiple: to facilitate data consumption by clients, to save some bandwidth, to enable a more (or less) human-readable response payload body in a language different than English, etc.

In this particular case, the result of the Entity retrieval will be as depicted below. It can be observed that the terms defined by the JSON-LD @context provided at retrieval time are used to render the Entity content (compaction), and not the terms that were provided at creation time (which may be no longer known by the broker).

It is also interesting to note that the @context array of the response payload body contains, indeed, our header-supplied

@context:

GET /ngsi-ld/v1/entities/urn:ngsi-ld:OffStreetParking:Downtown1

Accept: application/ld+json


Link:; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"

{

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": "Downtown One" | |
| | | }, | | |

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": 121, | |
| | | | "observedAt": "2017-07-29T12:05:02Z" | |
| | | }, | | |

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "OffP",
"name": {
"type": "Property",
"value": "Downtown One"
},
"ava": {
"total": {
"type": "Property",
"value": 200
},
"@context": [
"http://example.org/ngsi-ld/latest/parking-abbreviated.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

Another interesting case to note is the one when an @context with no matching terms or no @context at all is supplied.

See the following example: GET /ngsi-ld/v1/entities/urn:ngsi-ld:OffStreetParking:Downtown1

Accept: application/ld+json

{

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": "Downtown One" | |
| | | }, | | |

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": 121, | |
| | | | "observedAt": "2017-07-29T12:05:02Z" | |
| | | }, | | |

"id": "urn:ngsi-ld:OffStreetParking:Downtown1",
"type": "http://example.org/parking/OffStreetParking",
"http://example.org/parking/name": {
"type": "Property",
"value": "Downtown One"
},
"http://example.org/parking/availableSpotNumber": {
"http://example.org/parking/totalSpotNumber": {
"type": "Property",
"value": 200
},
"@context": "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

| | | | "type": "Property", | |
| --- | --- | --- | --- | --- |
| | | | "value": 200 | |
| | | }, | | |

}

In this particular case it can be observed that the user names (Entity Type, Attributes) in the response payload body have not been compacted, and as a result the Fully Qualified Names are included. However, the core API terms have been compacted, as the Core @context is always considered to be implicitly present if not specified explicitly (and that is why it is included in the JSON-LD response, as mandated by the specification).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — @context utilization clarifications
