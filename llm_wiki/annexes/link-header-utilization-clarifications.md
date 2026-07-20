---
type: Reference
title: "Link header utilization clarifications"
description: "The JSON-LD Specification [2] states clearly that only one HTTP Link header with the link relationship is required to appear."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.8
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.8"
---

The JSON-LD Specification [2] states clearly that only one HTTP Link header with the link relationship is required to appear. Such statement has implications in terms of providing the JSON-LD @context when using the NGSI-LD API. The main implication is that if the @context is a compound one, i.e. an @context which references multiple individual @context, served by resources behind different URIs, then a wrapper @context has to be created and hosted. The final aim is that only one @context is referenced from the

JSON-LD Link header. This can be illustrated with an example:

Imagine that it is desired to create an Entity providing @context terms which are defined in two different JSON-LD

@context resources:

- http://example.org/vehicle/v1/vehicle-context.jsonld

- https://schema.org


If a developer wants to reference these two @context resources from a Link header, a wrapper @context can be easily created as follows:

{

"@context": [ "http://example.org/vehicle/v1/vehicle-context.jsonld", "https://schema.org", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

]

}
As such wrapper @context needs to be referenced from a Link header by using a URI, then it will have to be hosted at
some place on the Web. Usually, developers will host @context using popular and simple solutions such as GitHub or
GitLab pages. As a result, developers will be able to use @context in queries or when using "application/json"
as main content type managed by their applications.
It is a good practice to include the Core @context in the wrapper @context so it can be used, off-the-shelf, by external
JSON-LD processing tools. However, it should be noted this is not necessary for NGSI-LD, as the Core @context is
always implicitly included.
Then, using such wrapper @context, (in our example hosted at https://hosting.example.com/ngsi-ld/v1/wrapper
context.jsonld), the developer will be able to issue requests like:
POST /ngsi-ld/v1/entities/
Content-Type: application/json
Content-Length: 200
Link:; rel="http://www.w3.org/ns/json
ld#context"; type="application/ld+json"

{

"id": "urn:ngsi-ld:Vehicle:V1",
"type": "Vehicle",
"builtYear": {
"type": "Property",
"value": "2014"
},
"speed": {
"type": "Property",
"value": 121,
"observedAt": "2017-07-29T12:05:02Z"

} }

201 Created
Location: /ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:V1
Link: < https://hosting.example.com/ngsi-ld/v1/wrapper-context.jsonld >; rel="http://www.w3.org/ns/json
ld#context"; type="application/ld+json"
GET /ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:V1
Accept: application/ld+json
Link:; rel="http://www.w3.org/ns/json
ld#context"; type="application/ld+json"
200 OK
Content-Type: application/ld+json

{

"id": "urn:ngsi-ld:Vehicle:V1",
"type": "Vehicle",
"builtYear": {
"type": "Property",
"value": "2014"

},


"speed": { "type": "Property", "value": 121, "observedAt": "2017-07-29T12:05:02Z" }, "@context": "https://hosting.example.com/ngsi-ld/v1/wrapper-context.jsonld"

}

Observe that in this case the broker is responding with the same wrapper @context in the Link header of the HTTP

Response or within the JSON-LD response payload body (when MIME type accepted is "application/ld+json"). However, that could not be always the case, as there could be situations where the broker could need to provide a wrapper @context hosted by itself, for instance, when there are inline @context terms or when the Core @context has not been previously included by the wrapper @context (not recommended) provided

within developer's requests.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Link header utilization clarifications
