---
type: Reference
title: "@context processing clarifications"
description: "JSON-LD Specification [2] says that \"If a term is redefined within a context, all previous rules associated with the previous definition are removed\"."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-C.9
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "C.9"
---

JSON-LD Specification [2] says that "If a term is redefined within a context, all previous rules associated with the previous definition are removed". In addition, it is stated that "Multiple contexts may be combined using an array,

which is processed in order".

In contrast to the JSON-LD Specification, the NGSI-LD specification states that the Core @context is protected and has to remain immutable. This essentially means that the Core @context has final precedence and, therefore, is always to be processed as the last one in the @context array. For clarity, data providers should place the Core @context in the final position. From the point of view of Data providers, care has to be taken so that there are no unexpected or undesired

term expansions. See the following example:

{

"id": "urn:ngsi-ld:Building:B1",
"type": "Building",
"name": {
"type": "Property",
"value": "Empire State"
},
"location": {
"type": "Property",
"value": "20 West 34th Street, New York City, NY 10001"
},
"@context": [
"https://schema.org/version/latest/schemaorg-current-https.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }

The problem of the example above is that the term "location" is defined in both the Core @context and the schema.org user @context and the Core @context takes precedence for the expansion. In these situations, if one wanted to refer to the schema.org "location", the solution is to prefix the conflicting terms, so that there cannot be any clashing. Therefore, if the intent is to refer to https://schema.org/location throughout, the example above can be

modified as shown below:

{

"id": "urn:ngsi-ld:Building:B1",
"type": "Building",
"name": {
"type": "Property",
"value": "Empire State"
},
"schema:location": {
"type": "Property",
"value": "20 West 34th Street, New York City, NY 10001"
},
"@context": [
"https://schema.org/version/latest/schemaorg-current-https.jsonld",
"http://example.org/ngsi-ld/latest/parking.jsonld",
"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld"

] }


Note that the Core @context should be placed in the last position of the @context array. NGSI-LD implementations are required to render content following this approach, which has been undertaken in order to maximize compatibility with JSON-LD processing tools. This example works because the "schema:" prefix has already been defined by the

schema.org @context.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause C.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — @context processing clarifications
