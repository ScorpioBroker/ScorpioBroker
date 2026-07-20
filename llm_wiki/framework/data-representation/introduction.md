---
type: NGSI-LD Clause
title: "Introduction"
description: "All NGSI-LD elements are represented in JSON-LD [2]."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.0
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.0"
---

All NGSI-LD elements are represented in JSON-LD [2]. For the use with the API, the compacted JSON-LD
representation is used, i.e. short terms are used, which are expanded by the component implementing the NGSI-LD API
using a JSON-LD @context, typically provided as part of the request. As described in clause 4.4, the NGSI-LD Core
@context is always considered to be part of the @context to be used.
The use of JSON-LD for NGSI-LD elements has some implications for the use of null values, as JSON-LD interprets
setting elements to null as elements to be removed when performing JSON-LD expansion. Thus, null cannot be used as
a value in NGSI-LD.
To nevertheless allow deletions as part of NGSI-LD operations that update NGSI-LD data, which is typically handled
by setting the respective JSON key to null (e.g. as in IETF RFC 7396 [16]), the URI "urn:ngsi-ld:null" is used
as a replacement for null in all places where URI strings are valid JSON values. If an array is required, an array with
one single NGSI-LD Null is used. For languageMap, the JSON object {"@none": "urn:ngsi-ld:null"} is to
be used as explained in clause 4.5.18. These encodings of null are referred to as NGSI-LD Null.
For representing deleted elements in notifications and in the temporal representation, the URI "urn:ngsi
ld:null" is used as a Property value or Relationship object and the JSON object {"@none": "urn:ngsi
ld:null"} for the "languageMap" of a Language Property, respectively.
As null cannot be used as a value in JSON-LD, there is still the possibility of using a JSON null literal represented as
{"@type": "@json", "@value": null} in JSON-LD instead. JSON literals are not to be expanded in JSON
LD and thus the respective element is not removed during JSON-LD expansion.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.18](/framework/data-representation/ngsi-ld-languageproperty-representations.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.0](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
