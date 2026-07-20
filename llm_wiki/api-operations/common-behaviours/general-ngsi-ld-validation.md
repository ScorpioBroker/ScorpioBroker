---
type: NGSI-LD Clause
title: "General NGSI-LD validation"
description: "All the operations that take a JSON-LD document as input shall process such JSON-LD document as follows: - If the request payload body is not a valid JSON document then an error of type InvalidRequest"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.4"
---

All the operations that take a JSON-LD document as input shall process such JSON-LD document as follows:

- If the request payload body is not a valid JSON document then an error of type InvalidRequest shall be raised.

- If the data included by the JSON-LD document is not syntactically correct, according to the @context or the API data type definitions, then an error of type BadRequestData shall be raised.

- A Context Producer may supply an additional hint regarding the overall validity of the payload body and the version of the NGSI-LD specification that the API datatype definitions conform to. When receiving such an annotated context production request, a Context Broker that it is only partially capable of interpreting the datatypes held within the payload body may use this information to amend the data held within the payload through applying fallbacks (see clause 4.3.6.8) prior to validation.


- Any attempt to use "urn:ngsi-ld:null" as a first level member value (" ": "urn:ngsi ld:null"), with the exception of NGSI-LD Fragments (see clause 5.4) used in partial update and merge operations (as mandated by clause 5.5.8 and clause 5.5.12) or to represent deleted Properties in concise representation as part of notifications, shall result in an error of type BadRequestData.
- Any attempt to use "urn:ngsi-ld:null" as the right-hand side of value in a Property, as the right-hand side of object in a Relationship or to use {"@none": "urn:ngsi-ld:null"} as the right-hand side of languageMap, with the exception of NGSI-LD Fragments (see clause 5.4) used in update and merge operations (as mandated by clauses 5.5.8 and 5.5.12) and the representation of deleted Properties, Relationships or Language Properties in notifications and the temporal evolution, shall result in an error of type BadRequestData.
- Any attempt to use "urn:ngsi-ld:null" as the value of a key value pair within a JSON object, which is the right-hand side of the value of a Property, with the exception of NGSI-LD Fragments used in merge operations (see clause 5.5.12), shall result in an error of type BadRequestData.

# Related

* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 5.4](/api-operations/ngsi-ld-fragments.md)
* [Clause 5.5.12](/api-operations/common-behaviours/merge-patch-behaviour.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — General NGSI-LD validation
