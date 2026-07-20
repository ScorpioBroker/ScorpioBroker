---
type: NGSI-LD Clause
title: "Core and user NGSI-LD @context"
description: "NGSI-LD serialization is based on JSON-LD [2], a JSON-based format to serialize Linked Data."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.4"
---

NGSI-LD serialization is based on JSON-LD [2], a JSON-based format to serialize Linked Data. The @context in JSON-LD is used to expand terms, provided as short-hand strings, to concepts, specified as URIs, and vice versa, to compact URIs into terms. The Core NGSI-LD (JSON-LD) @context is defined as a JSON-LD @context which contains:

- The core terms needed to uniquely represent the key concepts defined by the NGSI-LD Information Model, as mandated by clause 4.2.
- The terms needed to uniquely represent all the members that define the API-related Data Types, as mandated by clauses 5.2 and 5.3.
- A fallback @vocab rule to expand or compact user-defined terms to a default URI, in case there is no other possible expansion or compaction as per the current @context.
- The core NGSI-LD @context defines the term "id", which is mapped to @id, and the term "type", which is mapped to @type. Since @id and @type are what is typically used in JSON-LD, they may also be used in NGSI-LD requests instead of "id" and "type" respectively, wherever this is applicable. In NGSI-LD responses, only "id" and "type" shall be used. NGSI-LD compliant implementations shall support such Core @context, which shall be implicitly present when processing or generating context information. Furthermore, the Core @context is protected and shall remain immutable and invariant during expansion or compaction of terms. Therefore, and as per the JSON-LD processing rules [2], when processing NGSI-LD content, implementations shall consider the Core @context as if it were in the last position of the @context array. Nonetheless, for the sake of compatibility and cleanness, data providers should generate JSON-LD content that conveys the Core @context in the last position. For the avoidance of doubt, when rendering NGSI-LD Elements, the Core @context shall always be treated as if it had been originally placed in the last position, so that, if needed, upstream JSON-LD processors can properly expand as NGSI-LD or override the resulting JSON-LD documents provided by API implementations. The NGSI-LD Core @context is publicly available at https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.9.jsonld and shall contain all the terms as mandated by annex B. In addition to the terms defined by the Core NGSI-LD @context (mandatory as per annex B), a user @context should be provided and it should contain the following terms:
- One term associated to the Entity Type, mapping the Entity Type name with its Type Identifier (URI).
- One term associated to the name of each Property or any of its subclasses mapping the Property name with its Property Identifier (URI). If the Property's range is a data type different than a native JSON type, then it shall be conveyed explicitly under this term by using a nested JSON object in the form: - "@type": <Datatype's URI>. - "@id": <Property's URI>.


- One term associated to the name of each Relationship mapping the Relationship name with the Relationship Identifier (URI) in the form: - "@type": "@id". - "@id": <Relationship's URI>. Whereas the Core NGSI-LD @context contains JSON-LD Scoped Contexts, the user @context shall not contain JSON LD Scoped Contexts (see [2], clause 4.1.8), as described in clause 5.5.7, because this would enable overwriting terms defined in the Core NGSI-LD @context. Depending on the binding, the @context may not just be provided embedded with the rest of the JSON content, but there could be other options. For example, in the HTTP binding, the @context can be made available through a Link header (see clause 6.3.5).

# Related

* [Clause 4.2](/framework/information-model/ngsi-ld-information-model.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Core and user NGSI-LD @context
