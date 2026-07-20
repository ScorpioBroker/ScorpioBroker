---
type: NGSI-LD Data Type
title: "Introduction"
description: "Implementations shall support the data types defined by the clauses below."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.1"
---

Implementations shall support the data types defined by the clauses below. For each member defined by each data type
(including nested ones) a term shall be added to the Core @context, as mandated by clause 4.5.
None of the members described admit a null value directly, as when a JSON-LD processor encounters null, the
associated entry or value is always removed when expanding the JSON-LD document.
However, in the context of a partial update or merge operation (see clauses 5.5.8 and 5.5.12), an NGSI-LD Null shall be
used to indicate the removal of a target member, as explained in clause 4.5.0. In all other cases, implementations shall
raise an error of type BadRequestData if an NGSI-LD Null value is encountered.
As null cannot be used as a value in JSON-LD, there is still the possibility of using a JSON null literal {"@type":
"@json", "@value": null} instead. JSON literals are not to be expanded in JSON-LD and thus the respective
element is not removed during JSON-LD expansion.
Non-normative JSON Schema [i.11] definitions of the referred data types are also available at [i.13].
The use of URI in the context of the present document also includes the use of International Resource Identifiers (IRIs)
as defined in IETF RFC 3987 [23], which extends the use of characters to Unicode characters [22] beyond the ASCII
character set, enabling the support of languages other than English.

# Related

* [Clause 4.5](/framework/data-representation/ngsi-ld-data-representation.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
