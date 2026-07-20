---
type: NGSI-LD Clause
title: "NGSI-LD ListProperty Representations"
description: "4.5.21.1 Introduction NGSI-LD defines a specialized type of Property named ListProperty, defined by the NGSI-LD @context described by the present document in clause 4.4."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.21
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.21"
---

4.5.21.1 Introduction

NGSI-LD defines a specialized type of Property named ListProperty, defined by the NGSI-LD @context described by
the present document in clause 4.4.
When dealing with NGSI-LD Entities, implementations shall interpret the JSON-LD nodes of type ListProperty as per
clause 4.5.21.2 (when in normalized representation) or clause 4.5.21.3 (when in concise representation).
Both normalized and concise representation of ListProperties shall be supported by implementations and can be
selected by Context Consumers through specific request parameters. An example of this representation can be
found in annex C, clause C.2.2.

4.5.21.2 Normalized NGSI-LD ListProperty

An NGSI-LD ListProperty shall be represented in normalized representation by a member whose key is the Property
name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation defined in
clause 4.5.2.2, with the following differences:
Mandatory
- "type": the fixed value "ListProperty".
- "valueList": a JSON representation ordered array of Property Values (see definition of NGSI-LD Value in clause 3.1). It represents a more specialized value. An array consisting of a single NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "valueList" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the ListProperty, as well as in notifications and in temporal evolutions (for encoding a deleted ListProperty). Output Only
- "previousValueList": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous ListProperty valueList, before the triggering change. The representation is the same as that of "valueList". Furthermore, an NGSI-LD ListProperty in the normalized representation shall never include the following members: Prohibited
- "value" and "previousValue": shall never be present, as value is a generalization of valueList.

4.5.21.3 Concise NGSI-LD ListProperty

An NGSI-LD ListProperty shall be represented in concise but lossless representation by a member whose key is the
ListProperty name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.3, with the following differences:
Mandatory
- "valueList": a JSON representation of a ordered array of Property Values (see definition of NGSI-LD Value in clause 3.1). It represents a more specialized value. An array consisting of a single NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "valueList" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the ListProperty, as well as in notifications and in temporal evolutions (for encoding a deleted ListProperty).

Optional - "type": If missing, "ListProperty" can be inferred by the presence of the "valueList" attribute.


Output Only

- "previousValueList": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous ListProperty "valueList", before the triggering change. The representation is the same as that of "valueList". Furthermore, an NGSI-LD ListProperty in the concise representation shall never include the following members: Prohibited
- "value" and "previousValue": shall never be present, as value is a generalization of valueList.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.5.21.2](/framework/data-representation/normalized-ngsi-ld-listproperty.md)
* [Clause 4.5.21.3](/framework/data-representation/concise-ngsi-ld-listproperty.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.21](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD ListProperty Representations
