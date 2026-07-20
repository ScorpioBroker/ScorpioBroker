---
type: NGSI-LD Clause
title: "Normalized NGSI-LD ListProperty"
description: "An NGSI-LD ListProperty shall be represented in normalized representation by a member whose key is the Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Represe"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.21.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.21.2"
---

An NGSI-LD ListProperty shall be represented in normalized representation by a member whose key is the Property
name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation defined in
clause 4.5.2.2, with the following differences:
Mandatory
- "type": the fixed value "ListProperty".
- "valueList": a JSON representation ordered array of Property Values (see definition of NGSI-LD Value in clause 3.1). It represents a more specialized value. An array consisting of a single NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "valueList" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the ListProperty, as well as in notifications and in temporal evolutions (for encoding a deleted ListProperty). Output Only
- "previousValueList": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous ListProperty valueList, before the triggering change. The representation is the same as that of "valueList". Furthermore, an NGSI-LD ListProperty in the normalized representation shall never include the following members: Prohibited
- "value" and "previousValue": shall never be present, as value is a generalization of valueList.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.21.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Normalized NGSI-LD ListProperty
