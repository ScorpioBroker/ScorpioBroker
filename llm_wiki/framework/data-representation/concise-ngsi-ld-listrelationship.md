---
type: NGSI-LD Clause
title: "Concise NGSI-LD ListRelationship"
description: "An NGSI-LD ListRelationship shall be represented in concise but lossless representation by a member whose key is the Relationship name (a term), whose value is the same as the JSON-LD object in NGSI-L"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.22.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.22.3"
---

An NGSI-LD ListRelationship shall be represented in concise but lossless representation by a member whose key is the
Relationship name (a term), whose value is the same as the JSON-LD object in NGSI-LD Relationship
Representation defined in clause 4.5.3.3, with the following differences:
Mandatory
- "objectList": this represents a more specialized object and is represented by an ordered array of either: - a JSON objects containing a single Attribute with a key called "object" and its value holding the Relationship's object represented by a URI. - Strings representing URIs only, where expansion to Relationship objects can be inferred by the presence of the "objectList" attribute. An array consisting of a single NGSI-LD Null (explained in 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "objectList" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the ListRelationship, as well as in notifications and in temporal evolutions (for encoding a deleted ListRelationship).

Optional

- "type": If missing, "ListRelationship" can be inferred by the presence of the "objectList" attribute. Output Only
- "entityList": only provided if the inline join option is explicitly requested (see clause 4.5.23.2), where it is used to define the target Linked Entities of a ListRelationship's "objectList" in concise representation.
- "previousObjectList": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous ListRelationship objectList, before the triggering change. Optional. The representation is the same as that of "objectList". Furthermore, an NGSI-LD ListRelationship in the concise representation shall never include the following members: Prohibited
- "entity": shall never be present as entity is a generalization of entityList.
- "object" and "previousObject": shall never be present as object is a generalization of objectList.

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.5.23.2](/framework/data-representation/inline-linked-entity-representation.md)
* [Clause 4.5.3.3](/framework/data-representation/concise-ngsi-ld-relationship.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.22.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Concise NGSI-LD ListRelationship
