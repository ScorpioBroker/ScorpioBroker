---
type: NGSI-LD Clause
title: "Concise NGSI-LD Relationship"
description: "An NGSI-LD Relationship in shall be represented in a concise but lossless representation by a member whose key is the Relationship name (a term) and whose value is a JSON-LD object (or JSON-LD array w"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.3.3
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.3.3"
---

An NGSI-LD Relationship in shall be represented in a concise but lossless representation by a member whose key is the Relationship name (a term) and whose value is a JSON-LD object (or JSON-LD array with such JSON-LD objects if there are multiple instances with the same Relationship name as described in clause 4.5.5) with the following terms: Mandatory

- "object": the Relationship's object represented by a URI or array of URIs. An NGSI-LD Null (explained in clause 4.5.0 and defined in clause 3.1) can be used as the right-hand side of the "object" during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) to indicate a deletion of the Relationship, as well as in notifications and in temporal evolutions (for encoding a deleted Relationship). Optional
- "datasetId": a URI as mandated by clause 4.5.5.
- "expiresAt": a string as mandated by clause 4.22.
- "ngsildproof": a Property, as mandated by clause 4.5.2, with the non-reified subproperties "entityIdSealed" and "entityTypeSealed" as specified in [35]. The value of its "value" element shall be an object containing the W3C® Data integrity "proof" structure [35]. See clause C.11 for an example.
- "observedAt": a string as mandated by clause 4.8.
- "objectType": a string as mandated by clause 4.5.23.
- "type": If missing, "Relationship" can be inferred by the presence of the "object" attribute.
- For each Relationship this Relationship is associated with, a member whose key is the Relationship name (a term) and whose value is the result of serializing a Relationship (or any of its subclasses) as per the rules of representation of a Relationship in concise representation. (see clause 4.5.3.3).
- For each Property this Relationship is associated with, a member whose key is the Property name (a term) and whose value is the result of serializing a Property (or any of its subclasses) as per the rules of representation of a Property in concise representation. (see clause 4.5.2.3). System Generated
- "createdAt": a string as mandated by clause 4.8.


- "deletedAt": a string as mandated by clause 4.8.
- "instanceId": a URI uniquely identifying a Relationship instance as mandated by clause 4.5.8.
- "modifiedAt": a string as mandated by clause 4.8. Output Only
- "entity": only provided in case of Linked Entity retrieval, and only if the inline join option is explicitly requested (see clause 4.5.23.2), where it used to define the target Linked Entity of a Relationship's object in concise representation.
- "previousObject": only provided if the showChanges option is explicitly requested. It represents the previous Relationship "object", before the triggering change. The representation is the same as that of "object". Furthermore, an NGSI-LD Relationship in the concise representation shall never include the following members: Prohibited
- "entityIdSealed" and "entityTypeSealed" shall never be present.
- "entityList": shall never be present, as it is used during inline Linked Entity retrieval (see clause 4.5.23.2) to define the target Entities of a ListRelationship's object.
- "languageMap" and "previousLanguageMap": shall never be present, as they define a LanguageProperty value.
- "json" and "previousJson": shall never be present, as they define a JsonProperty value.
- "objectList" and "previousObjectList": shall never be present, as they define an ordered array of Relationship object URIs.
- "unitCode": shall never be present, as Relationships are unitless.
- "valueList" and "previousValueList": shall never be present, as they define an ordered array of Property values.
- "value" and "previousValue": shall never be present, as they define a Property value.
- "vocab" and "previousVocab": shall never be present, as they define a VocabProperty value. Notwithstanding the definition above, during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12), an NGSI-LD Relationship with a value of NGSI-LD Null and without a datasetId should be represented in a concise representation by a member whose key is the Relationship name (a term) and whose value is "urn:ngsi ld:null".

# Related

* [Clause 3.1](/overview/terms.md)
* [Clause 4.22](/framework/languages/transient-storage-of-entities-and-attributes.md)
* [Clause 4.5.0](/framework/data-representation/introduction.md)
* [Clause 4.5.2](/framework/data-representation/ngsi-ld-property-representations.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.5.23.2](/framework/data-representation/inline-linked-entity-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.3.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Concise NGSI-LD Relationship
