---
type: NGSI-LD Clause
title: "Introduction"
description: "NGSI-LD defines a specialized type of Property named LanguageProperty, defined by the NGSI-LD @context described by the present document in clause 4.4."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.18.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.18.1"
---

NGSI-LD defines a specialized type of Property named LanguageProperty, defined by the NGSI-LD @context
described by the present document in clause 4.4.
When dealing with NGSI-LD Entities, implementations shall interpret the JSON-LD nodes of type LanguageProperty
as per clause 4.5.18.2 (when in normalized representation) or clause 4.5.18.3 (when in concise representation).
Both normalized and concise representation of LanguageProperties shall be supported by implementations and can be
selected by Context Consumers through specific request parameters. An example of this representation can be
found in annex C, clause C.2.2.


4.5.18.2 Normalized NGSI-LD LanguageProperty

An NGSI-LD LanguageProperty shall be represented in normalized representation by a member whose key is the
Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.2, with the following differences:
Mandatory
- "languageMap": a JSON object consisting of a set of a non-empty language tags as defined by IETF RFC 5646 [28] or the language tag "@none" which represents a default language, with each language tag mapping to a single string or array of strings. It represents a more specialized value. An NGSI-LD Null used during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) shall be encoded as the JSON object {"@none": "urn:ngsi-ld:null"}. The same representation is also used to indicate a deletion in notifications and in temporal evolutions for encoding a deleted LanguageProperty.
- "type": the fixed value "LanguageProperty". Output Only
- "previousLanguageMap": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous LanguageProperty languageMap, before the triggering change. The representation is the same as that of "languageMap". Furthermore, an NGSI-LD LanguageProperty in the normalized representation shall never include the following members: Prohibited
- "unitCode": shall never be present, as language maps are always strings and hence unitless.
- "value" and "previousValue": shall never be present, as value is a generalization of languageMap.

4.5.18.3 Concise NGSI-LD LanguageProperty

An NGSI-LD LanguageProperty shall be represented in concise but lossless representation by a member whose key is
the Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.3, with the following differences:
Mandatory
- "languageMap": a JSON object consisting of a set of a non-empty language tags as defined by IETF RFC 5646 [28] or the language tag "@none" which represents a default language, with each language tag mapping to a single string or array of strings. It represents a more specialized value. An NGSI-LD Null used during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12) shall be encoded as the JSON object {"@none": "urn:ngsi-ld:null"}. The same representation is also used to indicate a deletion in notifications and the temporal evolutions for encoding a deleted LanguageProperty.

Optional

- "type": If missing, "LanguageProperty" can be inferred by the presence of the "languageMap" attribute. Output Only
- "previousLanguageMap": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous LanguageProperty languageMap, before the triggering change. The representation is the same as that of "languageMap". Furthermore, an NGSI-LD LanguageProperty in the concise representation shall never include the following members:


Prohibited

- "unitCode": shall never be present, as language maps are always strings and hence unitless.
- "value" and "previousValue": shall never be present, as it is a generalization of "languageMap". Notwithstanding the definition above, during partial update patch and merge patch (see clauses 5.5.8 and 5.5.12), an NGSI-LD LanguageProperty with a value of NGSI-LD Null without a datasetId should be represented in a concise representation by a member whose key is the LanguageProperty name (a term) and whose value is "urn:ngsi-ld:null".

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.18.2](/framework/data-representation/normalized-ngsi-ld-languageproperty.md)
* [Clause 4.5.18.3](/framework/data-representation/concise-ngsi-ld-languageproperty.md)
* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.18.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
