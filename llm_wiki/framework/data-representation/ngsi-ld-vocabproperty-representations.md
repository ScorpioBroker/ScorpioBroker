---
type: NGSI-LD Clause
title: "NGSI-LD VocabProperty Representations"
description: "4.5.20.1 Introduction NGSI-LD defines a specialized type of Property named VocabProperty, defined by the NGSI-LD @context described by the present document in clause 4.4."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.20
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.20"
---

4.5.20.1 Introduction

NGSI-LD defines a specialized type of Property named VocabProperty, defined by the NGSI-LD @context described by the present document in clause 4.4.


When dealing with NGSI-LD Entities, implementations shall interpret the JSON-LD nodes of type VocabProperty as per clause 4.5.20.2 (when in normalized representation) or clause 4.5.20.3 (when in concise representation). Both normalized and concise representation of VocabProperties shall be supported by implementations and can be selected by Context Consumers through specific request parameters. An example of this representation can be found in annex C, clause C.2.2.

4.5.20.2 Normalized NGSI-LD VocabProperty

An NGSI-LD VocabProperty shall be represented in normalized representation by a member whose key is the
Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.2, with the following differences:
Mandatory
- "type": the fixed value "VocabProperty".
- "vocab": a JSON object consisting of a single string or array of strings which can be type coerced into an IRI or array of IRIs. It represents a more specialized value. Output Only
- "previousVocab": only provided in case of notifications and only if the showChanges option is explicitly requested. It represents the previous VocabProperty vocab, before the triggering change. The representation is the same as that of "vocab". Furthermore, an NGSI-LD VocabProperty in the normalized representation shall never include the following members: Prohibited
- "unitCode": shall never be present, as vocabs are always strings and hence unitless.
- "value" and "previousValue": shall never be present, as value is a generalization of vocab.

4.5.20.3 Concise NGSI-LD VocabProperty

An NGSI-LD VocabProperty shall be represented in concise but lossless representation by a member whose key is
the Property name (a term), whose value is the same as the JSON-LD object in NGSI-LD Property Representation
defined in clause 4.5.2.3, with the following differences.
Mandatory
- "vocab": a JSON object consisting of a single string or array of strings which can be type coerced into an IRI or array of IRIs. It represents a more specialized value.

Optional

- "type": If missing, "VocabProperty" can be inferred by the presence of the "vocab" attribute. Output Only
- "previousVocab": only provided only in the case of notifications and only if the showChanges option is explicitly requested. It represents the previous VocabProperty vocab, before the triggering change. The representation is the same as that of "vocab". Furthermore, an NGSI-LD VocabProperty in the concise representation shall never include the following members: Prohibited
- "unitCode": shall never be present, as vocabs are always strings and hence unitless.
- "value" and "previousValue": shall never be present, as it is a generalization of vocab.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
* [Clause 4.5.2.2](/framework/data-representation/normalized-ngsi-ld-property.md)
* [Clause 4.5.2.3](/framework/data-representation/concise-ngsi-ld-property.md)
* [Clause 4.5.20.2](/framework/data-representation/normalized-ngsi-ld-vocabproperty.md)
* [Clause 4.5.20.3](/framework/data-representation/concise-ngsi-ld-vocabproperty.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.20](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD VocabProperty Representations
